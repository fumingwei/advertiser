import os
import ulid
from pprint import pformat
from decimal import Decimal
from sqlalchemy import asc
from libs.internal.api_service import APIService
from libs.internal.crm_external_service import CRMExternalService
from settings.db import SessionLocal
from settings.base import configs
from settings.log import celery_log
from my_celery.main import celery_app
from apps.finance.define import (
    TransferTradeType,
    TransferTradeResult,
    BalanceTransferStatus, TransferType,
)
from apps.finance.models import (
    BalanceTransfer,
    BalanceTransferRequest,
    BalanceTransferDetail,
)
from tools.constant import InternalRequestStatus, ExternalRequestStatus
from apps.finance.utils import get_expire_account_from_redis, empty_expire_account_from_redis

api_key = configs.MAPI_KEY
file_path = os.getcwd()


def balance_transfer_refund(balance_transfer_id: int):
    """
    余额转移-转出流程(路由函数调用)
    """
    try:
        with SessionLocal() as db:
            refund_accounts = (
                db.query(BalanceTransferDetail)
                .filter(
                    BalanceTransferDetail.balance_transfer_id == balance_transfer_id,
                    BalanceTransferDetail.trade_type == TransferTradeType.REFUND.value,
                    BalanceTransferDetail.trade_result
                    == ExternalRequestStatus.EMPTY.value,
                )
                .order_by(asc(BalanceTransferDetail.order_num))
                .all()
            )

            refund_params = []
            refund_account_ids = []
            for i in refund_accounts:
                refund_params.append(
                    {
                        "account_id": i.account_id,
                        "recharge_num": -i.amount,  # 负值
                        "medium": i.medium,
                        "bc_id": i.bc_id,
                    }
                )
                refund_account_ids.append(i.account_id)
            # 保存余额转移请求状态为REQUEST_REDAY
            balance_transfer_request = (
                db.query(BalanceTransferRequest)
                .filter(
                    BalanceTransferRequest.balance_transfer_id == balance_transfer_id,
                    BalanceTransferRequest.trade_type == TransferTradeType.REFUND.value,
                )
                .first()
            )
            if balance_transfer_request:
                celery_log.log_info("已存在余额转出的请求")
                return
            balance_transfer_request = BalanceTransferRequest(
                balance_transfer_id=balance_transfer_id,
                trade_type=TransferTradeType.REFUND.value,
                internal_request_status=InternalRequestStatus.REQUEST_REDAY.value,
                transfer_type=TransferType.ACCOUNT.value
            )
            db.add(balance_transfer_request)
            db.commit()
            celery_log.log_info(f"请求参数:\n{pformat(refund_params)}")
            json_ = {"recharge_data": refund_params}
            res_result = APIService.recharge(json=json_, **{'trace_id': str(ulid.new())})
            res_code = res_result.get("code")
            if res_code == 0:
                # 更新余额转移请求状态为SUCCESS,并保存request_id
                external_request_id = res_result.get("data", {}).get("request_id")
                external_request_status = res_result.get("data", {}).get(
                    "request_status", ExternalRequestStatus.RECEIVED.value
                )
                balance_transfer_request.internal_request_status = (
                    InternalRequestStatus.REQUEST_SUCCESS.value
                )
                balance_transfer_request.external_request_id = external_request_id
                balance_transfer_request.external_request_status = (
                    external_request_status
                )
                db.commit()
            else:
                # 更新余额转移请求状态为FAILURE
                balance_transfer_request.internal_request_status = (
                    InternalRequestStatus.REQUEST_FAILURE.value
                )
                # 更新余额转移工单为全部失败
                db.query(BalanceTransfer).filter(
                    BalanceTransfer.id == balance_transfer_id
                ).update(
                    {
                        "transfer_status": BalanceTransferStatus.COMPLETE_FAILURE.desc,
                        "remark": "余额转移全部失败，网络异常。",
                    }
                )
                # 更新余额转移-转出账户的详情为失败
                db.query(BalanceTransferDetail).filter(
                    BalanceTransferDetail.balance_transfer_id == balance_transfer_id,
                    BalanceTransferDetail.trade_type == TransferTradeType.REFUND.value,
                ).update({"trade_result": TransferTradeResult.FAILURE.value, "remark": "余额转出失败，网络异常。"})

                # 更新余额转移-转入账户的详情为失败
                db.query(BalanceTransferDetail).filter(
                    BalanceTransferDetail.balance_transfer_id == balance_transfer_id,
                    BalanceTransferDetail.trade_type == TransferTradeType.RECHARGE.value,
                ).update({"trade_result": TransferTradeResult.FAILURE.value, "remark": "余额转出失败，网络异常。"})
                db.commit()

                # 此时Redi广告账户还在Redis中标记，剩余的充值账户不会再请求充值，需要删除Redis中余额转移的账户信息，以便可以进行下一笔余额转移。
                recharge_accounts = db.query(BalanceTransferDetail.account_id).filter(
                    BalanceTransferDetail.balance_transfer_id == balance_transfer_id,
                    BalanceTransferDetail.trade_type == TransferTradeType.RECHARGE.value,
                ).all()
                recharge_account_ids = [i.account_id for i in recharge_accounts]
                empty_expire_account_from_redis(refund_account_ids + recharge_account_ids)
    except Exception as e:
        celery_log.log_error(f'文件位置：{file_path}，tasks.py发生异常，异常原因：{e}')
        raise e


def balance_transfer_recharge(balance_transfer_id, actual_refund_amount):
    """
    余额转移-转入流程

    余额转出完成后，根据实际转出金额进行余额转入
    """
    celery_log.log_info(f"实际转出金额为{str(actual_refund_amount)}")
    try:
        with SessionLocal() as db:
            recharge_accounts = (
                db.query(BalanceTransferDetail)
                .filter(
                    BalanceTransferDetail.balance_transfer_id == balance_transfer_id,
                    BalanceTransferDetail.trade_type
                    == TransferTradeType.RECHARGE.value,
                    BalanceTransferDetail.trade_result
                    == ExternalRequestStatus.EMPTY.value,
                )
                .order_by(asc(BalanceTransferDetail.order_num))
                .all()
            )
            recharge_params = []
            for i in recharge_accounts:
                # 根据实际转出金额进行按表单账户转入顺序进行余额转入
                if actual_refund_amount <= 0:
                    break
                if i.amount <= actual_refund_amount:
                    recharge_num = Decimal(str(i.amount))
                    actual_refund_amount -= Decimal(str(i.amount))
                else:
                    recharge_num = actual_refund_amount
                    actual_refund_amount -= recharge_num
                recharge_params.append(
                    {
                        "account_id": i.account_id,
                        "recharge_num": str(recharge_num),
                        "medium": i.medium,
                        "bc_id": i.bc_id,
                    }
                )
            balance_transfer_request = (
                db.query(BalanceTransferRequest)
                .filter(
                    BalanceTransferRequest.balance_transfer_id == balance_transfer_id,
                    BalanceTransferRequest.trade_type
                    == TransferTradeType.RECHARGE.value,
                )
                .first()
            )
            if balance_transfer_request:
                celery_log.log_info("已存在余额转入的请求")
                return
            # 保存余额转移请求状态为REQUEST_REDAY
            balance_transfer_request = BalanceTransferRequest(
                balance_transfer_id=balance_transfer_id,
                trade_type=TransferTradeType.RECHARGE.value,
                internal_request_status=InternalRequestStatus.REQUEST_REDAY.value,
            )
            db.add(balance_transfer_request)
            db.commit()
            celery_log.log_info(f"请求参数:\n{pformat(recharge_params)}")
            json_ = {"recharge_data": recharge_params}
            res_result = APIService.recharge(json=json_, **{'trace_id': str(ulid.new())})
            res_code = res_result.get("code")
            if res_code == 0:
                # 更新余额转移请求状态为SUCCESS,并保存request_id
                external_request_id = res_result.get("data", {}).get("request_id")
                external_request_status = res_result.get("data", {}).get(
                    "request_status", ExternalRequestStatus.RECEIVED.value
                )
                balance_transfer_request.internal_request_status = (
                    InternalRequestStatus.REQUEST_SUCCESS.value
                )
                balance_transfer_request.external_request_id = external_request_id
                balance_transfer_request.external_request_status = (
                    external_request_status
                )
                db.commit()
            else:
                # 更新余额转移请求状态为FAILURE
                balance_transfer_request.internal_request_status = (
                    InternalRequestStatus.REQUEST_FAILURE.value
                )
                # 更新余额转移工单为全部失败
                db.query(BalanceTransfer).filter(
                    BalanceTransfer.id == balance_transfer_id
                ).update(
                    {
                        "transfer_status": BalanceTransferStatus.COMPLETE_FAILURE.desc,
                        "remark": f"余额转移网络异常，异常转出余额为{str(actual_refund_amount)}，请联系运营人员进行处理。",
                    }
                )
                # 更新余额转移-转入账户的详情为失败
                db.query(BalanceTransferDetail).filter(
                    BalanceTransferDetail.balance_transfer_id == balance_transfer_id,
                    BalanceTransferDetail.trade_type == TransferTradeType.RECHARGE.value,
                ).update({"trade_result": TransferTradeResult.FAILURE.value, "remark": "余额转入失败，网络异常。"})
                db.commit()
    except Exception as e:
        db.rollback()
        celery_log.log_error(f'文件位置：{file_path}，tasks.py下的balance_transfer_recharge发生异常，异常原因：{e}')
        raise e


@celery_app.task(name="beat_balance_transfer_refund_result")
def beat_balance_transfer_refund_result():
    """
    定时任务-同步余额转出的结果，并根据实际转出的金额进行余额转入
    """
    celery_log.log_info("同步余额转出结果...")
    try:
        with SessionLocal() as db:
            balance_transfer_requests = (
                db.query(BalanceTransferRequest)
                .filter(
                    BalanceTransferRequest.transfer_type == TransferType.ACCOUNT.value,
                    BalanceTransferRequest.trade_type == TransferTradeType.REFUND.value,
                    BalanceTransferRequest.internal_request_status
                    == InternalRequestStatus.REQUEST_SUCCESS.value,
                    BalanceTransferRequest.external_request_status.in_(
                        [
                            ExternalRequestStatus.RECEIVED.value,
                            ExternalRequestStatus.RUNNING.value,
                        ]
                    ),
                )
                .all()
            )

            for i in balance_transfer_requests:
                balance_transfer_id = i.balance_transfer_id
                actual_refund_amount = 0  # 实际转出金额
                request_id = i.external_request_id
                params = {"api_key": api_key, "request_id": request_id}
                res = APIService.get_common_model_result(params, **{'trace_id': str(ulid.new())})
                res_result = res
                celery_log.log_info(f"API响应内容:\n{pformat(res_result)}")
                res_code = res_result.get("code")
                request_status = res_result.get("data", {}).get("request_status")
                if (
                        res_code == 0
                        and request_status == ExternalRequestStatus.FINISHED.value
                ):
                    account_refund_details = res_result.get("data", {}).get("data", [])
                    balance_transfer_remark = ""
                    for i in account_refund_details:
                        account_id = i.get("account_id")
                        recharge_result = i.get("recharge_result")
                        amount = Decimal(str(i.get("recharge_num", 0)))  # 此金额为退款金额为负值
                        remark = i.get("remark")
                        with SessionLocal() as db:
                            update_values = {
                                "trade_result": recharge_result,
                                "amount": amount,
                                "remark": remark,
                            }
                            if recharge_result == TransferTradeResult.SUCCESS.value:
                                # 根据转移状态更新剩余余额
                                update_values["after_balance"] = (
                                        BalanceTransferDetail.before_balance + amount
                                )
                            elif recharge_result == TransferTradeResult.FAILURE.value:
                                # 根据转移状态记录Remark信息
                                if not balance_transfer_remark:
                                    update_values["amount"] = 0
                                    balance_transfer_remark = remark
                            db.query(BalanceTransferDetail).filter(
                                BalanceTransferDetail.balance_transfer_id
                                == balance_transfer_id,
                                BalanceTransferDetail.account_id == account_id,
                                BalanceTransferDetail.trade_type
                                == TransferTradeType.REFUND.value,
                            ).update(update_values)
                            db.commit()
                        if recharge_result == TransferTradeResult.SUCCESS.value:
                            actual_refund_amount += -amount
                    balance_transfer = (
                        db.query(BalanceTransfer)
                        .filter(
                            BalanceTransfer.id == balance_transfer_id,
                        )
                        .first()
                    )
                    if actual_refund_amount == 0:
                        # 当实际转出金额小于等于0时，直接更新余额转移状态为FAILURE
                        balance_transfer.transfer_status = (
                            BalanceTransferStatus.COMPLETE_FAILURE.desc
                        )
                        balance_transfer.remark = f"余额转出异常，实际转出金额为{str(actual_refund_amount)}，{balance_transfer_remark}。"
                        # 更新余额转移-转入账户的详情为失败
                        db.query(BalanceTransferDetail).filter(
                            BalanceTransferDetail.balance_transfer_id == balance_transfer_id,
                            BalanceTransferDetail.trade_type == TransferTradeType.RECHARGE.value,
                        ).update(
                            {
                                "trade_result": TransferTradeResult.FAILURE.value,
                                "amount": 0,
                                "remark": "余额转出金额为0"
                            }
                        )
                        db.commit()
                    else:
                        if balance_transfer_remark:
                            balance_transfer.remark = f"余额转出异常，实际转出金额为{str(actual_refund_amount)}，{balance_transfer_remark}。"
                            db.commit()
                        # 余额转出完成，进行余额转入操作
                        celery_log.log_info(f"实际转出余额: {actual_refund_amount}")
                        balance_transfer_recharge(
                            balance_transfer_id, actual_refund_amount
                        )
                    # 更新余额转移请求状态为SUCCESS,并保存实际转出金额
                    with SessionLocal() as db:
                        db.query(BalanceTransferRequest).filter(
                            BalanceTransferRequest.trade_type
                            == TransferTradeType.REFUND.value,
                            BalanceTransferRequest.balance_transfer_id
                            == balance_transfer_id,
                        ).update(
                            {
                                "external_request_status": ExternalRequestStatus.FINISHED.value,
                                "actual_amount": actual_refund_amount,
                            }
                        )
                        db.commit()
    except Exception as e:
        db.rollback()
        celery_log.log_error(f"beat_balance_transfer_refund_result发生异常:{e}")
        raise e


@celery_app.task(name="beat_account_transfer_purse_result")
def beat_account_transfer_purse_result():
    """
    获取账户转钱包结果
    """
    db = SessionLocal()
    balance_transfer_requests = db.query(BalanceTransferRequest).filter(
        BalanceTransferRequest.external_request_status == ExternalRequestStatus.RECEIVED.value,
        BalanceTransferRequest.transfer_type == TransferType.PURSE.value,
    ).order_by(-BalanceTransferRequest.id).all()[:10]
    for balance_transfer_request in balance_transfer_requests:
        request_id = balance_transfer_request.external_request_id
        params = {"api_key": api_key, "request_id": request_id}
        res_result = APIService.get_common_model_result(params, **{'trace_id': str(ulid.new())})
        res_code = res_result.get("code")
        request_status = res_result.get("data", {}).get("request_status")
        if res_code == 0 and request_status == ExternalRequestStatus.FINISHED.value:
            try:
                account_refund_detail = res_result.get("data", {}).get("data", [])[0]
                recharge_result = account_refund_detail.get("recharge_result")
                balance_transfer = db.query(BalanceTransfer).filter(
                    BalanceTransfer.id == balance_transfer_request.balance_transfer_id
                ).first()
                balance_transfer_details = db.query(BalanceTransferDetail).filter(
                    BalanceTransferDetail.balance_transfer_id == balance_transfer.id
                ).all()
                account_id = db.query(BalanceTransferDetail).filter(
                    BalanceTransferDetail.balance_transfer_id == balance_transfer.id
                ).first().account_id
                balance_transfer_request.external_request_status = ExternalRequestStatus.FINISHED.value
                if recharge_result == "1":
                    balance_transfer.transfer_status = BalanceTransferStatus.COMPLETE_SUCCESS.desc
                    before_balance = account_refund_detail.get("recharge_before_balance")
                    after_balance = account_refund_detail.get("recharge_after_balance")
                    # 更改客户钱包
                    before_purse_balance = None
                    after_purse_balance = None
                    update_purse_status = False
                    crm_res = CRMExternalService.update_customer_purse(
                        json={
                            "customer_id": balance_transfer.customer_id,
                            "balance": abs(float(balance_transfer.transfer_amount)),
                            "user_id": balance_transfer.user_id,
                            "account_id": account_id
                        },
                        **{'trace_id': str(ulid.new()), "is_handle": True}
                    )
                    if crm_res.get("code") == 0:
                        update_purse_status = True
                        before_purse_balance = crm_res["data"]["before_balance"]
                        after_purse_balance = crm_res["data"]["after_balance"]
                    for balance_transfer_detail in balance_transfer_details:
                        if balance_transfer_detail.trade_type == TransferTradeType.REFUND.value:
                            balance_transfer_detail.before_balance = before_balance
                            balance_transfer_detail.after_balance = after_balance
                            balance_transfer_detail.trade_result = TransferTradeResult.SUCCESS.value
                        if balance_transfer_detail.trade_type == TransferTradeType.RECHARGE.value:
                            balance_transfer_detail.before_purse_balance = before_purse_balance
                            balance_transfer_detail.after_purse_balance = after_purse_balance
                            balance_transfer_detail.trade_result = TransferTradeResult.SUCCESS.value
                            if not update_purse_status:
                                balance_transfer.transfer_status = BalanceTransferStatus.PARTIAL_SUCCESS.desc
                                balance_transfer_detail.trade_result = TransferTradeResult.FAILURE.value
                                balance_transfer_detail.remark = "账户转出成功,但转入钱包失败,请联系我司运营人员处理"
                else:
                    balance_transfer.transfer_status = BalanceTransferStatus.COMPLETE_FAILURE.desc
                    for balance_transfer_detail in balance_transfer_details:
                        balance_transfer_detail.trade_result = TransferTradeResult.FAILURE.value
                        if balance_transfer_detail.trade_type == TransferTradeType.REFUND.value:
                            balance_transfer_detail.remark = account_refund_detail.get("remark")
                        if balance_transfer_detail.trade_type == TransferTradeType.RECHARGE.value:
                            balance_transfer_detail.remark = "账户转出失败"
            except Exception as e:
                celery_log.log_error(f"账户转入钱包获取结果失败{e.__str__()}")
                db.rollback()
            else:
                db.commit()
    db.close()


@celery_app.task(name="beat_balance_transfer_recharge_result")
def beat_balance_transfer_recharge_result():
    """
    定时任务-同步余额转入的结果
    """
    celery_log.log_info("同步余额转入结果...")
    try:
        with SessionLocal() as db:
            balance_transfer_requests = (
                db.query(BalanceTransferRequest)
                .filter(
                    BalanceTransferRequest.trade_type
                    == TransferTradeType.RECHARGE.value,
                    BalanceTransferRequest.transfer_type == TransferType.ACCOUNT.value,
                    BalanceTransferRequest.internal_request_status
                    == InternalRequestStatus.REQUEST_SUCCESS.value,
                    BalanceTransferRequest.external_request_status.in_(
                        [
                            ExternalRequestStatus.RECEIVED.value,
                            ExternalRequestStatus.RUNNING.value,
                        ]
                    ),
                )
                .all()
            )
        for i in balance_transfer_requests:
            balance_transfer_id = i.balance_transfer_id
            actual_amount = 0  # 实际转入金额
            request_id = i.external_request_id
            params = {"api_key": api_key, "request_id": request_id}
            res_result = APIService.get_common_model_result(params, **{'trace_id': str(ulid.new())})
            celery_log.log_info(f"API响应内容:\n{pformat(res_result)}")
            res_code = res_result.get("code")
            request_status = res_result.get("data", {}).get("request_status")
            if res_code == 0 and request_status == ExternalRequestStatus.FINISHED.value:
                account_recharge_details = res_result.get("data", {}).get("data", [])
                for i in account_recharge_details:
                    account_id = i.get("account_id")
                    recharge_result = i.get("recharge_result")
                    amount = Decimal(str(i.get("recharge_num", 0)))
                    remark = i.get("remark")
                    with SessionLocal() as db:
                        update_values = {
                            "trade_result": recharge_result,
                            "amount": amount,
                            "remark": remark,
                        }
                        if recharge_result == TransferTradeResult.SUCCESS.value:
                            # 根据转移状态更新剩余余额
                            update_values["after_balance"] = (
                                    BalanceTransferDetail.before_balance + amount
                            )
                        db.query(BalanceTransferDetail).filter(
                            BalanceTransferDetail.balance_transfer_id
                            == balance_transfer_id,
                            BalanceTransferDetail.account_id == account_id,
                            BalanceTransferDetail.trade_type
                            == TransferTradeType.RECHARGE.value,
                        ).update(update_values)
                        db.commit()
                    if recharge_result == TransferTradeResult.SUCCESS.value:
                        actual_amount += amount
                balance_transfer_refund_request = (
                    db.query(BalanceTransferRequest)
                    .filter(
                        BalanceTransferRequest.balance_transfer_id
                        == balance_transfer_id,
                        BalanceTransferRequest.trade_type
                        == TransferTradeType.REFUND.value,
                    )
                    .first()
                )
                actual_refund_amount = balance_transfer_refund_request.actual_amount
                if actual_amount <= 0:
                    # 当实际转入金额小于等于0时，直接更新余额转移状态为FAILURE
                    db.query(BalanceTransfer).filter(
                        BalanceTransfer.id == balance_transfer_id,
                    ).update(
                        {
                            "transfer_status": BalanceTransferStatus.COMPLETE_FAILURE.desc,
                            "remark": f"余额转入失败，异常转出金额为{str(actual_refund_amount)}， 请联系运营人员进行处理。",
                        }
                    )
                    db.commit()
                elif actual_amount > 0:
                    balance_transfer = (
                        db.query(BalanceTransfer)
                        .filter(
                            BalanceTransfer.id == balance_transfer_id,
                        )
                        .first()
                    )
                    balance_transfer_details = (
                        db.query(BalanceTransferDetail)
                        .filter(
                            BalanceTransferDetail.balance_transfer_id == balance_transfer_id
                        )
                        .all()
                    )
                    for i in balance_transfer_details:
                        account_id = i.account_id
                        expire = get_expire_account_from_redis(account_id)
                        if expire:
                            # 检测到Redis中存在未过期的的账户，此时API数据还没更新完，不能修改余额转移状态为全部成功、部分成功
                            return "Redis中的广告账户未过期，不能进行余额转移状态为全部成功或者部分成功"
                    transfer_amount = balance_transfer.transfer_amount
                    transfer_amount = Decimal(str(transfer_amount))
                    if actual_amount == transfer_amount:
                        balance_transfer.transfer_status = (
                            BalanceTransferStatus.COMPLETE_SUCCESS.desc
                        )
                    elif actual_amount < transfer_amount:
                        balance_transfer.transfer_status = (
                            BalanceTransferStatus.PARTIAL_SUCCESS.desc
                        )
                        if actual_amount == actual_refund_amount:
                            balance_transfer.remark += (
                                f"余额转入完成，实际转入总金额为{str(actual_amount)}，请知悉。"
                            )
                        else:
                            balance_transfer.remark += (
                                f"余额转入完成，实际转入总金额为{str(actual_amount)}。存在异常金额，请联系运营人员。"
                            )
                    db.commit()
                # 更新余额转移请求状态为SUCCESS,并保存实际转入金额
                db.query(BalanceTransferRequest).filter(
                    BalanceTransferRequest.trade_type
                    == TransferTradeType.RECHARGE.value,
                    BalanceTransferRequest.balance_transfer_id == balance_transfer_id,
                ).update(
                    {
                        "external_request_status": ExternalRequestStatus.FINISHED.value,
                        "actual_amount": actual_amount,
                    }
                )
                db.commit()
    except Exception as e:
        db.rollback()
        celery_log.log_error(
            f'文件位置：{file_path}，tasks.py下的beat_balance_transfer_recharge_result发生异常，异常原因：{e}')
        raise e


if __name__ == "__main__":
    # beat_balance_transfer_refund_result()
    beat_balance_transfer_recharge_result()
