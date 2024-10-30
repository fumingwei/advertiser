from decimal import Decimal
import ulid
from starlette.requests import Request
from fastapi_utils.cbv import cbv
from fastapi import APIRouter
from fastapi import Depends, Query
from sqlalchemy import func, case, or_
from sqlalchemy.orm import Session
from apps.common.utils import get_is_second, get_redis_account_group, permission_check, user_authorization_account, \
    get_open_subject_name
from libs.internal.api_service import APIService
from settings.base import configs
from settings.db import get_db, MyPagination, get_redis_connection, RedisClient
from settings.log import web_log
from tools.common import CommonQueryParams, row_list
from tools.constant import RET, OperationType, error_map, InternalRequestStatus, ExternalRequestStatus
from tools.resp import MyResponse
from libs.internal.crm_external_service import CRMExternalService
from apps.accounts.utils import get_customer_ids
from apps.advertiser.models import AdvertiserUser, UserCusRelationship, AdvertiserRegister, ProjectGroup
from apps.finance.models import WorkOrder, BalanceTransferRequest
from apps.finance.utils import (
    WorkOrderApply,
    get_medium_account_info_from_redis,
    set_expire_account_to_redis,
    get_expire_account_from_redis
)
from apps.finance.schemas import (
    BalanceTransferVerifySchema,
    BalaneTransferPostSchema,
    AddRechargeModel,
    AddResetModel,
    AccountMatchModel,
    AccountBalanceTransferSchema,
    AccountMatchModel, AccountTransferPurseSchema
)
from apps.finance.define import (
    GoogleAccountStatus,
    TiktokAccountStatus,
    BalanceTransferStatus,
    TransferTradeType,
    TransferTradeResult, TransferType,
)
from apps.finance.models import BalanceTransfer, BalanceTransferDetail
from apps.finance.tasks import balance_transfer_refund

FinanceRouter = APIRouter(tags=["财务管理"])


# 充值
@cbv(FinanceRouter)
class RechargeServer:
    request: Request

    @FinanceRouter.post("/recharges", description="提交充值")
    async def add_recharge(self, data: AddRechargeModel, db: Session = Depends(get_db)):
        try:
            apply_user_id = self.request.state.user.user_id
            data = data.dict()
            accounts = data.get("accounts", [])
            account_ids = [account.get("account_id") for account in accounts]
            user = db.query(AdvertiserUser).filter(AdvertiserUser.id == apply_user_id).first()
            is_second = get_is_second(apply_user_id)
            # 是子账户还是二代客户
            if user and user.p_id and is_second:
                no_allow_account = permission_check(apply_user_id, account_ids, OperationType.RECHARGE)
                if no_allow_account:
                    return MyResponse(code=RET.INVALID_DATA, msg=f"{no_allow_account}账户未被授权此操作类型")
            advertiser_user = (
                db.query(AdvertiserUser)
                .filter(AdvertiserUser.id == apply_user_id)
                .first()
            )
            company_id = advertiser_user.company_id if advertiser_user else None
            customer_obj = (
                db.query(UserCusRelationship)
                .filter(
                    UserCusRelationship.company_id == company_id,
                    UserCusRelationship.is_delete == False,
                )
                .first()
            )
            advertiser_customer = [
                int(customer_id)
                for customer_id in customer_obj.customer_id
                if customer_obj
            ]
            if not advertiser_customer:
                return MyResponse(code=RET.DATA_ERR, msg="您输入的广告账户尚未获得授权，请联系我们的工作人员授权!")
            data["advertiser_customer"] = advertiser_customer
            data["user_id"] = apply_user_id
            work_order_id = WorkOrderApply.generate_work_order_id(apply_user_id)
            WorkOrderApply.create_work_order(db=db,
                                             work_order_id=work_order_id,
                                             flow_code="recharge",
                                             user_id=apply_user_id,
                                             company_id=company_id)
            data["work_order_id"] = work_order_id
        except Exception as e:
            db.rollback()
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg=e.__str__())
        else:
            db.commit()
            response = CRMExternalService.post_recharge(json=data, **{'trace_id': self.request.state.trace_id})
            code = response.get("code")
            msg = response.get("msg")
        return MyResponse(code=code, msg=msg)

    @FinanceRouter.post('/account_mediums', description="根据账户匹配媒介")
    async def account_mediums(self, data: AccountMatchModel):
        data = data.dict()
        response = CRMExternalService.post_account_info(json=data, **{'trace_id': self.request.state.trace_id})
        data = response.get("data", [])
        msg = response.get("msg")
        code = response.get("code")
        return MyResponse(code=code, msg=msg, data=data)

    @FinanceRouter.get("/recharges", description="充值列表")
    async def get_recharge(
            self,
            common_query: CommonQueryParams = Depends(),
            approval_status: str = None,
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        work_order_ids = []
        advertiser_user_obj = (
            db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id).first()
        )
        if advertiser_user_obj:
            p_id = advertiser_user_obj.p_id if advertiser_user_obj.p_id else None
            company_id = advertiser_user_obj.company_id
            # 登录人是子账户
            if all([p_id, company_id]):
                work_order_ids = (
                    db.query(WorkOrder.work_order_id)
                    .filter(
                        WorkOrder.apply_user_id == user_id,
                        WorkOrder.company_id == company_id,
                        WorkOrder.flow_code == "recharge",
                        WorkOrder.is_delete == 0)
                    .all()
                )
            # 登录人是主账户
            if not p_id:
                work_order_ids = (
                    db.query(WorkOrder.work_order_id)
                    .filter(WorkOrder.company_id == company_id,
                            WorkOrder.flow_code == "recharge",
                            WorkOrder.is_delete == 0)
                    .all()
                )
            work_order_ids = [i[0] for i in work_order_ids] if work_order_ids else []
        json_ = {
            "page": common_query.page,
            "page_size": common_query.page_size,
            "q": common_query.q,
            "approval_status": approval_status,
            "work_order_ids": work_order_ids
        }
        response = CRMExternalService.post_recharge_list(json=json_, **{'trace_id': self.request.state.trace_id})
        msg = response.get("msg")
        code = response.get("code")
        data = response.get("data", [])
        total = response.get("total", 0)
        return MyResponse(code=code, msg=msg, data=data, total=total)

    @FinanceRouter.get('/recharges/{id}', description="账户充值详情")
    async def get_recharge_detail(self, id: int, common_query: CommonQueryParams = Depends(),
                                  db: Session = Depends(get_db)):
        params = {"id": id,
                  "page": common_query.page,
                  "page_size": common_query.page_size
                  }
        is_second = False
        try:
            response = CRMExternalService.get_recharge_detail(
                params=params,
                **{'trace_id': self.request.state.trace_id}
            )
            data = response.get("data", [])
            msg = response.get("msg")
            total = response.get("total")
            code = response.get("code")
            if code == RET.OK:
                user_id = self.request.state.user.user_id
                is_second = get_is_second(user_id)
                user_pid = db.query(AdvertiserUser.p_id).filter(AdvertiserUser.id == user_id,
                                                                AdvertiserUser.is_delete == 0).scalar()
                is_primary = False if user_pid else True
                # 是二代客户主账户
                if is_second and is_primary:
                    # 获取redis数据
                    pipe_account_list, group_name_mapping = get_redis_account_group(data)

                    # 处理数据
                    for item, account_groups in zip(data, pipe_account_list):
                        group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                        group_name_str = ", ".join(group_names) or '-'
                        item["group_name"] = group_name_str
                        item["recharge_num"] = round(float(item.get('recharge_num')), 2)
                # 不是二代客户
                else:
                    for recharge_dict in data:
                        recharge_dict["recharge_num"] = round(float(recharge_dict.get('recharge_num')), 2)
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg=e.__str__(), total=0, data=[],
                              other_data={"is_second": is_second})
        return MyResponse(code=code, msg=msg, total=total, data=data,
                          other_data={"is_second": True if (is_second and is_primary) else False})

    # @FinanceRouter.post("/recharges/export", description="充值导出")
    # async def recharge_export(
    #         self, data: RechargeExportModel, db: Session = Depends(get_db)
    # ):
    #     user_id = self.request.state.user.user_id
    #     work_order_ids = []
    #     advertiser_user_obj = (
    #         db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id).first()
    #     )
    #     if advertiser_user_obj:
    #         p_id = advertiser_user_obj.p_id if advertiser_user_obj.p_id else None
    #         company_id = advertiser_user_obj.company_id
    #         # 登录人是子账户
    #         if all([p_id, company_id]):
    #             work_order_ids = (
    #                 db.query(WorkOrder.work_order_id)
    #                 .filter(
    #                     WorkOrder.apply_user_id == user_id,
    #                     WorkOrder.company_id == company_id,
    #                     WorkOrder.flow_code == "recharge",
    #                     WorkOrder.is_delete == 0)
    #                 .all()
    #             )
    #         # 登录人是主账户
    #         if not p_id:
    #             work_order_ids = (
    #                 db.query(WorkOrder.work_order_id)
    #                 .filter(WorkOrder.company_id == company_id,
    #                         WorkOrder.flow_code == "recharge",
    #                         WorkOrder.is_delete == 0)
    #                 .all()
    #             )
    #         work_order_ids = [i[0] for i in work_order_ids] if work_order_ids else []
    #     json_ = {
    #         "customer_id": data.customer_id,
    #         "date_start": data.date_start,
    #         "date_end": data.date_end,
    #         "user_id": user_id,
    #         "work_order_ids": work_order_ids
    #     }
    #     # response = CRMExternalService.post_recharge_export(json=json_)
    #     common_task(asy_recharge_export, (json_,))
    #     return MyResponse(msg="充值导出成功，请前往下载中心查看。")

    @FinanceRouter.get("/customer_id_name", description="客户id、name")
    async def get_customer_id_name(
            self,
            is_all: bool = False,
            query: str = Query(None),
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False)
            .first()
        )
        customer_ids = []
        account_ids = []
        if user.p_id:
            account_ids = user_authorization_account(user_id)
        else:
            customer_obj = (
                db.query(UserCusRelationship)
                .filter(
                    UserCusRelationship.company_id == user.company_id,
                    UserCusRelationship.is_delete == False,
                )
                .first()
            )
            customer_ids = [
                customer_id for customer_id in customer_obj.customer_id if customer_obj
            ]
        json = {
            "is_all": is_all,
            "query": query,
            "customer_ids": customer_ids,
            "account_ids": list(account_ids)
        }
        response = CRMExternalService.customer_id_name(json, **{'trace_id': self.request.state.trace_id})
        data = response.get("data", [])
        msg = response.get("msg")
        code = response.get("code")
        return MyResponse(code=code, msg=msg, data=data)


#  清零
@cbv(FinanceRouter)
class ResetServer:
    request: Request

    @FinanceRouter.post('/resets', description='提交清零')
    async def add_reset(self, data: AddResetModel, db: Session = Depends(get_db)):
        try:
            user_id = self.request.state.user.user_id
            data = data.dict()
            accounts = data.get("accounts", [])
            account_ids = [account.get("account_id") for account in accounts]
            user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id).first()
            is_second = get_is_second(user_id)
            # 是子账户还是二代客户
            if user and user.p_id and is_second:
                no_allow_account = permission_check(user_id, account_ids, OperationType.RESET)
                if no_allow_account:
                    return MyResponse(code=RET.INVALID_DATA, msg=f"{no_allow_account}账户未被授权此操作类型")
            advertiser_user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id).first()
            company_id = advertiser_user.company_id if advertiser_user else None
            customer_obj = db.query(UserCusRelationship).filter(UserCusRelationship.company_id == company_id,
                                                                UserCusRelationship.is_delete == False).first()
            advertiser_customer = [int(customer_id) for customer_id in customer_obj.customer_id if customer_obj]
            if not advertiser_customer:
                return MyResponse(code=RET.DATA_ERR, msg="您输入的广告账户尚未获得授权，请联系我们的工作人员授权!")
            data['advertiser_customer'] = advertiser_customer
            data["user_id"] = user_id
            work_order_id = WorkOrderApply.generate_work_order_id(user_id)
            WorkOrderApply.create_work_order(db=db,
                                             work_order_id=work_order_id,
                                             flow_code="account_reset",
                                             user_id=user_id,
                                             company_id=company_id)
            data["work_order_id"] = work_order_id
        except Exception as e:
            db.rollback()
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg=e.__str__())
        else:
            db.commit()
            response = CRMExternalService.post_reset(json=data, **{'trace_id': self.request.state.trace_id})
            code = response.get("code")
            msg = response.get("msg")
        return MyResponse(code=code, msg=msg)

    @FinanceRouter.get('/resets', description="清零列表")
    async def get_reset(self, common_query: CommonQueryParams = Depends(), approval_status: str = None,
                        db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        work_order_ids = []
        advertiser_user_obj = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id).first()
        if advertiser_user_obj:
            p_id = advertiser_user_obj.p_id if advertiser_user_obj.p_id else None
            company_id = advertiser_user_obj.company_id
            # 登录人是子账户
            if all([p_id, company_id]):
                work_order_ids = db.query(WorkOrder.work_order_id).filter(
                    WorkOrder.apply_user_id == user_id,
                    WorkOrder.company_id == company_id,
                    WorkOrder.flow_code == 'account_reset',
                    WorkOrder.is_delete == 0).all()
            # 登录人是主账户
            if not p_id:
                work_order_ids = db.query(WorkOrder.work_order_id).filter(WorkOrder.company_id == company_id,
                                                                          WorkOrder.flow_code == 'account_reset',
                                                                          WorkOrder.is_delete == 0).all()
            work_order_ids = [i[0] for i in work_order_ids] if work_order_ids else []
        json_ = {"page": common_query.page,
                 "page_size": common_query.page_size,
                 "q": common_query.q,
                 "approval_status": approval_status,
                 "work_order_ids": work_order_ids
                 }
        response = CRMExternalService.get_reset_list(json=json_, **{'trace_id': self.request.state.trace_id})
        code = response.get('code')
        msg = response.get('msg')
        data = response.get('data', [])
        total = response.get('total', 0)
        return MyResponse(code=code, msg=msg, data=data, total=total)

    @FinanceRouter.get('/resets/{id}', description="清零详情")
    async def get_reset_detail(self, id: int, common_query: CommonQueryParams = Depends(),
                               db: Session = Depends(get_db)):
        params = {"id": id,
                  "page": common_query.page,
                  "page_size": common_query.page_size
                  }
        is_second = False
        try:
            response = CRMExternalService.get_reset_detail(params=params, **{'trace_id': self.request.state.trace_id})
            data = response.get("data", [])
            msg = response.get("msg")
            total = response.get("total")
            code = response.get("code")
            if code == RET.OK:
                user_id = self.request.state.user.user_id
                is_second = get_is_second(user_id)
                user_pid = db.query(AdvertiserUser.p_id).filter(AdvertiserUser.id == user_id,
                                                                AdvertiserUser.is_delete == 0).scalar()
                is_primary = False if user_pid else True
                # 是二代客户主账户
                if is_second and is_primary:
                    # 获取redis数据
                    pipe_account_list, group_name_mapping = get_redis_account_group(data)

                    # 重组数据
                    for item, account_groups in zip(data, pipe_account_list):
                        group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                        group_name_str = ", ".join(group_names) or '-'
                        item["group_name"] = group_name_str
                        item["refund_charge"] = round(float(item.get('refund_charge')), 2)
                # 不是二代客户
                else:
                    for reset_dict in data:
                        reset_dict["refund_charge"] = round(float(reset_dict.get('refund_charge')), 2)
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg=e.__str__(), total=0, data=[],
                              other_data={"is_second": is_second})
        return MyResponse(code=code, msg=msg, total=total, data=data,
                          other_data={"is_second": True if (is_second and is_primary) else False})

    @FinanceRouter.post('/resets/account_mediums', description="根据账户id补充其他信息")
    async def reset_account_mediums(self, data: AccountMatchModel):
        data = data.dict()
        response = CRMExternalService.post_account_info(json=data, **{'trace_id': self.request.state.trace_id})
        redis_connection = get_redis_connection("medium_account")
        data = response.get("data", [])
        for i in data:
            account_id = i.get("account_id")
            account_balance = redis_connection.hmget(f'account:{account_id.replace("-", "")}', 'available_balance')
            i["available_balance"] = account_balance[0] if account_balance[0] is not None else None
        msg = response.get("msg")
        code = response.get("code")
        return MyResponse(code=code, msg=msg, data=data)


@cbv(FinanceRouter)
class BalanceTransferServer:
    request: Request

    @FinanceRouter.get("/transfer/result", description="余额转移操作")
    async def get_operatio_result(self):
        data = [
            {
                "label": "成功",
                "en_label": "Success",
                "value": "成功"
            },
            {
                "label": "失败",
                "en_label": "Fail",
                "value": "失败"
            },
            {
                "label": "转移中",
                "en_label": "Transfer In Progress",
                "value": "转移中"
            }
        ]
        return MyResponse(data=data)

    # 账户转账户
    @FinanceRouter.post("/balance_transfer", description="提交账户转账")
    async def account_balance_transfer(self, data: AccountBalanceTransferSchema, db: Session = Depends(get_db)):
        refund_accounts = data.refund_accounts
        recharge_accounts = data.recharge_accounts
        amount = data.amount
        if refund_accounts == recharge_accounts:
            return MyResponse(code=RET.INVALID_DATA, msg='转出账户不能是转入账户')
        accounts = refund_accounts + recharge_accounts
        for i in accounts:
            expire = get_expire_account_from_redis(i)
            if expire:
                return MyResponse(code=RET.PARAM_ERR, msg=f'广告账户：{i},操作频繁')
        # 只要库中有请求是："进行中",并且下次请求中广告账户包含前几次的请求的广告账户将不能进行账户转账
        user_id = self.request.state.user.user_id
        subquery = db.query(
            BalanceTransferDetail
        ).filter(
            BalanceTransferDetail.account_id.in_(accounts)
        ).subquery()
        exist_accounts = db.query(
            BalanceTransfer.id,
            subquery.c.account_id.label('account_id')
        ).filter(
            BalanceTransfer.transfer_status == BalanceTransferStatus.PENDING.desc
        ).join(
            subquery,
            subquery.c.balance_transfer_id == BalanceTransfer.id
        )
        exist_accounts = {account_info.account_id for account_info in exist_accounts}
        if exist_accounts:
            return MyResponse(code=RET.DATA_ERR, msg=f"{','.join(exist_accounts)}广告账户在进行中,请稍后重试")
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id).first()
        is_second = get_is_second(user_id)
        if user and user.p_id and is_second:
            no_allow_account = permission_check(user_id, accounts, OperationType.BALANCETRANSFER)
            if no_allow_account:
                return MyResponse(code=RET.INVALID_DATA, msg=f"{no_allow_account}账户未被授权此操作类型")
        request_data = [{"account_id": i} for i in accounts]
        crm_res = CRMExternalService.post_account_info(
            json={"accounts": request_data},
            **{'trace_id': self.request.state.trace_id}
        )
        crm_account_data = crm_res.get("data", [])
        customer_id = crm_account_data[0]["customer_id"]
        # 判断广告账户是否属于同一个客户简称
        check_customer_same = all(
            i["customer_id"] == crm_account_data[0]["customer_id"]
            for i in crm_account_data
        )
        if not check_customer_same:
            return MyResponse(code=RET.PARAM_ERR, msg="广告账户结算名称不一致，无法进行转账")
        # 判断广告账户媒介是否支持余额转移
        check_medium_support = {i["medium"].lower() for i in crm_account_data}
        if not check_medium_support.issubset({"meta", "google", "tiktok"}):
            return MyResponse(code=RET.PARAM_ERR, msg="媒介不支持余额转移")
        # 请求Redis获取广告账户余额
        result = {"refund_accounts": [], "recharge_accounts": []}
        mediums = set()
        for i in crm_account_data:
            account_id = i["account_id"]
            medium = i["medium"]
            mediums.add(medium)
            if len(mediums) != 1:
                return MyResponse(code=RET.DATA_ERR, msg='无数据，不能被选中')
            bc_id = i["bc_id"] if i["bc_id"] else ""
            # 从Redis获取广告账户信息
            redis_account = get_medium_account_info_from_redis(account_id)
            account_status = redis_account.get("account_status")
            # 判断广告账m户的状态是否被禁
            if (
                    medium.lower() == "google"
                    and account_status in [GoogleAccountStatus.CANCELED]
            ):
                return MyResponse(
                    code=RET.PARAM_ERR,
                    msg=f"Google广告账户{account_id}状态异常，余额转移无法操作",
                )
            if (
                    medium.lower() == "tiktok"
                    and account_status != TiktokAccountStatus.STATUS_ENABLE
            ):
                return MyResponse(
                    code=RET.PARAM_ERR,
                    msg=f"Tiktok广告账户{account_id}状态异常，余额转移无法操作"
                )
            if medium.lower() in ["meta", "google"]:
                # Meta Google账户需要预留出来0.01
                spend_cap = redis_account.get("spend_cap")
                amount_spent = redis_account.get("amount_spent")
                if not spend_cap or not amount_spent:
                    return MyResponse(code=RET.DATA_ERR, msg="花费数据状态异常")
                if medium.lower() == "google" and str(spend_cap) == "0.00":
                    # Google新申请的广告账户预算可以为0.00
                    available_balance = str(spend_cap)
                elif medium.lower() == "meta" and str(spend_cap) == str(amount_spent):
                    # Meta 广告账户预算等于消耗金额时，可用余额为0.00
                    available_balance = str(0.00)
                else:
                    """meta的redis数据available_balance，需要减去0.01，才是账户可以用的钱"""
                    available_balance = str(
                        (
                                Decimal(spend_cap) - Decimal(amount_spent)
                        ).quantize(Decimal("0.00"))
                    )
                    current_balance = float(available_balance) - 0.01
                    if (current_balance <= 0 or current_balance < float(amount)) and account_id in refund_accounts:
                        return MyResponse(RET.PARAM_ERR,
                                          msg=f'广告账户{account_id}转出金额大于账户余额，无法进行账户转账')
            elif medium.lower() in ["tiktok"]:
                available_balance = redis_account.get("available_balance")
                if not available_balance:
                    return MyResponse(code=RET.DATA_ERR, msg="账户可用余额状态异常")
                if float(amount) < 10.0:
                    return MyResponse(code=RET.DATA_ERR, msg=f"tiktok{account_id}最小交易金额为10.0")
            else:
                return MyResponse(code=RET.PARAM_ERR, msg="媒介错误")
            if account_id in refund_accounts:
                if float(available_balance) == 0:
                    return MyResponse(
                        code=RET.PARAM_ERR,
                        msg=f"广告账户{account_id}可用余额为0，无法进行账户转账",
                    )
                if float(available_balance) < float(amount):
                    return MyResponse(code=RET.REQ_ERR,
                                      msg=f'广告账户{account_id}，转出金额大于账户余额，无法进行账户转账')
                result["refund_accounts"].append(
                    {
                        "account_id": account_id,
                        "medium": medium,
                        "available_balance": available_balance,
                        "bc_id": bc_id,
                        "amount": amount
                    }
                )
            elif account_id in recharge_accounts:
                result["recharge_accounts"].append(
                    {
                        "account_id": account_id,
                        "medium": medium,
                        "available_balance": available_balance,
                        "bc_id": bc_id,
                        "amount": amount
                    }
                )
            else:
                return MyResponse(code=RET.PARAM_ERR, msg="广告账户错误")
        balance_transfer_obj = BalanceTransfer(
            user_id=user_id,
            customer_id=customer_id,
            transfer_amount=amount,
            transfer_status=BalanceTransferStatus.PENDING.desc,
            medium=''.join(check_medium_support)
        )
        db.add(balance_transfer_obj)
        db.flush()
        balance_transfer_id = balance_transfer_obj.id
        for index, account_info in enumerate(result["refund_accounts"]):
            db.add(
                BalanceTransferDetail(
                    balance_transfer_id=balance_transfer_id,
                    account_id=account_info['account_id'],
                    bc_id=account_info['bc_id'],
                    medium=account_info['medium'],
                    before_balance=account_info['available_balance'],
                    after_balance=account_info['available_balance'],
                    amount=account_info['amount'],
                    trade_type=TransferTradeType.REFUND.value,
                    trade_result=TransferTradeResult.EMPTY.value,
                    order_num=index,
                )
            )
        for index, account_info in enumerate(result['recharge_accounts']):
            db.add(
                BalanceTransferDetail(
                    balance_transfer_id=balance_transfer_id,
                    account_id=account_info['account_id'],
                    bc_id=account_info['bc_id'],
                    medium=account_info['medium'],
                    before_balance=account_info['available_balance'],
                    after_balance=account_info['available_balance'],
                    amount=account_info['amount'],
                    trade_type=TransferTradeType.RECHARGE.value,
                    trade_result=TransferTradeResult.EMPTY.value,
                    order_num=index,
                )
            )
        try:
            db.commit()
            # 本次操作所有账户10分钟后才能下次操作
            for i in accounts:
                set_expire_account_to_redis(i)
            balance_transfer_refund(balance_transfer_id)
        except Exception as e:
            db.rollback()
            web_log.log_error(f'账户转账失败，原因：{e.__str__()}')
            return MyResponse(code=RET.UNKNOW_ERR, msg='未知错误')
        return MyResponse(msg='已成功提交账户转账请求，请稍后查看结果。')

    # 账户转钱包
    @FinanceRouter.post("/add_account_transfer_purse", description="创建账户转钱包工单")
    async def add_account_transfer_purse(
            self, data: AccountTransferPurseSchema, db: Session = Depends(get_db)
    ):
        account_id = data.account_id
        transfer_amount = data.transfer_amount
        subquery = db.query(
            BalanceTransferDetail
        ).filter(
            BalanceTransferDetail.account_id == account_id
        ).subquery()
        exist_accounts = db.query(
            BalanceTransfer.id,
            subquery.c.account_id.label('account_id')
        ).filter(
            BalanceTransfer.transfer_status == BalanceTransferStatus.PENDING.desc
        ).join(
            subquery,
            subquery.c.balance_transfer_id == BalanceTransfer.id
        )
        exist_accounts = {account_info.account_id for account_info in exist_accounts}
        if exist_accounts:
            return MyResponse(code=RET.DATA_ERR, msg=f"{','.join(exist_accounts)}广告账户在进行中,请稍后重试")
        user_id = self.request.state.user.user_id
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id).first()
        is_second = get_is_second(user_id)
        if user and user.p_id and is_second:
            no_allow_account = permission_check(user_id, [data.account_id], OperationType.BALANCETRANSFER)
            if no_allow_account:
                return MyResponse(code=RET.INVALID_DATA, msg=f"{no_allow_account}账户未被授权此操作类型")
        try:
            request_data = [
                {"account_id": account_id}
            ]
            crm_res = CRMExternalService.post_account_info(
                json={"accounts": request_data},
                **{'trace_id': self.request.state.trace_id}
            )
            if crm_res["code"] != 0:
                web_log.log_error(f"账户转钱包失败{crm_res}")
                return MyResponse(RET.UNKNOW_ERR, "网络失败")
            customer_id = crm_res["data"][0]["customer_id"]
            medium = crm_res["data"][0]["medium"]
            bc_id = crm_res["data"][0].get("bc_id") if crm_res["data"][0].get("bc_id") else ""

            # 从Redis获取广告账户信息
            redis_account = get_medium_account_info_from_redis(account_id)
            account_status = redis_account.get("account_status")
            # 判断广告账户的状态是否被禁
            if (
                    medium.lower() == "google"
                    and account_status in [GoogleAccountStatus.CANCELED]
            ):
                return MyResponse(
                    code=RET.PARAM_ERR,
                    msg=f"Google广告账户{account_id}状态异常，无法进行账户转账",
                )
            if (
                    medium.lower() == "tiktok"
                    and account_status != TiktokAccountStatus.STATUS_ENABLE
            ):
                return MyResponse(
                    code=RET.PARAM_ERR,
                    msg=f"Tiktok广告账户{account_id}状态异常，无法进行账户转账"
                )
            if medium.lower() in ["meta", "google"]:
                # Meta Google账户需要预留出来0.01
                spend_cap = redis_account.get("spend_cap")
                amount_spent = redis_account.get("amount_spent")
                if not spend_cap or not amount_spent:
                    return MyResponse(code=RET.DATA_ERR, msg="花费数据状态异常")
                if medium.lower() == "google" and str(spend_cap) == "0.00":
                    # Google新申请的广告账户预算可以为0.00
                    available_balance = str(spend_cap)
                elif medium.lower() == "meta" and str(spend_cap) == str(amount_spent):
                    # Meta 广告账户预算等于消耗金额时，可用余额为0.00
                    available_balance = str(0.00)
                else:
                    available_balance = str(
                        (
                                Decimal(spend_cap) - Decimal(amount_spent) - Decimal("0.01")
                        ).quantize(Decimal("0.00"))
                    )
            elif medium.lower() in ["tiktok"]:
                available_balance = redis_account.get("available_balance")
                if not available_balance:
                    return MyResponse(code=RET.DATA_ERR, msg="账户可用余额状态异常")
                if float(transfer_amount) < 10.0:
                    return MyResponse(code=RET.DATA_ERR, msg=f"tiktok{account_id}最小交易金额为10.0")
            else:
                return MyResponse(code=RET.PARAM_ERR, msg="账号所属媒介错误")
            if float(available_balance) < float(transfer_amount):
                return MyResponse(code=RET.REQ_ERR, msg=f'广告账户{account_id}，转出金额大于账户余额，无法进行账户转账')
            if float(available_balance) == 0:
                return MyResponse(
                    code=RET.PARAM_ERR,
                    msg=f"广告账户{account_id}可用余额为0，无法进行余额转移",
                )

            balance_transfer = BalanceTransfer(
                customer_id=customer_id,
                user_id=self.request.state.user.user_id,
                transfer_amount=float(transfer_amount),
                medium=medium
            )
            db.add(balance_transfer)
            db.flush()
            balance_transfer_request = BalanceTransferRequest(
                balance_transfer_id=balance_transfer.id,
                internal_request_status=InternalRequestStatus.REQUEST_REDAY.value,
                transfer_type=TransferType.PURSE.value,
                actual_amount=float(transfer_amount),
                trade_type=TransferTradeType.REFUND.value,
            )
            db.add(balance_transfer_request)
            db.flush()
            refund_data = [
                {
                    "account_id": account_id,
                    "recharge_num": -float(transfer_amount),  # 负值
                    "medium": medium,
                    "bc_id": bc_id,
                }
            ]
            json_ = {"recharge_data": refund_data}
            params = {
                "api_key": configs.MAPI_KEY
            }
            res_result = APIService.recharge(json=json_, params=params, **{'trace_id': str(ulid.new())})
            res_code = res_result.get("code")
            if res_code != 0:
                web_log.log_error(f"提交账户退款失败{res_result}")
                return MyResponse(RET.UNKNOW_ERR, "网络失败")
            request_id = res_result["data"]["request_id"]
            balance_transfer_request.external_request_id = request_id
            balance_transfer_request.external_request_status = ExternalRequestStatus.RECEIVED.value
            balance_transfer_request.internal_request_status = InternalRequestStatus.REQUEST_SUCCESS.value
            balance_transfer_detail = BalanceTransferDetail(
                balance_transfer_id=balance_transfer.id,
                balance_transfer_request_id=balance_transfer_request.external_request_id,
                account_id=account_id,
                medium=medium,
                bc_id=bc_id,
                amount=float(transfer_amount),
                transfer_type=TransferType.PURSE.value,
                trade_type=TransferTradeType.REFUND.value,
                trade_result=TransferTradeResult.DEFAULT.value,
                order_num=0
            )
            db.add(balance_transfer_detail)
            balance_transfer_detail = BalanceTransferDetail(
                balance_transfer_id=balance_transfer.id,
                balance_transfer_request_id=balance_transfer_request.external_request_id,
                account_id=account_id,
                medium=medium,
                bc_id=bc_id,
                amount=float(transfer_amount),
                transfer_type=TransferType.PURSE.value,
                trade_type=TransferTradeType.RECHARGE.value,
                trade_result=TransferTradeResult.DEFAULT.value,
                order_num=0
            )
            db.add(balance_transfer_detail)
        except Exception as e:
            db.rollback()
            web_log.log_error(f"账户转钱包失败{e.__str__()}")
            return MyResponse(RET.UNKNOW_ERR, "网络失败")
        else:
            db.commit()
        return MyResponse()

    @FinanceRouter.get("/balance_transfer", description="余额转移列表")
    async def get_balance_trnasfer(
            self,
            common_query: CommonQueryParams = Depends(),
            release_media: str = None,
            operation_result: str = None,
            start_date: str = Query(None, regex=r"^(''|\d{4}-\d{2}-\d{2})$"),
            end_date: str = Query(None, regex=r"^(''|\d{4}-\d{2}-\d{2})$"),
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        sub_user_ids = self.request.state.user.sub_user_ids
        customer_ids = get_customer_ids(db, user_id)
        operation_result_mapping = {
            "成功": 1,
            "失败": 2,
            "转移中": "Empty"
        }
        filter_list = []
        if sub_user_ids:
            # 如果有子账户
            sub_user_ids.append(user_id)
            filter_list.append(BalanceTransfer.user_id.in_(sub_user_ids))
        else:
            filter_list.append(BalanceTransfer.user_id == user_id)
        json = {
            "customer_ids": customer_ids,
        }
        result = CRMExternalService.customer_id_name(json, **{'trace_id': self.request.state.trace_id})
        customer_dict = {i["id"]: i["name"] for i in result["data"]}
        if common_query.q:
            json = {
                "customer_ids": customer_ids,
                "query": common_query.q
            }
            result = CRMExternalService.customer_id_name(json, **{'trace_id': self.request.state.trace_id})
            query_customer_ids = [i["id"] for i in result["data"]]
            filter_list.append(
                or_(
                    AdvertiserUser.real_name.like(f"%{common_query.q}%"),
                    BalanceTransfer.customer_id.in_(query_customer_ids),
                )
            )
        if all([start_date, end_date]):
            filter_list.extend([
                func.date(BalanceTransfer.created_time) <= func.date(end_date),
                func.date(BalanceTransfer.created_time) >= func.date(start_date)
            ]
            )
        subquery = db.query(
            BalanceTransferDetail.balance_transfer_id,
            func.group_concat(BalanceTransferDetail.account_id).label('accounts_id'),
            func.group_concat(func.distinct(BalanceTransferDetail.medium)).label("mediums"),
        ).filter(
            BalanceTransferDetail.is_delete == False
        ).group_by(
            BalanceTransferDetail.balance_transfer_id
        ).subquery()
        if release_media:
            filter_list.append(subquery.c.mediums.ilike(f"%{release_media}%"))
        if operation_result in operation_result_mapping:
            mapped_result = operation_result_mapping[operation_result]

            matching_transfer_ids = (
                db.query(BalanceTransferDetail.balance_transfer_id)
                .filter(
                    BalanceTransferDetail.trade_result == mapped_result,
                    BalanceTransferDetail.is_delete == False
                ).all()
            )
            ids = [i[0] for i in matching_transfer_ids]
            filter_list.append(BalanceTransfer.id.in_(ids))
        query = (
            db.query(
                BalanceTransfer.id,
                BalanceTransfer.customer_id,
                BalanceTransfer.transfer_amount,
                BalanceTransfer.created_time,
                BalanceTransfer.transfer_status.label('cn_approval_status'),
                BalanceTransfer.remark,
                # BalanceTransfer.medium.label('medium_set_'),
                AdvertiserUser.real_name.label('apply_user'),
                subquery.c.accounts_id,
                subquery.c.mediums.label('medium_set')
            )
            .filter(*filter_list)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == BalanceTransfer.user_id)
            .outerjoin(subquery, subquery.c.balance_transfer_id == BalanceTransfer.id)
            .order_by(BalanceTransfer.created_time.desc())
        )
        paginator = MyPagination(query, common_query.page, common_query.page_size)
        # 遍历pageinator.data, 添加customer_name,open_subject_name字段
        data = paginator.data
        redis_cli = RedisClient(db=6).get_redis_client()
        redis_pipe = redis_cli.pipeline()
        for item in data:
            item["customer_name"] = customer_dict.get(item.pop("customer_id", ''))
            item['open_subject_names'] = get_open_subject_name(redis_pipe, f"{item['accounts_id']}".split(','))
        return MyResponse(total=paginator.counts, data=data)

    @FinanceRouter.get("/balance_transfer/{id}", description="账户转账详情")
    async def get_balance_transfer_detail(self, id: int, db: Session = Depends(get_db)):
        balance_transfer = (
            db.query(BalanceTransfer).filter(BalanceTransfer.id == id).first()
        )
        if not balance_transfer:
            return MyResponse(code=RET.PARAM_ERR, msg="参数错误")
        refund_accounts = (
            db.query(
                BalanceTransferDetail.id,
                BalanceTransferDetail.balance_transfer_id,
                BalanceTransferDetail.account_id,
                BalanceTransferDetail.medium,
                BalanceTransferDetail.before_balance,
                BalanceTransferDetail.after_balance,
                func.abs(BalanceTransferDetail.amount).label("amount"),
                case(
                    [
                        (
                            BalanceTransferDetail.trade_result
                            == TransferTradeResult.EMPTY.value,
                            TransferTradeResult.DEFAULT.desc,
                        ),
                        (
                            BalanceTransferDetail.trade_result
                            == TransferTradeResult.DEFAULT.value,
                            TransferTradeResult.DEFAULT.desc,
                        ),
                        (
                            BalanceTransferDetail.trade_result
                            == TransferTradeResult.SUCCESS.value,
                            TransferTradeResult.SUCCESS.desc,
                        ),
                        (
                            BalanceTransferDetail.trade_result
                            == TransferTradeResult.FAILURE.value,
                            TransferTradeResult.FAILURE.desc,
                        ),
                    ],
                    else_="",
                ).label("trade_result"),
                BalanceTransferDetail.remark,
                BalanceTransferDetail.order_num,
                BalanceTransferDetail.trade_type,
                BalanceTransferDetail.before_purse_balance,
                BalanceTransferDetail.after_purse_balance,
                case([
                    (BalanceTransferDetail.transfer_type == TransferType.ACCOUNT.value, TransferType.ACCOUNT.desc)
                ], else_=TransferType.PURSE.desc).label('cn_transfer_type')

            )
            .filter(
                BalanceTransferDetail.balance_transfer_id == id,
                BalanceTransferDetail.trade_type == TransferTradeType.REFUND.value,
            )
            .order_by(BalanceTransferDetail.medium.asc(), BalanceTransferDetail.order_num.asc())
            .all()
        )
        recharge_accounts = (
            db.query(
                BalanceTransferDetail.id,
                BalanceTransferDetail.balance_transfer_id,
                BalanceTransferDetail.account_id,
                BalanceTransferDetail.medium,
                BalanceTransferDetail.before_balance,
                BalanceTransferDetail.after_balance,
                BalanceTransferDetail.amount,
                case(
                    [
                        (
                            BalanceTransferDetail.trade_result
                            == TransferTradeResult.EMPTY.value,
                            TransferTradeResult.DEFAULT.desc,
                        ),
                        (
                            BalanceTransferDetail.trade_result
                            == TransferTradeResult.DEFAULT.value,
                            TransferTradeResult.DEFAULT.desc,
                        ),
                        (
                            BalanceTransferDetail.trade_result
                            == TransferTradeResult.SUCCESS.value,
                            TransferTradeResult.SUCCESS.desc,
                        ),
                        (
                            BalanceTransferDetail.trade_result
                            == TransferTradeResult.FAILURE.value,
                            TransferTradeResult.FAILURE.desc,
                        ),
                    ],
                    else_="",
                ).label("trade_result"),
                BalanceTransferDetail.remark,
                BalanceTransferDetail.order_num,
                BalanceTransferDetail.trade_type,
                BalanceTransferDetail.before_purse_balance,
                BalanceTransferDetail.after_purse_balance,
                case([
                    (BalanceTransferDetail.transfer_type == TransferType.ACCOUNT.value, TransferType.ACCOUNT.desc)
                ], else_=TransferType.PURSE.desc).label('cn_transfer_type')
            )
            .filter(
                BalanceTransferDetail.balance_transfer_id == id,
                BalanceTransferDetail.trade_type == TransferTradeType.RECHARGE.value,
            )
            .order_by(BalanceTransferDetail.medium.asc(), BalanceTransferDetail.order_num.asc())
            .all()
        )

        user_id = self.request.state.user.user_id
        is_second = get_is_second(user_id)

        user_pid = db.query(AdvertiserUser.p_id).filter(AdvertiserUser.id == user_id,
                                                        AdvertiserUser.is_delete == 0).scalar()
        is_primary = False if user_pid else True
        # 是二代客户主账户
        if is_second and is_primary:
            # 转入账户
            recharge_data = row_list(recharge_accounts)
            # 获取redis数据
            pipe_account_list, group_name_mapping = get_redis_account_group(recharge_data)
            # 重组数据
            for item, account_groups in zip(recharge_data, pipe_account_list):
                group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                group_name_str = ", ".join(group_names) or '-'
                item['group_name'] = group_name_str

            # 转出数据
            refund_data = row_list(refund_accounts)
            # 获取redis数据
            pipe_account_list2, group_name_mapping2 = get_redis_account_group(refund_data)

            # 重组数据
            for item, account_groups in zip(refund_data, pipe_account_list2):
                group_names2 = [group_name_mapping2.get(group_id, '') for group_id in (account_groups or [])]
                group_name_str2 = ", ".join(group_names2) or '-'
                item['group_name'] = group_name_str2
            data = {
                "refund_accounts": refund_data,
                "recharge_accounts": recharge_data
            }
        # 不是二代客户
        else:
            data = {
                "refund_accounts": [i._asdict() for i in refund_accounts],
                "recharge_accounts": [i._asdict() for i in recharge_accounts],
            }
        return MyResponse(data=data, other_data={"is_second": True if (is_second and is_primary) else False})
