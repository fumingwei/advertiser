from libs.internal.crm_external_service import CRMExternalService
from apps.finance.models import WorkOrder
from apps.finance.models import BalanceTransfer, BalanceTransferDetail
from settings.db import MyPagination
from sqlalchemy import or_, func, case, desc
from apps.common.utils import get_is_second, get_redis_account_group, get_is_primary
from apps.accounts.models import (
    AccountRename,
    BmAccount,
    BmAccountDetail,
    BcAccount,
    BcAccountDetail,
)
from apps.operation.define import gop
from apps.accounts.define import (
    BMGrantType,
    BCGrantType,
    AdvertiserStatusResult,
    OperateResult,
)
from tools.constant import Operation
from apps.pixel.models import PixelAccount, PixelAccountDetail
from settings.log import web_log
from tools.constant import RET, error_map
from tools.common import row_list, row_dict
from tools.resp import MyResponse
from apps.finance.define import (
    TransferTradeType,
    TransferTradeResult, TransferType,
)
from apps.onboarding.models import OeOpenAccount
from apps.onboarding.define import OeApproveStatus
from apps.advertiser.models import (
    AdvertiserUser,
    AdvertiserRegister
)
from apps.operation.define import OperationType
from settings.db import RedisClient


class OperationListUtils:
    @staticmethod
    def recharges_list(
            db,
            user_id,
            params,
            start_date,
            end_date,
            medium,
            operation_result,
            trace_id=None
    ):
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
                    )
                    .all()
                )
            # 登录人是主账户
            if not p_id:
                work_order_ids = (
                    db.query(WorkOrder.work_order_id)
                    .filter(WorkOrder.company_id == company_id)
                    .all()
                )
            work_order_ids = [i[0] for i in work_order_ids] if work_order_ids else []
        json_ = {
            "page": params.page,
            "page_size": params.page_size,
            "result": operation_result,
            "work_order_ids": work_order_ids,
            "start_date": start_date,
            "end_date": end_date,
            "medium": medium,
            "q": params.q
        }
        response = CRMExternalService.post_recharge_list(json=json_, **{'trace_id': trace_id})
        data = response.get("data", [])
        total = response.get("total", 0)

        return data, total

    @staticmethod
    def resets_list(
            db,
            user_id,
            params,
            start_date,
            end_date,
            medium,
            operation_result,
            trace_id=None
    ):
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
                    WorkOrder.flow_code == 'account_reset').all()
            # 登录人是主账户
            if not p_id:
                work_order_ids = db.query(WorkOrder.work_order_id).filter(WorkOrder.company_id == company_id,
                                                                          WorkOrder.flow_code == 'account_reset').all()
            work_order_ids = [i[0] for i in work_order_ids] if work_order_ids else []
        json_ = {
            "page": params.page,
            "page_size": params.page_size,
            "result": operation_result,
            "work_order_ids": work_order_ids,
            "start_date": start_date,
            "end_date": end_date,
            "medium": medium,
            "q": params.q
        }
        response = CRMExternalService.get_reset_list(json=json_, **{'trace_id': trace_id})
        data = response.get('data', [])
        total = response.get('total', 0)
        return data, total

    @staticmethod
    def balance_transfer(
            db,
            user_id,
            sub_user_ids,
            customer_ids,
            params,
            start_date,
            end_date,
            medium,
            operation_result,
            trace_id=None
    ):
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
        result = CRMExternalService.customer_id_name(json, **{'trace_id': trace_id})
        customer_dict = {i["id"]: i["name"] for i in result["data"]}
        # 查询关联的 BalanceTransferDetail 获取去重后的 medium
        medium_subquery = (
            db.query(
                BalanceTransferDetail.balance_transfer_id,
                func.group_concat(func.distinct(BalanceTransferDetail.medium)).label("mediums"),
            )
            .filter(
                BalanceTransferDetail.is_delete == False,
            )
            .group_by(BalanceTransferDetail.balance_transfer_id)
            .subquery()
        )
        if medium:
            filter_list.append(medium_subquery.c.mediums.ilike(f"%{medium}%"))
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

        if params.q:
            json = {
                "customer_ids": customer_ids,
                "query": params.q
            }
            result = CRMExternalService.customer_id_name(json, **{'trace_id': trace_id})
            query_customer_ids = [i["id"] for i in result["data"]]
            balance_ids = db.query(
                BalanceTransferDetail.balance_transfer_id
            ).filter(BalanceTransferDetail.account_id.ilike(f'%{params.q}%'))
            filter_list.append(
                or_(
                    AdvertiserUser.real_name.like(f"%{params.q}%"),
                    BalanceTransfer.customer_id.in_(query_customer_ids),
                    BalanceTransfer.id.in_(balance_ids),
                )
            )
        if all([start_date, end_date]):
            filter_list.extend([
                func.date(BalanceTransfer.created_time) >= func.date(start_date),
                func.date(BalanceTransfer.created_time) <= func.date(end_date)
            ])
        query = (
            db.query(
                BalanceTransfer.id,
                BalanceTransfer.customer_id,
                BalanceTransfer.transfer_amount,
                BalanceTransfer.created_time,
                BalanceTransfer.transfer_status.label("cn_approval_status"),
                BalanceTransfer.remark,
                AdvertiserUser.real_name.label("apply_user"),
                medium_subquery.c.mediums.label("mediums"),  # 添加 medium 字段
            )
            .filter(*filter_list)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == BalanceTransfer.user_id)
            .outerjoin(medium_subquery, BalanceTransfer.id == medium_subquery.c.balance_transfer_id)  # 关联 medium 子查询
            .order_by(BalanceTransfer.created_time.desc())
        )

        paginator = MyPagination(query, params.page, params.page_size)

        # 遍历 paginator.data, 添加 customer_name 和 medium 字段
        data = paginator.data
        for item in data:
            mediums = item.pop("mediums")
            item["customer_name"] = customer_dict.get(item.pop("customer_id", '-'))
            item["medium_set"] = mediums if mediums else '-'  # 添加 medium 字段
            item["operate_type"] = "账户转账"
        return data, paginator.counts

    @staticmethod
    def accounts_rename(
            db,
            user_id,
            params,
            start_date,
            end_date,
            operation_result,
            release_media,
    ):
        redis_client = RedisClient(db=6).get_redis_client()
        where = [AccountRename.is_delete == False]
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False)
            .first()
        )
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
            )
            sub_ids = [i.id for i in sub_user.all()]
            sub_ids.append(user.id)
            where.append(AccountRename.user_id.in_(sub_ids))
        # 如果是子账号
        else:
            where.append(AccountRename.user_id == user.id)
        if release_media:
            where.append(AccountRename.medium == release_media)
        if params.q:
            # 模糊搜索能搜索广告账户ID、修改前名称、修改后名称和提交人
            user_ids = db.query(AdvertiserUser.id).filter(AdvertiserUser.real_name.like(f'%{params.q}%')).all()
            user_ids_list = [row.id for row in user_ids]
            where.append(
                or_(
                    AccountRename.before_account_name.ilike(f"%{params.q}%"),
                    AccountRename.after_account_name.ilike(f"%{params.q}%"),
                    AccountRename.account_id.ilike(f"%{params.q}%"),
                    AccountRename.user_id.in_(user_ids_list),
                )
            )
        if operation_result:
            where.append(AccountRename.operate_result == operation_result)

        if all([start_date, end_date]):
            where.extend([
                func.date(AccountRename.created_time) >= func.date(start_date),
                func.date(AccountRename.created_time) <= func.date(end_date)
            ])
        query = (
            db.query(
                AccountRename,
                AdvertiserUser.real_name.label('apply_user'),
                case(
                    [
                        (
                            AccountRename.operate_result == OperateResult.SUCCESS.value,
                            OperateResult.SUCCESS.desc,
                        ),
                        (
                            AccountRename.operate_result == OperateResult.DEFAULT.value,
                            OperateResult.DEFAULT.desc,
                        ),
                        (
                            AccountRename.operate_result == OperateResult.FAIL.value,
                            OperateResult.FAIL.desc,
                        ),
                    ],
                    else_="",
                ).label("cn_approval_status"),
            )
            .filter(*where)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == AccountRename.user_id)
            .order_by(desc(AccountRename.id))
        )
        obj = MyPagination(query, params.page, params.page_size)
        data = obj.data
        for item in data:
            account_id = item.pop("account_id")
            open_subject_name = redis_client.hget(f"account:{account_id}", "open_subject_name")
            item["open_subject_name"] = open_subject_name if open_subject_name else "-"
            item["medium_set"] = item.pop("medium", "-")
        return data, obj.counts

    @staticmethod
    def bc_accounts(
            db,
            user_id,
            params,
            start_date,
            end_date,
            operation_result,
            release_media
    ):
        redis_client = RedisClient(db=6).get_redis_client()
        if release_media and release_media != "Tiktok":
            return [], 0
        where = [BcAccount.is_delete == False]
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False)
            .first()
        )
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
            )
            sub_names = [i.id for i in sub_user.all()]
            sub_names.append(user.id)
            where.append(BcAccount.user_id.in_(sub_names))
        # 如果是子账号
        else:
            where.append(BcAccount.user_id == user.id)
        subquery = (
            db.query(
                BcAccountDetail.tiktok_bc_account_id,
                func.count(BcAccountDetail.account_id).label("detail_count"),
                func.group_concat(BcAccountDetail.account_id).label("account_ids"),
                func.group_concat(BcAccountDetail.business_id).label("business_ids"),
            )
            .filter(BcAccountDetail.is_delete == False)
            .group_by(BcAccountDetail.tiktok_bc_account_id)
            .subquery()
        )
        if operation_result:
            matching_transfer_ids = (
                db.query(BcAccountDetail.tiktok_bc_account_id)
                .filter(
                    BcAccountDetail.operate_result == operation_result,
                    BcAccountDetail.is_delete == False
                ).all()
            )
            ids = [i[0] for i in matching_transfer_ids]
            where.append(BcAccount.id.in_(ids))
        if params.q:
            # 可以搜索Pixel_ID、提交人、account_id
            user_ids = db.query(AdvertiserUser.id).filter(AdvertiserUser.real_name.like(f'%{params.q}%')).all()
            user_ids_list = [row.id for row in user_ids]
            where.append(
                or_(
                    BcAccount.cooperative_id.like(f"%{params.q}%"),
                    BcAccount.user_id.in_(user_ids_list),
                    subquery.c.account_ids.like(f"%{params.q}%"),
                    subquery.c.business_ids.like(f"%{params.q}%"),
                    BcAccount.operate_result == operation_result
                )
            )
        if all([start_date, end_date]):
            where.extend([
                func.date(BcAccount.created_time) >= func.date(start_date),
                func.date(BcAccount.created_time) <= func.date(end_date)
            ])
        query = (
            db.query(
                BcAccount,
                AdvertiserUser.real_name.label('apply_user'),
                subquery.c.detail_count,
                subquery.c.account_ids,
                case(
                    [
                        (
                            BcAccount.operate_result
                            == AdvertiserStatusResult.DEFAULT.value,
                            AdvertiserStatusResult.DEFAULT.desc,
                        ),
                        (
                            BcAccount.operate_result
                            == AdvertiserStatusResult.PART.value,
                            AdvertiserStatusResult.PART.desc,
                        ),
                        (
                            BcAccount.operate_result
                            == AdvertiserStatusResult.ALL_SUCCEED.value,
                            AdvertiserStatusResult.ALL_SUCCEED.desc,
                        ),
                        (
                            BcAccount.operate_result
                            == AdvertiserStatusResult.ALL_FAIL.value,
                            AdvertiserStatusResult.ALL_FAIL.desc,
                        ),
                    ],
                    else_="",
                ).label("cn_approval_status"),
                case(
                    [
                        (
                            BcAccount.operate_type == Operation.BIND.value,
                            Operation.BIND.desc,
                        ),
                        (
                            BcAccount.operate_type == Operation.UNBIND.value,
                            Operation.UNBIND.desc,
                        ),
                    ],
                    else_="",
                ).label("cn_operate_type"),
                case(
                    [
                        (
                            BcAccount.grant_type == BCGrantType.ANALYST.value,
                            BCGrantType.ANALYST.desc,
                        ),
                        (
                            BcAccount.grant_type == BCGrantType.OPERATOR.value,
                            BCGrantType.OPERATOR.desc,
                        ),
                    ],
                    else_="-",
                ).label("cn_grant_type"),
                subquery.c.account_ids.label("account_ids"),
            )
            .outerjoin(subquery, BcAccount.id == subquery.c.tiktok_bc_account_id)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == BcAccount.user_id)
            .filter(*where)
            .order_by(desc(BcAccount.id))
        )
        obj = MyPagination(query, params.page, params.page_size)
        data = obj.data

        # 处理返回数据
        for item in data:
            account_ids = list(item.pop("account_ids").split(","))
            # 使用 Redis 管道
            pipeline = redis_client.pipeline()
            # 向管道中添加获取 open_subject_name 的命令
            for account_id in account_ids:
                pipeline.hget(f"account:{account_id}", "open_subject_name")

            # 执行管道，获取所有结果
            results = pipeline.execute()
            # 去重 open_subject_name
            open_subject_names = set()
            for result in results:
                if result:  # 如果存在值
                    open_subject_names.add(result)  # 转换为字符串并添加到集合

            # 将去重后的结果以逗号分隔形式拼接成字符串
            item["open_subject_name"] = ', '.join(open_subject_names) if open_subject_names else "-"
            item["operate_type"] = "BC绑定/解绑"
            item["medium_set"] = "Tiktok"

        return data, obj.counts

    @staticmethod
    def bm_accounts(
            user_id,
            params,
            start_date,
            end_date,
            db,
            operation_result,
            release_media
    ):
        redis_client = RedisClient(db=6).get_redis_client()
        if release_media and release_media != "Meta":
            return [], 0
        where = [BmAccount.is_delete == False]
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False)
            .first()
        )
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
            )
            sub_names = [i.id for i in sub_user.all()]
            sub_names.append(user.id)
            where.append(BmAccount.user_id.in_(sub_names))
        # 如果是子账号
        else:
            where.append(BmAccount.user_id == user.id)
        subquery = (
            db.query(
                BmAccountDetail.bm_account_id,
                func.count(BmAccountDetail.account_id).label("detail_count"),
                func.group_concat(BmAccountDetail.account_id).label("account_ids"),
            )
            .filter(BmAccountDetail.is_delete == False)
            .group_by(BmAccountDetail.bm_account_id)
            .subquery()
        )

        if params.q:
            # 搜索商业账户ID、广告账户id和提交人
            user_ids = db.query(AdvertiserUser.id).filter(AdvertiserUser.real_name.like(f'%{params.q}%')).all()
            user_ids_list = [row.id for row in user_ids]
            where.append(
                or_(
                    BmAccount.business_id.like(f"%{params.q}%"),
                    BmAccount.user_id.in_(user_ids_list),
                    subquery.c.account_ids.like(f"%{params.q}%"),
                )
            )

        if operation_result:
            matching_transfer_ids = (
                db.query(BmAccountDetail.bm_account_id)
                .filter(
                    BmAccountDetail.operate_result == operation_result,
                    BmAccountDetail.is_delete == False
                ).all()
            )
            ids = [i[0] for i in matching_transfer_ids]
            where.append(BmAccount.id.in_(ids))
        if all([start_date, end_date]):
            where.extend([
                func.date(BmAccount.created_time) >= func.date(start_date),
                func.date(BmAccount.created_time) <= func.date(end_date)
            ])
        query = (
            db.query(
                BmAccount,
                AdvertiserUser.real_name.label('apply_user'),
                subquery.c.detail_count,
                case(
                    [
                        (
                            BmAccount.operate_result
                            == AdvertiserStatusResult.DEFAULT.value,
                            AdvertiserStatusResult.DEFAULT.desc,
                        ),
                        (
                            BmAccount.operate_result
                            == AdvertiserStatusResult.PART.value,
                            AdvertiserStatusResult.PART.desc,
                        ),
                        (
                            BmAccount.operate_result
                            == AdvertiserStatusResult.ALL_SUCCEED.value,
                            AdvertiserStatusResult.ALL_SUCCEED.desc,
                        ),
                        (
                            BmAccount.operate_result
                            == AdvertiserStatusResult.ALL_FAIL.value,
                            AdvertiserStatusResult.ALL_FAIL.desc,
                        ),
                    ],
                    else_="",
                ).label("cn_approval_status"),
                case(
                    [
                        (
                            BmAccount.operate_type == Operation.BIND.value,
                            Operation.BIND.desc,
                        ),
                        (
                            BmAccount.operate_type == Operation.UNBIND.value,
                            Operation.UNBIND.desc,
                        ),
                    ],
                    else_="",
                ).label("cn_operate_type"),
                case(
                    [
                        (
                            BmAccount.grant_type == BMGrantType.ANALYZE.value,
                            BMGrantType.ANALYZE.desc,
                        ),
                        (
                            BmAccount.grant_type == BMGrantType.ADVERTISE_ANALYZE.value,
                            BMGrantType.ADVERTISE_ANALYZE.desc,
                        ),
                        (
                            BmAccount.grant_type
                            == BMGrantType.MANAGE_ADVERTISE_ANALYZE.value,
                            BMGrantType.MANAGE_ADVERTISE_ANALYZE.desc,
                        ),
                    ],
                    else_="-",
                ).label("cn_grant_type"),
                subquery.c.account_ids.label("account_ids")
            )
            .outerjoin(subquery, BmAccount.id == subquery.c.bm_account_id)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == BmAccount.user_id)
            .filter(*where)
            .order_by(desc(BmAccount.id))
        )
        obj = MyPagination(query, params.page, params.page_size)
        data = obj.data
        # 处理返回数据
        for item in data:
            account_ids = list(item.pop("account_ids").split(","))
            # 使用 Redis 管道
            pipeline = redis_client.pipeline()
            # 向管道中添加获取 open_subject_name 的命令
            for account_id in account_ids:
                pipeline.hget(f"account:{account_id}", "open_subject_name")

            # 执行管道，获取所有结果
            results = pipeline.execute()
            # 去重 open_subject_name
            open_subject_names = set()
            for result in results:
                if result:  # 如果存在值
                    open_subject_names.add(result)  # 转换为字符串并添加到集合

            # 将去重后的结果以逗号分隔形式拼接成字符串
            item["open_subject_name"] = ', '.join(open_subject_names) if open_subject_names else "-"
            item["operate_type"] = "BM绑定/解绑"
            item["medium_set"] = "Meta"
        return data, obj.counts

    @staticmethod
    def pixel_utils(
            user_id,
            params,
            start_date,
            end_date,
            db,
            operation_result,
            release_media
    ):
        redis_client = RedisClient(db=6).get_redis_client()
        if release_media and release_media != "Meta":
            return [], 0
        where = [PixelAccount.is_delete == False]
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id,
                                               AdvertiserUser.is_delete == False).first()
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(AdvertiserUser.p_id == user_id,
                                                       AdvertiserUser.is_delete == False)
            sub_ids = [i.id for i in sub_user.all()]
            sub_ids.append(user.id)
            where.append(PixelAccount.user_id.in_(sub_ids))
        # 如果是子账号
        else:
            where.append(PixelAccount.user_id == user.id)
        if start_date and end_date:
            where.extend([
                func.date(PixelAccount.created_time) >= func.date(start_date),
                func.date(PixelAccount.created_time) <= func.date(end_date)])
        subquery = db.query(
            PixelAccountDetail.pixel_account_id,
            func.count(PixelAccountDetail.account_id).label("detail_count"),
            func.group_concat(PixelAccountDetail.account_id).label("account_ids")
        ).filter(PixelAccountDetail.is_delete == False).group_by(PixelAccountDetail.pixel_account_id).subquery()
        if params.q:
            # 可以搜索Pixel_ID、提交人、account_id
            user_ids = db.query(AdvertiserUser.id).filter(AdvertiserUser.real_name.like(f'%{params.q}%')).all()
            user_ids_list = [row.id for row in user_ids]
            where.append(
                or_(PixelAccount.pixel_id.like(f'%{params.q}%'),
                    PixelAccount.user_id.in_(user_ids_list),
                    subquery.c.account_ids.like(f'%{params.q}%')))
        if operation_result:
            matching_transfer_ids = (
                db.query(PixelAccountDetail.pixel_account_id)
                .filter(
                    PixelAccountDetail.operate_result == operation_result,
                    PixelAccountDetail.is_delete == False
                ).all()
            )
            ids = [i[0] for i in matching_transfer_ids]
            where.append(PixelAccount.id.in_(ids))
        query = (db.query(
            PixelAccount,
            subquery.c.detail_count,
            subquery.c.account_ids,
            AdvertiserUser.real_name.label("apply_user"),
            case([
                (PixelAccount.operate_result == AdvertiserStatusResult.DEFAULT.value,
                 AdvertiserStatusResult.DEFAULT.desc),
                (PixelAccount.operate_result == AdvertiserStatusResult.ALL_FAIL.value,
                 AdvertiserStatusResult.ALL_FAIL.desc),
                (PixelAccount.operate_result == AdvertiserStatusResult.ALL_SUCCEED.value,
                 AdvertiserStatusResult.ALL_SUCCEED.desc),
                (PixelAccount.operate_result == AdvertiserStatusResult.PART.value,
                 AdvertiserStatusResult.PART.desc),
            ], else_='').label("cn_approval_status"),
            case([
                (PixelAccount.operate_type == Operation.BIND.value, Operation.BIND.desc),
                (PixelAccount.operate_type == Operation.UNBIND.value, Operation.UNBIND.desc)
            ], else_='').label("cn_operate_type")
        ).outerjoin(
            subquery,
            PixelAccount.id == subquery.c.pixel_account_id
        ).outerjoin(AdvertiserUser, AdvertiserUser.id == PixelAccount.user_id).filter(*where
                                                                                      ).order_by(desc(PixelAccount.id)))
        obj = MyPagination(query, params.page, params.page_size)
        data = obj.data
        for item in data:
            account_ids = list(item.pop("account_ids").split(","))
            # 使用 Redis 管道
            pipeline = redis_client.pipeline()
            # 向管道中添加获取 open_subject_name 的命令
            for account_id in account_ids:
                pipeline.hget(f"account:{account_id}", "open_subject_name")

            # 执行管道，获取所有结果
            results = pipeline.execute()
            # 去重 open_subject_name
            open_subject_names = set()
            for result in results:
                if result:  # 如果存在值
                    open_subject_names.add(result)  # 转换为字符串并添加到集合

            # 将去重后的结果以逗号分隔形式拼接成字符串
            item["open_subject_name"] = ', '.join(open_subject_names) if open_subject_names else "-"
            item["operate_type"] = "Pixel"
            item["medium_set"] = "Meta"
        return data, obj.counts


class OperationDetailsUtils:
    @staticmethod
    async def fetch_recharge_detail(
            id: int,
            common_query,
            user_id,
            trace_id=None
    ):
        params = {
            "id": id,
            "page": common_query.page,
            "page_size": common_query.page_size
        }
        dict_list = []
        is_second = False
        is_primary = False
        try:
            # 从外部 CRM 服务获取数据
            response = CRMExternalService.get_recharge_detail(
                params=params,
                **{'trace_id': trace_id}
            )
            data = response.get("data", [])
            msg = response.get("msg")
            total = response.get("total")
            code = response.get("code")

            if code == RET.OK:
                # 从请求上下文获取用户信息
                is_second = get_is_second(user_id)
                is_primary = get_is_primary(user_id)
                # 根据用户是否为二代客户主账户处理数据
                if is_second and is_primary:
                    # 获取 Redis 数据
                    pipe_account_list, group_name_mapping = get_redis_account_group(data)

                    # 处理数据
                    for item, account_groups in zip(data, pipe_account_list):
                        group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                        group_name_str = ", ".join(group_names) or '-'
                        item["group_name"] = group_name_str
                        item["recharge_num"] = round(float(item.get('recharge_num')), 2)
                else:
                    for recharge_dict in data:
                        recharge_dict["recharge_num"] = round(float(recharge_dict.get('recharge_num')), 2)

        except Exception as e:
            web_log.log_critical(str(e))
            return MyResponse(code=RET.UNKNOW_ERR, msg=str(e), total=0, data=dict_list,
                              other_data={"is_second": True if (is_second and is_primary) else False})

        return MyResponse(code=code, msg=msg, total=total, data=data,
                          other_data={"is_second": True if (is_second and is_primary) else False})

    @staticmethod
    async def fetch_reset_detail(id: int, common_query, user_id, trace_id=None):
        params = {"id": id,
                  "page": common_query.page,
                  "page_size": common_query.page_size
                  }
        dict_list = []
        is_second = False
        is_primary = False
        try:
            response = CRMExternalService.get_reset_detail(params=params, **{'trace_id': trace_id})
            data = response.get("data", [])
            msg = response.get("msg")
            total = response.get("total")
            code = response.get("code")
            if code == RET.OK:
                is_second = get_is_second(user_id)
                is_primary = get_is_primary(user_id)
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
            web_log.log_critical(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg=e.__str__(), total=0, data=dict_list,
                              other_data={"is_second": True if (is_second and is_primary) else False})
        return MyResponse(code=code, msg=msg, total=total, data=data,
                          other_data={"is_second": True if (is_second and is_primary) else False})

    @staticmethod
    async def balance_transfer_detail(id: int, user_id, db):
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
                case(
                    [
                        (
                            BalanceTransferDetail.transfer_type
                            == TransferType.ACCOUNT.value,
                            TransferType.ACCOUNT.desc,
                        )
                    ],
                    else_=TransferType.PURSE.desc,
                ).label("transfer_type"),
                BalanceTransferDetail.remark,
                BalanceTransferDetail.order_num,
                BalanceTransferDetail.trade_type,
                BalanceTransferDetail.update_time,
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
                case(
                    [
                        (
                            BalanceTransferDetail.transfer_type
                            == TransferType.ACCOUNT.value,
                            TransferType.ACCOUNT.desc,
                        )
                    ],
                    else_=TransferType.PURSE.desc,
                ).label("transfer_type"),
                BalanceTransferDetail.remark,
                BalanceTransferDetail.order_num,
                BalanceTransferDetail.trade_type,
                BalanceTransferDetail.update_time,
            )
            .filter(
                BalanceTransferDetail.balance_transfer_id == id,
                BalanceTransferDetail.trade_type == TransferTradeType.RECHARGE.value,
            )
            .order_by(BalanceTransferDetail.medium.asc(), BalanceTransferDetail.order_num.asc())
            .all()
        )

        is_second = get_is_second(user_id)
        is_primary = get_is_primary(user_id)
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

    @staticmethod
    async def fetch_recharge_detail(
            id: int,
            common_query,
            user_id,
            trace_id=None
    ):
        params = {
            "id": id,
            "page": common_query.page,
            "page_size": common_query.page_size
        }
        dict_list = []
        is_second = False
        is_primary = False
        try:
            # 从外部 CRM 服务获取数据
            response = CRMExternalService.get_recharge_detail(
                params=params,
                **{'trace_id': trace_id}
            )
            data = response.get("data", [])
            msg = response.get("msg")
            total = response.get("total")
            code = response.get("code")

            if code == RET.OK:
                # 从请求上下文获取用户信息
                is_second = get_is_second(user_id)
                is_primary = get_is_primary(user_id)
                # 根据用户是否为二代客户主账户处理数据
                if is_second and is_primary:
                    # 获取 Redis 数据
                    pipe_account_list, group_name_mapping = get_redis_account_group(data)

                    # 处理数据
                    for item, account_groups in zip(data, pipe_account_list):
                        group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                        group_name_str = ", ".join(group_names) or '-'
                        item["group_name"] = group_name_str
                        item["recharge_num"] = round(float(item.get('recharge_num')), 2)
                else:
                    for recharge_dict in data:
                        recharge_dict["recharge_num"] = round(float(recharge_dict.get('recharge_num')), 2)

        except Exception as e:
            web_log.log_critical(str(e))
            return MyResponse(code=RET.UNKNOW_ERR, msg=str(e), total=0, data=dict_list,
                              other_data={"is_second": True if (is_second and is_primary) else False})

        return MyResponse(code=code, msg=msg, total=total, data=data,
                          other_data={"is_second": True if (is_second and is_primary) else False})

    @staticmethod
    async def fetch_reset_detail(id: int, common_query, user_id, trace_id=None):
        params = {"id": id,
                  "page": common_query.page,
                  "page_size": common_query.page_size
                  }
        dict_list = []
        is_second = False
        is_primary = False
        try:
            response = CRMExternalService.get_reset_detail(params=params, **{'trace_id': trace_id})
            data = response.get("data", [])
            msg = response.get("msg")
            total = response.get("total")
            code = response.get("code")
            if code == RET.OK:
                is_second = get_is_second(user_id)
                is_primary = get_is_primary(user_id)
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
            web_log.log_critical(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg=e.__str__(), total=0, data=dict_list,
                              other_data={"is_second": True if (is_second and is_primary) else False})
        return MyResponse(code=code, msg=msg, total=total, data=data,
                          other_data={"is_second": True if (is_second and is_primary) else False})

    @staticmethod
    async def balance_transfer_detail(id: int, user_id, db):
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
                BalanceTransferDetail.update_time,
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
                BalanceTransferDetail.update_time,
            )
            .filter(
                BalanceTransferDetail.balance_transfer_id == id,
                BalanceTransferDetail.trade_type == TransferTradeType.RECHARGE.value,
            )
            .order_by(BalanceTransferDetail.medium.asc(), BalanceTransferDetail.order_num.asc())
            .all()
        )

        is_second = get_is_second(user_id)
        is_primary = get_is_primary(user_id)
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

    @staticmethod
    async def get_account_rename_detail(rename_id: int, userid, db):
        where = [AccountRename.is_delete == False]
        # 获取当前用户信息
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        res_id = user.p_id if user and user.p_id else userid

        # 获取主账号的注册信息，判断是否为二代客户
        res_user = (
            db.query(AdvertiserRegister)
            .filter(AdvertiserRegister.user_id == res_id, AdvertiserRegister.is_delete == False)
            .first()
        )
        is_second = res_user.is_second if res_user else False

        # 设置查询条件
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == userid, AdvertiserUser.is_delete == False
            )
            sub_ids = [i.id for i in sub_user.all()]
            sub_ids.append(user.id)
            where.append(AccountRename.user_id.in_(sub_ids))
        else:
            where.append(AccountRename.user_id == user.id)

        # 添加重命名记录ID的过滤条件
        where.append(AccountRename.id == rename_id)

        # 查询详情数据
        result = (
            db.query(
                AccountRename,
                AdvertiserUser.real_name.label('username'),
                case(
                    [
                        (
                            AccountRename.operate_result == OperateResult.SUCCESS.value,
                            OperateResult.SUCCESS.desc,
                        ),
                        (
                            AccountRename.operate_result == OperateResult.DEFAULT.value,
                            OperateResult.DEFAULT.desc,
                        ),
                        (
                            AccountRename.operate_result == OperateResult.FAIL.value,
                            OperateResult.FAIL.desc,
                        ),
                    ],
                    else_="",
                ).label("cn_operation_result"),
            )
            .filter(*where)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == AccountRename.user_id)
            .first()  # 获取单条记录
        )
        if not result:
            return MyResponse(RET.DATA_ERR, error_map[RET.DATA_ERR])
        # 处理返回数据
        response_data = [{
            "before_account_name": result.AccountRename.before_account_name,
            "after_account_name": result.AccountRename.after_account_name,
            "remark": result.AccountRename.remark,
            "account_id": result.AccountRename.account_id,
            # 其他字段可以在这里添加
        }]

        # 是二代客户
        is_primary = get_is_primary(userid)
        # 是二代客户主账户
        if is_second and is_primary:
            # 获取redis数据
            pipe_account_list, group_name_mapping = get_redis_account_group(response_data)

            # 赋值组名称
            for item, account_groups in zip(response_data, pipe_account_list):
                group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                group_name_str = ", ".join(group_names) or '-'
                item['group_name'] = group_name_str

        return MyResponse(data=response_data, total=1,
                          other_data={"is_second": True if (is_second and is_primary) else False})

    @staticmethod
    async def bm_detail(id, common_query, user_id, db):
        query = db.query(
            BmAccountDetail.id,
            BmAccountDetail.account_id,
            case(
                [
                    (
                        BmAccountDetail.operate_result == OperateResult.DEFAULT.value,
                        OperateResult.DEFAULT.desc,
                    ),
                    (
                        BmAccountDetail.operate_result == OperateResult.SUCCESS.value,
                        OperateResult.SUCCESS.desc,
                    ),
                    (
                        BmAccountDetail.operate_result == OperateResult.FAIL.value,
                        OperateResult.FAIL.desc,
                    ),
                ],
                else_="",
            ).label("cn_operate_result"),
            case(
                [
                    (
                        BmAccount.operate_type == Operation.BIND.value,
                        Operation.BIND.desc,
                    ),
                    (
                        BmAccount.operate_type == Operation.UNBIND.value,
                        Operation.UNBIND.desc,
                    ),
                ],
                else_="",
            ).label("cn_operate_type"),
            case(
                [
                    (
                        BmAccount.grant_type == BMGrantType.ANALYZE.value,
                        BMGrantType.ANALYZE.desc,
                    ),
                    (
                        BmAccount.grant_type == BMGrantType.ADVERTISE_ANALYZE.value,
                        BMGrantType.ADVERTISE_ANALYZE.desc,
                    ),
                    (
                        BmAccount.grant_type
                        == BMGrantType.MANAGE_ADVERTISE_ANALYZE.value,
                        BMGrantType.MANAGE_ADVERTISE_ANALYZE.desc,
                    ),
                ],
                else_="-",
            ).label("cn_grant_type"),
            BmAccountDetail.remark,
            BmAccount.business_id,
            BmAccount.grant_type,
        ).outerjoin(
            BmAccount, BmAccountDetail.bm_account_id == BmAccount.id  # 添加联接条件
        ).filter(
            BmAccountDetail.is_delete == False, BmAccountDetail.bm_account_id == id
        )
        paginator = MyPagination(query, common_query.page, common_query.page_size)

        res_data = paginator.data

        # 是二代客户
        is_second = get_is_second(user_id)
        is_primary = get_is_primary(user_id)
        # 是二代客户主账户
        if is_second and is_primary:
            # 获取redis数据
            pipe_account_list, group_name_mapping = get_redis_account_group(res_data)

            # 赋值组名称
            for item, account_groups in zip(res_data, pipe_account_list):
                group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                group_name_str = ", ".join(group_names) or '-'
                item['group_name'] = group_name_str
        return MyResponse(total=paginator.counts, data=res_data,
                          other_data={"is_second": True if (is_second and is_primary) else False})

    @staticmethod
    async def bc_detail(id, common_query, user_id, db):
        query = (db.query(
            BcAccountDetail.id,
            BcAccountDetail.account_id,
            BcAccountDetail.business_id,
            case(
                [
                    (
                        BcAccountDetail.operate_result == OperateResult.DEFAULT.value,
                        OperateResult.DEFAULT.desc,
                    ),
                    (
                        BcAccountDetail.operate_result == OperateResult.SUCCESS.value,
                        OperateResult.SUCCESS.desc,
                    ),
                    (
                        BcAccountDetail.operate_result == OperateResult.FAIL.value,
                        OperateResult.FAIL.desc,
                    ),
                ],
                else_="",
            ).label("cn_operate_result"),
            case(
                [
                    (
                        BcAccount.operate_type == Operation.BIND.value,
                        Operation.BIND.desc,
                    ),
                    (
                        BcAccount.operate_type == Operation.UNBIND.value,
                        Operation.UNBIND.desc,
                    ),
                ],
                else_="",
            ).label("cn_operate_type"),
            case(
                [
                    (
                        BcAccount.grant_type == BCGrantType.ANALYST.value,
                        BCGrantType.ANALYST.desc,
                    ),
                    (
                        BcAccount.grant_type == BCGrantType.OPERATOR.value,
                        BCGrantType.OPERATOR.desc,
                    ),
                ],
                else_="-",
            ).label("cn_grant_type"),
            BcAccountDetail.remark,
            BcAccount.cooperative_id
        ).outerjoin(BcAccount, BcAccount.id == BcAccountDetail.tiktok_bc_account_id)
        .filter(
            BcAccountDetail.is_delete == False,
            BcAccountDetail.tiktok_bc_account_id == id,
        ))
        paginator = MyPagination(query, common_query.page, common_query.page_size)
        res_data = paginator.data

        is_second = get_is_second(user_id)
        is_primary = get_is_primary(user_id)
        # 是二代客户主账户
        if is_second and is_primary:
            # 获取redis数据
            pipe_account_list, group_name_mapping = get_redis_account_group(res_data)

            # 赋值组名称
            for item, account_groups in zip(res_data, pipe_account_list):
                group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                group_name_str = ", ".join(group_names) or '-'
                item['group_name'] = group_name_str
        return MyResponse(total=paginator.counts, data=res_data,
                          other_data={"is_second": True if (is_second and is_primary) else False})

    @staticmethod
    async def pixel_details(id, common_query, user_id, db):
        query = db.query(
            PixelAccountDetail.id,
            PixelAccountDetail.account_id,
            case([
                (PixelAccountDetail.operate_result == OperateResult.DEFAULT.value,
                 OperateResult.DEFAULT.desc),
                (PixelAccountDetail.operate_result == OperateResult.SUCCESS.value,
                 OperateResult.SUCCESS.desc),
                (PixelAccountDetail.operate_result == OperateResult.FAIL.value,
                 OperateResult.FAIL.desc)
            ], else_="").label("cn_operate_result"),
            case([
                (PixelAccount.operate_type == Operation.BIND.value, Operation.BIND.desc),
                (PixelAccount.operate_type == Operation.UNBIND.value, Operation.UNBIND.desc)
            ], else_='').label("cn_operate_type"),
            PixelAccount.pixel_id,
            case([
                (PixelAccountDetail.remark == "", "-"),
                (PixelAccountDetail.remark == None, "-")
            ], else_=PixelAccountDetail.remark).label("remark")
        ).outerjoin(PixelAccount, PixelAccount.id == PixelAccountDetail.pixel_account_id).filter(
            PixelAccountDetail.is_delete == False, PixelAccountDetail.pixel_account_id == id)
        paginator = MyPagination(query, common_query.page, common_query.page_size)
        res_data = paginator.data

        is_second = get_is_second(user_id)
        is_primary = get_is_primary(user_id)
        # 是二代客户主账户
        if is_second and is_primary:
            # 获取redis数据
            pipe_account_list, group_name_mapping = get_redis_account_group(res_data)

            # 赋值组名称
            for item, account_groups in zip(res_data, pipe_account_list):
                group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                group_name_str = ", ".join(group_names) or '-'
                item['group_name'] = group_name_str
        return MyResponse(total=paginator.counts, data=res_data,
                          other_data={"is_second": True if (is_second and is_primary) else False})

    @staticmethod
    async def open_account(
            ticket_id,
            db,
            trace_id=None
    ):
        # 获取开户工单详情
        oe_open_account = (
            db.query(
                OeOpenAccount.ticket_id,
                OeOpenAccount.created_time,
                OeOpenAccount.chinese_legal_entity_name.label("business_license_name"),
                case(
                    [
                        (
                            OeOpenAccount.approval_status
                            == OeApproveStatus.PENDING.value,
                            OeApproveStatus.PENDING.desc,
                        ),
                        (
                            OeOpenAccount.approval_status
                            == OeApproveStatus.APPROVED.value,
                            OeApproveStatus.APPROVED.desc,
                        ),
                        (
                            OeOpenAccount.approval_status
                            == OeApproveStatus.DISAPPROVED.value,
                            OeApproveStatus.DISAPPROVED.desc,
                        ),
                        (
                            OeOpenAccount.approval_status
                            == OeApproveStatus.CHANGES_REQUESTED.value,
                            OeApproveStatus.CHANGES_REQUESTED.desc,
                        ),
                        (
                            OeOpenAccount.approval_status
                            == OeApproveStatus.AUTO_DISAPPROVED.value,
                            OeApproveStatus.AUTO_DISAPPROVED.desc,
                        ),
                    ]
                ).label("approval_status"),
                OeOpenAccount.promotion_website,
                OeOpenAccount.org_ad_account_count.label("account_count"),
                OeOpenAccount.user_id,
                OeOpenAccount.customer_id,
                OeOpenAccount.oe_number,
                AdvertiserUser.real_name.label("real_name"),
            )
            .select_from(OeOpenAccount)
            .filter(OeOpenAccount.ticket_id == ticket_id)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == OeOpenAccount.user_id)
            .first()
        )
        if oe_open_account:
            res_data = row_dict(oe_open_account)
            if res_data["customer_id"]:
                json_ = {
                    "customer_ids": [res_data["customer_id"]]
                }
                crm_result = CRMExternalService.customer_id_name(json_, **{'trace_id': trace_id})
                customer_name = None
                try:
                    customer_name = crm_result["data"][0]["name"]
                except Exception as e:
                    web_log.log_critical(e)
                    res_data["customer_name"] = None
                else:
                    res_data["customer_name"] = customer_name
            return MyResponse(data=res_data)
        else:
            return MyResponse(code=RET.DATA_ERR, msg="工单不存在")


class OperationMediums:
    @staticmethod
    def get_recharges_mediums(user_id, db, trace_id=None):
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
                    )
                    .all()
                )
            # 登录人是主账户
            if not p_id:
                work_order_ids = (
                    db.query(WorkOrder.work_order_id)
                    .filter(WorkOrder.company_id == company_id)
                    .all()
                )
            work_order_ids = [i[0] for i in work_order_ids] if work_order_ids else []
            json_ = {
                "work_order_ids": work_order_ids,
                "operation_type": OperationType.ACCOUNT_RECHARGE
            }
            response = CRMExternalService.get_recharge_reset_medium(json=json_, **{'trace_id': trace_id})
            recharge_medium_list = response.get("data", [])
            return recharge_medium_list

    @staticmethod
    def get_reset_mediums(user_id, db, trace_id=None):
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
                    WorkOrder.flow_code == 'account_reset').all()
            # 登录人是主账户
            if not p_id:
                work_order_ids = db.query(WorkOrder.work_order_id).filter(WorkOrder.company_id == company_id,
                                                                          WorkOrder.flow_code == 'account_reset').all()
            work_order_ids = [i[0] for i in work_order_ids] if work_order_ids else []
            json_ = {
                "work_order_ids": work_order_ids,
                "operation_type": OperationType.ACCOUNT_RESET
            }
            response = CRMExternalService.get_recharge_reset_medium(json=json_, **{'trace_id': trace_id})
            recharge_medium_list = response.get("data", [])

            return recharge_medium_list

    @staticmethod
    def get_balance_mediums(user_id, db, sub_user_ids):
        filter_list = []

        if sub_user_ids:
            # 如果有子账户
            sub_user_ids.append(user_id)
            filter_list.append(BalanceTransfer.user_id.in_(sub_user_ids))
        else:
            filter_list.append(BalanceTransfer.user_id == user_id)

        # 子查询，获取每个 balance_transfer_id 对应的去重 medium 列表
        medium_subquery = (
            db.query(
                BalanceTransferDetail.balance_transfer_id,
                func.group_concat(func.distinct(BalanceTransferDetail.medium)).label("mediums"),
            )
            .filter(BalanceTransferDetail.is_delete == False)
            .group_by(BalanceTransferDetail.balance_transfer_id)
            .subquery()
        )

        # 主查询：关联 BalanceTransfer 和 medium_subquery，并应用过滤条件
        query = (
            db.query(
                medium_subquery.c.mediums.label("mediums")  # 返回 mediums 字段
            )
            .select_from(BalanceTransfer)
            .outerjoin(
                AdvertiserUser,
                AdvertiserUser.id == BalanceTransfer.user_id
            )
            .join(
                medium_subquery,
                BalanceTransfer.id == medium_subquery.c.balance_transfer_id
            )
            .filter(*filter_list)
            .order_by(BalanceTransfer.created_time.desc())
        )
        # 执行查询
        result = query.all()

        # 如果有查询结果，处理 mediums 字段
        medium_list = []
        for row in result:
            if row.mediums:
                medium_list.extend(row.mediums.split(','))  # 将 mediums 字符串拆分为列表

        # 对 mediums 列表去重
        balances_mediums_list = list(set(medium_list))

        # 返回去重后的 mediums 列表
        return balances_mediums_list

    @staticmethod
    def get_renames_mediums(user_id, db):
        where = [AccountRename.is_delete == False]
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False)
            .first()
        )
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
            )
            sub_ids = [i.id for i in sub_user.all()]
            sub_ids.append(user.id)
            where.append(AccountRename.user_id.in_(sub_ids))
        # 如果是子账号
        else:
            where.append(AccountRename.user_id == user.id)

        query = (
            db.query(
                AccountRename.medium,
            )
            .filter(*where)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == AccountRename.user_id)
            .order_by(desc(AccountRename.id))
        )
        result = query.all()
        # 如果有查询结果，处理 mediums 字段
        medium_list = []
        for row in result:
            if row.medium:
                medium_list.extend(row.medium.split(','))  # 将 mediums 字符串拆分为列表

        # 对 mediums 列表去重
        renames_mediums_list = list(set(medium_list))
        # 返回去重后的 mediums 列表
        return renames_mediums_list

    @staticmethod
    def get_bm_mediums(db, user_id):
        where = [BmAccount.is_delete == False]
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False)
            .first()
        )
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
            )
            sub_names = [i.id for i in sub_user.all()]
            sub_names.append(user.id)
            where.append(BmAccount.user_id.in_(sub_names))
        # 如果是子账号
        else:
            where.append(BmAccount.user_id == user.id)
        query = (
            db.query(
                BmAccount,
                AdvertiserUser.real_name.label('apply_user')
            )
            .select_from(BmAccount)
            .filter(*where)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == BmAccount.user_id)
            .order_by(desc(BmAccount.id))
        )
        result = query.all()
        if result:
            bm_mediums_list = ["Meta"]
        else:
            bm_mediums_list = []
        return bm_mediums_list

    @staticmethod
    def get_bc_mediums(db, user_id):
        where = [BcAccount.is_delete == False]
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False)
            .first()
        )
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
            )
            sub_names = [i.id for i in sub_user.all()]
            sub_names.append(user.id)
            where.append(BcAccount.user_id.in_(sub_names))
        # 如果是子账号
        else:
            where.append(BcAccount.user_id == user.id)
        query = (
            db.query(
                BcAccount,
                AdvertiserUser.real_name.label('apply_user')
            )
            .select_from(BcAccount)
            .filter(*where)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == BcAccount.user_id)
            .order_by(desc(BcAccount.id))
        )
        result = query.all()
        if result:
            bc_mediums_list = ["Tiktok"]
        else:
            bc_mediums_list = []
        return bc_mediums_list

    @staticmethod
    def get_pixel_mediums(db, user_id):
        where = [PixelAccount.is_delete == False]
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id,
                                               AdvertiserUser.is_delete == False).first()
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(AdvertiserUser.p_id == user_id,
                                                       AdvertiserUser.is_delete == False)
            sub_ids = [i.id for i in sub_user.all()]
            sub_ids.append(user.id)
            where.append(PixelAccount.user_id.in_(sub_ids))
        # 如果是子账号
        else:
            where.append(PixelAccount.user_id == user.id)

        query = (
            db.query(
                PixelAccount,
                AdvertiserUser.real_name.label('apply_user')
            )
            .select_from(PixelAccount)
            .filter(*where)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == PixelAccount.user_id)
            .order_by(desc(PixelAccount.id))
        )
        result = query.all()
        if result:
            pixel_mediums_list = ["Meta"]
        else:
            pixel_mediums_list = []
        return pixel_mediums_list
