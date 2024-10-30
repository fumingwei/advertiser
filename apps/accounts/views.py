# -*- coding: utf-8 -*-
import json
from datetime import datetime
from fastapi import APIRouter
from fastapi import Depends, Query
from fastapi_utils.cbv import cbv
from sqlalchemy import case, or_, func, and_, desc
from sqlalchemy.orm import Session
from starlette.requests import Request
from apps.common.utils import get_is_second, get_redis_account_group, permission_check
from apps.onboarding.define import OeAccountStatus, OeApproveStatus, oe_account_status_english, \
    oe_approve_status_english
from apps.onboarding.models import OeOpenAccount
from libs.internal.crm_external_service import CRMExternalService
from libs.internal.api_service import APIService
from apps.accounts.define import (
    BMGrantType,
    BCGrantType,
    AdvertiserStatusResult,
    OperateResult,
    medium_account_status_object,
    custom_account_status_object,
    Medium,
)
from apps.accounts.models import (
    AccountRename,
    BmAccount,
    BmAccountDetail,
    BcAccount,
    BcAccountDetail,
)
from apps.accounts.schemas import (
    SubmitAccountRenameSchemas,
    AccountRenameSchemas,
    BMOperateSchemas,
    BCOperateSchemas,
    BmAccountGrantTypeSchema,
)
from apps.accounts.utils import get_customer_ids
from apps.advertiser.define import RegisterStatus
from apps.advertiser.models import (
    UserCusRelationship,
    AdvertiserUser,
    AdvertiserRegister, GroupMemberRelationship, ProjectGroup,
)
from settings.db import get_db, MyPagination
from tools.common import CommonQueryParams, row_dict
from tools.constant import RET, error_map, Operation, OperationType
from tools.resp import MyResponse
from settings.log import web_log
from apps.callback.tasks import mapi_request_result

AccountRouter = APIRouter(tags=["账户列表"])


# 账户重命名
@cbv(AccountRouter)
class AccountRenameServer:
    request: Request

    @AccountRouter.get("/account_renames", description="账户重命名列表")
    async def GetAccountRename(
            self, common_query: CommonQueryParams = Depends(), db: Session = Depends(get_db)
    ):
        where = [AccountRename.is_delete == False]
        userid = self.request.state.user.user_id
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        res_id = user.p_id if user and user.p_id else userid
        res_user = db.query(AdvertiserRegister).filter(AdvertiserRegister.user_id == res_id,
                                                       AdvertiserRegister.is_delete == False).first()
        is_second = res_user.is_second if res_user else False
        is_primary = False if user.p_id else True
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == userid, AdvertiserUser.is_delete == False
            )
            sub_ids = [i.id for i in sub_user.all()]
            sub_ids.append(user.id)
            where.append(AccountRename.user_id.in_(sub_ids))
        # 如果是子账号
        else:
            where.append(AccountRename.user_id == user.id)
        if common_query.q:
            # 模糊搜索能搜索广告账户ID、修改前名称、修改后名称和提交人
            user_ids = db.query(AdvertiserUser.id).filter(AdvertiserUser.real_name.like(f'%{common_query.q}%')).all()
            user_ids_list = [row.id for row in user_ids]
            where.append(
                or_(
                    AccountRename.before_account_name.ilike(f"%{common_query.q}%"),
                    AccountRename.after_account_name.ilike(f"%{common_query.q}%"),
                    AccountRename.account_id.ilike(f"%{common_query.q}%"),
                    AccountRename.user_id.in_(user_ids_list),
                )
            )
        query = (
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
            .order_by(desc(AccountRename.id))
        )
        obj = MyPagination(query, common_query.page, common_query.page_size)
        res_data = obj.data

        # 是二代客户主账户
        if is_second and is_primary:
            # 获取redis数据
            pipe_account_list, group_name_mapping = get_redis_account_group(res_data)

            # 赋值组名称
            for item, account_groups in zip(res_data, pipe_account_list):
                group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                group_name_str = ", ".join(group_names) or '-'
                item['group_name'] = group_name_str
        return MyResponse(data=res_data, total=obj.counts, other_data={"is_second": is_second})

    @AccountRouter.post("/account_submit", description="提交账户重命名信息")
    async def SubmitAccountRename(self, data: SubmitAccountRenameSchemas, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        data = data.dict()
        accounts = data.get("account_rename", [])
        account_ids = [item.get("account_id") for item in accounts]
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False)
            .first()
        )
        is_second = get_is_second(user_id)
        # 是子账户还是二代客户
        if user and user.p_id and is_second:
            no_allow_account = permission_check(user_id, account_ids, OperationType.ACCOUNTRENAME)
            if no_allow_account:
                return MyResponse(code=RET.INVALID_DATA, msg=f"{no_allow_account}账户未被授权此操作类型")
        json_ = {
            "account_ids": account_ids
        }
        res = CRMExternalService.get_account_customers(json_, **{'trace_id': self.request.state.trace_id})
        empty_customer_ids = [
            str(item["account_id"])
            for item in res.get("data")
            if item["customer_id"] is None
        ]
        joined_ids = ",".join(empty_customer_ids)
        if empty_customer_ids:
            return MyResponse(
                code=RET.DATA_ERR, msg=f"广告账户{joined_ids}不在系统中,请先录入"
            )
        medium_res = CRMExternalService.get_accounts_medium(json_, **{'trace_id': self.request.state.trace_id})
        medium_data = [item['medium'] for item in medium_res.get('data') if item['medium'] not in ['Meta', 'Tiktok']]
        if medium_data:
            return MyResponse(
                code=RET.DATA_ERR, msg="暂未开通除Meta和Tiktok媒介外的账户重命名功能"
            )
        # 判断广告账户的投放方式
        put_way_res = CRMExternalService.get_account_put_way(json_, **{'trace_id': self.request.state.trace_id})
        if put_way_res.get("code") != RET.OK:
            return MyResponse(code=medium_res.get("code"), msg=medium_res.get("msg"))
        if put_way_res.get('data') == '代投':
            return MyResponse(code=RET.DATA_ERR, msg="代投账户没有权限重命名")
        account_data = db.query(AccountRename.account_id).filter(
            and_(AccountRename.account_id.in_(account_ids),
                 AccountRename.operate_result == OperateResult.DEFAULT.value)).all()
        if account_data:
            joined_accounts = ",".join(item[0] for item in account_data)
            return MyResponse(
                code=RET.DATA_ERR, msg=f"广告账户{joined_accounts}重命名还在处理中，请稍后再试"
            )
        # 获取广告账户id 名字
        rename_list = []
        account_id_dict = {}  # 用于存储最后一个account_id对应的AccountRenameDetailSchemas对象
        for i in accounts:
            account_id_dict[i.get("account_id")] = i
        account_dict = {account["account_id"]: account for account in res.get("data")}
        for account_id, account_detail in account_id_dict.items():
            account = account_dict.get(account_id)
            if account:
                # 返回给前端广告账户id、更改前名称和更改后名称
                rename_list.append(
                    {
                        "account_id": account.get("account_id"),
                        "before_account_name": (
                            account.get("account_name")
                            if account.get("account_name") != None
                            else ""
                        ),
                        "after_account_name": account_detail.get("after_account_name"),
                    }
                )
        return MyResponse(code=RET.OK, msg="提交账号重命名信息成功", data=rename_list)

    @AccountRouter.post("/account_renames", description="账户重命名")
    async def AddAccountRename(
            self, data: AccountRenameSchemas, db: Session = Depends(get_db)
    ):
        userid = self.request.state.user.user_id
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        customer_ids = (
            db.query(UserCusRelationship.customer_id)
            .filter(
                UserCusRelationship.company_id == user.company_id,
                UserCusRelationship.is_delete == False,
            )
            .first()
        )
        # 传所有输入的账户ID返回客户ID判断客户ID在不在登录的用户授权的客户列表里面
        data = data.dict()
        accounts = data.get("account_rename", [])
        account_ids = [item.get("account_id") for item in accounts]
        account_ids_ = list(set(account_ids))
        if len(account_ids) != len(account_ids_):
            return MyResponse(code=RET.INVALID_DATA, msg=f"账户不允许重复！")
        is_second = get_is_second(userid)
        # 是子账户还是二代客户
        if user and user.p_id and is_second:
            no_allow_account = permission_check(userid, account_ids, OperationType.ACCOUNTRENAME)
            if no_allow_account:
                return MyResponse(code=RET.INVALID_DATA, msg=f"{no_allow_account}账户未被授权此操作类型")
        json_ = {"account_ids": account_ids}
        customer_res = CRMExternalService.get_account_customers(json_, **{'trace_id': self.request.state.trace_id})
        accept_customer_ids = [
            str(item["customer_id"]) for item in customer_res.get("data")
        ]
        not_exists_in_customer_ids = [
            item for item in accept_customer_ids if int(item) not in customer_ids[0]
        ]
        if not_exists_in_customer_ids:
            return MyResponse(
                code=RET.NO_DATA,
                msg="您输入的广告账户尚未获得授权，请联系我们的工作人员授权!",
            )
        account_data = (db.query(AccountRename.account_id).filter(and_(AccountRename.account_id.in_(account_ids),
                                                                       AccountRename.operate_result == OperateResult.DEFAULT.value)).all())
        if account_data:
            joined_accounts = ",".join(item[0] for item in account_data)
            return MyResponse(
                code=RET.DATA_ERR, msg=f"广告账户{joined_accounts}重命名还在处理中，请稍后再试"
            )
        # 获取需要重命名的账户信息
        account_ids = [i.get("account_id") for i in accounts]
        json_ = {"account_ids": account_ids}
        medium_res = CRMExternalService.get_accounts_medium(json_, **{'trace_id': self.request.state.trace_id})
        if medium_res.get("code") != RET.OK:
            return MyResponse(code=medium_res.get("code"), msg=medium_res.get("msg"))
        # 构建账户信息字典，以便快速查找
        account_rename_dict = {
            i.get("account_id"): i.get("after_account_name")
            for i in accounts
        }
        # 构建账户重命名数据
        rename_data = []
        for item in medium_res.get("data"):
            rename_data.append(
                {
                    "data": [
                        {
                            "account_id": item.get("account_id"),
                            "name": account_rename_dict[item.get("account_id")],
                        }
                    ],
                    "medium": item.get("medium"),
                }
            )
        accounts_all = {"rename_data": rename_data}
        try:
            # 调用重命名http
            request_id = APIService.post_rename_account(accounts_all, **{'trace_id': self.request.state.trace_id})
            if not request_id:
                return MyResponse(
                    code=RET.PARAM_ERR, msg="请重新输入要绑定的广告账户ID"
                )
            new_users = []
            for i in accounts:
                for item in medium_res.get("data"):
                    if item.get("account_id") == i.get("account_id"):
                        new_user = AccountRename(
                            request_id=request_id,
                            account_id=item.get("account_id"),
                            medium=item.get("medium"),
                            before_account_name=i.get("before_account_name"),
                            after_account_name=i.get("after_account_name"),
                            remark="",
                            operate_time=datetime.now(),
                            user_id=userid,
                        )
                        new_users.append(new_user)
            db.add_all(new_users)
        except Exception as e:
            web_log.log_error(
                f"账户重命名调用失败原因：{e}"
            )
            return MyResponse(code=RET.DATA_ERR, msg="调用媒体接口失败")
        else:
            db.commit()
            mapi_request_result.delay(request_id)
        return MyResponse(code=RET.OK, msg="账户重命名成功")


# BM账户
@cbv(AccountRouter)
class BmAccountServer:
    request: Request

    @AccountRouter.get("/bm_accounts", description="BM账户列表")
    async def bm_accounts(
            self, common_query: CommonQueryParams = Depends(), db: Session = Depends(get_db)
    ):
        """
        BM操作账户列表
        """
        where = [BmAccount.is_delete == False]
        userid = self.request.state.user.user_id
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == userid, AdvertiserUser.is_delete == False
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
        if common_query.q:
            # 搜索商业账户ID、广告账户id和提交人
            user_ids = db.query(AdvertiserUser.id).filter(AdvertiserUser.real_name.like(f'%{common_query.q}%')).all()
            user_ids_list = [row.id for row in user_ids]
            where.append(
                or_(
                    BmAccount.business_id.like(f"%{common_query.q}%"),
                    BmAccount.user_id.in_(user_ids_list),
                    subquery.c.account_ids.like(f"%{common_query.q}%"),
                )
            )
        query = (
            db.query(
                BmAccount,
                AdvertiserUser.real_name.label('username'),
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
            )
            .outerjoin(subquery, BmAccount.id == subquery.c.bm_account_id)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == BmAccount.user_id)
            .filter(*where)
            .order_by(desc(BmAccount.id))
        )
        paginator = MyPagination(query, common_query.page, common_query.page_size)
        return MyResponse(total=paginator.counts, data=paginator.data)

    @AccountRouter.get("/bm_accounts_detail/{id}", description="BM账户列表详情")
    async def bm_accounts_detail(
            self,
            id: int,
            common_query: CommonQueryParams = Depends(),
            db: Session = Depends(get_db),
    ):
        """
        BM操作账户详情
        """
        user_id = self.request.state.user.user_id
        is_second = get_is_second(user_id)

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
            BmAccountDetail.remark,
        ).filter(
            BmAccountDetail.is_delete == False, BmAccountDetail.bm_account_id == id
        )
        paginator = MyPagination(query, common_query.page, common_query.page_size)
        res_data = paginator.data

        user_pid = db.query(AdvertiserUser.p_id).filter(AdvertiserUser.id == user_id,
                                                        AdvertiserUser.is_delete == 0).scalar()
        is_primary = False if user_pid else True
        # 是二代客户主账户
        if is_second and is_primary:
            # 获取redis数据
            pipe_account_list, group_name_mapping = get_redis_account_group(res_data)

            # 赋值组名称
            for item, account_groups in zip(res_data, pipe_account_list):
                group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                group_name_str = ", ".join(group_names) or '-'
                item['group_name'] = group_name_str
        return MyResponse(total=paginator.counts, data=res_data, other_data={"is_second": is_second})

    # @AccountRouter.post("/bm_export", description="BM导出")
    # async def bm_accounts_export(self, data: BmExportSchemas, db: Session = Depends(get_db)):
    #     """
    #     导出BM_ACCOUNT xlsx文件
    #     """
    #     data = data.dict()
    #     start_date = data.get("start_date")
    #     end_date = data.get("end_date")
    #     bm_id = data.get("bm_id")
    #     userid = self.request.state.user.user_id
    #     where = []
    #     user = (
    #         db.query(AdvertiserUser)
    #         .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
    #         .first()
    #     )
    #     # 如果是主账号
    #     if not user.p_id:
    #         sub_user = db.query(AdvertiserUser).filter(
    #             AdvertiserUser.p_id == userid, AdvertiserUser.is_delete == False
    #         )
    #         sub_names = [i.id for i in sub_user.all()]
    #         sub_names.append(user.id)
    #         where.append(f"cu_meta_bm_accounts.user_id IN ({', '.join(str(id) for id in sub_names)})")
    #     # 如果是子账号
    #     else:
    #         where.append(f"cu_meta_bm_accounts.user_id = {user.id}")
    #     if start_date:
    #         where.append(
    #             f"date(cu_meta_bm_accounts.operate_time) >= date('{start_date}')"
    #         )
    #     if end_date:
    #         where.append(
    #             f"date(cu_meta_bm_accounts.operate_time) <= date('{end_date}')"
    #         )
    #     if bm_id:
    #         where.append(f"cu_meta_bm_accounts.business_id = '{bm_id}'")
    #     where_sql = " AND " + " AND ".join(where) if where else ""
    #     sql = f"""
    #                 SELECT
    #                 cu_meta_bm_accounts.business_id AS "商业账户ID",
    #                 cu_meta_bm_account_details.account_id AS "广告账户ID",
    #                 CASE
    #                     WHEN cu_meta_bm_accounts.grant_type="{BMGrantType.ANALYZE.value}" THEN "{BMGrantType.ANALYZE.desc}"
    #                     WHEN cu_meta_bm_accounts.grant_type="{BMGrantType.ADVERTISE_ANALYZE.value}" THEN "{BMGrantType.ADVERTISE_ANALYZE.desc}"
    #                     WHEN cu_meta_bm_accounts.grant_type="{BMGrantType.MANAGE_ADVERTISE_ANALYZE.value}" THEN "{BMGrantType.MANAGE_ADVERTISE_ANALYZE.desc}"
    #                     ELSE ""
    #                 END AS "授权类型",
    #                 CASE
    #                     WHEN cu_meta_bm_accounts.operate_type="{Operation.BIND.value}" THEN "{Operation.BIND.desc}"
    #                     WHEN cu_meta_bm_accounts.operate_type="{Operation.UNBIND.value}" THEN "{Operation.UNBIND.desc}"
    #                     ELSE ""
    #                 END AS "操作类型",
    #                 CASE
    #                     WHEN cu_meta_bm_account_details.operate_result="{OperateResult.DEFAULT.value}" THEN "{OperateResult.DEFAULT.desc}"
    #                     WHEN cu_meta_bm_account_details.operate_result="{OperateResult.SUCCESS.value}" THEN "{OperateResult.SUCCESS.desc}"
    #                     WHEN cu_meta_bm_account_details.operate_result="{OperateResult.FAIL.value}" THEN "{OperateResult.FAIL.desc}"
    #                     ELSE ""
    #                 END AS "操作结果",
    #                 cu_meta_bm_account_details.remark AS "备注",
    #                 cu_meta_bm_account_details.operate_time AS "操作时间",
    #                 cu_advertiser_users.real_name AS "提交人"
    #                 FROM
    #                     cu_meta_bm_accounts
    #                 LEFT OUTER JOIN
    #                     cu_meta_bm_account_details
    #                 ON
    #                     cu_meta_bm_accounts.id = cu_meta_bm_account_details.bm_account_id
    #                 LEFT OUTER JOIN
    #                     cu_advertiser_users
    #                 ON
    #                     cu_meta_bm_accounts.user_id = cu_advertiser_users.id
    #                 WHERE
    #                     cu_meta_bm_accounts.is_delete = false
    #                     {where_sql}
    #                 ORDER BY
    #                     cu_meta_bm_accounts.id DESC
    #             """
    #     df = pd.read_sql(sql=sql, con=engine)
    #     if df.empty:
    #         return MyResponse(code=RET.NO_DATA, msg=error_map.get(RET.NO_DATA))
    #     bio = BytesIO()
    #     writer = pd.ExcelWriter(bio, engine="xlsxwriter")
    #     df.to_excel(excel_writer=writer, index=False)
    #     writer.close()
    #     response = Response(bio.getvalue())
    #     response.headers["Content-Type"] = "application/vnd.ms-excel; charset=UTF-8"
    #     quto_file_name = quote("BM绑定广告账户", encoding="utf-8")
    #     response.headers["Content-Disposition"] = (
    #         f"attachment;filename={quto_file_name}.xlsx"
    #     )
    #     return response

    @AccountRouter.post("/bm_grant_type", description="BM账户授权类型")
    async def bm_accounts_grant_type(
            self, data: BmAccountGrantTypeSchema, db: Session = Depends(get_db)
    ):
        userid = self.request.state.user.user_id
        customer_ids = get_customer_ids(db, userid)
        account_ids = data.account_ids
        # 根据广告账户ID来判断投放方式
        account_ids = [account["account_id"] for account in account_ids]
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        is_second = get_is_second(userid)
        # 是子账户还是二代客户
        if user and user.p_id and is_second:
            no_allow_account = permission_check(userid, account_ids, OperationType.BMACCOUNT)
            if no_allow_account:
                return MyResponse(code=RET.INVALID_DATA, msg=f"{no_allow_account}账户未被授权此操作类型")
        json_ = {"account_ids": account_ids}
        res = CRMExternalService.get_account_customers(json_, **{'trace_id': self.request.state.trace_id})
        empty_customer_ids = [
            str(item["account_id"])
            for item in res.get("data")
            if item["customer_id"] is None
        ]
        joined_ids = ",".join(empty_customer_ids)
        if empty_customer_ids:
            return MyResponse(
                code=RET.DATA_ERR, msg=f"{joined_ids}账户不在系统中,请先录入"
            )
        accept_customer_ids = [str(item["customer_id"]) for item in res.get("data")]
        not_exists_in_customer_ids = [
            item for item in accept_customer_ids if int(item) not in customer_ids
        ]
        if not_exists_in_customer_ids:
            return MyResponse(
                code=RET.NO_DATA,
                msg="您输入的广告账户尚未获得授权，请联系我们的工作人员授权!",
            )
        res = CRMExternalService.get_account_put_way(json_, **{'trace_id': self.request.state.trace_id})
        if res.get("code") != RET.OK:
            return MyResponse(
                code=res.get("code"),
                msg="您输入的广告账户ID不是同一个投放方式，请重新输入",
            )
        type = res.get("data")
        # 仅报告
        if type == "代投":
            grant_type = {
                "desc": BMGrantType.ANALYZE.desc,
                "value": BMGrantType.ANALYZE.value,
            }
        # 一般用户
        if type == "自投":
            grant_type = {
                "desc": BMGrantType.ADVERTISE_ANALYZE.desc,
                "value": BMGrantType.ADVERTISE_ANALYZE.value,
            }
        return MyResponse(code=RET.OK, msg="返回BM账户授权类型成功", data=grant_type)

    @AccountRouter.post("/bm_operate", description="BM绑定/解绑")
    async def bm_accounts_operate(
            self, data: BMOperateSchemas, db: Session = Depends(get_db)
    ):
        userid = self.request.state.user.user_id
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        customer_ids = (
            db.query(UserCusRelationship.customer_id)
            .filter(
                UserCusRelationship.company_id == user.company_id,
                UserCusRelationship.is_delete == False,
            )
            .first()
        )
        # 如果是解绑就不需要拿广告账户ID查授权类型
        operate_type = data.operation
        business_id = data.business_id
        account_ids = data.account_ids
        grant_type = data.grant_type
        account_ids = [account["account_id"] for account in account_ids]
        account_ids_ = list(set(account_ids))
        if len(account_ids) != len(account_ids_):
            return MyResponse(code=RET.INVALID_DATA, msg=f"账户不允许重复！")
        is_second = get_is_second(userid)
        # 是子账户还是二代客户
        if user and user.p_id and is_second:
            no_allow_account = permission_check(userid, account_ids, OperationType.BMACCOUNT)
            if no_allow_account:
                return MyResponse(code=RET.INVALID_DATA, msg=f"{no_allow_account}账户未被授权此操作类型")
        json_ = {"account_ids": account_ids}
        business_ids = []
        business_ids.append(business_id)
        res = CRMExternalService.get_account_customers(json_, **{'trace_id': self.request.state.trace_id})
        empty_customer_ids = [
            str(item["account_id"])
            for item in res.get("data")
            if item["customer_id"] is None
        ]
        joined_ids = ",".join(empty_customer_ids)
        if empty_customer_ids:
            return MyResponse(
                code=RET.DATA_ERR, msg=f"{joined_ids}账户不在系统中,请先录入"
            )
        accept_customer_ids = [str(item["customer_id"]) for item in res.get("data")]
        not_exists_in_customer_ids = [
            item for item in accept_customer_ids if int(item) not in customer_ids[0]
        ]
        if not_exists_in_customer_ids:
            return MyResponse(
                code=RET.NO_DATA,
                msg="您输入的广告账户尚未获得授权，请联系我们的工作人员授权!",
            )
        medium_res = CRMExternalService.get_accounts_medium(json_, **{'trace_id': self.request.state.trace_id})
        medium_data = [item['medium'] for item in medium_res.get('data') if item['medium'] != 'Meta']
        if medium_data:
            return MyResponse(
                code=RET.DATA_ERR, msg="BM绑定/解绑仅支持meta广告账户"
            )
        # 如果操作类型是绑定
        if operate_type == Operation.BIND.value:
            bm_account = {
                "operate_type": operate_type.value,
                "business_ids": business_ids,
                "accounts": account_ids,
                "grant_type": grant_type.value,
            }
            try:
                # http回调接口
                request_id = APIService.post_bm_account(bm_account, **{'trace_id': self.request.state.trace_id})
                if not request_id:
                    return MyResponse(
                        code=RET.PARAM_ERR, msg="请重新输入要绑定的广告账户ID"
                    )
                new_bm = BmAccount(
                    request_id=request_id,
                    business_id=business_id,
                    grant_type=grant_type,
                    operate_type=operate_type,
                    user_id=userid,
                    operate_time=datetime.now(),
                )
                db.add(new_bm)
                db.flush()
                new_bm_detail_list = list()
                for i in account_ids:
                    new_bm_detail = BmAccountDetail(
                        account_id=i, bm_account_id=new_bm.id, remark=""
                    )
                    new_bm_detail_list.append(new_bm_detail)
                db.add_all(new_bm_detail_list)
            except Exception as e:
                web_log.log_error(
                    f"bm绑定广告账户调用失败原因：{e}"
                )
                return MyResponse(code=RET.DATA_ERR, msg="调用媒体接口失败")
            else:
                db.commit()
                mapi_request_result.delay(request_id)
            return MyResponse(code=RET.OK, msg="绑定成功")

        # 解绑
        if operate_type == Operation.UNBIND.value:
            bm_account = {
                "operate_type": operate_type.value,
                "business_ids": business_ids,
                "accounts": account_ids,
            }
            try:
                request_id = APIService.post_bm_account(bm_account, **{'trace_id': self.request.state.trace_id})
                if not request_id:
                    return MyResponse(
                        code=RET.PARAM_ERR, msg="请重新输入要绑定的广告账户ID"
                    )
                new_bm = BmAccount(
                    request_id=request_id,
                    business_id=business_id,
                    operate_type=operate_type,
                    user_id=userid,
                    operate_time=datetime.now(),
                )
                db.add(new_bm)
                db.flush()
                new_bm_detail_list = list()
                for i in account_ids:
                    new_bm_detail = BmAccountDetail(
                        account_id=i, bm_account_id=new_bm.id, remark=""
                    )
                    new_bm_detail_list.append(new_bm_detail)
                db.add_all(new_bm_detail_list)
            except Exception as e:
                web_log.log_error(
                    f"bm解绑广告账户调用失败原因：{e}"
                )
                return MyResponse(code=RET.DATA_ERR, msg="调用媒体接口失败")
            else:
                db.commit()
                mapi_request_result.delay(request_id)
            return MyResponse(code=RET.OK, msg="解绑成功")
        return MyResponse(code=RET.PARAM_ERR, msg="未输入操作类型")


# BC账户
@cbv(AccountRouter)
class BcAccountServer:
    request: Request

    @AccountRouter.get("/bc_accounts", description="BC账户列表")
    async def bc_accounts(
            self, common_query: CommonQueryParams = Depends(), db: Session = Depends(get_db)
    ):
        where = [BcAccount.is_delete == False]
        userid = self.request.state.user.user_id
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == userid, AdvertiserUser.is_delete == False
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
        if common_query.q:
            # 可以搜索Pixel_ID、提交人、account_id
            user_ids = db.query(AdvertiserUser.id).filter(AdvertiserUser.real_name.like(f'%{common_query.q}%')).all()
            user_ids_list = [row.id for row in user_ids]
            where.append(
                or_(
                    BcAccount.cooperative_id.like(f"%{common_query.q}%"),
                    BcAccount.user_id.in_(user_ids_list),
                    subquery.c.account_ids.like(f"%{common_query.q}%"),
                    subquery.c.business_ids.like(f"%{common_query.q}%"),
                )
            )
        query = (
            db.query(
                BcAccount,
                AdvertiserUser.real_name.label('username'),
                subquery.c.detail_count,
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
            )
            .outerjoin(subquery, BcAccount.id == subquery.c.tiktok_bc_account_id)
            .outerjoin(AdvertiserUser, AdvertiserUser.id == BcAccount.user_id)
            .filter(*where)
            .order_by(desc(BcAccount.id))
        )
        paginator = MyPagination(query, common_query.page, common_query.page_size)
        return MyResponse(total=paginator.counts, data=paginator.data)

    @AccountRouter.get("/bc_accounts_detail/{id}", description="BC账户列表详情")
    async def bc_accounts_detail(
            self,
            id: int,
            common_query: CommonQueryParams = Depends(),
            db: Session = Depends(get_db),
    ):
        """
        BC操作账户详情
        """
        user_id = self.request.state.user.user_id
        is_second = get_is_second(user_id)
        query = db.query(
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
            BcAccountDetail.remark,
        ).filter(
            BcAccountDetail.is_delete == False,
            BcAccountDetail.tiktok_bc_account_id == id,
        )
        paginator = MyPagination(query, common_query.page, common_query.page_size)
        res_data = paginator.data

        # 是二代客户
        user_pid = db.query(AdvertiserUser.p_id).filter(AdvertiserUser.id == user_id,
                                                        AdvertiserUser.is_delete == 0).scalar()
        is_primary = False if user_pid else True
        # 是二代客户主账户
        if is_second and is_primary:
            # 获取redis数据
            pipe_account_list, group_name_mapping = get_redis_account_group(res_data)

            # 赋值组名称
            for item, account_groups in zip(res_data, pipe_account_list):
                group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                group_name_str = ", ".join(group_names) or '-'
                item['group_name'] = group_name_str
        return MyResponse(total=paginator.counts, data=res_data, other_data={"is_second": is_second})

    # @AccountRouter.post("/bc_export", description="BC导出")
    # async def bc_accounts_export(self, data: BcExportSchemas, db: Session = Depends(get_db)):
    #     """
    #     导出BC账户xlsx文件
    #     """
    #     data = data.dict()
    #     start_date = data.get("start_date")
    #     end_date = data.get("end_date")
    #     cooperative_id = data.get("cooperative_id")
    #     userid = self.request.state.user.user_id
    #     where = []
    #     user = (
    #         db.query(AdvertiserUser)
    #         .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
    #         .first()
    #     )
    #     # 如果是主账号
    #     if not user.p_id:
    #         sub_user = db.query(AdvertiserUser).filter(
    #             AdvertiserUser.p_id == userid, AdvertiserUser.is_delete == False
    #         )
    #         sub_names = [i.id for i in sub_user.all()]
    #         sub_names.append(user.id)
    #         where.append(f"cu_tiktok_bc_accounts.user_id IN ({', '.join(str(id) for id in sub_names)})")
    #     # 如果是子账号
    #     else:
    #         where.append(f"cu_tiktok_bc_accounts.user_id = {user.id}")
    #     if start_date:
    #         where.append(
    #             f"date(cu_tiktok_bc_accounts.operate_time) >= date('{start_date}')"
    #         )
    #     if end_date:
    #         where.append(
    #             f"date(cu_tiktok_bc_accounts.operate_time) <= date('{end_date}')"
    #         )
    #     if cooperative_id:
    #         where.append(f"cu_tiktok_bc_accounts.cooperative_id = '{cooperative_id}'")
    #     where_sql = " AND " + " AND ".join(where) if where else ""
    #     sql = f"""
    #                 SELECT
    #                     cu_tiktok_bc_accounts.cooperative_id AS "合作伙伴ID",
    #                     cu_tiktok_bc_accounts_details.account_id AS "广告账户ID",
    #                     cu_tiktok_bc_accounts_details.business_id AS "商务中心ID",
    #                     CASE
    #                         WHEN cu_tiktok_bc_accounts.grant_type="{BCGrantType.ANALYST.value}" THEN "{BCGrantType.ANALYST.desc}"
    #                         WHEN cu_tiktok_bc_accounts.grant_type="{BCGrantType.OPERATOR.value}" THEN "{BCGrantType.OPERATOR.desc}"
    #                         ELSE ""
    #                     END AS "授权类型",
    #                     CASE
    #                         WHEN cu_tiktok_bc_accounts.operate_type="{Operation.BIND.value}" THEN "{Operation.BIND.desc}"
    #                         WHEN cu_tiktok_bc_accounts.operate_type="{Operation.UNBIND.value}" THEN "{Operation.UNBIND.desc}"
    #                         ELSE ""
    #                     END AS "操作类型",
    #                     CASE
    #                         WHEN cu_tiktok_bc_accounts_details.operate_result="{OperateResult.DEFAULT.value}" THEN "{OperateResult.DEFAULT.desc}"
    #                         WHEN cu_tiktok_bc_accounts_details.operate_result="{OperateResult.SUCCESS.value}" THEN "{OperateResult.SUCCESS.desc}"
    #                         WHEN cu_tiktok_bc_accounts_details.operate_result="{OperateResult.FAIL.value}" THEN "{OperateResult.FAIL.desc}"
    #                         ELSE ""
    #                     END AS "操作结果",
    #                     cu_tiktok_bc_accounts_details.operate_time AS "操作时间",
    #                     cu_tiktok_bc_accounts_details.remark AS "备注",
    #                     cu_advertiser_users.real_name AS "提交人"
    #                 FROM
    #                     cu_tiktok_bc_accounts
    #                 LEFT OUTER JOIN
    #                     cu_tiktok_bc_accounts_details
    #                 ON
    #                     cu_tiktok_bc_accounts_details.tiktok_bc_account_id = cu_tiktok_bc_accounts.id
    #                 LEFT OUTER JOIN
    #                     cu_advertiser_users
    #                 ON
    #                     cu_tiktok_bc_accounts.user_id = cu_advertiser_users.id
    #                 WHERE
    #                     cu_tiktok_bc_accounts.is_delete = false
    #                     {where_sql}
    #                 ORDER BY
    #                     cu_tiktok_bc_accounts.id DESC
    #                 """
    #     df = pd.read_sql(sql=sql, con=engine)
    #     if df.empty:
    #         return MyResponse(code=RET.NO_DATA, msg=error_map.get(RET.NO_DATA))
    #     bio = BytesIO()
    #     writer = pd.ExcelWriter(bio, engine="xlsxwriter")
    #     df.to_excel(excel_writer=writer, index=False)
    #     writer.close()
    #     response = Response(bio.getvalue())
    #     response.headers["Content-Type"] = "application/vnd.ms-excel; charset=UTF-8"
    #     quto_file_name = quote("BC绑定广告账户", encoding="utf-8")
    #     response.headers["Content-Disposition"] = (
    #         f"attachment;filename={quto_file_name}.xlsx"
    #     )
    #     return response

    @AccountRouter.post("/bc_put_way", description="BC账户访问权限")
    async def bc_accounts_grant_type(
            self, data: BmAccountGrantTypeSchema, db: Session = Depends(get_db)
    ):
        userid = self.request.state.user.user_id
        customer_ids = get_customer_ids(db, userid)
        account_ids = data.account_ids
        # 根据广告账户ID来判断投放方式
        account_ids = [account["account_id"] for account in account_ids]
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        is_second = get_is_second(userid)
        # 是子账户还是二代客户
        if user and user.p_id and is_second:
            no_allow_account = permission_check(userid, account_ids, OperationType.BCACCOUNT)
            if no_allow_account:
                return MyResponse(code=RET.INVALID_DATA, msg=f"{no_allow_account}账户未被授权此操作类型")
        json_ = {"account_ids": account_ids}
        res = CRMExternalService.get_account_customers(json_, **{'trace_id': self.request.state.trace_id})
        empty_customer_ids = [
            str(item["account_id"])
            for item in res.get("data")
            if item["customer_id"] is None
        ]
        joined_ids = ",".join(empty_customer_ids)
        if empty_customer_ids:
            return MyResponse(
                code=RET.DATA_ERR, msg=f"{joined_ids}账户不在系统中,请先录入"
            )
        accept_customer_ids = [str(item["customer_id"]) for item in res.get("data")]
        not_exists_in_customer_ids = [
            item for item in accept_customer_ids if int(item) not in customer_ids
        ]
        if not_exists_in_customer_ids:
            return MyResponse(
                code=RET.NO_DATA,
                msg="您输入的广告账户尚未获得授权，请联系我们的工作人员授权!",
            )
        res = CRMExternalService.get_account_put_way(json_, **{'trace_id': self.request.state.trace_id})
        if res.get("code") != RET.OK:
            return MyResponse(
                code=res.get("code"),
                msg="您输入的广告账户ID不是同一个投放方式，请重新输入",
            )
        type = res.get("data")
        # 仅报告
        if type == "代投":
            grant_type = {
                "desc": BCGrantType.ANALYST.desc,
                "value": BCGrantType.ANALYST.value,
            }
        # 一般用户
        if type == "自投":
            grant_type = {
                "desc": BCGrantType.OPERATOR.desc,
                "value": BCGrantType.OPERATOR.value,
            }
        return MyResponse(code=RET.OK, msg="返回BC访问权限成功", data=grant_type)

    @AccountRouter.post("/bc_operate", description="BC解绑/绑定")
    async def bc_accounts_operate(
            self, data: BCOperateSchemas, db: Session = Depends(get_db)
    ):
        userid = self.request.state.user.user_id
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        customer_ids = (
            db.query(UserCusRelationship.customer_id)
            .filter(
                UserCusRelationship.company_id == user.company_id,
                UserCusRelationship.is_delete == False,
            )
            .first()
        )
        operate_type = data.operation
        cooperative_id = data.cooperative_id
        account_ids = data.account_ids
        grant_type = data.grant_type
        # 获取输入的所有广告账户id
        account_ids = [account["account_id"] for account in account_ids]
        account_ids_ = list(set(account_ids))
        if len(account_ids) != len(account_ids_):
            return MyResponse(code=RET.INVALID_DATA, msg=f"账户不允许重复！")
        is_second = get_is_second(userid)
        # 是子账户还是二代客户
        if user and user.p_id and is_second:
            no_allow_account = permission_check(userid, account_ids, OperationType.BCACCOUNT)
            if no_allow_account:
                return MyResponse(code=RET.INVALID_DATA, msg=f"{no_allow_account}账户未被授权此操作类型")
        json_ = {"account_ids": account_ids}
        res = CRMExternalService.get_account_customers(json_, **{'trace_id': self.request.state.trace_id})
        if not res.get("data"):
            return MyResponse(code=RET.NO_DATA, msg=error_map.get(RET.NO_DATA))
        # 判断广告账户id在crm系统是否存在
        empty_customer_ids = [
            str(item["account_id"])
            for item in res.get("data")
            if item["customer_id"] is None
        ]
        joined_ids = ",".join(empty_customer_ids)
        if empty_customer_ids:
            return MyResponse(
                code=RET.DATA_ERR, msg=f"{joined_ids}账户不在系统中,请先录入"
            )
        # 获取所有的客户id
        accept_customer_ids = [str(item["customer_id"]) for item in res.get("data")]
        # 是否存在未授权给该用户的客户
        not_exists_in_customer_ids = [
            item for item in accept_customer_ids if int(item) not in customer_ids[0]
        ]
        if not_exists_in_customer_ids:
            return MyResponse(
                code=RET.NO_DATA,
                msg="您输入的广告账户尚未获得授权，请联系我们的工作人员授权!",
            )
        # 获取所有的广告账户id
        account_list = [{"account_id": i} for i in account_ids]
        account_res = CRMExternalService.post_account_info(
            json={"accounts": account_list},
            **{'trace_id': self.request.state.trace_id}
        )
        medium_data = [item['medium'] for item in account_res.get('data') if item['medium'] != 'Tiktok']
        if medium_data:
            return MyResponse(
                code=RET.DATA_ERR, msg="BC绑定/解绑仅支持tiktok广告账户"
            )
        # 如果操作类型是绑定
        if operate_type == Operation.BIND.value:
            partner_data = []
            account_data = []
            for item in account_res.get('data'):
                account_data.append(item['account_id'])
                partner_data.append({
                    "bc_id": item['bc_id'],
                    "partner_id": cooperative_id,
                    "asset_ids": account_data,
                    "advertiser_role": (
                        "ANALYST"
                        if grant_type == BCGrantType.ANALYST.value
                        else "OPERATOR"
                    )
                })
            bc_account = {
                "operate_type": operate_type.value,
                "partner_data": partner_data,
            }
            try:
                request_id = APIService.post_bc_account(bc_account, **{'trace_id': self.request.state.trace_id})
                if not request_id:
                    return MyResponse(
                        code=RET.PARAM_ERR, msg="请重新输入要绑定的广告账户ID"
                    )
                new_bc = BcAccount(
                    request_id=request_id,
                    cooperative_id=cooperative_id,
                    grant_type=grant_type,
                    operate_type=operate_type,
                    user_id=userid,
                    operate_time=datetime.now(),
                )
                db.add(new_bc)
                db.flush()
                new_bc_detail_list = list()
                account_info = [{"account_id": item['account_id'], "bc_id": item['bc_id']} for item in
                                account_res.get('data')]
                for i in account_info:
                    new_bc_detail = BcAccountDetail(
                        account_id=i['account_id'], tiktok_bc_account_id=new_bc.id, remark="", business_id=i['bc_id'],
                    )
                    new_bc_detail_list.append(new_bc_detail)
                db.add_all(new_bc_detail_list)
            except Exception as e:
                web_log.log_error(
                    f"bc绑定广告账户调用失败原因：{e}"
                )
                return MyResponse(code=RET.DATA_ERR, msg="调用媒体接口失败")
            else:
                db.commit()
                mapi_request_result.delay(request_id)
            return MyResponse(code=RET.OK, msg="绑定成功")
        # 解绑 如果是解绑就不需要拿广告账户ID查授权类型
        if operate_type == Operation.UNBIND.value:
            partner_data = []
            account_data = []
            for item in account_res.get('data'):
                account_data.append(item['account_id'])
                partner_data.append({
                    "bc_id": item['bc_id'],
                    "partner_id": cooperative_id,
                    "asset_ids": account_data,
                })
            bc_account = {
                "operate_type": operate_type.value,
                "partner_data": partner_data
            }
            try:
                request_id = APIService.post_bc_account(bc_account, **{'trace_id': self.request.state.trace_id})
                if not request_id:
                    return MyResponse(
                        code=RET.PARAM_ERR, msg="请重新输入要绑定的广告账户ID"
                    )
                new_bc = BcAccount(
                    request_id=request_id,
                    cooperative_id=cooperative_id,
                    operate_type=operate_type,
                    user_id=userid,
                    operate_time=datetime.now(),
                )
                db.add(new_bc)
                db.flush()
                new_bc_detail_list = list()
                account_info = [{"account_id": item['account_id'], "bc_id": item['bc_id']} for item in
                                account_res.get('data')]
                for i in account_info:
                    new_bc_detail = BcAccountDetail(
                        account_id=i['account_id'], tiktok_bc_account_id=new_bc.id, remark="", business_id=i['bc_id'],
                    )
                    new_bc_detail_list.append(new_bc_detail)
                db.add_all(new_bc_detail_list)
            except Exception as e:
                web_log.log_error(
                    f"bc解绑广告账户调用失败原因：{e}"
                )
                return MyResponse(code=RET.DATA_ERR, msg="调用媒体接口失败")
            else:
                db.commit()
                mapi_request_result.delay(request_id)
            return MyResponse(code=RET.OK, msg="解绑成功")
        return MyResponse(code=RET.PARAM_ERR, msg="未输入操作类型")


# 账户列表
@cbv(AccountRouter)
class AccountManageServer:
    request: Request

    # 账户列表
    @AccountRouter.get("/accounts", description="账户列表")
    async def GetAccountManage(
            self,
            db: Session = Depends(get_db),
            medium: str = Query(None),
            account_status: str = Query(None),
            start_date: str = Query(None),
            end_date: str = Query(None),
            start_spend_date: str = Query(None),
            end_spend_date: str = Query(None),
            common_query: CommonQueryParams = Depends(),
    ):
        user_id = self.request.state.user.user_id
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id,
                                               AdvertiserUser.is_delete == False).first()
        res_id = user.p_id or user_id
        is_primary = False if user.p_id else True
        res_user = db.query(AdvertiserRegister).filter(AdvertiserRegister.user_id == res_id,
                                                       AdvertiserRegister.is_delete == False).first()
        is_second = res_user.is_second if res_user else False
        group_list = []
        if res_user and res_user.is_second and user.p_id:
            groups = db.query(GroupMemberRelationship.project_group_id
                              ).filter(GroupMemberRelationship.user_id == user_id,
                                       GroupMemberRelationship.is_delete == 0).all()
            group_list = [group_id for (group_id,) in groups]

        customer_ids = get_customer_ids(db, user_id)
        json_ = {
            "customer_ids": customer_ids,
            "page": common_query.page,
            "page_size": common_query.page_size,
            "q": common_query.q,
            "is_second": is_second,
            "is_primary": is_primary,
            "group_id": group_list,
            "medium": medium
        }
        if all([start_date, end_date]):
            json_["start_date"] = start_date
            json_["end_date"] = end_date
        if all([start_spend_date, end_spend_date]):
            json_["start_spend_date"] = start_spend_date
            json_["end_spend_date"] = end_spend_date
        if account_status and account_status != "全部":
            json_["account_status"] = custom_account_status_object.get(account_status.upper(), [])
        try:
            crm_result = CRMExternalService.get_accounts(
                json_,
                **{'trace_id': self.request.state.trace_id}
            )
            account_info = crm_result["data"]

            # 是二代客户主账户
            if is_second and is_primary:
                # 获取redis数据
                pipe_account_list, group_name_mapping = get_redis_account_group(account_info)

                # 赋值组名称
                for item, account_groups in zip(account_info, pipe_account_list):
                    group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                    group_name_str = ", ".join(group_names) or '-'
                    item['group_name'] = group_name_str
                    account_status = item.get("account_status", "-")
                    item["account_status"] = medium_account_status_object.get(account_status.upper(), account_status)
            #  不是二代客户
            else:
                for i in account_info:
                    account_status = i.get("account_status", "-")
                    i["account_status"] = medium_account_status_object.get(account_status.upper(), account_status)
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg='账户列表异常', total=0, data=[], other_data={"is_second": False})
        return MyResponse(code=crm_result['code'], msg=crm_result['msg'], data=account_info, total=crm_result["total"],
                          other_data={"is_second": True if (is_second and is_primary) else False})

    # 账户列表导出
    # @AccountRouter.post("/export", description="账户列表导出")
    # async def AccountManageExport(
    #         self, data: ExportAccountManage, db: Session = Depends(get_db)
    # ):
    #     user_id = self.request.state.user.user_id
    #     user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id,
    #                                            AdvertiserUser.is_delete == False).first()
    #     res_id = user.p_id if user.p_id else user_id
    #     res_user = db.query(AdvertiserRegister).filter(AdvertiserRegister.user_id == res_id,
    #                                                    AdvertiserRegister.is_delete == False).first()
    #     customer_ids = get_customer_ids(db, user_id)
    #     json_ = {
    #         "customer_ids": customer_ids
    #     }
    #     if data.medium:
    #         json_["medium"] = data.medium
    #     if data.start_date:
    #         json_["start_date"] = data.start_date
    #     if data.end_date:
    #         json_["end_date"] = data.end_date
    #     result = CRMExternalService.get_accounts(json_, headers={"advertiser_user_id": str(user_id)})
    #     if not result["data"]["account_infos"]:
    #         return MyResponse(code=RET.NO_DATA, msg=error_map.get(RET.NO_DATA))
    #     df = pd.DataFrame(result["data"]["account_infos"])
    #     df["account_status"] = df["account_status"].map(medium_account_status_object)
    #     df.drop(columns=["id"], inplace=True)
    #     df.rename(
    #         columns={
    #             "customer_name": "结算名称",
    #             "open_subject": "开户主体",
    #             "account_id": "广告账户ID",
    #             "account_name": "广告账户名称",
    #             "account_status": "账户状态",
    #             "medium": "投放媒介",
    #             "put_way_name": "投放方式",
    #             "yesterday_spend": "昨日消耗",
    #             "available_balance": "账户余额($)",
    #             "amount_spent": "已花费金额($)",
    #             "spend_cap": "花费上限($)/总预算($)",
    #             "open_date": "开户时间",
    #             "update_time": "更新时间",
    #         },
    #         inplace=True,
    #     )
    #     excel_file = BytesIO()
    #     df.to_excel(excel_file, index=False)
    #     excel_file.seek(0)
    #     return StreamingResponse(
    #         excel_file,
    #         media_type="application/vnd.ms-excel",
    #         headers={"Content-Disposition": "attachment; filename=account_manage.xlsx"},
    #     )

    # 获取媒介
    @AccountRouter.get("/mediums", description="获取媒介")
    async def GetMediums(self, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id,
                                               AdvertiserUser.is_delete == False).first()
        res_id = user.p_id if user.p_id else user_id
        res_user = db.query(AdvertiserRegister).filter(AdvertiserRegister.user_id == res_id,
                                                       AdvertiserRegister.is_delete == False).first()
        customer_ids = get_customer_ids(db, user_id)
        json_ = {
            "customer_ids": customer_ids,
            "group_id": [],
            "is_second": 0
        }
        result = CRMExternalService.get_account_mediums(
            json_,
            headers={"advertiser_user_id": str(user_id)},
            **{'trace_id': self.request.state.trace_id}
        )
        data = result["data"]
        if res_user and user:
            if res_user.is_second and user.p_id:
                # 获取组id
                group_member = db.query(GroupMemberRelationship.project_group_id
                                        ).filter(GroupMemberRelationship.user_id == user_id,
                                                 GroupMemberRelationship.is_delete == 0)
                # 取二代客户子账户媒介
                groups = db.query(ProjectGroup.mediums).filter(ProjectGroup.id.in_(group_member),
                                                               ProjectGroup.is_delete == 0).all()
                # 提取媒介并去重
                medium_type_list = {medium for group in groups for medium in group["mediums"] if
                                    isinstance(medium, str)}
                data = [{"label": i, "value": i} for i in medium_type_list]
        return MyResponse(data=data)

    @AccountRouter.get("/account_medium", description="账户列表下投放媒介")
    async def get_account_medium(
            self,
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        # 查询用户信息
        user = db.query(AdvertiserUser).filter(
            AdvertiserUser.id == user_id,
            AdvertiserUser.is_delete == False
        ).first()
        res_id = user.p_id if user.p_id else user_id
        is_primary = False if user.p_id else True

        # 查询注册信息
        res_user = db.query(AdvertiserRegister).filter(
            AdvertiserRegister.user_id == res_id,
            AdvertiserRegister.is_delete == False
        ).first()

        is_second = res_user.is_second if res_user else False

        # 获取用户组
        group_list = []
        if res_user and res_user.is_second and user.p_id:
            groups = db.query(GroupMemberRelationship.project_group_id).filter(
                GroupMemberRelationship.user_id == user_id,
                GroupMemberRelationship.is_delete == 0
            ).all()
            group_list = [group_id for (group_id,) in groups]

        # 获取客户ID
        customer_ids = get_customer_ids(db, user_id)

        json_payload = {
            "customer_ids": customer_ids,
            "is_second": is_second,
            "is_primary": is_primary,
            "group_id": group_list
        }
        # 调用CRM服务并处理异常
        try:
            crm_result = CRMExternalService.post_account_export(
                json_payload,
                **{'trace_id': self.request.state.trace_id}
            )
            account_info = crm_result.get("data", [])
            all_medium = Medium.values()
            # 媒介按照指定顺序先后排序
            medium_set = sorted({i.get("medium") for i in account_info if i.get("medium") in all_medium},
                                key=lambda x: all_medium.index(x))
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg="获取账户列表媒介异常", data=[])
        return MyResponse(code=crm_result['code'], msg=crm_result['msg'], data=list(medium_set))

    @AccountRouter.get("/account_batch", description="账户批量提交列表")
    async def get_batch_account(
            self,
            db: Session = Depends(get_db),
            medium: str = Query(...),
            account_ids: str = Query([]),
            account_status: str = Query(None),
            open_subject_name: str = Query(None),
            common_query: CommonQueryParams = Depends(),
    ):
        user_id = self.request.state.user.user_id
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id,
                                               AdvertiserUser.is_delete == False).first()
        res_id = user.p_id or user_id
        res_user = db.query(AdvertiserRegister).filter(AdvertiserRegister.user_id == res_id,
                                                       AdvertiserRegister.is_delete == False).first()
        is_second = res_user.is_second if res_user else False
        group_list = []
        if res_user and res_user.is_second and user.p_id:
            groups = db.query(GroupMemberRelationship.project_group_id
                              ).filter(GroupMemberRelationship.user_id == user_id,
                                       GroupMemberRelationship.is_delete == 0).all()
            group_list = [group_id for (group_id,) in groups]

        customer_ids = get_customer_ids(db, user_id)
        json_ = {
            "customer_ids": customer_ids,
            "page": common_query.page,
            "page_size": common_query.page_size,
            "is_second": is_second,
            "medium": medium,
            "is_primary": False if user.p_id else True,
            "group_id": group_list,
            "account_ids": json.loads(account_ids) if account_ids else [],
            "open_subject_name": open_subject_name
        }
        if account_status and account_status != "全部":
            json_["account_status"] = custom_account_status_object.get(account_status.upper(), [])

        try:
            crm_result = CRMExternalService.post_account_batch(
                json_,
                **{'trace_id': self.request.state.trace_id}
            )
            account_info = crm_result.get("data", [])

            for i in account_info:
                account_status = i.get("account_status", "-")
                i["account_status"] = medium_account_status_object.get(account_status.upper(), account_status)
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg="账户批量提交列表异常", total=0, data=[])
        return MyResponse(code=crm_result.get('code'), msg=crm_result.get('msg'),
                          total=crm_result.get("total"), data=account_info)

    @AccountRouter.get("/regular_number", description="常用充值金额")
    async def regular_number(self):
        data = [{"label": i, "value": i} for i in [50, 100, 200, 300, 500, 1000]]
        return MyResponse(data=data)

    @AccountRouter.get("/customer_purses", description="客户钱包余额")
    async def get_customer_balance(self, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        customer_ids = get_customer_ids(db, user_id)
        json_ = {"customer_ids": customer_ids}
        try:
            result = CRMExternalService.post_customer_purse(
                json_,
                **{'trace_id': self.request.state.trace_id}
            )
            account_info = result.get("data", [])
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg='客户钱包余额异常', data=[])
        return MyResponse(code=result.get('code'), msg=result.get('msg'), data=account_info)

    @AccountRouter.get("/account_search", description="广告账户搜索")
    async def get_account_search(
            self,
            db: Session = Depends(get_db),
            medium: str = Query(...),
            account_id: str = Query(None),
    ):
        user_id = self.request.state.user.user_id
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id,
                                               AdvertiserUser.is_delete == False).first()
        res_id = user.p_id or user_id
        res_user = db.query(AdvertiserRegister).filter(AdvertiserRegister.user_id == res_id,
                                                       AdvertiserRegister.is_delete == False).first()
        is_second = res_user.is_second if res_user else False
        group_list = []
        if res_user and res_user.is_second and user.p_id:
            groups = db.query(GroupMemberRelationship.project_group_id
                              ).filter(GroupMemberRelationship.user_id == user_id,
                                       GroupMemberRelationship.is_delete == 0).all()
            group_list = [group_id for (group_id,) in groups]

        customer_ids = get_customer_ids(db, user_id)
        json_ = {
            "customer_ids": customer_ids,
            "is_second": is_second,
            "medium": medium,
            "account_id": account_id,
            "is_primary": False if user.p_id else True,
            "group_id": group_list,
        }
        try:
            crm_result = CRMExternalService.post_account_search(
                json_,
                **{'trace_id': self.request.state.trace_id}
            )
            data = [{"label": i.get("account_id"), "value": i.get("account_id")} for i in crm_result.get("data", [])]
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg="广告账户搜索异常", data=[])
        return MyResponse(code=crm_result.get('code'), msg=crm_result.get('msg'), data=data)

    @AccountRouter.get("/account_info", description="广告账户匹配信息")
    async def get_account_matching_info(
            self,
            db: Session = Depends(get_db),
            medium: str = Query(...),
            account_id: str = Query(...),
            common_query: CommonQueryParams = Depends()
    ):
        user_id = self.request.state.user.user_id
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id,
                                               AdvertiserUser.is_delete == False).first()
        res_id = user.p_id or user_id
        res_user = db.query(AdvertiserRegister).filter(AdvertiserRegister.user_id == res_id,
                                                       AdvertiserRegister.is_delete == False).first()
        is_second = res_user.is_second if res_user else False
        group_list = []
        if res_user and res_user.is_second and user.p_id:
            groups = db.query(GroupMemberRelationship.project_group_id
                              ).filter(GroupMemberRelationship.user_id == user_id,
                                       GroupMemberRelationship.is_delete == 0).all()
            group_list = [group_id for (group_id,) in groups]

        customer_ids = get_customer_ids(db, user_id)
        json_ = {
            "page": common_query.page,
            "page_size": common_query.page_size,
            "customer_ids": customer_ids,
            "is_second": is_second,
            "is_primary": False if user.p_id else True,
            "group_id": group_list,
            "account_ids": [],
            "medium": medium,
            "account_id": account_id,
            "open_subject_name": ""
        }
        try:
            crm_result = CRMExternalService.post_account_batch(
                json_,
                **{'trace_id': self.request.state.trace_id}
            )
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg="广告账户匹配信息异常", data=[])
        return MyResponse(code=crm_result.get('code'), msg=crm_result.get('msg'), data=crm_result.get("data", []))


# 个人中心
@cbv(AccountRouter)
class PersonalCenterServer:
    request: Request

    # 个人中心个人资料
    @AccountRouter.get("/personal_data", description="个人中心个人资料")
    async def get_personal_data(self, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == user_id)
            .first()
        )
        auth_count = 0
        if user.company_id:
            total_count_result = (
                db.query(UserCusRelationship)
                .filter(UserCusRelationship.company_id == user.company_id)
                .first()
            )
            if total_count_result:
                auth_count = (
                    db.query(
                        func.concat(
                            func.count(AdvertiserUser.id),
                            "/",
                            total_count_result.auth_num,
                        )
                    )
                    .filter(AdvertiserUser.p_id == user_id)
                    .scalar()
                )
        personal_data = (
            db.query(
                AdvertiserUser.mobile.label("mobile"),
                AdvertiserRegister.company_name.label("company_name"),
                AdvertiserRegister.is_second.label("is_second"),
                AdvertiserUser.real_name.label("real_name"),
                AdvertiserUser.email.label("email"),
                AdvertiserUser.avatar_url.label("avatar_url"),
            )
            .filter(AdvertiserUser.id == user_id)
            .outerjoin(
                AdvertiserRegister,
                and_(
                    AdvertiserRegister.mobile == AdvertiserUser.mobile,
                    AdvertiserRegister.status == RegisterStatus.AGREE.value,
                    AdvertiserRegister.is_delete == False,
                ),
            )
            .first()
        )
        data = row_dict(personal_data)
        data.update({"auth_count": auth_count, "is_main": True})
        # 如果是子账号
        if user.p_id:
            user_p = (
                db.query(AdvertiserRegister)
                .filter(
                    AdvertiserRegister.user_id == user.p_id,
                    AdvertiserRegister.is_delete == 0,
                )
                .first()
            )
            if user_p:
                if user_p.company_name:
                    data.update({"company_name": user_p.company_name, "is_main": False})
                # 如果是二代客户
                if user_p.is_second:
                    # 获取组id
                    group_member = db.query(GroupMemberRelationship.project_group_id
                                            ).filter(GroupMemberRelationship.user_id == user_id,
                                                     GroupMemberRelationship.is_delete == 0)
                    # 取项目组名称
                    groups = db.query(ProjectGroup.project_name).filter(ProjectGroup.id.in_(group_member),
                                                                        ProjectGroup.is_delete == 0).all()
                    project_name_list = [project_name for (project_name,) in groups]
                    data.update({"group_name": project_name_list, "is_second": True})
        return MyResponse(data=data)

    # 个人中心基本信息
    @AccountRouter.get("/basic_account", description="个人中心基本信息")
    async def GetBasicAccount(self, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        customer_ids = get_customer_ids(db, user_id)
        json_ = {"customer_ids": customer_ids}
        result = CRMExternalService.basic_info_list(json_, **{'trace_id': self.request.state.trace_id})["data"]
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False).first()
        # 如果是主账号
        result['is_main'] = 1
        if user.p_id:
            result["is_main"] = 0
        return MyResponse(data=result)

    # 个人中心详细信息
    @AccountRouter.get("/open_account_detail", description="个人中心详细信息")
    async def GetOpenAccountDetail(self, pk: int):
        params = {"pk": pk}
        response = CRMExternalService.detail_info_list(params, **{'trace_id': self.request.state.trace_id})
        data_list = []
        for item in response.get("data"):
            data_dict = {
                "id": item.get("id"),
                "name": item.get("name"),
                "rebate_balance": (
                    round(item.get("rebate_balance"), 2)
                    if item.get("rebate_balance")
                    else 0
                ),
                "receivable_balance": (
                    round(item.get("receivable_balance"), 2)
                    if item.get("receivable_balance")
                    else 0
                ),
                "total_limit": (
                    round(item.get("total_limit"), 2) if item.get("total_limit") else 0
                ),
                "used_limit": (
                    round(item.get("used_limit"), 2) if item.get("used_limit") else 0
                ),
                "cn_pay_way": item.get("cn_pay_way"),
                "pay_way": item.get("pay_way"),
                "contract_date_end": item.get("contract_date_end"),
                "sell_name": item.get("sell_name"),
                "payment_days": item.get("payment_days"),
                "date_month": item.get("date_month"),
                "available_balance": (
                    round(item.get("available_balance"), 2)
                    if item.get("available_balance")
                    else 0
                ),
                "actual_balance": (
                    round(item.get("actual_balance"), 2)
                    if item.get("actual_balance")
                    else 0
                ),
                "remark": item.get("remark"),
                "remain_balance": (
                    round(item.get("remain_balance"), 2)
                    if item.get("remain_balance")
                    else 0
                ),
                "total_spend": (
                    round(item.get("total_spend"), 2) if item.get("total_spend") else 0
                ),
            }
            data_list.append(data_dict)
        return MyResponse(data=data_list)


# 开户历史
@cbv(AccountRouter)
class OeOpenAccountServer:
    request: Request

    @AccountRouter.get("/oe_accounts", description="开户列表")
    async def get_oe_account(
            self,
            db: Session = Depends(get_db),
            oe_status: str = Query(None),
            account_status: str = Query(None),
            start_date: str = Query(None),
            end_date: str = Query(None),
            common_query: CommonQueryParams = Depends(),
    ):
        where = [OeOpenAccount.is_delete == 0]
        user_id = self.request.state.user.user_id
        customer_ids = get_customer_ids(db, user_id)
        # 测试数据
        # customer_ids = [3410]
        where.append(OeOpenAccount.customer_id.in_(customer_ids))
        if common_query.q:
            where.append(or_(OeOpenAccount.oe_number.like(f'%{common_query.q}%'),
                             OeOpenAccount.chinese_legal_entity_name.like(f'%{common_query.q}%')))
        if oe_status:
            where.append(OeOpenAccount.approval_status == oe_status)

        if account_status:
            where.append(OeOpenAccount.ad_account_creation_request_status == account_status)

        if all([start_date, end_date]):
            where.extend([func.date(OeOpenAccount.created_time) >= start_date,
                          func.date(OeOpenAccount.created_time) <= end_date])

        query = db.query(OeOpenAccount.id,
                         OeOpenAccount.oe_number,
                         OeOpenAccount.created_time,
                         OeOpenAccount.chinese_legal_entity_name,
                         OeOpenAccount.approval_status,
                         case(
                             [
                                 (OeOpenAccount.ad_account_creation_request_status == OeAccountStatus.EMPTY.desc, "-")
                             ],
                             else_=OeOpenAccount.ad_account_creation_request_status
                         ).label("ad_account_creation_request_status")
                         ).filter(*where).order_by(OeOpenAccount.id.desc())

        obj = MyPagination(query, common_query.page, common_query.page_size)
        return MyResponse(data=obj.data, total=obj.counts)

    @AccountRouter.get("/oe_account_detail/{id}", description="开户详情")
    async def get_oe_account_detail(self, id: int, db: Session = Depends(get_db)):
        oe_obj = db.query(OeOpenAccount.ad_accounts).filter(OeOpenAccount.id == id,
                                                            OeOpenAccount.is_delete == 0
                                                            ).first()
        if not oe_obj:
            return MyResponse(code=RET.NO_DATA, msg="详情不存在！")
        data = oe_obj.ad_accounts
        return MyResponse(data=data)

    @AccountRouter.get("/oe_status", description="OE审批状态")
    async def get_oe_approval_status(self):
        data = [{"label": i,
                 "en_label": oe_approve_status_english.get(i),
                 "value": i} for i in OeApproveStatus.descs()]
        return MyResponse(data=data)

    @AccountRouter.get("/oe_account_status", description="OE账户状态")
    async def get_oe_account_status(self):
        unique_values = sorted(set(OeAccountStatus.descs()), key=lambda x: len(x))
        data = [{"label": i,
                 "en_label": oe_account_status_english.get(i),
                 "value": i} for i in unique_values if i]
        return MyResponse(data=data)
