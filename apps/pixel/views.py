# -*- coding: utf-8 -*-
from datetime import datetime
from fastapi import APIRouter
from fastapi import Depends, Query
from fastapi_utils.cbv import cbv
from sqlalchemy import case, or_, func, desc
from sqlalchemy.orm import Session
from starlette.requests import Request
from apps.common.utils import get_is_second, get_redis_account_group, permission_check
from apps.pixel.define import AdvertiserStatusResult, OperateResult
from apps.pixel.models import PixelAccount, PixelAccountDetail
from apps.pixel.schemas import (
    OperationPixelSchemas
)
from apps.advertiser.models import (
    UserCusRelationship,
    AdvertiserUser,
)
from settings.db import get_db, MyPagination
from tools.common import CommonQueryParams
from tools.constant import RET, Operation, OperationType
from tools.resp import MyResponse
from settings.log import web_log
from libs.internal.crm_external_service import CRMExternalService
from libs.internal.api_service import APIService
from apps.callback.tasks import mapi_request_result

PixelRouter = APIRouter(tags=["pixel"])


# Pixel操作(广告账户)
@cbv(PixelRouter)
class PixelServer:
    request: Request

    @PixelRouter.get('/pixel_accounts', description='Pixel账户列表')
    async def pixel_accounts(self, common_query: CommonQueryParams = Depends(),
                             start_date: str = Query(None, regex='^[\d]{4}-[\d]{2}-[\d]{2}$'),
                             end_date: str = Query(None, regex='^[\d]{4}-[\d]{2}-[\d]{2}$'),
                             db: Session = Depends(get_db)):
        # 广告账户展示全部成功，全部失败，部分成功
        where = [PixelAccount.is_delete == False]
        userid = self.request.state.user.user_id
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == userid,
                                               AdvertiserUser.is_delete == False).first()
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(AdvertiserUser.p_id == userid,
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
        if common_query.q:
            # 可以搜索Pixel_ID、提交人、account_id
            user_ids = db.query(AdvertiserUser.id).filter(AdvertiserUser.real_name.like(f'%{common_query.q}%')).all()
            user_ids_list = [row.id for row in user_ids]
            where.append(
                or_(PixelAccount.pixel_id.like(f'%{common_query.q}%'),
                    PixelAccount.user_id.in_(user_ids_list),
                    subquery.c.account_ids.like(f'%{common_query.q}%')))
        query = (db.query(
            PixelAccount,
            subquery.c.detail_count,
            AdvertiserUser.real_name.label("username"),
            case([
                (PixelAccount.operate_result == AdvertiserStatusResult.DEFAULT.value,
                 AdvertiserStatusResult.DEFAULT.desc),
                (PixelAccount.operate_result == AdvertiserStatusResult.ALL_FAIL.value,
                 AdvertiserStatusResult.ALL_FAIL.desc),
                (PixelAccount.operate_result == AdvertiserStatusResult.ALL_SUCCEED.value,
                 AdvertiserStatusResult.ALL_SUCCEED.desc),
                (PixelAccount.operate_result == AdvertiserStatusResult.PART.value,
                 AdvertiserStatusResult.PART.desc),
            ], else_='').label("cn_operate_result"),
            case([
                (PixelAccount.operate_type == Operation.BIND.value, Operation.BIND.desc),
                (PixelAccount.operate_type == Operation.UNBIND.value, Operation.UNBIND.desc)
            ], else_='').label("cn_operate_type")
        ).outerjoin(
            subquery,
            PixelAccount.id == subquery.c.pixel_account_id
        ).outerjoin(AdvertiserUser, AdvertiserUser.id == PixelAccount.user_id).filter(*where
                                                                                      ).order_by(desc(PixelAccount.id)))
        paginator = MyPagination(query, common_query.page, common_query.page_size)
        return MyResponse(total=paginator.counts, data=paginator.data)

    @PixelRouter.get('/pixel_accounts_detail/{id}', description='Pixel绑定广告账户详情')
    async def bc_accounts_detail(self, id: int, common_query: CommonQueryParams = Depends(),
                                 db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        is_second = get_is_second(user_id)
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
                (PixelAccountDetail.remark == "", "-"),
                (PixelAccountDetail.remark == None, "-")
            ], else_=PixelAccountDetail.remark).label("remark")
        ).filter(PixelAccountDetail.is_delete == False, PixelAccountDetail.pixel_account_id == id)
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

    @PixelRouter.post("/pixel_operations", description="Pixel解绑/绑定")
    async def OperationPixel(
            self, data: OperationPixelSchemas, db: Session = Depends(get_db)
    ):
        userid = self.request.state.user.user_id
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == userid,
                                               AdvertiserUser.is_delete == False).first()
        customer_ids = db.query(UserCusRelationship.customer_id).filter(
            UserCusRelationship.company_id == user.company_id,
            UserCusRelationship.is_delete == False).first()
        operation = data.operation
        pixel_id = data.pixel_id
        data = data.dict()
        property_ids = data.pop("account_ids")
        property_ids = [account['account_id'] for account in property_ids]
        property_ids_ = list(set(property_ids))
        if len(property_ids) != len(property_ids_):
            return MyResponse(code=RET.INVALID_DATA, msg=f"账户不允许重复！")
        is_second = get_is_second(userid)
        # 是子账户还是二代客户
        if user and user.p_id and is_second:
            no_allow_account = permission_check(userid, property_ids, OperationType.PIXEL)
            if no_allow_account:
                return MyResponse(code=RET.INVALID_DATA, msg=f"{no_allow_account}账户未被授权此操作类型")
        json_ = {"account_ids": property_ids}
        pixel_data = {'operate_type': operation.value, 'pixel_ids': [pixel_id], 'account_ids': property_ids}
        res = CRMExternalService.get_account_customers(json_, **{'trace_id': self.request.state.trace_id})
        empty_customer_ids = [str(item['account_id']) for item in res.get('data') if item['customer_id'] is None]
        joined_ids = ','.join(empty_customer_ids)
        if empty_customer_ids:
            return MyResponse(code=RET.DATA_ERR, msg=f'{joined_ids}账户不在系统中,请先录入')
        accept_customer_ids = [str(item['customer_id']) for item in res.get('data')]
        not_exists_in_customer_ids = [item for item in accept_customer_ids if int(item) not in customer_ids[0]]
        if not_exists_in_customer_ids:
            return MyResponse(code=RET.NO_DATA, msg='您输入的广告账户尚未获得授权，请联系我们的工作人员授权!')
        medium_res = CRMExternalService.get_accounts_medium(json_, **{'trace_id': self.request.state.trace_id})
        medium_data = [item['medium'] for item in medium_res.get('data') if item['medium'] != 'Meta']
        if medium_data:
            return MyResponse(
                code=RET.DATA_ERR, msg="Pixel绑定/解绑仅支持meta广告账户"
            )
        try:
            if property_ids:
                request_id = APIService.post_pixel_account(pixel_data, **{'trace_id': self.request.state.trace_id})
                if not request_id:
                    return MyResponse(code=RET.PARAM_ERR, msg='请重新输入要绑定的广告账户ID')
                new_pixel = PixelAccount(
                    request_id=request_id,
                    pixel_id=pixel_id,
                    operate_type=operation,
                    binding_time=datetime.now(),
                    user_id=userid
                )
                db.add(new_pixel)
                db.flush()
                new_pixel_detail_list = list()
                for property_id in property_ids:
                    new_pixel_detail = PixelAccountDetail(
                        account_id=property_id,
                        remark="",
                        pixel_account_id=new_pixel.id
                    )
                    new_pixel_detail_list.append(new_pixel_detail)
                db.add_all(new_pixel_detail_list)
        except Exception as e:
            web_log.log_error(
                f"pixel绑定/解绑广告账户调用失败原因：{e}")
            return MyResponse(code=RET.DATA_ERR, msg='调用媒体接口失败')
        else:
            db.commit()
            mapi_request_result.delay(request_id)
        desc = Operation.BIND.desc if operation == Operation.BIND.value else Operation.UNBIND.desc
        return MyResponse(code=RET.OK, msg=f'{desc}成功')
