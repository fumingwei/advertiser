import uuid
import logging
import json
import requests
from starlette.requests import Request
from fastapi import APIRouter, Depends, Query
from fastapi_utils.cbv import cbv
from sqlalchemy import or_, func, desc
from sqlalchemy.orm import Session
from settings.db import get_db, MyPagination
from settings.base import configs
from settings.log import web_log
from tools.common import CommonQueryParams, row_dict
from tools.resp import MyResponse
from tools.constant import RET
from libs.ali.ali_oss import OssManage
from libs.internal.crm_external_service import CRMExternalService
from apps.onboarding.define import OeApproveStatus
from apps.onboarding.schemas import OeOpenAccountSchema
from apps.onboarding.models import OeOpenAccount
from apps.advertiser.models import AdvertiserUser
from apps.onboarding.define import TIMEZONE
from libs.internal.api_service import APIService
from sqlalchemy import case

logger = logging.getLogger(__name__)

ALIOSS_URL = configs.ALIOSS_URL
OSS_PREFIX = configs.OSS_PREFIX

OpenAccountRouter = APIRouter(tags=["开户"])


# 账户开户工单
@cbv(OpenAccountRouter)
class OpenAccountTicketServer:
    request: Request

    @OpenAccountRouter.get("/open_account", description="开户工单列表")
    async def get_open_account_ticket(
            self,
            common_query: CommonQueryParams = Depends(),
            approval_status: str = Query(None),
            start_date: str = Query(None),
            end_date: str = Query(None),
            db: Session = Depends(get_db),
    ):
        oe_filter_list = [OeOpenAccount.is_delete == False]
        user_id = self.request.state.user.user_id
        sub_user_ids = self.request.state.user.sub_user_ids
        if sub_user_ids:
            # 如果有子账户
            sub_user_ids.append(user_id)  # 添加主账户ID
            oe_filter_list.append(OeOpenAccount.user_id.in_(sub_user_ids))
        else:
            # 如果没有子账户
            oe_filter_list.append(OeOpenAccount.user_id == user_id)
        if common_query.q:
            oe_filter_list.append(
                or_(
                    OeOpenAccount.ticket_id.like(f"%{common_query.q}%"),
                    OeOpenAccount.oe_number.like(f"%{common_query.q}%"),
                    OeOpenAccount.chinese_legal_entity_name.like(f"%{common_query.q}%"),
                    AdvertiserUser.real_name.like(f"%{common_query.q}%"),
                )
            )
        if start_date:
            oe_filter_list.append(func.date(OeOpenAccount.created_time) >= start_date)
        if end_date:
            oe_filter_list.append(func.date(OeOpenAccount.created_time) <= end_date)
        if approval_status:
            if approval_status == OeApproveStatus.PENDING.desc:
                oe_filter_list.append(OeOpenAccount.approval_status == OeApproveStatus.PENDING.value)
            if approval_status == OeApproveStatus.APPROVED.desc:
                oe_filter_list.append(OeOpenAccount.approval_status == OeApproveStatus.APPROVED.value)
            if approval_status == OeApproveStatus.DISAPPROVED.desc:
                oe_filter_list.append(OeOpenAccount.approval_status == OeApproveStatus.DISAPPROVED.value)
            if approval_status == OeApproveStatus.CHANGES_REQUESTED.desc:
                oe_filter_list.append(OeOpenAccount.approval_status == OeApproveStatus.CHANGES_REQUESTED.value)
            if approval_status == OeApproveStatus.AUTO_DISAPPROVED.desc:
                oe_filter_list.append(OeOpenAccount.approval_status == OeApproveStatus.AUTO_DISAPPROVED.value)
        oe_query = (
            db.query(
                OeOpenAccount.ticket_id.label("ticket_id"),
                OeOpenAccount.oe_number.label("oe_number"),
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
                        )
                    ]
                ).label("approval_status"),
                OeOpenAccount.created_time.label("created_time"),
                AdvertiserUser.real_name.label("real_name"),
                case(
                    [
                        (
                            OeOpenAccount.approval_status == OeApproveStatus.PENDING.value,
                            None,
                        ),
                        (
                            OeOpenAccount.approval_status
                            == OeApproveStatus.APPROVED.value,
                            OeOpenAccount.approval_time,
                        ),
                        (
                            OeOpenAccount.approval_status
                            == OeApproveStatus.DISAPPROVED.value,
                            OeOpenAccount.approval_time,
                        ),
                        (
                            OeOpenAccount.approval_status
                            == OeApproveStatus.CHANGES_REQUESTED.value,
                            OeOpenAccount.approval_time,
                        ),
                        (
                            OeOpenAccount.approval_status
                            == OeApproveStatus.AUTO_DISAPPROVED.value,
                            OeOpenAccount.approval_time,
                        )
                    ]
                ).label("approval_time"),
            )
            .outerjoin(AdvertiserUser, AdvertiserUser.id == OeOpenAccount.user_id)
            .filter(*oe_filter_list)
        )
        query = oe_query.order_by(desc(OeOpenAccount.created_time))
        paginator = MyPagination(query, common_query.page, common_query.page_size)
        return MyResponse(
            total=paginator.counts,
            data=paginator.data,
        )

    @OpenAccountRouter.post("/open_account", description="创建开户工单")
    async def create_open_account_ticket(self, data: OeOpenAccountSchema, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        data = data.dict()
        customer_id = data.get("customer_id")
        oe_number = data.get("oe_number")
        oe_open_account = db.query(OeOpenAccount).filter(OeOpenAccount.oe_number == oe_number,
                                                         OeOpenAccount.approval_status != OeApproveStatus.CHANGES_REQUESTED.value,
                                                         OeOpenAccount.is_delete == False).first()
        if oe_open_account:
            approval_status = OeApproveStatus[oe_open_account.approval_status]
            desc = approval_status.desc
            return MyResponse(code=RET.PARAM_ERR, msg=f'该OE开户申请{desc}')
        res = APIService.get_oe_open_account(oe_number)['data']
        if res.get('error'):
            return MyResponse(code=RET.PARAM_ERR, msg='请输入有效的OE编号')
        # 开户营业执照链接
        business_registration = res.get('business_registration')
        uuid_hex = uuid.uuid4().hex
        file_extension = (requests.get(business_registration))
        content_type = file_extension.headers.get('Content-Type').split('/')[-1]
        new_filename = f"{uuid_hex[:6]}.{content_type}"
        oss_file_key = f"{OSS_PREFIX}/onboarding/{new_filename}"
        OssManage().file_upload(key=oss_file_key, file=file_extension)
        business_url = f"https://{configs.BUCKET_NAME}.{configs.END_POINT}/{oss_file_key}"
        # 广告账户
        ad_accounts = [
            {
                "ad_account_name": item["ad_account_name"],
                "timezone": TIMEZONE.get(str(item["timezone_id"]))[3:].replace("_", "/", 1),
                "ad_account_id": "待开户"
            }
            for item in res["ad_accounts_info"]
        ]
        for item in ad_accounts:
            parts = item["timezone"].split('/')
            parts[0] = parts[0].capitalize()
            item["timezone"] = '/'.join(parts)
        # 公共主页
        promotable_pages = [
            {
                "page_id": item["pageID"],
                "page_name": item["pageName"]
            }
            for item in res.get('promotable_pages')
        ]
        try:
            oe_account = OeOpenAccount(
                customer_id=customer_id,
                oe_number=oe_number,
                chinese_legal_entity_name=res.get('chinese_legal_entity_name'),
                customer_type=res.get('signal').get('advertiser_segment'),
                business_registration=business_url,
                org_ad_account_count=res.get('org_ad_account_count'),
                ad_account_limit=res.get('ad_account_limit'),
                ad_accounts=ad_accounts,
                promotable_pages=promotable_pages,
                promotable_app_ids=res.get('promotable_app_ids', []),
                promotion_website=res.get('promotable_urls', []),
                ad_account_creation_request_id=res.get('ad_account_creation_request_id', {}).get("id"),
                remark='',
                user_id=user_id
            )
            db.add(oe_account)
            db.commit()
        except Exception as e:
            db.rollback()
            logger.exception(e)
            return MyResponse(code=RET.REQ_ERR, msg=str(e))
        return MyResponse()

    @OpenAccountRouter.get("/open_account/{ticket_id}", description="开户工单详情")
    async def get_open_account_ticket_detail(
            self, ticket_id: str, db: Session = Depends(get_db)
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
                crm_result = CRMExternalService.customer_id_name(json_, **{'trace_id': self.request.state.trace_id})
                try:
                    customer_name = crm_result["data"][0]["name"]
                except Exception as e:
                    web_log.log_error(e)
                    res_data["customer_name"] = None
                else:
                    res_data["customer_name"] = customer_name
            return MyResponse(data=res_data)
        else:
            return MyResponse(code=RET.DATA_ERR, msg="工单不存在")
