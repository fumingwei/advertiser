from starlette.requests import Request
from fastapi_utils.cbv import cbv
from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.orm import Session
from settings.db import get_db
from settings.log import web_log
from tools.common import CommonQueryParams
from tools.constant import RET
from tools.resp import MyResponse
from libs.internal.crm_external_service import CRMExternalService
from apps.accounts.utils import get_customer_ids


ReportRouter = APIRouter(tags=["数据报表管理"])


# 账单
@cbv(ReportRouter)
class ReportBillServer:
    request: Request

    @ReportRouter.get("/bills", description="账单列表")
    async def get_bill_list(
            self,
            common_query: CommonQueryParams = Depends(),
            is_cancel: str = None,
            put_way: str = None,
            start_date: str = None,
            end_date: str = None,
            medium: str = None,
            pay_way: str = None,
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        customer_ids = get_customer_ids(db, user_id)
        # TODO:测试数据
        # customer_ids = [3373]
        json_ = {
            "page": common_query.page,
            "page_size": common_query.page_size,
            "q": common_query.q,
            "customer_id": customer_ids,
            "is_cancel": is_cancel,
            "put_way": put_way,
            "start_date": start_date,
            "end_date": end_date,
            "medium": medium,
            "pay_way": pay_way
        }
        response = CRMExternalService.post_bill_list(json=json_, **{'trace_id': self.request.state.trace_id})
        msg = response.get("msg")
        code = response.get("code")
        data = response.get("data", [])
        total = response.get("total", 0)
        return MyResponse(code=code, msg=msg, data=data, total=total)

    @ReportRouter.get('/bills_detail/{id}', description="账单详情")
    async def get_bill_detail(self, id: int, common_query: CommonQueryParams = Depends()):
        params = {"bill_id": id,
                  "page": common_query.page,
                  "page_size": common_query.page_size,
                  "q": common_query.q
                  }
        try:
            response = CRMExternalService.get_bill_detail(
                params=params,
                **{'trace_id': self.request.state.trace_id}
            )
            data = response.get("data", [])
            msg = response.get("msg")
            total = response.get("total")
            code = response.get("code")
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg=e.__str__(), total=0, data=[])
        return MyResponse(code=code, msg=msg, total=total, data=data)

    @ReportRouter.get('/bills_medium', description="账单媒介")
    async def get_bill_medium(self, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        customer_ids = get_customer_ids(db, user_id)
        # customer_ids = [3218]
        json_ = {"customer_id": customer_ids}
        try:
            response = CRMExternalService.post_bill_medium(json=json_, **{'trace_id': self.request.state.trace_id})
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg="获取账单媒介异常", data=[])
        return MyResponse(code=response.get("code"), msg=response.get("msg"), data=response.get("data", []))


# 返点使用
@cbv(ReportRouter)
class ReportRebateUseServer:
    request: Request

    @ReportRouter.get('/rebate_uses', description="返点使用列表")
    async def get_rebate_use(self, common_query: CommonQueryParams = Depends(),
                             approval_status: str = None,
                             apply_user_id: str = None,
                             use_way: str = None,
                             db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        customer_ids = get_customer_ids(db, user_id)
        # TODO:测试客户
        # customer_ids = [2518]
        json_ = {"page": common_query.page,
                 "page_size": common_query.page_size,
                 "q": common_query.q,
                 "approval_status": approval_status,
                 "use_way": use_way,
                 "apply_user_id": apply_user_id,
                 "customer_id": customer_ids
                 }
        response = CRMExternalService.post_rebate_use_list(json=json_, **{'trace_id': self.request.state.trace_id})
        code = response.get('code')
        msg = response.get('msg')
        data = response.get('data', [])
        total = response.get('total', 0)
        return MyResponse(code=code, msg=msg, data=data, total=total)

    @ReportRouter.get('/rebate_uses/{id}', description="返点使用详情")
    async def get_rebate_use_detail(self, id: int):
        try:
            response = CRMExternalService.get_rebate_use_detail(
                params={"id": id},
                **{'trace_id': self.request.state.trace_id}
            )
            data = response.get("data", [])
            msg = response.get("msg")
            total = response.get("total")
            code = response.get("code")
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg=e.__str__(), total=0, data=[])
        return MyResponse(code=code, msg=msg, total=total, data=data)


# 账户信息
@cbv(ReportRouter)
class ReportAccountInfoServer:
    request: Request

    @ReportRouter.get('/accounts', description="账户状态分类")
    async def get_account_status(self, db: Session = Depends(get_db)):

        user_id = self.request.state.user.user_id
        customer_ids = get_customer_ids(db, user_id)
        json_ = {"customer_ids": customer_ids}
        try:
            crm_result = CRMExternalService.post_account_count(
                json_,
                headers={"advertiser_user_id": str(user_id)},
                **{'trace_id': self.request.state.trace_id}
            )
            data = crm_result.get("data", {})
            return MyResponse(data=data)
        except Exception as e:
            web_log.log_error(e.__str__())
            return MyResponse(code=RET.UNKNOW_ERR, msg=e.__str__())

