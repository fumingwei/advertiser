# -*- coding: utf-8 -*-
from fastapi import APIRouter
from fastapi_utils.cbv import cbv
from starlette.requests import Request
from apps.callback.schemas import OnCompleteSchema
from tools.resp import MyResponse
from tools.constant import RET, error_map
from apps.callback.utils import function_dict
from settings.log import web_log

CallbackRouter = APIRouter(tags=['ACCOUNT'])


@cbv(CallbackRouter)
class CallBackApi:
    request: Request
    """
    gatherone_api_integration回调结果保存
    """

    @CallbackRouter.post('/on_complete', description='待api项目回调')
    def on_complete(self, data: OnCompleteSchema):
        data = data.dict()
        fun = function_dict.get(data.get('operation_type'))
        # api_key = self.request.query_params.get('api_key')
        # verify_dict = verify_api_key(api_key)
        # if not verify_dict:
        #     log_error("未提供有效的api_key")
        #     return MyResponse(RET.PER_ERR, "未提供有效的api_key")
        if fun:
            try:
                fun(data)
            except Exception as e:
                web_log.log_error(f'回调失败原因：{e}')
                return MyResponse(RET.DATA_ERR, error_map.get(RET.DATA_ERR))
        return MyResponse()
