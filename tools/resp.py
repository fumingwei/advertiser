# -*- coding:utf-8 -*-
import sys
import traceback
from typing import Optional, Union, List, Dict
from starlette.responses import JSONResponse
from tools.constant import RET, error_map
from tools.exceptions import GatheroneError
from settings.base import configs


class MyResponse(JSONResponse):
    def __init__(
        self,
        code: int = RET.OK,
        msg: str = error_map[RET.OK],
        total: Optional[int] = None,
        data: Union[List, Dict] = None,
        other_data: Dict = {},
        err=None,
        **kwargs,
    ):
        response_data = {"code": code, "msg": msg}
        response_data.update(other_data)
        if total is not None:
            response_data["total"] = total
        if data is not None:
            response_data["data"] = data
        if err and configs.ENVIRONMENT.lower() == "development":
            # 仅在开发环境下返回可供调试的错误信息，生产环境下返回固定错误信息
            if isinstance(err, GatheroneError):
                # 显示被调用服务响应的错误提示信息
                response_data["msg"] = str(err)
            else:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_info = traceback.extract_tb(exc_traceback)
                filename, line, func, text = tb_info[-1]
                module = filename.split("gatherone_advertiser/")[-1]
                response_data["msg"] = (
                    f"Error Occur IN:{module},Line NO:{line},Error Msg: {exc_value}"
                )
        super().__init__(content=response_data, **kwargs)
