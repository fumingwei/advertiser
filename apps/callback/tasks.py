import time
import ulid
from pprint import pformat
from libs.internal.api_service import APIService
from settings.log import celery_log
from my_celery.main import celery_app
from settings.base import configs
from apps.callback.utils import function_dict


@celery_app.task(name="mapi_request_result", queue="mapi_request_result")
def mapi_request_result(request_id):
    is_success = False
    while True:
        params = {
            'api_key': configs.MAPI_KEY,
            'request_id': request_id
        }
        res = APIService.get_common_model_result(params, **{'trace_id': str(ulid.new())})
        data = res.get('data', {})
        request_status = data.get('request_status')
        if request_status != "Finished":
            time.sleep(1)
            continue
        celery_log.log_info(f'请求参数：\n{pformat(params)}; 处理结果：\n{pformat(data)}')
        fun = function_dict.get(data.get('operation_type'))
        if fun and request_status == "Finished":
            try:
                fun(data)
                is_success = True
                break
            except Exception as e:
                celery_log.log_error(f'tasks.py发生异常，异常原因：{e.__str__()}')
    return "执行成功" if is_success else "执行失败"
