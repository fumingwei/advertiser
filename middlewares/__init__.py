# -*- coding: utf-8 -*-
import random
import string
import time
import sys
import json
import re
import threading
import traceback
import ulid
from munch import DefaultMunch
from starlette.requests import Request
from starlette.middleware.cors import CORSMiddleware
from fastapi.middleware.wsgi import WSGIMiddleware
from urllib.parse import unquote
from middlewares.user_verify import AdvertiserUserVerify
from tools.resp import MyResponse
from settings.base import configs
from apps.system.models import OperateLog
from tools.constant import RET, error_map
from tools import is_privilege_ip
from settings.db import SessionLocal
from libs.open.open import OpenAPIRequest
from settings.log import web_log
from starlette.concurrency import iterate_in_threadpool


def middleware_init(app):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["*"],
    )

    def log_record(path, params, method, process_time, ip, request_user, content_type, code, msg, trace_id, error='',
                   level='info'):
        log_content = {
            'path': path,
            'params': params,
            'method': method,
            'process_time': process_time,
            'ip': ip,
            'request_user': request_user,
            'content_type': content_type,
            'code': code,
            'msg': msg,
            'trace_id': trace_id,
            'error': error
        }
        if level == 'error':
            web_log.log_error(str(log_content))
            return
        web_log.log_info(str(log_content))

    # 校验token
    @app.middleware("http")
    async def auth_verify(request: Request, call_next):
        # 静态文件无需校验身份信息
        request_path = request.url.path
        method = request.method
        start_time = time.time()
        params = unquote(request.query_params.__str__())
        content_type = 'application/json'  # 默认值application/json
        ip = request.client.host  # 请求ip地址
        trace_id = request.headers.get('X-B3-TraceId')
        if not trace_id:
            # 如果没有追踪ID，则生成一个新的追踪ID
            trace_id = str(ulid.new())
        setattr(request.state, 'trace_id', trace_id)
        if request_path in [
            "/docs",
            "/openapi.json",
            f"{configs.API_VERSION_STR}/advertiser/registrations",  # 用户自助系统注册
            f"{configs.API_VERSION_STR}/advertiser/login",  # 用户自助系统登录
            f"{configs.API_VERSION_STR}/advertiser/forget_password",  # 用户自助系统忘记密码登录
            f"{configs.API_VERSION_STR}/advertiser/sms_codes",  # 用户自助系统发送验证码
            f"{configs.API_VERSION_STR}/advertiser/sms_login",  # 用户自助系统验证码登录
        ] or request_path.startswith(f"{configs.API_VERSION_STR}/static"):
            return await call_next(request)
        elif request_path == f"{configs.API_VERSION_STR}/callback/on_complete":
            # 回调接口 需校验是否是内网ip
            base_url = request.client.host
            is_private = is_privilege_ip(base_url)
            if not is_private:
                return MyResponse(RET.PER_ERR, msg="暂无权限~")
        else:
            Authorization = request.headers.get('Authorization')
            code, ret = AdvertiserUserVerify.auth(Authorization)
            if code != RET.OK:
                process_time = round(time.time() - start_time, 2)  # 认证处理时间
                log_record(
                    request_path,
                    params,
                    method,
                    process_time,
                    ip,
                    '',
                    content_type,
                    code,
                    ret,
                    trace_id
                )
                return MyResponse(code=code, msg=ret)
            setattr(request.state, 'user', DefaultMunch.fromDict(ret))
            request_user = request.state.user.real_name
            try:
                response = await call_next(request)
                process_time = time.time() - start_time  # 获取接口处理时间
                content_type = response.headers.get('content-type', '')
                # json数据类型获取自定义信息
                if 'application/json' in content_type:
                    response_body = [chunk async for chunk in response.body_iterator]
                    response.body_iterator = iterate_in_threadpool(iter(response_body))
                    json_res = json.loads(b''.join(response_body).decode())
                    code = json_res.get('code')
                    msg = json_res.get('msg')
                # 其他类型默认自定义信息
                else:
                    code = RET.OK
                    msg = 'OK'
                log_record(
                    request_path,
                    params,
                    method,
                    process_time,
                    ip,
                    request_user,
                    content_type,
                    code,
                    msg,
                    trace_id
                )
                return response
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_info = traceback.extract_tb(exc_traceback)
                filename, line, func, text = tb_info[-1]
                error_info = {
                    'error_file': filename,
                    'error_line': line,
                    'error_func': func,
                    'error_text': text,
                    'error_exc': exc_value
                }
                code = RET.SERVER_ERR
                msg = error_map[code]
                process_time = time.time() - start_time  # 获取接口处理时间
                log_record(
                    request_path,
                    params,
                    method,
                    process_time,
                    ip,
                    request_user,
                    content_type,
                    code,
                    msg,
                    trace_id,
                    str(error_info),
                    'error'
                )
                return MyResponse(code=RET.SERVER_ERR, msg=error_map[RET.SERVER_ERR])

    def insert_data(request, log_str, response_data, formatted_process_time):
        path_parts = request.url.path.split("/")
        x_forwarded_for = request.headers.get("X-Forwarded-For")
        user_ip = request.client.host
        user_id = request.state.user.user_id if hasattr(request.state, 'user') else None
        query_params_dict = dict(request.query_params)
        handler = request.scope.get("endpoint", None)
        if request.url.path in [f'{configs.API_VERSION_STR}/systems/operate_log', '/favicon.ico', '/', '']:
            return
        request_path = request.url.path
        if callable(handler):
            if hasattr(handler, '__name__'):
                operation = f"{handler.__module__}/{handler.__name__}"
            else:
                operation = handler.__module__
        else:
            if handler is not None:
                operation = f"{handler.__module__}/{handler}"
            else:
                operation = "default_operation"
        user_ip = x_forwarded_for.split(',')[0].strip() if x_forwarded_for else user_ip
        ip_info: dict = OpenAPIRequest.ip_parse(user_ip)
        keys_to_extract = ['country']
        address_info = '/'.join([ip_info.get(key, '') for key in keys_to_extract]) if ip_info else ""
        if len(path_parts) < 4:
            modules = path_parts[-1]
        else:
            modules = path_parts[3]
        operation_desc = ''
        request_method = set()
        request_method.add(request.method)
        for route in app.routes:
            path_list = route.path.split('/')[0:-1]
            path_last = route.path.split('/')[-1]
            path = '/'.join(path_list)
            pattern = re.compile(f"{path}/.+")
            pattern_last = r'\{(.+?)\}'
            try:
                if request_path == route.path and request_method == route.methods:
                    operation_desc = route.description
                    break
                elif pattern.match(request_path) and request_method == route.methods and re.search(pattern_last,
                                                                                                   path_last):
                    operation_desc = route.description
                    break
                else:
                    operation_desc = ''
            except Exception as e:
                web_log.log_error(f'获取操作描述失败原因：{e}')
                operation_desc = ''
        msg_start_index = log_str.find("msg=") + len("msg=")
        msg_value = log_str[msg_start_index:]
        if msg_value == "成功" or "http_status_code=200" in log_str or "self_status_code=0" in log_str:
            request_status = "Success"
        else:
            request_status = "Error"
        data_to_insert = [
            {"request_path": request_path, "module": modules, "required_params": query_params_dict,
             "request_method": request_method, "request_ip": user_ip,
             "request_address": address_info, "operation": operation,
             "request_user_id": user_id, "return_params": response_data,
             "request_status": request_status, "spent_time": formatted_process_time,
             "operation_desc": operation_desc}
        ]
        try:
            with SessionLocal() as db:
                db.bulk_insert_mappings(OperateLog, data_to_insert)
                db.commit()
        except Exception as e:
            web_log.log_error(f"Error: {e}")

    @app.middleware('http')
    async def log_requests(request, call_next):
        idem = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        start_time = time.time()
        response = await call_next(request)
        process_time = (time.time() - start_time) * 1000
        formatted_process_time = '{0:.2f}'.format(process_time)
        log_str = f"rid={idem} start request {request.method} " \
                  f"request_user={request.state.user.user_id if hasattr(request.state, 'user') else '匿名用户'} " \
                  f"path={request.url.path} " \
                  f"completed_in={formatted_process_time}ms "
        content_type = response.headers.get('content-type', '')
        response_data = {}
        if 'application/json' not in content_type:
            log_str += f"http_status_code={response.status_code} "
        else:
            response_body = [chunk async for chunk in response.body_iterator]
            response.body_iterator = iterate_in_threadpool(iter(response_body))
            json_res = json.loads(b''.join(response_body).decode())
            log_str += f"http_status_code={response.status_code} msg={json_res['detail']}" if 'detail' in json_res else \
                f"self_status_code={json_res.get('code')} msg={json_res.get('msg')}"
            for key, value in json_res.items():
                response_data[key] = value
        threading.Thread(
            target=insert_data,
            args=(request, log_str, response_data, formatted_process_time)
        ).start()
        return response

    WSGIMiddleware(app)
