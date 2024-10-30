# -*- coding: utf-8 -*-
import os
import sys
import jinja2
import uuid
import inspect
import threading
import traceback
import netifaces
import requests

from typing import Optional, Union, List, Dict
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from settings.base import configs
from settings.db import Base, Row, RedisClient
from tools.constant import RET, error_map
from starlette.responses import JSONResponse

retry = Retry(
    total=3, connect=3, read=3, status=3, backoff_factor=0.5, raise_on_redirect=False
)
session = requests.Session()
session.mount("http://", HTTPAdapter(max_retries=retry))
session.mount("https://", HTTPAdapter(max_retries=retry))


def convert_to_largest_unit(size):
    # 字节单位
    units = ['KB', 'MB', 'GB', 'TB']
    # 初始最小单位
    unit = 'B'
    for u in units:
        if size < 1024:
            break
        size /= 1024
        unit = u
    return f"{round(size, 3)}{unit}"


# 将查询改为dict
def row_dict(query_result):
    if isinstance(query_result, Base):
        return query_result.to_dict()
    elif isinstance(query_result, Row):
        _map = dict(query_result._mapping)
        dic = {}
        for key, value in _map.items():
            if isinstance(value, Base):
                dic.update(value.to_dict())
            else:
                dic[key] = value
        return dic
    else:
        raise Exception("query_result should be a Row or Base~")


# 查询结果为list
def row_list(query_result):
    _data = []
    for i in query_result:
        if isinstance(i, Row):
            _map = dict(i._mapping)
            dic = {}
            for key, j in _map.items():
                if isinstance(j, Base):
                    dic.update(j.to_dict())
                else:
                    dic[key] = j
            _data.append(dic)
        elif isinstance(i, Base):
            _data.append(i.to_dict())
        else:
            raise Exception("query_result should be a Row or Base~")
    return _data


# 公共查询参数依赖
class CommonQueryParams:
    def __init__(self, q: Optional[str] = None, page: int = 1, page_size: int = 10):
        self.q = q
        self.page = page
        self.page_size = page_size


class SingletonType(type):
    _instance_lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            with SingletonType._instance_lock:
                if not hasattr(cls, "_instance"):
                    cls._instance = super(SingletonType, cls).__call__(*args, **kwargs)
        return cls._instance


class MyResponse(JSONResponse):
    def __init__(
            self,
            code: int = RET.OK,
            msg: str = error_map[RET.OK],
            total: Optional[int] = None,
            data: Union[List, Dict] = None,
            err=None,
            **kwargs,
    ):
        response_data = {"code": code, "msg": msg}
        if total is not None:
            response_data["total"] = total
        if data is not None:
            response_data["data"] = data
        if err:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_info = traceback.extract_tb(exc_traceback)
            filename, line, func, text = tb_info[-1]
            module = filename.split("gatherone_crm/")[-1]
            response_data["msg"] = (
                f"Error Occur IN:{module},Line NO:{line},Error Msg: {exc_value}"
            )
        super().__init__(content=response_data, **kwargs)


class CommonMethod:
    @staticmethod
    def render_template(template_file, context):
        current_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        target_folder = os.path.join(current_directory, "templates")
        sys.path.append(target_folder)
        template_loader = jinja2.FileSystemLoader(searchpath=target_folder)
        template_env = jinja2.Environment(loader=template_loader)
        template = template_env.get_template(template_file)
        html_content = template.render(context)
        return html_content

    @staticmethod
    def generate_uuid():
        return str(uuid.uuid4())

    # 获取一个类的所有自定义属性
    @staticmethod
    def get_cls_attributes(cls):
        attributes = []
        for attr, val in inspect.getmembers(cls):
            if not attr.startswith("__") and not inspect.ismethod(val):
                attributes.append(attr)
        return attributes


# 获取局域网ip
def get_lan_ip():
    lan_ips = []
    for interface in netifaces.interfaces():
        if netifaces.AF_INET in netifaces.ifaddresses(interface):
            for link in netifaces.ifaddresses(interface)[netifaces.AF_INET]:
                if not link['addr'].startswith("127."):
                    lan_ips.append(link['addr'])
    return lan_ips[0]


# 获取公网ip
def get_public_ip():
    try:
        response = requests.get('https://api.ipify.org')  # 使用ipify.org提供的服务
        if response.status_code == 200:
            return response.text.strip()
        else:
            print("Failed to retrieve IP address, status code:", response.status_code)
    except Exception as e:
        print("An error occurred:", e)


def power(user_id, account_ids):
    """
    权限鉴定，并且返回没有授权的账户,传入子账号
    第一个参数：是否存在权限，
    第二个参数：哪些账户没权限
    """

    redis_cli = RedisClient(db=configs.REDIS_STORAGE['accredit_account']).get_redis_client()
    res = redis_cli.smembers(f'ad_sub_accounts:{user_id}')
    if res is None:
        return False, None
    account_ids = set(account_ids)
    data = res | account_ids
    if data == res:
        return True, None
    else:
        diff = data - res
    return False, diff
