# -*- coding: utf-8 -*-
import threading
import time
import jwt
from starlette.requests import Request
from settings.base import configs
from settings.db import SessionLocal
from tools.common import CommonMethod
from tools.constant import RET
from apps.system.models import LoginHistory
from settings.log import web_log
from user_agents import parse
from libs.open.open import OpenAPIRequest
from settings.db import RedisClient
from libs.internal.crm_external_service import CRMExternalService
from sqlalchemy import func, or_, desc
from tools.common import MyResponse
from apps.advertiser.models import (
    UserCusRelationship,
    ProjectGroup,
    GroupAccountRelationship
)


class UserMethod:
    # 从头像链接解析key
    @staticmethod
    def parse_key_from_url(avatar_url):
        key = avatar_url.replace(
            f"https://{configs.BUCKET_NAME}.{configs.END_POINT}/", ""
        )
        return key

    # 上传头像/删除旧头像
    @staticmethod
    def update_avatar(avatar_url, file_content, file_type):
        """
        更新头像异步删除，顺序上传
        """
        from libs.ali.ali_oss import OssManage
        bucket = OssManage()
        if avatar_url:
            old_key = UserMethod.parse_key_from_url(avatar_url)
            del_thread = threading.Thread(
                target=bucket.file_delete, kwargs={"key": old_key}
            )
            del_thread.start()
        new_key = "avatars/" + CommonMethod.generate_uuid() + f".{file_type}"
        bucket.file_upload(key=new_key, file=file_content)
        # put_thread = threading.Thread(
        #     target=bucket.put_object, kwargs={"key": new_key, "data": file_content}
        # )
        # put_thread.start()
        new_avatar_url = f"https://{configs.BUCKET_NAME}.{configs.END_POINT}/{new_key}"
        return new_avatar_url

    @staticmethod
    def login_histories(login_from, user_id, device, browser, ip, country, request_status, login_info):
        data_to_insert = [
            {"login_from": login_from, "user_id": user_id, "device": device, "browser": browser, "ip": ip,
             "country": country, "request_status": request_status, "login_info": login_info}
        ]
        try:
            with SessionLocal() as db:
                db.bulk_insert_mappings(LoginHistory, data_to_insert)
                db.commit()
        except Exception as e:
            web_log.log_error(f"Error: {e}")


# 获取session_id，请求ip，设备信息
def get_device(request: Request):
    client_ip = request.headers.get('X-Forwarded-For')
    if client_ip:
        ip = client_ip.split(',')[0].strip()
    else:
        ip = request.client.host
    user_agent = request.headers.get('user-agent')
    user_agent_ = parse(user_agent)
    device = f'{user_agent_.os.family} {user_agent_.os.version_string}'  # 设备名
    browser = user_agent_.browser.family  # 浏览器名
    return device, browser, ip


def add_login_record(user_id, mobile, request_status, login_info, device='', browser='', ip=''):
    ip_res: dict = OpenAPIRequest.ip_parse(ip)
    with SessionLocal() as db:
        new = LoginHistory(
            user_id=user_id,
            mobile=mobile,
            device=device,
            browser=browser,
            request_status=request_status,
            login_info=login_info,
            ip=ip_res.get('ip'),
            country=ip_res.get('country')
        )
        db.add(new)
        db.commit()
    return f'{user_id},登录信息添加成功'


class JwtTokenUtil:
    AUTH_HEADER_KEY = "Authorization"
    TOKEN_PREFIX = "Bearer "
    EXPIRATION_TIME = configs.ACCESS_TOKEN_EXPIRE  # token过期时间  默认8小时

    @staticmethod
    def generate_jwt(payload, expiry=EXPIRATION_TIME, secret=None):
        """
        生成jwt
        :param payload: dict 载荷
        :param expiry: datetime 有效期
        :param secret: 密钥
        :return: jwt
        """
        _payload = {'exp': int(time.time()) + expiry}
        _payload.update(payload)
        if not secret:
            secret = configs.SECRET_KEY
        token = jwt.encode(payload=_payload, key=secret, algorithm='HS256')
        return token

    @staticmethod
    def verify_jwt(token, secret=None):
        """
        检验jwt
        :param token: jwt
        :param secret: 密钥
        :return: dict: payload
        """
        if not secret:
            secret = configs.SECRET_KEY

        try:
            payload = jwt.decode(token, secret, algorithms=['HS256'])
        except jwt.PyJWTError:
            payload = None

        return payload


def verify_sms_code(code_type, mobile, sms_code, delete=False):
    redis_coon = RedisClient(db=configs.REDIS_STORAGE['sms_code']).get_redis_client()
    _sms_code = redis_coon.get(f'{code_type}_{mobile}')
    if not _sms_code:
        return RET.CODE_ERR, "验证码不存在，请重新发送"
    if sms_code != _sms_code:
        redis_coon.close()
        return RET.CODE_ERR, "验证码错误，请重新输入"
    if delete:
        redis_coon.delete(f'{code_type}_{mobile}')
    redis_coon.close()
    return RET.OK, "验证通过"


def get_customer_ids(db, group_id):
    # 需要条件： 组ID
    group_info = db.query(ProjectGroup).filter(ProjectGroup.id == group_id).one_or_none()
    if not group_info:
        return MyResponse(RET.DATA_ERR, '未找到对应项目组')
    # 获取公司id
    company_id = group_info.company_id
    customers = db.query(UserCusRelationship.customer_id).filter(UserCusRelationship.company_id == company_id).all()
    if not customers:
        return MyResponse(RET.DATA_ERR, '该项目组无客户绑定')
    # 获取客户id列表
    cus_id_list = customers[0][0]
    return cus_id_list


def get_authorized_accounts(db, group_id, medium, q, start_time, end_time):
    filters = [GroupAccountRelationship.is_delete == False,
               GroupAccountRelationship.project_group_id == group_id]
    if q:
        filters.append(or_(
            GroupAccountRelationship.account_id.ilike(f'%{q}%'),
            GroupAccountRelationship.account_name.ilike(f'%{q}%')
        ))
    if medium:
        filters.append(
            GroupAccountRelationship.medium == medium
        )
    if all([start_time, end_time]):
        filters.extend([
            func.date(GroupAccountRelationship.created_time) >= func.date(start_time),
            func.date(GroupAccountRelationship.created_time) <= func.date(end_time)
        ])
    query = db.query(GroupAccountRelationship).filter(*filters)
    query = query.order_by(desc(GroupAccountRelationship.created_time))
    return query


def get_unauthorized_accounts(user_id, cus_id_list, medium, q, group_id, page=None, page_size=None, trace_id=None):
    if not group_id:
        return False, MyResponse(RET.DATA_ERR, '未获取到组信息')
    header = {
        'advertiser_user_id': str(user_id)
    }
    payload = {
        "customer_ids": cus_id_list,
        "group_id": [group_id],
        "is_second": True,
        "is_primary": True,
        "medium": medium if medium else "",
        "q": q if q else ""
    }
    if page and page_size:
        payload["page"] = page
        payload["page_size"] = page_size
    try:
        result = CRMExternalService.get_ua_accounts(payload, headers=header, **{'trace_id': trace_id})
    except Exception as e:
        return False, MyResponse(RET.THIRD_ERR, str(e))
    return True, result


def get_new_mediums(db, group_id):
    group_adv_mediums = db.query(GroupAccountRelationship.medium).filter(
        GroupAccountRelationship.project_group_id == group_id,
        GroupAccountRelationship.is_delete == False
    ).distinct().all()
    mediums = []
    for medium in group_adv_mediums:
        if medium[0]:
            mediums.append(medium[0])
    return mediums


def get_unauthorized_accounts_mediums(user_id, cus_id_list, group_id, trace_id=None):
    if not group_id:
        return False, MyResponse(RET.DATA_ERR, '未获取到组信息')
    header = {
        'advertiser_user_id': str(user_id)
    }
    payload = {
        "customer_ids": cus_id_list,
        "group_id": [group_id],
        "is_second": 1
    }
    try:
        result = CRMExternalService.get_account_mediums(
            payload,
            headers=header,
            **{'trace_id': trace_id}
        )
    except Exception as e:
        return False, MyResponse(RET.THIRD_ERR, str(e))
    return True, result


def generate_new_dict(medium, all_data):
    result = {}
    for key in medium:
        result[key] = [value for value in medium[key] if value in all_data[key]]
    return result
