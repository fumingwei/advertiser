# -*- coding: utf-8 -*-
"""
消息推送服务  这里只用短信

SMS_468905319

模板类型   验证码
模板名称   GatherOne验证码-客户自助系统-通用
模板CODE  SMS_468905319
模板内容  自助平台验证码为：${code}，请勿泄露给他人，5分钟内有效！
变量属性  code-仅数字；
关联签名  GatherOne
应用场景  https://www.gatherone.com
场景说明  https://oms.gatherone.com/ 注册验证码发送
创建时间  2024-07-09 18:32:58
"""
from pprint import pformat
from settings.base import configs
from random import randint
from settings.db import RedisClient
from libs.internal.base_service import BaseService, get_service_url
from tools.constant import RET
from settings.log import web_log
from core.customer_consul import CustomerConsul


class PushService(BaseService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app_secret = configs.ACCESSKEY_SECRET
        self.app_key = configs.ACCESSKEY_ID
        self.domestic_template_id = configs.DOMESTIC_TEMPLATE_ID
        self.foreign_template_id = configs.FOREIGN_TEMPLATE_ID

    @classmethod
    def ali_cloud_sms(cls, json, **kwargs):
        # url = f'{configs.PUSH_BASE_URL}/sms/ali_cloud_sms'
        my_consul = CustomerConsul()
        # 获取推送服务地址
        server_host, server_port = my_consul.discover_service('push')
        path = get_service_url('push', 'message_push')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, service='推送', **kwargs)

    def storage_sms_code(self, code_type, mobile, sms_code):
        redis_coon = RedisClient(db=configs.REDIS_STORAGE['sms_code']).get_redis_client()
        _code = redis_coon.exists(f'{code_type}_{mobile}')
        if _code == 1:
            return False, '验证码发送频繁'
        redis_coon.setex(f'{code_type}_{mobile}', 60 * 5, sms_code)
        redis_coon.close()
        return True, '验证码发送成功'

    def send_verification_code(self, code_type, phone, area_code, trace_id):
        # 生成验证码
        sms_code = '%06d' % randint(0, 999999)
        # 存redis
        send_res, send_msg = self.storage_sms_code(code_type, phone, sms_code)
        if send_res:
            # 发送
            data = {
                "message_type": "sms",
                "message_weight": "1",
                "message_content": {
                    "data_content": {"code": sms_code},
                    "template_id": self.domestic_template_id if area_code == '+86' else self.foreign_template_id,
                    "phone": phone,
                    "app_secret": self.app_secret,
                    "app_key": self.app_key
                }
            }
            web_log.log_error(f'调用推送服务请求参数：\n{pformat(data)}')
            self.ali_cloud_sms(json=data, **{'trace_id': trace_id})
        return RET.OK if send_res else RET.REQ_ERR, send_msg


"""
自己生成6位 code   存redis  string 前缀_手机号     用于后续校验
调用push 

template_id = ''
data_content  = {'code': '123456', 'msg': '1231241241'}
phone  = ''
ACCESSKEY_ID = ''
ACCESSKEY_SECRET = ''

你好，你的验证码是{code},请勿泄露给他人！

  
拿着手机号  + 验证码   ----》   redis   前缀_手机号==123456   ---》生成注册信息/修改密码/登录.....
"""
