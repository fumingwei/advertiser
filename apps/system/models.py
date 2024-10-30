# -*- coding: utf-8 -*-
import sys
import inspect
from settings.db import BaseModel
from sqlalchemy import Column, VARCHAR, Integer, Boolean, JSON, Enum
from sqlalchemy.orm import class_mapper


# 模块表
class Modules(BaseModel):
    __tablename__ = "cu_modules"
    module_code = Column(VARCHAR(50), comment="模块代码")
    module_name = Column(VARCHAR(50), comment="模块名称")


# 操作日志
class OperateLog(BaseModel):
    __tablename__ = "cu_operate_logs"

    module = Column(VARCHAR(100), default="", comment="系统模块")
    request_path = Column(VARCHAR(100), default="", comment="请求地址")
    request_user_id = Column(Integer, comment="操作人员")
    request_ip = Column(VARCHAR(50), default="", comment="操作IP")
    request_address = Column(VARCHAR(50), default="", comment="操作地址")
    request_status = Column(
        Enum("Success", "Error"), default="Success", comment="操作状态"
    )
    spent_time = Column(Integer, default=0, comment="消耗时间(毫秒)")
    session_id = Column(VARCHAR(100), default="", comment="会话编号")
    request_method = Column(VARCHAR(50), default="", comment="请求方式")
    required_params = Column(JSON(), nullable=True, comment="请求参数")
    return_params = Column(JSON(), nullable=True, comment="返回参数")
    operation = Column(VARCHAR(200), default="", comment="操作方法")
    operation_desc = Column(VARCHAR(50), default="", comment="操作描述")

    def __repr__(self):
        return "<OperateLog>{}:{}:{}".format(
            self.request_user_id, self.request_path, self.request_status
        )


class LoginHistory(BaseModel):
    __tablename__ = 'cu_login_histories'

    user_id = Column(Integer, nullable=True, comment='用户id')
    mobile = Column(VARCHAR(20), nullable=True, comment="手机号")
    device = Column(VARCHAR(50), default='', comment='操作系统')
    browser = Column(VARCHAR(50), default='', comment='浏览器')
    ip = Column(VARCHAR(50), default='', comment='ip地址')
    country = Column(VARCHAR(50), default='', comment='国家')
    request_status = Column(
        Enum("Success", "Error"), default="Success", comment="登录状态"
    )
    login_info = Column(VARCHAR(50), default='', comment='登录信息')

    def __repr__(self):
        return '<LoginHistory>{}:{}'.format(self.session_id, self.user_id)


classes = inspect.getmembers(sys.modules[__name__], inspect.isclass)
system_models = []
for name, cls in classes:
    try:
        class_mapper(cls)
        system_models.append(cls)
    except:
        continue
