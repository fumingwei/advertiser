# -*- coding: utf-8 -*-
from pydantic import BaseModel, Field
from tools.enum import Enum
from tools.constant import CodeType


class CodeTypeEnum(str, Enum):
    AUTHORIZED_ACCOUNT = CodeType.AUTHORIZED_ACCOUNT
    ADVERTISER_REGISTER = CodeType.ADVERTISER_REGISTER
    ADVERTISER_LOGIN = CodeType.ADVERTISER_LOGIN
    ADVERTISER_FORGET_PWD = CodeType.ADVERTISER_FORGET_PWD
    ADVERTISER_UPDATE_MOBILE = CodeType.ADVERTISER_UPDATE_MOBILE


class RegisterSchema(BaseModel):
    """
    注册
    """

    company_name: str = Field(..., min_length=1, max_length=50)
    code_type: CodeTypeEnum
    contact: str = Field(..., min_length=1, max_length=20)
    email: str = Field(..., regex=r"^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$")
    sms_code: str = Field(..., min_length=6, max_length=6)
    mobile: str = Field(..., min_length=1, max_length=20)


class LoginSchema(BaseModel):
    """
    登录
    """

    mobile: str = Field(..., min_length=1, max_length=20)
    password: str = Field(..., min_length=1)


class SmsLoginSchema(BaseModel):
    """
    短信验证码登录
    """

    mobile: str = Field(..., min_length=1, max_length=20)
    sms_code: str = Field(..., min_length=6, max_length=6)
    code_type: CodeTypeEnum


class AccreditSchema(BaseModel):
    """授权子账号"""

    username: str = Field(..., min_length=1, max_length=20)
    mobile: str = Field(..., min_length=1, max_length=20)
    code_type: CodeTypeEnum
    sms_code: str = Field(..., min_length=6, max_length=6)


class ChangeMobileSchema(BaseModel):
    """修改手机号"""

    mobile: str = Field(..., min_length=1, max_length=20)
    sms_code: str = Field(..., min_length=6, max_length=6)
    code_type: CodeTypeEnum


class ChangeEmailSchema(BaseModel):
    """修改邮箱"""

    email: str = Field(..., regex=r"^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$")


class ChangePasswordSchema(BaseModel):
    """修改密码"""

    password1: str = Field(..., min_length=1)
    password2: str = Field(..., min_length=1)
    old_password: str = Field(..., min_length=1)


class ResetPasswordSchema(BaseModel):
    """重置密码"""

    mobile: str = Field(..., min_length=1, max_length=20)
    password1: str = Field(..., min_length=1)
    password2: str = Field(..., min_length=1)
    code_type: CodeTypeEnum
    sms_code: str = Field(..., min_length=6, max_length=6)


class UserFeedbackSchema(BaseModel):
    """用户反馈"""

    content: str = Field(..., max_length=255)


class AccountsInfoSchema(BaseModel):
    """授权账户"""

    account_id: list
    accredit: int = Field(...)
    son_id: int = Field(...)


class ProjectGroupSchemas(BaseModel):
    """新建/编辑项目组"""

    name: str = Field(..., min_length=1, max_length=100)
    user_ids: list = None
    operation_type: list
    remark: str = Field(..., max_length=200)


class GroupAuthorizedSchema(BaseModel):
    """组授权账户"""
    account_ids: list = Field([])
    mediums: list = Field([])
    account_names: list = Field([])
    group_id: int
    target_medium: str = Field('')
    q: str = Field('')


class GroupUnauthorizedSchema(BaseModel):
    """组取消授权账户"""
    account_ids: list = Field([])
    group_id: int
    target_medium: str = Field('')
    q: str = Field('')
    start_time: str = Field('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$')
    end_time: str = Field('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$')
