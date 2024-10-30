from typing import List
from pydantic import constr, Field, conlist, validator
from tools.constant import Operation
from apps.accounts.define import BMGrantType
from pydantic import BaseModel
from tools.enum import Enum


# 操作类型
class PixelOperationSchemas(str, Enum):
    BIND = Operation.BIND.value
    UNBIND = Operation.UNBIND.value


class BMGrantTypeSchemas(str, Enum):
    ANALYZE = BMGrantType.ANALYZE.value
    ADVERTISE_ANALYZE = BMGrantType.ADVERTISE_ANALYZE.value
    MANAGE_ADVERTISE_ANALYZE = BMGrantType.MANAGE_ADVERTISE_ANALYZE.value


# 提交的账户重命名信息
class AccountRenameSubmitSchemas(BaseModel):
    account_id: str = Field(..., min_length=1, max_length=32)
    after_account_name: str = Field(..., min_length=1, max_length=500)


# 账户重命名信息
class AccountRenameListSchemas(BaseModel):
    account_id: str = Field(..., min_length=1, max_length=32)
    before_account_name: str = Field(None, min_length=1, max_length=500)
    after_account_name: str = Field(..., min_length=1, max_length=500)


# 提交账户重命名
class SubmitAccountRenameSchemas(BaseModel):
    account_rename: conlist(AccountRenameSubmitSchemas, max_items=100)

    @validator('account_rename')
    def check_accounts_length(cls, v):
        if len(v) > 50:
            raise ValueError('单次提交限制50个账户，请减少数量后重试')
        return v


# 账户重命名
class AccountRenameSchemas(BaseModel):
    account_rename: conlist(AccountRenameListSchemas, max_items=100)

    @validator('account_rename')
    def check_accounts_length(cls, v):
        if len(v) > 50:
            raise ValueError('单次提交限制50个账户，请减少数量后重试')
        return v


# 筛选 开始时间-结束时间
class DateTimeSchema(BaseModel):
    start_date: constr(regex="^[\d]{4}-[\d]{2}-[\d]{2}$") = None
    end_date: constr(regex="^[\d]{4}-[\d]{2}-[\d]{2}$") = None


# bm导出
class BmExportSchemas(DateTimeSchema):
    bm_id: str = Field(None, min_length=1, max_length=32)


# bm获取授权类型,bc获取访问权限
class BmAccountGrantTypeSchema(BaseModel):
    account_ids: list


# BM解绑/绑定
class BMOperateSchemas(BaseModel):
    operation: PixelOperationSchemas
    business_id: str = Field(..., min_length=1, max_length=32)
    account_ids: conlist(dict, max_items=100)
    grant_type: BMGrantTypeSchemas = None

    @validator('account_ids')
    def check_accounts_length(cls, v):
        if len(v) > 50:
            raise ValueError('单次提交限制50个账户，请减少数量后重试')
        return v


# bc导出
class BcExportSchemas(DateTimeSchema):
    cooperative_id: str = Field(None, min_length=1, max_length=32)


# BC解绑/绑定
class BCOperateSchemas(BaseModel):
    operation: PixelOperationSchemas
    cooperative_id: str = Field(..., min_length=1, max_length=32)
    account_ids: conlist(dict, max_items=50)
    grant_type: BMGrantTypeSchemas = None


# 导出账户列表
class ExportAccountManage(BaseModel):
    medium: str = Field(None, min_length=1, max_length=20)
    start_date: constr(regex="^[\d]{4}-[\d]{2}-[\d]{2}$") = None
    end_date: constr(regex="^[\d]{4}-[\d]{2}-[\d]{2}$") = None
