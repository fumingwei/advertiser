from pydantic import BaseModel, Field, validator, conlist
from pydantic import constr


class AccountRechargeModel(BaseModel):
    account_id: str = Field(..., min_length=1, max_length=50)
    # recharge_num: float = Field(gt=-99999999.99, le=99999999.99)
    recharge_num: str = Field(..., min_length=1, max_length=10)


# 充值信息
class AddRechargeModel(BaseModel):
    accounts: conlist(AccountRechargeModel, max_items=100)

    @validator('accounts')
    def check_accounts_length(cls, v):
        if len(v) > 50:
            raise ValueError('单次提交限制50个账户，请减少数量后重试')
        return v


class AddResetDetailModel(BaseModel):
    account_id: str = Field(..., min_length=1, max_length=50)


# 账户清零
class AddResetModel(BaseModel):
    accounts: conlist(AddResetDetailModel, max_items=100)

    @validator('accounts')
    def check_accounts_length(cls, v):
        if len(v) > 50:
            raise ValueError('单次提交限制50个账户，请减少数量后重试')
        return v


class AccountMediumDetailModel(BaseModel):
    account_id: str = Field(..., min_length=1, max_length=50)
    num: float = None


# 通过账户匹配客户和媒介
class AccountMatchModel(BaseModel):
    accounts: conlist(AccountMediumDetailModel, max_items=100)

    @validator('accounts')
    def check_accounts_length(cls, v):
        if len(v) > 50:
            raise ValueError('单次提交限制50个账户，请减少数量后重试')
        return v


# 充值列表导出
class RechargeExportModel(BaseModel):
    customer_id: int = None
    date_start: constr(regex="^[\d]{4}-[\d]{2}-[\d]{2}$") = None
    date_end: constr(regex="^[\d]{4}-[\d]{2}-[\d]{2}$") = None


# 单个账户充值记录导出
class SingleRechargeExportModel(BaseModel):
    account_id: str = Field(..., min_length=1, max_length=50)
    customer_id: str = ""
    start_date: constr(regex="^[\d]{4}-[\d]{2}-[\d]{2}$") = None
    end_date: constr(regex="^[\d]{4}-[\d]{2}-[\d]{2}$") = None


# 余额转移账户
class BalanceTransferVerifySchema(BaseModel):
    refund_accounts: list[str]  # 退款账户ID列表
    recharge_accounts: list[str]  # 充值账户ID列表


# 余额转移 账户转钱包
class AccountTransferPurseSchema(BaseModel):
    account_id: str
    transfer_amount: str = Field(..., regex=r'^(?!\.?$)\d{1,11}(\.\d{1,2})?$')


class BalanceTransferRefuncdSchema(BaseModel):
    account_id: str
    bc_id: str
    medium: str
    available_balance: str
    amount: str


class BalanceTransferRechargeSchema(BaseModel):
    account_id: str
    bc_id: str
    medium: str
    available_balance: str
    amount: str


class BalaneTransferPostSchema(BaseModel):
    refund_accounts: list[BalanceTransferRefuncdSchema]
    recharge_accounts: list[BalanceTransferRechargeSchema]


class BalanceTransferExportSchema(BaseModel):
    """余额转移导出Schema"""
    customer_id: str = ""
    date_start: str = ""
    date_end: str = ""


class ResetExportModel(BaseModel):
    customer_id: str = ""
    date_start: constr(regex="^[\d]{4}-[\d]{2}-[\d]{2}$") = None
    date_end: constr(regex="^[\d]{4}-[\d]{2}-[\d]{2}$") = None


class AccountBalanceTransferSchema(BaseModel):
    refund_accounts: list[str]
    recharge_accounts: list[str]
    amount: float = Field(..., gt=0)

    @validator('recharge_accounts')
    def check_names_not_empty(cls, v, values):
        if len(v) != 1 or len(values.get('refund_accounts')) != 1:
            raise ValueError('账户数量错误')
        else:
            return v
