from tools.constant import Operation
from pydantic import BaseModel, Field, validator, conlist
from tools.enum import Enum


# 操作类型
class PixelOperationSchemas(str, Enum):
    BIND = Operation.BIND.value
    UNBIND = Operation.UNBIND.value


# 操作Pixel
class OperationPixelSchemas(BaseModel):
    operation: PixelOperationSchemas
    pixel_id: str = Field(..., min_length=1, max_length=50)
    account_ids: conlist(dict, max_items=100)

    @validator('account_ids')
    def check_accounts_length(cls, v):
        if len(v) > 50:
            raise ValueError('单次提交限制50个账户，请减少数量后重试')
        return v

