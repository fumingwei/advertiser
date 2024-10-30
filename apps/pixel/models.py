# -*- coding: utf-8 -*-
import inspect
import sys
from sqlalchemy.orm import class_mapper
from sqlalchemy.dialects.mysql import DATETIME
from settings.db import BaseModel
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR
from sqlalchemy import Column, Enum
from apps.pixel.define import AdvertiserStatusResult, OperateResult
from tools.constant import Operation


# Pixel绑定广告账户主表(绑定/解绑)
class PixelAccount(BaseModel):
    __tablename__ = 'cu_meta_pixel_accounts'

    request_id = Column(VARCHAR(100), comment='请求编号')
    pixel_id = Column(VARCHAR(32), comment='Pixel_ID')
    operate_type = Column(Enum(*list(Operation.values())), default=Operation.BIND.value, comment='操作类型')
    operate_result = Column(Enum(*list(AdvertiserStatusResult.values())), nullable=True,
                            default=AdvertiserStatusResult.DEFAULT.value, comment='操作结果')
    binding_time = Column(DATETIME, nullable=True, comment='绑定时间')
    user_id = Column(INTEGER(), comment="提交人id")


# Pixel绑定广告账户子表(绑定/解绑)
class PixelAccountDetail(BaseModel):
    __tablename__ = 'cu_meta_pixel_accounts_detail'
    account_id = Column(VARCHAR(32), comment="广告账户_ID")
    operate_result = Column(Enum(*list(OperateResult.values())), nullable=True,
                            default=OperateResult.DEFAULT.value, comment='操作结果')
    binding_time = Column(DATETIME, nullable=True, comment='绑定时间')
    remark = Column(VARCHAR(2000), default="", comment='备注')
    pixel_account_id = Column(INTEGER, comment='Pixel,账户主表id')


classes = inspect.getmembers(sys.modules[__name__], inspect.isclass)
pixel_models = []
for name, cls in classes:
    try:
        class_mapper(cls)
        pixel_models.append(cls)
    except:
        continue
