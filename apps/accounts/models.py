# -*- coding: utf-8 -*-
import inspect
import sys
from sqlalchemy.orm import class_mapper
from sqlalchemy.dialects.mysql import DATETIME
from settings.db import BaseModel
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR
from sqlalchemy import Column, Enum
from apps.accounts.define import Medium, BMGrantType, BCGrantType, \
    AdvertiserStatusResult, OperateResult
from tools.constant import Operation


# 账户重命名
class AccountRename(BaseModel):
    __tablename__ = 'cu_account_renames'

    account_id = Column(VARCHAR(32), comment='广告账户ID')
    medium = Column(Enum(*list(Medium.values())), comment='媒体类型')
    before_account_name = Column(VARCHAR(500), comment='广告账户更改前名称')
    after_account_name = Column(VARCHAR(500), comment='广告账户更改后名称')
    operate_result = Column(Enum(*list(OperateResult.values())), nullable=True,
                            default=OperateResult.DEFAULT.value, comment='操作结果')
    operate_time = Column(DATETIME, nullable=True, comment='操作时间')
    remark = Column(VARCHAR(2000), default="", comment='备注')
    user_id = Column(INTEGER(), comment="提交人id")
    request_id = Column(VARCHAR(100), comment='请求编号')


# BM账户主表(绑定/解绑)
class BmAccount(BaseModel):
    __tablename__ = 'cu_meta_bm_accounts'

    request_id = Column(VARCHAR(100), comment='请求编号')
    business_id = Column(VARCHAR(32), comment='商业账户ID')
    grant_type = Column(Enum(*list(BMGrantType.values())), nullable=True, comment='授权类型')
    operate_type = Column(Enum(*list(Operation.values())), default=Operation.BIND.value, comment='操作类型')
    operate_result = Column(Enum(*list(AdvertiserStatusResult.values())), nullable=True,
                            default=AdvertiserStatusResult.DEFAULT.value, comment='操作结果')
    operate_time = Column(DATETIME, nullable=True, comment='操作时间')
    user_id = Column(INTEGER(), comment="提交人id")


# BM账户子表(绑定/解绑)
class BmAccountDetail(BaseModel):
    __tablename__ = 'cu_meta_bm_account_details'

    account_id = Column(VARCHAR(32), comment='广告账户ID')
    operate_result = Column(Enum(*list(OperateResult.values())), nullable=True,
                            default=OperateResult.DEFAULT.value, comment='操作结果')
    operate_time = Column(DATETIME, nullable=True, comment='操作时间')
    remark = Column(VARCHAR(2000), default="", comment='备注')
    bm_account_id = Column(INTEGER, comment='bm账户主表id')


# BC账户主表(绑定/解绑)
class BcAccount(BaseModel):
    """
        grant_type == '1': API参数 permitted_tasks = ["ANALYZE"]
        grant_type == '2': API参数 permitted_tasks = ["OPERATOR"]
    """
    __tablename__ = 'cu_tiktok_bc_accounts'

    request_id = Column(VARCHAR(100), comment='请求编号')
    cooperative_id = Column(VARCHAR(32), comment='合作伙伴id')
    grant_type = Column(Enum(*list(BCGrantType.values())), nullable=True, comment='授权类型')
    operate_type = Column(Enum(*list(Operation.values())), default=Operation.BIND.value, comment='操作类型')
    operate_result = Column(Enum(*list(AdvertiserStatusResult.values())), nullable=True,
                            default=AdvertiserStatusResult.DEFAULT.value, comment='操作结果')
    operate_time = Column(DATETIME, nullable=True, comment='操作时间')
    user_id = Column(INTEGER(), comment="提交人id")


# BC账户子表(绑定/解绑)
class BcAccountDetail(BaseModel):
    __tablename__ = 'cu_tiktok_bc_accounts_details'

    account_id = Column(VARCHAR(32), comment='广告账户ID')
    business_id = Column(VARCHAR(32), comment='商业账户ID')
    operate_result = Column(Enum(*list(OperateResult.values())), nullable=True,
                            default=OperateResult.DEFAULT.value, comment='操作结果')
    operate_time = Column(DATETIME, nullable=True, comment='操作时间')
    remark = Column(VARCHAR(2000), default="", comment='备注')
    tiktok_bc_account_id = Column(INTEGER, comment='bc账户主表id')


classes = inspect.getmembers(sys.modules[__name__], inspect.isclass)
account_models = []
for name, cls in classes:
    try:
        class_mapper(cls)
        account_models.append(cls)
    except:
        continue
