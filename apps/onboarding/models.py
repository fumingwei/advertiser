import sys
import uuid
import inspect
from sqlalchemy import Column, JSON
from sqlalchemy.orm import class_mapper
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER, ENUM, DATETIME, BOOLEAN
from settings.db import BaseModel
from apps.onboarding.define import OeApproveStatus, OeAccountStatus


# 账户开户（OE）
class OeOpenAccount(BaseModel):
    __tablename__ = "cu_oe_open_accounts"

    ticket_id = Column(VARCHAR(32), default=lambda: uuid.uuid4().hex, comment="工单号")
    customer_id = Column(INTEGER, comment="结算名称ID")
    oe_number = Column(VARCHAR(32), default="", comment="0E开户参考编号")
    chinese_legal_entity_name = Column(VARCHAR(50), default="", comment="开户营业执照名称")
    customer_type = Column(VARCHAR(32), default="", comment="客户类型")
    business_registration = Column(VARCHAR(1000), default="", comment="开户营业执照链接")
    main_industry = Column(VARCHAR(50), default="", comment="主行业")
    sub_industry = Column(VARCHAR(50), default="", comment="子行业")
    org_ad_account_count = Column(VARCHAR(50), default="", comment="广告账户拥有数")
    ad_account_limit = Column(VARCHAR(50), default="", comment="广告账户允许数量上限")
    english_business_name = Column(VARCHAR(100), default="", comment="法律实体英文名称")
    ad_accounts = Column(JSON, default=[], comment="广告账户")
    promotable_pages = Column(JSON, default=[], comment="公共主页")
    promotable_app_ids = Column(JSON, default=[], comment="应用编号")
    promotion_website = Column(JSON, default=[], comment="推广网站")
    ad_account_creation_request_id = Column(VARCHAR(100), default="", comment='广告账户创建请求')
    ad_account_creation_request_status = Column(
        ENUM(*list(OeAccountStatus.descs())),
        default=OeAccountStatus.EMPTY.desc,
        comment='广告账户创建状态'
    )
    approval_status = Column(
        ENUM(*list(OeApproveStatus.descs())),
        default=OeApproveStatus.PENDING.desc,
        comment="OE审批状态",
    )
    operate_asset_account_status = Column(BOOLEAN, default=False, comment="销售资产组账户绑定状态")
    operate_asset_account_id = Column(JSON, default=[], comment="销售资产组账户绑定主键数组")
    approval_time = Column(DATETIME, nullable=True, comment='审批时间')
    approval_user_id = Column(INTEGER, nullable=True, comment='审批人')
    remark = Column(VARCHAR(200), default="", comment="备注")
    user_id = Column(INTEGER, comment="提交人ID")


classes = inspect.getmembers(sys.modules[__name__], inspect.isclass)
onboarding_models = []
for name, cls in classes:
    try:
        class_mapper(cls)
        onboarding_models.append(cls)
    except Exception:
        continue
