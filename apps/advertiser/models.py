# -*- coding: utf-8 -*-
from apps.advertiser.define import RegisterStatus
from settings.db import BaseModel
from sqlalchemy import JSON, Enum, Boolean, Column
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, BOOLEAN


class AdvertiserRegister(BaseModel):
    __tablename__ = "cu_advertiser_registers"

    company_name = Column(VARCHAR(255), default="", comment="公司名称")
    contact = Column(VARCHAR(20), default="", comment="联系人")
    email = Column(VARCHAR(50), default="", comment="邮箱")
    mobile = Column(VARCHAR(20), comment="手机号")
    password = Column(VARCHAR(500), default="", comment="密码", nullable=True)
    status = Column(Enum(*list(RegisterStatus.values())), default="0", comment="状态")
    reason = Column(VARCHAR(500), default="", comment="拒绝原因", nullable=True)
    pass_time = Column(VARCHAR(255), nullable=True, default="", comment="通过时间")
    user_id = Column(
        INTEGER(display_width=11), nullable=True, comment="用户id", default=None
    )
    is_second = Column(BOOLEAN, default=False, comment='是否二代客户')

    def __repr__(self):
        return "<User>{}:{}".format(self.contact, self.mobile)


# 客户授权信息
class UserCusRelationship(BaseModel):
    __tablename__ = "cu_user_cus_relationship"
    company_id = Column(INTEGER(display_width=11), comment="公司id")
    customer_id = Column(JSON(), default=[], comment="客户id")
    auth_num = Column(INTEGER(), default=5, comment="可授权账户总数")


class AdvertiserUser(BaseModel):
    __tablename__ = "cu_advertiser_users"

    mobile = Column(VARCHAR(20), unique=True, comment="手机号")
    real_name = Column(VARCHAR(20), default="", comment="真实姓名")
    email = Column(VARCHAR(50), default="", comment="电子邮箱")
    is_active = Column(Boolean, default=1, comment="状态，是否可用，0-不可用，1-可用")
    password = Column(VARCHAR(500), default="", comment="密码", nullable=True)
    p_id = Column(INTEGER(display_width=11), nullable=True, comment="上级id")
    company_id = Column(INTEGER(display_width=11), nullable=True, comment="公司id")
    avatar_url = Column(VARCHAR(255), default="", comment="头像")
    is_open = Column(BOOLEAN, default=True, comment='是否打开')


# 用户反馈信息
class UserFeedback(BaseModel):
    __tablename__ = "cu_user_feedback"

    content = Column(VARCHAR(255), nullable=True, default="", comment="反馈内容")
    user_id = Column(
        INTEGER(display_width=11), comment="用户id", default=None
    )


# 项目组
class ProjectGroup(BaseModel):
    __tablename__ = "cu_project_groups"
    project_name = Column(VARCHAR(100), default="", comment="项目组名称")
    operation_type = Column(JSON(), default=[], comment="操作类型")
    mediums = Column(JSON(), default=[], comment="投放媒介")
    remark = Column(VARCHAR(200), default="", comment="备注")
    company_id = Column(INTEGER(display_width=11), comment="创建人id")


# 项目组和成员关系表
class GroupMemberRelationship(BaseModel):
    __tablename__ = "cu_group_member_relationship"
    project_group_id = Column(INTEGER(display_width=11), comment="项目组id")
    user_id = Column(INTEGER(display_width=11), comment="子账号id")


class GroupAccountRelationship(BaseModel):
    __tablename__ = "cu_group_account_relationship"
    project_group_id = Column(INTEGER(display_width=11), comment="项目组id")
    account_id = Column(VARCHAR(50), comment="广告账户id")
    account_name = Column(VARCHAR(100), comment="广告账户名称")
    medium = Column(VARCHAR(20), comment="投放媒介")
