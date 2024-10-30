import inspect
import sys
from sqlalchemy.orm import class_mapper
from sqlalchemy import Column, DECIMAL, BOOLEAN
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, ENUM
from settings.db import BaseModel
from tools.constant import ApproveResult
from apps.accounts.define import Medium
from apps.finance.define import SystemType, TransferTradeType, BalanceTransferStatus, TransferTradeResult, TransferType
from tools.constant import ExternalRequestStatus, InternalRequestStatus


class BalanceTransfer(BaseModel):
    __cn_tablename__ = "余额转移工单"
    __tablename__ = "cu_balance_transfers"

    customer_id = Column(INTEGER, index=True, comment="客户主键")
    user_id = Column(INTEGER, index=True, comment="申请人ID")
    transfer_status = Column(
        ENUM(*list(BalanceTransferStatus.descs())),
        default=BalanceTransferStatus.PENDING.desc,
        index=True,
        comment="转移状态",
    )
    medium = Column(VARCHAR(500), default="", comment="所有媒介")
    transfer_amount = Column(DECIMAL(10, 2), default=0, comment="转移总金额")
    remark = Column(VARCHAR(500), default="", comment="备注")


class BalanceTransferRequest(BaseModel):
    __cn_tablename__ = "余额转移接口调用记录表"
    __tablename__ = "cu_balance_transfer_requests"

    balance_transfer_id = Column(
        INTEGER(display_width=11), index=True, comment="余额转移工单主键"
    )
    internal_request_status = Column(
        VARCHAR(20),
        ENUM(*list(InternalRequestStatus.values())),
        index=True,
        comment="请求调用状态",
    )
    external_request_id = Column(VARCHAR(50), default="", comment="外部请求ID")
    external_request_status = Column(
        ENUM(*list(ExternalRequestStatus.values())),
        index=True,
        default=ExternalRequestStatus.EMPTY.value,
        comment="接口返回的请求状态",
    )
    actual_amount = Column(DECIMAL(10, 2), default=0, comment="实际交易金额")
    transfer_type = Column(
        ENUM(*list(TransferType.values())),
        default=TransferType.ACCOUNT.value,
        comment="转账类型"
    )
    trade_type = Column(
        VARCHAR(10), ENUM(*list(TransferTradeType.values())), index=True, comment="交易类型"
    )


class BalanceTransferDetail(BaseModel):
    __cn_tablename__ = "余额转移广告账户详情"
    __tablename__ = "cu_balance_transfer_details"

    balance_transfer_id = Column(
        INTEGER(display_width=11), index=True, comment="余额转移工单主键"
    )
    balance_transfer_request_id = Column(
        VARCHAR(50), index=True, default="", comment="余额转移主键"
    )
    account_id = Column(VARCHAR(50), index=True, comment="广告账户")
    medium = Column(ENUM(*list(Medium.values())), index=True, comment="广告账户媒介")
    bc_id = Column(VARCHAR(50), default="", comment="BC_ID")
    before_balance = Column(DECIMAL(10, 2), default=0, comment="交易前账户余额")
    after_balance = Column(DECIMAL(10, 2), default=0, comment="转出后账户余额")
    before_purse_balance = Column(DECIMAL(10, 2), nullable=True, comment="交易前钱包余额")
    after_purse_balance = Column(DECIMAL(10, 2), nullable=True, comment="转出后钱包余额")
    amount = Column(DECIMAL(10, 2), comment="交易金额")
    transfer_type = Column(
        ENUM(*list(TransferType.values())),
        default=TransferType.ACCOUNT.value,
        comment="转账类型"
    )
    trade_type = Column(
        VARCHAR(10),
        ENUM(*list(TransferTradeType.values())),
        index=True,
        comment="交易类型",
    )
    trade_result = Column(
        VARCHAR(10),
        ENUM(*list(TransferTradeResult.values())),
        default=TransferTradeResult.EMPTY.value,
        comment="广告账户充值结果结果"
    )
    remark = Column(VARCHAR(500), default="", comment="备注")
    order_num = Column(INTEGER(display_width=11), comment="排序字段")


# 工单
class WorkOrder(BaseModel):
    __cn_tablename__ = "审批工单"
    __tablename__ = "cu_work_orders"
    work_order_id = Column(VARCHAR(30), unique=True, index=True, comment="工单号")
    account_id = Column(VARCHAR(50), default="", comment="广告账户ID")
    flow_code = Column(VARCHAR(20), comment="代码")
    current_node = Column(INTEGER, comment="当前节点编号")
    apply_user_id = Column(INTEGER, comment="申请人id")
    approval_status = Column(
        ENUM(*list(ApproveResult.values())),
        default=ApproveResult.PROCESS.value,
        comment="审批状态",
    )
    remark = Column(VARCHAR(500), default="", comment="备注")
    is_special = Column(BOOLEAN, default=False, comment="是否特批工单")
    company_id = Column(INTEGER(display_width=11), nullable=True, comment="公司id")
    system_type = Column(
        ENUM(*list(SystemType.values())),
        default=SystemType.ADVERTISER.value,
        comment="系统类型",
    )


classes = inspect.getmembers(sys.modules[__name__], inspect.isclass)
finance_models = []
for name, cls in classes:
    try:
        class_mapper(cls)
        finance_models.append(cls)
    except Exception:
        continue
