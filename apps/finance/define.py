# -*- coding: utf-8 -*-
from tools.enum import BaseEnum


class SystemType(BaseEnum):
    CRM = ("1", "CRM系统")
    ADVERTISER = ("2", "客户自助系统")


# 余额转移 转账类型
class TransferType(BaseEnum):
    ACCOUNT = ("1", "账户")
    PURSE = ("2", "钱包")


# 余额转移 交易类型
class TransferTradeType(BaseEnum):
    RECHARGE = ("RECHARGE", "充值")
    REFUND = ("REFUND", "退款")


class TransferTradeResult(BaseEnum):
    """
    余额转移交易状态
    对应媒体API的recharge_result字段
    """
    EMPTY = ("Empty", "未操作")
    DEFAULT = ("0", "转移中")
    SUCCESS = ("1", "成功")
    FAILURE = ("2", "失败")


# 余额转移 状态
class BalanceTransferStatus(BaseEnum):
    PENDING = ("PENDING", "进行中")
    # SUCCESS = ("SUCCESS", "成功")
    # FAILURE = ("FAILURE", "失败")
    # MANUAL = ("MANUAL", "人工处理")
    PARTIAL_SUCCESS = ("PARTIAL_SUCCESS", "部分成功")
    COMPLETE_SUCCESS = ("COMPLETE_SUCCESS", "全部成功")
    COMPLETE_FAILURE = ("COMPLETE_FAILURE", "全部失败")


balance_transfer_result_status = {
    "成功": ["全部成功", "部分成功"],
    "失败": ["全部失败", "部分成功"],
    "转移中": ["进行中"]
}


class MetaAccountStatus:
    ACTIVE = "ACTIVE"
    DISABLED = "DISABLED"
    UNSETTLED = "UNSETTLED"
    PENDING_RISK_REVIEW = "PENDING_RISK_REVIEW"
    PENDING_SETTLEMENT = "PENDING_SETTLEMENT"
    IN_GRACE_PERIOD = "IN_GRACE_PERIOD"
    PENDING_CLOSURE = "PENDING_CLOSURE"
    CLOSED = "CLOSED"
    ANY_ACTIVE = "ANY_ACTIVE"
    ANY_CLOSED = "ANY_CLOSED"


class GoogleAccountStatus:
    """
    Google广告账户的状态

    ENABLED: 账户正常
    除ENABLED外，其他状态均为异常状态，都需要进行CRM客户钱包充值和账户清零操作
    """

    ENABLED = 'ENABLED'
    CANCELED = 'CANCELED'
    CLOSED = 'CLOSED'
    SUSPENDED = 'SUSPENDED'
    UNKNOWN = 'UNKNOWN'
    UNSPECIFIED = 'UNSPECIFIED'


class TiktokAccountStatus:
    """
    Tiktok广告账户状态
    """

    STATUS_DISABLE = "STATUS_DISABLE"
    STATUS_PENDING_CONFRIM = "STATUS_PENDING_CONFRIM"
    STATUS_PENDING_VERIFY = "STATUS_PENDING_VERIFY"
    STATUS_CONFIRM_FAIL = "STATUS_CONFIRM_FAIL"
    STATUS_ENABLE = "STATUS_ENABLE"
    STATUS_CONFIRM_FAIL_END = "STATUS_CONFIRM_FAIL_END"
    STATUS_PENDING_CONFIRM_MODIFY = "STATUS_PENDING_CONFIRM_MODIFY"
    STATUS_CONFIRM_MODIFY_FAIL = "STATUS_CONFIRM_MODIFY_FAIL"
    STATUS_LIMIT = "STATUS_LIMIT"
    STATUS_WAIT_FOR_BPM_AUDIT = "STATUS_WAIT_FOR_BPM_AUDIT"
    STATUS_WAIT_FOR_PUBLIC_AUTH = "STATUS_WAIT_FOR_PUBLIC_AUTH"
    STATUS_SELF_SERVICE_UNAUDITED = "STATUS_SELF_SERVICE_UNAUDITED"
    STATUS_CONTRACT_PENDING = "STATUS_CONTRACT_PENDING"


if __name__ == "__main__":
    print(SystemType.ADVERTISER.value)
