# -*- coding: utf-8 -*-
from tools.enum import BaseEnum


# 媒介
class Medium(BaseEnum):
    META = ("Meta", "Meta")
    GOOGLE = ("Google", "Google")
    TIKTOK = ("Tiktok", "Tiktok")
    KWAI = ("Kwai", "Kwai")
    X = ("X", "X")
    PETAL = ("Petal", "Petal")
    # TWITTER = ("Twitter", "Twitter")
    # APPLE = ("Apple", "Apple")


class BMGrantType(BaseEnum):
    ANALYZE = ("1", "仅报告:ANALYZE")
    ADVERTISE_ANALYZE = ("2", "一般用户:ADVERTISE/ANALYZE")
    MANAGE_ADVERTISE_ANALYZE = ("3", "管理:MANAGE/ADVERTISE/ANALYZE")


class BCGrantType(BaseEnum):
    """
    分析员是代投，操作员是自投
    """

    ANALYST = ("1", "分析员")
    OPERATOR = ("2", "操作员")


class AdvertiserStatusResult(BaseEnum):
    DEFAULT = ("DEFAULT", "处理中")
    PART = ("PART", "部分成功")
    ALL_SUCCEED = ("ALL_SUCCEED", "全部成功")
    ALL_FAIL = ("ALL_FAIL", "全部失败")


# 操作结果
class OperateResult(BaseEnum):
    DEFAULT = ("0", "处理中")
    SUCCESS = ("1", "成功")
    FAIL = ("2", "失败")


meta_account_status_object = {
    "1": "ACTIVE",
    "2": "DISABLED",
    "3": "UNSETTLED",
    "7": "PENDING_RISK_REVIEW",
    "8": "PENDING_SETTLEMENT",
    "9": "IN_GRACE_PERIOD",
    "100": "PENDING_CLOSURE",
    "101": "CLOSED",
    "201": "ANY_ACTIVE",
    "202": "ANY_CLOSED",
}


class MetaAccountStatus(BaseEnum):
    """
    Meta广告账户状态
    """

    ACTIVE = ("ACTIVE", "ACTIVE")
    DISABLED = ("DISABLED", "DISABLED")
    UNSETTLED = ("UNSETTLED", "其他")
    PENDING_RISK_REVIEW = ("PENDING_RISK_REVIEW", "其他")
    PENDING_SETTLEMENT = ("PENDING_SETTLEMENT", "其他")
    IN_GRACE_PERIOD = ("IN_GRACE_PERIOD", "其他")
    PENDING_CLOSURE = ("PENDING_CLOSURE", "其他")
    CLOSED = ("CLOSED", "其他")
    ANY_ACTIVE = ("ANY_ACTIVE", "其他")
    ANY_CLOSED = ("ANY_CLOSED", "其他")


class GoogleAccountStatus(BaseEnum):
    """
    谷歌账户状态
    """

    ENABLED = ("ENABLED", "ACTIVE")
    CANCELED = ("CANCELED", "其他")
    CLOSED = ("CLOSED", "其他")
    SUSPENDED = ("SUSPENDED", "其他")
    UNKNOWN = ("UNKNOWN", "其他")
    UNSPECIFIED = ("UNSPECIFIED", "其他")


class TiktokAccountStatus(BaseEnum):
    """
    Tiktok广告账户状态
    """

    STATUS_DISABLE = ("STATUS_DISABLE", "DISABLED")
    STATUS_PENDING_CONFRIM = ("STATUS_PENDING_CONFRIM", "其他")
    STATUS_PENDING_VERIFY = ("STATUS_PENDING_VERIFY", "其他")
    STATUS_CONFIRM_FAIL = ("STATUS_CONFIRM_FAIL", "其他")
    STATUS_ENABLE = ("STATUS_ENABLE", "ACTIVE")
    STATUS_CONFIRM_FAIL_END = ("STATUS_CONFIRM_FAIL_END", "其他")
    STATUS_PENDING_CONFIRM_MODIFY = ("STATUS_PENDING_CONFIRM_MODIFY", "其他")
    STATUS_CONFIRM_MODIFY_FAIL = ("STATUS_CONFIRM_MODIFY_FAIL", "其他")
    STATUS_LIMIT = ("STATUS_LIMIT", "其他")
    STATUS_WAIT_FOR_BPM_AUDIT = ("STATUS_WAIT_FOR_BPM_AUDIT", "其他")
    STATUS_WAIT_FOR_PUBLIC_AUTH = ("STATUS_WAIT_FOR_PUBLIC_AUTH", "其他")
    STATUS_SELF_SERVICE_UNAUDITED = ("STATUS_SELF_SERVICE_UNAUDITED", "其他")
    STATUS_CONTRACT_PENDING = ("STATUS_CONTRACT_PENDING", "其他")


class AllMediumAccountStatus(BaseEnum):
    """
    所有媒体广告账户状态
    """

    pass


medium_account_status_object = {
    "": "-",
    "-": "-",
    # Meta广告状态映射
    "1": "ACTIVE",
    "2": "DISABLED",
    "3": "其他",
    "7": "其他",
    "8": "其他",
    "9": "其他",
    "100": "DISABLED",
    "101": "DISABLED",
    "201": "其他",
    "202": "其他",
    # Google广告状态映射
    "ENABLED": "ACTIVE",
    "CANCELED": "DISABLED",
    "CLOSED": "其他",
    "SUSPENDED": "DISABLED",
    "UNKNOWN": "其他",
    "UNSPECIFIED": "其他",
    # Tiktok广告状态映射
    "STATUS_DISABLE": "DISABLED",
    "STATUS_PENDING_CONFRIM": "其他",
    "STATUS_PENDING_VERIFY": "其他",
    "STATUS_CONFIRM_FAIL": "其他",
    "STATUS_ENABLE": "ACTIVE",
    "STATUS_CONFIRM_FAIL_END": "其他",
    "STATUS_PENDING_CONFIRM_MODIFY": "其他",
    "STATUS_CONFIRM_MODIFY_FAIL": "其他",
    "STATUS_LIMIT": "DISABLED",
    "STATUS_WAIT_FOR_BPM_AUDIT": "其他",
    "STATUS_WAIT_FOR_PUBLIC_AUTH": "其他",
    "STATUS_SELF_SERVICE_UNAUDITED": "其他",
    "STATUS_CONTRACT_PENDING": "其他",
}

custom_account_status_object = {
    "ACTIVE": ["1", "ENABLED", "STATUS_ENABLE"],
    "DISABLED": ["2", "100", "101", "STATUS_DISABLE", "STATUS_LIMIT", "SUSPENDED", "CANCELED"],
    "其他": [
        # Meta 其他广告状态
        "3",
        "7",
        "8",
        "9",
        "201",
        "202",
        # Google 其他广告状态
        "CLOSED",
        "UNKNOWN",
        "UNSPECIFIED",
        # Tiktok 其他广告状态
        "STATUS_PENDING_CONFRIM",
        "STATUS_PENDING_VERIFY",
        "STATUS_CONFIRM_FAIL",
        "STATUS_CONFIRM_FAIL_END",
        "STATUS_PENDING_CONFIRM_MODIFY",
        "STATUS_CONFIRM_MODIFY_FAIL",
        "STATUS_WAIT_FOR_BPM_AUDIT",
        "STATUS_WAIT_FOR_PUBLIC_AUTH",
        "STATUS_SELF_SERVICE_UNAUDITED",
        "STATUS_CONTRACT_PENDING",
    ],
}
