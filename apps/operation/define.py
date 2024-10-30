from enum import Enum


# 定义操作类型的枚举
class OperationType(str, Enum):
    ACCOUNT_RECHARGE = "账户充值"
    ACCOUNT_RESET = "账户清零"
    BALANCE_TRANSFER = "账户转账"
    ACCOUNT_RENAME = "账户重命名"
    BC_ACCOUNT = "BC绑定/解绑"
    BM_ACCOUNT = "BM绑定/解绑"
    PIXEL = "Pixel"
    OPEN_ACCOUNT = "账户开户"


# 定义不同操作类型对应的状态枚举
class AccountRechargeStatus(str, Enum):
    PENDING = "充值中"
    APPROVED = "成功"
    REJECTED = "失败"  # 账户充值


class ResetStatus(str, Enum):
    PENDING = "清零中"
    APPROVED = "成功"
    REJECTED = "失败"  # 账户清零


class BalanceTransferStatus(str, Enum):
    ALL_SUCCESS = "成功"
    ALL_FAILED = "失败"
    IN_PROGRESS = "转移中"  # 余额转移


class RenameStatus(str, Enum):
    ALL_SUCCESS = "成功"
    ALL_FAILED = "失败"
    IN_PROGRESS = "处理中"  # 账户重命名


class BcBmPixelAccount(str, Enum):
    ALL_SUCCESS = "成功"
    ALL_FAILED = "失败"
    IN_PROGRESS = "处理中"


class OpenAccountStatus(str, Enum):
    PENDING = "审批中"
    APPROVED = "已通过"
    DISAPPROVED = "已被拒"
    CHANGES_REQUESTED = "需要修改"
    AUTO_DISAPPROVED = "被自动拒绝"


# 定义媒介类型枚举
class MediaType(str, Enum):
    X = "X"
    Tiktok = "Tiktok"
    Apple = "Apple"
    Petal = "Petal"
    Google = "Google"
    Kwai = "Kwai"
    Meta = "Meta"


gop = {
    AccountRechargeStatus.PENDING: "0",
    AccountRechargeStatus.REJECTED: "1",
    AccountRechargeStatus.APPROVED: "2",
    ResetStatus.PENDING: "0",
    ResetStatus.APPROVED: "1",
    ResetStatus.REJECTED: "2"
}
