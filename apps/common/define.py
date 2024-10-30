# -*- coding: utf-8 -*-
from tools.enum import BaseEnum
from tools.constant import OperationType


# 文件状态
class FileStatus(BaseEnum):
    PROCESS = ('PROCESS', '生成中')
    SUCCEED = ('SUCCEED', '成功')
    FAIL = ('FAIL', '失败')


class OperateTypeDefine(BaseEnum):
    """
    所有导出操作
    """
    RECHARGE = (OperationType.RECHARGE, '账户充值')
    RESET = (OperationType.RESET, '账户清零')
    BalanceTransfer = (OperationType.BALANCETRANSFER, '账户转账')
    RENAME = (OperationType.ACCOUNTRENAME, '账户重命名')
    PixelBindAccount = (OperationType.PIXEL, 'PIXEL')
    BmBindAccount = (OperationType.BMACCOUNT, 'BM绑定')
    BcBindAccount = (OperationType.BCACCOUNT, 'BC绑定')
    ACCOUNTLIST = ('账户列表', '账户列表')
    # ALL = ('ALL', '所有操作记录')
    BillSummary = ('账单总览', '账单总览')
    BillDetail = ('账单总览查看明细', '账单总览查看明细')
    Rebate = ('返点使用记录', '返点使用记录')
    AccountInfo = ('账户信息', '账户信息')
    OpenAccountHistory = ('开户历史', '开户历史')


class EXPORTOperationType:
    """
    操作记录不要账户列表和余额转移
    """
    RECHARGE = "账户充值"
    RESET = "账户清零"
    BALANCETRANSFER = "账户转账"
    ACCOUNTRENAME = "账户重命名"
    BMACCOUNT = "BM绑定/解绑"
    BCACCOUNT = "BC绑定/解绑"
    PIXEL = "Pixel"
    # ACCOUNTLIST = "账户列表"
