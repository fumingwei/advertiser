# -*- coding: utf-8 -*-
from tools.enum import BaseEnum


class RegisterStatus(BaseEnum):
    WAIT = ("0", "待通过")
    AGREE = ("1", "已通过")
    REFUSE = ("2", "已拒绝")
    DISABLED = ("3", "已禁用")


class MediumOperationType:
    Meta = {
        "batch_operation": ["批量充值", "BM批量绑定/解绑", "批量清零", "批量重命名", "Pixel批量绑定/解绑"],
        "single_operation": ["账户充值", "BM绑定/解绑", "账户清零", "账户重命名", "账户转账", "Pixel绑定/解绑"]
    }
    Google = {
        "batch_operation": ["批量充值", "批量清零"],
        "single_operation": ["账户充值", "账户清零", "账户转账"]
    }
    Tiktok = {
        "batch_operation": ["批量充值", "BC批量绑定/解绑", "批量清零", "批量重命名"],
        "single_operation": ["账户充值", "BC绑定/解绑", "账户清零", "账户重命名", "账户转账"]
    }
