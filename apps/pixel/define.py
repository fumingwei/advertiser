# -*- coding: utf-8 -*-
from tools.enum import BaseEnum


class AssetType(BaseEnum):
    """
    资产类型
    """
    BM = ('BM', 'BM_ID')
    ADVERTISING = ('广告账户', '广告账户ID')


class AdvertiserStatusResult(BaseEnum):
    DEFAULT = ('DEFAULT', '处理中')
    PART = ('PART', '部分成功')
    ALL_SUCCEED = ('ALL_SUCCEED', '全部成功')
    ALL_FAIL = ('ALL_FAIL', '全部失败')


# 操作结果
class OperateResult(BaseEnum):
    DEFAULT = ('0', '处理中')
    SUCCESS = ('1', '成功')
    FAIL = ('2', '失败')
