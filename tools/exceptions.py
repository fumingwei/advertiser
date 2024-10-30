# -*- coding: utf-8 -*-

class GatheroneError(Exception):
    """
    自定义异常基类
    """
    pass


class InternalNetworkError(GatheroneError):
    """
    网络请求异常
    """
    pass


class InternalRequestError(GatheroneError):
    """
    内部服务之间调用异常
    """
    pass


class RedisEmptyError(GatheroneError):
    """
    Redis数据为空异常
    """
    pass
