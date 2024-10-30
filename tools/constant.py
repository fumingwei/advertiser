# -*- coding: utf-8 -*-
from tools.enum import BaseEnum


class RET:
    OK = 0

    DB_ERR = 4001
    NO_DATA = 4002
    DATA_EXIST = 4003
    DATA_ERR = 4004
    INVALID_DATA = 4005

    SESSION_ERR = 4101
    LOGIN_ERR = 4102
    PARAM_ERR = 4103
    USER_ERR = 4104
    ROLE_ERR = 4105
    PWD_ERR = 4106
    CODE_ERR = 4107
    BIND_ERR = 4108
    PER_ERR = 4109
    PHONE_EXISTED = 4110
    INVITATION_CODE_ERR = 4111
    EMAIL_EXISTED = 4112
    CHANGE_FAILED = 4113
    QR_EXPIRED = 4114
    NO_SCAN = 4115
    WS_DISCONNECTED = 4116

    REQ_ERR = 4201
    IP_ERR = 4202
    THIRD_ERR = 4301
    IO_ERR = 4302

    SERVER_ERR = 4500
    UNKNOW_ERR = 4501
    EXPORT_ERR = 4502
    NO_SYSTEM_PERMISSION = 4503

    INTERNAL_NETWORK_ERR = 4600
    INTERNAL_REQUEST_ERR = 4601


error_map = {
    RET.OK: "成功",
    RET.DB_ERR: "数据库查询错误",
    RET.NO_DATA: "无数据",
    RET.INVALID_DATA: "无效数据",
    RET.DATA_EXIST: "数据已存在",
    RET.DATA_ERR: "数据错误",
    RET.SESSION_ERR: "用户未登录",
    RET.LOGIN_ERR: "用户登录失败",
    RET.PARAM_ERR: "参数错误",
    RET.USER_ERR: "用户不存在或未激活",
    RET.ROLE_ERR: "用户身份错误",
    RET.PWD_ERR: "密码错误",
    RET.CODE_ERR: "验证码错误",
    RET.BIND_ERR: "未绑定系统用户",
    RET.PER_ERR: "权限错误",
    RET.PHONE_EXISTED: "手机号已被注册",
    RET.INVITATION_CODE_ERR: "邀请码错误",
    RET.EMAIL_EXISTED: "邮箱已被注册",
    RET.CHANGE_FAILED: "修改失败",
    RET.QR_EXPIRED: "二维码失效",
    RET.NO_SCAN: "未扫码",
    RET.WS_DISCONNECTED: "WebSocket断开连接",
    RET.REQ_ERR: "非法请求或请求次数受限",
    RET.IP_ERR: "IP受限",
    RET.THIRD_ERR: "第三方系统错误",
    RET.IO_ERR: "文件读写错误",
    RET.SERVER_ERR: "内部错误",
    RET.UNKNOW_ERR: "未知错误",
    RET.EXPORT_ERR: "导出错误",
    RET.NO_SYSTEM_PERMISSION: "无系统权限",
    RET.INTERNAL_NETWORK_ERR: "内部网络错误",
    RET.INTERNAL_REQUEST_ERR: "内部请求错误",
}


class ApproveResult(BaseEnum):
    PROCESS = ("1", "审批中")
    AGREE = ("2", "已通过")
    REJECT = ("3", "已被拒")


class Operation(BaseEnum):
    BIND = ("1", "绑定")
    UNBIND = ("2", "解绑")


class ExternalRequestStatus(BaseEnum):
    """
    外部服务请求状态, 对应媒体API的reqeust_status字段
    """
    EMPTY = ("Empty", "未操作")
    RECEIVED = ("Received", "已接收")
    RUNNING = ("Running", "运行中")
    FINISHED = ("Finished", "已完成")


# 内部服务请求状态
class InternalRequestStatus(BaseEnum):
    REQUEST_REDAY = ("REQUEST_REDAY", "准备调用内部服务")
    REQUEST_SUCCESS = ("REQUEST_SUCCESS", "调用内部服务成功")
    REQUEST_FAILURE = ("REQUEST_FAILURE", "调用内部服务失败")


class CodeType:
    # 客户自助系统专用code
    AUTHORIZED_ACCOUNT = "authorized_account"
    ADVERTISER_REGISTER = "advertiser_register"
    ADVERTISER_LOGIN = "advertiser_login"
    ADVERTISER_FORGET_PWD = "advertiser_forget_pwd"
    ADVERTISER_UPDATE_MOBILE = "advertiser_update_mobile"


# 登录描述
class LoginDesc:
    USERNOTEXIST = "用户不存在"
    NOTACTIVE = "用户注册申请未通过或已被禁用"
    NOCUSTOMERS = "未授权客户"
    PASSWORDERROR = "密码错误"
    LOGINSUCCESS = "登录成功"


# 登录状态
class LoginStatus:
    SUCCESS = "Success"
    ERROR = "Error"


class OperationType:
    RECHARGE = "账户充值"
    RESET = "账户清零"
    ACCOUNTRENAME = "账户重命名"
    BMACCOUNT = "BM绑定/解绑"
    BCACCOUNT = "BC绑定/解绑"
    PIXEL = "Pixel绑定/解绑"
    BALANCETRANSFER = "账户转账"
    # OPENACCOUNT = "账户开户"


class EnOperationType:
    RECHARGE = ("账户充值", "Recharge")
    RESET = ("账户清零", "Reset")
    ACCOUNTRENAME = ("账户重命名", "Rename")
    BMACCOUNT = ("BM绑定/解绑", "BM bind/unbind")
    BCACCOUNT = ("BC绑定/解绑", "BC bind/unbind")
    PIXEL = ("Pixel绑定/解绑", "Pixel bind/unbind")
    BALANCETRANSFER = ("账户转账", "Transfer")


batch_operation_type = {
    "账户充值": "批量充值",
    "BM绑定/解绑": "BM批量绑定/解绑",
    "账户清零": "批量清零",
    "账户重命名": "批量重命名",
    "Pixel绑定/解绑": "Pixel批量绑定/解绑",
    "BC绑定/解绑": "BC批量绑定/解绑",
    "账户转账": "账户转账"
}

all_operate_results = {
    '账户充值': {
        '充值中': '0',
        '成功': '1',
        '失败': '2'
    },
    '账户清零': {
        '清零中': '0',
        '成功': '1',
        '失败': '2'
    },
    '账户转账': {
        '进行中': '进行中',
        '全部成功': '全部成功',
        '部分成功': '部分成功',
        '全部失败': '全部失败'
    },
    '账户重命名': {
        '处理中': '0',
        '成功': '1',
        '失败': '2'
    },
    'BC绑定/解绑': {
        '处理中': '0',
        '成功': '1',
        '失败': '2'
    },
    'BM绑定/解绑': {
        '处理中': '0',
        '成功': '1',
        '失败': '2'
    },
    'Pixel绑定/解绑': {
        '处理中': '0',
        '成功': '1',
        '失败': '2'
    },
    '账户开户': {
        '审批中': '审批中',
        '已通过': '已通过',
        '已被拒': '已被拒',
        '被自动拒绝': '被自动拒绝',
        '需要修改': '需要修改'
    },
}

all_operate_results_english = {
    "充值中": "Recharge Is In Progress",
    "清零中": "Reset Is In Progress",
    "处理中": "In Process",
    "进行中": "Under Way",
    "成功": "Success",
    "失败": "Fail",
    "全部成功": "All Success",
    "部分成功": "Partial Success",
    "全部失败": "All Fail",
    "审批中": "Approval In Progress",
    "已通过": "Already Passed",
    "已被拒": "Already Declined",
    "需要修改": "Need To Modify",
    "被自动拒绝": "Be Automatically Rejected"
}
