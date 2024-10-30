# -*- coding: utf-8 -*-
from apps.system.views import SystemRouter
from apps.advertiser.views import CustomerRouter
from settings.base import configs
from fastapi import FastAPI


def router_init(app: FastAPI):
    from apps.accounts.views import AccountRouter
    from apps.finance.views import FinanceRouter
    from apps.onboarding.views import OpenAccountRouter
    from apps.callback.views import CallbackRouter
    from apps.pixel.views import PixelRouter
    from apps.workbench.views import WorkbenchRouter
    from apps.common.views import CommonRouter
    from apps.operation.views import OperationRouter
    from apps.report.views import ReportRouter

    # 注册路由
    # 账户管理
    app.include_router(AccountRouter, prefix=f"{configs.API_VERSION_STR}/account")
    # 系统管理
    app.include_router(SystemRouter, prefix=f"{configs.API_VERSION_STR}/systems")
    # 财务管理
    app.include_router(FinanceRouter, prefix=f"{configs.API_VERSION_STR}/finance")
    # 用户注册
    app.include_router(CustomerRouter, prefix=f"{configs.API_VERSION_STR}/advertiser")
    # 账户开户
    app.include_router(OpenAccountRouter, prefix=f"{configs.API_VERSION_STR}/onboarding")
    # 回调接口
    app.include_router(CallbackRouter, prefix=f"{configs.API_VERSION_STR}/callback")
    # pixel接口
    app.include_router(PixelRouter, prefix=f"{configs.API_VERSION_STR}/pixel")
    # 工作台接口
    app.include_router(WorkbenchRouter, prefix=f"{configs.API_VERSION_STR}/workbench")
    # 公共
    app.include_router(CommonRouter, prefix=f"{configs.API_VERSION_STR}/common")
    # 操作记录接口
    app.include_router(OperationRouter, prefix=f"{configs.API_VERSION_STR}/operation")
    # 数据报表
    app.include_router(ReportRouter, prefix=f"{configs.API_VERSION_STR}/report")
