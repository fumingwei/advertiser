# -*- coding: utf-8 -*-
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from middlewares import middleware_init
from settings.routers import router_init
from settings.base import configs


def create_app():
    app = FastAPI()

    # 初始化中间件
    middleware_init(app)

    # 初始化路由
    router_init(app)

    # 注册静态文件目录
    # app.mount(f"{configs.API_VERSION_STR}/static", StaticFiles(directory="static"),name="static")
    return app
