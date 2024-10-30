# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

# 设置环境变量
os.environ.setdefault("GatherOne Advertiser", "settings.base.configs")


def make_celery():
    # 实例化
    app = Celery("gatherone_advertiser", include=[
        "apps.accounts.tasks",
        "apps.finance.tasks",
        "apps.workbench.tasks",
        "apps.callback.tasks",
        "apps.common.tasks"
    ])
    # 加载celery配置文件
    app.config_from_object("my_celery.config")

    return app


celery_app = make_celery()

if __name__ == "__main__":
    args = ["worker", "--loglevel=INFO"]
    celery_app.worker_main(argv=args)
