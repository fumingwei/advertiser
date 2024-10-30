# -*- coding: utf-8 -*-
"""
统一队列任务启动接口
"""


def account_task(func, args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, **options):
    """
    邮件
    """
    func.apply_async(args=args, kwargs=kwargs, task_id=task_id, producer=producer, link=link, link_error=link_error,
                     queue="oms_email", **options)


def common_task(func, args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, **options):
    """
    公共异步，导出文件
    """
    func.apply_async(args=args, kwargs=kwargs, task_id=task_id, producer=producer, link=link, link_error=link_error,
                     queue='export_files', **options)
