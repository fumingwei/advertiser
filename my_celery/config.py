# -*- coding: utf-8 -*-
from settings.base import configs
from kombu import Exchange, Queue
from celery.beat import crontab
from celery.schedules import crontab, timedelta

MQ_USERNAME = configs.MQ_USERNAME
MQ_PASSWORD = configs.MQ_PASSWORD
MQ_VIRTUAL_HOST = configs.MQ_VIRTUAL_HOST
MQ_HOST = configs.MQ_HOST
# worker
broker_url = f"amqp://{MQ_USERNAME}:{MQ_PASSWORD}@{MQ_HOST}/{MQ_VIRTUAL_HOST}"
# result_store
result_backend = (
    f"redis://:{configs.REDIS_PASSWORD}@{configs.REDIS_HOST}:{configs.REDIS_PORT}/15"
)
# 用于存储计划的 Redis 服务器的 URL，默认为 broker_url的值
redbeat_redis_url = (
    f"redis://:{configs.REDIS_PASSWORD}@{configs.REDIS_HOST}:{configs.REDIS_PORT}/14"
)
# 时区
timezone = "Asia/Shanghai"
# UTC
enable_utc = False
# celery内容等消息的格式设置，默认json
accept_content = [
    "application/json",
]
task_serializer = "json"
result_serializer = "json"
# 为任务设置超时时间，单位秒。超时即中止，执行下个任务。
task_time_limit = 60
# 为存储结果设置过期日期，默认1天过期。如果beat开启，Celery每天会自动清除。
# 设为0，存储结果永不过期
result_expires = 300
# Worker并发数量，一般默认CPU核数，可以不设置
worker_concurrency = 5
# 每个worker执行了多少任务就会死掉，默认是无限的
# 防止内存泄漏
worker_max_tasks_per_child = 20
# 断开重连
broker_connection_retry_on_startup = True
# 定时任务
# beat_scheduler = "redbeat.RedBeatScheduler"

# 任务前缀
# redbeat_key_prefix = 'redbeat'
# RedBeat 使用分布式锁来防止多个实例同时运行。要禁用此功能，请设置：
# redbeat_lock_key = None
# # 配置交换机
exchanges = {
    "update_accounts": Exchange("update_accounts", type="direct"),
    "balance_transfer_refund": Exchange("balance_transfer_refund", type="direct"),
    "balance_transfer_recharge": Exchange("balance_transfer_recharge", type="direct"),
    "account_transfer_purse": Exchange("account_transfer_purse", type="direct"),
    "get_account_infos": Exchange("get_account_infos", type="direct"),
    "mapi_request_result": Exchange("mapi_request_result", type="direct"),
    "export_files": Exchange("export_files", type="direct"),
}
# 配置队列
queues = (
    Queue(
        name="update_accounts",
        exchange=exchanges["update_accounts"],
        routing_key="update_accounts",
    ),  # 回调接口队列
    Queue(
        name="balance_transfer_refund",
        exchange=exchanges["balance_transfer_refund"],
        routing_key="balance_transfer_refund",
    ),  # 余额转移退款结果队列
    Queue(
        name="balance_transfer_recharge",
        exchange=exchanges["balance_transfer_recharge"],
        routing_key="balance_transfer_recharge",
    ),  # 余额转移充值结果队列
    Queue(
        name="account_transfer_purse",
        exchange=exchanges["account_transfer_purse"],
        routing_key="account_transfer_purse",
    ),  # 余额转移充值结果队列
    Queue(
        name="get_account_infos",
        exchange=exchanges["get_account_infos"],
        routing_key="get_account_infos",
    ),  # 存储工作台广告账户信息队列
    Queue(
        name="mapi_request_result",
        exchange=exchanges["mapi_request_result"],
        routing_key="mapi_request_result",
    ),  # 获取mpi处理结果队列
    Queue(
        name="export_files",
        exchange=exchanges["export_files"],
        routing_key="export_files"
    )  # 导出文件
)
# 定时任务
beat_schedule = {
    "update_accounts": {
        "task": "update_accounts",
        "schedule": timedelta(seconds=30),
        "args": (),
        "kwargs": {},
        "options": {"queue": "update_accounts"},
    },
    "beat_balance_transfer_refund_result": {
        "task": "beat_balance_transfer_refund_result",
        "schedule": timedelta(seconds=30),
        "args": (),
        "kwargs": {},
        "options": {"queue": "balance_transfer_refund"},
    },
    "beat_balance_transfer_recharge_result": {
        "task": "beat_balance_transfer_recharge_result",
        "schedule": timedelta(seconds=30),
        "args": (),
        "kwargs": {},
        "options": {"queue": "balance_transfer_recharge"},
    },
    "beat_account_transfer_purse_result": {
        "task": "beat_account_transfer_purse_result",
        "schedule": timedelta(seconds=30),
        "args": (),
        "kwargs": {},
        "options": {"queue": "account_transfer_purse"},
    },
    "get_account_infos": {
        "task": "get_account_infos",
        "schedule": timedelta(hours=1.5),
        "args": (),
        "kwargs": {},
        "options": {"queue": "get_account_infos"},
    },
}
