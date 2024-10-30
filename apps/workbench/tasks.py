# -*- coding: utf-8 -*-
import json
import os
from pprint import pformat
from datetime import datetime, timedelta

import ulid

from my_celery.main import celery_app
from settings.log import web_log
from settings.db import SessionLocal, RedisClient
from settings.base import configs
from libs.internal.rtdp_service import RTDPService
from apps.accounts.utils import get_customer_ids
from apps.advertiser.models import AdvertiserUser
from apps.workbench.define import Date
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)
file_path = os.getcwd()


@celery_app.task(name='get_account_infos')
def get_account_infos():
    with SessionLocal() as db:
        try:
            # 所有用户id
            user_ids = db.query(AdvertiserUser.id, AdvertiserUser.mobile).filter(
                AdvertiserUser.is_delete == False,
                AdvertiserUser.is_active == True
            ).all()
            # 所有客户id
            r = RedisClient(db=configs.configs.REDIS_STORAGE.get('workbench')).get_redis_client()
            pipe = r.pipeline()
            for user_id, mobile in user_ids:
                customer_ids = get_customer_ids(db, user_id)
                # 账户消耗排名时间默认存今天
                date = Date.TODAY.value
                account_dict = dict()
                account_dict['customer_ids'] = customer_ids
                # 获取开始日期和结束日期
                approval_status = Date[date.upper()]
                desc = approval_status.desc
                account_dict['date_start'] = desc[0]
                account_dict['date_end'] = desc[1]
                # 获取账户消耗数据
                spend_data = dict()
                now = datetime.now()
                spend_data['date_start'] = (now - timedelta(days=6)).strftime('%Y-%m-%d')
                spend_data['date_end'] = now.strftime('%Y-%m-%d')
                spend_data['customer_ids'] = customer_ids
                # 调用rtdp服务获取账户近7天总花费
                logger.info(f'获取账户近7天总花费请求参数：\n{pformat(customer_ids)}')
                total_data = RTDPService.last_7_days_total_cost(
                    json={"customer_ids": customer_ids}, **{'trace_id': str(ulid.new)}
                )
                # 调用rtdp服务获取账户消耗排名
                logger.info(f'获取账户消耗排名请求参数：\n{pformat(account_dict)}')
                account_list = RTDPService.spend_rank(account_dict, **{'trace_id': str(ulid.new())})
                consumption_rank = []
                for item in account_list:
                    new_item = {
                        "account": f"{item['account_name']}-{item['medium']}-{item['account_id']}",
                        "spend": item['total_spend']
                    }
                    consumption_rank.append(new_item)
                # 调用rtdp服务获取账户消耗数据
                logger.info(f'获取账户消耗数据请求参数：\n{pformat(spend_data)}')
                account_data = RTDPService.insight_spend(spend_data, **{'trace_id': str(ulid.new())})
                start_time = datetime.strptime(spend_data['date_start'], "%Y-%m-%d")
                end_time = datetime.strptime(spend_data['date_end'], "%Y-%m-%d")
                x = [(start_time + timedelta(days=x)).strftime("%Y-%m-%d") for x in
                     range((end_time - start_time).days + 1)]
                y = []
                for platform, platform_data in account_data.items():
                    platform_name = platform.capitalize()
                    platform_result = {"name": platform_name, "data": []}
                    for date in x:
                        spend_value = next((d["spend"] for d in platform_data if d["date"] == date), 0)
                        platform_result["data"].append(spend_value)
                    y.append(platform_result)
                consumption_data = dict()
                consumption_data["line_data"] = {"x": x, "y": y}
                pipe.hmset(f"user:{mobile}",
                           {"total_data": json.dumps(total_data), "cs_rank": json.dumps(consumption_rank),
                            "cs_data": json.dumps(consumption_data), "cs_data_start": spend_data['date_start'],
                            "cs_data_end": spend_data['date_end']})
                pipe.execute()
        except Exception as e:
            web_log.log_error(f'文件位置：{file_path}，tasks.py下的get_account_infos发生异常，存储工作台广告账户信息失败原因：{e}')

