import random
import string

from typing import List
from datetime import datetime

from apps.advertiser.models import AdvertiserUser
from apps.finance.models import WorkOrder
from settings.db import SessionLocal, RedisClient
from settings.log import web_log
from tools.constant import ApproveResult
from tools.exceptions import RedisEmptyError


# 生成工单类
class WorkOrderApply:

    @staticmethod
    def generate_work_order_id(user_id):
        str_num = ''.join(random.choices(string.digits, k=4))
        work_order_id = datetime.now().strftime('%Y%m%d%H%M%S%f') + str_num + ('%04d' % user_id)
        return work_order_id

    # 生成工单
    @staticmethod
    def create_work_order(
            db,
            work_order_id,
            flow_code,
            user_id,
            company_id,
            account_id="",
            remark="",
            is_special=False,
            flat=False,
            system_type="2",
    ):
        try:
            work_order = WorkOrder(
                work_order_id=work_order_id,
                account_id=account_id,
                flow_code=flow_code,
                current_node=1,
                apply_user_id=user_id,
                company_id=company_id,
                remark=remark,
                is_special=is_special,
                system_type=system_type,
                approval_status=flat
                                and ApproveResult.AGREE.value
                                or ApproveResult.PROCESS.value,
            )
            db.add(work_order)
            db.flush()
            return work_order
        except Exception as e:
            web_log.log_error(e.__str__())
            raise e


# 主-子账户关系
def get_user_list(user_id):
    with SessionLocal() as db:
        user_pid_obj = db.query(AdvertiserUser.p_id).filter(
            AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False
        )
        user_pid_id = (
            db.query(AdvertiserUser.id)
            .filter(
                AdvertiserUser.p_id.in_(user_pid_obj), AdvertiserUser.is_delete == False
            )
            .all()
        )
        user_id_obj = (
            db.query(AdvertiserUser.id)
            .filter(AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False)
            .all()
        )
        user_pids = []
        user_ids = []
        user_pid_ids = []
        if user_pid_obj.all():
            user_pids = [i[0] for i in user_pid_obj.all() if i[0]]
        if user_id_obj:
            user_ids = [j[0] for j in user_id_obj if j[0]]
        if user_pid_id:
            user_pid_ids = [n[0] for n in user_pid_id if n[0]]
        all_user_id = user_pids + user_ids + user_pid_ids
        all_user_id.append(user_id)
        all_user_id = list(set(all_user_id))
    return all_user_id


def get_medium_account_info_from_redis(account_id: str):
    """
    从Redis获取媒体广告账户的基本信息
    """
    redis_client = RedisClient().get_redis_client()
    account_info = redis_client.hgetall(f"account:{account_id.replace('-', '')}")  # Google广告账户ID中有横杠，需要去掉
    if not account_info:
        web_log.log_error(f"账户：{account_id}，redis存贮账户信息为空")
        raise RedisEmptyError("redis账户信息为空")
    return account_info


def set_expire_account_to_redis(account_id: str, medium: str = ""):
    """
    设置余额转移广告账户的过期时间到Redis
    """
    redis_client = RedisClient().get_redis_client()
    # 自动过期时间为2分钟，防止媒体API服务停止
    expire = redis_client.set(f"expire:{account_id}", "expire", ex=2 * 60, nx=True)
    return expire


def get_expire_account_from_redis(account_id: str):
    """
    从Redis获取余额转移广告账户的过期时间
    """
    redis_client = RedisClient().get_redis_client()
    expire = redis_client.get(f"expire:{account_id}")
    return expire


def empty_expire_account_from_redis(account_ids: List[str]):
    """
    清空余额转移广告账户的过期时间
    """
    redis_client = RedisClient().get_redis_client()
    pipe = redis_client.pipeline()
    for account_id in account_ids:
        pipe.delete(f"expire:{account_id}")
    pipe.execute()


if __name__ == "__main__":
    # info = get_medium_account_info_from_redis("361190318877633")
    # print(info)
    expire = set_expire_account_to_redis("361190318877633")
    print(expire)
    expire = get_expire_account_from_redis("361190318877633")
    print(expire)
