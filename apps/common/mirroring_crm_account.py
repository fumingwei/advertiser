import json
import time
from kombu import Connection, Exchange, Queue, Consumer
from settings.base import configs
from settings.db import SessionLocal
from apps.advertiser.models import (
    UserCusRelationship,
    ProjectGroup,
    GroupAccountRelationship
)
from settings.db import get_redis_connection
from apps.advertiser.utils import get_new_mediums
from settings.log import crm_tasks

MQ_USERNAME = configs.MQ_USERNAME
MQ_PASSWORD = configs.MQ_PASSWORD
MQ_VIRTUAL_HOST = configs.MQ_VIRTUAL_HOST
MQ_HOST = configs.MQ_HOST


def update_account_id(db, data):
    before_account_id = data.get("before_account_id")
    after_account_id = data.get("after_account_id")
    if not before_account_id or not after_account_id:
        return
    # 先修改mysql
    query = db.query(GroupAccountRelationship).filter(GroupAccountRelationship.account_id == before_account_id).all()
    group_ids = set()
    for group_account in query:
        if not group_account.is_delete:
            group_ids.add(group_account.project_group_id)
        group_account.account_id = after_account_id
    db.commit()
    # 再修改redis
    r = get_redis_connection("accredit_account")
    if group_ids:
        pipe = r.pipeline()
        for group_id in group_ids:
            set_key = f"group_advertising_account:{group_id}"
            pipe.srem(set_key, before_account_id)
            pipe.sadd(set_key, after_account_id)
        pipe.execute()
    return True


def update_account_is_delete(db, data):
    account_id = data.get("account_id")
    before_is_delete = data.get("before_is_delete")
    after_is_delete = data.get("after_is_delete")
    if account_id is None or before_is_delete is None or after_is_delete is None:
        return
    # 修改前为已删除的账户不处理
    if before_is_delete == 1:
        return
    # 先删除mysql关系表
    query = db.query(GroupAccountRelationship).filter(
        GroupAccountRelationship.account_id == account_id,
        GroupAccountRelationship.is_delete == False
    ).all()
    group_ids = []
    for group_account in query:
        group_ids.append(group_account.project_group_id)
        group_account.is_delete = True
    db.commit()
    # 再修改redis关系表、项目组的媒介
    r = get_redis_connection("accredit_account")
    pipe = r.pipeline()
    r_account = get_redis_connection("medium_account")
    for group_id in group_ids:
        pipe.srem(f"group_advertising_account:{group_id}", account_id)
        # 更改项目组媒介
        query = db.query(ProjectGroup).filter(ProjectGroup.id == group_id).one_or_none()
        if not query:
            continue
        query.mediums = get_new_mediums(db, group_id)
    db.commit()
    pipe.execute()
    # 清空所有组信息
    r_account.hset(f"account:{account_id.replace('-', '')}", 'project_groups', json.dumps([]))
    return True


def update_account_medium(db, data):
    account_id = data.get("account_id")
    before_medium = data.get("before_medium")
    after_medium = data.get("after_medium")
    if not account_id or not after_medium or not before_medium:
        return
    # 先修改关系表
    query = db.query(GroupAccountRelationship).filter(
        GroupAccountRelationship.account_id == account_id,
        GroupAccountRelationship.medium == before_medium
    ).all()
    group_ids = set()
    for group_account in query:
        group_ids.add(group_account.project_group_id)
        group_account.medium = after_medium
    db.commit()
    # 再修改项目组表
    for group_id in group_ids:
        query = db.query(ProjectGroup).filter(ProjectGroup.id == group_id).one_or_none()
        if not query:
            continue
        query.mediums = get_new_mediums(db, group_id)
    db.commit()
    return True


def update_account_name(db, data):
    account_id = data.get("account_id")
    after_name = data.get("after_name")
    if not account_id or not after_name:
        return
    query = db.query(GroupAccountRelationship).filter(
        GroupAccountRelationship.account_id == account_id
    ).all()
    for group_account in query:
        group_account.account_name = after_name
    db.commit()
    return True


def update_account_customer(db, data):
    account_id = data.get('account_id')
    after_customer_id = data.get('after_customer_id')
    if not account_id or not after_customer_id:
        return
    # 首先查看账户在那个组， 获取到组id
    query = db.query(GroupAccountRelationship).filter(
        GroupAccountRelationship.account_id == account_id,
        GroupAccountRelationship.is_delete == False
    ).all()
    # 根据组ID，获取公司id，
    group_ids = []
    for group_account in query:
        group_ids.append(group_account.project_group_id)
    r = get_redis_connection("accredit_account")
    pipe = r.pipeline()
    r_account = get_redis_connection("medium_account")
    for group_id in group_ids:
        query = db.query(ProjectGroup).filter(
            ProjectGroup.id == group_id,
            ProjectGroup.is_delete == False
        ).one_or_none()
        if not query:
            continue
        # 获取公司id下有哪些客户
        company = db.query(UserCusRelationship).filter(
            UserCusRelationship.company_id == query.company_id,
            UserCusRelationship.is_delete == False
        ).one_or_none()
        if not company:
            continue
        # 判断变更之后的客户是否还在公司下的客户组内
        if after_customer_id in company.customer_id:
            # 如果还在，无需改变
            continue
        # 如果不在，对应删除mysql关系表，redis关系表，更新项目组表的媒介信息
        group_account = db.query(GroupAccountRelationship).filter(
            GroupAccountRelationship.account_id == account_id,
            GroupAccountRelationship.project_group_id == group_id,
            GroupAccountRelationship.is_delete == False
        ).one_or_none()
        if not group_account:
            continue
        group_account.is_delete = True
        db.commit()
        pipe.srem(f"group_advertising_account:{group_id}", account_id)
        hash_key = f"account:{account_id.replace('-', '')}"
        # 获取当前列表
        current_list_json = r_account.hget(hash_key, 'project_groups')
        current_list = json.loads(current_list_json)
        if group_id in current_list:
            current_list.remove(group_id)
        # 将更新后的列表存回哈希表
        r_account.hset(hash_key, 'project_groups', json.dumps(current_list))
        query.mediums = get_new_mediums(db, group_id)
    db.commit()
    pipe.execute()
    return True


def my_callback(body, message):
    data = json.loads(body)
    synchronization_dict = {
        "update_account_id": update_account_id,
        "update_account_is_delete": update_account_is_delete,
        "update_account_medium": update_account_medium,
        "update_account_name": update_account_name,
        "update_account_customer": update_account_customer,
    }
    task_name = synchronization_dict.get(data.get('type'))
    if not task_name:
        message.ack()
        return
    db = SessionLocal()
    # 应答 告诉mq消息已被正常处理
    try:
        res = task_name(db, data)
        crm_tasks.log_info(f'执行{task_name}方法完成：\n传入参数为：{data}\n结果返回：{res}')
        message.ack()
    # 发生异常 重新入队列
    except Exception as e:
        crm_tasks.log_error(f'执行{task_name}方法失败：\n传入参数为：{data}\n报错信息为：{e}')
        try:
            res = task_name(db, data)
            crm_tasks.log_info(f'重新执行{task_name}方法完成：\n传入参数为：{data}\n结果返回：{res}')
        except Exception as e:
            crm_tasks.log_error(f'重新执行{task_name}方法失败：\n传入参数为：{data}\n报错信息为：{e}')
        message.ack()
    finally:
        db.close()


def setup_connection():
    return Connection(f"amqp://{MQ_USERNAME}:{MQ_PASSWORD}@{MQ_HOST}/{MQ_VIRTUAL_HOST}")


def setup_consumer(conn):
    exchange = Exchange('monitor', type='topic')
    queue = Queue('advertiser_synchronization_crm_account', exchange, routing_key='advertiser_account_key')
    return Consumer(conn, queues=[queue], callbacks=[my_callback], prefetch_count=1)


def main(max_retries=5, retry_delay=5):
    retries = 0
    while retries < max_retries:
        try:
            crm_tasks.log_info('同步功能已启动')
            conn = setup_connection()
            consumer = setup_consumer(conn)
            consumer.consume()
            try:
                while True:
                    conn.drain_events()
                    retries = 0
            except KeyboardInterrupt:
                consumer.cancel()
                conn.close()
                crm_tasks.log_info('同步功能已关闭')
                break
            except OSError:
                retries += 1
                if retries < max_retries:
                    crm_tasks.log_info('等待5秒后再重试')
                    time.sleep(retry_delay)
                else:
                    crm_tasks.log_error('达到最大重试次数，退出')
                    break
        except Exception as e:
            crm_tasks.log_error(f'连接发生错误：{e}')
            break


if __name__ == '__main__':
    main()
