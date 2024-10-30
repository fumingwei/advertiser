import ast
import io
import datetime
import pandas as pd
from sqlalchemy import and_, func, case, or_
from apps.advertiser.models import AdvertiserUser, AdvertiserRegister, ProjectGroup, GroupAccountRelationship, \
    GroupMemberRelationship
from apps.finance.define import balance_transfer_result_status
from apps.onboarding.models import OeOpenAccount
from apps.operation.define import gop
from settings.base import configs
from settings.db import SessionLocal, RedisClient, engine
from apps.accounts.define import OperateResult, BCGrantType, BMGrantType, medium_account_status_object, \
    custom_account_status_object
from apps.accounts.utils import get_customer_ids
from apps.accounts.models import BmAccount, BcAccount, BmAccountDetail, BcAccountDetail, AccountRename
from apps.pixel.models import PixelAccount, PixelAccountDetail
from apps.finance.models import WorkOrder, BalanceTransfer
from libs.internal.crm_external_service import CRMExternalService
from libs.internal.rtdp_service import RTDPService
from tools.common import SingletonType
from tools.constant import Operation
from settings.log import celery_log


def get_is_second(user_id):
    with SessionLocal() as db:
        # 查询用户
        user = db.query(AdvertiserUser.p_id
                        ).filter(AdvertiserUser.id == user_id,
                                 AdvertiserUser.is_delete.is_(False)
                                 ).scalar()
        res_id = user if user else user_id
        # 查询注册信息
        res_user = db.query(AdvertiserRegister.is_second
                            ).filter(AdvertiserRegister.user_id == res_id,
                                     AdvertiserRegister.is_delete.is_(False)
                                     ).scalar()
    return res_user if res_user is not None else False


# 获取主账户标识
def get_is_primary(user_id):
    with SessionLocal() as db:
        user = db.query(AdvertiserUser.p_id
                        ).filter(AdvertiserUser.id == user_id,
                                 AdvertiserUser.is_delete.is_(False)
                                 ).scalar()
    return False if user else True


def get_open_subject_name(redis_pipe, accounts_id):
    """
    传入多个广告账户，获取开户主体
    """
    if not accounts_id:
        return '-'
    for account in accounts_id:
        redis_pipe.hget(f'account:{account.replace("-", "")}', 'open_subject_name')
    res_lt = {res for res in redis_pipe.execute() if res}
    if not res_lt:
        return '-'
    return ','.join(res_lt)


def get_redis_account_group(data):
    # 连接Redis
    redis_client = RedisClient(db=configs.REDIS_STORAGE.get('medium_account'))
    redis_connection = redis_client.get_redis_client()
    pipe = redis_connection.pipeline()

    # 批量获取Redis中的账户信息
    if len(data) > 0 and isinstance(data, list) and isinstance(data[-1], dict):
        account_ids = [account.get("account_id").replace("-", "") for account in data]
    else:
        account_ids = [str(i).replace('-', '') for i in data]
    for account_id in account_ids:
        pipe.hmget(f'account:{account_id}', 'project_groups')
    pipe_account_list = [ast.literal_eval(item[0]) if item[0] is not None else [] for item in pipe.execute()]

    # 获取组id
    group_ids_set = set()
    for account_groups in pipe_account_list:
        if account_groups:
            group_ids_set.update([i for i in account_groups if i])
    group_ids = list(group_ids_set)

    # 批量获取组名称
    with SessionLocal() as db:
        groups = db.query(
            ProjectGroup.id,
            ProjectGroup.project_name
        ).filter(ProjectGroup.id.in_(group_ids)).all()
        group_name_mapping = {group.id: group.project_name for group in groups}

    return pipe_account_list, group_name_mapping


# 权限校验
def permission_check(user_id, account_ids, operation_type):
    with SessionLocal() as db:
        user_group = db.query(GroupMemberRelationship.project_group_id
                              ).filter(GroupMemberRelationship.user_id == user_id,
                                       GroupMemberRelationship.is_delete.is_(False))

        operation_group = db.query(ProjectGroup.id
                                   ).filter(ProjectGroup.id.in_(user_group),
                                            func.JSON_CONTAINS(ProjectGroup.operation_type,
                                                               func.JSON_QUOTE(operation_type)),
                                            ProjectGroup.is_delete.is_(False))

        accounts = db.query(GroupAccountRelationship.account_id
                            ).filter(GroupAccountRelationship.project_group_id.in_(operation_group),
                                     GroupAccountRelationship.is_delete.is_(False)).all()
        user_operation_group_account = [account_id for (account_id,) in accounts]

        account_ids = set(account_ids)
        user_operation_group_account = set(user_operation_group_account)
        no_allow_account = list(account_ids - user_operation_group_account)
    return no_allow_account


# 用户授权过的账户列表
def user_authorization_account(user_id):
    account_ids = []
    with SessionLocal() as db:
        # 查询子账户
        user = db.query(AdvertiserUser.p_id
                        ).filter(AdvertiserUser.id == user_id,
                                 AdvertiserUser.is_delete.is_(False)
                                 ).scalar()
        res_id = user if user else None
        # 查询主账户标识
        if res_id:
            res_user = db.query(AdvertiserRegister.is_second
                                ).filter(AdvertiserRegister.user_id == res_id,
                                         AdvertiserRegister.is_delete.is_(False)
                                         ).scalar()
            is_second_mark = res_user if res_user is not None else False
            # 是二代客户
            if is_second_mark:
                user_group = db.query(GroupMemberRelationship.project_group_id
                                      ).filter(GroupMemberRelationship.user_id == user_id,
                                               GroupMemberRelationship.is_delete.is_(False))
                accounts = db.query(GroupAccountRelationship.account_id.distinct()
                                    ).filter(GroupAccountRelationship.project_group_id.in_(user_group),
                                             GroupAccountRelationship.is_delete.is_(False)).all()
                account_ids = [account_id for (account_id,) in accounts]
    return account_ids


# 获取账户的开户主体
def get_redis_account_info(accounts_ids: list) -> pd.DataFrame:
    redis_client = RedisClient(db=configs.REDIS_STORAGE.get('medium_account'))
    redis_connection = redis_client.get_redis_client()
    pipe = redis_connection.pipeline()
    for account in accounts_ids:
        account_id = str(account).replace('-', '')
        pipe.hmget(f'account:{account_id}', ['account_id', 'open_subject_name'])
    redis_res = pipe.execute()
    df = pd.DataFrame(data=redis_res, columns=['广告账户id', '开户主体'])
    return df.fillna("_")


class Export(metaclass=SingletonType):
    @staticmethod
    def recharge(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        """
        充值导出
        """
        db = SessionLocal()
        user = db.query(
            AdvertiserUser
        ).filter(
            AdvertiserUser.id == user_id,
            AdvertiserUser.is_delete == False
        ).first()
        res_id = user.p_id if user and user.p_id else user_id
        res_user = db.query(
            AdvertiserRegister
        ).filter(
            AdvertiserRegister.user_id == res_id,
            AdvertiserRegister.is_delete == False
        ).first()
        is_second_ = res_user.is_second if res_user else False
        advertiser_user_obj = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id).first()
        work_order_ids = list()
        if advertiser_user_obj:
            p_id = advertiser_user_obj.p_id if advertiser_user_obj.p_id else None
            company_id = advertiser_user_obj.company_id
            # 登录人是子账户
            if all([p_id, company_id]):
                work_order_ids = (
                    db.query(WorkOrder.work_order_id)
                    .filter(
                        WorkOrder.apply_user_id == user_id,
                        WorkOrder.company_id == company_id,
                    )
                    .all()
                )
            # 登录人是主账户
            if not p_id:
                work_order_ids = (
                    db.query(WorkOrder.work_order_id)
                    .filter(WorkOrder.company_id == company_id)
                    .all()
                )
        db.close()
        work_order_ids = [i[0] for i in work_order_ids] if work_order_ids else []
        json_ = {
            "user_id": user_id,
            "work_order_ids": work_order_ids
        }
        if kwargs.get('q'):
            json_.update({"q": kwargs.get('q')})
        if customer_id:
            json_.update({"customer_id": customer_id})
        if start_date and end_date:
            json_.update({"date_end": end_date, "date_start": start_date})
        if account_id:
            json_.update({'account_id': account_id})
        if medium:
            json_.update({'medium': medium})
        if operate_result:
            json_.update({'operate_result': operate_result})
        if operate_user_id:
            json_.update({'operate_user_id': operate_user_id})
        res = CRMExternalService.post_recharge_export(json=json_, **{'trace_id': kwargs.get('trace_id')})
        df = pd.read_csv(io.BytesIO(res))
        is_primary = get_is_primary(user_id)
        # 根据用户是否为二代客户主账户处理数据
        if is_second_ and is_primary and not df.empty:
            # 获取redis数据
            df['项目组名称'] = ''
            account_ids = df['账户ID'].to_list()
            pipe_account_list, group_name_mapping = get_redis_account_group(account_ids)
            for item, account_groups in zip(account_ids, pipe_account_list):
                if project_group:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                        if group_id == project_group
                    ]
                else:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                    ]
                group_name_str = ", ".join(group_names) or '-'
                df.loc[df['账户ID'] == item, '项目组名称'] = group_name_str
            if project_group:
                df = df[df['项目组名称'] == project_group]
        df["账户ID"] = df["账户ID"].apply(lambda x: str(x) + "\t")
        return df

    @staticmethod
    def reset(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        """
        清零导出
        """
        db = SessionLocal()
        advertiser_user_obj = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id).first()
        user = db.query(
            AdvertiserUser
        ).filter(
            AdvertiserUser.id == user_id,
            AdvertiserUser.is_delete == False
        ).first()
        res_id = user.p_id if user and user.p_id else user_id
        res_user = db.query(
            AdvertiserRegister
        ).filter(
            AdvertiserRegister.user_id == res_id,
            AdvertiserRegister.is_delete == False
        ).first()
        is_second_ = res_user.is_second if res_user else False
        work_order_ids = list()
        if advertiser_user_obj:
            p_id = advertiser_user_obj.p_id if advertiser_user_obj.p_id else None
            company_id = advertiser_user_obj.company_id
            # 登录人是子账户
            if all([p_id, company_id]):
                work_order_ids = (
                    db.query(WorkOrder.work_order_id)
                    .filter(
                        WorkOrder.apply_user_id == user_id,
                        WorkOrder.company_id == company_id,
                        WorkOrder.flow_code == 'account_reset'
                    )
                    .all()
                )
            # 登录人是主账户
            if not p_id:
                work_order_ids = (
                    db.query(WorkOrder.work_order_id)
                    .filter(WorkOrder.company_id == company_id)
                    .all()
                )
        db.close()
        work_order_ids = [i[0] for i in work_order_ids] if work_order_ids else []
        json_ = {
            "work_order_ids": work_order_ids
        }
        if kwargs.get('q'):
            json_.update({"q": kwargs.get('q')})
        if customer_id:
            json_.update({"customer_id": customer_id})
        if start_date and end_date:
            json_.update({"date_end": end_date, "date_start": start_date})
        if account_id:
            json_.update({'account_id': account_id})
        if medium:
            json_.update({'medium': medium})
        if operate_result:
            json_.update({'operate_result': operate_result})
        if operate_user_id:
            json_.update({'operate_user_id': operate_user_id})
        res = CRMExternalService.post_reset_export(json=json_, **{'trace_id': kwargs.get('trace_id')})
        df = pd.read_csv(io.BytesIO(res))
        is_primary = get_is_primary(user_id)
        # 根据用户是否为二代客户主账户处理数据
        if is_second_ and is_primary and not df.empty:
            # 获取redis数据
            df['项目组名称'] = ''
            account_ids = df['账户ID'].to_list()
            pipe_account_list, group_name_mapping = get_redis_account_group(account_ids)
            for item, account_groups in zip(account_ids, pipe_account_list):
                if project_group:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                        if group_id == project_group
                    ]
                else:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                    ]
                group_name_str = ", ".join(group_names) or '-'
                df.loc[df['账户ID'] == item, '项目组名称'] = group_name_str
            if project_group:
                df = df[df['项目组名称'] == project_group]
        df["账户ID"] = df["账户ID"].apply(lambda x: str(x) + "\t")
        return df

    @staticmethod
    def balance_transfer(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        """
        账户转账
        """
        db = SessionLocal()
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id).first()
        where = [BalanceTransfer.is_delete == False]
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
            )
            sub_names = [i.id for i in sub_user.all()]
            sub_names.append(user.id)
            where.append(BalanceTransfer.user_id.in_(sub_names))
        # 如果是子账号
        else:
            where.append(BalanceTransfer.user_id == user_id)
        if customer_id:
            customer_ids = [customer_id]
        else:
            customer_ids = get_customer_ids(db, user_id)
            customer_ids = list(set(customer_ids))
        where.append(BalanceTransfer.customer_id.in_(customer_ids))
        if kwargs.get('q'):
            where.append(AdvertiserUser.real_name.like(f"%{kwargs.get('q')}%"))
        if medium:
            where.append(BalanceTransfer.medium == medium)
        if all([start_date, end_date]):
            where.append(and_(
                func.date(BalanceTransfer.created_time) >= func.date(start_date),
                func.date(BalanceTransfer.created_time) <= func.date(end_date)
            ))
        if operate_result:
            result_list = balance_transfer_result_status.get(operate_result)
            where.append(BalanceTransfer.transfer_status.in_(result_list))
        params = {
            "customer_ids": customer_ids,
        }
        query = db.query(
            BalanceTransfer.customer_id.label("结算名称"),
            BalanceTransfer.medium.label("投放媒介"),
            BalanceTransfer.transfer_amount.label("转移金额"),
            BalanceTransfer.created_time.label("操作时间"),
            BalanceTransfer.transfer_status.label("操作结果"),
            AdvertiserUser.real_name.label("操作人")
        ).outerjoin(
            AdvertiserUser,
            AdvertiserUser.id == BalanceTransfer.user_id
        ).filter(*where).order_by(BalanceTransfer.created_time.desc())
        db.close()
        result = CRMExternalService().customer_id_name(params, **{'trace_id': kwargs.get('trace_id')})
        customer_dict = {i["id"]: i["name"] for i in result["data"]}
        df = pd.read_sql(query.statement, query.session.bind)
        if not df.empty:
            df['结算名称'] = df['结算名称'].map(customer_dict)
        return df

    @staticmethod
    def rename(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        """
        重命名导出
        """
        with SessionLocal() as db:
            where = list()
            user = db.query(
                AdvertiserUser
            ).filter(
                AdvertiserUser.id == user_id,
                AdvertiserUser.is_delete == False
            ).first()
            is_second = get_is_second(user_id)
            is_primary = get_is_primary(user_id)
            # 如果是主账号
            if not user.p_id:
                sub_user = db.query(AdvertiserUser).filter(
                    AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
                )
                sub_names = [i.id for i in sub_user.all()]
                sub_names.append(user.id)
                where.append(AccountRename.user_id.in_(sub_names))
            # 如果是子账号
            else:
                where.append(AccountRename.user_id == user_id)
            if all([start_date, end_date]):
                where.extend(
                    [
                        func.date(AccountRename.created_time) >= func.date(start_date),
                        func.date(AccountRename.created_time) <= func.date(end_date)
                    ]
                )
            if account_id:
                where.append(AccountRename.account_id == account_id)
            if operate_result:
                where.append(AccountRename.operate_result == operate_result)
            if medium:
                where.append(AccountRename.medium == medium)
            if kwargs.get('q'):
                # 模糊搜索能搜索广告账户ID、修改前名称、修改后名称、提交人
                user_ids = db.query(AdvertiserUser.id).filter(
                    AdvertiserUser.real_name.like(f"%{kwargs.get('q')}%")).all()
                user_ids_list = [row.id for row in user_ids]
                where.append(
                    or_(
                        AccountRename.before_account_name.ilike(f"%{kwargs.get('q')}%"),
                        AccountRename.after_account_name.ilike(f"%{kwargs.get('q')}%"),
                        AccountRename.account_id.ilike(f"%{kwargs.get('q')}%"),
                        AccountRename.user_id.in_(user_ids_list),
                    )
                )
            query = db.query(
                AccountRename.account_id.label('广告账户id'),
                AccountRename.before_account_name.label('更改前名称'),
                AccountRename.after_account_name.label('更改后名称'),
                AccountRename.medium.label('媒介'),
                case([
                    (AccountRename.operate_result == OperateResult.SUCCESS.value, OperateResult.SUCCESS.desc),
                    (AccountRename.operate_result == OperateResult.DEFAULT.value, OperateResult.DEFAULT.desc),
                    (AccountRename.operate_result == OperateResult.FAIL.value, OperateResult.FAIL.desc),
                ], else_="").label('操作结果'),
                AccountRename.created_time.label('操作时间'),
                AccountRename.remark.label('备注'),
                AdvertiserUser.real_name.label('操作人')
            ).outerjoin(
                AdvertiserUser,
                AdvertiserUser.id == AccountRename.user_id
            ).filter(*where).order_by(AccountRename.id.desc())
        df = pd.read_sql_query(sql=query.statement, con=engine.connect())
        account_ids = df['广告账户id'].to_list()
        if is_second and is_primary and not df.empty:
            # 获取redis数据
            df['项目组名称'] = ''
            pipe_account_list, group_name_mapping = get_redis_account_group(account_ids)
            for item, account_groups in zip(account_ids, pipe_account_list):
                group_names = [group_name_mapping.get(group_id, '') for group_id in (account_groups or [])]
                group_name_str = ", ".join(group_names) or '-'
                df.loc[df['广告账户id'] == item, '项目组名称'] = group_name_str
        # 获取开户主体的df
        account_open_subject_df = get_redis_account_info(list(set(account_ids)))
        # 数据合并
        merged_df = pd.merge(df, account_open_subject_df, on='广告账户id', how='left')
        merged_df["广告账户id"] = merged_df["广告账户id"].apply(lambda x: x + "\t")
        return merged_df

    @staticmethod
    def bm(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        """
        bm导出
        """
        with SessionLocal() as db:
            where = list()
            user = db.query(
                AdvertiserUser
            ).filter(
                AdvertiserUser.id == user_id,
                AdvertiserUser.is_delete == False
            ).first()
            is_second = get_is_second(user_id)
            is_primary = get_is_primary(user_id)
            # 如果是主账号
            if not user.p_id:
                sub_user = db.query(AdvertiserUser).filter(
                    AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
                )
                sub_names = [i.id for i in sub_user.all()]
                sub_names.append(user.id)
                where.append(BmAccount.user_id.in_(sub_names))
            # 如果是子账号
            else:
                where.append(BmAccount.user_id == user_id)
            if all([start_date, end_date]):
                where.extend(
                    [
                        func.date(BmAccount.created_time) >= func.date(start_date),
                        func.date(BmAccount.created_time) <= func.date(end_date)
                    ]
                )
            if kwargs.get('q'):
                # 搜索商业账户ID、广告账户id和提交人
                user_ids = db.query(AdvertiserUser.id).filter(
                    AdvertiserUser.real_name.like(f"%{kwargs.get('q')}%")).all()
                user_ids_list = [row.id for row in user_ids]
                where.append(
                    or_(
                        BmAccount.business_id.like(f"%{kwargs.get('q')}%"),
                        BmAccount.user_id.in_(user_ids_list),
                        BmAccountDetail.account_id.like(f"%{kwargs.get('q')}%"),
                    ))
            if account_id:
                where.append(BmAccountDetail.account_id == account_id)
            if operate_result:
                where.append(BmAccountDetail.operate_result == operate_result)
            if operate_user_id:
                where.append(BmAccount.user_id == operate_user_id)
            if bm_id := kwargs.get('bm_id'):
                where.append(BmAccount.business_id.ilike(f"%{bm_id}%"))
            query = db.query(
                BmAccount.business_id.label('商业账户ID'),
                BmAccountDetail.account_id.label('广告账户id'),
                case([
                    (BmAccount.grant_type == BMGrantType.ANALYZE.value, BMGrantType.ANALYZE.desc),
                    (BmAccount.grant_type == BMGrantType.ADVERTISE_ANALYZE.value, BMGrantType.ADVERTISE_ANALYZE.desc),
                    (BmAccount.grant_type == BMGrantType.MANAGE_ADVERTISE_ANALYZE.value,
                     BMGrantType.MANAGE_ADVERTISE_ANALYZE.desc),
                ], else_='').label('授权类型'),
                case(
                    [
                        (BmAccount.operate_type == Operation.BIND.value, Operation.BIND.desc),
                        (BmAccount.operate_type == Operation.UNBIND.value, Operation.UNBIND.desc)
                    ], else_='').label('操作类型'),
                case(
                    [
                        (BmAccountDetail.operate_result == OperateResult.DEFAULT.value, OperateResult.DEFAULT.desc),
                        (BmAccountDetail.operate_result == OperateResult.SUCCESS.value, OperateResult.SUCCESS.desc),
                        (BmAccountDetail.operate_result == OperateResult.FAIL.value, OperateResult.FAIL.desc)
                    ], else_='未知').label('操作结果'),
                BmAccountDetail.remark.label('备注'),
                BmAccount.created_time.label('操作时间'),
                AdvertiserUser.real_name.label('操作人')
            ).outerjoin(
                BmAccountDetail,
                BmAccountDetail.bm_account_id == BmAccount.id
            ).outerjoin(
                AdvertiserUser,
                AdvertiserUser.id == BmAccount.user_id
            ).filter(*where).order_by(BmAccount.created_time.desc())
        df = pd.read_sql_query(sql=query.statement, con=engine.connect())
        account_ids = df['广告账户id'].to_list()
        if is_second and is_primary and not df.empty:
            # 获取redis数据
            df['项目组名称'] = ''
            pipe_account_list, group_name_mapping = get_redis_account_group(account_ids)
            for item, account_groups in zip(account_ids, pipe_account_list):
                if project_group:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                        if group_id == project_group
                    ]
                else:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                    ]
                group_name_str = ", ".join(group_names) or '-'
                df.loc[df['广告账户id'] == item, '项目组名称'] = group_name_str
            if project_group:
                df = df[df['项目组名称'] == project_group]
        # 获取开户主体的df
        account_open_subject_df = get_redis_account_info(list(set(account_ids)))
        # 数据合并
        merged_df = pd.merge(df, account_open_subject_df, on='广告账户id', how='left')
        merged_df["广告账户id"] = merged_df["广告账户id"].apply(lambda x: x + "\t")
        return merged_df

    @staticmethod
    def bc(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        """
        bc导出
        """
        with SessionLocal() as db:
            where = list()
            user = db.query(
                AdvertiserUser
            ).filter(
                AdvertiserUser.id == user_id,
                AdvertiserUser.is_delete == False
            ).first()
            is_second = get_is_second(user_id)
            is_primary = get_is_primary(user_id)
            # 如果是主账号
            if not user.p_id:
                sub_user = db.query(AdvertiserUser).filter(
                    AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
                )
                sub_names = [i.id for i in sub_user.all()]
                sub_names.append(user.id)
                where.append(BcAccount.user_id.in_(sub_names))
            # 如果是子账号
            else:
                where.append(BcAccount.user_id == user_id)
            if all([start_date, end_date]):
                where.extend(
                    [
                        func.date(BcAccount.created_time) >= start_date,
                        func.date(BcAccount.created_time) <= end_date
                    ]
                )
            if account_id:
                where.append(BcAccountDetail.account_id == account_id)
            if operate_result:
                where.append(BcAccountDetail.operate_result == operate_result)
            if cooperative_id := kwargs.get('cooperative_id'):
                where.append(BcAccount.cooperative_id.ilike(f"{cooperative_id}"))
            if operate_user_id:
                where.append(BcAccount.user_id == operate_user_id)
            if kwargs.get('q'):
                # 可以搜索提交人、合作伙伴id、account_id、商业账户ID
                user_ids = db.query(AdvertiserUser.id).filter(
                    AdvertiserUser.real_name.like(f"%{kwargs.get('q')}%")).all()
                user_ids_list = [row.id for row in user_ids]
                where.append(
                    or_(
                        BcAccount.cooperative_id.like(f"%{kwargs.get('q')}%"),
                        BcAccount.user_id.in_(user_ids_list),
                        BcAccountDetail.account_id.like(f"%{kwargs.get('q')}%"),
                        BcAccountDetail.business_id.like(f"%{kwargs.get('q')}%"),
                    ))
            query = db.query(
                BcAccount.cooperative_id.label('合作伙伴id'),
                BcAccountDetail.business_id.label('商务中心ID'),
                BcAccountDetail.account_id.label('广告账户id'),
                case(
                    [
                        (BcAccount.grant_type == BCGrantType.ANALYST.value, BCGrantType.ANALYST.desc),
                        (BcAccount.grant_type == BCGrantType.OPERATOR.value, BCGrantType.OPERATOR.desc)
                    ], else_='').label('授权类型'),
                case(
                    [
                        (BcAccount.operate_type == Operation.BIND.value, Operation.BIND.desc),
                        (BcAccount.operate_type == Operation.UNBIND.value, Operation.UNBIND.desc)
                    ], else_='').label('操作类型'),
                case(
                    [
                        (BcAccountDetail.operate_result == OperateResult.DEFAULT.value, OperateResult.DEFAULT.desc),
                        (BcAccountDetail.operate_result == OperateResult.SUCCESS.value, OperateResult.SUCCESS.desc),
                        (BcAccountDetail.operate_result == OperateResult.FAIL.value, OperateResult.FAIL.desc)
                    ], else_='未知').label('操作结果'),
                BcAccountDetail.remark.label('备注'),
                BcAccount.created_time.label('操作时间'),
                AdvertiserUser.real_name.label('操作人')
            ).outerjoin(
                BcAccountDetail,
                BcAccount.id == BcAccountDetail.tiktok_bc_account_id
            ).outerjoin(
                AdvertiserUser,
                AdvertiserUser.id == BcAccount.user_id
            ).filter(*where).order_by(BcAccount.created_time.desc())
        df = pd.read_sql_query(sql=query.statement, con=engine.connect())
        account_ids = df['广告账户id'].to_list()
        if is_second and is_primary and not df.empty:
            # 获取redis数据
            df['项目组名称'] = ''
            pipe_account_list, group_name_mapping = get_redis_account_group(account_ids)
            for item, account_groups in zip(account_ids, pipe_account_list):
                if project_group:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                        if group_id == project_group
                    ]
                else:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                    ]
                group_name_str = ", ".join(group_names) or '-'
                df.loc[df['广告账户id'] == item, '项目组名称'] = group_name_str
            if project_group:
                df = df[df['项目组名称'] == project_group]
        # 获取开户主体的df
        account_open_subject_df = get_redis_account_info(list(set(account_ids)))
        # 数据合并
        merged_df = pd.merge(df, account_open_subject_df, on='广告账户id', how='left')
        merged_df["广告账户id"] = merged_df["广告账户id"].apply(lambda x: x + "\t")
        return merged_df

    @staticmethod
    def pixel(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:

        """
        pixel导出
        """
        with SessionLocal() as db:
            where = list()
            user = db.query(
                AdvertiserUser
            ).filter(
                AdvertiserUser.id == user_id,
                AdvertiserUser.is_delete == False
            ).first()
            is_second = get_is_second(user_id)
            is_primary = get_is_primary(user_id)
            # 如果是主账号
            if not user.p_id:
                sub_user = db.query(AdvertiserUser).filter(
                    AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
                )
                sub_names = [i.id for i in sub_user.all()]
                sub_names.append(user.id)
                where.append(PixelAccount.user_id.in_(sub_names))
            # 如果是子账号
            else:
                where.append(PixelAccount.user_id == user_id)
            if all([start_date, end_date]):
                where.extend(
                    [
                        func.date(PixelAccount.created_time) >= start_date,
                        func.date(PixelAccount.created_time) <= end_date
                    ]
                )
            if account_id:
                where.append(PixelAccountDetail.account_id == account_id)
            if operate_result:
                where.append(PixelAccountDetail.operate_result == operate_result)
            if operate_user_id:
                where.append(PixelAccount.user_id == operate_user_id)
            if kwargs.get('q'):
                # 可以搜索Pixel_ID、提交人、account_id
                user_ids = db.query(AdvertiserUser.id).filter(
                    AdvertiserUser.real_name.like(f"%{kwargs.get('q')}%")).all()
                user_ids_list = [row.id for row in user_ids]
                where.append(
                    or_(PixelAccount.pixel_id.like(f"%{kwargs.get('q')}%"),
                        PixelAccount.user_id.in_(user_ids_list),
                        PixelAccountDetail.account_id.like(f"%{kwargs.get('q')}%")))
            query = db.query(
                PixelAccount.pixel_id.label('Pixel_ID'),
                PixelAccountDetail.account_id.label('广告账户id'),
                case(
                    [
                        (PixelAccount.operate_type == Operation.BIND.value, Operation.BIND.desc),
                        (PixelAccount.operate_type == Operation.UNBIND.value, Operation.UNBIND.desc)
                    ], else_='').label('操作类型'),
                case(
                    [
                        (PixelAccountDetail.operate_result == OperateResult.DEFAULT.value, OperateResult.DEFAULT.desc),
                        (PixelAccountDetail.operate_result == OperateResult.SUCCESS.value, OperateResult.SUCCESS.desc),
                        (PixelAccountDetail.operate_result == OperateResult.FAIL.value, OperateResult.FAIL.desc)
                    ], else_='').label('操作结果'),
                PixelAccountDetail.remark.label('备注'),
                PixelAccount.created_time.label('操作时间'),
                AdvertiserUser.real_name.label('操作人')
            ).outerjoin(
                PixelAccountDetail,
                PixelAccountDetail.pixel_account_id == PixelAccount.id
            ).outerjoin(
                AdvertiserUser,
                AdvertiserUser.id == PixelAccount.user_id
            ).filter(*where).order_by(PixelAccount.created_time.desc())
        df = pd.read_sql_query(sql=query.statement, con=engine.connect())
        account_ids = df['广告账户id'].to_list()
        if is_second and is_primary and not df.empty:
            # 获取redis数据
            df['项目组名称'] = ''
            pipe_account_list, group_name_mapping = get_redis_account_group(account_ids)
            for item, account_groups in zip(account_ids, pipe_account_list):
                if project_group:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                        if group_id == project_group
                    ]
                else:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                    ]
                group_name_str = ", ".join(group_names) or '-'
                df.loc[df['广告账户id'] == item, '项目组名称'] = group_name_str
            if project_group:
                df = df[df['项目组名称'] == project_group]
        # 获取开户主体的df
        account_open_subject_df = get_redis_account_info(list(set(account_ids)))
        # 数据合并
        merged_df = pd.merge(df, account_open_subject_df, on='广告账户id', how='left')
        merged_df["广告账户id"] = merged_df["广告账户id"].apply(lambda x: x + "\t")
        return merged_df

    @staticmethod
    def accounts(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        """
        账户列表导出
        """
        db = SessionLocal()
        user = db.query(
            AdvertiserUser
        ).filter(
            AdvertiserUser.id == user_id,
            AdvertiserUser.is_delete == False
        ).first()
        res_id = user.p_id or user_id
        res_user = db.query(
            AdvertiserRegister
        ).filter(
            AdvertiserRegister.user_id == res_id,
            AdvertiserRegister.is_delete == False
        ).first()
        is_second_ = res_user.is_second if res_user else False
        is_primary = get_is_primary(user_id)
        customer_ids = get_customer_ids(db, user_id)
        group_list = []
        # 是二代子账户
        if res_user and res_user.is_second and user.p_id:
            groups = db.query(GroupMemberRelationship.project_group_id).filter(
                GroupMemberRelationship.user_id == user_id, GroupMemberRelationship.is_delete == 0).all()
            group_list = [group_id for (group_id,) in groups]
        db.close()
        json_ = {
            "customer_ids": customer_ids,
            'group_id': group_list,
            "is_second": is_second_,
            "is_primary": False if user.p_id else True
        }
        if medium:
            json_["medium"] = [medium]
        if all([start_date, end_date]):
            json_["start_date"] = start_date
            json_["end_date"] = end_date
        if kwargs.get('account_status') and kwargs.get('account_status') != "全部":
            json_["account_status"] = custom_account_status_object.get(kwargs.get('account_status').upper(), [])
        if kwargs.get('q'):
            json_["q"] = kwargs.get('q')
        crm_result = CRMExternalService.post_account_export(
            json=json_,
            headers={"advertiser_user_id": str(user_id)},
            **{'trace_id': kwargs.get('trace_id')}
        )
        if crm_result.get("code") != 0:
            return pd.DataFrame()
        # 默认是最近7天
        now = datetime.datetime.now()
        end_spend_date = now.date()
        start_spend_date = (now - datetime.timedelta(days=6)).strftime("%Y-%m-%d")
        end_spend_date = end_spend_date.strftime("%Y-%m-%d")
        if all([kwargs.get('start_spend_date'), kwargs.get('end_spend_date')]):
            end_spend_date = kwargs.get('end_spend_date')
            start_spend_date = kwargs.get('start_spend_date')
        meta_accounts_id = []
        google_accounts_id = []
        kwai_accounts_id = []
        tiktok_accounts_id = []
        x_accounts_id = []
        crm_data = crm_result.get('data')
        if not crm_data:
            df = pd.DataFrame(crm_data)
            return df
        # 去除广告账户里面的-
        for account in crm_data:
            if account.get("medium").lower() == "meta":
                meta_accounts_id.append(account.get("account_id").replace('-', ''))
            elif account.get("medium").lower() == "google":
                google_accounts_id.append(account.get("account_id").replace('-', ''))
            elif account.get("medium").lower() == "tiktok":
                tiktok_accounts_id.append(account.get("account_id").replace('-', ''))
            elif account.get("medium").lower() == "kwai":
                kwai_accounts_id.append(account.get("account_id").replace('-', ''))
            elif account.get("medium").lower() == "x":
                x_accounts_id.append(account.get("account_id").replace('-', ''))
            else:
                continue
        for item in crm_data:
            if isinstance(item, dict) and "account_id" in item:
                item["account_ids"] = item["account_id"].replace('-', '')
        account_info = {
            "date_start": start_spend_date,
            "date_end": end_spend_date,
            "meta_accounts_id": meta_accounts_id,
            "google_accounts_id": google_accounts_id,
            "kwai_accounts_id": kwai_accounts_id,
            "tiktok_accounts_id": tiktok_accounts_id,
            "x_accounts_id": x_accounts_id
        }
        try:
            rtdp_result = RTDPService.account_spend(
                json=account_info,
                headers={'trace_id': kwargs.get('trace_id')}
            )
        except:
            account_data = [{"account_id": "", "medium": "", "spend": ""}]
        else:
            account_data = [
                {"account_id": account["account_id"], "medium": account["medium"], "spend": account["spend"]} for
                account in rtdp_result] if rtdp_result else [{"account_id": "", "medium": "", "spend": ""}]
        df_crm = pd.DataFrame(crm_data)
        df_rtdp = pd.json_normalize(account_data, sep='-')
        # 将df_rtdp转换为DataFrame，并提取account_id、medium和spend值
        df_rtdp.columns = ['account_id', 'medium', 'spend']
        # 合并两个DataFrame
        df = pd.merge(df_crm, df_rtdp, how='left', left_on=['account_ids', 'medium'], right_on=['account_id', 'medium'])
        df['spend'] = df['spend'].fillna('-')
        # 删除重复的数据
        df = df.drop_duplicates()
        if df.empty:
            return df
        df['account_status'] = df['account_status'].map(medium_account_status_object).fillna('-')
        # 删除不需要的列
        df.drop(columns=["id", "account_ids", "account_id_y"], inplace=True)
        df.rename(
            columns={
                "customer_name": "结算名称",
                "open_subject": "开户主体",
                "account_id_x": "广告账户id",
                "account_name": "广告账户名称",
                "account_status": "账户状态",
                "medium": "投放媒介",
                "put_way_name": "投放方式",
                "yesterday_spend": "昨日消耗",
                "available_balance": "账户余额($)",
                "amount_spent": "已花费金额($)",
                "spend_cap": "花费上限($)/总预算($)",
                "open_date": "开户时间",
                "update_time": "更新时间",
                "spend": "消耗"
            },
            inplace=True,
        )
        account_ids = df['广告账户id'].to_list()
        # 二代主账户
        if is_second_ and is_primary and not df.empty:
            # 获取redis数据
            df['项目组名称'] = ''
            pipe_account_list, group_name_mapping = get_redis_account_group(account_ids)
            for item, account_groups in zip(account_ids, pipe_account_list):
                if project_group:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                        if group_id == project_group
                    ]
                else:
                    group_names = [
                        group_name_mapping.get(group_id, '') for group_id in (account_groups or [])
                    ]
                group_name_str = ", ".join(group_names) or '-'
                df.loc[df['广告账户id'] == item, '项目组名称'] = group_name_str
            if project_group:
                df = df[df['项目组名称'] == project_group]
        df["广告账户id"] = df["广告账户id"].apply(lambda x: x + "\t")
        return df

    @staticmethod
    def bill_summary(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        """
        账单总览导出
        """
        db = SessionLocal()
        json_ = {
            "customer_ids": get_customer_ids(db, user_id),
        }
        if customer_id:
            json_["customer_id"] = customer_id
        if medium:
            json_["medium"] = medium
        if kwargs.get('q'):
            json_["q"] = kwargs.get('q')
        if kwargs.get('put_way'):
            json_["put_way"] = kwargs.get('put_way')
        if kwargs.get('is_cancel'):
            json_["is_cancel"] = int(kwargs.get('is_cancel'))
        if start_date and end_date:
            json_["start_month"] = start_date
            json_["end_month"] = end_date
        result = CRMExternalService().post_bills_export(
            json_,
            headers={"advertiser_user_id": str(user_id)},
            **{'trace_id': kwargs.get('trace_id')}
        )
        df = pd.read_excel(io.BytesIO(result))
        if df.empty:
            return pd.DataFrame()
        return df

    @staticmethod
    def rebate(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        """
        返点使用记录导出
        """
        db = SessionLocal()
        json_ = {
            "customer_ids": get_customer_ids(db, user_id),
        }
        if kwargs.get('q'):
            json_["q"] = kwargs.get('q')
        if kwargs.get('approval_status'):
            json_["approval_status"] = kwargs.get('approval_status')
        if kwargs.get('use_customer_id'):
            json_["use_customer_id"] = kwargs.get('use_customer_id')
        if kwargs.get("rebate_customer_id"):
            json_["rebate_customer_id"] = kwargs.get('rebate_customer_id')
        if kwargs.get("use_way"):
            json_["use_way"] = kwargs.get('use_way')
        if kwargs.get("rebate_date_start") and kwargs.get("rebate_date_end"):
            json_["rebate_date_start"] = kwargs.get('rebate_date_start')
            json_["rebate_date_end"] = kwargs.get('rebate_date_end')
        if start_date and end_date:
            json_["use_date_start"] = start_date
            json_["use_date_end"] = end_date
        result = CRMExternalService().post_rebate_uses_export(
            json_,
            headers={"advertiser_user_id": str(user_id)},
            **{'trace_id': kwargs.get('trace_id')}
        )
        df = pd.read_excel(io.BytesIO(result))
        if df.empty:
            return pd.DataFrame()
        return df

    @staticmethod
    def bill_detail(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        """
        账单总览查看明细导出
        """
        json = {
            "pk": kwargs.get('bill_id'),
            "q": kwargs.get('q')
        }
        result = CRMExternalService().post_bills_detail_export(
            json=json,
            headers={"advertiser_user_id": str(user_id)},
            **{'trace_id': kwargs.get('trace_id')}
        )
        df = pd.read_excel(io.BytesIO(result))
        if df.empty:
            return pd.DataFrame()
        df["广告账户"] = df["广告账户"].apply(lambda x: str(x) + "\t")
        return df

    @staticmethod
    def accounts_info(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        """
        账户信息
        """
        db = SessionLocal()
        customer_ids = get_customer_ids(db, user_id)
        res_user = db.query(AdvertiserRegister).filter(AdvertiserRegister.user_id == user_id,
                                                       AdvertiserRegister.is_delete == False).first()
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == user_id,
                                               AdvertiserUser.is_delete == False).first()
        db.close()
        json_ = {
            "customer_ids": customer_ids,
            "is_second": True if res_user.is_second else False,
            "is_primary": False if user.p_id else True,
            "medium": ["Google", "TikTok", "Meta"]
        }
        crm_result = CRMExternalService.post_account_export(
            json_,
            headers={"advertiser_user_id": str(user_id)},
            **{'trace_id': kwargs.get('trace_id')}
        )
        if not crm_result.get('data'):
            df = pd.DataFrame(crm_result)
            return df
        account_info = crm_result["data"]
        df = pd.DataFrame(account_info)
        # 需要导出的字段名称
        field_df = df[['customer_name', 'account_id', 'account_name', 'medium', 'account_status']].copy()
        if field_df.empty:
            return field_df
        field_df['account_status'] = field_df['account_status'].map(medium_account_status_object).fillna('-')
        field_df["account_id"] = field_df["account_id"].apply(lambda x: str(x) + "\t")
        return field_df

    @staticmethod
    def oe_open_account(
            project_group=None,
            account_id=None,
            medium=None,
            customer_id=None,
            operate_result=None,
            user_id=None,
            operate_user_id=None,
            start_date=None,
            end_date=None,
            file_id=None,
            **kwargs
    ) -> pd.DataFrame:
        where = [OeOpenAccount.is_delete == False]
        if kwargs.get('q'):
            where.append(or_(OeOpenAccount.oe_number.like(f'%{kwargs.get("q")}%'),
                             OeOpenAccount.chinese_legal_entity_name.like(f'%{kwargs.get("q")}%')))
        if kwargs.get("oe_status"):
            where.append(OeOpenAccount.approval_status == (kwargs.get("oe_status")))
        if kwargs.get("account_status"):
            where.append(OeOpenAccount.ad_account_creation_request_status == (kwargs.get("account_status")))
        if all([start_date, end_date]):
            where.extend(
                [
                    func.date(OeOpenAccount.created_time) >= start_date,
                    func.date(OeOpenAccount.created_time) <= end_date
                ]
            )
        with SessionLocal() as db:
            customer_ids = get_customer_ids(db, user_id)
            # customer_ids = [3410]
            where.append(OeOpenAccount.customer_id.in_(customer_ids))
            query = db.query(
                OeOpenAccount.id.label('id'),
                OeOpenAccount.oe_number.label('OE参考编号'),
                OeOpenAccount.created_time.label('申请时间'),
                OeOpenAccount.chinese_legal_entity_name.label('营业执照名称'),
                OeOpenAccount.approval_status.label('审批状态'),
                OeOpenAccount.ad_account_creation_request_status.label('账户开户状态'),
                OeOpenAccount.ad_accounts.label('广告账户详情')
            ).filter(*where).order_by(OeOpenAccount.id.desc())
        df = pd.read_sql_query(query.statement, engine.connect())
        if df.empty:
            return df
        df_id_account_info = df[['id', '广告账户详情']]
        df_to_dict = df_id_account_info.to_dict('records')
        df_list = list()
        for i in df_to_dict:
            id_, v = i.get('id'), i.get('广告账户详情')
            if v:
                for account_info in v:
                    df_list.append(
                        {
                            'id': id_,
                            '广告账户id': account_info.get('ad_account_id'),
                            '广告账户名称': account_info.get('ad_account_name')
                        }
                    )
            else:
                df_list.append(
                    {
                        'id': id_,
                        '广告账户id': '-',
                        '广告账户名称': '-'
                    }
                )
        df_analyse_after = pd.DataFrame(df_list).fillna('-')
        df_res = pd.merge(df, df_analyse_after, on=['id'], how='right')
        df_res.drop('广告账户详情', axis=1, inplace=True)
        df_res.drop('id', axis=1, inplace=True)
        return df_res


if __name__ == '__main__':
    """
    开发使用
    """
    # e = Export()
    # e.balance_transfer()
    # is_second = get_is_second(user_id=244)
    # print(is_second, type(is_second))

    # account_ids = ['2', '154576765657', '78881898911']
    # account_ids = ['154576765657', '78881898911']
    # not_allow_account = permission_check(263, ['154576765657', '78881898911'], "账户重命名")
    # print(not_allow_account)

    # user_account = user_authorization_account(263)
    # print(user_account, type(user_account))

    Export().oe_open_account(user_id='248')
