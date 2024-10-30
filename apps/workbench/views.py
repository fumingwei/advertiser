import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from fastapi_utils.cbv import cbv
from fastapi import APIRouter, Depends, Query
from settings.db import get_db, RedisClient
from settings.base import configs
from sqlalchemy.orm import Session
from starlette.requests import Request
from libs.internal.rtdp_service import RTDPService
from apps.accounts.utils import get_customer_ids
from apps.onboarding.models import OeOpenAccount
from apps.pixel.models import PixelAccount
from apps.advertiser.models import AdvertiserUser
from apps.onboarding.define import OeApproveStatus
from apps.pixel.define import AdvertiserStatusResult
from apps.workbench.define import Date
from tools.resp import MyResponse
from tools.constant import RET

WorkbenchRouter = APIRouter(tags=["工作台"])


@cbv(WorkbenchRouter)
class WorkbenchServer:
    request: Request

    @WorkbenchRouter.get('/account_info', description='账户信息')
    async def account_info(self, db: Session = Depends(get_db)):
        now = datetime.now()
        user_id = self.request.state.user.user_id
        mobile = db.query(AdvertiserUser.mobile).filter(AdvertiserUser.id == user_id,
                                                        AdvertiserUser.is_delete == False).scalar()
        r = RedisClient(db=configs.configs.REDIS_STORAGE.get('workbench')).get_redis_client()
        total_data = json.loads(r.hget(f"user:{mobile}", "total_data")) if r.hget(
            f"user:{mobile}", "total_data") else r.hget(f"user:{mobile}", "total_data")
        data_end = r.hget(f"user:{mobile}", "cs_data_end")
        today = now.strftime('%Y-%m-%d')[:10]
        if data_end != today:
            customer_ids = get_customer_ids(db, user_id)
            # 调用rtdp服务获取账户近7天总花费
            total_data = RTDPService.last_7_days_total_cost(
                json={"customer_ids": customer_ids},
                **{'trace_id': self.request.state.trace_id}
            )
        return MyResponse(code=RET.OK, data=total_data)

    @WorkbenchRouter.get('/account_processed', description='获取正在处理中的开户或者Pixel')
    async def account_processed(self, db: Session = Depends(get_db)):
        oe_where = [OeOpenAccount.is_delete == False, OeOpenAccount.approval_status == OeApproveStatus.PENDING.value]
        pixel_where = [PixelAccount.is_delete == False,
                       PixelAccount.operate_result == AdvertiserStatusResult.DEFAULT.value]
        user_id = self.request.state.user.user_id
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False)
            .first()
        )
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
            )
            sub_ids = [i.id for i in sub_user.all()]
            sub_ids.append(user.id)
            oe_where.append(OeOpenAccount.user_id.in_(sub_ids))
            pixel_where.append(PixelAccount.user_id.in_(sub_ids))
        # 如果是子账号
        else:
            oe_where.append(OeOpenAccount.user_id == user.id)
            pixel_where.append(PixelAccount.user_id == user.id)
        oe_open_account = db.query(OeOpenAccount).filter(*oe_where).count()
        pixel_account = db.query(PixelAccount).filter(*pixel_where).count()
        processed_data = {"oe_open_account": oe_open_account, "pixel_account": pixel_account}
        return MyResponse(code=RET.OK, data=processed_data)

    @WorkbenchRouter.get('/spend_rank', description='账户消耗排名')
    async def spend_rank(self, date: str = Query(None),
                         db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        mobile = db.query(AdvertiserUser.mobile).filter(AdvertiserUser.id == user_id,
                                                        AdvertiserUser.is_delete == False).scalar()
        r = RedisClient(db=configs.configs.REDIS_STORAGE.get('workbench')).get_redis_client()
        new_data = json.loads(r.hget(f"user:{mobile}", "cs_rank")) if r.hget(f"user:{mobile}", "cs_rank") else r.hget(
            f"user:{mobile}", "cs_rank")
        if not date:
            date = Date.TODAY.value
        approval_status = Date[date.upper()]
        desc = approval_status.desc
        rank_datetime = desc[0]
        data_end = r.hget(f"user:{mobile}", "cs_data_end")
        if rank_datetime != data_end:
            customer_ids = get_customer_ids(db, user_id)
            account_dict = dict()
            account_dict['customer_ids'] = customer_ids
            # 获取开始日期和结束日期
            account_dict['date_start'] = desc[0]
            account_dict['date_end'] = desc[1]
            account_data = RTDPService.spend_rank(account_dict, **{'trace_id': self.request.state.trace_id})
            new_data = []
            for item in account_data:
                new_item = {
                    "account": f"{item['account_name']}-{item['medium']}-{item['account_id']}",
                    "spend": item['total_spend']
                }
                new_data.append(new_item)
        return MyResponse(code=RET.OK, data=new_data)

    @WorkbenchRouter.get('/spend_data', description='消耗数据')
    async def spend_data(self, start_date: str = Query(default='', regex=r'^\d{4}-\d{1,2}-\d{1,2}$'),
                         end_date: str = Query(default='', regex=r'^\d{4}-\d{1,2}-\d{1,2}$'),
                         db: Session = Depends(get_db)):
        now = datetime.now()
        user_id = self.request.state.user.user_id
        if not start_date and not end_date:
            # 计算一周前的时间
            start_date = (now - timedelta(days=6)).strftime('%Y-%m-%d')[:10]
            end_date = now.strftime('%Y-%m-%d')[:10]
        mobile = db.query(AdvertiserUser.mobile).filter(AdvertiserUser.id == user_id,
                                                        AdvertiserUser.is_delete == False).scalar()
        r = RedisClient(db=configs.configs.REDIS_STORAGE.get('workbench')).get_redis_client()
        new_data = json.loads(r.hget(f"user:{mobile}", "cs_data")) if r.hget(f"user:{mobile}", "cs_data") else r.hget(
            f"user:{mobile}", "cs_data")
        cs_data_start = r.hget(f"user:{mobile}", "cs_data_start")
        cs_data_end = r.hget(f"user:{mobile}", "cs_data_end")
        if start_date != cs_data_start or end_date != cs_data_end:
            spend_data = dict()
            spend_data['date_start'] = cs_data_start
            spend_data['date_end'] = cs_data_end
            if start_date and end_date:
                start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
                end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
                # 计算过去六个月的时间
                six_months_before = end_datetime - relativedelta(months=6)
                if start_datetime < six_months_before:
                    return MyResponse(code=RET.PARAM_ERR, msg="该系统仅支持查询半年内的消耗数据")
                spend_data['date_start'] = start_datetime.strftime('%Y-%m-%d')
                spend_data['date_end'] = end_datetime.strftime('%Y-%m-%d')
            customer_ids = get_customer_ids(db, user_id)
            spend_data['customer_ids'] = customer_ids
            account_data = RTDPService.insight_spend(spend_data, **{'trace_id': self.request.state.trace_id})
            start_time = datetime.strptime(spend_data['date_start'], "%Y-%m-%d")
            end_time = datetime.strptime(spend_data['date_end'], "%Y-%m-%d")
            x = [(start_time + timedelta(days=x)).strftime("%Y-%m-%d") for x in range((end_time - start_time).days + 1)]
            y = []
            for platform, platform_data in account_data.items():
                platform_name = platform.capitalize()
                platform_result = {"name": platform_name, "data": []}
                for date in x:
                    spend_value = next((d["spend"] for d in platform_data if d["date"] == date), 0)
                    platform_result["data"].append(spend_value)
                y.append(platform_result)
            new_data = dict()
            new_data["line_data"] = {"x": x, "y": y}
        return MyResponse(code=RET.OK, data=new_data)
