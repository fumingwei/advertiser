# # -*- coding: utf-8 -*-
import datetime
from fastapi_utils.cbv import cbv
from fastapi import Depends, APIRouter, Query
from sqlalchemy import desc, case, literal_column, or_, func
from sqlalchemy.orm import Session
from starlette.requests import Request
from settings.db import get_db, MyPagination
from tools.common import CommonQueryParams
from apps.system.models import OperateLog, Modules, LoginHistory
from apps.advertiser.models import AdvertiserUser
from tools.resp import MyResponse

SystemRouter = APIRouter(tags=["系统管理"])


@cbv(SystemRouter)
class LogServer:
    request: Request

    # 操作日志
    @SystemRouter.get('/operate_log', description="操作日志")
    async def GetOperateLog(self, request_status: str = Query(None), common_query: CommonQueryParams = Depends(),
                            start_time: str = Query(None), end_time: str = Query(None), db: Session = Depends(get_db)):
        where = [OperateLog.is_delete == False]
        userid = self.request.state.user.user_id
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == userid,
                                               AdvertiserUser.is_delete == False).first()
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(AdvertiserUser.p_id == userid,
                                                       AdvertiserUser.is_delete == False)
            sub_ids = [i.id for i in sub_user.all()]
            sub_ids.append(user.id)
            where.append(OperateLog.request_user_id.in_(sub_ids))
        # 如果是子账号
        else:
            where.append(OperateLog.request_user_id == user.id)
        # 过滤掉日志记录查询接口请求
        if common_query.q:
            user_id = db.query(AdvertiserUser.id).filter(AdvertiserUser.real_name.ilike(f'%{common_query.q}%'))
            module_code = db.query(Modules.module_code).filter(Modules.module_name.ilike(f'%{common_query.q}%'))
            # 模糊搜索系统模块、操作地址、操作人员
            where.append(or_(
                OperateLog.request_user_id.in_(user_id),
                OperateLog.module.in_(module_code),
                OperateLog.request_ip.like(f'%{common_query.q}%')
            ))
        if request_status:
            where.append(OperateLog.request_status == request_status)
        if start_time and end_time:
            end_time = (datetime.datetime.strptime(end_time + " 00:00:00", "%Y-%m-%d %H:%M:%S") +
                        datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            where.append(OperateLog.created_time.between(start_time, end_time))
        anonymous_user = case([(OperateLog.request_user_id.is_(None), '匿名用户')],
                              else_=AdvertiserUser.real_name)
        query = db.query(OperateLog.id, OperateLog.created_time, OperateLog.operation_desc,
                         Modules.module_name.label("module_name"), anonymous_user.label("request_user"),
                         OperateLog.request_status, OperateLog.request_address,
                         OperateLog.request_ip,
                         (literal_column("CONCAT(spent_time, '毫秒')")).label("formatted_spent_time")
                         ).outerjoin(
            Modules, Modules.module_code == OperateLog.module).outerjoin(
            AdvertiserUser, AdvertiserUser.id == OperateLog.request_user_id).filter(*where)
        query = query.order_by(desc(OperateLog.id))
        obj = MyPagination(query, common_query.page, common_query.page_size)
        return MyResponse(total=obj.counts, data=obj.data)

    @SystemRouter.get('/login_log', description='登录日志')
    async def GetLoginLog(self, common_query: CommonQueryParams = Depends(), request_status: str = Query(None),
                          start_time: str = Query('', min_length=0, regex='^\d{4}-\d{1,2}-\d{1,2}'),
                          end_time: str = Query('', min_length=0, regex='^\d{4}-\d{1,2}-\d{1,2}'),
                          db: Session = Depends(get_db)):
        where = [LoginHistory.is_delete == False]
        userid = self.request.state.user.user_id
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == userid,
                                               AdvertiserUser.is_delete == False).first()
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(AdvertiserUser.p_id == userid,
                                                       AdvertiserUser.is_delete == False)
            sub_ids = [i.id for i in sub_user.all()]
            sub_ids.append(user.id)
            where.append(LoginHistory.user_id.in_(sub_ids))
        # 如果是子账号
        else:
            where.append(LoginHistory.user_id == user.id)
        # 搜索登录名称、登录地址、操作状态
        if common_query.q:
            user_id = db.query(AdvertiserUser.id).filter(AdvertiserUser.real_name.ilike(f'%{common_query.q}%'))
            where.append(or_(
                LoginHistory.user_id.in_(user_id),
                LoginHistory.ip.ilike(f'%{common_query.q}%')
            ))
        if request_status:
            where.append(LoginHistory.request_status == request_status)
        if all([start_time, end_time]):
            where.extend([
                func.date(LoginHistory.created_time) >= start_time,
                func.date(LoginHistory.created_time) <= end_time
            ])
        query = db.query(LoginHistory,
                         AdvertiserUser.real_name if LoginHistory.user_id else LoginHistory.mobile).outerjoin(
            AdvertiserUser, AdvertiserUser.id == LoginHistory.user_id).filter(
            *where).order_by(LoginHistory.id.desc())
        paginator = MyPagination(query, common_query.page, common_query.page_size)
        return MyResponse(total=paginator.counts, data=paginator.data)
