from datetime import datetime
from uuid import uuid4
from fastapi import APIRouter, Depends, Query
from fastapi_utils.cbv import cbv
from sqlalchemy import case, func, and_
from sqlalchemy.orm import Session
from starlette.requests import Request
from apps.common.utils import get_is_second
from libs.internal.crm_external_service import CRMExternalService
from apps.common.define import FileStatus, OperateTypeDefine, EXPORTOperationType
from apps.common.models import CuFile
from apps.common.schemas import ExportSchema
from apps.common.tasks import asy_recharge_export, asy_reset_export, asy_pixel_export, \
    asy_rename_export, asy_bc_export, asy_bm_export, asy_all_operate_export, asy_accounts_export, \
    asy_bill_summary_export, asy_account_info_export, asy_bill_detail_export, asy_rebate_uses_export, \
    asy_oe_open_account_export, asy_balance_transfer_export
from apps.advertiser.models import AdvertiserUser, ProjectGroup, GroupMemberRelationship
from my_celery.utils import common_task
from settings.base import configs
from settings.db import get_db, MyPagination
from tools.common import CommonQueryParams
from tools.constant import RET, all_operate_results, all_operate_results_english
from tools.resp import MyResponse

CommonRouter = APIRouter(tags=['公共'])


@cbv(CommonRouter)
class DownloadCenter:
    request: Request

    @CommonRouter.post("/export", description='导出')
    async def export(
            self,
            data: ExportSchema,
            db: Session = Depends(get_db)
    ):
        data = data.dict()
        """
        导出任务分发,异步导出文件
        阿里云/为下个目录，sheet表不能存在/
        """
        operate_type_task_dict = {
            OperateTypeDefine.RECHARGE.value: {
                'file_name': OperateTypeDefine.RECHARGE.desc,
                'task': asy_recharge_export
            },
            OperateTypeDefine.RESET.value: {
                'file_name': OperateTypeDefine.RESET.desc,
                'task': asy_reset_export
            },
            OperateTypeDefine.BalanceTransfer.value: {
                'file_name': OperateTypeDefine.BalanceTransfer.desc,
                'task': asy_balance_transfer_export
            },
            OperateTypeDefine.RENAME.value: {
                'file_name': OperateTypeDefine.RENAME.desc,
                'task': asy_rename_export
            },
            OperateTypeDefine.PixelBindAccount.value: {
                'file_name': OperateTypeDefine.PixelBindAccount.desc,
                'task': asy_pixel_export
            },
            OperateTypeDefine.BmBindAccount.value: {
                'file_name': OperateTypeDefine.BmBindAccount.desc,
                'task': asy_bm_export
            },
            OperateTypeDefine.BcBindAccount.value: {
                'file_name': OperateTypeDefine.BcBindAccount.desc,
                'task': asy_bc_export
            },
            OperateTypeDefine.ACCOUNTLIST.value: {
                'file_name': OperateTypeDefine.ACCOUNTLIST.desc,
                'task': asy_accounts_export
            },
            # OperateTypeDefine.ALL.value: {
            #     'file_name': OperateTypeDefine.ALL.desc,
            #     'task': asy_all_operate_export
            # },
            OperateTypeDefine.BillSummary.value: {
                'file_name': OperateTypeDefine.BillSummary.desc,
                'task': asy_bill_summary_export
            },
            OperateTypeDefine.AccountInfo.value: {
                'file_name': OperateTypeDefine.AccountInfo.desc,
                'task': asy_account_info_export
            },
            OperateTypeDefine.Rebate.value: {
                'file_name': OperateTypeDefine.Rebate.desc,
                'task': asy_rebate_uses_export
            },
            OperateTypeDefine.BillDetail.value: {
                'file_name': OperateTypeDefine.BillDetail.desc,
                'task': asy_bill_detail_export
            },
            OperateTypeDefine.OpenAccountHistory.value: {
                'file_name': OperateTypeDefine.OpenAccountHistory.desc,
                'task': asy_oe_open_account_export
            }
        }
        user_id = self.request.state.user.user_id
        customer_name = None
        use_customer_name = None
        rebate_customer_name = None
        name_map = {
            "customer_id": customer_name,
            "use_customer_id": use_customer_name,
            "rebate_customer_id": rebate_customer_name
        }
        for key, method in name_map.items():
            if data.get(key):
                json_params = {
                    "customer_ids": [data.get(key)]
                }
                res = CRMExternalService.customer_id_name(json=json_params, **{'trace_id': self.request.state.trace_id})
                name = res.get('data', [])[-1].get('name')
                name_map[key] = name
        if data.get('source'):
            file_name = '操作记录.xlsx'
        else:
            file_name = operate_type_task_dict.get(data['operate_type'])['file_name'] + '.xlsx'
        if data.get('operate_user'):
            operate_user = db.query(AdvertiserUser.real_name).filter(
                AdvertiserUser.is_delete == False,
                AdvertiserUser.id == data.get('operate_user')
            ).first()
            if not operate_user:
                return MyResponse(code=RET.DB_ERR, msg='该账户异常，或不存在')
            operate_user = operate_user.real_name
        else:
            operate_user = None
        cn_result = ''
        for k, v in all_operate_results.get(data['operate_type'], {}).items():
            if data.get('operation_result'):
                for i in data.get('operation_result'):
                    if v == i:
                        cn_result += f'-{k}'
        params = {
            "搜索": data.get('q'),
            '操作类型': data.get('operate_type'),
            '项目组': data.get('project_group'),
            '广告账户id': data.get('account_id'),
            '媒介': data.get('medium'),
            '客户简称': name_map.get("customer_id"),
            '操作结果': cn_result,
            '操作人': operate_user,
            '合作伙伴ID': data.get('cooperative_id'),
            'BM_ID': data.get('bm_id'),
            '开始时间': data.get('start_date'),
            '结束时间': data.get('end_date'),
            '消耗开始时间': data.get('start_spend_date'),
            '消耗结束时间': data.get('end_spend_date'),
            '是否核销': data.get('is_cancel'),
            '账单开始月份': data.get('start_month'),
            '账单结束月份': data.get('end_month'),
            '返点使用结算': name_map.get("use_customer_id"),
            '返点所属结算': name_map.get("rebate_customer_id"),
            '使用方式': data.get('use_way'),
            '投放方式': data.get('put_way'),
            '审批状态': data.get('approval_status'),
            'OE审批状态': data.get('oe_status'),
            '账户状态': data.get('account_status'),
            '返点开始季度': data.get('rebate_date_start'),
            '返点结束季度': data.get('rebate_date_end')
        }
        operate_type_to_params = {
            OperateTypeDefine.BillSummary.desc: ('生成开始时间', '生成结束时间'),
            OperateTypeDefine.Rebate.desc: ('使用开始时间', '使用结束时间')
        }
        for operate_type, (start_date_key, end_date_key) in operate_type_to_params.items():
            if data.get('operate_type') == operate_type:
                if "开始时间" and "结束时间" in params:
                    params[start_date_key] = params.pop('开始时间')
                    params[end_date_key] = params.pop('结束时间')
        if data.get('operate_type') == OperateTypeDefine.ACCOUNTLIST.value:
            params['开户开始时间'] = params.pop('开始时间')
            params['开户结束时间'] = params.pop('结束时间')
        description = file_name.split('.')[-2]
        for k, v in params.items():
            if v:
                if k == "是否核销":
                    v = '是' if data.get('is_cancel') == '1' else '否'
                if k == '使用方式':
                    use_ways = {
                        '1': "预付款充值",
                        '2': "后付款核销账单",
                        '3': "返点退款"
                    }
                    # 根据 v 的值获取对应的字符串
                    v = use_ways.get(v, use_ways.get(v))
                if k == '审批状态':
                    approval_status = {
                        '1': "审批中",
                        '2': "已通过",
                        '3': "已被拒",
                        '4': "已撤销",
                    }
                    # 根据 v 的值获取对应的字符串
                    v = approval_status.get(v, approval_status.get(v))
                if k == "返点开始季度" or k == "返点结束季度":
                    date = {
                        '01-01': "Q1",
                        '03-31': "Q1",
                        '04-01': "Q2",
                        '06-30': "Q2",
                        '07-01': "Q3",
                        '09-30': "Q3",
                        '10-01': "Q4",
                        '12-31': "Q4",
                    }
                    year = v[:4]
                    month = v[5:]
                    v = year + "年" + date.get(month, date.get(month))
                description += f'-{k}({v})'
        full_file_name = f"{file_name}-" + uuid4().hex + '.xlsx'
        file_key = f"{configs.OSS_PREFIX}/{user_id}/{full_file_name}"
        file_data = {
            'file_key': file_key,
            'file_name': file_name,
            'file_type': "xlsx",
            'file_status': FileStatus.PROCESS.value,
            'description': description,
            'upload_user_id': user_id
        }
        file_obj = CuFile(**file_data)
        db.add(file_obj)
        db.commit()
        common_task(
            operate_type_task_dict[data.get('operate_type')]['task'],
            (
                data.get('project_group'),
                data.get('account_id'),
                data.get('medium'),
                data.get('customer_id'),
                data.get('operation_result'),
                self.request.state.user.user_id,
                data.get('operate_user'),
                data.get('start_date'),
                data.get('end_date'),
                file_obj.id
            ),
            {
                'q': data.get('q'),
                'cooperative_id': data.get('cooperative_id'),
                'bm_id': data.get('bm_id'),
                'start_spend_date': data.get('start_spend_date'),
                'end_spend_date': data.get('end_spend_date'),
                'trace_id': self.request.state.trace_id,
                'bill_id': data.get('bill_id'),
                'is_cancel': data.get('is_cancel'),
                'start_month': data.get('start_month'),
                'end_month': data.get('end_month'),
                'use_customer_id': data.get('use_customer_id'),
                'rebate_customer_id': data.get('rebate_customer_id'),
                'use_way': data.get('use_way'),
                'put_way': data.get('put_way'),
                'approval_status': data.get('approval_status'),
                'account_status': data.get('account_status'),
                'oe_status': data.get('oe_status'),
                'rebate_date_start': data.get('rebate_date_start'),
                'rebate_date_end': data.get('rebate_date_end')
            }
        )
        return MyResponse(msg='导出成功，请前往下载中心查看。')

    @CommonRouter.get('/files', description='文件列表')
    async def files(
            self, common_query: CommonQueryParams = Depends(),
            download_status: bool = None, file_status: str = None,
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        where = [CuFile.upload_user_id == user_id, CuFile.is_delete == False]
        if common_query.q:
            where.append(CuFile.file_name.ilike(f'%{common_query.q}%'))
        if download_status != None:
            where.append(CuFile.download_status == download_status)
        if file_status:
            where.append(CuFile.file_status == file_status)
        query = db.query(
            CuFile, AdvertiserUser.real_name,
            case([
                (CuFile.file_status == FileStatus.PROCESS.value,
                 FileStatus.PROCESS.desc),
                (CuFile.file_status == FileStatus.SUCCEED.value,
                 FileStatus.SUCCEED.desc)
            ], else_=FileStatus.FAIL.desc).label('cn_file_status'),
            case([
                (CuFile.download_status, "已下载"),
            ], else_="未下载").label('cn_download_status'),
            case([
                (and_(CuFile.expire_time >= func.now(), CuFile.file_status == FileStatus.SUCCEED.value), 0),
            ], else_=1).label('is_expire'),
            func.concat(configs.ALIOSS_URL, "/", CuFile.file_key).label("file_url")
        ).outerjoin(
            AdvertiserUser, AdvertiserUser.id == CuFile.upload_user_id
        ).filter(*where)
        paginator = MyPagination(query.order_by(-CuFile.id), common_query.page, common_query.page_size)
        return MyResponse(total=paginator.counts, data=paginator.data)

    @CommonRouter.post('/file_local/{id}', description='下载文件到本地')
    async def file_local(self, id: int, db: Session = Depends(get_db)):
        file = db.query(CuFile).filter(CuFile.id == id).first()
        if file.file_status != FileStatus.SUCCEED.value:
            return MyResponse(RET.DATA_ERR, "文件未生成成功~")
        if datetime.strptime(file.expire_time, "%Y-%m-%d %H:%M:%S") < datetime.now():
            return MyResponse(RET.DATA_ERR, "文件已过期")
        file.download_status = True
        db.commit()
        return MyResponse()


@cbv(CommonRouter)
class CommonViews:
    request: Request

    @CommonRouter.get('/submit_users', description='所有操作人')
    async def get_submit_user(self, common_query: CommonQueryParams = Depends(), db: Session = Depends(get_db)):
        """
        导出赛选条件，提交人
        """
        user_id = self.request.state.user.user_id
        where = [AdvertiserUser.is_delete == False]
        user = db.query(AdvertiserUser).filter(
            AdvertiserUser.id == user_id,
            AdvertiserUser.is_delete == False
        ).first()
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
            )
            sub_ids = [i.id for i in sub_user.all()]
            sub_ids.append(user.id)
            where.append(AdvertiserUser.id.in_(sub_ids))
        # 如果是子账号
        else:
            where.append(AdvertiserUser.id == user.id)
        query = db.query(
            AdvertiserUser.real_name.label('username'),
            AdvertiserUser.id.label('id'),
        ).filter(*where)
        pagination = MyPagination(query, common_query.page, common_query.page_size)
        is_second = get_is_second(user_id)
        return MyResponse(total=pagination.counts, data=pagination.data, other_data={'is_second': is_second})

    @CommonRouter.get('/groups', description='项目组')
    async def get_groups(self, common_query: CommonQueryParams = Depends(), db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        where = [GroupMemberRelationship.is_delete == False]
        user = db.query(
            AdvertiserUser
        ).filter(
            AdvertiserUser.id == user_id,
            AdvertiserUser.is_delete == False
        ).first()
        # 如果是主账号
        if not user.p_id:
            sub_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.p_id == user_id, AdvertiserUser.is_delete == False
            )
            sub_ids = [i.id for i in sub_user.all()]
            sub_ids.append(user.id)
            where.append(GroupMemberRelationship.user_id.in_(sub_ids))
        # 如果是子账号
        else:
            where.append(GroupMemberRelationship.user_id == user_id)
        filter_list = [ProjectGroup.is_delete == False]
        if common_query.q:
            filter_list.append(ProjectGroup.project_name.ilike(f'%{common_query.q}%'))
        project_ids = db.query(
            GroupMemberRelationship.project_group_id
        ).filter(*where)
        filter_list.append(ProjectGroup.id.in_(project_ids))
        query = db.query(
            ProjectGroup.project_name,
            ProjectGroup.id
        ).filter(*filter_list)
        pagination = MyPagination(query, common_query.page, common_query.page_size)
        return MyResponse(total=pagination.counts, data=pagination.data)

    @CommonRouter.get('/results', description='所有操作结果')
    async def get_results(self, operation_type: str = Query(description="操作类型，默认为账户充值")):
        res = list()
        for k, v in all_operate_results.get(operation_type, {}).items():
            res.append({
                'en_label': all_operate_results_english.get(k),
                'label': k,
                'value': v
            })
        return MyResponse(data=res)

    @CommonRouter.get('/operation_type_select', description='选择操作类型')
    async def operation_type_select(self):
        operation_type = [getattr(EXPORTOperationType, attr) for attr in EXPORTOperationType.__dict__ if
                          not attr.startswith('__')]
        operation_type_list = [
            {"label": value, "value": value}
            for value in operation_type
        ]
        return MyResponse(data=operation_type_list)
