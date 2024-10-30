from fastapi import APIRouter, Depends, Query
from starlette.requests import Request
from fastapi_utils.cbv import cbv
from tools.common import CommonQueryParams
from typing import Optional
from sqlalchemy.orm import Session
from settings.db import get_db
from apps.operation.utils import OperationListUtils, OperationDetailsUtils, OperationMediums
from tools.resp import MyResponse
from apps.accounts.utils import get_customer_ids
from apps.operation.define import OperationType, AccountRechargeStatus, BalanceTransferStatus, BcBmPixelAccount, \
    ResetStatus, RenameStatus, OpenAccountStatus, MediaType

OperationRouter = APIRouter(tags=["操作记录"])


@cbv(OperationRouter)
class WorkbenchServer:
    request: Request

    @OperationRouter.get('/account_rename', description="账户重命名记录列表")
    async def get_account_rename(
            self,
            start_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            end_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            release_media: Optional[str] = None,
            operation_result: Optional[str] = None,
            params: CommonQueryParams = Depends(),
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        data, total = OperationListUtils.accounts_rename(
            db=db, user_id=user_id, params=params, start_date=start_date, end_date=end_date,
            operation_result=operation_result, release_media=release_media
        )
        return MyResponse(data=data, total=total)

    @OperationRouter.get('/bc_account', description="BC账户记录列表")
    async def get_bc_account(
            self,
            start_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            end_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            release_media: Optional[str] = None,
            operation_result: Optional[str] = None,
            params: CommonQueryParams = Depends(),
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        data, total = OperationListUtils.bc_accounts(
            db=db, user_id=user_id, params=params, start_date=start_date, end_date=end_date,
            operation_result=operation_result, release_media=release_media,
        )
        return MyResponse(data=data, total=total)

    @OperationRouter.get('/bm_account', description="BM账户记录列表")
    async def get_bm_account(
            self,
            start_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            end_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            release_media: Optional[str] = None,
            operation_result: Optional[str] = None,
            params: CommonQueryParams = Depends(),
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        data, total = OperationListUtils.bm_accounts(
            db=db, user_id=user_id, params=params, start_date=start_date, end_date=end_date,
            operation_result=operation_result, release_media=release_media
        )
        return MyResponse(data=data, total=total)

    @OperationRouter.get('/pixel', description="Pixel操作记录列表")
    async def get_pixel_operation(
            self,
            start_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            end_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            release_media: Optional[str] = None,
            operation_result: Optional[str] = None,
            params: CommonQueryParams = Depends(),
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        data, total = OperationListUtils.pixel_utils(
            db=db, user_id=user_id, params=params, start_date=start_date, end_date=end_date,
            operation_result=operation_result, release_media=release_media,

        )
        return MyResponse(data=data, total=total)

    @OperationRouter.get('/account_rename/{operation_id}', description="账户重命名操作记录/详情页")
    async def get_account_rename_detail(self, operation_id: int, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        # 调用账户重命名详情处理函数
        return await OperationDetailsUtils.get_account_rename_detail(operation_id, userid=user_id, db=db)

    @OperationRouter.get('/bm_account/{operation_id}', description="BM账户操作记录/详情页")
    async def get_bm_account_detail(self, operation_id: int, db: Session = Depends(get_db),
                                    common_query: CommonQueryParams = Depends()):
        user_id = self.request.state.user.user_id
        # 调用BM账户详情处理函数
        return await OperationDetailsUtils.bm_detail(operation_id, common_query=common_query, user_id=user_id, db=db)

    @OperationRouter.get('/bc_account/{operation_id}', description="BC账户操作记录/详情页")
    async def get_bc_account_detail(self, operation_id: int, db: Session = Depends(get_db),
                                    common_query: CommonQueryParams = Depends()):
        user_id = self.request.state.user.user_id
        # 调用BC账户详情处理函数
        return await OperationDetailsUtils.bc_detail(operation_id, common_query=common_query, user_id=user_id, db=db)

    @OperationRouter.get('/pixel/{operation_id}', description="Pixel操作记录/详情页")
    async def get_pixel_detail(self, operation_id: int, db: Session = Depends(get_db),
                               common_query: CommonQueryParams = Depends()):
        user_id = self.request.state.user.user_id
        # 调用Pixel详情处理函数
        return await OperationDetailsUtils.pixel_details(operation_id, common_query=common_query, user_id=user_id,
                                                         db=db)

    @OperationRouter.get('/operation_result', description="操作记录/获取操作结果")
    def get_operation_status(
            self,
            operation_type: OperationType = Query(OperationType.ACCOUNT_RECHARGE,
                                                  )
    ):
        # 操作类型与状态枚举的映射
        status_map = {
            OperationType.ACCOUNT_RECHARGE: AccountRechargeStatus,
            OperationType.ACCOUNT_RESET: ResetStatus,
            OperationType.BALANCE_TRANSFER: BalanceTransferStatus,
            OperationType.ACCOUNT_RENAME: RenameStatus,
            OperationType.BC_ACCOUNT: BcBmPixelAccount,
            OperationType.BM_ACCOUNT: BcBmPixelAccount,
            OperationType.PIXEL: BcBmPixelAccount,
            OperationType.OPEN_ACCOUNT: OpenAccountStatus
        }

        # 获取对应状态枚举
        status_enum = status_map.get(operation_type)
        # 格式化结果
        data = [{"label": status.value, 'en_label': status.name, "value": status.value} for status in status_enum]
        return MyResponse(data=data)

    @OperationRouter.get('/operation_media', description="操作记录/获取媒介结果")
    async def get_media_options(self, operation_type: OperationType = Query(OperationType.ACCOUNT_RECHARGE,
                                                                            ), db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        sub_user_ids = self.request.state.user.sub_user_ids
        operation_map = {
            OperationType.ACCOUNT_RECHARGE: (
                OperationMediums.get_recharges_mediums,
                dict(db=db, user_id=user_id, trace_id=self.request.state.trace_id
                     )
            ),
            OperationType.ACCOUNT_RESET: (
                OperationMediums.get_reset_mediums,
                dict(db=db, user_id=user_id,
                     trace_id=self.request.state.trace_id
                     )
            ),
            OperationType.BALANCE_TRANSFER: (
                OperationMediums.get_balance_mediums,
                dict(db=db, user_id=user_id, sub_user_ids=sub_user_ids)
            ),
            OperationType.ACCOUNT_RENAME: (
                OperationMediums.get_renames_mediums,
                dict(db=db, user_id=user_id)
            ),
            OperationType.BC_ACCOUNT: (
                OperationMediums.get_bc_mediums,
                dict(db=db, user_id=user_id)
            ),
            OperationType.BM_ACCOUNT: (
                OperationMediums.get_bm_mediums,
                dict(db=db, user_id=user_id)
            ),
            OperationType.PIXEL: (
                OperationMediums.get_pixel_mediums,
                dict(db=db, user_id=user_id)
            ),
        }
        func, args = operation_map.get(operation_type)
        result = func(**args)
        # 构建返回的 media_options 格式
        media_options = [{"label": medium, "value": medium} for medium in result]
        return MyResponse(data=media_options)


# 交易记录
@cbv(OperationRouter)
class TransactionRecordServer:
    request: Request

    @OperationRouter.get('/recharges', description="充值列表")
    async def get_recharge_list(
            self,
            start_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            end_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            medium: Optional[str] = None,
            operation_result: Optional[str] = None,
            params: CommonQueryParams = Depends(),
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        data, total = OperationListUtils.recharges_list(
            db=db, user_id=user_id, params=params, start_date=start_date, end_date=end_date,
            operation_result=operation_result, medium=medium
        )
        return MyResponse(data=data, total=total)

    @OperationRouter.get('/recharges/{recharge_id}', description="充值详情页")
    async def get_recharge_detail(self, recharge_id: int, common_query: CommonQueryParams = Depends()):
        user_id = self.request.state.user.user_id
        # 调用获取充值详情
        return await OperationDetailsUtils.fetch_recharge_detail(id=recharge_id,
                                                                 common_query=common_query,
                                                                 user_id=user_id)

    @OperationRouter.get('/resets', description="清零列表")
    async def get_reset_list(
            self,
            start_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            end_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            medium: Optional[str] = None,
            operation_result: Optional[str] = None,
            params: CommonQueryParams = Depends(),
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        data, total = OperationListUtils.resets_list(
            db=db, user_id=user_id, params=params, start_date=start_date, end_date=end_date,
            operation_result=operation_result, medium=medium
        )
        return MyResponse(data=data, total=total)

    @OperationRouter.get('/resets/{reset_id}', description="清零详情页")
    async def get_reset_detail(self, reset_id: int, common_query: CommonQueryParams = Depends()):
        user_id = self.request.state.user.user_id
        # 调用获取清零详情
        return await OperationDetailsUtils.fetch_reset_detail(id=reset_id, common_query=common_query, user_id=user_id)

    @OperationRouter.get('/balance_transfer', description="余额转移列表")
    async def get_balance_transfer_list(
            self,
            start_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            end_date: Optional[str] = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
            release_media: Optional[str] = None,
            operation_result: Optional[str] = None,
            params: CommonQueryParams = Depends(),
            db: Session = Depends(get_db),
    ):
        user_id = self.request.state.user.user_id
        sub_user_ids = self.request.state.user.sub_user_ids
        customer_ids = get_customer_ids(db, user_id)
        # user_id = 242
        # customer_ids = [2873]
        data, total = OperationListUtils.balance_transfer(
            db=db, user_id=user_id, sub_user_ids=sub_user_ids, customer_ids=customer_ids,
            params=params, start_date=start_date, end_date=end_date, release_media=release_media,
            operation_result=operation_result)
        return MyResponse(data=data, total=total)

    @OperationRouter.get('/balance_transfer/{id}', description="余额转移详情页")
    async def get_balance_transfer_detail(self, id: int, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        # user_id = 242
        # 调用获取余额转移详情
        return await OperationDetailsUtils.balance_transfer_detail(id=id, user_id=user_id, db=db)
