# -*- coding: utf-8 -*-
import ulid
from pprint import pformat
from my_celery.main import celery_app
from settings.base import configs
from settings.db import SessionLocal
from apps.callback.utils import function_dict
from apps.accounts.models import AccountRename, BmAccount, BcAccount
from apps.pixel.models import PixelAccount
from apps.accounts.define import AdvertiserStatusResult, OperateResult
from libs.internal.api_service import APIService
from settings.log import common_log


def update_account(request_id):
    params = {
        'api_key': configs.MAPI_KEY,
        'request_id': request_id
    }
    common_log.log_info(f'请求参数：\n{pformat(params)}')
    res = APIService.get_common_model_result(params)
    data = res.get('data', {})
    common_log.log_info(f'请求处理结果：\n{pformat(data)}')
    request_status = data.get('request_status')
    fun = function_dict.get(res.get('data', {}).get('operation_type'))
    if fun and request_status == "Finished":
        try:
            fun(res.get('data'))
        except Exception as e:
            common_log.log_info(f'tasks.py发生异常，回调失败原因：{e}')


@celery_app.task(name='update_accounts')
def update_accounts():
    with SessionLocal() as db:
        # 账户重命名
        account_name = db.query(AccountRename).filter(
            AccountRename.operate_result == OperateResult.DEFAULT.value,
            AccountRename.is_delete == False
        )
        for i in account_name:
            request_id = i.request_id
            update_account(request_id)
        # BM账户
        bm_account = db.query(BmAccount).filter(BmAccount.operate_result == AdvertiserStatusResult.DEFAULT.value,
                                                BmAccount.is_delete == False)
        for i in bm_account:
            request_id = i.request_id
            update_account(request_id)
        # BC账户
        bc_account = db.query(BcAccount).filter(BcAccount.operate_result == AdvertiserStatusResult.DEFAULT.value,
                                                BcAccount.is_delete == False)
        for i in bc_account:
            request_id = i.request_id
            update_account(request_id)
        # Pixel账户
        pixel_account = db.query(PixelAccount).filter(
            PixelAccount.operate_result == AdvertiserStatusResult.DEFAULT.value,
            PixelAccount.is_delete == False)
        for i in pixel_account:
            request_id = i.request_id
            update_account(request_id)
