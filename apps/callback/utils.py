import os.path
from settings.db import SessionLocal
from settings.log import web_log
from datetime import datetime, timedelta
from apps.callback.define import MediumOperation
from typing import Dict
from apps.accounts.models import AccountRename, BcAccount, BcAccountDetail, BmAccount, BmAccountDetail
from apps.pixel.models import PixelAccountDetail, PixelAccount
from apps.callback.define import AdvertiserStatusResult
from settings.db import RedisClient
from settings.base import configs

# 中国时间
file_path = os.getcwd()


class CallBackApi:
    @staticmethod
    def rename_operate(data: Dict):
        """
        重命名数据更新
        """
        request_id = data.get('request_id')
        items = data.get('data')
        r = RedisClient().get_redis_client()
        with SessionLocal() as db:
            try:
                for item in items:
                    account_id = item.get('account_id')
                    medium = item.get('medium').lower()
                    account_name = r.hmget(f'account:{account_id.replace("-", "")}', 'account_name')[0]
                    res = db.query(AccountRename).filter(
                        AccountRename.request_id == request_id,
                        AccountRename.account_id == item.get('account_id')).first()
                    if account_name != item.get('after_account_name') and medium == 'tiktok':
                        operate_result = item.get('result')
                        if operate_result == '2':
                            pass
                        else:
                            now = datetime.now()
                            operate_time = datetime.strptime(res.operate_time, '%Y-%m-%d %H:%M:%S')
                            if now - operate_time < timedelta(seconds=int(configs.RENAME_EXPIRATION)):
                                res.after_account_name = item.get('after_account_name')
                                continue
                    res.operate_result = item.get('result')
                    res.after_account_name = item.get('after_account_name')
                    res.remark = item.get('remark')
            except Exception as e:
                web_log.log_error(f'重命名回调失败原因：{e}')
            else:
                db.commit()

    @staticmethod
    def bm_bind_unbind(data):
        """
        bm账户(绑定解绑)
        """
        request_id = data.get('request_id')
        items = data.get('data')
        with SessionLocal() as db:
            try:
                tb1 = db.query(BmAccount).filter(BmAccount.request_id == request_id).first()
                real_count, success_count = len(items), 0
                for item in items:
                    tb2 = db.query(
                        BmAccountDetail).filter(
                        BmAccountDetail.bm_account_id == tb1.id,
                        BmAccountDetail.account_id == item.get('account_id')).first()
                    tb2.operate_result = item.get('result')
                    tb2.operate_time = item.get('update_time')
                    if item.get('remark'):
                        try:
                            remark = eval(item.get('remark', {}))
                            tb2.remark = remark.get('error', {}).get('message', '第三方系统错误')
                        except Exception as e:
                            web_log.log_error(f'获取BM备注失败原因：{e}')
                            tb2.remark = '第三方系统错误'
                    else:
                        tb2.remark = ''
                    if item.get('result') == '1':
                        success_count += 1
                if success_count == real_count:
                    tb1.operate_result = AdvertiserStatusResult.ALL_SUCCEED.value
                elif success_count == 0:
                    tb1.operate_result = AdvertiserStatusResult.ALL_FAIL.value
                else:
                    tb1.operate_result = AdvertiserStatusResult.PART.value
            except Exception as e:
                web_log.log_error(f'bm回调失败原因：{e}')
            else:
                db.commit()

    @staticmethod
    def bc_bind_unbind(data):
        """
        bc账户(绑定解绑)
        """
        request_id = data.get('request_id')
        items = data.get('data')
        with SessionLocal() as db:
            try:
                tb1 = db.query(BcAccount).filter(BcAccount.request_id == request_id).first()
                real_count, success_count = len(items), 0
                for item in items:
                    tb2 = db.query(
                        BcAccountDetail).filter(
                        BcAccountDetail.tiktok_bc_account_id == tb1.id,
                        BcAccountDetail.account_id == item.get('advertiser_id')).first()
                    tb2.operate_result = item.get('result')
                    tb2.operate_time = item.get('update_time')
                    if item.get('remark'):
                        try:
                            remark = eval(item.get('remark', {}))
                            tb2.remark = remark.get('message', '第三方系统错误')
                        except Exception as e:
                            web_log.log_error(f'获取BC备注失败原因：{e}')
                            tb2.remark = '第三方系统错误'
                    else:
                        tb2.remark = ''
                    if item.get('result') == '1':
                        success_count += 1
                if success_count == real_count:
                    tb1.operate_result = AdvertiserStatusResult.ALL_SUCCEED.value
                elif success_count == 0:
                    tb1.operate_result = AdvertiserStatusResult.ALL_FAIL.value
                else:
                    tb1.operate_result = AdvertiserStatusResult.PART.value
            except Exception as e:
                web_log.log_error(f'bc回调失败原因：{e}')
            else:
                db.commit()

    @staticmethod
    def pixel_bind_unbind(data):
        """
        pixel账户(绑定解绑)
        """
        request_id = data.get('request_id')
        items = data.get('data')
        with SessionLocal() as db:
            try:
                tb1 = db.query(PixelAccount).filter(PixelAccount.request_id == request_id).first()
                real_count, success_count = len(items), 0
                for item in items:
                    tb2 = db.query(
                        PixelAccountDetail).filter(
                        PixelAccountDetail.pixel_account_id == tb1.id,
                        PixelAccountDetail.account_id == item.get('account_id')).first()
                    tb2.operate_result = item.get('result')
                    tb2.binding_time = item.get('update_time')
                    if item.get('remark'):
                        try:
                            remark = eval(item.get('remark', {}))
                            tb2.remark = remark.get('error', {}).get('message', '第三方系统错误')
                        except Exception as e:
                            web_log.log_error(f'获取Pixel备注失败原因：{e}')
                            tb2.remark = '第三方系统错误'
                    else:
                        tb2.remark = ''
                    if item.get('result') == '1':
                        success_count += 1
                if success_count == real_count:
                    tb1.operate_result = AdvertiserStatusResult.ALL_SUCCEED.value
                elif success_count == 0:
                    tb1.operate_result = AdvertiserStatusResult.ALL_FAIL.value
                else:
                    tb1.operate_result = AdvertiserStatusResult.PART.value
            except Exception as e:
                web_log.log_error(f'Pixel回调失败原因：{e}')
            else:
                db.commit()


function_dict = {
    # 重命名
    MediumOperation.AccountRename.value: CallBackApi.rename_operate,
    # bm账户(绑定/解绑)
    MediumOperation.MetaBmAccountBind.value: CallBackApi.bm_bind_unbind,
    MediumOperation.MetaBmAccountUnBind.value: CallBackApi.bm_bind_unbind,
    # pixel，账户，bm(绑定/解绑)
    MediumOperation.MetaPixelBind.value: CallBackApi.pixel_bind_unbind,
    MediumOperation.MetaPixelUnBind.value: CallBackApi.pixel_bind_unbind,
    # bc合作伙伴id(解绑/绑定)
    MediumOperation.TiktokAdvertiserPartnerBind.value: CallBackApi.bc_bind_unbind,
    MediumOperation.TiktokAdvertiserPartnerUnBind.value: CallBackApi.bc_bind_unbind
}
