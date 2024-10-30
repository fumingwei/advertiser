# -*- coding: utf-8 -*-
from munch import DefaultMunch
from apps.advertiser.define import RegisterStatus
from apps.advertiser.models import AdvertiserRegister, AdvertiserUser
from apps.advertiser.utils import JwtTokenUtil
from settings.db import SessionLocal
from tools.constant import RET


class AdvertiserUserVerify:
    """
    广告主账号验证
    """

    @staticmethod
    def auth(Authorization):
        # 校验请求头
        if not Authorization or not Authorization.startswith('Bearer '):
            return RET.SESSION_ERR, '未提供有效的身份令牌'
        token = Authorization.split(' ')[1]
        # 校验token
        payload = JwtTokenUtil.verify_jwt(token)
        if not payload:
            return RET.SESSION_ERR, '身份令牌已过期'

        advertiser_user_id = payload.get("user_id")
        with SessionLocal() as db:
            advertiser_user = db.query(AdvertiserUser).filter(
                AdvertiserUser.id == advertiser_user_id,
                AdvertiserUser.is_delete == False).first()  # 获取用户对象
            if not advertiser_user:
                return RET.USER_ERR, "用户不存在或已注销"
            # 判断是否是主账号，如果是子账号 主账号被禁用则无法登录
            if advertiser_user.is_active == 0:
                return RET.USER_ERR, "该账号已被禁用"
            # 组织注册 对象
            login_user_id = advertiser_user_id if not advertiser_user.p_id else advertiser_user.p_id
            com_register = db.query(AdvertiserRegister).filter(
                AdvertiserRegister.user_id == login_user_id,
                AdvertiserRegister.status == RegisterStatus.AGREE.value,
                AdvertiserRegister.is_delete == False).first()
            # 不存在注册表或者注册表中的状态不是已同意
            if not com_register:
                return RET.USER_ERR, "用户注册申请未通过或已被禁用"
            sub_user = db.query(
                AdvertiserUser.id
            ).filter(
                AdvertiserUser.p_id == com_register.user_id,
                AdvertiserUser.is_delete == False
            ).all()
            sub_user_ids = [i.id for i in sub_user]
            user_data = {
                "user_id": advertiser_user_id,
                "p_id": advertiser_user.p_id,
                'sub_user_ids': sub_user_ids,
                'real_name': advertiser_user.real_name,
                "avatar_url": advertiser_user.avatar_url
            }
            return RET.OK, DefaultMunch.fromDict(user_data)
