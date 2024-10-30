# -*- coding: utf-8 -*-
import asyncio
import datetime
import json
import jwt
import re
import time
import pandas as pd
import threading
import random
import string
from fastapi import Depends, APIRouter, Query, UploadFile, File
from fastapi_utils.cbv import cbv
from starlette.requests import Request
from apps.advertiser.schemas import (
    RegisterSchema,
    AccreditSchema,
    LoginSchema,
    ChangeEmailSchema,
    ChangeMobileSchema,
    ChangePasswordSchema,
    ResetPasswordSchema,
    SmsLoginSchema,
    UserFeedbackSchema,
    AccountsInfoSchema,
    ProjectGroupSchemas,
    GroupAuthorizedSchema,
    GroupUnauthorizedSchema
)
from sqlalchemy.orm import Session
from sqlalchemy import text
from apps.advertiser.models import (
    AdvertiserUser,
    UserCusRelationship,
    AdvertiserRegister,
    UserFeedback,
    ProjectGroup,
    GroupMemberRelationship,
    GroupAccountRelationship
)
from tools.common import MyResponse, CommonQueryParams
from tools.constant import CodeType, RET, error_map, LoginDesc, LoginStatus, OperationType, batch_operation_type, \
    EnOperationType
from settings.db import get_db, get_redis_connection, MyPagination, engine
from werkzeug.security import generate_password_hash, check_password_hash
from settings.base import configs
from apps.advertiser.utils import (
    UserMethod,
    JwtTokenUtil,
    get_customer_ids,
    get_authorized_accounts,
    get_unauthorized_accounts,
    get_new_mediums,
    get_unauthorized_accounts_mediums, generate_new_dict
)
from sqlalchemy.sql import literal_column
from sqlalchemy import case, func, or_, desc
from apps.advertiser.define import RegisterStatus, MediumOperationType
from apps.advertiser.utils import get_device, add_login_record, verify_sms_code
from libs.internal.push_service import PushService
from settings.log import web_log

CustomerRouter = APIRouter(tags=["客户注册"])


@cbv(CustomerRouter)
class UserServer:
    request: Request

    @CustomerRouter.get("/sms_codes", description="发送短信验证码")
    async def send_sms_code(
            self,
            code_type: str = Query(...),
            mobile: str = Query(...),
            db: Session = Depends(get_db),
    ):
        area_mobile = mobile
        area_code = mobile.split(' ')[0]
        user_mobile = mobile.replace("+", "").replace(" ", "")
        if len(area_mobile) > 20:
            return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        if area_code == '+86':
            user_mobile = re.sub(r"86", "", user_mobile, 1)
            if not re.match(r'^1[3-9]\d{9}$', user_mobile):
                return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        register_ins = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.mobile == area_mobile,
                AdvertiserRegister.status != RegisterStatus.REFUSE.value,
                AdvertiserRegister.is_delete == False,
            )
            .first()
        )
        user_ins = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.mobile == area_mobile, AdvertiserUser.is_delete == False)
            .first()
        )
        # 当验证码类型为自助系统注册时
        if code_type == CodeType.ADVERTISER_REGISTER:
            # 如果注册用户存在并且状态不是已拒绝则返回手机号已存在
            if register_ins:
                return MyResponse(
                    code=RET.PHONE_EXISTED, msg=error_map[RET.PHONE_EXISTED]
                )
            # 如果用户已存在并且是激活状态则返回手机号已存在
            if user_ins:
                return MyResponse(
                    code=RET.PHONE_EXISTED, msg=error_map[RET.PHONE_EXISTED]
                )
        # 当验证码类型为授权子账号时
        if code_type == CodeType.AUTHORIZED_ACCOUNT:
            # 如果是子账号或者用户的状态不是已拒绝
            if register_ins:
                if register_ins.status == RegisterStatus.WAIT.value:
                    return MyResponse(
                        code=RET.PHONE_EXISTED, msg="该账号已提交注册申请，尚在等待通过，暂时无法被授权",
                        data={"flag": False}
                    )
                if register_ins.status == RegisterStatus.DISABLED.value:
                    return MyResponse(
                        code=RET.PHONE_EXISTED, msg="该账号已被禁用，无法获得授权", data={"flag": False}
                    )
                if register_ins.status == RegisterStatus.AGREE.value:
                    return MyResponse(
                        code=RET.PHONE_EXISTED, msg="该账号是主账号，不能被授权", data={"flag": False}
                    )
            if user_ins:
                return MyResponse(code=RET.PHONE_EXISTED, msg="该手机号已被授权")
        # 当验证码类型为忘记密码或者自助系统登录时
        if code_type in [CodeType.ADVERTISER_FORGET_PWD, CodeType.ADVERTISER_LOGIN]:
            # 如果用户不存在
            if not user_ins:
                return MyResponse(code=RET.USER_ERR, msg=error_map[RET.USER_ERR])
            # 如果用户之前是子账号并且被现在是被取消状态
            if user_ins.p_id and user_ins.is_active == False:
                return MyResponse(code=RET.USER_ERR, msg=error_map[RET.USER_ERR])
            code_type = CodeType.ADVERTISER_FORGET_PWD if code_type == CodeType.ADVERTISER_FORGET_PWD else CodeType.ADVERTISER_LOGIN
        # 调用发送验证码的推送服务
        push_code, push_res = PushService().send_verification_code(code_type, user_mobile, area_code,
                                                                   self.request.state.trace_id)
        return MyResponse(code=push_code, msg=push_res)

    @CustomerRouter.post("/registrations", description="注册")
    async def register(self, data: RegisterSchema, db: Session = Depends(get_db)):
        area_mobile = data.mobile
        area_code = data.mobile.split(' ')[0]
        user_mobile = data.mobile.replace("+", "").replace(" ", "")
        if len(area_mobile) > 20:
            return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        if area_code == '+86':
            user_mobile = re.sub(r"86", "", user_mobile, 1)
            if not re.match(r'^1[3-9]\d{9}$', user_mobile):
                return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        register_ins = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.mobile == area_mobile,
                AdvertiserRegister.status != RegisterStatus.REFUSE.value,
                AdvertiserRegister.is_delete == False,
            )
            .first()
        )
        # 如果注册用户存在并且注册用户的状态不是已拒绝
        if register_ins:
            return MyResponse(code=RET.PHONE_EXISTED, msg=error_map[RET.PHONE_EXISTED])
        verify_code, verify_msg = verify_sms_code(data.code_type, user_mobile, data.sms_code, delete=True)
        # 校验验证码
        if verify_code != RET.OK:
            return MyResponse(code=verify_code, msg=verify_msg)
        # 添加到用户注册表
        new_user = AdvertiserRegister(
            company_name=data.company_name,
            contact=data.contact,
            mobile=area_mobile,
            email=data.email,
        )
        db.add(new_user)
        db.commit()
        return MyResponse(code=RET.OK, msg="注册成功")

    @CustomerRouter.post("/login", description="密码登录")
    async def login(self, data: LoginSchema, req_dev=Depends(get_device), db: Session = Depends(get_db)):
        area_mobile = data.mobile
        area_code = data.mobile.split(' ')[0]
        user_mobile = data.mobile.replace("+", "").replace(" ", "")
        if len(area_mobile) > 20:
            return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        if area_code == '+86':
            user_mobile = re.sub(r"86", "", user_mobile, 1)
            if not re.match(r'^1[3-9]\d{9}$', user_mobile):
                return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        register_ins = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.mobile == area_mobile,
                AdvertiserRegister.is_delete == False,
            )
            .first()
        )
        register_info = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.mobile == area_mobile,
                AdvertiserRegister.is_delete == True,
            )
            .first()
        )
        user_ins = (
            db.query(AdvertiserUser)
            .filter(
                AdvertiserUser.mobile == area_mobile, AdvertiserUser.is_delete == False
            )
            .first()
        )
        # 判断用户是否存在
        if not user_ins:
            # 不存在注册表
            if not register_ins:
                t = threading.Thread(target=add_login_record,
                                     args=(None, area_mobile, LoginStatus.ERROR, LoginDesc.USERNOTEXIST, *req_dev))
                t.start()
                return MyResponse(code=RET.USER_ERR, msg="该账号未注册")
            if register_info:
                t = threading.Thread(target=add_login_record,
                                     args=(
                                         register_info.user_id, area_mobile, LoginStatus.ERROR, LoginDesc.USERNOTEXIST,
                                         *req_dev))
                t.start()
                return MyResponse(code=RET.DATA_ERR, msg="该账号已注销")
            t = threading.Thread(target=add_login_record,
                                 args=(
                                     register_ins.user_id, area_mobile, LoginStatus.ERROR, LoginDesc.NOTACTIVE,
                                     *req_dev))
            t.start()
            return MyResponse(code=RET.DATA_ERR, msg="该账号注册申请未通过")
        # 判断用户状态是否是禁用
        if user_ins.is_active == False:
            t = threading.Thread(target=add_login_record,
                                 args=(user_ins.id, area_mobile, LoginStatus.ERROR, LoginDesc.NOTACTIVE, *req_dev))
            t.start()
            return MyResponse(code=RET.USER_ERR, msg="该账号已被禁用")
        # 判断密码是否一致
        if not check_password_hash(user_ins.password, data.password):
            t = threading.Thread(target=add_login_record,
                                 args=(user_ins.id, area_mobile, LoginStatus.ERROR, LoginDesc.PASSWORDERROR, *req_dev))
            t.start()
            return MyResponse(code=RET.DATA_ERR, msg="密码不正确")
        payload = {"user_id": user_ins.id}
        token = JwtTokenUtil.generate_jwt(payload)
        t = threading.Thread(target=add_login_record,
                             args=(user_ins.id, area_mobile, LoginStatus.SUCCESS, LoginDesc.LOGINSUCCESS, *req_dev))
        t.start()
        return MyResponse(code=RET.OK, msg="登录成功", data={"token": token})

    @CustomerRouter.post("/forget_password", description="忘记密码")
    async def forget_password(
            self, data: ResetPasswordSchema, db: Session = Depends(get_db)
    ):
        area_mobile = data.mobile
        area_code = data.mobile.split(' ')[0]
        user_mobile = data.mobile.replace("+", "").replace(" ", "")
        if len(area_mobile) > 20:
            return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        if area_code == '+86':
            user_mobile = re.sub(r"86", "", user_mobile, 1)
            if not re.match(r'^1[3-9]\d{9}$', user_mobile):
                return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        register_ins = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.mobile == area_mobile,
                AdvertiserRegister.status == RegisterStatus.AGREE.value,
                AdvertiserRegister.is_delete == False,
            )
            .first()
        )
        user_ins = (
            db.query(AdvertiserUser)
            .filter(
                AdvertiserUser.mobile == area_mobile, AdvertiserUser.is_delete == False
            )
            .first()
        )
        # 如果用户不存在或者用户是禁用状态
        if not user_ins or not user_ins.is_active:
            return MyResponse(code=RET.USER_ERR, msg="用户不存在或已禁用")
        # res = verify_sms_code(data.code_type, data.mobile, data.sms_code)
        # if res.code != RET.OK:
        #     return MyResponse(code=res.code, msg=error_map[res.code])
        verify_code, verify_msg = verify_sms_code(data.code_type, user_mobile, data.sms_code, delete=True)
        # 校验验证码
        if verify_code != RET.OK:
            return MyResponse(code=verify_code, msg=verify_msg)
        if register_ins:
            register_ins.password = data.password1
        user_ins.password = generate_password_hash(data.password1)
        db.commit()
        return MyResponse(code=RET.OK, msg="重置密码成功")

    @CustomerRouter.post("/sms_login", description="验证码登录")
    async def sms_login(self, data: SmsLoginSchema, req_dev=Depends(get_device), db: Session = Depends(get_db)):
        area_mobile = data.mobile
        area_code = data.mobile.split(' ')[0]
        user_mobile = data.mobile.replace("+", "").replace(" ", "")
        if len(area_mobile) > 20:
            return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        if area_code == '+86':
            user_mobile = re.sub(r"86", "", user_mobile, 1)
            if not re.match(r'^1[3-9]\d{9}$', user_mobile):
                return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        verify_code, verify_msg = verify_sms_code(data.code_type, user_mobile, data.sms_code, delete=True)
        # 校验验证码
        if verify_code != RET.OK:
            return MyResponse(code=verify_code, msg=verify_msg)
        register_ins = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.mobile == area_mobile,
                AdvertiserRegister.is_delete == False,
            )
            .first()
        )
        register_info = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.mobile == area_mobile,
                AdvertiserRegister.is_delete == True,
            )
            .first()
        )
        user_ins = (
            db.query(AdvertiserUser)
            .filter(
                AdvertiserUser.mobile == area_mobile, AdvertiserUser.is_delete == False
            )
            .first()
        )
        # 如果用户不存在
        if not user_ins:
            # 不存在注册表
            if not register_ins:
                t = threading.Thread(target=add_login_record,
                                     args=(None, area_mobile, LoginStatus.ERROR, LoginDesc.USERNOTEXIST, *req_dev))
                t.start()
                return MyResponse(code=RET.DATA_ERR, msg="该账号未注册")
            if register_info:
                t = threading.Thread(target=add_login_record,
                                     args=(register_info.user_id, area_mobile, LoginDesc.USERNOTEXIST, *req_dev))
                t.start()
                return MyResponse(code=RET.DATA_ERR, msg="该账号已注销")
            t = threading.Thread(target=add_login_record,
                                 args=(
                                     register_info.user_id, area_mobile, LoginStatus.ERROR, LoginDesc.NOTACTIVE,
                                     *req_dev))
            t.start()
            return MyResponse(code=RET.DATA_ERR, msg="该账号注册申请未通过或已被禁用")
        # 如果用户状态是禁用状态
        if user_ins.is_active == False:
            t = threading.Thread(target=add_login_record,
                                 args=(user_ins.id, area_mobile, LoginStatus.ERROR, LoginDesc.NOTACTIVE, *req_dev))
            t.start()
            return MyResponse(code=RET.USER_ERR, msg="该账号已被禁用")
        header = {"alg": "HS256", "typ": "JWT"}
        exp = time.time() + configs.ACCESS_TOKEN_EXPIRE  # 过期时间为8小时后
        secret = configs.SECRET_KEY  # 用于签名和验证的密钥
        payload = {"user_id": user_ins.id, "exp": exp}  # 要包含在JWT中的负载数据
        token = jwt.encode(payload, secret, algorithm="HS256", headers=header)
        t = threading.Thread(target=add_login_record,
                             args=(user_ins.id, area_mobile, LoginStatus.SUCCESS, LoginDesc.LOGINSUCCESS, *req_dev))
        t.start()
        return MyResponse(code=RET.OK, msg="登录成功", data={"token": token})


@cbv(CustomerRouter)
class AccreditServer:
    request: Request

    @CustomerRouter.get("/verify", description="校验是否可以授权")
    async def verify(self, mobile: str = Query(...), db: Session = Depends(get_db)):
        userid = self.request.state.user.user_id
        area_mobile = mobile
        area_code = mobile.split(' ')[0]
        user_mobile = mobile.replace("+", "").replace(" ", "")
        if len(area_mobile) > 20:
            return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        if area_code == '+86':
            user_mobile = re.sub(r"86", "", user_mobile, 1)
            if not re.match(r'^1[3-9]\d{9}$', user_mobile):
                return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == userid).first()
        # 如果现在登录的账号是子账号
        if user.p_id:
            return MyResponse(
                code=RET.DATA_ERR,
                msg="该账号是子账号没有授权功能",
                data={"flag": False},
            )
        register_ins = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.mobile == area_mobile,
                AdvertiserRegister.status != RegisterStatus.REFUSE.value,
                AdvertiserRegister.is_delete == False,
            )
            .first()
        )
        user_ins = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.mobile == area_mobile, AdvertiserUser.is_delete == False)
            .first()
        )
        # 如果注册用户存在并且注册用户的状态不是已拒绝
        if register_ins:
            if register_ins.status == RegisterStatus.WAIT.value:
                return MyResponse(
                    code=RET.PHONE_EXISTED, msg="该账号已提交注册申请，尚在等待通过，暂时无法被授权", data={"flag": False}
                )
            if register_ins.status == RegisterStatus.DISABLED.value:
                return MyResponse(
                    code=RET.PHONE_EXISTED, msg="该账号已被禁用，无法获得授权", data={"flag": False}
                )
        # 如果授权的手机号满足子账号条件
        if (
                user_ins
                and user_ins.p_id
                and user_ins.is_delete == False
                and user_ins.is_active == True
        ):
            return MyResponse(
                code=RET.PHONE_EXISTED,
                msg="该账号已授权其他主账号",
                data={"flag": False},
            )
        # 获取用户的子账号数量
        count = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.p_id == userid, AdvertiserUser.is_delete == False)
            .count()
        )
        # 获取用户最大可以授权子账号的数量
        max_count = (
            db.query(UserCusRelationship.auth_num)
            .filter(UserCusRelationship.company_id == user.company_id)
            .scalar()
        )
        # 如果可授权子账号总数不大于现在的子账号数量时
        if max_count <= count:
            return MyResponse(
                code=RET.DATA_ERR,
                msg="该账号授权子账号数量已达上限",
                data={"flag": False},
            )
        return MyResponse(code=RET.OK, msg="可以进行授权", data={"flag": True})

    @CustomerRouter.post("/accredit", description="授权子账号")
    async def accredit(self, data: AccreditSchema, db: Session = Depends(get_db)):
        userid = self.request.state.user.user_id
        area_mobile = data.mobile
        area_code = data.mobile.split(' ')[0]
        user_mobile = data.mobile.replace("+", "").replace(" ", "")
        if len(area_mobile) > 20:
            return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        if area_code == '+86':
            user_mobile = re.sub(r"86", "", user_mobile, 1)
            if not re.match(r'^1[3-9]\d{9}$', user_mobile):
                return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        init_password = "123456"
        hash_pwd = generate_password_hash(init_password)
        user_ins = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        if not user_ins:
            return MyResponse(code=RET.USER_ERR, msg=error_map[RET.USER_ERR])
        if user_ins.p_id:
            return MyResponse(code=RET.DATA_ERR, msg="该账号是子账号没有授权功能")
        verify_code, verify_msg = verify_sms_code(data.code_type, user_mobile, data.sms_code, delete=True)
        # 校验验证码
        if verify_code != RET.OK:
            return MyResponse(code=verify_code, msg=verify_msg)
        register_ins = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.mobile == area_mobile,
                AdvertiserRegister.status != RegisterStatus.REFUSE.value,
                AdvertiserRegister.is_delete == False,
            )
            .first()
        )
        user = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.mobile == area_mobile)
            .first()
        )
        # 如果注册用户存在并且注册用户的状态不是已拒绝
        if register_ins:
            if register_ins.status == RegisterStatus.WAIT.value:
                return MyResponse(
                    code=RET.PHONE_EXISTED, msg="该账号已提交注册申请，尚在等待通过，暂时无法被授权", data={"flag": False}
                )
            if register_ins.status == RegisterStatus.DISABLED.value:
                return MyResponse(
                    code=RET.PHONE_EXISTED, msg="该账号已被禁用，无法获得授权", data={"flag": False}
                )
        if user:
            # 如果是子账号
            if (
                    user
                    and user.p_id
                    and user.is_delete == False
                    and user.is_active == True
            ):
                return MyResponse(code=RET.PHONE_EXISTED, msg="该账号已授权其他主账号")
            user.real_name = data.username
            user.is_active = True
            user.p_id = userid
            user.is_delete = False
            user.company_id = user_ins.company_id
            user.password = hash_pwd
            user.email = None
            db.commit()
        if not user:
            new_user = AdvertiserUser(
                mobile=area_mobile,
                real_name=data.username,
                password=hash_pwd,
                p_id=userid,
                company_id=user_ins.company_id,
            )
            db.add(new_user)
            db.commit()
        return MyResponse(
            code=RET.OK, msg="用户授权子账号成功", data={"password": "123456"}
        )

    @CustomerRouter.post("/cancel", description="取消授权子账号")
    async def cancel(self, mobile: str = Query(...), db: Session = Depends(get_db)):
        userid = self.request.state.user.user_id
        user_ins = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.mobile == mobile, AdvertiserUser.is_delete == False)
            .first()
        )
        user = db.query(AdvertiserUser).filter(AdvertiserUser.id == userid).first()
        group_member = db.query(GroupMemberRelationship).filter(
            GroupMemberRelationship.is_delete == False,
            GroupMemberRelationship.user_id == user_ins.id
        ).all()
        try:
            # 如果用户存在并且是子账号
            if user and user.p_id:
                return MyResponse(code=RET.PER_ERR, msg="该用户没有权限取消授权")
            if not user_ins:
                return MyResponse(code=RET.DATA_ERR, msg="该用户没有被授权")
            user_ins.is_active = False
            user_ins.is_delete = True
            user_ins.p_id = None
            user_ins.company_id = None
            user_ins.avatar_url = None
            for member in group_member:
                # 删除项目组成员关系表的数据
                member.is_delete = True
            db.commit()
        except Exception as e:
            web_log.log_error(
                f"取消授权子账号失败原因：{e}"
            )
        return MyResponse(code=RET.OK, msg="用户取消授权子账号成功")

    @CustomerRouter.get("/accredit_list", description="查看授权子账号列表")
    async def accredit_list(self, db: Session = Depends(get_db)):
        userid = self.request.state.user.user_id
        if (
                db.query(AdvertiserUser)
                        .filter(AdvertiserUser.id == userid)
                        .filter(AdvertiserUser.p_id != None)
                        .first()
        ):
            return MyResponse(code=RET.OK, msg="该账号是子账号没有权限查看授权列表")
        user = (
            db.query(
                AdvertiserUser.mobile,
                AdvertiserUser.real_name,
                AdvertiserUser.avatar_url,
                AdvertiserUser.id,
            )
            .filter(AdvertiserUser.p_id == userid)
            .order_by(AdvertiserUser.id.desc())
            .all()
        )
        users_list = [
            dict(zip(["mobile", "real_name", "avatar_url", "id"], user)) for user in user
        ]
        return MyResponse(code=RET.OK, msg="展示授权子账号列表成功", data=users_list)

    @CustomerRouter.post("/accounts", description="授权账户")
    async def accounts(self, data: AccountsInfoSchema):
        accredit_redis = get_redis_connection("accredit_account")  # 授权账户
        account_redis = get_redis_connection("medium_account")  # 账户信息
        accredit_pipe = accredit_redis.pipeline()
        account_pipe = account_redis.pipeline()
        accredit_action = accredit_pipe.sadd if data.accredit else accredit_pipe.srem
        try:
            for i in data.account_id:
                accredit_action(f'ad_sub_accounts:{data.son_id}', i)
                # 获取这个账户授权给哪些子账号
                son_value = account_redis.hget(f"account:{i.replace('-', '')}", "ad_subs")
                current_list = []
                # 如果该账户存在授权
                if son_value:
                    # 现在存储的是之前已经授权的子账号id
                    current_list = json.loads(son_value)
                # 如果是授权的话
                if data.accredit:
                    # 将现在要授权的子账号id添加到列表中
                    if data.son_id not in current_list:
                        current_list.append(data.son_id)
                        account_pipe.hset(f"account:{i.replace('-', '')}", "ad_subs", json.dumps(current_list))
                    continue
                if data.son_id in current_list:
                    current_list.remove(data.son_id)
                    account_pipe.hset(f"account:{i.replace('-', '')}", "ad_subs", json.dumps(current_list))
                if len(current_list) == 0:
                    account_pipe.hdel(f"account:{i.replace('-', '')}", "ad_subs")
            accredit_pipe.execute()
            account_pipe.execute()
        except Exception as e:
            web_log.log_error(
                f"授权账户储存Redis失败原因：{e}"
            )
        return MyResponse()


@cbv(CustomerRouter)
class PersonalServer:
    request: Request

    @CustomerRouter.put("/change_mobile", description="更改手机号")
    async def change_mobile(
            self, data: ChangeMobileSchema, db: Session = Depends(get_db)
    ):
        area_mobile = data.mobile
        area_code = data.mobile.split(' ')[0]
        user_mobile = data.mobile.replace("+", "").replace(" ", "")
        if len(area_mobile) > 20:
            return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        if area_code == '+86':
            user_mobile = re.sub(r"86", "", user_mobile, 1)
            if not re.match(r'^1[3-9]\d{9}$', user_mobile):
                return MyResponse(code=RET.DATA_ERR, msg="手机号格式不正确，请重新输入")
        userid = self.request.state.user.user_id
        register_user = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.user_id == userid,
                AdvertiserRegister.is_delete == False,
            )
            .first()
        )
        user_ins = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        # 如果用户不存在
        if not user_ins:
            return MyResponse(code=RET.USER_ERR, msg=error_map[RET.USER_ERR])
        user = (
            db.query(AdvertiserUser)
            .filter(
                AdvertiserUser.mobile == area_mobile
            )
            .first()
        )
        register_ins = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.mobile == area_mobile,
                AdvertiserRegister.status != RegisterStatus.REFUSE.value,
                AdvertiserRegister.is_delete == False,
            )
            .first()
        )

        if user:
            # 如果填写的手机号在用户表存在或者在注册表存在且不是已拒绝状态
            if not user.is_delete or register_ins:
                return MyResponse(code=RET.PHONE_EXISTED, msg=error_map[RET.PHONE_EXISTED])
            if user.is_delete:
                # 随机生成两位大小写字母
                random_letters = ''.join(random.choice(string.ascii_letters) for _ in range(2))
                user.mobile = user.mobile + f"-{random_letters}"
                db.commit()
        verify_code, verify_msg = verify_sms_code(data.code_type, user_mobile, data.sms_code, delete=True)
        # 校验验证码
        if verify_code != RET.OK:
            return MyResponse(code=verify_code, msg=verify_msg)
        if register_user:
            register_user.mobile = area_mobile
        user_ins.mobile = area_mobile
        db.commit()
        return MyResponse(code=RET.OK, msg="更改手机号成功")

    @CustomerRouter.put("/change_email", description="更改邮箱")
    async def change_email(
            self, data: ChangeEmailSchema, db: Session = Depends(get_db)
    ):
        userid = self.request.state.user.user_id
        register_ins = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.user_id == userid,
                AdvertiserRegister.is_delete == False,
            )
            .first()
        )
        user_ins = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        if not user_ins:
            return MyResponse(code=RET.USER_ERR, msg=error_map[RET.USER_ERR])
        if register_ins:
            register_ins.email = data.email
        user_ins.email = data.email
        db.commit()
        return MyResponse(code=RET.OK, msg="更改邮箱成功")

    @CustomerRouter.put("/change_password", description="更改密码")
    async def change_password(
            self, data: ChangePasswordSchema, db: Session = Depends(get_db)
    ):
        userid = self.request.state.user.user_id
        register_ins = (
            db.query(AdvertiserRegister)
            .filter(
                AdvertiserRegister.user_id == userid,
                AdvertiserRegister.is_delete == False,
            )
            .first()
        )
        user_ins = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        if not user_ins:
            return MyResponse(code=RET.USER_ERR, msg=error_map[RET.USER_ERR])
        if not check_password_hash(user_ins.password, data.old_password):
            return MyResponse(code=RET.DATA_ERR, msg="原密码不正确")
        if data.old_password == data.password1:
            return MyResponse(code=RET.DATA_ERR, msg="新密码与旧密码不能相同")
        if register_ins:
            register_ins.password = data.password1
        user_ins.password = generate_password_hash(data.password1)
        db.commit()
        return MyResponse(code=RET.OK, msg="更改密码成功")

    @CustomerRouter.get("/detail", description="登录信息")
    async def get_detail(self, db: Session = Depends(get_db)):
        userid = self.request.state.user.user_id
        # 子账户是二代
        # userid = 263
        if userid:
            user = (
                db.query(AdvertiserUser)
                .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
                .first()
            )
            # 如果是子账号
            if user.p_id:
                user1 = (
                    db.query(AdvertiserRegister)
                    .filter(
                        AdvertiserRegister.user_id == user.p_id,
                        AdvertiserRegister.is_delete == False,
                    )
                    .first()
                )
                personal_data = (
                    db.query(
                        literal_column(f"'{user1.company_name}'").label("company_name"),
                        case([(user1.is_second == 1, False)],
                             else_=False).label("is_second"),
                        AdvertiserUser.real_name.label("real_name"),
                        AdvertiserUser.id.label("user_id"),
                        AdvertiserUser.mobile,
                        AdvertiserUser.email,
                        AdvertiserUser.avatar_url,
                        AdvertiserUser.is_open
                    )
                    .filter(
                        AdvertiserUser.id == userid, AdvertiserUser.is_delete == False
                    )
                    .outerjoin(
                        AdvertiserRegister,
                        AdvertiserRegister.mobile == AdvertiserUser.mobile,
                    )
                )
            # 如果是主账号
            if not user.p_id:
                personal_data = (
                    db.query(
                        AdvertiserRegister.company_name.label("company_name"),
                        case([(AdvertiserRegister.is_second == 1, True)],
                             else_=False).label("is_second"),
                        AdvertiserUser.real_name.label("real_name"),
                        AdvertiserUser.id.label("user_id"),
                        AdvertiserUser.mobile,
                        AdvertiserUser.email,
                        AdvertiserUser.avatar_url,
                        AdvertiserUser.is_open
                    )
                    .filter(
                        AdvertiserUser.id == userid,
                        AdvertiserUser.is_delete == False,
                        AdvertiserRegister.status == RegisterStatus.AGREE.value,
                        AdvertiserRegister.is_delete == False,
                    )
                    .outerjoin(
                        AdvertiserRegister,
                        AdvertiserRegister.mobile == AdvertiserUser.mobile,
                    )
                )
            users_list = [
                dict(zip(["company_name", "is_second", "real_name", "user_id", "mobile",
                          "email", "avatar_url", "is_open"], user))
                for user in personal_data
            ]
            return MyResponse(data=users_list[0])
        return MyResponse(code=RET.USER_ERR, msg=error_map[RET.USER_ERR])

    @CustomerRouter.post("/change_avatar_url", description="更改头像")
    async def change_avatar_url(
            self, file: UploadFile = File(...), db: Session = Depends(get_db)
    ):
        userid = self.request.state.user.user_id
        file_types = ['jpg', 'jpeg', 'png']
        file_type = file.filename.split(".")[-1]
        if file_type not in file_types or file.content_type != 'image/jpeg':
            return MyResponse(code=RET.DATA_ERR, msg=f'图片类型仅支持{file_types}')
        if file.size >= 1024 * 1024 * 10:
            return MyResponse(code=RET.DATA_ERR, msg='文件大小请保持10mb以下')
        # 读取文件内容
        file_content = await file.read()
        new_avatar_url = UserMethod.update_avatar(
            self.request.state.user.avatar_url,
            file_content,
            file_type,
        )
        user_ins = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        if not user_ins:
            return MyResponse(code=RET.USER_ERR, msg=error_map[RET.USER_ERR])
        user_ins.avatar_url = new_avatar_url
        db.commit()
        return MyResponse(msg="头像更换成功", data={"avatar_url": new_avatar_url})

    # 用户反馈
    @CustomerRouter.post("/user_feedback", description="用户反馈")
    async def user_feedback(
            self, data: UserFeedbackSchema, db: Session = Depends(get_db)
    ):
        userid = self.request.state.user.user_id
        content = data.content
        user_ins = (
            db.query(AdvertiserUser)
            .filter(AdvertiserUser.id == userid, AdvertiserUser.is_delete == False)
            .first()
        )
        if not user_ins:
            return MyResponse(code=RET.USER_ERR, msg=error_map[RET.USER_ERR])
        new_feedback = UserFeedback(
            user_id=userid,
            content=content
        )
        db.add(new_feedback)
        db.commit()
        return MyResponse(msg="我们已经收到您的反馈意见")

    @CustomerRouter.get("/second_user", description="主账户二代客户用户标识")
    async def get_user_second(self, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        # 查询子账账户
        user = db.query(AdvertiserUser.p_id
                        ).filter(AdvertiserUser.id == user_id,
                                 AdvertiserUser.is_delete.is_(False)
                                 ).scalar()
        if user is not None:
            is_second = False
        else:
            # 查询主账户
            res_user = db.query(AdvertiserRegister.is_second
                                ).filter(AdvertiserRegister.user_id == user_id,
                                         AdvertiserRegister.is_delete.is_(False)
                                         ).scalar()
            is_second = res_user if res_user is not None else False
        return MyResponse(data={"is_second": is_second})

    @CustomerRouter.put("/close_dialog", description="关闭查看更新日志弹框")
    async def close_dialog(self, db: Session = Depends(get_db)):
        userid = self.request.state.user.user_id
        db.query(AdvertiserUser).filter(
            AdvertiserUser.id == userid, AdvertiserUser.is_delete == False
        ).update({"is_open": False})
        db.commit()
        return MyResponse()


@cbv(CustomerRouter)
class ProjectGroupServer:
    request: Request

    def check_second(self, db):
        company_id = db.query(AdvertiserRegister.id).filter(
            AdvertiserRegister.is_delete == False,
            AdvertiserRegister.status == RegisterStatus.AGREE.value,
            AdvertiserRegister.user_id == self.request.state.user.user_id,
            AdvertiserRegister.is_second == True
        ).scalar()
        return company_id

    @CustomerRouter.get("/mediums_select", description="选择投放媒介")
    async def medium_select(self, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        company_id = db.query(AdvertiserRegister.id).filter(
            AdvertiserRegister.is_delete == False,
            AdvertiserRegister.status == RegisterStatus.AGREE.value,
            AdvertiserRegister.user_id == user_id
        ).scalar()
        # 该公司创建的所有项目组的媒介
        mediums_query = db.query(
            func.group_concat(ProjectGroup.mediums)
        ).filter(ProjectGroup.company_id == company_id, ProjectGroup.is_delete == False).scalar()
        mediums_data = []
        if mediums_query:
            for i in json.loads(f"[{mediums_query}]"):
                mediums_data.extend(i)
        mediums_list = [{"value": j, "label": j} for j in list(set(mediums_data)) if j]
        return MyResponse(data=mediums_list)

    @CustomerRouter.get("/operation", description="操作类型")
    async def get_operation(self, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        if user_id:
            single_operation = ["账户充值", "账户清零", "账户转账", "账户重命名", "BM绑定/解绑", "BC绑定/解绑",
                                "Pixel绑定/解绑"]
            user = (
                db.query(AdvertiserUser)
                .filter(AdvertiserUser.id == user_id, AdvertiserUser.is_delete == False)
                .first()
            )
            # 如果是子账号
            if user.p_id:
                user_p = db.query(AdvertiserRegister.is_second).filter(
                    AdvertiserRegister.user_id == user.p_id,
                    AdvertiserRegister.is_delete == False,
                ).first()
                # 如果是二代客户
                if user_p.is_second:
                    # 获取组id
                    group_member = db.query(GroupMemberRelationship.project_group_id
                                            ).filter(GroupMemberRelationship.user_id == user_id,
                                                     GroupMemberRelationship.is_delete == 0).all()
                    group_ids = [group_id for (group_id,) in group_member]
                    # 取操作类型
                    groups = db.query(ProjectGroup.operation_type).filter(ProjectGroup.id.in_(group_ids),
                                                                          ProjectGroup.is_delete == 0).all()
                    # 提取操作类型并去重
                    operation_type_list = {op_type for group in groups for op_type in group["operation_type"] if
                                           isinstance(op_type, str)}
                    single_operation = [i for i in operation_type_list]
            batch_operation = [batch_operation_type.get(i) for i in single_operation]
            all_data = {"batch_operation": batch_operation, "single_operation": single_operation}

            # 根据不同媒介按照固定顺序生成新字典
            meta = generate_new_dict(MediumOperationType.Meta, all_data)
            google = generate_new_dict(MediumOperationType.Google, all_data)
            tiktok = generate_new_dict(MediumOperationType.Tiktok, all_data)

            data = {
                "Meta": meta,
                "Google": google,
                "Tiktok": tiktok,
            }
            return MyResponse(data=data)
        return MyResponse(code=RET.USER_ERR, msg=error_map[RET.USER_ERR])

    @CustomerRouter.get("/project_groups", description="项目组列表")
    async def project_group_list(
            self,
            medium: str = Query(None),
            start_date: str = Query(None, regex='^[\d]{4}-[\d]{2}-[\d]{2}$'),
            end_date: str = Query(None, regex='^[\d]{4}-[\d]{2}-[\d]{2}$'),
            common_query: CommonQueryParams = Depends(),
            db: Session = Depends(get_db)
    ):
        user_id = self.request.state.user.user_id
        company_id = db.query(AdvertiserRegister.id).filter(
            AdvertiserRegister.is_delete == False,
            AdvertiserRegister.status == RegisterStatus.AGREE.value,
            AdvertiserRegister.user_id == user_id
        ).scalar()
        where = [ProjectGroup.company_id == company_id, ProjectGroup.is_delete == False]
        if common_query.q:
            # 模糊搜索能搜索项目组名称、组成员、功能模块和备注
            project_groups = db.query(GroupMemberRelationship.project_group_id).join(
                AdvertiserUser, AdvertiserUser.id == GroupMemberRelationship.user_id
            ).filter(
                or_(
                    AdvertiserUser.real_name.ilike(f'%{common_query.q}%'),
                    AdvertiserUser.mobile.like(f'%{common_query.q}%')
                )
            ).all()
            project_group_ids = [i[0] for i in project_groups]
            where.append(
                or_(
                    ProjectGroup.project_name.ilike(f"%{common_query.q}%"),
                    ProjectGroup.id.in_(project_group_ids),
                    ProjectGroup.operation_type.contains(f"%{common_query.q}%"),
                    ProjectGroup.remark.ilike(f"%{common_query.q}%")
                )
            )
        if medium:
            where.append(ProjectGroup.mediums.contains(medium))
        if start_date and end_date:
            where.extend([
                func.date(ProjectGroup.created_time) >= func.date(start_date),
                func.date(ProjectGroup.created_time) <= func.date(end_date)])
        query = db.query(
            ProjectGroup.id,
            ProjectGroup.project_name,
            ProjectGroup.created_time,
            # 操作类型以字符串形式返回
            func.regexp_replace(ProjectGroup.operation_type, '["\\[\\]]', '').label('operation_type'),
            case(
                [
                    (ProjectGroup.remark != '', ProjectGroup.remark)
                ],
                else_='-'
            ).label('remark'),
            # 投放媒介以字符串形式返回
            case(
                [
                    (func.json_length(ProjectGroup.mediums) > 0,
                     func.regexp_replace(ProjectGroup.mediums, '["\\[\\]]', ''))
                ],
                else_="-"
            ).label('mediums')
        ).filter(*where).group_by(ProjectGroup.id).order_by(desc(ProjectGroup.created_time))
        project_groups = MyPagination(query, common_query.page, common_query.page_size)
        return MyResponse(total=project_groups.counts, data=project_groups.data)

    @CustomerRouter.get("/group_members/{project_group_id}", description="组成员列表")
    async def group_member_list(
            self,
            project_group_id: int,
            common_query: CommonQueryParams = Depends(),
            db: Session = Depends(get_db)
    ):
        where = [GroupMemberRelationship.is_delete == False,
                 GroupMemberRelationship.project_group_id == project_group_id]
        if common_query.q:
            where.append(
                or_(
                    AdvertiserUser.real_name.ilike(f"%{common_query.q}%"),
                    AdvertiserUser.mobile.ilike(f"%{common_query.q}%")
                )
            )
        query = db.query(AdvertiserUser.id, AdvertiserUser.mobile, AdvertiserUser.real_name,
                         AdvertiserUser.avatar_url).join(
            GroupMemberRelationship, AdvertiserUser.id == GroupMemberRelationship.user_id
        ).filter(*where).order_by(desc(GroupMemberRelationship.created_time))
        group_members = MyPagination(query, common_query.page, common_query.page_size)
        return MyResponse(total=group_members.counts, data=group_members.data)

    @CustomerRouter.get("/group_members_select", description="选择组成员")
    async def group_member_select(self, db: Session = Depends(get_db)):
        user_id = self.request.state.user.user_id
        query = db.query(AdvertiserUser).filter(
            AdvertiserUser.is_delete == False,
            AdvertiserUser.p_id == user_id
        ).all()
        group_members = [
            {"id": item.id, "name": f"{item.real_name} {item.mobile}"}
            for item in query
        ]
        return MyResponse(data=group_members)

    @CustomerRouter.get("/operation_type_select", description="选择操作类型")
    async def operation_type_select(self):
        operation_type = [getattr(EnOperationType, attr) for attr in OperationType.__dict__ if
                          not attr.startswith('__')]
        operation_type_list = [
            {"label": value[0], "value": value[0], "en_label": value[1]}
            for value in operation_type
        ]
        return MyResponse(data=operation_type_list)

    @CustomerRouter.post("/project_groups", description="新建项目组")
    async def add_project_group(
            self, data: ProjectGroupSchemas, db: Session = Depends(get_db)
    ):
        company_id = self.check_second(db)
        project_group = db.query(ProjectGroup).filter(
            ProjectGroup.is_delete == False,
            ProjectGroup.company_id == company_id,
            ProjectGroup.project_name == data.name
        ).all()
        if not company_id:
            return MyResponse(code=RET.NO_DATA, msg="您非二代客户，暂无二代工具操作权限")
        if project_group:
            return MyResponse(code=RET.DATA_EXIST, msg="该项目组已存在")
        try:
            # 添加到项目组表
            new_project_group = ProjectGroup(
                project_name=data.name,
                operation_type=data.operation_type,
                remark=data.remark,
                company_id=company_id
            )
            db.add(new_project_group)
            db.flush()
            if data.user_ids:
                new_group_member = [
                    GroupMemberRelationship(project_group_id=new_project_group.id, user_id=user_id)
                    for user_id in data.user_ids
                ]
                db.add_all(new_group_member)
        except Exception as e:
            db.rollback()
            web_log.log_error(f"创建项目组失败原因：{e}")
            return MyResponse(code=RET.DATA_ERR, msg="创建失败")
        else:
            db.commit()
        return MyResponse(msg="创建成功")

    @CustomerRouter.get("/project_groups/{project_group_id}", description="项目组详情")
    async def project_group_detail(self, project_group_id: int, db: Session = Depends(get_db)):
        # 建立主查询
        base_subquery = db.query(
            ProjectGroup.id,
            ProjectGroup.project_name,
            ProjectGroup.operation_type,
            ProjectGroup.remark
        ).filter(ProjectGroup.id == project_group_id, ProjectGroup.is_delete == False).subquery()
        # 建立子查询
        subquery = db.query(
            GroupMemberRelationship,
            AdvertiserUser.real_name,
            AdvertiserUser.mobile
        ).outerjoin(
            AdvertiserUser, AdvertiserUser.id == GroupMemberRelationship.user_id
        ).filter(GroupMemberRelationship.is_delete == False).subquery()
        # 查询项目组信息和组成员信息
        query = db.query(
            base_subquery.c.id,
            base_subquery.c.project_name,
            base_subquery.c.operation_type,
            base_subquery.c.remark,
            func.json_arrayagg(
                case(
                    [
                        (subquery.c.project_group_id.isnot(None),
                         func.json_object(
                             'name', func.concat(subquery.c.real_name, " ", subquery.c.mobile),
                             'id', subquery.c.user_id
                         ))
                    ],
                    else_=None
                )
            ).label('group_members')
        ).outerjoin(
            subquery, subquery.c.project_group_id == base_subquery.c.id
        ).group_by(base_subquery.c.id).first()
        data = dict()
        if query:
            data = {
                "id": query.id,
                "project_name": query.project_name,
                "group_members": json.loads(query.group_members) if json.loads(query.group_members)[0] != None else [],
                "operation_type": [{'label': i, 'value': i} for i in query.operation_type],
                "remark": query.remark
            }
        return MyResponse(data=data)

    @CustomerRouter.put("/project_groups/{project_group_id}", description="编辑项目组")
    async def update_project_group(self, project_group_id: int, data: ProjectGroupSchemas,
                                   db: Session = Depends(get_db)):
        company_id = self.check_second(db)
        project_group = db.query(ProjectGroup).filter(
            ProjectGroup.is_delete == False,
            ProjectGroup.company_id == company_id,
            ProjectGroup.project_name == data.name,
            ProjectGroup.id != project_group_id
        ).all()
        if not company_id:
            return MyResponse(code=RET.NO_DATA, msg="您非二代客户，暂无二代工具操作权限")
        if project_group:
            return MyResponse(code=RET.DATA_EXIST, msg="该项目组已存在")
        project_group = db.query(ProjectGroup).filter(
            ProjectGroup.is_delete == False,
            ProjectGroup.id == project_group_id
        ).first()
        group_member = db.query(GroupMemberRelationship.user_id).filter(
            GroupMemberRelationship.is_delete == False,
            GroupMemberRelationship.project_group_id == project_group_id
        ).all()
        member_ids = set([item[0] for item in group_member])
        # 要删除的项目组成员
        delete_member_ids = list(set(member_ids) - set(data.user_ids))
        # 要添加的项目组成员
        add_member_ids = list(set(data.user_ids) - set(member_ids))
        try:
            if delete_member_ids:
                for i in delete_member_ids:
                    member = db.query(GroupMemberRelationship).filter(
                        GroupMemberRelationship.is_delete == False,
                        GroupMemberRelationship.project_group_id == project_group_id,
                        GroupMemberRelationship.user_id == i
                    ).first()
                    member.is_delete = True
            if add_member_ids:
                for i in add_member_ids:
                    member = db.query(GroupMemberRelationship).filter(
                        GroupMemberRelationship.project_group_id == project_group_id,
                        GroupMemberRelationship.user_id == i
                    ).first()
                    if not member:
                        new_member = GroupMemberRelationship(
                            project_group_id=project_group_id,
                            user_id=i
                        )
                        db.add(new_member)
                    else:
                        member.is_delete = False
            project_group.operation_type = data.operation_type
            project_group.remark = data.remark
            project_group.project_name = data.name
        except Exception as e:
            db.rollback()
            web_log.log_error(f"编辑项目组失败原因：{e}")
            return MyResponse(code=RET.DATA_ERR, msg="编辑失败")
        else:
            db.commit()
        return MyResponse(msg="编辑成功")

    @CustomerRouter.delete("/project_groups/{project_group_id}", description="删除项目组")
    async def delete_project_group(self, project_group_id: int, db: Session = Depends(get_db)):
        company_id = self.check_second(db)
        if not company_id:
            return MyResponse(code=RET.NO_DATA, msg="您非二代客户，暂无二代工具操作权限")
        try:
            db.query(ProjectGroup).filter(
                ProjectGroup.is_delete == False,
                ProjectGroup.id == project_group_id
            ).update({"is_delete": 1})
            # 删除项目组成员
            db.query(GroupMemberRelationship).filter(
                GroupMemberRelationship.is_delete == False,
                GroupMemberRelationship.project_group_id == project_group_id
            ).update({"is_delete": 1})
            # 获取项目组账户
            group_accounts = db.query(GroupAccountRelationship).filter(
                GroupAccountRelationship.is_delete == False,
                GroupAccountRelationship.project_group_id == project_group_id
            ).all()
            accredit_redis = get_redis_connection("accredit_account")  # 授权账户
            account_redis = get_redis_connection("medium_account")  # 账户信息
            get_account_pipe = account_redis.pipeline()
            account_pipe = account_redis.pipeline()
            # 判断这个项目组有没有授权过广告账户
            if group_accounts:
                # 删除Redis7号库存储的项目组与广告账户信息
                accredit_redis.delete(f"group_advertising_account:{project_group_id}")
                account_ids = []
                for account in group_accounts:
                    account_id = account.account_id
                    account_id = f"account:{account_id.replace('-', '')}"
                    # 获取这个广告账户授权给哪些项目组
                    get_account_pipe.hget(account_id, "project_groups")
                    account_ids.append(account_id)
                data = get_account_pipe.execute()
                # 更改账户所属组
                for index, project_groups in enumerate(data):
                    current_list = []
                    # 如果该账户存在授权
                    if project_groups:
                        # 现在存储的是之前已经授权的项目组id
                        current_list = json.loads(project_groups)
                    if project_group_id in current_list:
                        # 删除Redis6号库广告账户下的这个项目组id
                        current_list.remove(project_group_id)
                        account_pipe.hset(account_ids[index], "project_groups",
                                          json.dumps(current_list))
                    if len(current_list) == 0:
                        # 如果删除了该项目组id 列表长度为0 就删除整个字段
                        account_pipe.hdel(account_ids[index], "project_groups")
                account_pipe.execute()
                # 删除项目组账户
                db.query(GroupAccountRelationship).filter(
                    GroupAccountRelationship.is_delete == False,
                    GroupAccountRelationship.project_group_id == project_group_id
                ).update({"is_delete": 1})
        except Exception as e:
            db.rollback()
            web_log.log_error(f"删除项目组失败原因：{e}")
            return MyResponse(code=RET.DATA_ERR, msg="删除失败")
        else:
            db.commit()
        return MyResponse(msg="删除成功")


@cbv(CustomerRouter)
class GroupAuthorizationServer:
    request: Request

    def second_check(self, db):
        company_id = db.query(AdvertiserRegister.id).filter(
            AdvertiserRegister.is_delete == False,
            AdvertiserRegister.status == RegisterStatus.AGREE.value,
            AdvertiserRegister.user_id == self.request.state.user.user_id,
            AdvertiserRegister.is_second == True
        ).scalar()
        return company_id

    def data_frame_insert_db(self, data_frame):
        connect = engine.connect()
        # 将DataFrame批量插入到数据库
        data_frame.to_sql('cu_group_account_relationship', con=connect, if_exists='append', index=False)
        connect.close()

    async def thread_data_async(self, data):
        result = await asyncio.to_thread(self.data_frame_insert_db, data)
        return result

    @CustomerRouter.get("/unauthorized", description="广告账户未授权列表")
    async def unauthorized_list(self,
                                group_id: int,
                                medium: str = None,
                                params: CommonQueryParams = Depends(),
                                db: Session = Depends(get_db)):
        cus_id_list = get_customer_ids(db, group_id)
        if not isinstance(cus_id_list, list):
            return cus_id_list
        status, result = get_unauthorized_accounts(
            self.request.state.user.user_id,
            cus_id_list, medium, params.q,
            group_id,
            params.page,
            params.page_size,
            trace_id=self.request.state.trace_id
        )
        return result

    @CustomerRouter.get("/authorized", description="广告账户已授权列表")
    async def authorized_list(self,
                              group_id: int,
                              start_time: str = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
                              end_time: str = Query('', regex='^(\d{4}-\d{1,2}-\d{1,2}|)$'),
                              medium: str = None,
                              params: CommonQueryParams = Depends(),
                              db: Session = Depends(get_db)):
        query = get_authorized_accounts(db, group_id, medium, params.q, start_time, end_time)
        if not query:
            return MyResponse(RET.NO_DATA, error_map[RET.NO_DATA])
        obj = MyPagination(query, params.page, params.page_size)
        return MyResponse(data=obj.data, total=obj.counts)

    @CustomerRouter.post("/authorized", description="广告账户授权")
    async def adv_authorized(self, data: GroupAuthorizedSchema, db: Session = Depends(get_db)):
        company_id = self.second_check(db)
        if not company_id:
            return MyResponse(code=RET.NO_DATA, msg="您非二代客户，暂无二代工具操作权限")
        data_dict = {}
        hash_keys = []
        # 获取redis链接
        set_key = f'group_advertising_account:{data.group_id}'
        r_ga = get_redis_connection('accredit_account')
        r_ga_pipe = r_ga.pipeline()
        r_ma = get_redis_connection('medium_account')
        r_ma_pipe = r_ma.pipeline()
        if not data.group_id:
            return MyResponse(RET.DATA_ERR, error_map[RET.DATA_ERR])
        project_group = db.query(ProjectGroup).filter(ProjectGroup.id == data.group_id).one_or_none()
        if not project_group:
            return MyResponse(RET.DATA_ERR, '未找到对应项目组')
        if not data.account_ids:
            cus_id_list = get_customer_ids(db, data.group_id)
            if not isinstance(cus_id_list, list):
                return cus_id_list
            status, crm_result = get_unauthorized_accounts(self.request.state.user.user_id,
                                                           cus_id_list=cus_id_list, medium=data.target_medium,
                                                           q=data.q, group_id=data.group_id,
                                                           trace_id=self.request.state.trace_id)
            if not status:
                return crm_result
            mediums_set = set()
            for account in crm_result["data"]:
                account_id = account["account_id"]
                data_dict[account_id] = {"medium": account["medium"],
                                         "account_name": account["account_name"]}
                r_ga_pipe.sadd(set_key, account_id)
                # 将组id写入redis 6库account：key的 project_groups
                hash_key = f"account:{account_id.replace('-', '')}"
                hash_keys.append(hash_key)
                # 获取当前project_groups列表
                r_ma_pipe.hget(hash_key, 'project_groups')
                mediums_set.add(account["medium"])
            data.mediums = list(mediums_set)
            r_ga_pipe.execute()
            results = r_ma_pipe.execute()
        else:
            for account_id, medium, account_name in zip(data.account_ids, data.mediums, data.account_names):
                data_dict[account_id] = {"medium": medium,
                                         "account_name": account_name}
                r_ga_pipe.sadd(set_key, account_id)
                # 将组id写入redis 6库account：key的 project_groups
                hash_key = f"account:{account_id.replace('-', '')}"
                hash_keys.append(hash_key)
                # 获取当前project_groups列表
                r_ma_pipe.hget(hash_key, 'project_groups')
            r_ga_pipe.execute()
            results = r_ma_pipe.execute()
        for key, current_list_json in zip(hash_keys, results):
            if current_list_json is None:
                current_list = []
            else:
                current_list = json.loads(current_list_json)
            current_list.append(data.group_id)
            new_current_list = list(set(current_list))
            # 将更新后的列表存回哈希表
            r_ma_pipe.hset(key, 'project_groups', json.dumps(new_current_list))
        r_ma_pipe.execute()
        # 关系表批量更新
        group_account = db.query(GroupAccountRelationship).filter(
            GroupAccountRelationship.account_id.in_(list(data_dict.keys())),
            GroupAccountRelationship.project_group_id == data.group_id,
            GroupAccountRelationship.is_delete == True
        )
        ids_to_update = [str(row.id) for row in group_account.all()]
        account_ids_list = [str(row.account_id) for row in group_account.all()]
        # 更新已有的关系数据
        list_length = len(ids_to_update)
        for i in range(0, list_length, 1000):
            chunk = ids_to_update[i:i + 1000]
            update_ids = ','.join(chunk)
            sql_text = f"UPDATE cu_group_account_relationship SET is_delete = 0, update_time = {func.now()} WHERE id in ({update_ids})"
            db.execute(text(sql_text))
            db.commit()
        for account_id in account_ids_list:
            if data_dict.get(account_id):
                del data_dict[account_id]
        tasks = []
        data_list = [
            {
                "created_time": datetime.datetime.now(),
                "update_time": datetime.datetime.now(),
                "is_delete": 0,
                "project_group_id": data.group_id,
                "account_id": key,
                "medium": values['medium'],
                "account_name": values['account_name']
            } for key, values in data_dict.items()
        ]
        list_length = len(data_list)
        for i in range(0, list_length, 5000):
            chunk = data_list[i:i + 5000]
            datas = pd.DataFrame(chunk)
            tasks.append(self.thread_data_async(datas))
        await asyncio.gather(*tasks)

        # 媒介写入ProjectGroup.mediums
        new_medium = data.mediums + project_group.mediums
        _medium = set(new_medium)
        project_group.mediums = list(_medium)
        db.commit()
        return MyResponse()

    @CustomerRouter.delete("/authorized", description="广告账户取消授权")
    async def adv_del_authorized(self, data: GroupUnauthorizedSchema, db: Session = Depends(get_db)):
        company_id = self.second_check(db)
        if not company_id:
            return MyResponse(code=RET.NO_DATA, msg="您非二代客户，暂无二代工具操作权限")
        if not data.group_id:
            return MyResponse(RET.DATA_ERR, error_map[RET.DATA_ERR])
        if not data.account_ids:
            query = get_authorized_accounts(db, data.group_id, data.target_medium, data.q, data.start_time,
                                            data.end_time)
            if not query:
                return MyResponse(RET.DATA_ERR, error_map[RET.DATA_ERR])
            data.account_ids = [data.account_id for data in query]
        project_group = db.query(ProjectGroup).filter(ProjectGroup.id == data.group_id).one_or_none()
        if not project_group:
            return MyResponse(RET.DATA_ERR, '未找到对应项目组')
        r_ga = get_redis_connection('accredit_account')
        r_ga_pipe = r_ga.pipeline()
        r_ma = get_redis_connection('medium_account')
        r_ma_pipe = r_ma.pipeline()
        set_key = f'group_advertising_account:{data.group_id}'
        # 关系表批量更新
        group_account = db.query(GroupAccountRelationship).filter(
            GroupAccountRelationship.account_id.in_(data.account_ids),
            GroupAccountRelationship.project_group_id == data.group_id,
            GroupAccountRelationship.is_delete == False
        )
        ids_to_update = [str(row.id) for row in group_account.all()]
        list_length = len(ids_to_update)
        for i in range(0, list_length, 1000):
            chunk = ids_to_update[i:i + 1000]
            update_ids = ','.join(chunk)
            sql_text = f"UPDATE cu_group_account_relationship SET is_delete = 1, update_time = {func.now()} WHERE id in ({update_ids})"
            db.execute(text(sql_text))
            db.commit()
        project_group.mediums = get_new_mediums(db, data.group_id)
        db.commit()
        # 更新redis
        for account_id in data.account_ids:
            r_ga_pipe.srem(set_key, account_id)
            hash_key = f"account:{account_id.replace('-', '')}"
            # 获取当前列表
            r_ma_pipe.hget(hash_key, 'project_groups')

        r_ga_pipe.execute()
        current_list_json = r_ma_pipe.execute()
        for current_list, account_id in zip(current_list_json, data.account_ids):
            hash_key = f"account:{account_id.replace('-', '')}"
            current_list = json.loads(current_list)
            if data.group_id in current_list:
                current_list.remove(data.group_id)
            # 将更新后的列表存回哈希表
            r_ma_pipe.hset(hash_key, 'project_groups', json.dumps(current_list))
        r_ma_pipe.execute()
        return MyResponse()

    @CustomerRouter.get("/group_mediums", description="项目组已授权媒介列表")
    async def select_mediums(self, group_id: int, db: Session = Depends(get_db)):
        mediums = [{"value": medium, "label": medium} for medium in get_new_mediums(db, group_id)]
        return MyResponse(data=mediums)

    @CustomerRouter.get("/group_un_mediums", description="项目组未授权媒介列表")
    async def select_un_mediums(self, group_id: int, db: Session = Depends(get_db)):
        cus_id_list = get_customer_ids(db, group_id)
        if not isinstance(cus_id_list, list):
            return cus_id_list
        status, result = get_unauthorized_accounts_mediums(
            self.request.state.user.user_id,
            cus_id_list,
            group_id,
            trace_id=self.request.state.trace_id
        )
        return result
