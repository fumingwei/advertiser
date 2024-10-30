import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from apps.common.define import FileStatus
from apps.common.models import CuFile
from apps.common.utils import Export
from libs.ali.ali_oss import OssManage
from my_celery.main import celery_app
from settings.db import SessionLocal
from settings.log import celery_log
from tools.common import convert_to_largest_unit


@celery_app.task()
def asy_recharge_export(
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
):
    """
    充值导出,请求的crm，文件流形式
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    db = SessionLocal()
    cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
    try:
        upload = True
        df = Export().recharge(*args_, **kwargs)
        if df.empty:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '充值导出无数据'
            upload = False
    except Exception as e:
        cu_file.file_size = 0
        cu_file.file_status = FileStatus.FAIL.value
        cu_file.remark = '文件生成失败'
        upload = False
        celery_log.log_error(f'充值上传文件失败，原因:{e.__str__()}')
    if upload:
        bio = BytesIO()
        df.to_excel(bio, sheet_name='充值', index=False)
        bytes_ = bio.getvalue()
        OssManage().file_upload(key=cu_file.file_key, file=bytes_)
        result = OssManage().get_obj(cu_file.file_key)
        file_size = convert_to_largest_unit(result.content_length)
        cu_file.file_size = file_size
        cu_file.file_status = FileStatus.SUCCEED.value
        cu_file.expire_time = datetime.now() + timedelta(days=3)
    db.commit()
    return '充值导出操作成功' if upload else "充值导出操作失败"


@celery_app.task()
def asy_reset_export(
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
):
    """
    清零导出，请求的crm，文件流形式
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    db = SessionLocal()
    cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
    try:
        upload = True
        df = Export().reset(*args_, **kwargs)
        if df.empty:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '清零导出无数据'
            upload = False
    except Exception as e:
        cu_file.file_size = 0
        cu_file.file_status = FileStatus.FAIL.value
        cu_file.remark = '文件生成失败'
        upload = False
        celery_log.log_error(f'清零上传文件失败，原因:{e.__str__()}')
    if upload:
        bio = BytesIO()
        df.to_excel(bio, sheet_name='清零', index=False)
        bytes_ = bio.getvalue()
        OssManage().file_upload(key=cu_file.file_key, file=bytes_)
        result = OssManage().get_obj(cu_file.file_key)
        file_size = convert_to_largest_unit(result.content_length)
        cu_file.file_size = file_size
        cu_file.file_status = FileStatus.SUCCEED.value
        cu_file.expire_time = datetime.now() + timedelta(days=3)
    db.commit()
    return '清零导出操作成功' if upload else '清零导出操作失败'


@celery_app.task()
def asy_rename_export(
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
):
    """
    重命名导出，旧版本没有该接口
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    with SessionLocal() as db:
        cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
        try:
            df = Export().rename(*args_, **kwargs)
            upload = True
            if df.empty:
                cu_file.file_size = 0
                cu_file.file_status = FileStatus.FAIL.value
                cu_file.remark = '重命名导出无数据'
                upload = False
        except Exception as e:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '文件生成失败'
            upload = False
            celery_log.log_error(f'上传文件失败，原因:{e.__str__()}')
        if upload:
            bio = BytesIO()
            df.to_excel(bio, sheet_name='重命名', index=False)
            bytes_ = bio.getvalue()
            OssManage().file_upload(key=cu_file.file_key, file=bytes_)
            result = OssManage().get_obj(cu_file.file_key)
            file_size = convert_to_largest_unit(result.content_length)
            cu_file.file_size = file_size
            cu_file.file_status = FileStatus.SUCCEED.value
            cu_file.expire_time = datetime.now() + timedelta(days=3)
        db.commit()
    return '重命名操作信息导出操作完成' if upload else '重命名操作信息导出操作完成'


@celery_app.task()
def asy_bm_export(
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
):
    """
    bm操作导出
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    with SessionLocal() as db:
        try:
            cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
            df = Export().bm(*args_, **kwargs)
            upload = True
            if df.empty:
                cu_file.file_size = 0
                cu_file.file_status = FileStatus.FAIL.value
                cu_file.remark = 'bm导出无数据'
                upload = False
        except Exception as e:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '文件生成失败'
            upload = False
            celery_log.log_error(f'BM绑定广告账户上传文件失败，原因:{e.__str__()}')
        if upload:
            bio = BytesIO()
            df.to_excel(bio, sheet_name='bm绑定解绑', index=False)
            bytes_ = bio.getvalue()
            OssManage().file_upload(key=cu_file.file_key, file=bytes_)
            result = OssManage().get_obj(cu_file.file_key)
            file_size = convert_to_largest_unit(result.content_length)
            cu_file.file_size = file_size
            cu_file.file_status = FileStatus.SUCCEED.value
            cu_file.expire_time = datetime.now() + timedelta(days=3)
        db.commit()
    return 'BM绑定广告账户导出操作成功' if upload else 'BM绑定广告账户导出操作失败'


@celery_app.task()
def asy_bc_export(
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
):
    """
    bc操作导出
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    with SessionLocal() as db:
        try:
            cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
            df = Export().bc(*args_, **kwargs)
            upload = True
            if df.empty:
                cu_file.file_size = 0
                cu_file.file_status = FileStatus.FAIL.value
                cu_file.remark = 'bc导出无数据'
                upload = False
        except Exception as e:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '文件生成失败'
            upload = False
            celery_log.log_error(f'BC绑定广告账户上传文件失败，原因:{e.__str__()}')
        if upload:
            bio = BytesIO()
            df.to_excel(bio, sheet_name='bc绑定解绑', index=False)
            bytes_ = bio.getvalue()
            OssManage().file_upload(key=cu_file.file_key, file=bytes_)
            result = OssManage().get_obj(cu_file.file_key)
            file_size = convert_to_largest_unit(result.content_length)
            cu_file.file_size = file_size
            cu_file.file_status = FileStatus.SUCCEED.value
            cu_file.expire_time = datetime.now() + timedelta(days=3)
        db.commit()
    return 'BC绑定广告账户导出操作成功' if upload else 'BC绑定广告账户导出操作失败'


@celery_app.task()
def asy_pixel_export(
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id,
        **kwargs
):
    """
    pixe操作导出
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    with SessionLocal() as db:
        try:
            cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
            df = Export().pixel(*args_, **kwargs)
            upload = True
            if df.empty:
                cu_file.file_size = 0
                cu_file.file_status = FileStatus.FAIL.value
                cu_file.remark = 'pixel导出无数据'
                upload = False
        except Exception as e:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '文件生成失败'
            upload = False
            celery_log.log_error(f'pixel绑定广告账户上传文件失败，原因:{e.__str__()}')
        if upload:
            bio = BytesIO()
            df.to_excel(bio, sheet_name='pixel绑定解绑', index=False)
            bytes_ = bio.getvalue()
            OssManage().file_upload(key=cu_file.file_key, file=bytes_)
            result = OssManage().get_obj(cu_file.file_key)
            file_size = convert_to_largest_unit(result.content_length)
            cu_file.file_size = file_size
            cu_file.file_status = FileStatus.SUCCEED.value
            cu_file.expire_time = datetime.now() + timedelta(days=3)
        db.commit()
    return 'pixel绑定广告账户导出操作成功' if upload else 'pixel绑定广告账户导出操作失败'


@celery_app.task()
def asy_balance_transfer_export(
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
):
    """
    账户转账导出
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    with SessionLocal() as db:
        cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
        try:
            df = Export().balance_transfer(*args_, **kwargs)
            upload = True
            if df.empty:
                cu_file.file_size = 0
                cu_file.file_status = FileStatus.FAIL.value
                cu_file.remark = '账户转账导出无数据'
                upload = False
        except Exception as e:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '文件生成失败'
            upload = False
            celery_log.log_error(f'账户转账上传文件失败，原因:{e.__str__()}')
        if upload:
            bio = BytesIO()
            df.to_excel(bio, sheet_name='账户转账', index=False)
            bytes_ = bio.getvalue()
            OssManage().file_upload(key=cu_file.file_key, file=bytes_)
            result = OssManage().get_obj(cu_file.file_key)
            file_size = convert_to_largest_unit(result.content_length)
            cu_file.file_size = file_size
            cu_file.file_status = FileStatus.SUCCEED.value
            cu_file.expire_time = datetime.now() + timedelta(days=3)
        db.commit()
    return '账户转账信息导出操作成功' if upload else '账户转账信息导出操作失败'


@celery_app.task()
def asy_accounts_export(
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
):
    """
    账户列表导出
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    db = SessionLocal()
    cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
    try:
        upload = True
        df = Export().accounts(*args_, **kwargs)
        if df.empty:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '账户列表导出无数据'
            upload = False
    except Exception as e:
        cu_file.file_size = 0
        cu_file.file_status = FileStatus.FAIL.value
        cu_file.remark = '文件生成失败'
        upload = False
        celery_log.log_error(f'账户列表导出上传文件失败，原因:{e.__str__()}')
    if upload:
        bio = BytesIO()
        df.style.set_properties(**{'text-align': 'left'}).to_excel(bio, sheet_name='账户列表', index=False)
        bytes_ = bio.getvalue()
        OssManage().file_upload(key=cu_file.file_key, file=bytes_)
        result = OssManage().get_obj(cu_file.file_key)
        file_size = convert_to_largest_unit(result.content_length)
        cu_file.file_size = file_size
        cu_file.file_status = FileStatus.SUCCEED.value
        cu_file.expire_time = datetime.now() + timedelta(days=3)
    db.commit()
    db.close()
    return '账户列表导出操作成功' if upload else '账户列表导出操作失败'


@celery_app.task()
def asy_all_operate_export(
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
):
    """
    操作记录导出,所有可以的操作类型导出，不同操作类型导出不同的sheet
    df1:充值
    df2:清零
    df3:bm绑定
    df4:bc绑定
    df5:余额转移
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    db = SessionLocal()
    cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
    try:
        upload = True
        df1 = Export().recharge(*args_, **kwargs)
        df2 = Export().reset(*args_, **kwargs)
        df6 = Export().rename(*args_, **kwargs)
        # df5 = Export().balance_transfer(*args_, **kwargs)
        # df8 = Export().accounts(*args_, **kwargs)
        dfs = {
            '充值': df1,
            '清零': df2,
            # '余额转移': df5,
            '重命名': df6,
            # '账户列表': df8
        }
        if medium == 'Tiktok' or not medium:
            df4 = Export().bc(*args_, **kwargs)
            dfs.update({'bc': df4})
        if medium == 'Meta' or not medium:
            df3 = Export().bm(*args_, **kwargs)
            df7 = Export().pixel(*args_, **kwargs)
            dfs.update({'bm': df3})
            dfs.update({'pixel': df7})
        not_empty_dfs = {k: v for k, v in dfs.items() if not v.empty}
        if not not_empty_dfs:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '所有操作导出无数据'
            upload = False
    except Exception as e:
        cu_file.file_size = 0
        cu_file.file_status = FileStatus.FAIL.value
        cu_file.remark = '文件生成失败'
        upload = False
        celery_log.log_error(f'上传文件失败，原因:{e.__str__()}')
    if upload:
        bio = BytesIO()
        with pd.ExcelWriter(bio, engine='xlsxwriter') as writer:
            for k, v in not_empty_dfs.items():
                v.to_excel(writer, sheet_name=k, index=False)
        bytes_ = bio.getvalue()
        OssManage().file_upload(key=cu_file.file_key, file=bytes_)
        result = OssManage().get_obj(cu_file.file_key)
        file_size = convert_to_largest_unit(result.content_length)
        cu_file.file_size = file_size
        cu_file.file_status = FileStatus.SUCCEED.value
        cu_file.expire_time = datetime.now() + timedelta(days=3)
    db.commit()
    db.close()
    return '所有操作信息导出操作成功' if upload else '所有操作信息导出操作失败'


@celery_app.task()
def asy_bill_summary_export(
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
):
    """
    账单总览导出
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    db = SessionLocal()
    cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
    try:
        cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
        df = Export().bill_summary(*args_, **kwargs)
        upload = True
        if df.empty:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '账单总览导出无数据'
            upload = False
    except Exception as e:
        cu_file.file_size = 0
        cu_file.file_status = FileStatus.FAIL.value
        cu_file.remark = '文件生成失败'
        upload = False
        celery_log.log_error(f'账单总览导出上传文件失败，原因:{e.__str__()}')
    if upload:
        bio = BytesIO()
        df.to_excel(bio, sheet_name='账单总览', index=False)
        bytes_ = bio.getvalue()
        OssManage().file_upload(key=cu_file.file_key, file=bytes_)
        result = OssManage().get_obj(cu_file.file_key)
        file_size = convert_to_largest_unit(result.content_length)
        cu_file.file_size = file_size
        cu_file.file_status = FileStatus.SUCCEED.value
        cu_file.expire_time = datetime.now() + timedelta(days=3)
    db.commit()
    db.close()
    return '账单总览导出操作成功' if upload else '账单总览导出操作失败'


@celery_app.task()
def asy_rebate_uses_export(
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
):
    """
    返点使用记录导出
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    db = SessionLocal()
    cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
    try:
        cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
        df = Export().rebate(*args_, **kwargs)
        upload = True
        if df.empty:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '返点使用记录导出无数据'
            upload = False
    except Exception as e:
        cu_file.file_size = 0
        cu_file.file_status = FileStatus.FAIL.value
        cu_file.remark = '文件生成失败'
        upload = False
        celery_log.log_error(f'返点使用记录导出上传文件失败，原因:{e.__str__()}')
    if upload:
        bio = BytesIO()
        df.to_excel(bio, sheet_name='返点使用记录', index=False)
        bytes_ = bio.getvalue()
        OssManage().file_upload(key=cu_file.file_key, file=bytes_)
        result = OssManage().get_obj(cu_file.file_key)
        file_size = convert_to_largest_unit(result.content_length)
        cu_file.file_size = file_size
        cu_file.file_status = FileStatus.SUCCEED.value
        cu_file.expire_time = datetime.now() + timedelta(days=3)
    db.commit()
    db.close()
    return '返点使用记录导出操作成功' if upload else '返点使用记录导出操作失败'


@celery_app.task()
def asy_bill_detail_export(
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
):
    """
    账单总览查看明细导出
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    db = SessionLocal()
    cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
    try:
        cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
        df = Export().bill_detail(*args_, **kwargs)
        upload = True
        if df.empty:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '账单总览查看明细导出无数据'
            upload = False
    except Exception as e:
        cu_file.file_size = 0
        cu_file.file_status = FileStatus.FAIL.value
        cu_file.remark = '文件生成失败'
        upload = False
        celery_log.log_error(f'账单总览查看明细导出上传文件失败，原因:{e.__str__()}')
    if upload:
        bio = BytesIO()
        df.to_excel(bio, sheet_name='账单总览查看明细', index=False)
        bytes_ = bio.getvalue()
        OssManage().file_upload(key=cu_file.file_key, file=bytes_)
        result = OssManage().get_obj(cu_file.file_key)
        file_size = convert_to_largest_unit(result.content_length)
        cu_file.file_size = file_size
        cu_file.file_status = FileStatus.SUCCEED.value
        cu_file.expire_time = datetime.now() + timedelta(days=3)
    db.commit()
    db.close()
    return '账单总览查看明细导出操作成功' if upload else '账单总览查看明细导出操作失败'


@celery_app.task()
def asy_account_info_export(
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
):
    """
    账户信息导出
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    db = SessionLocal()
    cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
    try:
        cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
        df = Export().accounts_info(*args_, **kwargs)
        upload = True
        if df.empty:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = '账户信息导出无数据'
            upload = False
    except Exception as e:
        cu_file.file_size = 0
        cu_file.file_status = FileStatus.FAIL.value
        cu_file.remark = '文件生成失败'
        upload = False
        celery_log.log_error(f'账户信息导出上传文件失败，原因:{e.__str__()}')
    if upload:
        bio = BytesIO()
        # 以媒介分组，不同媒介分成不同sheet表
        grouped_df = df.groupby('medium')
        with pd.ExcelWriter(bio, engine='xlsxwriter') as writer:
            for medium, group in grouped_df:
                group.rename(columns={
                    'account_id': "广告账户ID",
                    "account_name": "广告账户名称",
                    "medium": "投放媒介",
                    "account_status": "广告账户状态",
                    "customer_name": "结算客户"
                }, inplace=True)
                group.to_excel(writer, index=False, sheet_name=medium, engine='openpyxl')
        bytes_ = bio.getvalue()
        OssManage().file_upload(key=cu_file.file_key, file=bytes_)
        result = OssManage().get_obj(cu_file.file_key)
        file_size = convert_to_largest_unit(result.content_length)
        cu_file.file_size = file_size
        cu_file.file_status = FileStatus.SUCCEED.value
        cu_file.expire_time = datetime.now() + timedelta(days=3)
    db.commit()
    db.close()
    return '账户信息导出操作成功' if upload else '账户信息导出操作失败'


@celery_app.task()
def asy_oe_open_account_export(
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
):
    """
    oe开户列表导出
    """
    args_ = (
        project_group,
        account_id,
        medium,
        customer_id,
        operate_result,
        user_id,
        operate_user_id,
        start_date,
        end_date,
        file_id
    )
    db = SessionLocal()
    cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
    try:
        cu_file = db.query(CuFile).filter(CuFile.id == file_id).first()
        df = Export().oe_open_account(*args_, **kwargs)
        upload = True
        if df.empty:
            cu_file.file_size = 0
            cu_file.file_status = FileStatus.FAIL.value
            cu_file.remark = 'oe开户导出无数据'
            upload = False
    except Exception as e:
        cu_file.file_size = 0
        cu_file.file_status = FileStatus.FAIL.value
        cu_file.remark = '文件生成失败'
        upload = False
        celery_log.log_error(f'oe开户导出上传文件失败，原因:{e.__str__()}')
    if upload:
        bio = BytesIO()
        df.to_excel(bio, sheet_name='开户历史', index=False)
        bytes_ = bio.getvalue()
        OssManage().file_upload(key=cu_file.file_key, file=bytes_)
        result = OssManage().get_obj(cu_file.file_key)
        file_size = convert_to_largest_unit(result.content_length)
        cu_file.file_size = file_size
        cu_file.file_status = FileStatus.SUCCEED.value
        cu_file.expire_time = datetime.now() + timedelta(days=3)
    db.commit()
    db.close()
    return 'oe开户导出操作成功' if upload else 'oe开户导出操作失败'


if __name__ == '__main__':
    asy_oe_open_account_export()
