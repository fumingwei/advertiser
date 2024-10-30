from typing import Optional
from pydantic import BaseModel, Field, validator
from apps.common.define import OperateTypeDefine


class ExportSchema(BaseModel):
    """
    导出数据校验
    customer_name: 只为导出文件做展示作用，无过滤作用
    source: 导出来源，只为导出文件做展示作用，无过滤作用
    cooperative_id：bc导出要用
    bm_id: bm导出要用。
    以下暂时去除======================================================================================
    # project_group: str = Field(None, title='项目组')
    # account_id: str = Field(None, title='广告账户id')
    # bill_id: int = Field(None, title='账单id')
    # is_cancel: str = Field(None, title='是否核销')
    # start_month: str = Field(None, regex=r'^(|[\d]{4}-[\d]{2})$', title='账单开始月份')
    # end_month: str = Field(None, regex=r'^(|[\d]{4}-[\d]{2})$', title='账单结束月份')
    # use_customer_id: str = Field(None, regex=r'^(|[\d]+)$', title='返点使用结算')
    # rebate_customer_id: str = Field(None, regex=r'^(|[\d]+)$', title='返点所属结算')
    # operate_user: str = Field(None, title='操作人')
    # source: str = Field(None, title='导出提交板块来源')
    # rebate_date_start: str = Field(None, regex=r'^(|[\d]{4}-[\d]{2}-[\d]{2})$', title='返点开始季度')
    # rebate_date_end: str = Field(None, regex=r'^(|[\d]{4}-[\d]{2}-[\d]{2})$', title='返点结束季度')
    """
    operate_type: str = Field(default=OperateTypeDefine.AccountInfo.value, title='操作类型')
    q: str = Field(None, title='公共查询参数')
    medium: str = Field(None, title='媒介')
    put_way: str = Field(None, title='投放方式')
    approval_status: str = Field(None, title='审批状态')
    account_status: str = Field(None, title='账户状态')
    customer_id: str = Field(None, regex=r'^(|[\d]+)$', title='客户id')
    operation_result: Optional[str | list] = Field(None, title='操作结果')
    start_date: str = Field(None, regex=r'^(|[\d]{4}-[\d]{2}(|-[\d]{2}))$', title='公共开始时间')
    end_date: str = Field(None, regex=r'^(|[\d]{4}-[\d]{2}(|-[\d]{2}))$', title='公共结束时间')
    # 返点使用记录
    use_way: str = Field(None)  # 使用方式
    # tiktok绑定
    cooperative_id: str = Field(None, title='tiktok绑定合作伙伴')
    # bm绑定
    bm_id: str = Field(None, title='bm绑定')
    # 账户列表相关
    start_spend_date: str = Field(None, regex=r'^(|[\d]{4}-[\d]{2}-[\d]{2})$')
    end_spend_date: str = Field(None, regex=r'^(|[\d]{4}-[\d]{2}-[\d]{2})$')
    # 开户历史相关
    oe_status: str = Field(None, title='oe_审批状态')
    bill_id: int = Field(None, title='账单id')
    is_cancel: str = Field(None, title='是否核销')

    @validator('operate_type', pre=True, allow_reuse=True)
    def check_operate_type(cls, v: Optional[str]) -> str:
        if not v or v not in OperateTypeDefine.values():
            raise ValueError(f'暂时不支持此{v}操作,如有需求请联系管理员')
        return v
