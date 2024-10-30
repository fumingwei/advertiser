from libs.internal.base_service import BaseService, get_service_url
from core.customer_consul import CustomerConsul


class CRMExternalService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_service():
        my_consul = CustomerConsul()
        server_host, server_port = my_consul.discover_service('crm')
        return server_host, server_port

    @classmethod
    def customer_id_name(cls, json, headers=None, **kwargs):
        """
        可以根据客户ID获取客户名称;
        可以客户名称模糊匹配客户ID;
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'customer_id_name')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url=url, json=json, headers=headers, service='crm', **kwargs)

    @classmethod
    def customer_id_name_oms(cls, json, headers=None, **kwargs):
        """
        可以根据客户ID获取客户名称;
        可以客户名称模糊匹配客户ID;
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'customer_id_name')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, service='crm', **kwargs)

    @classmethod
    def get_recharge_reset_medium(cls, json, headers=None, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'recharge_reset_medium')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, service='crm', **kwargs)

    @classmethod
    def get_accounts(cls, json=None, headers=None, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'accounts')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, timeout=15, service='crm', **kwargs)

    @classmethod
    def get_ua_accounts(cls, json=None, headers=None, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'unauthorized_accounts')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, timeout=15, service='crm', **kwargs)

    @classmethod
    def get_account_mediums(cls, json=None, headers=None, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'mediums')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_recharge(cls, json=None, params=None, headers=None, **kwargs):
        """
        充值提交信息
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'recharges')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_recharge_list(cls, json=None, params=None, headers=None, **kwargs):
        """
        获取充值列表数据
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'recharge_list')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_recharge_export(cls, json=None, params=None, headers=None, **kwargs):
        """
        充值导出
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'recharge_export')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.export(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def get_recharge_detail(cls, params=None, headers=None, **kwargs):
        """
        获取充值详情
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'recharge_detail')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.get(url, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_reset(cls, json=None, params=None, headers=None, **kwargs):
        """
        清零提交信息
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'resets')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def get_reset_list(cls, json=None, headers=None, **kwargs):
        """
        获取清零列表数据
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'reset_list')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, service='crm', **kwargs)

    @classmethod
    def get_reset_detail(cls, params=None, headers=None, **kwargs):
        """
        获取清零详情
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'reset_detail')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.get(url, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_reset_export(cls, json=None, params=None, headers=None, **kwargs):
        """
        清零导出
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'reset_export')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.export(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def basic_info_list(cls, json=None, headers=None, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'basic_info_list')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, service='crm', **kwargs)

    # 获取广告账户对应客户列表
    @classmethod
    def get_account_customers(cls, json=None, headers=None, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'account_customers')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, service='crm', **kwargs)

    # 获取账户投放方式
    @classmethod
    def get_account_put_way(cls, json=None, headers=None, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'account_put_ways')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, service='crm', **kwargs)

    # 获取账户媒介
    @classmethod
    def get_accounts_medium(cls, json=None, headers=None, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'account_mediums')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, service='crm', **kwargs)

    @classmethod
    def detail_info_list(cls, params=None, headers=None, **kwargs):
        """
        个人中心详细信息
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'detail_info_list')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.get(url, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_account_info(cls, json=None, params=None, headers=None, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'account_info')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_bill_list(cls, json=None, params=None, headers=None, **kwargs):
        """
        获取账单列表数据
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'bills')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def get_bill_detail(cls, json=None, params=None, headers=None, **kwargs):
        """
        获取账单详情
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'bills_detail')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.get(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_rebate_use_list(cls, json=None, params=None, headers=None, **kwargs):
        """
        获取返点使用列表数据
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'rebate_uses')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def get_rebate_use_detail(cls, json=None, params=None, headers=None, **kwargs):
        """
        获取返点使用详情
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'rebate_uses_detail')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.get(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_bills_export(cls, json=None, params=None, headers=None, **kwargs):
        """
        账单总览导出
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'bills_export')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.export(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_rebate_uses_export(cls, json=None, params=None, headers=None, **kwargs):
        """
        返点使用记录导出
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'rebate_uses_export')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.export(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_bills_detail_export(cls, json=None, params=None, headers=None, **kwargs):
        """
        账单总览查看明细导出
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'bills_detail_export')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.export(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_account_count(cls, json=None, params=None, headers=None, **kwargs):
        """
        账户状态统计
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'account_count')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.export(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_account_export(cls, json=None, params=None, headers=None, **kwargs):
        """
        账户信息/账户列表导出
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'accounts_export')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.export(url, json=json, params=params, headers=headers, service='crm', **kwargs)

    @classmethod
    def post_account_batch(cls, json=None, headers=None, **kwargs):
        """
        批量提交获取账户基础信息
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'account_batch')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, timeout=10, service='crm', **kwargs)

    @classmethod
    def post_customer_purse(cls, json=None, headers=None, **kwargs):
        """
        获取客户余额
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'customer_purses')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, timeout=10, service='crm', **kwargs)

    @classmethod
    def post_account_search(cls, json=None, headers=None, **kwargs):
        """
        广告账户搜索
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'account_id_screen')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, timeout=10, service='crm', **kwargs)

    @classmethod
    def update_customer_purse(cls, json=None, headers=None, **kwargs):
        """
        更改客户钱包
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'update_customer_purse')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, headers=headers, timeout=10, service='crm', **kwargs)

    @classmethod
    def post_bill_medium(cls, json=None, params=None, headers=None, **kwargs):
        """
        获取账单所有媒介
        """
        server_host, server_port = cls.get_service()
        path = get_service_url('crm', 'bill_medium')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, params=params, headers=headers, service='crm', **kwargs)
