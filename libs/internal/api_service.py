from settings.base import configs
from libs.internal.base_service import BaseService, get_service_url
from core.customer_consul import CustomerConsul


class APIService(BaseService):
    api_key = configs.MAPI_KEY

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_service():
        my_consul = CustomerConsul()
        server_host, server_port = my_consul.discover_service('mapi')
        return server_host, server_port

    @classmethod
    def post_pixel_account(cls, json, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('mapi', 'meta_pixel_account')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, service='mapi', **kwargs)['data']['request_id']

    @classmethod
    def post_rename_account(cls, json, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('mapi', 'common_account_rename')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, service='mapi', **kwargs)['data']['request_id']

    @classmethod
    def post_bm_account(cls, json, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('mapi', 'meta_bm_account')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, service='mapi', **kwargs)['data']['request_id']

    @classmethod
    def post_bc_account(cls, json, **kwargs):
        server_host, server_port = cls.get_service()
        path = get_service_url('mapi', 'tiktok_advertiser_partner')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, service='mapi', **kwargs)['data']['request_id']

    # 获取oe开户信息
    @classmethod
    def get_oe_open_account(cls, oe_number, **kwargs):
        """
        获取oe开户信息
        """
        server_host, server_port = cls.get_service()
        ser_url = get_service_url('mapi', 'meta_oe_open_account')
        path = ser_url + f'/{oe_number}'
        url = f"http://{server_host}:{server_port}{path}"
        return cls.get(url, service='mapi', **kwargs)

    @classmethod
    def get_common_model_result(cls, params, **kwargs):
        """
        根据require_id, 获取详情数据
        """
        server_host, server_port = cls.get_service()
        uri = get_service_url('mapi', 'common_model_result')
        url = f"http://{server_host}:{server_port}{uri}"
        return cls.get(url, params=params, service='mapi', **kwargs)

    @classmethod
    def recharge(cls, json, **kwargs):
        """
        充值
        """
        server_host, server_port = cls.get_service()
        uri = get_service_url('mapi', 'common_account_recharge')
        url = f"http://{server_host}:{server_port}{uri}"
        res = cls.post(url, json=json, service='mapi', **kwargs)
        return res

    @classmethod
    def reset(cls, json, **kwargs):
        """
        清零
        """
        server_host, server_port = cls.get_service()
        uri = get_service_url('mapi', 'common_account_recharge')
        url = f"http://{server_host}:{server_port}{uri}"
        return cls.post(url, json=json, service='mapi', **kwargs)
