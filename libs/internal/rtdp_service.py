from libs.internal.base_service import BaseService, get_service_url
from core.customer_consul import CustomerConsul


class RTDPService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_service():
        my_consul = CustomerConsul()
        server_host, server_port = my_consul.discover_service('rtdp')
        return server_host, server_port

    @classmethod
    def last_7_days_total_cost(cls, json, **kwargs):
        """近7天总花费"""
        server_host, server_port = cls.get_service()
        path = get_service_url('rtdp', 'common_customer_last_seven_days_cost')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, service='rtdp', **kwargs)['data']

    @classmethod
    def spend_rank(cls, json, **kwargs):
        """广告账户消耗排名"""
        server_host, server_port = cls.get_service()
        path = get_service_url('rtdp', 'common_customer_account_spend_rank')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, service='rtdp', **kwargs)['data']

    @classmethod
    def insight_spend(cls, json, **kwargs):
        """广告账户消耗数据"""
        server_host, server_port = cls.get_service()
        path = get_service_url('rtdp', 'common_customer_spend_every')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, service='rtdp', **kwargs)['data']

    @classmethod
    def account_spend(cls, json=None, **kwargs):
        """账户列表消耗数据"""
        server_host, server_port = cls.get_service()
        path = get_service_url('rtdp', 'account_spend')
        url = f"http://{server_host}:{server_port}{path}"
        return cls.post(url, json=json, service='rtdp', **kwargs, timeout=10)['data']
