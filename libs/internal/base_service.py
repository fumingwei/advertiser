import requests
from abc import ABC
from core.customer_consul import CustomerConsul
from core.micro_service import get_service_path
from tools.common import MyResponse
from tools.constant import RET
from tools.exceptions import InternalNetworkError, InternalRequestError
from requests.adapters import HTTPAdapter, Retry
from settings.log import common_log

session = requests.Session()
retries = Retry(total=3, backoff_factor=1)
session.mount('http://', HTTPAdapter(max_retries=retries))
session.mount('https://', HTTPAdapter(max_retries=retries))


class BaseService(ABC):
    api_key = ""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def get(cls, url, params=None, service='', headers=None, timeout=5, **kwargs):
        if not params:
            params = dict()
        if not headers:
            headers = dict()
        if cls.api_key:
            params["api_key"] = cls.api_key
        if kwargs.get('trace_id'):
            headers.update({'X-B3-TraceId': kwargs.get('trace_id')})
        try:
            res = session.get(url, params=params, headers=headers, timeout=timeout)
            res.raise_for_status()
        except requests.exceptions.ReadTimeout as e:
            common_log.log_error(f"{url}, 调用{service}请求超时:{e}")
            raise InternalNetworkError(f"调用{service}服务请求超时")
        except requests.exceptions.RequestException as e:
            common_log.log_error(f"{url}, 调用{service}服务异常, 原因:{e}")
            raise InternalNetworkError(f"调用{service}服务异常")
        except Exception as e:
            common_log.log_error(f"{url}, 调用{service}未知错误, 错误信息:{e}")
            raise InternalNetworkError(f"调用{service}服务异常")
        result = res.json()
        code = result.get("code")
        if code is None:
            common_log.log_error(f"{url}, 响应内容无'code'字段: {result}")
            raise InternalRequestError(f"调用{service}服务未获取到响应码")
        if code == 0:
            return result
        if code == 4101:
            common_log.log_info(f"{url}, 响应码: 4101, 详细信息: {result}")
            raise InternalRequestError(f"{service}服务网络异常4101")
        else:
            common_log.log_info(f"{url}, 调用{service}服务详细信息: {result}")
            raise InternalRequestError(result.get("msg", "未知错误"))

    @classmethod
    def post(cls, url, data=None, json=None, params=None, headers=None, service='', timeout=5, **kwargs):
        if not params:
            params = dict()
        if not json:
            json = dict()
        if not headers:
            headers = dict()
        if not data:
            data = dict()
        if cls.api_key:
            params["api_key"] = cls.api_key
        if kwargs.get('trace_id'):
            headers.update({'X-B3-TraceId': kwargs.get('trace_id')})
        is_handle = kwargs.get("is_handle")
        try:
            res = session.post(url, data=data, json=json, params=params, headers=headers, timeout=timeout)
            res.raise_for_status()
        except requests.exceptions.ReadTimeout as e:
            common_log.log_error(f"{url}, 调用{service}请求超时:{e}")
            if is_handle:
                return {}
            raise InternalNetworkError(f"调用{service}服务请求超时")
        except requests.exceptions.RequestException as e:
            common_log.log_error(f"{url}, 调用{service}服务异常, 原因:{e}")
            if is_handle:
                return {}
            raise InternalNetworkError(f"调用{service}服务异常")
        except Exception as e:
            common_log.log_error(f"{url}, 调用{service}未知错误, 错误信息:{e}")
            if is_handle:
                return {}
            raise InternalNetworkError(f"调用{service}服务异常")
        result = res.json()
        code = result.get("code")
        if code is None:
            common_log.log_error(f"{url}, 响应内容无'code'字段: {result}")
            if is_handle:
                return result
            raise InternalRequestError(f"调用{service}服务未获取到响应码")
        if code == 0:
            return result
        if code == 4101:
            common_log.log_info(f"{url}, 响应码: 4101, 详细信息: {result}")
            if is_handle:
                return result
            raise InternalRequestError(f"{service}服务网络异常4101")
        else:
            common_log.log_info(f"{url}, 调用{service}服务详细信息: {result}")
            if is_handle:
                return result
            raise InternalRequestError(result.get("msg", "未知错误"))

    @classmethod
    def export(cls, url, data=None, json=None, params=None, headers=None, service='', timeout=20, **kwargs):
        """
        导出数据调用，返回文件流
        保存的文件名为ExportFile.csv
        """
        if not json:
            json = dict()
        if not params:
            params = dict()
        if not headers:
            headers = dict()
        if not data:
            data = dict()
        if cls.api_key:
            params["api_key"] = cls.api_key
        if kwargs.get('trace_id'):
            headers.update({'X-B3-TraceId': kwargs.get('trace_id')})
        try:
            res = session.post(url, data=data, json=json, params=params, headers=headers, timeout=timeout)
            res.raise_for_status()
        except requests.exceptions.ReadTimeout as e:
            common_log.log_error(f"{url}, 调用{service}服务请求超时:{e}")
            raise InternalNetworkError(f"调用{service}服务请求超时")
        except requests.exceptions.RequestException as e:
            common_log.log_error(f"{url}, 调用{service}服务异常, 原因:{e}")
            raise InternalNetworkError(f"调用{service}服务异常")
        except Exception as e:
            common_log.log_error(f"{url}, 调用{service}未知错误, 错误信息:{e}")
            raise InternalNetworkError(f"调用{service}服务异常")
        if res.headers.get("Content-Type") == "application/json":
            response = res.json()
            code = response.get("code")
            if code is None:
                common_log.log_error(f"{url}, 响应内容无'code'字段: {response}")
                raise InternalRequestError(f"调用{service}服务未获取到响应码")
            if code == 0:
                return response
            if code == 4101:
                common_log.log_error(f"{url}, 响应码: 4101, 详细信息: {response}")
                raise InternalRequestError(f"{service}服务网络异常4101")
            else:
                common_log.log_info(f"{url}, 调用{service}服务详细信息: {response}")
                raise InternalRequestError(response.get("msg", "未知错误"))
        return res.content


def get_service_url(service, path_name):
    my_consul = CustomerConsul()
    service_url = my_consul.discover_service(service)
    if not service_url:
        return MyResponse(code=RET.SERVER_ERR, msg=f'{service}服务异常')
    uri = get_service_path(service, path_name)
    return uri


if __name__ == '__main__':
    # url_list = []
    # url = get_service_url('crm', 'customer_id_name')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'accounts')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'mediums')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'recharges')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'recharge_list')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'recharge_export')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'recharge_detail')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'resets')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'reset_list')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'reset_detail')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'reset_export')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'basic_info_list')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'account_customers')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'account_put_ways')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'account_mediums')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'detail_info_list')
    # url_list.append(url)
    #
    # url = get_service_url('crm', 'account_info')
    # url_list.append(url)
    #
    # url = get_service_url('mapi', 'meta_pixel_account')
    # url_list.append(url)
    #
    # url = get_service_url('mapi', 'common_account_rename')
    # url_list.append(url)
    #
    # url = get_service_url('mapi', 'meta_bm_account')
    # url_list.append(url)
    #
    # url = get_service_url('mapi', 'tiktok_advertiser_partner')
    # url_list.append(url)
    #
    # url = get_service_url('mapi', 'meta_oe_open_account')
    # url_list.append(url)
    #
    # url = get_service_url('rtdp', 'common_customer_last_seven_days_cost')
    # url_list.append(url)
    #
    # url = get_service_url('rtdp', 'common_customer_account_spend_rank')
    # url_list.append(url)

    # url = get_service_url('rtdp', 'common_customer_spend_every')
    # url_list.append(url)

    # url = get_service_url('mapi', 'common_account_recharge')
    # url_list.append(url)

    url = get_service_url('mapi', 'common_model_result')
    # url_list.append(url)

    print(url, type(url))
    # print(len(url_list), url_list)
