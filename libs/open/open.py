# -*- coding: utf-8 -*-
from libs.open import session
from tools.common import SingletonType


class OpenAPIRequest(metaclass=SingletonType):
    # ip地址解析
    @staticmethod
    def ip_parse(ip):
        url = f"http://ip-api.com/json/{ip}?lang=zh-CN"
        try:
            json_res = session.get(url, timeout=3).json()
            if json_res.get("status") == "success":
                return {
                    "ip": ip,  # ip地址
                    "country": json_res.get("country"),  # 国家
                    "province": json_res.get("regionName"),  # 省份
                    "city": json_res.get("city"),  # 城市
                    "countrycode": json_res.get("countrycode"),  # 国家代码
                    "longitude": json_res.get("lon"),  # 维度
                    "latitude": json_res.get("lat"),  # 经度
                    "isp": json_res.get("isp"),  # 运营商
                }
            return {"ip": ip, "country": "局域网"}
        except:
            return {}
