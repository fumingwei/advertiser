# -*- coding: utf-8 -*-
import ipaddress


# 判定ip是否是内网ip   防火墙+内网ip双机制
def is_privilege_ip(ip) -> bool:
    try:
        ip_obj = ipaddress.ip_address(ip)
        return ip_obj.is_private
    except ValueError:
        return False
