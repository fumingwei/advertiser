# -*- coding: utf-8 -*-
import random
from consul import Consul, Check
from settings.base import configs


class CustomerConsul:
    def __init__(self, consul_host=configs.CONSUL_HOST, consul_port=configs.CONSUL_PORT):
        self.consul_host = consul_host
        self.consul_port = consul_port
        self.consul_client = Consul(host=self.consul_host, port=self.consul_port)

    # 服务发现
    def discover_service(self, service_name):
        if self.consul_client is None:
            raise ValueError("Consul client has not been initialized.")
        services: tuple = self.consul_client.catalog.service(service_name)
        if services[1]:
            # 随机选择一个服务
            service = random.choice(services[1])
            return service['ServiceAddress'], service['ServicePort']
        return None, None

    # 服务注册
    def register_service(self, service_id, service_name, service_host, service_port):
        if self.consul_client is None:
            raise ValueError("Consul client has not been initialized.")
        check_http = Check.http(
            url='http://' + service_host + ':' + str(service_port) + '/healthcheck',
            interval='10s',
            timeout='5s',
            deregister='1m'
        )
        self.consul_client.agent.service.register(
            name=service_name,
            service_id=service_id,
            address=service_host,
            port=service_port,
            check=check_http
        )

    # 服务注销
    def deregister_service(self, service_id):
        if self.consul_client is None:
            raise ValueError("Consul client has not been initialized.")
        self.consul_client.agent.service.deregister(service_id)


if __name__ == '__main__':
    my_consul = CustomerConsul()
    server_host, server_port = my_consul.discover_service('ucenter')
    print(server_host, server_port)
