# -*- coding: utf-8 -*-
import configparser
import os

config = configparser.ConfigParser()
current_script_path = os.path.dirname(os.path.dirname(__file__))
file_path = os.path.join(current_script_path, 'micro_config.ini')
config.read(file_path, encoding='utf-8')


def get_service_path(service, path_name):
    path = config.get(service, path_name)
    return path


if __name__ == '__main__':
    demo_path = get_service_path('crm', 'customer_id_name')
    print(type(demo_path), demo_path)

