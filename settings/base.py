# -*- coding: utf-8 -*-
import io
import os
from contextlib import contextmanager
from functools import lru_cache
from io import StringIO
from dotenv.main import DotEnv
from pydantic import BaseSettings, Field
from typing import Optional


def my_get_stream(self):
    """重写python-dotenv读取文件的方法，使用utf-8，支持读取包含中文的.env配置文件"""
    if isinstance(self.dotenv_path, StringIO):
        yield self.dotenv_path
    elif os.path.isfile(self.dotenv_path):
        with io.open(self.dotenv_path, encoding="utf-8") as stream:
            yield stream
    else:
        if self.verbose:
            print("File doesn't exist %s", self.dotenv_path)
        yield StringIO("")


DotEnv._get_stream = contextmanager(my_get_stream)


class Settings(BaseSettings):
    """System configurations."""

    # 系统环境
    ENVIRONMENT: Optional[str] = Field(None, env="ENVIRONMENT")

    # 系统安全秘钥
    SECRET_KEY: Optional[str] = Field(None, env="SECRET_KEY")

    # API版本号
    API_VERSION_STR = "/api/v1"

    # token过期时间8小时
    ACCESS_TOKEN_EXPIRE = 60 * 60 * 8

    # 算法
    ALGORITHM = "HS256"

    # 产品名称
    PRODUCTION_NAME = "gatherone_advertiser"

    # 账户重命名处理时间
    RENAME_EXPIRATION: Optional[str] = Field(480, env="DEV_RENAME_EXPIRATION")  # 默认480

    # REDIS存储
    REDIS_STORAGE = {
        'workbench': 7,  # 工作台缓存
        'sms_code': 7,  # 手机验证码
        'medium_account': 6,  # 账户信息缓存
        'accredit_account': 7  # 授权账户id
    }

    # 加载.env文件的配置
    class Config:
        env_file = ".env"
        case_sensitive = True


class DevConfig(Settings):
    """Development configurations."""

    # MQ
    MQ_HOST: Optional[str] = Field(None, env="DEV_MQ_HOST")
    MQ_PORT: Optional[str] = Field(None, env="DEV_MQ_PORT")
    MQ_USERNAME: Optional[str] = Field(None, env="DEV_MQ_USERNAME")
    MQ_PASSWORD: Optional[str] = Field(None, env="DEV_MQ_PASSWORD")
    MQ_VIRTUAL_HOST: Optional[str] = Field(None, env="DEV_MQ_VIRTUAL_HOST")

    # Redis
    REDIS_HOST: Optional[str] = Field(None, env="DEV_REDIS_HOST")
    REDIS_PORT: Optional[int] = Field(None, env="DEV_REDIS_PORT")
    REDIS_USERNAME: Optional[str] = Field(None, env="DEV_REDIS_USERNAME")
    REDIS_PASSWORD: Optional[str] = Field(None, env="DEV_REDIS_PASSWORD")

    # Mysql
    MYSQL_SERVER: Optional[str] = Field(None, env="DEV_MYSQL_SERVER")
    MYSQL_USER: Optional[str] = Field(None, env="DEV_MYSQL_USER")
    MYSQL_PASSWORD: Optional[str] = Field(None, env="DEV_MYSQL_PASSWORD")
    MYSQL_DB_NAME: Optional[str] = Field(None, env="DEV_MYSQL_DB_NAME")
    MYSQL_PORT: Optional[int] = Field(None, env="DEV_MYSQL_PORT")

    # 阿里云
    ACCESSKEY_ID: Optional[str] = Field(None, env="DEV_ACCESSKEY_ID")
    ACCESSKEY_SECRET: Optional[str] = Field(None, env="DEV_ACCESSKEY_SECRET")
    BUCKET_NAME: Optional[str] = Field(None, env="DEV_BUCKET_NAME")
    END_POINT: Optional[str] = Field(None, env="DEV_END_POINT")
    TEMPLATE_CODE: Optional[str] = Field(None, env="DEV_TEMPLATE_CODE")
    ALIOSS_URL: Optional[str] = Field(None, env="DEV_ALIOSS_URL")
    OSS_PREFIX: Optional[str] = Field(None, env="DEV_OSS_PREFIX")
    DOMESTIC_TEMPLATE_ID: Optional[str] = Field(None, env="DEV_DOMESTIC_TEMPLATE_ID")
    FOREIGN_TEMPLATE_ID: Optional[str] = Field(None, env="DEV_FOREIGN_TEMPLATE_ID")

    # API web服务
    MAPI_KEY: Optional[str] = Field(None, env="DEV_MAPI_KEY")

    # Consul
    CONSUL_HOST: Optional[str] = Field(None, env="DEV_CONSUL_HOST")
    CONSUL_PORT: Optional[int] = Field(None, env="DEV_CONSUL_PORT")


class ProdConfig(Settings):
    """Production configurations."""

    # MQ
    MQ_HOST: Optional[str] = Field(None, env="PROD_MQ_HOST")
    MQ_PORT: Optional[str] = Field(None, env="PROD_MQ_PORT")
    MQ_USERNAME: Optional[str] = Field(None, env="PROD_MQ_USERNAME")
    MQ_PASSWORD: Optional[str] = Field(None, env="PROD_MQ_PASSWORD")
    MQ_VIRTUAL_HOST: Optional[str] = Field(None, env="PROD_MQ_VIRTUAL_HOST")

    # Redis
    REDIS_HOST: Optional[str] = Field(None, env="PROD_REDIS_HOST")
    REDIS_PORT: Optional[int] = Field(None, env="PROD_REDIS_PORT")
    REDIS_USERNAME: Optional[str] = Field(None, env="PROD_REDIS_USERNAME")
    REDIS_PASSWORD: Optional[str] = Field(None, env="PROD_REDIS_PASSWORD")

    # Mysql
    MYSQL_SERVER: Optional[str] = Field(None, env="PROD_MYSQL_SERVER")
    MYSQL_USER: Optional[str] = Field(None, env="PROD_MYSQL_USER")
    MYSQL_PASSWORD: Optional[str] = Field(None, env="PROD_MYSQL_PASSWORD")
    MYSQL_DB_NAME: Optional[str] = Field(None, env="PROD_MYSQL_DB_NAME")
    MYSQL_PORT: Optional[int] = Field(None, env="PROD_MYSQL_PORT")

    # 阿里云
    ACCESSKEY_ID: Optional[str] = Field(None, env="PROD_ACCESSKEY_ID")
    ACCESSKEY_SECRET: Optional[str] = Field(None, env="PROD_ACCESSKEY_SECRET")
    BUCKET_NAME: Optional[str] = Field(None, env="PROD_BUCKET_NAME")
    END_POINT: Optional[str] = Field(None, env="PROD_END_POINT")
    TEMPLATE_CODE: Optional[str] = Field(None, env="PROD_TEMPLATE_CODE")
    ALIOSS_URL: Optional[str] = Field(None, env="PROD_ALIOSS_URL")
    OSS_PREFIX: Optional[str] = Field(None, env="PROD_OSS_PREFIX")
    DOMESTIC_TEMPLATE_ID: Optional[str] = Field(None, env="PROD_DOMESTIC_TEMPLATE_ID")
    FOREIGN_TEMPLATE_ID: Optional[str] = Field(None, env="PROD_FOREIGN_TEMPLATE_ID")

    # API web服务
    MAPI_KEY: Optional[str] = Field(None, env="PROD_MAPI_KEY")

    # Consul
    CONSUL_HOST: Optional[str] = Field(None, env="PROD_CONSUL_HOST")
    CONSUL_PORT: Optional[int] = Field(None, env="PROD_CONSUL_PORT")


class FactoryConfig:
    """Returns a config instance dependending on the ENV_STATE variable."""

    def __init__(self, env_state: Optional[str]):
        self.env_state = env_state

    def __call__(self):
        if self.env_state == "development":
            return DevConfig()

        elif self.env_state == "production":
            return ProdConfig()


@lru_cache()
def get_configs():
    """加载一下环境文件"""
    from dotenv import load_dotenv

    load_dotenv(encoding="utf-8")
    return FactoryConfig(Settings().ENVIRONMENT)()


configs = get_configs()
