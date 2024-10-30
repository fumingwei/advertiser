# -*- coding: utf-8 -*-
import sys
import threading
from math import ceil
import redis
from fastapi.encoders import jsonable_encoder
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    func,
    DateTime,
    Boolean,
)
from sqlalchemy.engine import Row
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm import declarative_base
from pymysql import converters, FIELD_TYPE
from settings.base import configs
from settings.log import web_log

conv = converters.conversions
conv[FIELD_TYPE.NEWDECIMAL] = float  # convert decimals to float
conv[FIELD_TYPE.DATE] = str  # convert dates to strings
conv[FIELD_TYPE.TIMESTAMP] = str  # convert dates to strings
conv[FIELD_TYPE.DATETIME] = str  # convert dates to strings
conv[FIELD_TYPE.TIME] = str  # convert dates to strings

# # Mysql数据库链接
SQLALCHEMY_DATABASE_URL = "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4".format(
    configs.MYSQL_USER,
    configs.MYSQL_PASSWORD,
    configs.MYSQL_SERVER,
    configs.MYSQL_PORT,
    configs.MYSQL_DB_NAME,
)

# """
# echo 标志是设置SQLAlchemy日志记录的快捷方式。 启用它后，我们将看到所有生成的SQL。
# Max_overflow 当连接池里的连接数已达到，pool_size时，且都被使用时。又要求从连接池里获取连接时，max_overflow就是允许再新建的连接数。
# create_engine() 的返回值是一个实例引擎,它代表了一个数据库的核心接口。此时的连接是惰性的，当create_engine()第一次返回的引擎，其实并没有试图连
# 接到数据库之中; 只有在第一次要求它对数据库执行任务时才会发生这种情况。
# """
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=20,
    max_overflow=20,
    echo=False,
    pool_recycle=3600,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def create_tb():
    """
    在sqlalchemy中创建元数据用来存储应用中所有数据表。通常一个应用中只有一个元数据。
    """
    # 删除所有表
    # Base.metadata.drop_all()
    # 创建所有表
    # Base.metadata.create_all()
    # base = automap_base(metadata=Base.metadata)
    # base.prepare()
    # all_models = base.classes
    # for model_name, model_class in all_models.items():
    #     print(model_name, model_class.__name__)
    from apps.accounts.models import account_models
    from apps.finance.models import finance_models
    from apps.onboarding.models import onboarding_models
    from apps.system.models import system_models

    for t in [
        *account_models,
        *finance_models,
        *onboarding_models,
        *system_models
        # 其他模块模型类
    ]:
        Base.metadata.create_all(tables=[t.__table__])


async def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


"""
创建基类
"""
Base = declarative_base(engine)


class BaseModel(Base):
    __abstract__ = True

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    created_time = Column(
        DateTime(timezone=True), default=func.now(), nullable=False, comment="创建时间"
    )
    update_time = Column(
        DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        onupdate=func.now(),
        comment="更新时间",
    )
    is_delete = Column(Boolean, default=False, nullable=False, comment="逻辑删除")

    def to_dict(self, db=None):
        model_dict = dict(self.__dict__)
        del model_dict["_sa_instance_state"]
        return model_dict

    Base.to_dict = to_dict

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for attr_name, attr_value in cls.__dict__.items():
            if isinstance(attr_value, Column):
                attr_value.nullable = False

    # 单个对象
    def single_to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    # 多个对象
    def many_to_dict(self):
        result = {}
        for key in self.__mapper__.c.keys():
            if getattr(self, key):
                result[key] = str(getattr(self, key))
            else:
                result[key] = getattr(self, key)
        return result

    @staticmethod
    def to_json(all_vendors):
        result = [ven.many_to_dict() for ven in all_vendors]
        return result

    @staticmethod
    def bulk_update(db: Session, model_class, list_dict):
        """

        :param db:
        :param model_class:模型名称
        :param list_dict:list[dict]
        :return:
        """
        db.bulk_update_mappings(model_class, list_dict)
        db.commit()

    @staticmethod
    def bulk_insert(db: Session, model_class, list_dict):
        """

        :param db:
        :param model_class:模型名称
        :param list_dict:list[dict]
        :return:
        """
        db.bulk_insert_mappings(model_class, list_dict)
        db.commit()

    # 复杂行转字典
    @staticmethod
    def mazy_to_dict(row):
        res = {}
        fast_json: dict = jsonable_encoder(row)
        for key, value in fast_json.items():
            if isinstance(value, (dict,)):
                res.update(value)
                continue
            res.update({key: value})
        return res


class RedisClient:
    _instance_lock = threading.Lock()

    def __init__(
            self,
            host=configs.REDIS_HOST,
            port=configs.REDIS_PORT,
            username=configs.REDIS_USERNAME,
            password=configs.REDIS_PASSWORD,
            db=configs.REDIS_STORAGE.get('medium_account'),  # 默认
    ):
        try:
            self.pool = redis.ConnectionPool(
                host=host,
                port=port,
                username=username,
                password=password,
                db=db,
                max_connections=100,
                decode_responses=True,  # redis 取出的结果默认是字节，设定 decode_responses=True 改成字符串
            )
        except Exception as e:
            web_log.log_error(e.__str__())
            sys.exit(0)

    def __new__(cls, *args, **kwargs):
        if not hasattr(RedisClient, "_instance"):
            with RedisClient._instance_lock:
                if not hasattr(RedisClient, "_instance"):
                    RedisClient._instance = object.__new__(cls)
        return RedisClient._instance

    def get_redis_client(self):
        try:
            # 从连接池中获取一个连接
            redis_coon = redis.Redis(connection_pool=self.pool)
            if redis_coon.ping():
                return redis_coon
        except Exception as e:
            web_log.log_error(f"redis连接异常:{e.__str__()}")
            return None


# 获取单个对象
async def get_obj(db, model, pk):
    obj = db.query(model).get(pk)
    return obj


# 获取redis链接
def get_redis_connection(conn_name: str):
    db = configs.REDIS_STORAGE.get(conn_name)
    redis_connection = RedisClient(db=db).get_redis_client()
    return redis_connection


# 分页器
class MyPagination:
    def __init__(self, query, page: int = 1, page_size: int = 10):
        """
        初始化分页参数
        :param query: 查询对象
        :param page_size: 一页多少内容
        :param page: 第几页 1起
        """
        self.query = query
        self.page = page
        self.page_size = page_size

    @property
    def items(self):
        """
        分页后数据
        :return: [model row / Model]
        """
        if self.page > self.pages:
            return []
        offset_num = (self.page - 1) * self.page_size  # 计算偏移量
        return self.query.limit(self.page_size).offset(offset_num).all()

    @staticmethod
    def _to_dict(piece):
        res = {}
        if isinstance(piece, Row):
            for key, value in piece._mapping.items():
                res.update(value.to_dict()) if isinstance(value, Base) else res.update(
                    {key: value}
                )
            return res
        return piece.to_dict()

    @property
    def data(self):
        return [self._to_dict(piece) for piece in self.items]

    @property
    def counts(self):
        """
        总数据量
        :return: int
        """
        return self.query.count()

    @property
    def pages(self):
        """
        总页数
        :return: int
        """
        return ceil(self.counts / self.page_size)

    @property
    def next_num(self):
        """下一页"""
        next_num = self.page + 1
        if self.pages < next_num:
            return None
        return next_num

    @property
    def prev_num(self):
        """上一页"""
        prev_num = self.page - 1
        if prev_num < 1:
            return None
        return prev_num

    def iter_pages(self, left=2, right=2):
        """
        页面列表
        :param left:
        :param right:
        :return:
        """
        length = left + right + 1
        # 页数大于
        if self.page > self.pages:
            range_start = self.pages - length
            if range_start <= 0:
                range_start = 1
            return range(range_start, self.pages + 1)

        # 页数小于最少分页数
        if self.pages < length:
            return range(1, self.pages + 1)

        # 页数正常的情况下,至少大于 length 长度
        l_boundary, r_boundary = left + 1, self.pages - right + 1
        if l_boundary < self.page < r_boundary:
            return range(self.page - left, self.page + right + 1)
        if self.page <= left:
            return range(1, length + 1)
        return range(self.pages - length, self.pages + 1)


if __name__ == "__main__":
    create_tb()
