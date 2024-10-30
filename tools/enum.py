# -*- coding: utf-8 -*-
from enum import Enum


class BaseEnum(Enum):
    def __init__(self, value, desc):
        super().__init__()
        self._value_ = value
        self.desc = desc

    @classmethod
    def dicts(cls):
        _enum_dict = {}
        for member in cls:
            _enum_dict[member.value] = member.desc
        return _enum_dict

    @classmethod
    def values(cls):
        _enum_values = []
        for member in cls:
            _enum_values.append(member.value)
        return _enum_values

    @classmethod
    def descs(cls):
        _enum_descs = []
        for member in cls:
            _enum_descs.append(member.desc)
        return _enum_descs

    def dict(self):
        _enum_dict = {}
        _enum_dict[self._value_] = self.desc
        return _enum_dict


class HttpEnum(BaseEnum):
    SUCCESS = (0, "成功")
    FAIL = (500, "服务器异常")


if __name__ == "__main__":
    print(HttpEnum.SUCCESS.value)
    print(type(HttpEnum.SUCCESS.value))
    print(HttpEnum.SUCCESS.desc)
    print(type(HttpEnum.SUCCESS.desc))
    print(HttpEnum.FAIL.value)
    print(type(HttpEnum.FAIL.value))
    print(HttpEnum.FAIL.desc)
    print(type(HttpEnum.FAIL.desc))
    print(HttpEnum.dicts())
    print(type(HttpEnum.dicts()))
    print(HttpEnum.values())
    print(type(HttpEnum.values()))
    print(HttpEnum.descs())
    print(type(HttpEnum.descs()))
    print(HttpEnum.SUCCESS.dict())
    print(type(HttpEnum.SUCCESS.dict()))
