# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER, ENUM, DATETIME, BOOLEAN
from apps.common.define import FileStatus
from settings.db import BaseModel


# 自助文件
class CuFile(BaseModel):
    __tablename__ = "cu_files"

    file_key = Column(VARCHAR(200), comment="文件key")
    file_name = Column(VARCHAR(100), comment="文件名")
    file_type = Column(VARCHAR(10), comment="文件类型")
    file_size = Column(VARCHAR(10), default="", comment="文件大小")
    file_status = Column(
        ENUM(*list(FileStatus.values())),
        default=FileStatus.PROCESS.value,
        comment="文件状态"
    )
    description = Column(VARCHAR(500), default="", comment="文件描述")
    expire_time = Column(DATETIME, nullable=True, comment='到期时间')
    upload_user_id = Column(INTEGER, comment="上传用户主键")
    download_status = Column(BOOLEAN, default=False, comment="下载状态")
    remark = Column(VARCHAR(1000), default="", comment="备注")
