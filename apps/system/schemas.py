# -*- coding: utf-8 -*-
from pydantic import BaseModel, Field


class AllotRoleModel(BaseModel):
    role_id: int
    permissions: list[int]
