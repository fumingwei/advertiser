# -*- coding: utf-8 -*-
from typing import List, Dict
from pydantic import BaseModel, constr


class OnCompleteSchema(BaseModel):
    request_id: constr()
    operation_type: constr()
    data: List[Dict]

