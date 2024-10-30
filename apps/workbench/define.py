# -*- coding: utf-8 -*-
from apps.workbench.utils import get_date
from tools.enum import BaseEnum


class Date(BaseEnum):
    TODAY = ("today", get_date('today'))
    YESTERDAY = ("yesterday", get_date('yesterday'))
    LAST_7D = ("last_7d", get_date('last_7d'))
    LAST_14D = ("last_14d", get_date('last_14d'))
    LAST_30D = ("last_30d", get_date('last_30d'))
    THIS_WEEK = ("this_week", get_date('this_week'))
    LAST_WEEK = ("last_week", get_date('last_week'))
    THIS_MONTH = ("this_month", get_date('this_month'))
    LAST_MONTH = ("last_month", get_date('last_month'))
