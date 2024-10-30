# -*- coding: utf-8 -*-
import calendar
from datetime import datetime, timedelta


# 获取日期
def get_date(date) -> tuple:
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    if date == "today":
        return today.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
    if date == "yesterday":
        return yesterday.strftime('%Y-%m-%d'), yesterday.strftime('%Y-%m-%d')
    if date == "last_7d":
        last_7d = today - timedelta(days=7)
        return last_7d.strftime('%Y-%m-%d'), yesterday.strftime('%Y-%m-%d')
    if date == "last_14d":
        last_14d = today - timedelta(days=14)
        return last_14d.strftime('%Y-%m-%d'), yesterday.strftime('%Y-%m-%d')
    if date == "last_30d":
        last_30d = today - timedelta(days=30)
        return last_30d.strftime('%Y-%m-%d'), yesterday.strftime('%Y-%m-%d')
    if date == "this_week":
        # 计算当前日期是星期几（0代表星期一，6代表星期日）
        current_weekday = today.weekday()
        # 计算本周的起始日期和结束日期
        start_this_week = today - timedelta(days=current_weekday)
        end_this_week = today
        return start_this_week.strftime('%Y-%m-%d'), end_this_week.strftime('%Y-%m-%d')
    if date == "last_week":
        current_weekday = today.weekday()
        # 计算本周的起始日期和结束日期
        start_last_week = today - timedelta(days=current_weekday + 7)
        end_last_week = today - timedelta(days=current_weekday + 1)
        return start_last_week.strftime('%Y-%m-%d'), end_last_week.strftime('%Y-%m-%d')
    if date == "this_month":
        # 获取本月的第一天
        start_this_month = today.replace(day=1)
        return start_this_month.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
    if date == "last_month":
        # 计算上个月的年份和月份
        last_month_year = today.year if today.month > 1 else today.year - 1
        last_month = today.month - 1 if today.month > 1 else 12
        # 获取上个月的天数
        last_month_days = calendar.monthrange(last_month_year, last_month)[1]
        # 计算上个月的起始日期和结束日期
        start_last_month = datetime(last_month_year, last_month, 1)
        end_last_month = datetime(last_month_year, last_month, last_month_days)
        return start_last_month.strftime('%Y-%m-%d'), end_last_month.strftime('%Y-%m-%d')


if __name__ == "__main__":
    print(get_date('today'))
