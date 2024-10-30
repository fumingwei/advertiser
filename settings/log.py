# -*- coding: utf-8 -*-
import datetime
import inspect
import logging
import os
import re
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import platform

class CustomerTimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, *args, **kwargs):
        super().__init__(filename, *args, **kwargs)
        if os.path.exists(filename):
            t = self.last_stat(filename)
        else:
            t = int(time.time())
        self.rolloverAt = self.computeRollover(t)

    def last_stat(self, filename):
        with open(filename, 'r', encoding="utf8") as file:
            first_line_str = file.readline()
        match = re.search(r'(\d{4}-\d{2}-\d{2})', first_line_str)
        if match:
            date_str = match.group(1)
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            t = date.timestamp()
        else:
            t = int(time.time())
        return t

class CustomerLog:
    level_relations = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critic": logging.CRITICAL
    }  # 日志级别关系映射

    def __init__(self, log_type):
        self.log_type = log_type
        # 获取Python版本号
        self.major_version = sys.version_info.major
        self.minor_version = sys.version_info.minor
        self.micro_version = sys.version_info.micro

    @staticmethod
    def log_with_location(logger, level, message, location_info):
        # 获取当前调用者的信息
        frame, filename, line_number, function_name, lines, index = location_info
        p = str(Path(filename))
        # 兼容Windows 路径分割
        filename_split = p.split('\\') if platform.system() == 'Windows' else filename.split('/')
        start = 0
        for index, val in enumerate(filename_split):
            if val.startswith('gatherone'):
                start = index
        filename = '/'.join(filename_split[start + 1:])
        # 格式化日志信息
        location_info = f"{filename}:{function_name}:{line_number}"
        logger.log(CustomerLog.level_relations.get(level), msg=f"{location_info} - {message}")

    def my_log(self, msg, location_info, level="error", when="D", back_count=15):
        """
        实例化 TimeRotatingFileHandler
        interval 是时间间隔， backupCount 是备份文件的个数，如果超过这个个数，就会自动删除，when 是间隔的时间单位，单位有以下几种
        S 秒
        M 分
        H 小时
        D 天
        每星期（interval == 0 时代表星期一
        midnight 每天凌晨
        """
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        log_path_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        logs_path = os.path.join(log_path_dir, f"logs/{self.log_type}" if self.log_type else "logs")
        if not os.path.exists(logs_path):
            os.makedirs(logs_path)

        my_logger = logging.getLogger(self.log_type)  # 定义日志收集器 my_logger
        my_logger.setLevel(self.level_relations.get(level))  # 设置日志级别

        format_str = logging.Formatter(
            "%(asctime)s-%(levelname)s-%(filename)s - 日志信息:%(message)s")  # 设置日志格式
        # 创建输出渠道
        sh = logging.StreamHandler()  # 往屏幕输出
        sh.setFormatter(format_str)  # 设置屏幕上显示的格式
        th = CustomerTimedRotatingFileHandler(filename=f'{logs_path}/{level}.log', when=when,
                                               backupCount=back_count, encoding="utf-8")
        # if level == "error":
        #     th = handlers.TimedRotatingFileHandler(filename=f'{logs_path}/{current}_{level}.log', when=when,
        #                                            backupCount=back_count, encoding="utf-8")
        # else:
        #     th = handlers.TimedRotatingFileHandler(filename=logs_path + "/{}_info.log".format(current), when=when,
        #                                            backupCount=back_count,
        #                                            encoding="utf-8")  # 往文件里写日志
        # 设置删除过期文件时正则表达式 (兼容python3.10.10以下版本/python3.10.10以上已调整)
        th.suffix = r"%Y-%m-%d.log"  # 日志采集根据文件后缀
        th.extMatch = re.compile(
            r"^log.\d{4}-\d{2}-\d{2}(\.\w+)?", re.ASCII)
        if (self.major_version, self.minor_version, self.micro_version) > (3, 10, 10):
            th.extMatch = re.compile(
                r"^\d{4}-\d{2}-\d{2}(\.\w+)?", re.ASCII)
        th.setFormatter(format_str)  # 设置文件里写入的格式
        my_logger.addHandler(sh)  # 将对象加入logger里
        my_logger.addHandler(th)

        self.log_with_location(my_logger, level, msg, location_info)

        my_logger.removeHandler(sh)
        my_logger.removeHandler(th)
        logging.shutdown()
        return my_logger

    def log_error(self, msg):
        # 获取当前调用者的信息
        location_info = inspect.getouterframes(inspect.currentframe())[1]
        self.my_log(msg, location_info, "error")

    def log_info(self, msg):
        # 获取当前调用者的信息
        location_info = inspect.getouterframes(inspect.currentframe())[1]
        self.my_log(msg, location_info, "info")

    def log_warning(self, msg):
        # 获取当前调用者的信息
        location_info = inspect.getouterframes(inspect.currentframe())[1]
        self.my_log(msg, location_info, "warning")

    def log_debug(self, msg):
        # 获取当前调用者的信息
        location_info = inspect.getouterframes(inspect.currentframe())[1]
        self.my_log(msg, location_info, "debug")

    def log_critical(self, msg):
        # 获取当前调用者的信息
        location_info = inspect.getouterframes(inspect.currentframe())[1]
        self.my_log(msg, location_info, "critic")


celery_log = CustomerLog(log_type='celery')
web_log = CustomerLog(log_type='web')
common_log = CustomerLog(log_type='common')
crm_tasks = CustomerLog(log_type='crm_tasks')
