# 使用Python 3.10作为基础镜像
FROM python:3.10.0

# 镜像作者 MAINTAINER Jaylen

# 设置 python 环境变量
ENV TZ=Asia/Shanghai
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 设置工作目录
ENV APP_HOME=/app
ENV PYTHONPATH="${PYTHONPATH}:$APP_HOME"

RUN mkdir -p $APP_HOME
WORKDIR $APP_HOME

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 安装项目依赖
COPY requirements.txt /app
RUN  pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 代码更新
COPY . /app
