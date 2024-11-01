# 使用Python 3.10作为基础镜像
FROM crpi-699ngo3okljkgvuf.ap-southeast-1.personal.cr.aliyuncs.com/base-go/test-base-advertiser:2024-10-31-13-40-49

# 以下是基础镜像包含内容 
# 设置 python 环境变量
# ENV TZ=Asia/Shanghai
# ENV PYTHONDONTWRITEBYTECODE=1
# ENV PYTHONUNBUFFERED=1

# 设置工作目录
# ENV APP_HOME=/app
# ENV PYTHONPATH="${PYTHONPATH}:$APP_HOME"
# 
# RUN mkdir -p $APP_HOME
# WORKDIR $APP_HOME
# 
# RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 安装项目依赖
# COPY requirements.txt /app
# RUN  pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 代码更新
COPY . /app
