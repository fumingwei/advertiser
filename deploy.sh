#!/bin/bash

# 检查是否提供参数
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 {start|restart|stop}"
  exit 1
fi

# 定义.env文件的路径
env_file=".env"

# 检查.env文件是否存在
if [ ! -f "$env_file" ]; then
  echo "Error: .env file not found."
  exit 1
fi

# 检查SERVER_VERSION参数是否存在
if ! grep -q '^SERVER_VERSION=' "$env_file"; then
  echo "Error: SERVER_VERSION parameter is missing in the .env file."
  exit 1
fi

# 根据提供的命令执行相应的docker-compose操作
case $1 in
    test)
        echo "Starting Docker Compose Test services..."
        docker-compose -f docker-compose-test.yml up
        ;;
    start)
        echo "Starting Docker Compose services..."
        docker-compose -f docker-compose.yml up -d
        ;;
    restart)
        echo "Restarting Docker Compose services..."
        docker-compose -f docker-compose.yml restart
        ;;
    stop)
        echo "Stopping Docker Compose services..."
        docker-compose -f docker-compose.yml down
        ;;
    *)
        echo "Invalid command. Usage: $0 {start|restart|stop}"
        exit 1
        ;;
esac

echo "Docker Compose operation '$1' completed."