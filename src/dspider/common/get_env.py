import os

# 配置
NACOS_SERVER_ADDRESSES = os.getenv("NACOS_SERVER_ADDRESSES", "localhost:8848")
SERVICE_NAME = os.getenv("SERVICE_NAME", "service1")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8001"))
SERVICE_IP = "127.0.0.1"  # 本地调试时使用本地IP