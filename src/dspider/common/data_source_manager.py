import logging
from enum import Enum

from dspider.common.rabbitmq_service import RabbitMQService
from dspider.common.mongodb_service import MongoDBService
from dspider.common.minio_service import MinIOService
from dspider.common.load_config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class data_source_type(Enum):
    """数据源类型枚举"""
    MONGODB = 'mongodb'
    MINIO = 'minio'
    RABBITMQ = 'rabbitmq'

class DataSourceManager:
    """数据源管理类"""
    def __init__(self):
        self.data_sources = {}
        self.data_source_types = {}
        self.register_data_source_type('mongodb', MongoDBService)
        self.register_data_source_type('minio', MinIOService)
        self.register_data_source_type('rabbitmq', RabbitMQService)
    
    def register_data_source_type(self, data_source_type: str, data_source_class: type):
        """注册数据源类型"""
        self.data_source_types[data_source_type] = data_source_class
        
    def create_data_source(self, data_source_type: str, **kwargs):
        """创建数据源实例"""
        data_source_class = self.data_source_types.get(data_source_type)
        if not data_source_class:
            raise ValueError(f"未注册数据源类型: {data_source_type}")
        return data_source_class(**kwargs)
    
    def get_data_source_with_config(self, data_source_type: str):
        """根据配置获取数据源实例"""
        data_source_config = config.get(data_source_type)
        if not data_source_config:
            raise ValueError(f"未配置数据源: {data_source_type}")
        return self.create_data_source(data_source_type, **data_source_config)