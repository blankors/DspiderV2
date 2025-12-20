import logging
from minio import Minio
from minio.error import S3Error
from typing import Optional, Dict, Any, Union, IO
import io
import time

from dspider.common.load_config import config

logger = logging.getLogger(__name__)

class MinIOService:
    """MinIO客户端管理类"""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False):
        """初始化MinIO客户端
        
        Args:
            endpoint: MinIO服务地址，格式为 "host:port"
            access_key: 访问密钥
            secret_key: 秘密密钥
            secure: 是否使用HTTPS
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.client: Optional[Minio] = None
        
        # 初始化客户端
        self.initialize_client()
    
    def initialize_client(self) -> None:
        """初始化MinIO客户端实例"""
        try:
            self.client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
            logger.info(f"成功初始化MinIO客户端: {self.endpoint}")
        except Exception as e:
            logger.error(f"初始化MinIO客户端失败: {str(e)}")
            self.client = None
    
    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """确保存储桶存在，如果不存在则创建
        
        Args:
            bucket_name: 存储桶名称
            
        Returns:
            bool: 是否成功
        """
        if not self.client:
            logger.error("MinIO客户端未初始化")
            return False
        
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"成功创建存储桶: {bucket_name}")
            return True
        except S3Error as e:
            logger.error(f"操作存储桶失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"确保存储桶存在失败: {str(e)}")
            return False
    
    def upload_text(self, bucket_name: str, object_name: str, text: str, content_type: str = "text/plain") -> bool:
        """上传文本内容到MinIO
        
        Args:
            bucket_name: 存储桶名称
            object_name: 对象名称
            text: 要上传的文本内容
            content_type: 内容类型
            
        Returns:
            bool: 是否成功
        """
        if not self.client:
            logger.error("MinIO客户端未初始化")
            return False
        
        try:
            # 确保存储桶存在
            if not self.ensure_bucket_exists(bucket_name):
                return False
            
            # 将文本转换为字节流
            data = io.BytesIO(text.encode('utf-8'))
            length = len(text.encode('utf-8'))
            
            # 上传对象
            self.client.put_object(
                bucket_name,
                object_name,
                data=data,
                length=length,
                content_type=content_type
            )
            
            logger.info(f"成功上传文本到MinIO: {bucket_name}/{object_name}")
            return True
        except S3Error as e:
            logger.error(f"上传文本到MinIO失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"上传文本到MinIO时发生未知错误: {str(e)}")
            return False
    
    def upload_file(self, bucket_name: str, object_name: str, file_path: str, content_type: Optional[str] = None) -> bool:
        """上传本地文件到MinIO
        
        Args:
            bucket_name: 存储桶名称
            object_name: 对象名称
            file_path: 本地文件路径
            content_type: 内容类型
            
        Returns:
            bool: 是否成功
        """
        if not self.client:
            logger.error("MinIO客户端未初始化")
            return False
        
        try:
            # 确保存储桶存在
            if not self.ensure_bucket_exists(bucket_name):
                return False
            
            # 上传文件
            self.client.fput_object(
                bucket_name,
                object_name,
                file_path,
                content_type=content_type
            )
            
            logger.info(f"成功上传文件到MinIO: {bucket_name}/{object_name}")
            return True
        except S3Error as e:
            logger.error(f"上传文件到MinIO失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"上传文件到MinIO时发生未知错误: {str(e)}")
            return False
    
    def get_text(self, bucket_name: str, object_name: str) -> Optional[str]:
        """从MinIO获取文本内容
        
        Args:
            bucket_name: 存储桶名称
            object_name: 对象名称
            
        Returns:
            Optional[str]: 获取的文本内容，如果失败则返回None
        """
        if not self.client:
            logger.error("MinIO客户端未初始化")
            return None
        
        try:
            # 获取对象
            response = self.client.get_object(bucket_name, object_name)
            
            # 读取内容
            content = response.read().decode('utf-8')
            response.close()
            response.release_conn()
            
            logger.info(f"成功从MinIO获取文本: {bucket_name}/{object_name}")
            return content
        except S3Error as e:
            logger.error(f"从MinIO获取文本失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"从MinIO获取文本时发生未知错误: {str(e)}")
            return None
