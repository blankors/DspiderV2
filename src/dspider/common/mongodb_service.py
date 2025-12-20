import pymongo
import logging
import time
from typing import Optional, Dict, Any, List

from dspider.common.load_config import config

logger = logging.getLogger(__name__)

class MongoDBService:
    """MongoDB连接管理类"""
    
    def __init__(self, host: str, port: int, username: Optional[str], password: Optional[str], db_name: str):
        """初始化MongoDB连接
        
        Args:
            host: MongoDB主机地址
            port: MongoDB端口
            username: 用户名（可选）
            password: 密码（可选）
            db_name: 数据库名称
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        self.client = None
        self.db = None
    
    def connect(self, max_retries: int = 3, retry_delay: int = 2) -> bool:
        """连接到MongoDB
        
        Args:
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            
        Returns:
            bool: 是否连接成功
        """
        for attempt in range(max_retries):
            try:
                if self.username and self.password:
                    self.client = pymongo.MongoClient(
                        host=self.host,
                        port=self.port,
                        username=self.username,
                        password=self.password,
                        authSource='admin'  # 指定使用admin数据库进行认证
                    )
                else:
                    self.client = pymongo.MongoClient(host=self.host, port=self.port)
                
                # 测试连接
                self.client.admin.command('ping')
                self.db = self.client[self.db_name]
                logger.info(f"成功连接到MongoDB: {self.host}:{self.port}/{self.db_name}")
                return True
            except Exception as e:
                logger.error(f"连接MongoDB失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        return False
    
    def disconnect(self):
        """断开MongoDB连接"""
        if self.client:
            self.client.close()
            logger.info("已断开MongoDB连接")
    
    def get_collection(self, collection_name: str):
        """获取集合
        
        Args:
            collection_name: 集合名称
            
        Returns:
            Collection: MongoDB集合对象
        """
        if self.db is None:
            logger.error("MongoDB未连接")
            return None
        return self.db[collection_name]
    
    def insert_one(self, collection_name: str, document: Dict[str, Any]) -> Optional[str]:
        """插入单条文档
        
        Args:
            collection_name: 集合名称
            document: 文档数据
            options: 插入选项（如指定_id）
            
        Returns:
            str: 插入的文档ID，失败返回None
        """
        # 如果指定了_id，将其添加到文档中
        if 'id' in document:
            document['_id'] = document['id']
        
        try:
            collection = self.get_collection(collection_name)
            if collection is not None:
                result = collection.insert_one(document)
                logger.info(f"成功插入文档到 {collection_name}")
                return str(result.inserted_id)
        except Exception as e:
            logger.error(f"插入文档失败: {str(e)}")
        return None
    
    def insert_many(self, collection_name: str, documents: List[Dict[str, Any]]) -> Optional[List[str]]:
        """批量插入文档
        
        Args:
            collection_name: 集合名称
            documents: 文档列表
            
        Returns:
            List[str]: 插入的文档ID列表，失败返回None
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is not None:
                result = collection.insert_many(documents)
                logger.info(f"成功插入 {len(documents)} 条文档到 {collection_name}")
                return [str(_id) for _id in result.inserted_ids]
        except Exception as e:
            logger.error(f"批量插入文档失败: {str(e)}")
        return None
    
    def find_one(self, collection_name: str, query: Dict[str, Any], projection: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """查找单条文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            projection: 投影条件
            
        Returns:
            Dict[str, Any]: 找到的文档，未找到返回None
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is not None:
                return collection.find_one(query, projection)
        except Exception as e:
            logger.error(f"查找文档失败: {str(e)}")
        return None
    
    def find(self, collection_name: str, query: Dict[str, Any], 
             projection: Optional[Dict[str, Any]] = None, 
             limit: int = 0, skip: int = 0) -> List[Dict[str, Any]]:
        """查找多条文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            projection: 投影条件
            limit: 返回数量限制
            skip: 跳过文档数
            
        Returns:
            List[Dict[str, Any]]: 文档列表
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is not None:
                cursor = collection.find(query, projection)
                if skip > 0:
                    cursor = cursor.skip(skip)
                if limit > 0:
                    cursor = cursor.limit(limit)
                return list(cursor)
        except Exception as e:
            logger.error(f"查询文档失败: {str(e)}")
        return []
    
    def update_one(self, collection_name: str, query: Dict[str, Any], 
                   update: Dict[str, Any]) -> bool:
        """更新单条文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            update: 更新内容
            
        Returns:
            bool: 是否更新成功
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is not None:
                result = collection.update_one(query, update)
                logger.info(f"更新文档结果: 匹配 {result.matched_count}, 修改 {result.modified_count}")
                return result.modified_count > 0
        except Exception as e:
            logger.error(f"更新文档失败: {str(e)}")
        return False
    
    def count_documents(self, collection_name: str, query: Dict[str, Any] = None) -> int:
        """统计文档数量
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            
        Returns:
            int: 文档数量
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is not None:
                return collection.count_documents(query or {})
        except Exception as e:
            logger.error(f"统计文档失败: {str(e)}")
        return 0
