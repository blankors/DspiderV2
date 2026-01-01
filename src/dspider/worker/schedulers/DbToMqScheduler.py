from typing import List
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod

from dspider.common.rabbitmq_service import RabbitMQService
from dspider.common.mongodb_service import MongoDBService
from dspider.common.load_config import config
from dspider.worker.scheduler_manager import SchedulerManager, Scheduler

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    handlers=[logging.StreamHandler()]
                    )
logger = logging.getLogger(__name__)

class DbToMqScheduler(Scheduler):
    def __init__(
        self, 
        scheduler_manager: SchedulerManager,
        name, interval_seconds: int, batch_size: int,
        db: dict, mq: dict,
    ):
        super().__init__(scheduler_manager, name, interval_seconds, batch_size)

        self.rabbitmq_service = scheduler_manager.rabbitmq_service
        self.mongodb_service = scheduler_manager.mongodb_service
        
        self.queue_name = mq['queue_name']
    
    def send_batch_to_queue(self, db_result: List[dict]):
        ids = [item['_id'] for item in db_result]
        data = self.construct_data_from_db_result(db_result)
        self.rabbitmq_service.publish_workqueue(
            message=data,
            queue_name=self.queue_name
        )
        self.update_db_status(ids)
    
    def construct_data_from_db_result(self, db_result: List[dict]):
        data = {
            'data_batch': db_result,
            'data_batch_size': len(db_result),
        }
        return data
    
    def update_db_status(self, ids):
        pass
    
    def handle_update_failure(self, ids):
        pass

    def whether_next_round(self, round: int):
        pass
    
    def get_data_from_db(self):
        try:
            result = self.mongodb_service.find({'state': 0})
            logger.info(f'从数据库中获取到{len(result)}条数据')
            return result
        except Exception as e:
            logger.exception(f'从数据库中获取数据失败：{e}')
    
    def run(self):
        
        try:
            self.whether_next_round(round)
            db_result = self.get_data_from_db()
            if not db_result:
                logger.info('数据库中暂无数据，等待下轮')
                return
            
            self.send_batch_to_queue(db_result)
            
        except Exception as e:
            logger.exception(f'调度器主循环异常：{e}')