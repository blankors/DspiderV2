from typing import List
from typing import TYPE_CHECKING
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod

from dspider.common.rabbitmq_service import RabbitMQService
from dspider.common.mongodb_service import MongoDBService
from dspider.common.load_config import config

if TYPE_CHECKING:
    from dspider.worker.scheduler_manager import SchedulerManager

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    handlers=[logging.StreamHandler()]
                    )
logger = logging.getLogger(__name__)

class SchedulerManagerTEMP:
    def __init__(self, rabbitmq_service: RabbitMQService, mongodb_service: MongoDBService):
        pass
    
    def start(self):
        with ThreadPoolExecutor as xxx:
            xxx.submit()
            
    def env_init(self):
        # 1.不同‘交换机类型’影响整个类的逻辑，所以交换机类型是与这个类绑定的，所以这里写死了
        # self.rabbitmq_service.declare_exchange(
        #     exchange_name='external_datasource_config', 
        #     exchange_type='direct', 
        #     durable=True
        # )
        queues = [
            {
                'queue_name': 'list_page',
                'durable': True,
                'auto_delete': False,
            },
            {
                'queue_name': 'list_page',
                'durable': True,
                'auto_delete': False,
            }
        ]
        for queue in queues:
            self.rabbitmq_service.declare_queue(
                queue_name=queue['queue_name'],
                durable=queue.get('durable', True),
                auto_delete=queue.get('auto_delete', False),
            )
            # self.rabbitmq_service.bind_queue(
            #     queue_name=queue['queue_name'],
            #     exchange_name='external_datasource_config',
            #     routing_key=queue['queue_name'],
            # )


class Scheduler(ABC):
    def __init__(
        self, 
        scheduler_manager: 'SchedulerManager', 
        name: str, interval_seconds: int, batch_size: int
    ):
        self.scheduler_manager = scheduler_manager
        self.name = name
        self.interval_seconds = interval_seconds
        self.batch_size = batch_size
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        """启动调度器线程"""
        if self._thread and self._thread.is_alive():
            logger.warning(f"[{self.name}] Already running.")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(f"[{self.name}] Started.")

    def _run_loop(self):
        """内部循环，定期调用 run()"""
        while not self._stop_event.wait(timeout=self.interval_seconds):
            try:
                self.run()
            except Exception as e:
                logger.exception(f"[{self.name}] Error in run(): {e}")

    @abstractmethod
    def run(self):
        """子类必须实现：执行一次调度任务"""
        pass

    def stop(self):
        """停止调度器"""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info(f"[{self.name}] Stopped.")
