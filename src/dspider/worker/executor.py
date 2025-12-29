import uuid
from importlib import import_module
from pkgutil import iter_modules
from typing import Optional, Dict, Any
from types import ModuleType

from dspider.worker import DataSourceManager, data_source_type
import logging

def walk_modules(path: str) -> list[ModuleType]:
    """Loads a module and all its submodules from the given module path and
    returns them. If *any* module throws an exception while importing, that
    exception is thrown back.

    For example: walk_modules('scrapy.utils')
    """

    mods: list[ModuleType] = []
    mod = import_module(path)
    mods.append(mod)
    if hasattr(mod, "__path__"):
        for _, subpath, ispkg in iter_modules(mod.__path__):
            fullpath = path + "." + subpath
            if ispkg:
                mods += walk_modules(fullpath)
            else:
                submod = import_module(fullpath)
                mods.append(submod)
    return mods

class Executor:
    def __init__(self, spider_name: str, task_config, rabbitmq_service, mongodb_service, minio_service):
        self.executor_id = str(uuid.uuid4())[:8]
        self.task_config = task_config
        self.spider_name = spider_name
        self.spider_config = self.task_config['spider'][spider_name]
        
        self.rabbitmq_service = rabbitmq_service
        self.queue_name = self.spider_config['queue_name']
        self.prefetch_count = self.spider_config['prefetch_count']
        
        self.mongodb_service = mongodb_service
        self.minio_service = minio_service
        
        self.logger = logging.getLogger(f"Executor-{self.executor_id}")
        self.logger.info(f"[{self.executor_id}] 初始化Executor for spider {self.spider_name}")
        
        worker_modules = walk_modules('dspider.worker.spider')
        for mod in worker_modules:
            if hasattr(mod, self.spider_name):
                self.spider_class = getattr(mod, self.spider_name)
                self.spider = self.spider_class(self)
                break
        else:
            raise ImportError(f"Spider {self.spider_name} not found in any module")
        
        self.executor_id = str(uuid.uuid4())[:8]
    
    def run(self):
        self.logger.info(f"[{self.executor_id}] Worker节点开始运行")
        try:
            self.rabbitmq_service.consume_messages(
                self.queue_name,
                callback=self.process,
                auto_ack=False,
                prefetch_count=self.prefetch_count
            )
        except KeyboardInterrupt:
            self.logger.info(f"[{self.executor_id}] 用户中断，停止Executor")
            raise
        except Exception as e:
            self.logger.error(f"[{self.executor_id}] 运行时错误: {str(e)}")
            raise
    
    def process(self, external_datasource_config: Dict[str, Any], properties: Dict[str, Any]) -> bool:
        self.logger.info(f"[{self.executor_id}] 收到任务: {external_datasource_config.get('_id', 'unknown')}")
        try:
            self.spider.start(external_datasource_config)
        except Exception as e:
            self.logger.error(f"[{self.executor_id}] 处理任务时出错: {str(e)}")
            return False
        else:
            return True