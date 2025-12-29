import uuid
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import Dict, Any, Optional, List, Callable, Type, Set
import logging
from enum import Enum
from dataclasses import dataclass, field
import threading

from dspider.common.load_config import config as global_config
from dspider.common.rabbitmq_service import RabbitMQService
from dspider.worker.executor import Executor  # 添加导入

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.StreamHandler()
                    ])

class WorkerStatus(Enum):
    """工作节点状态枚举"""
    INITIALIZED = "initialized"
    RUNNING = "running"
    PAUSED = "paused"
    SHUTTING_DOWN = "shutting_down"
    SHUTDOWN = "shutdown"


@dataclass
class WorkerConfig:
    """工作节点配置数据类"""
    task_queue_name: str = "task_queue"
    prefetch_count: int = 10
    max_thread_pool_workers: int = 20
    retry_attempts: int = 3
    retry_delay: float = 2.0
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'WorkerConfig':
        """从字典创建配置"""
        return cls(
            task_queue_name=config_dict.get('task_queue_name', "task_queue"),
            prefetch_count=config_dict.get('prefetch_count', 10),
            max_thread_pool_workers=config_dict.get('max_thread_pool_workers', 20),
            retry_attempts=config_dict.get('retry_attempts', 3),
            retry_delay=config_dict.get('retry_delay', 2.0)
        )


class TaskProcessor:
    """任务处理器接口，用于扩展不同类型的任务处理逻辑"""
    def process(self, worker: 'Worker', task: Dict[str, Any]) -> bool:
        """处理任务
        
        Args:
            worker: 工作节点实例
            task: 任务数据
            
        Returns:
            bool: 是否成功处理
        """
        raise NotImplementedError("process method must be implemented")


class DefaultTaskProcessor(TaskProcessor):
    """默认任务处理器，用于处理爬虫任务"""
    def process(self, worker: 'Worker', task: Dict[str, Any]) -> bool:
        """处理爬虫任务
        
        Args:
            worker: 工作节点实例
            task: 任务数据
            
        Returns:
            bool: 是否成功处理
        """
        task_id = task.get('_id', str(uuid.uuid4()))
        
        try:
            spider_info: dict = task['spider']
            futures = []
            
            for spider_name, spider_config in spider_info.items():
                spider_p_num = spider_config['p_num']
                worker.logger.info(f"[{worker.worker_id}] 任务 {task_id}: 创建 {spider_p_num} 个线程处理 {spider_name}")
                
                # 使用工作节点的线程池提交任务
                for _ in range(spider_p_num):
                    future = worker.thread_pool.submit(worker._init_executor, spider_name, task)
                    futures.append(future)
            
            # 线程创建成功后
            # 1、立即ack
            # 2、立即return
            
            # 等待所有任务完成并处理结果
            # all_success = True
            # for future in as_completed(futures):
            #     try:
            #         result = future.result()
            #         if not result:
            #             all_success = False
            #     except Exception as e:
            #         worker.logger.error(f"[{worker.worker_id}] 任务执行异常: {str(e)}", exc_info=True)
            #         all_success = False
            
            # return all_success
        
        except KeyError as e:
            worker.logger.error(f"[{worker.worker_id}] 任务数据格式错误，缺少字段: {str(e)}")
            return False
        except Exception as e:
            worker.logger.error(f"[{worker.worker_id}] 处理任务 {task_id} 失败: {str(e)}", exc_info=True)
            return False


class Worker:
    """工作节点类，负责消费任务并创建执行器处理任务"""
    
    def __init__(self, 
                 rabbitmq_service: RabbitMQService,
                 config: dict,
                 task_processor: TaskProcessor):
        """初始化工作节点
        
        Args:
            rabbitmq_service: RabbitMQ服务实例（用于依赖注入）
            data_source_manager: 数据源管理器实例（用于依赖注入）
            config: 工作节点配置
            task_processor: 任务处理器（用于扩展任务处理逻辑）
        """
        self.worker_id = str(uuid.uuid4())[:8]
        
        # 状态管理
        self._status = WorkerStatus.INITIALIZED
            
        self.rabbitmq_service = rabbitmq_service
        
        self.config = config
        
        # 任务处理器（策略模式）
        self.task_processor = task_processor
        
        # 初始化线程池
        self.thread_pool = ThreadPoolExecutor(max_workers=self.config['max_thread_pool_workers'])
        
        # 任务跟踪
        self.active_tasks: Set[str] = set()
        self.task_futures: Dict[str, Future] = {}  # 任务ID到Future的映射
        self._tasks_lock = threading.RLock()  # 用于任务跟踪的锁
        
        self.logger = logging.getLogger(f"WorkerNode-{self.worker_id}")
        self.logger.info(f"[{self.worker_id}] 初始化完成，配置: {self.config}")
    
    @property
    def status(self) -> WorkerStatus:
        """获取工作节点状态"""
        return self._status
    
    def run(self):
        """启动工作节点，开始消费任务"""
        if self._status != WorkerStatus.INITIALIZED:
            self.logger.warning(f"[{self.worker_id}] 当前状态 {self._status.value} 不允许启动")
            return
        
        self._status = WorkerStatus.RUNNING
        
        try:
            self.logger.info(f"[{self.worker_id}] 开始运行，监听队列: {self.config['task_queue']['queue_name']}")
            self.rabbitmq_service.consume_messages(
                self.config['task_queue']['queue_name'],
                callback=self._on_message_received,
                auto_ack=False,
                prefetch_count=self.config['task_queue']['prefetch_count']
            )
        except KeyboardInterrupt:
            self.logger.info(f"[{self.worker_id}] 用户中断，准备退出")
            self._status = WorkerStatus.SHUTTING_DOWN
        except Exception as e:
            self.logger.error(f"[{self.worker_id}] 运行时错误: {str(e)}", exc_info=True)
            self._status = WorkerStatus.SHUTTING_DOWN
        finally:
            self._shutdown()
    
    def pause(self):
        """暂停工作节点"""
        if self._status == WorkerStatus.RUNNING:
            self.logger.info(f"[{self.worker_id}] 暂停工作节点")
            self._status = WorkerStatus.PAUSED
            # 实现暂停逻辑，例如停止消费新消息
            # self.rabbitmq_service.pause_consume()
    
    def resume(self):
        """恢复工作节点"""
        if self._status == WorkerStatus.PAUSED:
            self.logger.info(f"[{self.worker_id}] 恢复工作节点")
            self._status = WorkerStatus.RUNNING
            # 实现恢复逻辑，例如重新开始消费消息
            # self.rabbitmq_service.resume_consume()
    
    def shutdown(self):
        """关闭工作节点"""
        if self._status in [WorkerStatus.RUNNING, WorkerStatus.PAUSED]:
            self.logger.info(f"[{self.worker_id}] 关闭工作节点")
            self._status = WorkerStatus.SHUTTING_DOWN
            self._shutdown()
    
    def _shutdown(self):
        """内部关闭方法"""
        if self._status == WorkerStatus.SHUTDOWN:
            return
            
        self.logger.info(f"[{self.worker_id}] 开始关闭资源")
        
        # 取消所有任务
        self.cancel_all_tasks()
        
        # 关闭线程池
        self.thread_pool.shutdown(wait=True, cancel_futures=True)
        
        # 清理其他资源
        # self.rabbitmq_service.close()
        
        # 清空任务跟踪
        with self._tasks_lock:
            self.active_tasks.clear()
            self.task_futures.clear()
        
        self._status = WorkerStatus.SHUTDOWN
        self.logger.info(f"[{self.worker_id}] 已优雅退出")

    def _on_message_received(self, task: Dict[str, Any], properties: Dict[str, Any]) -> bool:
        """消息接收回调函数
        
        Args:
            task: 任务数据
            properties: 消息属性
            
        Returns:
            bool: 是否成功处理
        """
        if self._status != WorkerStatus.RUNNING:
            self.logger.warning(f"[{self.worker_id}] 当前状态 {self._status.value}，拒绝处理新任务")
            return False
        
        task_id = task.get('_id', str(uuid.uuid4()))
        self.logger.info(f"[{self.worker_id}] 收到任务: {task_id}")
        self.logger.debug(f"[{self.worker_id}] 任务详情: {task}")
        
        # 任务跟踪（线程安全）
        with self._tasks_lock:
            self.active_tasks.add(task_id)
        
        try:
            # 使用任务处理器处理任务（策略模式）
            success = self.task_processor.process(self, task)
            return success
        finally:
            # 从活跃任务列表中移除（线程安全）
            with self._tasks_lock:
                if task_id in self.active_tasks:
                    self.active_tasks.remove(task_id)
                if task_id in self.task_futures:
                    del self.task_futures[task_id]
    
    def _init_executor(self, spider_name: str, task_config: Dict[str, Any]) -> bool:
        """初始化执行器并运行爬虫
        
        Args:
            spider_name: 爬虫名称
            task_config: 任务配置
            
        Returns:
            bool: 是否成功执行
        """
        try:
            self.logger.info(f"[{self.worker_id}] 初始化Executor for spider {spider_name}")
            executor = Executor(spider_name, task_config)
            executor.run()
            return True
        except Exception as e:
            self.logger.error(f"[{self.worker_id}] 初始化或运行Executor {spider_name} 失败: {str(e)}", exc_info=True)
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取工作节点指标
        
        Returns:
            Dict[str, Any]: 工作节点指标
        """
        with self._tasks_lock:
            active_tasks_count = len(self.active_tasks)
            active_tasks_list = list(self.active_tasks)
        
        return {
            'worker_id': self.worker_id,
            'status': self._status.value,
            'active_tasks_count': active_tasks_count,
            'active_tasks': active_tasks_list,
            'config': {
                'task_queue_name': self.config['task_queue']['queue_name'],
                'prefetch_count': self.config['task_queue']['prefetch_count'],
                'max_thread_pool_workers': self.config['max_thread_pool_workers'],
                'retry_attempts': self.config['retry_attempts'],
                'retry_delay': self.config['retry_delay']
            },
            'thread_pool_size': self.config['max_thread_pool_workers']
        }
    
    def cancel_task(self, task_id: str) -> bool:
        """取消指定任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            bool: 是否成功取消
        """
        with self._tasks_lock:
            if task_id in self.task_futures:
                future = self.task_futures[task_id]
                if not future.done():
                    self.logger.info(f"[{self.worker_id}] 取消任务: {task_id}")
                    future.cancel()
                    self.active_tasks.discard(task_id)
                    del self.task_futures[task_id]
                    return True
        
        self.logger.warning(f"[{self.worker_id}] 无法取消任务: {task_id}（任务不存在或已完成）")
        return False
    
    def cancel_all_tasks(self) -> int:
        """取消所有正在运行的任务
        
        Returns:
            int: 成功取消的任务数量
        """
        canceled_count = 0
        
        with self._tasks_lock:
            task_ids = list(self.task_futures.keys())
            
        for task_id in task_ids:
            if self.cancel_task(task_id):
                canceled_count += 1
        
        self.logger.info(f"[{self.worker_id}] 已取消 {canceled_count} 个任务")
        return canceled_count

if __name__ == '__main__':
    worker = Worker(worker_id='test_worker', config=WorkerConfig())
    worker.start()