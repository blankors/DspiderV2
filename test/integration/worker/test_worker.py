import unittest
import uuid
import time
import threading
import socket
import pika
from typing import Dict, Any
from unittest.mock import Mock, patch

from dspider.worker.worker import Worker, WorkerStatus, DefaultTaskProcessor
from dspider.common.rabbitmq_service import RabbitMQService
from dspider.common.load_config import config


class WorkerIntegrationTest(unittest.TestCase):
    """Worker类的集成测试"""
    
    def setUp(self):
        """设置测试环境，创建真实的RabbitMQ连接"""
        # 获取测试配置
        self.rabbitmq_config = config['rabbitmq']
        self.worker_config = config['worker']
        self.test_queue_name = self.worker_config['task_queue']['queue_name']
        
        # 创建RabbitMQ服务实例
        self.rabbitmq_service = RabbitMQService(
            host=self.rabbitmq_config['host'],
            port=self.rabbitmq_config['port'],
            username=self.rabbitmq_config['username'],
            password=self.rabbitmq_config['password'],
            virtual_host=self.rabbitmq_config['virtualhost']
        )
        
        # 连接到RabbitMQ
        self.assertTrue(self.rabbitmq_service.connect(), "Failed to connect to RabbitMQ")
        
        # 声明测试队列
        self.assertTrue(
            self.rabbitmq_service.declare_queue(self.test_queue_name, durable=False, auto_delete=True),
            "Failed to declare test queue"
        )
        
        # 创建Worker实例
        self.worker = Worker(
            rabbitmq_service=self.rabbitmq_service,
            config=self.worker_config,
            task_processor=DefaultTaskProcessor()
        )
    
    def tearDown(self):
        """清理测试环境"""
        # 确保Worker已经关闭
        if self.worker.status != WorkerStatus.SHUTDOWN:
            self.worker.shutdown()
        
        # 清理队列，添加连接状态检查
        try:
            if (self.rabbitmq_service.connection and self.rabbitmq_service.connection.is_open and 
                self.rabbitmq_service.channel and self.rabbitmq_service.channel.is_open):
                self.rabbitmq_service.purge_queue(self.test_queue_name)
                self.rabbitmq_service.channel.queue_delete(queue=self.test_queue_name)
        except Exception as e:
            # 如果连接已关闭，忽略错误
            self.assertTrue(isinstance(e, (pika.exceptions.ChannelClosedByBroker, 
                                            pika.exceptions.ConnectionClosedByBroker, 
                                            pika.exceptions.StreamLostError)), 
                           f"Unexpected error during teardown: {str(e)}")
        
        # 断开RabbitMQ连接
        self.rabbitmq_service.disconnect()
    
    def test_worker_lifecycle(self):
        """测试Worker的完整生命周期"""
        # 测试初始状态
        self.assertEqual(self.worker.status, WorkerStatus.INITIALIZED)
        
        # 测试启动Worker（在后台线程中）
        def run_worker():
            try:
                self.worker.run()
            except KeyboardInterrupt:
                pass
        
        worker_thread = threading.Thread(target=run_worker)
        worker_thread.daemon = True
        worker_thread.start()
        
        # 等待Worker启动
        time.sleep(1)
        self.assertEqual(self.worker.status, WorkerStatus.RUNNING)
        
        # 测试暂停和恢复
        self.worker.pause()
        self.assertEqual(self.worker.status, WorkerStatus.PAUSED)
        
        self.worker.resume()
        self.assertEqual(self.worker.status, WorkerStatus.RUNNING)
        
        # 测试关闭
        self.worker.shutdown()
        time.sleep(0.5)  # 等待关闭完成
        self.assertEqual(self.worker.status, WorkerStatus.SHUTDOWN)
        
        worker_thread.join(timeout=1)  # 等待线程结束
    
    def test_task_processing(self):
        """测试Worker接收和处理任务"""
        # 创建一个自定义的任务处理器来捕获处理的任务
        class TestTaskProcessor(DefaultTaskProcessor):
            def __init__(self):
                super().__init__()
                self.processed_tasks = []
                
            def process(self, worker, task):
                self.processed_tasks.append(task)
                return True
        
        # 替换Worker的任务处理器
        test_processor = TestTaskProcessor()
        self.worker.task_processor = test_processor
        
        # 创建测试任务
        test_task = {
            "_id": str(uuid.uuid4()),
            "spider": {
                "test_spider": {
                    "p_num": 1,
                    "config": {"url": "https://example.com"}
                }
            }
        }
        
        # 启动Worker（在后台线程中）
        def run_worker():
            try:
                self.worker.run()
            except KeyboardInterrupt:
                pass
        
        worker_thread = threading.Thread(target=run_worker)
        worker_thread.daemon = True
        worker_thread.start()
        
        # 等待Worker启动
        time.sleep(1)
        self.assertEqual(self.worker.status, WorkerStatus.RUNNING)
        
        # 发布任务到队列
        self.rabbitmq_service.publish_workqueue(
            message=test_task,
            queue_name=self.test_queue_name
        )
        
        # 等待任务处理
        max_wait_time = 5
        start_time = time.time()
        while len(test_processor.processed_tasks) == 0:
            if time.time() - start_time > max_wait_time:
                break
            time.sleep(0.5)
        
        # 验证任务被处理
        self.assertGreater(len(test_processor.processed_tasks), 0)
        self.assertEqual(test_processor.processed_tasks[0], test_task)
        
        # 关闭Worker
        self.worker.shutdown()
        time.sleep(0.5)
        worker_thread.join(timeout=1)
    
    def test_metrics_collection(self):
        """测试Worker的指标收集功能"""
        # 启动Worker（在后台线程中）
        def run_worker():
            try:
                self.worker.run()
            except KeyboardInterrupt:
                pass
        
        worker_thread = threading.Thread(target=run_worker)
        worker_thread.daemon = True
        worker_thread.start()
        
        # 等待Worker启动
        time.sleep(1)
        self.assertEqual(self.worker.status, WorkerStatus.RUNNING)
        
        # 获取指标
        metrics = self.worker.get_metrics()
        
        # 验证指标内容
        self.assertEqual(metrics["worker_id"], self.worker.worker_id)
        self.assertEqual(metrics["status"], WorkerStatus.RUNNING.value)
        self.assertEqual(metrics["active_tasks_count"], 0)
        self.assertEqual(metrics["config"]["task_queue_name"], self.worker_config['task_queue']['queue_name'])
        self.assertEqual(metrics["config"]["max_thread_pool_workers"], self.worker_config['max_thread_pool_workers'])
        
        # 关闭Worker
        self.worker.shutdown()
        time.sleep(0.5)
        worker_thread.join(timeout=1)
    
    def test_executor_creation(self):
        """测试Worker创建和使用Executor的功能"""
        # 模拟Executor以避免实际运行爬虫
        original_init_executor = self.worker._init_executor
        
        def mock_init_executor(spider_name: str, task_config: Dict[str, Any]) -> bool:
            """模拟初始化执行器"""
            self.executor_called = True
            self.executor_spider_name = spider_name
            self.executor_task_config = task_config
            return True
        
        self.worker._init_executor = mock_init_executor
        self.executor_called = False
        
        # 创建测试任务
        test_task = {
            "_id": str(uuid.uuid4()),
            "spider": {
                "test_spider": {
                    "p_num": 1
                }
            }
        }
        
        # 启动Worker
        def run_worker():
            try:
                self.worker.run()
            except KeyboardInterrupt:
                pass
        
        worker_thread = threading.Thread(target=run_worker)
        worker_thread.daemon = True
        worker_thread.start()
        
        # 等待Worker启动
        time.sleep(1)
        
        # 发布任务
        self.rabbitmq_service.publish_workqueue(
            message=test_task,
            queue_name=self.test_queue_name
        )
        
        # 等待任务处理
        max_wait_time = 5
        start_time = time.time()
        while not self.executor_called:
            if time.time() - start_time > max_wait_time:
                break
            time.sleep(0.5)
        
        # 验证Executor被调用
        self.assertTrue(self.executor_called)
        self.assertEqual(self.executor_spider_name, "test_spider")
        self.assertEqual(self.executor_task_config, test_task)
        
        # 恢复原始方法
        self.worker._init_executor = original_init_executor
        
        # 关闭Worker
        self.worker.shutdown()
        time.sleep(0.5)
        worker_thread.join(timeout=1)


if __name__ == '__main__':
    # 运行集成测试
    unittest.main()
