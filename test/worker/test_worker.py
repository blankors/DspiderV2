import unittest
import uuid
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any
from concurrent.futures import Future

from dspider.worker.worker_temp import Worker, WorkerStatus, TaskProcessor, DefaultTaskProcessor
from dspider.common.load_config import config

class TestWorker(unittest.TestCase):
    """Worker类的单元测试"""
    
    def setUp(self):
        """设置测试环境"""
        # 创建模拟依赖
        self.mock_rabbitmq = Mock()
        self.mock_data_source_manager = Mock()
        self.mock_data_source_manager.get_data_source_with_config.return_value = self.mock_rabbitmq
        
        # 创建测试配置
        self.test_config = config['worker']
        print(self.test_config)
    
    def test_worker_initialization(self):
        """测试Worker类的初始化"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            config=self.test_config,
            task_processor=DefaultTaskProcessor()
        )
        
        # 验证初始化状态
        self.assertEqual(worker.status, WorkerStatus.INITIALIZED)
        self.assertEqual(worker.config, self.test_config)
        self.assertEqual(worker.rabbitmq_service, self.mock_rabbitmq)
        self.assertIsInstance(worker.task_processor, DefaultTaskProcessor)
        self.assertIsNotNone(worker.thread_pool)
        self.assertEqual(len(worker.active_tasks), 0)
        self.assertEqual(len(worker.task_futures), 0)
    
    def test_status_property(self):
        """测试状态属性"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        self.assertEqual(worker.status, WorkerStatus.INITIALIZED)
        
        # 测试状态改变
        worker._status = WorkerStatus.RUNNING
        self.assertEqual(worker.status, WorkerStatus.RUNNING)
    
    def test_run(self):
        """测试run方法"""
        # 模拟consume_messages方法在调用后立即返回
        self.mock_rabbitmq.consume_messages = MagicMock(side_effect=KeyboardInterrupt)
        
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        worker.run()
        
        # 验证状态变化和方法调用
        self.mock_rabbitmq.consume_messages.assert_called_once_with(
            "test_queue",
            callback=worker._on_message_received,
            auto_ack=False,
            prefetch_count=5
        )
        self.assertEqual(worker.status, WorkerStatus.SHUTDOWN)
    
    def test_pause_and_resume(self):
        """测试暂停和恢复功能"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        # 先启动worker
        worker._status = WorkerStatus.RUNNING
        
        # 测试暂停
        worker.pause()
        self.assertEqual(worker.status, WorkerStatus.PAUSED)
        
        # 测试恢复
        worker.resume()
        self.assertEqual(worker.status, WorkerStatus.RUNNING)
    
    def test_shutdown(self):
        """测试关闭功能"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        # 模拟线程池
        worker.thread_pool = Mock()
        
        # 先启动worker
        worker._status = WorkerStatus.RUNNING
        
        # 测试关闭
        worker.shutdown()
        
        # 验证状态和资源清理
        self.assertEqual(worker.status, WorkerStatus.SHUTDOWN)
        worker.thread_pool.shutdown.assert_called_once()
        self.assertEqual(len(worker.active_tasks), 0)
        self.assertEqual(len(worker.task_futures), 0)
    
    def test_on_message_received_running_status(self):
        """测试在运行状态下接收消息"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        # 设置状态为运行
        worker._status = WorkerStatus.RUNNING
        
        # 模拟任务处理器
        mock_task_processor = Mock()
        mock_task_processor.process.return_value = True
        worker.task_processor = mock_task_processor
        
        # 测试消息接收
        test_task = {"_id": "test_task_id", "spider": {"test_spider": {"p_num": 1}}}
        test_properties = {}
        
        result = worker._on_message_received(test_task, test_properties)
        
        # 验证处理结果
        self.assertTrue(result)
        mock_task_processor.process.assert_called_once_with(worker, test_task)
        
        # 验证任务从活跃列表中移除
        self.assertEqual(len(worker.active_tasks), 0)
    
    def test_on_message_received_not_running_status(self):
        """测试在非运行状态下接收消息"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        # 设置状态为暂停
        worker._status = WorkerStatus.PAUSED
        
        # 测试消息接收
        test_task = {"_id": "test_task_id", "spider": {"test_spider": {"p_num": 1}}}
        test_properties = {}
        
        result = worker._on_message_received(test_task, test_properties)
        
        # 验证拒绝处理
        self.assertFalse(result)
    
    def test_on_message_received_with_task_processor_exception(self):
        """测试任务处理器抛出异常的情况"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        # 设置状态为运行
        worker._status = WorkerStatus.RUNNING
        
        # 模拟任务处理器抛出异常
        mock_task_processor = Mock()
        mock_task_processor.process.side_effect = Exception("Test exception")
        worker.task_processor = mock_task_processor
        
        # 测试消息接收
        test_task = {"_id": "test_task_id", "spider": {"test_spider": {"p_num": 1}}}
        test_properties = {}
        
        result = worker._on_message_received(test_task, test_properties)
        
        # 验证异常被捕获且任务被清理
        self.assertFalse(result)
        self.assertEqual(len(worker.active_tasks), 0)
    
    @patch('dspider.worker.worker.Executor')
    def test_init_executor(self, mock_executor_class):
        """测试初始化执行器"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        # 模拟执行器
        mock_executor = Mock()
        mock_executor_class.return_value = mock_executor
        
        # 测试初始化执行器
        spider_name = "test_spider"
        task_config = {"spider": {"test_spider": {"p_num": 1}}}
        
        result = worker._init_executor(spider_name, task_config)
        
        # 验证执行器创建和运行
        self.assertTrue(result)
        mock_executor_class.assert_called_once_with(spider_name, task_config)
        mock_executor.run.assert_called_once()
    
    @patch('dspider.worker.worker.Executor')
    def test_init_executor_exception(self, mock_executor_class):
        """测试初始化执行器时抛出异常的情况"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        # 模拟执行器抛出异常
        mock_executor_class.side_effect = Exception("Executor exception")
        
        # 测试初始化执行器
        result = worker._init_executor("test_spider", {"spider": {"test_spider": {"p_num": 1}}})
        
        # 验证异常被捕获
        self.assertFalse(result)
    
    def test_get_metrics(self):
        """测试获取工作节点指标"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        # 添加一些活跃任务
        worker.active_tasks.add("task1")
        worker.active_tasks.add("task2")
        
        # 获取指标
        metrics = worker.get_metrics()
        
        # 验证指标内容
        self.assertEqual(metrics["worker_id"], worker.worker_id)
        self.assertEqual(metrics["status"], WorkerStatus.INITIALIZED.value)
        self.assertEqual(metrics["active_tasks_count"], 2)
        self.assertEqual(set(metrics["active_tasks"]), {"task1", "task2"})
        self.assertEqual(metrics["config"]["task_queue_name"], "test_queue")
        self.assertEqual(metrics["config"]["max_thread_pool_workers"], 10)
    
    def test_cancel_task(self):
        """测试取消指定任务"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        # 创建模拟Future
        mock_future = Mock(spec=Future)
        mock_future.done.return_value = False
        
        # 添加任务到跟踪列表
        task_id = "test_task_id"
        worker.active_tasks.add(task_id)
        worker.task_futures[task_id] = mock_future
        
        # 测试取消任务
        result = worker.cancel_task(task_id)
        
        # 验证取消操作
        self.assertTrue(result)
        mock_future.cancel.assert_called_once()
        self.assertNotIn(task_id, worker.active_tasks)
        self.assertNotIn(task_id, worker.task_futures)
    
    def test_cancel_task_not_exists(self):
        """测试取消不存在的任务"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        # 测试取消不存在的任务
        result = worker.cancel_task("non_existent_task")
        
        # 验证操作失败
        self.assertFalse(result)
    
    def test_cancel_all_tasks(self):
        """测试取消所有任务"""
        worker = Worker(
            rabbitmq_service=self.mock_rabbitmq,
            data_source_manager=self.mock_data_source_manager,
            config=self.test_config
        )
        
        # 创建模拟Future对象
        mock_future1 = Mock(spec=Future)
        mock_future1.done.return_value = False
        mock_future2 = Mock(spec=Future)
        mock_future2.done.return_value = False
        
        # 添加多个任务
        task_ids = ["task1", "task2", "task3"]
        for task_id, future in zip(task_ids[:2], [mock_future1, mock_future2]):
            worker.active_tasks.add(task_id)
            worker.task_futures[task_id] = future
        
        # 测试取消所有任务
        canceled_count = worker.cancel_all_tasks()
        
        # 验证取消操作
        self.assertEqual(canceled_count, 2)
        mock_future1.cancel.assert_called_once()
        mock_future2.cancel.assert_called_once()
        self.assertEqual(len(worker.active_tasks), 0)
        self.assertEqual(len(worker.task_futures), 0)

class TestTaskProcessor(unittest.TestCase):
    """TaskProcessor相关的单元测试"""
    
    def test_task_processor_interface(self):
        """测试任务处理器接口"""
        processor = TaskProcessor()
        
        # 验证未实现的方法会抛出异常
        with self.assertRaises(NotImplementedError):
            processor.process(Mock(), {})
    
    @patch.object(DefaultTaskProcessor, '_init_executor')
    def test_default_task_processor(self, mock_init_executor):
        """测试默认任务处理器"""
        processor = DefaultTaskProcessor()
        worker = Mock()
        task = {
            "_id": "test_task",
            "spider": {
                "spider1": {"p_num": 2},
                "spider2": {"p_num": 1}
            }
        }
        
        # 模拟线程池提交和结果
        mock_future1 = Mock(spec=Future)
        mock_future1.result.return_value = True
        mock_future2 = Mock(spec=Future)
        mock_future2.result.return_value = True
        mock_future3 = Mock(spec=Future)
        mock_future3.result.return_value = True
        
        worker.thread_pool.submit.side_effect = [mock_future1, mock_future2, mock_future3]
        
        # 模拟as_completed返回顺序
        with patch('dspider.worker.worker.as_completed', return_value=[mock_future1, mock_future2, mock_future3]):
            result = processor.process(worker, task)
        
        # 验证处理结果
        self.assertTrue(result)
        self.assertEqual(worker.thread_pool.submit.call_count, 3)


if __name__ == '__main__':
    unittest.main()