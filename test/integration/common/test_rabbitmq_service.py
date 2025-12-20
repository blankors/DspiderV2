import unittest
import time
import uuid
from unittest.mock import Mock

from dspider.common.rabbitmq_service import RabbitMQService
from dspider.common.load_config import config


class RabbitMQServiceIntegrationTest(unittest.TestCase):
    """RabbitMQService类的集成测试"""
    
    def setUp(self):
        """设置测试环境，创建真实的RabbitMQ连接"""
        # 获取测试配置
        self.rabbitmq_config = config['rabbitmq']
        
        # 生成唯一的测试队列名，避免测试冲突
        self.test_queue_name = f"test_queue_{uuid.uuid4().hex[:8]}"
        
        # 创建RabbitMQ服务实例
        self.rabbitmq_service = RabbitMQService(
            host=self.rabbitmq_config['host'],
            port=self.rabbitmq_config['port'],
            username=self.rabbitmq_config['username'],
            password=self.rabbitmq_config['password'],
            virtual_host=self.rabbitmq_config['virtualhost']
        )
        
    def tearDown(self):
        """清理测试环境"""
        # 如果连接已建立，清理队列
        if self.rabbitmq_service.channel:
            try:
                # 先清空队列
                self.rabbitmq_service.purge_queue(self.test_queue_name)
                # 再删除队列
                self.rabbitmq_service.channel.queue_delete(queue=self.test_queue_name)
            except Exception as e:
                pass  # 忽略清理过程中的错误
        
        # 断开RabbitMQ连接
        self.rabbitmq_service.disconnect()
    
    def test_connect(self):
        """测试连接到RabbitMQ"""
        # 测试连接
        result = self.rabbitmq_service.connect()
        self.assertTrue(result, "连接RabbitMQ失败")
        self.assertIsNotNone(self.rabbitmq_service.connection)
        self.assertIsNotNone(self.rabbitmq_service.channel)
        self.assertTrue(self.rabbitmq_service.connection.is_open)
        self.assertTrue(self.rabbitmq_service.channel.is_open)
    
    def test_disconnect(self):
        """测试断开RabbitMQ连接"""
        # 先连接
        self.rabbitmq_service.connect()
        self.assertTrue(self.rabbitmq_service.connection.is_open)
        
        # 断开连接
        self.rabbitmq_service.disconnect()
        self.assertFalse(self.rabbitmq_service.connection.is_open)
    
    def test_reset_connection(self):
        """测试重置RabbitMQ连接"""
        # 先连接
        self.rabbitmq_service.connect()
        old_connection = self.rabbitmq_service.connection
        old_channel = self.rabbitmq_service.channel
        
        # 重置连接
        self.rabbitmq_service.reset_connection()
        
        # 验证连接已更新
        self.assertNotEqual(old_connection, self.rabbitmq_service.connection)
        self.assertNotEqual(old_channel, self.rabbitmq_service.channel)
        self.assertTrue(self.rabbitmq_service.connection.is_open)
        self.assertTrue(self.rabbitmq_service.channel.is_open)
    
    def test_declare_queue(self):
        """测试声明队列"""
        # 连接到RabbitMQ
        self.rabbitmq_service.connect()
        
        # 声明队列
        result = self.rabbitmq_service.declare_queue(
            self.test_queue_name,
            durable=False,
            auto_delete=True
        )
        
        self.assertTrue(result, "声明队列失败")
        
        # 验证队列已存在
        try:
            self.rabbitmq_service.channel.queue_declare(
                queue=self.test_queue_name,
                passive=True  # 只检查队列是否存在，不创建
            )
        except Exception as e:
            self.fail(f"队列不存在: {str(e)}")
    
    def test_declare_priority_queue(self):
        """测试声明优先级队列"""
        # 连接到RabbitMQ
        self.rabbitmq_service.connect()
        
        # 声明优先级队列
        priority_queue_name = f"{self.test_queue_name}_priority"
        result = self.rabbitmq_service.declare_priority_queue(
            priority_queue_name,
            priority=10,
            durable=False,
            auto_delete=True
        )
        
        self.assertTrue(result, "声明优先级队列失败")
        
        # 清理
        self.rabbitmq_service.channel.queue_delete(queue=priority_queue_name)
    
    def test_publish_and_consume_message(self):
        """测试发布和消费消息"""
        # 连接到RabbitMQ
        self.rabbitmq_service.connect()
        
        # 声明队列
        self.rabbitmq_service.declare_queue(
            self.test_queue_name,
            durable=False,
            auto_delete=True
        )
        
        # 准备测试消息
        test_message = {"key": "value", "number": 42}
        received_messages = []
        
        # 定义消息处理函数
        def message_handler(message_body, message_properties):
            received_messages.append(message_body)
            # 返回 True 表示需要确认消息
            return True
        
        # 启动消费者线程
        import threading
        def consume_messages():
            self.rabbitmq_service.consume_messages(
                queue_name=self.test_queue_name,
                callback=message_handler,
                auto_ack=False
            )
        
        consumer_thread = threading.Thread(target=consume_messages)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # 等待消费者启动
        time.sleep(0.5)
        
        # 发布消息
        self.rabbitmq_service.publish(
            message=test_message,
            routing_key=self.test_queue_name
        )
        
        # 等待消息被消费
        max_wait_time = 5
        start_time = time.time()
        while not received_messages and time.time() - start_time < max_wait_time:
            time.sleep(0.5)
        
        # 验证消息已被消费
        self.assertEqual(len(received_messages), 1)
        self.assertEqual(received_messages[0], test_message)
    
    def test_purge_queue(self):
        """测试清空队列"""
        # 连接到RabbitMQ
        self.rabbitmq_service.connect()
        
        # 声明队列
        self.rabbitmq_service.declare_queue(
            self.test_queue_name,
            durable=False,
            auto_delete=True
        )
        
        # 发布一些测试消息
        for i in range(5):
            self.rabbitmq_service.publish(
                message={"count": i},
                routing_key=self.test_queue_name
            )
        
        # 等待消息发布完成
        time.sleep(0.5)
        
        # 获取队列中的消息数量
        result = self.rabbitmq_service.channel.queue_declare(
            queue=self.test_queue_name,
            passive=True
        )
        message_count = result.method.message_count
        self.assertGreater(message_count, 0, "队列中没有消息")
        
        # 清空队列
        result = self.rabbitmq_service.purge_queue(self.test_queue_name)
        self.assertTrue(result, "清空队列失败")
        
        # 验证队列已清空
        result = self.rabbitmq_service.channel.queue_declare(
            queue=self.test_queue_name,
            passive=True
        )
        new_message_count = result.method.message_count
        self.assertEqual(new_message_count, 0, "队列未清空")


if __name__ == '__main__':
    unittest.main()
