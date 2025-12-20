import pika
import logging
import json
import time
from typing import Optional, Dict, Any, Callable, Union

from pika.exceptions import ChannelClosedByBroker, ConnectionClosedByBroker, IncompatibleProtocolError, StreamLostError
from pika import BasicProperties

from dspider.common.load_config import config

logger = logging.getLogger(__name__)

class RabbitMQService:
    """RabbitMQ连接管理类"""
    
    def __init__(self, host: str, port: int, username: str, password: str, virtual_host: str):
        """初始化RabbitMQ连接
        
        Args:
            host: RabbitMQ主机地址
            port: RabbitMQ端口
            username: 用户名
            password: 密码
            virtual_host: 虚拟主机
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.connection = None
        self.channel = None
    
    def connect(self, max_retries: int = 3, retry_delay: int = 2) -> bool:
        """连接到RabbitMQ
        
        Args:
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            
        Returns:
            bool: 是否连接成功
        """
        for attempt in range(max_retries):
            try:
                credentials = pika.PlainCredentials(self.username, self.password)
                parameters = pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    virtual_host=self.virtual_host,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
                
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                logger.info(f"成功连接到RabbitMQ: {self.host}:{self.port}/{self.virtual_host}")
                return True
            except Exception as e:
                logger.error(f"连接RabbitMQ失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        return False
    
    def disconnect(self):
        """断开RabbitMQ连接"""
        try:
            if self.connection and self.connection.is_open:
                # 先关闭通道
                if self.channel and self.channel.is_open:
                    self.channel.close()
                # 再关闭连接
                self.connection.close()
                logger.info("已断开RabbitMQ连接")
        except Exception as e:
            logger.warning(f"断开RabbitMQ连接时发生错误: {str(e)}")
    
    def reset_connection(self):
        """重置RabbitMQ连接"""
        logger.info("开始重置RabbitMQ连接")
        # 断开现有连接
        self.disconnect()
        # 重新建立连接
        self.connect(max_retries=3, retry_delay=2)
    
    def declare_queue(self, queue_name: str, durable: bool = True,
                      exclusive: bool = False, auto_delete: bool = False,
                      arguments: Optional[Dict[str, Any]] = None) -> bool:
        """声明队列
        
        Args:
            queue_name: 队列名称
            durable: 是否持久化
            exclusive: 是否排他
            auto_delete: 是否自动删除
            arguments: 队列参数
            
        Returns:
            bool: 是否声明成功
        """
        try:
            if not self.channel:
                logger.error("RabbitMQ未连接")
                return False
            
            self.channel.queue_declare(
                queue=queue_name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments
            )
            logger.info(f"队列声明成功: {queue_name}")
            return True
        except Exception as e:
            logger.error(f"声明队列失败: {str(e)}")
            return False
    
    def declare_priority_queue(self, queue_name: str, priority: int = 0,
                               durable: bool = True, exclusive: bool = False,
                               auto_delete: bool = False) -> bool:
        """声明优先级队列
        
        Args:
            queue_name: 队列名称
            priority: 优先级（0-9）
            durable: 是否持久化
            exclusive: 是否排他
            auto_delete: 是否自动删除
            
        Returns:
            bool: 是否声明成功
        """
        arguments = {'x-max-priority': 10}
        return self.declare_queue(
            queue_name,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments
        )
    
    def declare_exchange(self, exchange_name: str, exchange_type: str = 'direct',
                        durable: bool = True, auto_delete: bool = False) -> bool:
        """声明交换机
        
        Args:
            exchange_name: 交换机名称
            exchange_type: 交换机类型（direct, topic, fanout等）
            durable: 是否持久化
            auto_delete: 是否自动删除
            
        Returns:
            bool: 是否声明成功
        """
        try:
            if not self.channel:
                logger.error("RabbitMQ未连接")
                return False
            
            self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=durable,
                auto_delete=auto_delete
            )
            logger.info(f"交换机声明成功: {exchange_name}")
            return True
        except Exception as e:
            logger.error(f"声明交换机失败: {str(e)}")
            return False
    
    def publish(self, message: any, routing_key: str, exchange: str = "", priority: int = 15, **kwargs):
        """
        发送到队列,json比data更优先
        :param routing_key: 路由绑定
        :param exchange: 交换机
        :param message:
        :param priority: 优先级,历史数据消息等级2, 日常消息等级15, 非优先级队列该参数无效
        :return: None
        :raise MessageSendError:
        """

        if not self.channel:
            raise ConnectionError("未连接channel")

        if isinstance(message, str):
            message = message.encode("utf-8")
        elif isinstance(message, dict):
            message = json.dumps(message)
        else:
            raise TypeError("message must be str or dict")

        # 尝试发送消息三次
        retry_times = 3
        while retry_times:
            try:
                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=message,
                    properties=BasicProperties(
                        delivery_mode=2,  # 2-持久化
                        priority=priority,
                        **kwargs,
                    ),
                )
                return
            # 只处理链接丢失相关的错误
            except (StreamLostError, ChannelClosedByBroker, IncompatibleProtocolError, ConnectionClosedByBroker):
                # pika.exceptions.ChannelWrongStateError: Channel is closed.
                logging.warning("检测到rabbit mq 链接丢失，重新链接")
                self.reset_connection()
                retry_times -= 1

        raise Exception(f"消息发送失败: {message}, 请检查消息服务器是否存活")
    
    def publish_workqueue(self, message: any, queue_name: str, priority: int = 15, **kwargs):
        '''
        工作队列，无交换机，直接发送到队列
        '''
        self.publish(message, queue_name, '', priority, **kwargs)
    
    def bind_queue(self, queue_name: str, exchange_name: str,
                   routing_key: str = '') -> bool:
        """绑定队列到交换机
        
        Args:
            queue_name: 队列名称
            exchange_name: 交换机名称
            routing_key: 路由键
            
        Returns:
            bool: 是否绑定成功
        """
        try:
            if not self.channel:
                logger.error("RabbitMQ未连接")
                return False
            
            self.channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key
            )
            logger.info(f"队列绑定成功: {queue_name} -> {exchange_name}:{routing_key}")
            return True
        except Exception as e:
            logger.error(f"队列绑定失败: {str(e)}")
            return False
    
    def publish_message(self, exchange_name: str, routing_key: str,
                       message: Union[Dict[str, Any], str],
                       persistent: bool = True) -> bool:
        """发布消息
        
        Args:
            exchange_name: 交换机名称
            routing_key: 路由键
            message: 消息内容（字典或字符串）
            persistent: 是否持久化消息
            
        Returns:
            bool: 是否发布成功
        """
        try:
            if not self.channel:
                logger.error("RabbitMQ未连接")
                return False
            
            # 序列化消息
            if isinstance(message, dict):
                message_body = json.dumps(message, ensure_ascii=False)
            else:
                message_body = str(message)
            
            # 设置消息属性
            properties = pika.BasicProperties(
                delivery_mode=2 if persistent else 1  # 2表示持久化
            )
            
            # 发布消息
            self.channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=message_body.encode('utf-8'),
                properties=properties
            )
            logger.info(f"消息发布成功: 交换机={exchange_name}, 路由键={routing_key}")
            return True
        except Exception as e:
            logger.error(f"发布消息失败: {str(e)}")
            return False
    
    def consume_messages(self, queue_name: str, callback: Callable[[str, Dict[str, Any]], bool],
                        auto_ack: bool = False, prefetch_count: int = 1) -> None:
        """消费消息
        
        Args:
            queue_name: 队列名称
            callback: 回调函数，接收消息体和属性，返回是否确认
            auto_ack: 是否自动确认
            prefetch_count: 预取消息数
        """
        try:
            if not self.channel:
                logger.error("RabbitMQ未连接")
                return
            
            # 设置预取数量
            self.channel.basic_qos(prefetch_count=prefetch_count)
            
            def _on_message(ch, method, properties, body):
                try:
                    # 尝试解析JSON
                    try:
                        message_body = json.loads(body.decode('utf-8'))
                    except json.JSONDecodeError:
                        message_body = body.decode('utf-8')
                    
                    # 调用回调函数
                    should_ack = callback(message_body, {
                        'delivery_tag': method.delivery_tag,
                        'redelivered': method.redelivered,
                        'routing_key': method.routing_key
                    })
                    
                    # 手动确认
                    if not auto_ack and should_ack:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    elif not auto_ack and not should_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                except Exception as e:
                    logger.exception(f"处理消息时出错: {str(e)}")
                    if not auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            # 开始消费
            logger.info(f"开始消费队列: {queue_name}")
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=_on_message,
                auto_ack=auto_ack
            )
            
            # 启动消费循环
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("用户中断消费")
            self.channel.stop_consuming()
        except Exception as e:
            logger.error(f"消费消息时出错: {str(e)}")
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
    
    def purge_queue(self, queue_name: str) -> bool:
        """清空队列
        
        Args:
            queue_name: 队列名称
            
        Returns:
            bool: 是否成功
        """
        try:
            if not self.channel:
                logger.error("RabbitMQ未连接")
                return False
            
            self.channel.queue_purge(queue=queue_name)
            logger.info(f"队列已清空: {queue_name}")
            return True
        except Exception as e:
            logger.error(f"清空队列失败: {str(e)}")
            return False
    
    def get_message_count(self, queue_name: str) -> int:
        """获取队列消息数量
        
        Args:
            queue_name: 队列名称
            
        Returns:
            int: 消息数量
        """
        try:
            if not self.channel:
                logger.error("RabbitMQ未连接")
                return 0
            
            result = self.channel.queue_declare(queue=queue_name, passive=True)
            return result.method.message_count
        except Exception as e:
            logger.error(f"获取队列消息数失败: {str(e)}")
            return 0

import aio_pika
import logging
import json
import asyncio
from typing import Optional, Dict, Any, Callable, Union, Coroutine

logger = logging.getLogger(__name__)

class AsyncRabbitMQClient:
    """异步RabbitMQ连接管理类"""
    
    def __init__(self, host: str, port: int, username: str, password: str, virtual_host: str):
        """初始化异步RabbitMQ连接
        
        Args:
            host: RabbitMQ主机地址
            port: RabbitMQ端口
            username: 用户名
            password: 密码
            virtual_host: 虚拟主机
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
    
    async def connect(self, max_retries: int = 3, retry_delay: int = 2) -> bool:
        """异步连接到RabbitMQ
        
        Args:
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            
        Returns:
            bool: 是否连接成功
        """
        for attempt in range(max_retries):
            try:
                self.connection = await aio_pika.connect(
                    host=self.host,
                    port=self.port,
                    login=self.username,
                    password=self.password,
                    virtualhost=self.virtual_host,
                    heartbeat=600,
                )
                self.channel = await self.connection.channel()
                logger.info(f"成功连接到RabbitMQ: {self.host}:{self.port}/{self.virtual_host}")
                return True
            except Exception as e:
                logger.error(f"连接RabbitMQ失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
        return False
    
    async def disconnect(self):
        """异步断开RabbitMQ连接"""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("已断开RabbitMQ连接")
    
    async def declare_queue(self, queue_name: str, durable: bool = True,
                          exclusive: bool = False, auto_delete: bool = False,
                          arguments: Optional[Dict[str, Any]] = None) -> bool:
        """异步声明队列
        
        Args:
            queue_name: 队列名称
            durable: 是否持久化
            exclusive: 是否排他
            auto_delete: 是否自动删除
            arguments: 队列参数
            
        Returns:
            bool: 是否声明成功
        """
        try:
            if not self.channel:
                logger.error("RabbitMQ未连接")
                return False
            
            await self.channel.declare_queue(
                name=queue_name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments
            )
            logger.info(f"队列声明成功: {queue_name}")
            return True
        except Exception as e:
            logger.error(f"声明队列失败: {str(e)}")
            return False
    
    async def declare_priority_queue(self, queue_name: str, priority: int = 0,
                                   durable: bool = True, exclusive: bool = False,
                                   auto_delete: bool = False) -> bool:
        """异步声明优先级队列
        
        Args:
            queue_name: 队列名称
            priority: 优先级（0-9）
            durable: 是否持久化
            exclusive: 是否排他
            auto_delete: 是否自动删除
            
        Returns:
            bool: 是否声明成功
        """
        arguments = {'x-max-priority': 10}
        return await self.declare_queue(
            queue_name,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments
        )
    
    async def declare_exchange(self, exchange_name: str, exchange_type: str = 'direct',
                             durable: bool = True, auto_delete: bool = False) -> bool:
        """异步声明交换机
        
        Args:
            exchange_name: 交换机名称
            exchange_type: 交换机类型（direct, topic, fanout等）
            durable: 是否持久化
            auto_delete: 是否自动删除
            
        Returns:
            bool: 是否声明成功
        """
        try:
            if not self.channel:
                logger.error("RabbitMQ未连接")
                return False
            
            await self.channel.declare_exchange(
                name=exchange_name,
                type=exchange_type,
                durable=durable,
                auto_delete=auto_delete
            )
            logger.info(f"交换机声明成功: {exchange_name}")
            return True
        except Exception as e:
            logger.error(f"声明交换机失败: {str(e)}")
            return False
    
    async def publish(self, message: any, routing_key: str, exchange: str = "", priority: int = 15, **kwargs):
        """
        异步发送到队列,json比data更优先
        :param routing_key: 路由绑定
        :param exchange: 交换机
        :param message: 消息内容
        :param priority: 优先级,历史数据消息等级2, 日常消息等级15, 非优先级队列该参数无效
        :return: None
        :raise MessageSendError: 消息发送失败时抛出
        """
        if not self.channel:
            raise ConnectionError("未连接channel")
        
        if isinstance(message, str):
            message_body = message.encode("utf-8")
        elif isinstance(message, dict):
            message_body = json.dumps(message).encode("utf-8")
        else:
            raise TypeError("message must be str or dict")
        
        # 尝试发送消息三次
        retry_times = 3
        while retry_times:
            try:
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message_body,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        priority=priority,
                        **kwargs,
                    ),
                    routing_key=routing_key,
                )
                return
            except Exception as e:
                logging.warning(f"检测到rabbit mq 链接丢失，重新链接: {e}")
                await self.disconnect()
                await self.connect()
                retry_times -= 1
        
        raise Exception(f"消息发送失败: {message}, 请检查消息服务器是否存活")
    
    async def publish_workqueue(self, message: any, queue_name: str, priority: int = 15, **kwargs):
        '''
        工作队列，无交换机，直接发送到队列
        '''
        await self.publish(message, queue_name, "", priority, **kwargs)
    
    async def bind_queue(self, queue_name: str, exchange_name: str,
                       routing_key: str = '') -> bool:
        """异步绑定队列到交换机
        
        Args:
            queue_name: 队列名称
            exchange_name: 交换机名称
            routing_key: 路由键
            
        Returns:
            bool: 是否绑定成功
        """
        try:
            if not self.channel:
                logger.error("RabbitMQ未连接")
                return False
            
            queue = await self.channel.get_queue(queue_name)
            await queue.bind(exchange_name, routing_key)
            logger.info(f"队列绑定成功: {queue_name} -> {exchange_name}:{routing_key}")
            return True
        except Exception as e:
            logger.error(f"队列绑定失败: {str(e)}")
            return False
    
    async def consume_messages(self, queue_name: str, 
                             callback: Union[Callable[[Any, Dict[str, Any]], bool],
                                            Callable[[Any, Dict[str, Any]], Coroutine[Any, Any, bool]]],
                             auto_ack: bool = False, prefetch_count: int = 1) -> None:
        """异步消费消息
        
        Args:
            queue_name: 队列名称
            callback: 回调函数，接收消息体和属性，返回是否确认
                      可以是同步函数或异步函数
            auto_ack: 是否自动确认
            prefetch_count: 预取消息数
        """
        try:
            if not self.channel:
                logger.error("RabbitMQ未连接")
                return
            
            # 设置预取数量
            await self.channel.set_qos(prefetch_count=prefetch_count)
            
            # 获取队列
            queue = await self.channel.get_queue(queue_name)
            
            async def _on_message(message: aio_pika.IncomingMessage):
                try:
                    # 尝试解析JSON
                    try:
                        message_body = json.loads(message.body.decode('utf-8'))
                    except json.JSONDecodeError:
                        message_body = message.body.decode('utf-8')
                    
                    # 调用回调函数
                    message_properties = {
                        'delivery_tag': message.delivery_tag,
                        'redelivered': message.redelivered,
                        'routing_key': message.routing_key
                    }
                    
                    # 检测回调函数是否为异步函数
                    if asyncio.iscoroutinefunction(callback):
                        # 调用异步回调函数
                        should_ack = await callback(message_body, message_properties)
                    else:
                        # 调用同步回调函数
                        should_ack = callback(message_body, message_properties)
                    
                    # 手动确认
                    if auto_ack:
                        await message.ack()
                    elif should_ack:
                        await message.ack()
                    else:
                        await message.nack(requeue=True)
                except Exception as e:
                    logger.error(f"处理消息时出错: {str(e)}")
                    if not auto_ack:
                        await message.nack(requeue=True)
            
            # 开始消费
            logger.info(f"开始消费队列: {queue_name}")
            await queue.consume(_on_message, auto_ack=auto_ack)
        except Exception as e:
            logger.error(f"消费消息时出错: {str(e)}")

