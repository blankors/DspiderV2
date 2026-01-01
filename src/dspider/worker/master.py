import logging
import aiohttp
import requests
import threading
import time
import asyncio

import grpc
from concurrent import futures

from dspider.common.nacos_service import NacosService
from dspider.worker.rpc import master_service_pb2, master_service_pb2_grpc
from dspider.worker.rpc import worker_service_pb2, worker_service_pb2_grpc  

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    handlers=[logging.StreamHandler()]
                    )
logger = logging.getLogger(__name__)

# 定义心跳超时时间（秒）
HEARTBEAT_TIMEOUT = 10
MASTER_ADDRESS = "127.0.0.1:50011"
class MasterService(master_service_pb2_grpc.MasterServiceServicer):
    def __init__(self):
        # 存储工作节点信息：{worker_id: {address, last_heartbeat, status, cpu, memory}}
        self.workers = {}
        self.lock = threading.Lock()
        # 启动心跳检查线程
        self.check_thread = threading.Thread(target=self.check_heartbeat, daemon=True)
        self.check_thread.start()

    def Register(self, request, context):
        """处理Worker注册请求"""
        with self.lock:
            worker_id = request.worker_id
            if worker_id in self.workers:
                response = master_service_pb2.RegisterResponse(
                    success=True,
                    message=f"Worker {worker_id} 已存在，更新信息",
                    master_address=MASTER_ADDRESS
                )
            else:
                response = master_service_pb2.RegisterResponse(
                    success=True,
                    message=f"Worker {worker_id} 注册成功",
                    master_address=MASTER_ADDRESS
                )
            
            # 更新/存储Worker信息
            self.workers[worker_id] = {
                "address": request.worker_address,
                "last_heartbeat": time.time(),
                "status": "idle",
                "cpu_core": request.cpu_core,
                "memory": request.memory
            }
            logger.info(f"[Master] Worker {worker_id} 注册成功，当前在线节点数：{len(self.workers)}")
        return response

    def SendHeartbeat(self, request, context):
        """处理Worker心跳上报"""
        with self.lock:
            worker_id = request.worker_id
            if worker_id not in self.workers:
                # 未注册的Worker，返回失败
                response = master_service_pb2.HeartbeatResponse(
                    success=False,
                    message=f"Worker {worker_id} 未注册，请先注册",
                    master_timestamp=int(time.time())
                )
                logger.info(f"[Master] 收到 {worker_id} 心跳，状态：{request.status}，失败原因：{response.message}")
            else:
                # 更新心跳时间和状态
                self.workers[worker_id]["last_heartbeat"] = time.time()
                self.workers[worker_id]["status"] = request.status
                response = master_service_pb2.HeartbeatResponse(
                    success=True,
                    message=f"心跳接收成功",
                    master_timestamp=int(time.time())
                )
                logger.info(f"[Master] 收到 {worker_id} 心跳，状态：{request.status}")
        return response

    def check_heartbeat(self):
        """定时检查Worker心跳，清理超时节点"""
        while True:
            time.sleep(5)  # 每5秒检查一次。这5s内如果有节点心跳超时，会发生什么？该怎么办？
            with self.lock:
                current_time = time.time()
                timeout_workers = []
                # 检查所有Worker的心跳超时
                for worker_id, info in self.workers.items():
                    if current_time - info["last_heartbeat"] > HEARTBEAT_TIMEOUT:
                        timeout_workers.append(worker_id)
                
                # 清理超时节点
                for worker_id in timeout_workers:
                    del self.workers[worker_id]
                    logger.info(f"[Master] Worker {worker_id} 心跳超时，已下线，当前在线节点数：{len(self.workers)}")

class Master:
    def __init__(self, nacos_service, mongodb_service, redis_service, rabbitmq_service):
        self.nacos_service = nacos_service
        self.mongodb_service = mongodb_service
        self.redis_service = redis_service
        self.rabbitmq_service = rabbitmq_service
        self.grpc_server = None
        self.master_service = MasterService()
        self.rpc_thnum = 10
        self.last_distribute_worker_index = 0
    
    def start_rpc(self):
        self.grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.rpc_thnum))
        master_service_pb2_grpc.add_MasterServiceServicer_to_server(self.master_service, self.grpc_server)
        self.grpc_server.add_insecure_port(self.address)
        self.grpc_server.start()
        logger.info(f"[Master] 服务已启动，监听地址：{self.address}")
        try:
            self.grpc_server.wait_for_termination()
        except KeyboardInterrupt:
            self.grpc_server.stop(0)
            logger.info("[Master] 服务已停止")

    def start(self):
        self.address = MASTER_ADDRESS
        try:
            self.rpc_thread = threading.Thread(target=self.start_rpc, daemon=True) # daemon
            self.rpc_thread.start()
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.schedule())
        except KeyboardInterrupt:
            logger.info("[Master] 服务已停止")
        except Exception as e:
            logger.error(f"Error in master: {e}")

    async def schedule(self):
        try:
            time.sleep(10) # 调整为先启动worker再启动master，worker找不到master 3s重试
            await self.schedule_once()
            await self.schedule_backend()
        except Exception as e:
            logger.error(f"Error scheduling tasks: {e}")
            raise e
        
    async def schedule_once(self):
        """Master刚启动时调用一次，调度所有任务
        """
        info = await self.get_info() # 必须要等到所有worker加入到nacos才可以获取到所有worker的信息
        servs = info["servs"]
        tasks = info["tasks"]
        server_idx = 0
        for task in tasks:
            serv = servs[server_idx]
            try_count = 0
            while True:
                try_count += 1
                if try_count > 3:
                    logger.error(f"Error scheduling task {task} to server {serv['ip']}:{serv['port']}: try 3 times")
                    break
                
                try:
                    self.distribute_task(task=task)
                except Exception as e:
                    pass
                else:
                    break
                
    async def schedule_backend(self):
        servs = await self.nacos_service.get_instances()
        self.rabbitmq_service.consume(callback=self.mq_callback)

    async def get_info(self):
        servs = await self.nacos_service.get_instances()
        status = self.redis_service.get("worker_status")
        tasks = self.get_tasks()
        
        return {"servs": servs, "status": status, "tasks": tasks}
    
    def get_tasks(self):
        tasks = self.mongodb_service.find({"status": 0})
        return tasks

    async def mq_callback(self, data):
        task = data["task"]
        info = await self.get_info()
        servs = info["servs"]
        self.distribute_task(task=task)
        self.mongodb_service.insert({"task_id": task["task_id"], "status": 1}) # 持久化任务

    def select_worker(self):
        """选择一个可用的 Worker（轮询负载均衡）"""
        with self.master_service.lock:
            available_workers = [
                worker_id for worker_id, info in self.master_service.workers.items()
                if info["status"] == "idle"
            ]
            
            if not available_workers:
                logger.warning("[Master] 没有可用的 Worker")
                return None, None
            
            worker_index = self.last_distribute_worker_index + 1
            worker_id = available_workers[worker_index % len(available_workers)]
            self.worker_index = worker_index
            
            worker_info = self.master_service.workers[worker_id]
            logger.info(f"[Master] 选择 Worker {worker_id}，地址：{worker_info['address']}")
            return worker_id, worker_info["address"]

    def distribute_task(self, task):
        """通过 gRPC 分发任务给 Worker"""
        try:
            worker_id, addr = self.select_worker()
            channel = grpc.insecure_channel(addr)
            stub = worker_service_pb2_grpc.WorkerServiceStub(channel)
            
            request = worker_service_pb2.DistributeTaskRequest(
                worker_id=worker_id,
                task=worker_service_pb2.TaskData(
                    task_id=task.get("task_id", ""),
                    url=task.get("url", ""),
                    method=task.get("method", "GET"),
                    headers=task.get("headers", {}),
                    body=task.get("body", ""),
                    priority=task.get("priority", 0),
                    retry_count=task.get("retry_count", 0),
                    metadata=task.get("metadata", {})
                ),
                timestamp=int(time.time())
            )
            
            response = stub.DistributeTask(request)
            channel.close()
            
            if response.success:
                logger.info(f"[Master] 任务 {response.task_id} 成功分发到 Worker {worker_id}")
                return True, response.message
            else:
                logger.error(f"[Master] 任务分发失败：{response.message}")
                return False, response.message
                
        except grpc.RpcError as e:
            logger.exception(f"[Master] gRPC 调用 Worker {worker_id} 失败：{e}")
            raise
        except Exception as e:
            logger.exception(f"[Master] 任务分发异常：{e}")
            raise

if __name__ == "__main__":
    m = Master()
    m.schedule()
    