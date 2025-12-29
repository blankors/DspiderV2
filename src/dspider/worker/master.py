import logging
import aiohttp
import requests
import threading
import time
import asyncio

import grpc
from concurrent import futures

from dspider.common.nacos_service import NacosService
from dspider.worker.rpc import master_worker_pb2, master_worker_pb2_grpc

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    handlers=[logging.StreamHandler()]
                    )
logger = logging.getLogger(__name__)

# 定义心跳超时时间（秒）
HEARTBEAT_TIMEOUT = 10
MASTER_ADDRESS = "localhost:50051"
class MasterService(master_worker_pb2_grpc.MasterServiceServicer):
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
                response = master_worker_pb2.RegisterResponse(
                    success=True,
                    message=f"Worker {worker_id} 已存在，更新信息",
                    master_address=MASTER_ADDRESS
                )
            else:
                response = master_worker_pb2.RegisterResponse(
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
                response = master_worker_pb2.HeartbeatResponse(
                    success=False,
                    message=f"Worker {worker_id} 未注册，请先注册",
                    master_timestamp=int(time.time())
                )
                logger.info(f"[Master] 收到 {worker_id} 心跳，状态：{request.status}，失败原因：{response.message}")
            else:
                # 更新心跳时间和状态
                self.workers[worker_id]["last_heartbeat"] = time.time()
                self.workers[worker_id]["status"] = request.status
                response = master_worker_pb2.HeartbeatResponse(
                    success=True,
                    message=f"心跳接收成功",
                    master_timestamp=int(time.time())
                )
                logger.info(f"[Master] 收到 {worker_id} 心跳，状态：{request.status}")
        return response

    def check_heartbeat(self):
        """定时检查Worker心跳，清理超时节点"""
        while True:
            time.sleep(5)  # 每5秒检查一次
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
        # 维护Master状态
        # 从Redis查询是否已有Master
        self.nacos_service = nacos_service
        self.mongodb_service = mongodb_service
        self.redis_service = redis_service
        self.rabbitmq_service = rabbitmq_service

    def start(self):
        self.address = MASTER_ADDRESS
        try:
            self.rpc_thread = threading.Thread(target=self.rpc, daemon=True)
            self.rpc_thread.start()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.schedule())
            
            self.rpc_thread.join()
            self.schedule_thread.join()
        except KeyboardInterrupt:
            logger.info("[Master] 服务已停止")
        except Exception as e:
            logger.error(f"Error in master: {e}")

    def rpc(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        master_worker_pb2_grpc.add_MasterServiceServicer_to_server(MasterService(), self.server)
        self.server.add_insecure_port(self.address)
        self.server.start()
        logger.info(f"[Master] 服务已启动，监听地址：{self.address}")
        try:
            while True:
                time.sleep(86400)  # 保持服务运行
        except KeyboardInterrupt:
            self.server.stop(0)
            logger.info("[Master] 服务已停止")

    def get_tasks(self):
        tasks = self.mongodb_service.find({"status": 0})
        return tasks
    
    async def schedule(self):
        try:
            await self.schedule_once()
            await self.schedule_backend()
        except Exception as e:
            logger.error(f"Error scheduling tasks: {e}")
            raise e
        
    async def get_info(self):
        servs = await self.nacos_service.get_instances()
        status = self.redis_service.get("worker_status")
        tasks = self.get_tasks()
        
        return {"servs": servs, "status": status, "tasks": tasks}
        
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
                    resp = requests.post(f"http://{serv['ip']}:{serv['port']}/worker/task", json=task)
                    if resp.status_code != 200:
                        raise
                except Exception as e:
                    logger.warning(f"请求 {serv['ip']}:{serv['port']} 失败，尝试第 {try_count} 次")
                else:
                    logger.info(f"Schedule task {task} to server {serv['ip']}:{serv['port']}")
                    break

            server_idx = (server_idx + 1) % len(servs)
                
    async def schedule_backend(self):
        servs = await self.nacos_service.get_instances()
        self.rabbitmq_service.consume(callback=self.xxx)

    async def xxx(self, data):
        task = data["task"]
        info = await self.get_info()
        servs = info["servs"]
        rpc(serv, task)
        self.mongodb_service.insert({"task_id": task["task_id"], "status": 1})

if __name__ == "__main__":
    m = Master()
    m.schedule()
    