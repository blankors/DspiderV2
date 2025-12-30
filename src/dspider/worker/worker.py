import grpc
import time
import threading
import uuid
from concurrent import futures
import logging

from dspider.worker.rpc import master_service_pb2
from dspider.worker.rpc import master_service_pb2_grpc
from dspider.worker.rpc import worker_service_pb2
from dspider.worker.rpc import worker_service_pb2_grpc

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    handlers=[logging.StreamHandler()]
                    )
logger = logging.getLogger(__name__)

class WorkerService(worker_service_pb2_grpc.WorkerServiceServicer):
    def __init__(self, worker):
        self.worker = worker

    def DistributeTask(self, request, context):
        """接收Master分发的任务"""
        print(f"[Worker {self.worker.worker_id}] 收到任务分发请求")
        print(f"  任务ID: {request.task.task_id}")
        print(f"  URL: {request.task.url}")
        print(f"  方法: {request.task.method}")
        print(f"  优先级: {request.task.priority}")
        
        try:
            self.worker.set_status("busy")
            
            response = worker_service_pb2.DistributeTaskResponse(
                success=True,
                message=f"任务 {request.task.task_id} 已接收",
                task_id=request.task.task_id,
                worker_timestamp=int(time.time())
            )
            return response
        except Exception as e:
            print(f"[Worker {self.worker.worker_id}] 任务分发处理失败: {e}")
            response = worker_service_pb2.DistributeTaskResponse(
                success=False,
                message=f"任务分发失败: {str(e)}",
                task_id=request.task.task_id,
                worker_timestamp=int(time.time())
            )
            return response

    def UpdateTaskStatus(self, request, context):
        """更新任务状态"""
        print(f"[Worker {self.worker.worker_id}] 收到任务状态更新请求")
        print(f"  任务ID: {request.task_id}")
        print(f"  状态: {request.status}")
        print(f"  结果: {request.result}")
        
        try:
            response = worker_service_pb2.TaskStatusResponse(
                success=True,
                message=f"任务 {request.task_id} 状态已更新",
                master_timestamp=int(time.time())
            )
            return response
        except Exception as e:
            print(f"[Worker {self.worker.worker_id}] 任务状态更新失败: {e}")
            response = worker_service_pb2.TaskStatusResponse(
                success=False,
                message=f"任务状态更新失败: {str(e)}",
                master_timestamp=int(time.time())
            )
            return response

MASTER_ADDRESS = "127.0.0.1:50011"
WORKER_ADDRESS = "127.0.0.1:50021"
class Worker:
    def __init__(self, master_address=MASTER_ADDRESS, worker_address=WORKER_ADDRESS):
        self.master_address = master_address
        self.worker_address = worker_address
        self.worker_service = WorkerService(self)
        # 生成唯一的Worker ID
        self.worker_id = f"worker_{uuid.uuid4().hex[:8]}"
        # CPU核心数（模拟）
        self.cpu_core = 4
        # 内存大小（模拟，单位MB）
        self.memory = 8192
        # Worker状态 (idle/busy/error)
        self.status = "idle"
        # 心跳线程
        self.heartbeat_thread = None
        self.running = False
        # 创建gRPC通道
        self.channel = grpc.insecure_channel(self.master_address)
        self.stub = master_service_pb2_grpc.MasterServiceStub(self.channel)

    def register(self):
        """向Master注册"""
        try:
            request = master_service_pb2.RegisterRequest(
                worker_id=self.worker_id,
                worker_address=self.worker_address,
                cpu_core=self.cpu_core,
                memory=self.memory
            )
            response = self.stub.Register(request)
            if response.success:
                print(f"[Worker {self.worker_id}] 注册成功：{response.message}")
                return True
            else:
                print(f"[Worker {self.worker_id}] 注册失败：{response.message}")
                return False
        except grpc.RpcError as e:
            print(f"[Worker {self.worker_id}] 注册失败：{e}")
            return False

    def send_heartbeat(self):
        """发送心跳包（循环执行）"""
        while self.running:
            try:
                request = master_service_pb2.HeartbeatRequest(
                    worker_id=self.worker_id,
                    worker_address=self.worker_address,
                    timestamp=int(time.time()),
                    status=self.status
                )
                response = self.stub.SendHeartbeat(request)
                if response.success:
                    print(f"[Worker {self.worker_id}] 心跳发送成功，Master时间戳：{response.master_timestamp}")
                else:
                    print(f"[Worker {self.worker_id}] 心跳发送失败：{response.message}")
            except grpc.RpcError as e:
                print(f"[Worker {self.worker_id}] 心跳发送失败：{e}")
                # 尝试重新注册
                self.register()
            time.sleep(3)  # 每3秒发送一次心跳

    def set_status(self, status):
        """更新Worker状态"""
        if status in ["idle", "busy", "error"]:
            self.status = status
            print(f"[Worker {self.worker_id}] 状态已更新为：{status}")
        else:
            print(f"[Worker {self.worker_id}] 无效状态：{status}")

    def start(self):
        """启动Worker"""
        # 先注册
        if not self.register():
            return
        # 启动心跳线程
        self.running = True
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        self.heartbeat_thread.start()
        print(f"[Worker {self.worker_id}] 已启动，开始发送心跳（每3秒一次）")
        try:
            self.rpc()
        except KeyboardInterrupt:
            self.server.stop(0)
            logger.info(f"[Worker {self.worker_id}] 服务已停止")
            self.running = False
            self.heartbeat_thread.join()
            self.channel.close()
            print(f"[Worker {self.worker_id}] 已停止")
            
    def rpc(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        worker_service_pb2_grpc.add_WorkerServiceServicer_to_server(self.worker_service, self.server)
        self.server.add_insecure_port(self.worker_address)
        self.server.start()
        logger.info(f"[Worker {self.worker_id}] 服务已启动，监听地址：{self.worker_address}")
        try:
            while True:
                time.sleep(86400)  # 保持服务运行
        except KeyboardInterrupt:
            self.server.stop(0)
            logger.info(f"[Worker {self.worker_id}] 服务已停止")
            self.running = False
            self.heartbeat_thread.join()
            self.channel.close()
            print(f"[Worker {self.worker_id}] 已停止")

if __name__ == "__main__":
    # 启动Worker节点
    worker = Worker()
    # 模拟状态变化（可手动修改测试）
    # worker.set_status("busy")
    worker.start()