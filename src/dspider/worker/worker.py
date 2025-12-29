import grpc
import time
import threading
import uuid
from dspider.worker.rpc import master_worker_pb2
from dspider.worker.rpc import master_worker_pb2_grpc

class Worker:
    def __init__(self, master_address="localhost:50051", worker_address="localhost:0"):
        self.master_address = master_address
        self.worker_address = worker_address
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
        self.stub = master_worker_pb2_grpc.MasterServiceStub(self.channel)

    def register(self):
        """向Master注册"""
        try:
            request = master_worker_pb2.RegisterRequest(
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
                request = master_worker_pb2.HeartbeatRequest(
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
            while True:
                time.sleep(86400)
        except KeyboardInterrupt:
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