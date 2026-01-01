from typing import Dict, Optional, List
from typing import TYPE_CHECKING
import threading
import logging
import time


from fastapi import FastAPI, HTTPException, Body
import uvicorn
from pydantic import BaseModel

from dspider.worker.schedulers.scheduler import Scheduler
from dspider.worker.utils.load_module import walk_modules
from dspider.common.rabbitmq_service import RabbitMQService
from dspider.common.mongodb_service import MongoDBService

if TYPE_CHECKING:
    from dspider.worker.schedulers import Scheduler

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    handlers=[logging.StreamHandler()]
                    )
logger = logging.getLogger(__name__)

def get_scheduler_class(scheduler_name: str) -> Optional[type]:
    """根据调度器名称获取调度器类"""
    from importlib import import_module
    from pkgutil import iter_modules
    
    scheduler_pkg = 'dspider.worker.schedulers'
    scheduler_name = 'DbToMqScheduler'
    modules = import_module(scheduler_pkg)
    
    
    for importer, modname, ispkg in iter_modules(
        modules.__path__,
        prefix=f'{modules.__name__}.',
    ):
        if ispkg:
            continue
        mod = import_module(modname)
        if hasattr(mod, scheduler_name):
            scheduler_class = getattr(mod, scheduler_name)
            return scheduler_class

class APIService:
    """调度器管理API服务"""
    def __init__(self, scheduler_manager: 'SchedulerManager', host: str = "0.0.0.0", port: int = 8000):
        self.scheduler_manager = scheduler_manager
        self.host = host
        self.port = port
        self.app = FastAPI(title="Scheduler Manager API")
        self._register_routes()  # 注册API路由

    def _register_routes(self) -> None:
        """注册所有API路由"""
        
        # 请求模型定义
        class SchedulerNameModel(BaseModel):
            scheduler_name: str
            params: dict = {}
            
        class StartAllResponse(BaseModel):
            status: str
            message: str
            schedulers: List[str]
            
        class SchedulerStatusModel(BaseModel):
            name: str
            is_running: bool
        
        @self.app.post("/register", summary="注册新调度器")
        async def register_scheduler(data: SchedulerNameModel = Body(...)):
            scheduler_name = data.scheduler_name
            scheduler_params = data.params
            scheduler_cls: type['Scheduler'] = get_scheduler_class(scheduler_name)

            scheduler = scheduler_cls(scheduler_manager=self.scheduler_manager, **scheduler_params)
            self.scheduler_manager.register_scheduler(scheduler)
        
        @self.app.post("/start_schedule", summary="启动指定调度器")
        async def start_schedule(data: SchedulerNameModel = Body(...)):
            scheduler_name = data.scheduler_name
            scheduler = self.scheduler_manager.get_scheduler(scheduler_name)
            if not scheduler:
                raise HTTPException(status_code=404, detail=f"调度器 {scheduler_name} 不存在")
            
            scheduler.start()
            return {
                "status": "success",
                "message": f"调度器 {scheduler_name} 已启动",
                "is_running": scheduler.is_running
            }
        
        @self.app.post("/stop_schedule", summary="停止指定调度器")
        async def stop_schedule(data: SchedulerNameModel = Body(...)):
            scheduler_name = data.scheduler_name
            scheduler = self.scheduler_manager.get_scheduler(scheduler_name)
            if not scheduler:
                raise HTTPException(status_code=404, detail=f"调度器 {scheduler_name} 不存在")
            
            scheduler.stop()
            return {
                "status": "success",
                "message": f"调度器 {scheduler_name} 已停止",
                "is_running": scheduler.is_running
            }
        
        @self.app.post("/start_all", summary="启动所有调度器", response_model=StartAllResponse)
        async def start_all():
            self.scheduler_manager.start_all()
            return {
                "status": "success",
                "message": "所有调度器已启动",
                "schedulers": self.scheduler_manager.list_schedulers()
            }
        
        @self.app.post("/stop_all", summary="停止所有调度器")
        async def stop_all():
            self.scheduler_manager.stop_all()
            return {
                "status": "success",
                "message": "所有调度器已停止"
            }
        
        @self.app.get("/list_schedulers", summary="查询所有调度器状态")
        async def list_schedulers() -> List[SchedulerStatusModel]:
            status_list = []
            for name in self.scheduler_manager.list_schedulers():
                scheduler = self.scheduler_manager.get_scheduler(name)
                status_list.append({
                    "name": name,
                    "is_running": scheduler.is_running
                })
            return status_list
        
        @self.app.get("/get_scheduler_status/{scheduler_name}", summary="查询指定调度器状态")
        async def get_scheduler_status(scheduler_name: str):
            scheduler = self.scheduler_manager.get_scheduler(scheduler_name)
            if not scheduler:
                raise HTTPException(status_code=404, detail=f"调度器 {scheduler_name} 不存在")
            
            return {
                "name": scheduler.name,
                "is_running": scheduler.is_running,
                "type": type(scheduler).__name__
            }

    def run(self) -> None:
        """启动API服务"""
        logger.info(f"API服务启动: http://{self.host}:{self.port}")
        logger.info(f"API文档地址: http://{self.host}:{self.port}/docs")
        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            log_level="info"
        )

class SchedulerManager:
    """调度器管理器（带API服务）"""
    def __init__(
        self, api_host: str = "0.0.0.0", api_port: int = 8000,
        rabbitmq_service: RabbitMQService = None,
        mongodb_service: MongoDBService = None,
    ):
        self.scheduler_module_path = "dspider.worker.schedulers"
        self._schedulers: Dict[str, Scheduler] = {}
        self.api_host = api_host
        self.api_port = api_port
        
        # 初始化API服务线程
        self.api_server: Optional[threading.Thread] = None
        self._api_service: Optional[APIService] = None
        
        self.rabbitmq_service = rabbitmq_service
        self.mongodb_service = mongodb_service
        
        # 创建API服务线程（守护线程）
        self.api_server = threading.Thread(
            target=self._run_api_server,
            daemon=True,
            name="API-Server-Thread"
        )

    def _run_api_server(self) -> None:
        """运行API服务（线程执行函数）"""
        self._api_service = APIService(self, self.api_host, self.api_port)
        self._api_service.run()

    def start(self) -> None:
        """启动管理器（包括API服务和所有调度器）"""
        # 启动API服务
        self.start_api_service()
        
        # 启动所有调度器（可选，也可通过API手动启动）
        # self.start_all()
        
        try:
            while True:
                time.sleep(24*60*60)
        except KeyboardInterrupt:
            logger.info("收到中断信号，开始停止所有服务...")
            self.stop_all()
            self.stop_api_service()
            logger.info("所有服务已停止")
        except Exception as e:
            logger.info(f"发生未知错误: {e}")
            self.stop_all()
            self.stop_api_service()
            logger.info("所有服务已停止")

    def start_api_service(self) -> None:
        """单独启动API服务"""
        if self.api_server and not self.api_server.is_alive():
            self.api_server.start()
            logger.info("API服务线程已启动")
        else:
            logger.info("API服务已在运行中")

    def register_scheduler(self, scheduler: Scheduler) -> None:
        """注册调度器"""
        # if not issubclass(scheduler.__class__, Scheduler):
        #     raise HTTPException(status_code=400, detail=f"{scheduler.__class__.__name__} 不是一个有效的调度器类")
        if scheduler.name in self._schedulers:
            raise HTTPException(status_code=400, detail=f"调度器 {scheduler.name} 已存在")
        self._schedulers[scheduler.name] = scheduler
        logger.info(f"调度器 {scheduler.name} 注册成功")

    def start_all(self) -> None:
        """启动所有调度器"""
        logger.info("开始启动所有调度器...")
        for scheduler in self._schedulers.values():
            scheduler.start()
        logger.info("所有调度器启动完成")

    def stop_all(self) -> None:
        """停止所有调度器"""
        logger.info("开始停止所有调度器...")
        for scheduler in self._schedulers.values():
            scheduler.stop()
        logger.info("所有调度器停止完成")

    def get_scheduler(self, name: str) -> Optional[Scheduler]:
        """获取指定调度器"""
        return self._schedulers.get(name)

    def list_schedulers(self) -> List[str]:
        """列出所有调度器名称"""
        return list(self._schedulers.keys())