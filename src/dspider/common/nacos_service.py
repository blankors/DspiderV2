import logging

from v2.nacos.common.client_config import ClientConfig
from v2.nacos.naming.nacos_naming_service import NacosNamingService
from v2.nacos.naming.model.naming_param import RegisterInstanceParam, ListInstanceParam

from dspider.common.get_env import NACOS_SERVER_ADDRESSES, SERVICE_NAME, SERVICE_PORT, SERVICE_IP

logger = logging.getLogger(__name__)

class NacosService:
    status = 0
    naming_service = None
    server_addresses = NACOS_SERVER_ADDRESSES
    
    @classmethod
    async def ainit(cls):
        try:
            client_config = ClientConfig(server_addresses=cls.server_addresses)
            cls.naming_service = await NacosNamingService.create_naming_service(client_config)
            cls.status = 1
        except Exception as e:
            logger.error(f"Error initializing NacosNamingService: {e}")
            raise e

    @classmethod
    async def check_init(cls):
        if cls.naming_service is None:
            await cls.ainit()
            
    @classmethod
    async def reg_nacos(cls, **kwargs):
        await cls.check_init()
        register_param = RegisterInstanceParam(**kwargs)
        
        success = await cls.naming_service.register_instance(register_param)
        if success:
            logger.info(f"Service {SERVICE_NAME} registered successfully")
        else:
            logger.error(f"Failed to register service {SERVICE_NAME}")
    
    @classmethod
    async def get_instances(cls):
        await cls.check_init()
        list_param = ListInstanceParam(service_name=SERVICE_NAME, healthy_only=None)
        instances = await cls.naming_service.list_instances(list_param) 
        return instances
    
    @classmethod
    async def close(cls):
        if cls.naming_service:
            try:
                naming_service = cls.naming_service
                cls.naming_service = None
                await naming_service.shutdown()
            except TypeError as e:
                if "can't be used in 'await' expression" in str(e):
                    # nacos 客户端的 shutdown() 方法内部调用了 gRPC channel 的 close() ，
                    # 该方法可能返回 None 而不是 awaitable 对象，导致 await None 报错。
                    logger.warning(f"Nacos shutdown warning (ignorable): {e}")
                else:
                    logger.error(f"Error shutting down Nacos naming service: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error shutting down Nacos naming service: {e}")
                cls.naming_service = None
