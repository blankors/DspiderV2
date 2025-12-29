import unittest
from dspider.common.nacos_service import NacosService
from dspider.common.get_env import SERVICE_NAME, SERVICE_PORT, SERVICE_IP

class TestNacosService(unittest.IsolatedAsyncioTestCase):
    
    async def asyncSetUp(self):
        await NacosService.ainit()
    
    async def asyncTearDown(self):
        await NacosService.close()
    
    async def test_reg_nacos(self):
        await NacosService.reg_nacos(
            service_name=SERVICE_NAME,
            ip=SERVICE_IP,
            port=SERVICE_PORT,
        )
    
    async def test_get_instances(self):
        await NacosService.reg_nacos(
            service_name=SERVICE_NAME,
            ip=SERVICE_IP,
            port=SERVICE_PORT,
        )
        instances = await NacosService.get_instances()
        self.assertGreater(len(instances), 0)
