import unittest
from unittest.mock import patch
from unittest.mock import AsyncMock
from unittest.mock import Mock

from dspider.worker.master import Master

class TestMaster(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.mongodb_service = Mock()
        self.nacos_service = AsyncMock()
        self.redis_service = Mock()
        self.rabbitmq_service = Mock()
        
        self.master = Master(self.nacos_service, self.mongodb_service, self.redis_service, self.rabbitmq_service)
        
    def test_get_tasks(self):
        self.mongodb_service.find.return_value = [{"task_id": "1", "status": 0}]
        tasks = self.master.get_tasks()
        self.assertIsInstance(tasks, list)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["task_id"], "1")
        self.assertEqual(tasks[0]["status"], 0)

    async def test_schedule_once(self):
        self.mongodb_service.find.return_value = [
                                                    {"task_id": "1", "status": 0},
                                                    {"task_id": "2", "status": 0},
                                                    {"task_id": "3", "status": 0},
                                                    {"task_id": "4", "status": 0},
                                                    {"task_id": "5", "status": 0},
                                                ]
        self.master.nacos_service.get_instances.return_value = [
                                                                {"ip": "127.0.0.1", "port": 8080}, 
                                                                {"ip": "127.0.0.2", "port": 8080},
                                                                {"ip": "127.0.0.3", "port": 8080},
                                                               ]
        self.master.redis_service.get.return_value = "worker_status"
        self.master.rabbitmq_service.publish.return_value = None
        
        with patch("requests.post") as mock_post:
            mock_post.return_value.status_code = 200
            await self.master.schedule_once()

    def test_start(self):
        self.mongodb_service.find.return_value = [
                                                    {"task_id": "1", "status": 0},
                                                    {"task_id": "2", "status": 0},
                                                    {"task_id": "3", "status": 0},
                                                    {"task_id": "4", "status": 0},
                                                    {"task_id": "5", "status": 0},
                                                ]
        self.master.nacos_service.get_instances.return_value = [
                                                                {"ip": "127.0.0.1", "port": 8080}, 
                                                                {"ip": "127.0.0.2", "port": 8080},
                                                                {"ip": "127.0.0.3", "port": 8080},
                                                               ]
        self.master.redis_service.get.return_value = "worker_status"
        self.master.rabbitmq_service.publish.return_value = None
        self.master.start()
