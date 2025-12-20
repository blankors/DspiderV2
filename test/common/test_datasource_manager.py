import unittest
from dspider.common.data_source_manager import DataSourceManager, data_source_type

class TestDataSourceManager(unittest.TestCase):
    def test_get_data_source_with_config(self):
        """测试根据配置获取数据源实例"""
        manager = DataSourceManager()
        data_source = manager.get_data_source_with_config(data_source_type.MONGODB.value)
        self.assertIsNotNone(data_source)
        self.assertEqual(data_source.__class__.__name__, 'MongoDBService')
        