import unittest
from dspider.common.load_config import config

class TestLoadConfig(unittest.TestCase):
    def test_config(self):
        print(config)
        self.assertIsNotNone(config)
        self.assertIn('mongodb', config)
        self.assertIn('mysql', config)
        self.assertIn('rabbitmq', config)
        self.assertIn('minio', config)
        self.assertIn('master', config)
        self.assertIn('worker', config)
        self.assertIn('processor', config)
        self.assertIn('logging', config)
