import unittest
import os
import sys

from dspider.worker.spider.list_spider import PageNumRequester

# Fix the import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from test.test_data.data import jd_config, jd_config_tencent, jd_result_tencent, task_config


class TestPageNumRequester(unittest.TestCase):
    def setUp(self):
        self.external_datasource_config = jd_config_tencent
        
    def test_request(self):
        page_num_requester = PageNumRequester(self.external_datasource_config)
        resp = page_num_requester.request(21)
        print(resp.text)
        