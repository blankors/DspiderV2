import sys
import os
import unittest
import json
import datetime
from unittest.mock import Mock, MagicMock, patch

from dspider.worker.spider.list_spider import PaginationGetterDefault
from dspider.worker.spider.list_spider import ListSpider, ListSpiderExtractorJson
# from dspider.worker.worker import WorkerNode, Executor

# Fix the import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from test.test_data.data import jd_config, jd_config_tencent, jd_result_tencent, task_config

class TestPaginationGetterDefault(unittest.TestCase):
    def setUp(self):
        self.task = jd_config_tencent
        
    def test_get_pagination(self):
        pagination_getter = PaginationGetterDefault()
        pagination = pagination_getter.get_pagination(self.task)
        print(pagination)

class TestListSpider(unittest.TestCase):
    def setUp(self):
        # Setup mock dependencies
        self.executor_mock = Mock()
        self.mongodb_service_mock = Mock()
        self.minio_client_mock = Mock()
        
        # Attach mocks to executor
        self.executor_mock.task_config = task_config
        self.executor_mock.mongodb_service = self.mongodb_service_mock
        self.executor_mock.minio_client = self.minio_client_mock
        
        # Create list spider instance
        self.list_spider = ListSpider(self.executor_mock)
        
        # Sample test data
        self.sample_resp_text = json.dumps(jd_result_tencent, ensure_ascii=False)
        
        self.task = jd_config_tencent
        self.parse_rule_list = self.task['parse_rule']['list_page']
    
    @patch('requests.request')
    @patch('dspider.worker.spider.list_spider.ListSpiderExtractorJson.extract_url')
    def test_single_request_success(self, mock_extract_url, mock_request):
        """Test successful single request"""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.text = self.sample_resp_text
        mock_request.return_value = mock_resp
        mock_extract_url.return_value = ["https://example.com/1", "https://example.com/2"]
        
        statistic = {'stop_reason': '', 'last_fail': -1, 'fail': [], 'last_resp_text': ''}
        result = self.list_spider.single_request(
            "https://example.com/api", {"User-Agent": "test"}, {"page": 1}, "GET", 1, 1, statistic, self.parse_rule_list
        )
        
        mock_request.assert_called_once()
        self.assertEqual(result, mock_resp)
        self.assertEqual(statistic["total"], 1)
        self.assertEqual(statistic["success"], 1)
        self.assertEqual(statistic["last_resp_text"], self.sample_resp_text)

    @patch('requests.request')
    def test_single_request_failure(self, mock_request):
        """Test failed single request"""
        mock_resp = Mock()
        mock_resp.status_code = 404
        mock_resp.text = "Not Found"
        mock_request.return_value = mock_resp
        
        statistic = {'stop_reason': '', 'last_fail': -1, 'fail': [], 'last_resp_text': ''}
        result = self.list_spider.single_request(
            "https://example.com/api", {"User-Agent": "test"}, {"page": 1}, "GET", 1, 1, statistic, self.parse_rule_list
        )
        
        mock_request.assert_called_once()
        self.assertIsNone(result)
        self.assertEqual(statistic["total"], 1)
        self.assertEqual(statistic["fail"], [1])
        self.assertEqual(statistic["last_fail"], 1)

    @patch('requests.request')
    @patch('dspider.worker.spider.list_spider.ListSpiderExtractorJson.extract_url')
    def test_single_request_duplicate_content(self, mock_extract_url, mock_request):
        """Test single request with duplicate content"""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.text = self.sample_resp_text
        mock_request.return_value = mock_resp
        mock_extract_url.return_value = ["https://example.com/1"]
        
        statistic = {'stop_reason': '', 'last_fail': -1, 'fail': [], 'last_resp_text': self.sample_resp_text}
        result = self.list_spider.single_request(
            "https://example.com/api", {"User-Agent": "test"}, {"page": 2}, "GET", 2, 1, statistic, self.parse_rule_list
        )
        
        mock_request.assert_called_once()
        self.assertIsNone(result)
        self.assertEqual(statistic["total"], 1)
        self.assertEqual(statistic["success"], 1)
        self.assertEqual(statistic["stop_reason"], "重复页响应内容，最后成功页：2")

    @patch('requests.request')
    def test_single_request_consecutive_failure(self, mock_request):
        """Test consecutive failed requests"""
        mock_resp = Mock()
        mock_resp.status_code = 404
        mock_resp.text = "Not Found"
        mock_request.return_value = mock_resp
        
        statistic = {'stop_reason': '', 'last_fail': 1, 'fail': [1], 'last_resp_text': ''}
        result = self.list_spider.single_request(
            "https://example.com/api", {"User-Agent": "test"}, {"page": 2}, "GET", 2, 1, statistic, self.parse_rule_list
        )
        
        mock_request.assert_called_once()
        self.assertIsNone(result)
        self.assertEqual(statistic["total"], 1)
        self.assertEqual(statistic["fail"], [1, 2])
        self.assertEqual(statistic["stop_reason"], "连续页请求失败，最后失败页：2")

    @patch.object(ListSpider, 'has_new_detail_url')
    @patch.object(ListSpider, 'single_request')
    def test_start(self, mock_single_request, mock_has_new_detail_url):
        """Test start method"""
        self.list_spider.req_method_judger.judge = Mock(return_value="GET")
        self.list_spider.pagination_getter.get_pagination = Mock(return_value=[1, 1])
        self.list_spider.get_page_filed = Mock(return_value={"location": "api_url", 'key': ''})
        
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.text = self.sample_resp_text
        mock_single_request.return_value = mock_resp
        def mock_single_request_side_effect(api_url, headers, postdata, req_method, cur, step, statistic, parse_rule_list):
            # Simulate the modifications to statistic that single_request would make
            statistic['total'] = statistic.get('total', 0) + 1
            statistic['success'] = statistic.get('success', 0) + 1
            statistic['last_resp_text'] = mock_resp.text
            return mock_resp
        
        mock_single_request.side_effect = mock_single_request_side_effect
        
        mock_has_new_detail_url.return_value = False
        
        self.executor_mock.minio_client.upload_text.return_value = True
        
        statistic = self.list_spider.start(self.task)
        
        # 实际测试
        print(statistic)
        mock_single_request.assert_called()
        self.assertEqual(statistic["total"], 1)
        self.assertEqual(statistic["success"], 1)
    
    # @patch('dspider.worker.spider.list_spider.time.sleep')
    # @patch.object(ListSpider, 'save')
    # @patch.object(ListSpider, 'has_new_detail_url')
    # @patch.object(ListSpider, 'single_request')
    # @patch.object(ListSpider, 'get_page_filed')
    # def test_start(self, mock_get_page_filed, mock_single_request, mock_has_new_detail_url, mock_save, mock_sleep):
    #     """Test start method"""
    #     # Setup mock return values
    #     self.req_method_judger_mock.judge.return_value = "GET"
    #     self.pagination_getter_mock.get_pagination.return_value = [1, 1]
    #     mock_get_page_filed.return_value = {"location": "api_url", "key": ""}
        
    #     # Mock the single_request response with side_effect to modify statistic
    #     mock_resp = Mock()
    #     mock_resp.status_code = 200
    #     mock_resp.text = self.sample_resp_text
        
    #     def mock_single_request_side_effect(api_url, headers, postdata, req_method, cur, step, statistic, parse_rule_list):
    #         # Simulate the modifications to statistic that single_request would make
    #         statistic['total'] = statistic.get('total', 0) + 1
    #         statistic['success'] = statistic.get('success', 0) + 1
    #         statistic['last_resp_text'] = mock_resp.text
    #         return mock_resp
        
    #     mock_single_request.side_effect = mock_single_request_side_effect
        
    #     # Mock the extract_url method
    #     mock_extracted_urls = [{"url": "https://example.com/1"}, {"url": "https://example.com/2"}]
    #     with patch('dspider.worker.spider.list_spider.ListSpiderExtractorJson') as mock_extractor_class:
    #         mock_extractor_instance = Mock()
    #         mock_extractor_instance.extract_url.return_value = mock_extracted_urls
    #         mock_extractor_class.return_value = mock_extractor_instance
            
    #         # Mock has_new_detail_url
    #         mock_has_new_detail_url.return_value = False  # This will cause the loop to break after one iteration
            
    #         # Mock the storage methods
    #         self.minio_client_mock.upload_text.return_value = True
    #         self.mongodb_service_mock.insert_one.return_value = Mock(acknowledged=True)
    #         mock_save.return_value = True
            
    #         # Call start method
    #         result_statistic = self.list_spider.start(self.task)
            
    #         # Verify all mocks were called
    #         self.req_method_judger_mock.judge.assert_called_once_with(self.task)
    #         self.pagination_getter_mock.get_pagination.assert_called_once_with(self.task)
    #         mock_get_page_filed.assert_called_once_with(self.task)
    #         mock_single_request.assert_called_once()
    #         mock_extractor_class.assert_called_once_with(self.task['parse_rule']['list_page'])
    #         mock_extractor_instance.extract_url.assert_called_once_with(self.sample_resp_text)
    #         mock_has_new_detail_url.assert_called_once_with(mock_extracted_urls)
    #         self.minio_client_mock.upload_text.assert_called_once()
    #         self.mongodb_service_mock.insert_one.assert_called_once()
    #         mock_save.assert_called_once()
    #         mock_sleep.assert_called_once_with(5)
            
    #         # Verify statistic is returned and contains expected data
    #         self.assertIsInstance(result_statistic, dict)
    #         self.assertIn('total', result_statistic)
    #         self.assertIn('success', result_statistic)
    #         self.assertIn('stop_reason', result_statistic)
    #         self.assertEqual(result_statistic['total'], 1)
    #         self.assertEqual(result_statistic['success'], 1)
    #         self.assertEqual(result_statistic['stop_reason'], "无新详情页，最后请求页：1")

if __name__ == '__main__':
    unittest.main()