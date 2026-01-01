import logging
import uuid
import time
import typing
import datetime
import json

import requests

from dspider.worker.spider.judge_requests_method import ReqMethodHasPostJudger
from dspider.worker.spider.pagination_getter import PaginationGetterDefault
from dspider.worker.spider.list_spider_extractor import ListSpiderExtractorJson

if typing.TYPE_CHECKING:
    from dspider.worker.worker_temp import Executor

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    handlers=[logging.StreamHandler()]
                    )
logger = logging.getLogger(__name__)

class ListSpider:
    def __init__(self, executor: 'Executor'):
        self.executor = executor
        self.mongodb_service = executor.mongodb_service
        self.minio_client = executor.minio_service
        
        self.list_collection_name = executor.task_config['datasource']['list_page']
        self.bucket_name = executor.task_config['datasource']['bucket_name']
    
    def start(self, request_params: list):
        for request_param in request_params:
            try_times = 3
            while True:
                try:
                    self.process_one(request_param=request_param)
                    break
                except Exception as e:
                    logger.exception(e)
            
    def process_one(self, request_param: dict):
        try:
            api_url, headers, postdata, request_method, cur, step, statistic, parse_rule_list = request_param
            resp = self.request(api_url, headers, postdata, request_method, cur, step, statistic, parse_rule_list)
        except Exception as e:
            raise
        
        try:
            if not self.is_resp_ok(resp=resp):
                raise
            extractor = ListSpiderExtractorJson(parse_rule_list) # Todo: 列表页一般情况下返回格式（json还是html）都是统一的
            urls = extractor.extract_url(resp.text)
            has = self.has_new_detail_url(urls)
            if not has:
                statistic['stop_reason'] = f"无新详情页，最后请求页：{cur}"
            save_info = self.get_save_info(external_datasource_config, resp.text, cur)
            self.store_to_minio(save_info['filepath'], resp.text) # 一致性：如果save失败，minio中也会有数据
            save_success = self.save(save_info)
        except Exception as e:
            logger

    def request(self, api_url, headers, postdata, req_method, cur, step, statistic, parse_rule_list):
        resp = requests.request(req_method, api_url, headers=headers, data=postdata)
        return resp
    
    def is_resp_ok(self, resp):
        if resp.status_code == 200:
            logger.info('请求成功')
            return True
        return False

    def start(self, external_datasource_config: dict):
        datasource_id = external_datasource_config.get('_id', str(uuid.uuid4()))
        logger.info(f"[{self.executor.executor_id}] 开始执行任务 {datasource_id}")
        logger.debug(f"[{self.executor.executor_id}] 任务参数 {external_datasource_config}")
     
        request_params = external_datasource_config['request_params']
        parse_rule_list = external_datasource_config['parse_rule']['list_page']
        
        request_method, api_url, headers, postdata_template = request_params['request_method'], request_params['api_url'], request_params['headers'], request_params['postdata']
        postdata = postdata_template.copy()
        
        pagination = external_datasource_config['pagination']
        start, cur, step, end = pagination[0], pagination[0], pagination[1], pagination[2]
        
        additional_params = request_params['additional']
        if additional_params['index_api_url'] != '' or \
            additional_params['index_postdata'] != {}:
                index_api_url = additional_params['index_api_url']
                index_postdata = additional_params['index_postdata']
                # TODO 处理首页特殊情况
        
        page_filed = self.get_page_filed(external_datasource_config)
        statistic = {
            'stop_reason': '',
            'last_fail': -1,
            'fail': [],
            'last_resp_text': '',
            'total': 0,
            'success': 0,
        }

        while True:
            if page_filed['location'] == 'api_url':
                api_url = api_url.format(cur)
            elif page_filed['location'] == 'postdata':
                postdata[page_filed['key']] = postdata_template[page_filed['key']].format(cur)
            
            resp = self.single_request(api_url, headers, postdata, request_method, cur, step, statistic, parse_rule_list)
            
            if not resp:
                break
            else:
                extractor = ListSpiderExtractorJson(parse_rule_list) # Todo: 列表页一般情况下返回格式（json还是html）都是统一的
                urls = extractor.extract_url(resp.text)
                has = self.has_new_detail_url(urls)
                if not has:
                    statistic['stop_reason'] = f"无新详情页，最后请求页：{cur}"
                    break
                save_info = self.get_save_info(external_datasource_config, resp.text, cur)
                self.store_to_minio(save_info['filepath'], resp.text) # 一致性：如果save失败，minio中也会有数据
                save_success = self.save(save_info)
            
            cur += step
            time.sleep(5)
        
        return statistic

class PageNumRequester:
    def __init__(self, external_datasource_config):
        self.request_params = external_datasource_config['request_params']
        self.parse_rule = external_datasource_config['parse_rule']
        
        self.pagenum = self.parse_rule['pagenum']
        
        self.req_method = self.request_params['method'].upper()
        self.api_url = self.request_params['api_url']
        self.headers = self.request_params['headers']
        self.params_template = self.request_params.get('params', {})
        self.postdata_template = self.request_params.get('postdata', {})

        pagination = external_datasource_config['pagination']
        start, cur, step = pagination[0], pagination[0], pagination[1]
        
        self.set_request_func(self.pagenum)
        
    def set_request_func(self, pagenum):
        page_in_params = pagenum.get('params')
        page_in_postdata = pagenum.get('postdata')
        if page_in_params and page_in_postdata:
            pass
        else:
            if page_in_params:
                if page_in_params.get('cur') and page_in_params.get('end'):
                    pass
                else:
                    if page_in_params.get('cur'):
                        self.cur_field = page_in_params['cur']
                        if self.req_method == 'GET':
                            self.request_func = self.get_dynamic_params
                        elif self.req_method == 'POST':
                            self.request_func = self.post_dynamic_params
                    elif page_in_params.get('end'):
                        pass
            elif page_in_postdata:
                if page_in_postdata.get('cur') and page_in_postdata.get('end'):
                    pass
                else:
                    if page_in_postdata.get('cur'):
                        self.cur_field = page_in_postdata['cur']
                        if self.params_template == {}:
                            self.request_func = self.post_dynamic_postdata_no_params
                        else:
                            self.request_func = self.post_dynamic_postdata_with_params
                    elif page_in_postdata.get('end'):
                        pass
                    
    def fill_page_by_postdata(self, page: int):
        t = self.postdata_template.copy()
        t.update(page)
        return t

    def fill_page_by_params(self, page: int):
        t = self.params_template.copy()
        t[self.cur_field] = page
        return t

    def fill_page_by_postdata_params(self, page: int):
        t = self.postdata_template.copy()
        t.update(page)
        return t
            
    def get_dynamic_params(self, page):
        params = self.fill_page_by_params(page)
        resp = requests.get(self.api_url, params=params, headers=self.headers)
        return resp

    def post_dynamic_params(self, page):
        params = self.fill_page_by_params(page)
        resp = requests.post(self.api_url, params=params, data=self.postdata_template, headers=self.headers)
        return resp
        
    def post_dynamic_postdata_no_params(self, page):
        postdata = self.fill_page_by_postdata(page)
        resp = requests.post(self.api_url, data=postdata, headers=self.headers)
        return resp

    def post_dynamic_postdata_with_params(self, page):
        postdata = self.fill_page_by_postdata(page)
        resp = requests.post(self.api_url, params=self.params_template, data=postdata, headers=self.headers)
        return resp
        
    def request(self, page: int):
        resp = self.request_func(page)
        return resp
    
class ShortListSpider:
    def __init__(self, executor: 'Executor'):
        self.executor = executor
        self.mongodb_service = executor.mongodb_service
        self.minio_client = executor.minio_client
        self.list_collection_name = executor.task_config['datasource']['list_page']
        self.bucket_name = executor.task_config['datasource']['bucket_name']

        self.logger = logging.getLogger(__name__)
    
    def start(self, external_datasource_config: dict):
        datasource_id = external_datasource_config.get('_id', str(uuid.uuid4()))
        self.logger.info(f"[{self.executor.executor_id}] 开始执行任务 {datasource_id}")
        self.logger.debug(f"[{self.executor.executor_id}] 任务参数 {external_datasource_config}")
     
        request_params = external_datasource_config['request_params']
        parse_rule_list = external_datasource_config['parse_rule']['list_page']
        pagenum = parse_rule_list['pagenum']
        
        req_method, api_url, headers, params_template, postdata_template = request_params['request_method'], request_params['api_url'], request_params['headers'], request_params['params'], request_params['postdata']
        postdata = postdata_template.copy()
        
        pagination = external_datasource_config['pagination']
        start, cur, step = pagination[0], pagination[0], pagination[1]
        
        additional_params = request_params['additional']
        if additional_params['index_api_url'] != '' or \
            additional_params['index_postdata'] != {}:
                index_api_url = additional_params['index_api_url']
                index_postdata = additional_params['index_postdata']
                # TODO 处理首页特殊情况
        
        page_filed = self.get_page_filed(external_datasource_config)
        statistic = {
            'stop_reason': '',
            'last_fail': -1,
            'fail': [],
            'last_resp_text': '',
            'total': 0,
            'success': 0,
        }

        while True:

                
            
            resp = self.single_request(api_url, headers, postdata, req_method, cur, step, statistic, parse_rule_list)
            
            if not resp:
                break
            else:
                extractor = ListSpiderExtractorJson(parse_rule_list) # Todo: 列表页一般情况下返回格式（json还是html）都是统一的
                urls = extractor.extract_url(resp.text)
                has = self.has_new_detail_url(urls)
                if not has:
                    statistic['stop_reason'] = f"无新详情页，最后请求页：{cur}"
                    break
                save_info = self.get_save_info(external_datasource_config, resp.text, cur)
                self.store_to_minio(save_info['filepath'], resp.text) # 一致性：如果save失败，minio中也会有数据
                save_success = self.save(save_info)
            
            cur += step
            time.sleep(5)
        
        return statistic    

class ListSpiderTEMP:
    def __init__(self, executor: 'Executor'):
        self.executor = executor
        self.mongodb_service = executor.mongodb_service
        self.minio_client = executor.minio_client
        self.list_collection_name = executor.task_config['datasource']['list_page']
        self.bucket_name = executor.task_config['datasource']['bucket_name']
        
        self.req_method_judger = ReqMethodHasPostJudger()
        self.pagination_getter = PaginationGetterDefault()
        self.logger = logging.getLogger(__name__)
    
    def start(self, external_datasource_config: dict):
        datasource_id = external_datasource_config.get('_id', str(uuid.uuid4()))
        self.logger.info(f"[{self.executor.executor_id}] 开始执行任务 {datasource_id}")
        self.logger.debug(f"[{self.executor.executor_id}] 任务参数 {external_datasource_config}")
     
        request_params = external_datasource_config['request_params']
        parse_rule_list = external_datasource_config['parse_rule']['list_page']
        
        api_url, headers, postdata_template = request_params['api_url'], request_params['headers'], request_params['postdata']
        postdata = postdata_template.copy()
        
        req_method = self.req_method_judger.judge(external_datasource_config)
        pagination = self.pagination_getter.get_pagination(external_datasource_config)
        start, cur, step = pagination[0], pagination[0], pagination[1]
        
        additional_params = request_params['additional']
        if additional_params['index_api_url'] != '' or \
            additional_params['index_postdata'] != {}:
                api_url = additional_params['index_api_url']
                postdata = additional_params['index_postdata']
        
        page_filed = self.get_page_filed(external_datasource_config)
        statistic = {
            'stop_reason': '',
            'last_fail': -1,
            'fail': [],
            'last_resp_text': '',
            'total': 0,
            'success': 0,
        }

        while True:
            if page_filed['location'] == 'api_url':
                api_url = api_url.format(cur)
            elif page_filed['location'] == 'postdata':
                postdata[page_filed['key']] = postdata_template[page_filed['key']].format(cur)
            
            resp = self.single_request(api_url, headers, postdata, req_method, cur, step, statistic, parse_rule_list)
            
            if not resp:
                break
            else:
                extractor = ListSpiderExtractorJson(parse_rule_list) # Todo: 列表页一般情况下返回格式（json还是html）都是统一的
                urls = extractor.extract_url(resp.text)
                has = self.has_new_detail_url(urls)
                if not has:
                    statistic['stop_reason'] = f"无新详情页，最后请求页：{cur}"
                    break
                save_info = self.get_save_info(external_datasource_config, resp.text, cur)
                self.store_to_minio(save_info['filepath'], resp.text) # 一致性：如果save失败，minio中也会有数据
                save_success = self.save(save_info)
            
            cur += step
            time.sleep(5)
        
        return statistic

    def get_save_info(self, external_datasource_config: dict, resp_text: str, cur: int):
        """ 
        输入：任务信息+响应信息
        输出：
        
        :param task: 任务字典
        :return: None
        """
        datasource_id = external_datasource_config.get('_id', str(uuid.uuid4()))
        schedule = external_datasource_config['schedule']
        round = schedule['round']
        import hashlib
        md5 = hashlib.md5(resp_text.encode('utf-8')).hexdigest()
        filepath = f"{datetime.datetime.now().strftime('%Y/%m/%d')}/{round}/{datasource_id}_{md5}.txt" # 示例：2023/08/25/123456.txt
        return {
            'datasource_id': datasource_id,
            'round': round,
            'filepath': filepath,
            'insert_time': datetime.datetime.now(),
            'page': cur,
        }

    def single_request(self, api_url, headers, postdata, req_method, cur, step, statistic, parse_rule_list):
        resp = requests.request(req_method, api_url, headers=headers, data=postdata)
        statistic['total'] = statistic.get('total', 0) + 1
        last_fail = statistic.get('last_fail')
        last_resp_text = statistic.get('last_resp_text')
        
        if resp.status_code == 200:
            statistic['success'] = statistic.get('success', 0) + 1
            if resp.text == last_resp_text:
                statistic['stop_reason'] = f"重复页响应内容，最后成功页：{cur}"
                return None
            else:
                statistic['last_resp_text'] = resp.text
                # urls = self.get_urls(resp, parse_rule_list)
                return resp
        else:
            statistic['fail'].append(cur)
            statistic['last_fail'] = cur
            if last_fail + step == cur: # 有连续页面请求失败
                statistic['stop_reason'] = f"连续页请求失败，最后失败页：{cur}"
                return None
            
    def has_new_detail_url(self, urls: list):
        return True
    
    def get_urls(self, resp, parse_rule_list) -> list:
        resp_json = json.loads(resp.text)
        list_items_rule = parse_rule_list['list_data']
        path = list_items_rule.split('.')
        list_items: list = resp_json
        for p in path:
            list_items = list_items.get(p)
        
        url_rule = parse_rule_list['url_rule']
        url_path, params, postdata_rule = url_rule['url_path'], url_rule['params'], url_rule['postdata']
        for item in list_items:
            target_url = url_path
            if postdata_rule == {}: # detail为get请求
                target_url += '?'
                for list_data_key, url_key in params.items():
                    target_url += f"{url_key}={item.get(list_data_key)}&" # TODO: 是否要考虑参数在目标URL的位置
                target_url = target_url[:-1] # 去掉最后一个&
                item['url'] = target_url
            else: # detail为post请求
                target_postdata = {}
                for list_data_key, url_key in postdata_rule.items():
                    target_postdata[url_key] = item.get(list_data_key)
                # TODO: 补充额外字段
                item['url'] = target_postdata
        print(list_items)

    def get_page_filed(self, datasource_config: dict):
        api_url = datasource_config['request_params']['api_url']
        postdata = datasource_config['request_params']['postdata']
        
        if '{0}' in api_url:
            return {
                'location': 'api_url',
                'key': ''
            }
        for k, v in postdata.items():
            if '{0}' in v:
                return {
                    'location': 'postdata',
                    'key': k
                }
        
    def store_to_minio(self, object_name: str, content: str) -> bool:
        success = self.minio_client.upload_text(self.bucket_name, object_name, content)
        self.logger.info(f"存储到MinIO成功: {object_name} {success}")
        return success

    def save(self, save_info: dict) -> bool:
        """将列表页路径保存到MongoDB
        
        Args:
            filepath: 列表页路径
            
        Returns:
            bool: 是否成功保存
        """
        collection = self.mongodb_service.get_collection(self.list_collection_name)
        result = collection.insert_one(save_info)
        return result.acknowledged
