import logging
import uuid
import time
import typing
import datetime
import json

import requests

from dspider.worker.judge_requests_method import ReqMethodHasPostJudger

if typing.TYPE_CHECKING:
    from dspider.worker.worker_temp import Executor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class PaginationGetter:
    """
    分页获取器基类
    """
    def __init__(self):
        pass
    
    def get_pagination(self, task: dict) -> list:
        """
        获取分页信息
        :param task: 任务字典
        :return: 分页信息列表。[1,1]代表起始位置与步长
        :raises KeyError: 如果任务字典中缺少pagination字段
        """
        pass

class PaginationGetterDefault(PaginationGetter):
    def __init__(self):
        pass
    
    def get_pagination(self, task: dict) -> list:
        return task['pagination']

class PaginationGetterCompute(PaginationGetter):
    def __init__(self):
        pass
    
    def get_pagination(self, task: dict) -> list:
        pass

class ListSpiderExtractor:
    def __init__(self, parse_rule_list):
        self.parse_rule_list = parse_rule_list

class ListSpiderExtractorJson(ListSpiderExtractor):
    
    def extract_list_data(self, resp_text: str):
        self.list_items_rule = self.parse_rule_list['list_data']
        resp_json = json.loads(resp_text)
        path = self.list_items_rule.split('.')
        list_items: list = resp_json
        for p in path:
            list_items = list_items.get(p)
        return list_items
    
    def extract_url(self, resp_text: str):
        list_items = self.extract_list_data(resp_text)
        return self._extract_url_handler(list_items)
        
    def extract_other(self, resp_text: str):
        pass
    
    def _extract_url_handler(self, list_items: list):
        url_rule = self.parse_rule_list['url_rule']
        url_path, params, postdata_rule = url_rule['url_path'], url_rule['params'], url_rule['postdata']
        urls = []
        postdata_list = []
        for item in list_items:
            target_url = url_path
            if postdata_rule == {}:
                target_url += '?'
                for list_data_key, url_key in params.items():
                    target_url += f"{url_key}={item.get(list_data_key)}&" # TODO: 是否要考虑参数在目标URL的位置
                target_url = target_url[:-1] # 去掉最后一个&
                item['url'] = target_url
                urls.append(target_url)
            else: # detail为post请求
                target_postdata = {}
                for list_data_key, url_key in postdata_rule.items():
                    target_postdata[url_key] = item.get(list_data_key)
                # TODO: 补充额外字段
                item['url'] = target_postdata
                postdata_list.append(target_postdata)
        return list_items
    
    def _extract_other_handler(self, list_items: list):
        pass

class ListSpiderExtractorHTML(ListSpiderExtractor):
    pass

class ListSpider:
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
