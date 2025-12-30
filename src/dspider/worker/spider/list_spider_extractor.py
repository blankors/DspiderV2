import json

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