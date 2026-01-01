import datetime

system_config = {
    "task_queue_name": "task",
}
task_config = { # TODO: 转为yaml
    "task_name": "JD",
    "spider": { # 元素必须为{"spider_name": {}}
        "ListSpider": {
            "spider_name": "ListSpider", # 保留
            "p_num": 3, # 必须
            "queue_name": "list", # 必须。消费external_datasource_config的队列
            "prefetch_count": 1,
            # round: 1, # 
        },
        # "DetailSpider": {
        #     "spider_name": "DetailSpider", # 保留
        #     "p_num": 1, # 必须
        #     "queue_name": "detail", # 必须。消费list的队列
        #     "prefetch_count": 1,
        #     # round: 1, # JD爬虫的detail不需要考虑round，因为一个detail url只会抓取一次
        # }
    },
    "datasource": { # executor中
        "list_page": "list_page",
        "bucket_name": "spider-results"
    }
}
jd_config = {
    "id": "1",
    "hr_index_url": "",
    "social_index_url": "https://zhaopin.jd.com/web/job/job_info_list/3",
    "need_headers": False,
    "request_params": {
        "api_url": "https://zhaopin.jd.com/web/job/job_list",
        "headers": {
            "referer": "https://zhaopin.jd.com/web/job/job_info_list/3",
        },
        "cookies": {
            "JSESSIONID": "0D9E36EE88A43018AA117ECA03FAF083.s1"
        },
        "postdata": {
            "pageIndex": "{0}",
            "pageSize": "10",
            "workCityJson": "[]",
            "jobTypeJson": "[]",
            "jobSearch": ""
        },
        "additional": {
            "index_api_url": "",
            "index_postdata": {}
        }
    },
    "pagination": [397,1],
    "parse_rule": {
        "list_page": {
            "list_data": "result.list",
            "url_rule": {
                "url_path": "https://careers.pddglobalhr.com/jobs/detail",
                "params": {"code": "code"},
                "postdata": {}
            }
            # https://careers.pddglobalhr.com/jobs/detail?code=I020206&type=fulltime
            # url -> url + {code: I020206, type: fulltime}
            # 根据I020206，去listitem中找这个字符串，得到对应的key
            # 计算这个key在listitem json的路径：list_item[0].code
            # "url_rule": {"code": "code", "type": "type"}
            # 意思是 {list_item中的key: url中params或postdata的key}
        },
        "detail_page": {
        }
    },
    "schedule": {
        "type": "", # 
        "round": 1, # 1轮
        "interval": 10
    }
}
jd_config_pdd = {
    "id": "1",
    "hr_index_url": "",
    "social_index_url": "https://careers.pddglobalhr.com/jobs",
    "need_headers": False,
    "request_params": {
        "api_url": "https://careers.pddglobalhr.com/api/recruit/position/list",
        "headers": {
            "referer": "https://careers.pddglobalhr.com/jobs",
        },
        "cookies": {
            "JSESSIONID": "0D9E36EE88A43018AA117ECA03FAF083.s1"
        },
        "postdata": {
            "pageIndex": "{0}",
            "pageSize": "10",
            "workCityJson": "[]",
            "jobTypeJson": "[]",
            "jobSearch": ""
        },
        "additional": {
            "index_api_url": "",
            "index_postdata": {}
        }
    },
    "pagination": [397,1],
    "parse_rule": {
        "list_page": {
            "list_data": "result.list",
            "url_rule": {
                "url_path": "https://careers.pddglobalhr.com/jobs/detail",
                "params": {"code": "code"},
                "postdata": {}
            }
            # https://careers.pddglobalhr.com/jobs/detail?code=I020206&type=fulltime
            # url -> url + {code: I020206, type: fulltime}
            # 根据I020206，去listitem中找这个字符串，得到对应的key
            # 计算这个key在listitem json的路径：list_item[0].code
            # "url_rule": {"code": "code", "type": "type"}
            # 意思是 {list_item中的key: url中params或postdata的key}
        },
        "detail_page": {
        }
    },
    "schedule": {
        "type": "", # 
        "interval": 10
    }
}
jd_config_tencent = {
    "id": "1",
    "hr_index_url": "https://careers.tencent.com/home.html",
    "social_index_url": "https://careers.tencent.com/search.html?query=at_1",
    "need_headers": False,
    "request_params": {
        "method": "GET",
        "api_url": "https://careers.tencent.com/tencentcareer/api/post/Query",
        "params": {
            "timestamp": "",
            "countryId": "",
            "cityId": "",
            "bgIds": "",
            "productId": "",
            "categoryId": "",
            "parentCategoryId": "",
            "attrId": 1,
            "keyword": "",
            "pageIndex": 0,
            "pageSize": 10,
            "language": "zh-cn",
            "area": "cn"
        },
        "headers": {
            "referer": "https://careers.tencent.com/search.html?query=at_1",
        },
        "cookies": {},
        "postdata": {},
        "additional": {
            "index_api_url": "",
            "index_postdata": {}
        }
    },
    "pagination": [1,1,100],
    "parse_rule": {
        "pagenum": {
            "params": {
                "cur": "pageIndex"
            }
        },
        "list_page": {
            "list_data": "Data.Posts",
            "url_rule": {
                "url_path": "http://careers.tencent.com/jobdesc.html",
                "params": {"PostId": "postId"},
                "postdata": {}
            }
            # https://careers.pddglobalhr.com/jobs/detail?code=I020206&type=fulltime
            # url -> url + {code: I020206, type: fulltime}
            # 根据I020206，去listitem中找这个字符串，得到对应的key
            # 计算这个key在listitem json的路径：list_item[0].code
            # "url_rule": {"code": "code", "type": "type"}
            # 意思是 {list_item中的key: url中params或postdata的key}
        },
        "detail_page": {
        }
    },
    "schedule": {
        "type": "", # 
        "interval": 10,
        "round": 0,
    }
}

jd_result_tencent = {
    "Code": 200,
    "Data": {
        "Count": 2305,
        "Posts": [
            {
                "Id": 0,
                "PostId": "1978338334812037120",
                "RecruitPostId": 115312,
                "RecruitPostName": "腾讯安全-WAF产品经理-深圳",
                "CountryName": "中国",
                "LocationName": "北京",
                "BGName": "CSIG",
                "ComCode": "",
                "ComName": "",
                "ProductName": "基础安全-三部",
                "CategoryName": "产品",
                "Responsibility": "1.负责腾讯云WAF产品策划与功能设计和商业化成功，调研、挖掘并洞察市场及用户需求，为所负责子业务的商业化结果成功负责，建设业内领先的WAF产品；\n2.负责腾讯云WAF产品的迭代与需求管理，输出功能设计原型图与需求文档；协调研发团队、设计团队与测试团队，推进产品迭代有节奏、高质量落地；\n3.负责腾讯云WAF产品子业务规划，持续关注行业发展、用户需求与竞品动态，输出有行业竞争优势的产品子业务策略设计、发展规划与演进路线图；\n4.参与腾讯云WAF产品运营与商业化运营并为子业务商业成功负责，与产品运营配合，推进产品的商业化进程，提升产品收入、规模与影响力。",
                "LastUpdateTime": "2025年12月15日",
                "PostURL": "http://careers.tencent.com/jobdesc.html?postId=1978338334812037120",
                "SourceID": 1,
                "IsCollect": False,
                "IsValid": True,
                "RequireWorkYearsName": "五年以上工作经验"
            }
        ]
    }
}

url = {
    "id"
    "config_id"
    "pagenum": 3,
    "cur": 3,
    "round": 2,
    "batch": 1,
    "request_params": {
        
    }
}

round = {
    "id": "",
    "jd_config_id": "",
    "round": 1, # 什么情况作为一轮，由于数据异常的重试要算作一轮吗？
    "round_datetime": 0,
    "statistic": {
        
    }
}



list_ = {
    "id": "",
    "jd_config_id": "",
    "round": 1,
    "page": 1, # 列表的第n页
               # 实际页码还是页码参数
    "file_path": "",
    "insert_time": 0,
    
}
detail_ = {
    "in_page": 2, # 在列表的第几页
    "round": 1
}

# 历史版本
config_data = {
    "id": "5",
    "jump_from_url": "",
    "hr_index_url": "",
    "state": 0,
    'url': 'https://zhaopin.jd.com/web/job/job_info_list/3',
    'api_url': '',
    'need_headers': False,
    'request_params': {
        'api_url': "https://zhaopin.jd.com/web/job/job_list",
        'headers': {
            "referer": "https://zhaopin.jd.com/web/job/job_info_list/3"
        },
        'cookies': {
            "JSESSIONID": "0D9E36EE88A43018AA117ECA03FAF083.s1"
        },
        'data': {
            "pageIndex": "2",
            "pageSize": "10",
            "workCityJson": "[]",
            "jobTypeJson": "[]",
            "jobSearch": ""
        }
    },
    'marked_fields': {
        'page': 'pageIndex',
        'page_size': 'pageSize',
        'page_start': '',
        'page_end': '',
    },
    'parse_rule': {
        'url_rule': ''
    },
    # 插入时间 格式2023-08-01 00:00:00
    'insert_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    'update_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
}