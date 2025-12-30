
class ReqMethodJudger:
    def __init__(self):
        pass
    
    def judge(self, task: dict) -> str:
        pass

class ReqMethodDefaultJudger(ReqMethodJudger):
    def __init__(self):
        pass
    
    def judge(self, task: dict) -> str:
        return task['request_params']['method'].lower()

class ReqMethodHasPostJudger(ReqMethodJudger):
    def __init__(self):
        pass
    
    def judge(self, task: dict) -> str:
        if task['request_params']['postdata']:
            return 'post'
        return 'get'