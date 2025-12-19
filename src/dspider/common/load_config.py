import yaml
import logging
import os
from typing import Dict, Any

logger = logging.getLogger(__name__)

default = 'test'
env = os.getenv('dspider_env', default)
if env is None:
    logger.warning(f"环境变量 dspider_env 未设置，默认使用 {default} 环境")

def load_yaml(config_path: str) -> Dict[str, Any]:
    """加载YAML配置文件"""
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        logger.error(f"配置文件 {config_path} 未找到")
        raise
    except yaml.YAMLError as e:
        logger.error(f"加载配置文件 {config_path} 失败: {str(e)}")
        raise

config = load_yaml(f'config/{env}.yaml')

