from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Dict, Any, List, Tuple
from loguru import logger

class BaseTrigger(ABC):
    """触发器基类
    
    所有触发器都需要继承这个基类，并实现check方法来判断是否触发再平衡。
    """
    
    def __init__(self, params: Dict[str, Any]):
        """
        初始化触发器
        
        Args:
            params: 触发器配置参数
        """
        self.params = params
        
    def open_trade(self, strategy):
        """开仓"""
        return False, ""

    def close_trade(self, strategy):
        """平仓"""
        return False, ""
        
    @abstractmethod 
    def check(self, strategy) -> Tuple[bool, str]:
        """
        检查是否触发再平衡，并返回触发再平衡的原因  
        
        Args:
            strategy: 策略实例，用于获取当前状态和数据
            
        Returns:
            bool: 是否触发再平衡
            str: 触发再平衡的原因
        """
        pass 