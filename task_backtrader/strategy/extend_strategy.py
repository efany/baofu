from abc import ABC, abstractmethod
from typing import Dict, Any, List
from loguru import logger
import backtrader as bt

class ExtendStrategy(ABC):
    """
    扩展策略的基础接口类
    
    用于在BaseStrategy中管理各种扩展策略，如融资策略、对冲策略等。
    所有的扩展策略都应该继承这个基类并实现相应的接口。
    """
    
    def __init__(self, params: Dict[str, Any]):
        """
        初始化扩展策略
        
        Args:
            params: 策略参数
            broker: backtrader的broker实例
        """
        self.params = params
        self.is_initialized = False
        self.broker = None
        self.main_strategy = None

    def setBroker(self, broker):
        self.broker = broker
    
    def setMainStrategy(self, main_strategy: bt.Strategy):
        self.main_strategy = main_strategy  
    
    @abstractmethod
    def next(self) -> None:
        """执行策略逻辑，在每个交易日调用"""
        pass