from typing import Dict, Any, List
from loguru import logger

from task_backtrader.strategy.trigger.base_trigger import BaseTrigger
from task_backtrader.strategy.trigger.date_trigger import DateTrigger
from task_backtrader.strategy.trigger.deviation_trigger import DeviationTrigger
from task_backtrader.strategy.trigger.sorting_trigger import SortingTrigger

class TriggerFactory:
    """触发器工厂类
    
    负责创建和管理各种触发器实例。
    """
    
    @staticmethod
    def create_triggers(params: Dict[str, Any]) -> List[BaseTrigger]:
        """
        根据配置创建触发器列表
        
        Args:
            params: 策略参数，包含triggers配置
            
        Returns:
            List[BaseTrigger]: 触发器列表
        """
        triggers = []
        
        if 'triggers' not in params:
            logger.warning("未配置触发器")
            return triggers
            
        trigger_config = params['triggers']

        if 'sorting' in trigger_config:
            triggers.append(SortingTrigger(params))
        
        # 创建日期触发器
        if 'dates' in trigger_config or 'period' in trigger_config:
            triggers.append(DateTrigger(params))
            
        # 创建偏离触发器
        if 'deviation' in trigger_config:
            triggers.append(DeviationTrigger(params))
            
        return triggers 