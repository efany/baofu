from typing import Dict, Any, Tuple
from loguru import logger
import datetime
from datetime import date, datetime, timedelta
from task_backtrader.strategy.trigger.base_trigger import BaseTrigger

class DeviationTrigger(BaseTrigger):
    """偏离触发器
    
    当产品权重偏离目标值达到阈值时触发再平衡。
    支持为每个产品单独设置上升和下降阈值。
    在开仓交易日或者触发再平衡后的3个交易日内不再触发。
    """
    
    def __init__(self, params: Dict[str, Any]):
        """
        初始化偏离触发器
        
        Args:
            params: 触发器配置参数，格式如下：
                {
                    "deviation": {
                        "159949.SZ": {
                            "rise": 0.1,  # 上升阈值，如0.1表示偏离10%
                            "fall": 0.1,  # 下降阈值
                        },
                        "512550.SS": {
                            "rise": 0.1,
                            "fall": 0.1,
                        }
                    }
                }
        """
        super().__init__(params)
        self.trigger_params = self.params.get('triggers', {})
        self.data_counter = 0
        self.last_rebalance_date = None
        if 'open_date' in self.params:
            start_date = self.params.get('open_date', date.today())
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            self.last_rebalance_date = start_date + timedelta(days=3)
        
        
    def _can_trigger_by_date(self, strategy) -> bool:
        """
        检查是否满足日期限制条件
        
        Args:
            strategy: 策略实例
            
        Returns:
            bool: 是否可以触发再平衡
        """
        current_date = strategy.datetime.date()
        
        # 如果没有上次再平衡日期记录，说明是首次触发，允许触发
        if self.last_rebalance_date is None:
            self.last_rebalance_date = current_date
            return True
        
        self.data_counter += 1
        if self.data_counter >= 3:
            self.data_counter = 0
            return True
        else:
            return False

    def check(self, strategy) -> Tuple[bool, str]:
        """
        检查是否触发再平衡
        
        Args:
            strategy: 策略实例
            
        Returns:
            bool: 是否触发再平衡
        """
        if 'deviation' not in self.trigger_params:
            return False, ""
            
        # 检查日期限制
        if not self._can_trigger_by_date(strategy):
            return False, "未满足交易日间隔限制"
    
        # 获取当前总资产
        total_value = strategy.get_total_asset()
        
        # 遍历deviation中的产品
        for symbol, deviation_config in self.trigger_params['deviation'].items():
            # 获取产品数据
            data = strategy.getdatabyname(symbol)
            if not data:
                continue

            # 获取当前持仓
            position = strategy.getposition(data)
            if not position:
                current_weight = 0
            else:
                current_weight = position.size * data.close[0] / total_value

            # 获取目标权重
            target_weight = float(strategy.params['target_weights'].get(symbol, 0))
            
            # 计算当前权重与目标权重的偏差
            deviation = current_weight - target_weight
            
            # 检查是否触发上升或下降条件
            rise_offset = float(deviation_config.get('rise', '0'))
            fall_offset = float(deviation_config.get('fall', '0'))
            
            if deviation > 0 and deviation > rise_offset:
                logger.info(f"日期：{strategy.datetime.date()}，产品{symbol}当前权重{current_weight:.2%}超过目标权重{target_weight:.2%}，偏差{deviation:.2%}大于上升阈值{rise_offset:.2%}，触发再平衡")
                self.last_rebalance_date = strategy.datetime.date()
                return True, f"日期：{strategy.datetime.date()}，产品{symbol}当前权重{current_weight:.2%}超过目标权重{target_weight:.2%}，偏差{deviation:.2%}大于上升阈值{rise_offset:.2%}，触发再平衡"
            elif deviation < 0 and abs(deviation) > fall_offset:
                logger.info(f"日期：{strategy.datetime.date()}，产品{symbol}当前权重{current_weight:.2%}低于目标权重{target_weight:.2%}，偏差{abs(deviation):.2%}大于下降阈值{fall_offset:.2%}，触发再平衡")
                self.last_rebalance_date = strategy.datetime.date()
                return True, f"日期：{strategy.datetime.date()}，产品{symbol}当前权重{current_weight:.2%}低于目标权重{target_weight:.2%}，偏差{abs(deviation):.2%}大于下降阈值{fall_offset:.2%}，触发再平衡"

        return False, ""