from typing import Dict, Any, Tuple
from loguru import logger
import datetime
from datetime import date, datetime, timedelta
from task_backtrader.strategy.trigger.base_trigger import BaseTrigger

class RPairingTrigger(BaseTrigger):
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
        self.upper_lower = 0
        self.data_counter = 0
        self.last_rebalance_date = None
        if 'open_date' in self.params:
            start_date = self.params.get('open_date', date.today())
            if isinstance(start_date, str) and start_date != "":
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
            self.last_rebalance_date = current_date + timedelta(days=3)
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
        if 'r_pairing' not in self.trigger_params:
            return False, ""

        # 检查日期限制
        if not self._can_trigger_by_date(strategy):
            return False, "未满足交易日间隔限制"

        r_pairing_params = self.trigger_params.get('r_pairing', {})
        products = r_pairing_params.get('products', [])
        if len(products) != 2:
            return False, "产品数量不正确"

        pairing_key = products[0] + "/" + products[1]
        pairing_data = strategy.analyzers.pairing.get_pairing_data_dict(pairing_key)

        data1 = strategy.getdatabyname(products[0])
        data2 = strategy.getdatabyname(products[1])
        r_value = data1.close[0] / data2.close[0]
        if r_value > pairing_data[-1]['r_upper'] and self.upper_lower != 1:
            logger.info(f"偏离上界: {r_value} > {pairing_data[-1]['r_upper']}")
            strategy.params['target_weights'][products[0]] = 0.45
            strategy.params['target_weights'][products[1]] = 0.55
            self.upper_lower = 1
            return True, "偏离上界"
        elif r_value < pairing_data[-1]['r_lower'] and self.upper_lower != -1:
            strategy.params['target_weights'][products[0]] = 0.55
            strategy.params['target_weights'][products[1]] = 0.45
            self.upper_lower = -1
            logger.info(f"偏离下界: {r_value} < {pairing_data[-1]['r_lower']}")
            return True, "偏离下界"
        else:
            return False, "未偏离"
        
        return False, ""