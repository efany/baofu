from typing import Dict, Any, Tuple
from datetime import date, datetime, timedelta
from loguru import logger
import pandas as pd

from task_backtrader.strategy.trigger.base_trigger import BaseTrigger

class SortingTrigger(BaseTrigger):
    """因子排序触发器
    
    根据传入的因子数据，在指定的周期对产品进行排序，
    并根据排序结果更新各产品的目标权重。
    """
    
    def __init__(self, params: Dict[str, Any]):
        """
        初始化排序触发器
        
        Args:
            params: 触发器配置参数，格式如下：
                {
                    "sorting": {
                        "freq": "month",  # 触发周期：day/week/month
                        "day": 1,            # 在周期的第几天触发，比如每月第1天
                        "factor": "MA120",  # 用于排序的因子字段名
                        "ascending": False,   # 排序方向，True为升序，False为降序
                        "top_n": 2,          # 选取排名前几的产品
                        "weights": [0.6, 0.4]  # 各排名对应的权重列表
                    }
                }
        """
        super().__init__(params)
        self.trigger_params = self.params.get('triggers', {}).get('sorting', {})
        self.last_trigger_date = None
        self.freq = self.trigger_params.get('freq', 'month')
        self.trigger_day = self.trigger_params.get('day', 1)
        self.factor = self.trigger_params.get('factor', '')
        self.ascending = self.trigger_params.get('ascending', False)
        self.top_n = self.trigger_params.get('top_n', 2)
        self.weights = self.trigger_params.get('weights', [0.6, 0.4])
        
        # 验证权重列表长度与top_n一致
        if len(self.weights) != self.top_n:
            raise ValueError(f"权重列表长度({len(self.weights)})与top_n({self.top_n})不一致")
            
        # 验证权重之和为1
        if abs(sum(self.weights) - 1.0) > 1e-6:
            raise ValueError(f"权重之和({sum(self.weights)})不等于1")
            
        self.rebalance_dates: Set[str] = set()  # 用set存储再平衡日期，提高查找效率
        self._init_dates()
        
    def _init_dates(self):
        """初始化再平衡日期"""
        trigger_params = self.params.get('triggers', {})

        # 处理周期触发
        if 'sorting' in trigger_params:
            sorting = trigger_params['sorting']
            freq = sorting.get('freq', 'month')
            day = sorting.get('day', -1)  # 默认为每周期最后一天
            
            # 获取数据的起止日期
            print(f"self.params:{self.params}")
            start_date = self.params.get('open_date', date.today())
            print(f"start_date:{start_date}")
            end_date = self.params.get('close_date', date.today())
            print(f"end_date:{end_date}")
            
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            logger.info(f"start_date:{start_date}, end_date:{end_date}")
            
            current_date = start_date + timedelta(days=7)
            while current_date <= end_date:
                if freq == 'month':
                    # 每月触发
                    if day > 0 and current_date.day == day:
                        self.rebalance_dates.add(current_date.strftime('%Y-%m-%d'))
                    elif day < 0 and (current_date + timedelta(days=1)).month != current_date.month:
                        self.rebalance_dates.add(current_date.strftime('%Y-%m-%d'))
                elif freq == 'quarter':
                    # 每季度触发
                    if current_date.month in [3, 6, 9, 12]:
                        if day > 0 and current_date.day == day:
                            self.rebalance_dates.add(current_date.strftime('%Y-%m-%d'))
                        elif day < 0 and (current_date + timedelta(days=1)).month != current_date.month:
                            self.rebalance_dates.add(current_date.strftime('%Y-%m-%d'))
                elif freq == 'year':
                    # 每年触发
                    if current_date.month == 12:
                        if day > 0 and current_date.day == day:
                            self.rebalance_dates.add(current_date.strftime('%Y-%m-%d'))
                        elif day < 0 and (current_date + timedelta(days=1)).year != current_date.year:
                            self.rebalance_dates.add(current_date.strftime('%Y-%m-%d'))
                current_date += timedelta(days=1)
        
        logger.info(f"再平衡日期列表: {self.rebalance_dates}")

    def _calculate_price_deviations(self, strategy) -> Dict[str, float]:
        """
        计算每个产品当前价格与因子值的偏移百分比
        
        Args:
            strategy: 策略实例
            
        Returns:
            Dict[str, float]: 产品代码到偏移百分比的映射，偏移百分比为(price - factor) / factor
        """
        deviations = {}
        for symbol in strategy.params['target_weights'].keys():
            data = strategy.getdatabyname(symbol)
            if not data or not hasattr(data, self.factor):
                logger.warning(f"产品{symbol}没有因子{self.factor}数据")
                continue
                
            current_price = data.close[0]  # 当前价格
            factor_value = getattr(data, self.factor)[0]  # 因子值
            
            # 如果因子值无效（比如MA计算窗口不足时为-1）或为0，跳过
            if factor_value <= 0:
                logger.warning(f"产品{symbol}的因子{self.factor}值无效: {factor_value}")
                continue
                
            # 计算偏移百分比
            deviation = (current_price - factor_value) / factor_value
            deviations[symbol] = deviation
            logger.debug(f"产品{symbol}当前价格{current_price:.4f}，"
                      f"因子值{factor_value:.4f}，"
                      f"偏移率{deviation:.2%}")
                
        return deviations

    def _adjust_weights_by_deviations(self, strategy, deviations: Dict[str, float]) -> Dict[str, float]:
        """
        根据价格偏移情况调整各产品的目标权重
        
        Args:
            deviations: Dict[str, float] - 产品代码到偏移百分比的映射
            
        Returns:
            Dict[str, float]: 产品代码到目标权重的映射
        """
        # 如果没有有效的偏移数据，返回空字典
        if not deviations:
            return {}
            
        # 按照偏移值排序，ascending参数决定升序还是降序
        sorted_products = sorted(
            strategy.params['target_weights'].keys(),
            key=lambda x: deviations.get(x, 99999),
            reverse=self.ascending
        )
        
        # 选取前top_n个产品
        selected_products = sorted_products[:self.top_n]
        
        # 构建新的权重字典
        new_weights = {}
        for symbol, weight in zip(selected_products, self.weights):
            new_weights[symbol] = weight
            logger.debug(f"产品{symbol}分配权重{weight:.2%}")

        return new_weights
    
    def open_trade(self, strategy):
        """开仓"""
        logger.info(f"开仓")
        need_rebalance, new_weights = self.check_rebalance(strategy)
        if need_rebalance:
            strategy.params['target_weights'] = new_weights
        return False, f"开仓，{self.factor}因子排序"

    def check(self, strategy) -> Tuple[bool, str]:
        """
        检查是否触发再平衡
        
        Args:
            strategy: 策略实例
            
        Returns:
            Tuple[bool, str]: (是否触发再平衡, 触发原因)
        """
        last_date = strategy.data0.datetime.date(-1)
        current_date = strategy.data0.datetime.date(0)
        
        # 获取所有小于等于当前日期的再平衡日期
        valid_dates = [d for d in self.rebalance_dates if datetime.strptime(d, '%Y-%m-%d').date() <= current_date]
        
        # 如果有有效日期
        if valid_dates:
            # 获取最大的有效日期
            latest_valid_date = max(valid_dates, key=lambda d: datetime.strptime(d, '%Y-%m-%d').date())
            # 如果这个日期大于上次再平衡日期
            if datetime.strptime(latest_valid_date, '%Y-%m-%d').date() > last_date:
                need_rebalance, new_weights = self.check_rebalance(strategy)
                if need_rebalance:
                    strategy.params['target_weights'] = new_weights
                return True, f"按照{self.factor}因子排序触发再平衡"
                
        return False, ""

    def check_rebalance(self, strategy) -> Tuple[bool, Dict[str, float]]:
        # 计算当前价格与因子值的偏移情况
        deviations = self._calculate_price_deviations(strategy)
        if not deviations:
            return False, {}
            
        # 根据偏移情况调整权重
        new_weights = self._adjust_weights_by_deviations(strategy, deviations)
        if not new_weights:
            return False, {}
        
        for symbol, weight in strategy.params['target_weights'].items():
            if symbol in new_weights and new_weights[symbol] != weight:
                logger.info(f"产品{symbol}权重从{weight:.2%}调整为{new_weights[symbol]:.2%}")

        # 更新策略的目标权重
        return True, new_weights