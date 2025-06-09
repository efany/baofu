from datetime import datetime, date, timedelta
from typing import Dict, Any, Set, Tuple
from loguru import logger

from task_backtrader.strategy.trigger.base_trigger import BaseTrigger

class DateTrigger(BaseTrigger):
    """日期触发器
    
    在指定日期触发再平衡。
    支持两种模式：
    1. 指定具体日期列表
    2. 按周期（月/季/年）触发
    """
    
    def __init__(self, params: Dict[str, Any]):
        """
        初始化日期触发器
        
        Args:
            params: 触发器配置参数，格式如下：
                {
                    'dates': ['2024-01-01', '2024-06-01'],  # 指定日期触发
                    'period': {  # 周期触发
                        'freq': 'month',  # 'month', 'quarter', 'year'
                        'day': 1,  # 每周期第几天，1表示第一天，-1表示最后一天
                    }
                }
        """
        super().__init__(params)
        print(f"params:{params}")
        self.rebalance_dates: Set[str] = set()  # 用set存储再平衡日期，提高查找效率
        self._init_dates()
        
    def _init_dates(self):
        """初始化再平衡日期"""
        # 处理指定日期触发
        trigger_params = self.params.get('triggers', {})
        if 'dates' in trigger_params:
            for date_str in trigger_params['dates']:
                self.rebalance_dates.add(date_str)
        
        # 处理周期触发
        if 'period' in trigger_params:
            period = trigger_params['period']
            freq = period.get('freq', 'month')
            day = period.get('day', -1)  # 默认为每周期最后一天
            
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
    
    def check(self, strategy) -> Tuple[bool, str]:
        """
        检查是否触发再平衡
        
        Args:
            strategy: 策略实例
            
        Returns:
            bool: 是否触发再平衡
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
                # 检查watermark阈值
                watermark = self.params.get('triggers', {}).get('period', {}).get('watermark', 0.0)
                if watermark > 0:
                    # 计算总资产
                    total_value = strategy.get_total_asset()
                    
                    # 检查每个产品的持仓偏移
                    max_deviation = 0
                    datas_deviation = {}
                    for symbol, target_weight in strategy.params['target_weights'].items():
                        target_weight = float(target_weight)
                        data = strategy.getdatabyname(symbol)
                        if not data:
                            continue
                            
                        position = strategy.getposition(data)
                        if not position:
                            current_weight = 0
                        else:
                            current_weight = position.size * data.close[0] / total_value
                        
                        # 计算当前权重与目标权重的偏差
                        deviation = abs(current_weight - target_weight)
                        datas_deviation[symbol] = deviation
                        if deviation > max_deviation:
                            max_deviation = deviation

                    # 如果最大偏差小于等于watermark，则不执行再平衡
                    if max_deviation <= watermark:
                        logger.info(f"{current_date} 最大持仓偏移 {max_deviation:.2%} 小于 watermark {watermark:.2%}，不执行再平衡")
                        strategy.print_positions()
                        return False, "偏移小于watermark"
                    else:
                        logger.info(f"{current_date} 最大持仓偏移 {max_deviation:.2%} 大于 watermark {watermark:.2%}，执行再平衡")
                        message = f"{current_date} 偏移大于watermark，执行再平衡"
                        for symbol, deviation in datas_deviation.items():
                            if deviation > watermark:
                                message += f"\n{symbol}: {deviation:.2%}"
                        return True, message

                return True, "未设置watermark，执行再平衡"
        
        return False, "非再平衡日期"