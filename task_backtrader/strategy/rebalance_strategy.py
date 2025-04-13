import sys
import os
import math
from typing import Dict, Any, List
from datetime import datetime, date, timedelta
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from task_backtrader.strategy.base_strategy import BaseStrategy


class RebalanceStrategy(BaseStrategy):
    """定期再平衡策略
    
    根据配置的触发条件调整投资组合的权重，实现再平衡。
    支持以下触发条件：
    1. 日期触发：在指定日期执行再平衡
    2. 周期触发：按月、按季度、按年执行再平衡
    3. 偏离触发：当产品权重偏离目标值达到阈值时执行再平衡
    """

    def __init__(self, params: Dict[str, Any]):
        """
        初始化策略
        
        params示例:
        {
            'open_date': '2024-01-01',  # 开仓日期，可选
            'close_date': '2024-12-31',  # 平仓日期，可选
            'triggers': {
                'dates': ['2024-01-01', '2024-06-01'],  # 指定日期触发
                'period': {  # 周期触发
                    'freq': 'month',  # 'month', 'quarter', 'year'
                    'day': 1,  # 每周期第几天，1表示第一天，-1表示最后一天
                },
                'deviation': {  # 偏离触发
                    'threshold': 0.1,  # 偏离阈值，如0.1表示偏离10%
                    'products': ['159949.SZ'],  # 监控的产品列表，空列表表示监控所有
                }
            },
            'target_weights': {  # 目标持仓权重
                '159949.SZ': "0.3",
                '512550.SS': "0.7"
            },
            'cash_reserve': 0.05  # 现金储备比例
        }
        """
        super().__init__(params)
        self.params = params
        
        # 验证参数
        if 'target_weights' not in self.params:
            raise ValueError("必须设置目标持仓权重")
        
        # 验证权重之和是否接近1
        total_weight = sum(float(weight) for weight in self.params['target_weights'].values())
        if not 0.99 <= total_weight <= 1.01:
            raise ValueError(f"目标持仓权重之和必须接近1，当前为{total_weight}")
        
        # 验证triggers参数
        if 'triggers' not in self.params:
            raise ValueError("必须设置触发条件")
        
        # 初始化变量
        self.rebalance_dates = set()  # 用set存储再平衡日期，提高查找效率
        self.current_weights = {}  # 记录当前持仓的目标权重
        self.last_rebalance_value = 0  # 记录上次再平衡时的投资组合总价值
        self.position_opened = False  # 是否已开仓
        self.position_closed = False  # 是否已平仓
        self.mark_balance = 0
        
        # 处理开仓日期
        open_date_param = self.params.get('open_date')
        if open_date_param is None or open_date_param == "":
            # 获取各个产品有效净值的最大日期作为开仓日期
            min_date = None
            for d in self.datas:
                valid_dates = [datetime.fromordinal(int(date)).date() for date in d.datetime.array]
                if not valid_dates:
                    continue
                current_min = min(valid_dates)
                if min_date is None or current_min > min_date:
                    min_date = current_min
            self.open_date = min_date
            logger.info(f"未指定开仓日期，使用各产品有效净值的最大日期作为开仓日期: {self.open_date}")
        else:
            self.open_date = datetime.strptime(open_date_param, '%Y-%m-%d').date()

        # 处理平仓日期
        close_date_param = self.params.get('close_date')
        if close_date_param is None or close_date_param == "":
            self.close_date = None
            logger.info(f"未指定平仓日期，不执行平仓")
        else:
            self.close_date = datetime.strptime(close_date_param, '%Y-%m-%d').date()
        
        # 初始化触发条件
        self._init_triggers()
        
        # 记录初始配置
        logger.info("策略初始化配置:")
        logger.info(f"开仓日期: {self.open_date}")
        logger.info(f"平仓日期: {self.close_date}")
        logger.info(f"目标权重: {self.params['target_weights']}")
        logger.info(f"触发条件: {self.params['triggers']}")
        logger.info(f"现金储备: {self.params['cash_reserve']:.1%}")
        
    def _init_triggers(self):
        """初始化触发条件"""
        triggers = self.params['triggers']
        
        # 处理日期触发
        if 'dates' in triggers:
            for date_str in triggers['dates']:
                self.rebalance_dates.add(date_str)
        
        # 处理周期触发
        if 'period' in triggers:
            period = triggers['period']
            freq = period.get('freq', 'month')
            day = period.get('day', -1)  # 默认为每周期最后一天
            
            # 获取数据的起止日期
            start_date = self.open_date
            end_date = self.close_date if self.close_date is not None else datetime.now().date()

            logger.info(f"start_date:{start_date},end_date:{end_date}")
            
            current_date = start_date
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
        logger.info(f"{self.rebalance_dates}")

    def _check_period_trigger(self) -> bool:
        need_rebalance = False

        last_date = self.data0.datetime.date(-1)
        current_date = self.data0.datetime.date(0)
        # 获取所有小于等于当前日期的再平衡日期
        valid_dates = [d for d in self.rebalance_dates if datetime.strptime(d, '%Y-%m-%d').date() <= current_date]
        # 如果有有效日期
        if valid_dates:
            # 获取最大的有效日期
            latest_valid_date = max(valid_dates, key=lambda d: datetime.strptime(d, '%Y-%m-%d').date())
            # 如果这个日期大于上次再平衡日期
            if datetime.strptime(latest_valid_date, '%Y-%m-%d').date() > last_date:
                need_rebalance = True
        
        if need_rebalance:
            # 获取触发配置中的watermark阈值
            watermark = self.params['triggers']['period'].get('watermark', 0.0)
            if watermark > 0:
                # 计算总资产
                total_value = self.get_total_asset()

                # 检查每个产品的持仓偏移
                max_deviation = 0
                for symbol, target_weight in self.params['target_weights'].items():
                    target_weight = float(target_weight)
                    data = self.getdatabyname(symbol)
                    if not data:
                        continue
                        
                    position = self.getposition(data)
                    if not position:
                        current_weight = 0
                    else:
                        current_weight = position.size * data.close[0] / total_value
                        
                    # 计算当前权重与目标权重的偏差
                    deviation = abs(current_weight - target_weight)
                    if deviation > max_deviation:
                        max_deviation = deviation
                        
                # 如果最大偏差小于等于watermark，则不执行再平衡
                if max_deviation <= watermark:
                    logger.info(f"{current_date} 最大持仓偏移 {max_deviation:.2%} 小于 watermark {watermark:.2%}，不执行再平衡")
                    self.print_positions()
                    need_rebalance = False
        return need_rebalance
    
    def _check_deviation_trigger(self) -> bool:
        """检查是否触发偏离条件"""
        triggers = self.params['triggers']
        target_weights = self.params['target_weights']
        if 'deviation' not in triggers:
            return False

        # "triggers": {
        #     "deviation": {
        #         "159949.SZ": {
        #             "rise": 0.1,
        #             "fall": 0.1,
        #         }
        #         "512550.SS": {
        #             "rise": 0.1,
        #             "fall": 0.1,
        #         }
        #         "159633.SZ": {
        #             "rise": 0.1,
        #             "fall": 0.1,
        #         }
        #         "159628.SZ": {
        #             "rise": 0.1,
        #             "fall": 0.1,
        #         }
        #     }
        # }
        # 获取当前总资产
        total_value = self.get_total_asset()
        
        # 遍历deviation中的产品
        for symbol, deviation_config in triggers['deviation'].items():
            # 获取产品数据
            data = self.getdatabyname(symbol)
            if not data:
                continue

            # 获取当前持仓
            position = self.getposition(data)
            if not position:
                current_weight = 0
            else:
                current_weight = position.size * data.close[0] / total_value

            # 获取目标权重
            target_weight = float(target_weights.get(symbol, 0))
            
            # 计算当前权重与目标权重的偏差
            deviation = (current_weight - target_weight) / target_weight
            
            # 检查是否触发上升或下降条件
            if deviation > 0 and deviation > deviation_config.get('rise', 0):
                logger.info(f"产品{symbol}当前权重{current_weight:.2%}超过目标权重{target_weight:.2%}，"
                          f"偏差{deviation:.2%}大于上升阈值{deviation_config['rise']:.2%}，触发再平衡")
                return True
            elif deviation < 0 and abs(deviation) > deviation_config.get('fall', 0):
                logger.info(f"产品{symbol}当前权重{current_weight:.2%}低于目标权重{target_weight:.2%}，"
                          f"偏差{abs(deviation):.2%}大于下降阈值{deviation_config['fall']:.2%}，触发再平衡")
                return True
        return False
    
    def _calculate_target_position(self, symbol: str) -> int:
        """
        计算指定产品的目标持仓数量
        
        Args:
            symbol: 产品代码
            
        Returns:
            int: 目标持仓数量，如果计算失败返回0
        """
        # 获取目标权重
        target_weights = self.params['target_weights']
        if symbol not in target_weights:
            logger.warning(f"产品{symbol}不在目标权重配置中")
            return 0
            
        weight = float(target_weights[symbol])
        if weight <= 0:
            logger.warning(f"产品{symbol}的目标权重小于等于0")
            return 0
            
        # 获取产品数据
        data = self.getdatabyname(symbol)
        if not data:
            logger.warning(f"找不到产品{symbol}的数据")
            return 0
            
        # 计算投资组合总价值
        portfolio_value = self.get_total_asset()
        
        # 预留现金
        cash_reserve = self.params['cash_reserve']
        available_value = portfolio_value * (1 - cash_reserve)
        
        # 计算目标市值
        target_value = available_value * weight
                    
        return target_value
    
    def _rebalance_sell(self, message:str = None):
        """
        执行投资组合再平衡
        
        Args:
            rebalance_type: 再平衡类型
                - 'both': 同时处理减仓和记录加仓（默认）
                - 'sell': 只处理减仓
                - 'buy': 只处理加仓
        """
        
        # 遍历所有产品
        for symbol in self.params['target_weights'].keys():
            data = self.getdatabyname(symbol)
            if data is None:
                continue

            current_position = self.broker.get_value([data])
            target_position = self._calculate_target_position(symbol)
            diff_position = target_position - current_position

            if diff_position < 0:
                # 使用后一日的收盘价
                price = data.open[1]
                sell_size = math.floor(diff_position / price)
                order = self.sell(data=data, size=-sell_size)
                self.order_message[order.ref] = message
                logger.info(f"{symbol}: sell {price:.2f}*{sell_size} = {(price*sell_size):.2f} --- current_position:{current_position} >> target_position:{target_position}")

    def _rebalance_buy(self, message:str = None):
        """
        执行投资组合再平衡
        
        Args:
            rebalance_type: 再平衡类型
                - 'both': 同时处理减仓和记录加仓（默认）
                - 'sell': 只处理减仓
                - 'buy': 只处理加仓
        """
        
        cash = self.broker.getcash()
        logger.info(f"cash:{cash}")
        total_diff = 0

        # 遍历所有产品
        for symbol in self.params['target_weights'].keys():
            data = self.getdatabyname(symbol)
            if data is None:
                continue

            current_position = self.broker.get_value([data])
            target_position = self._calculate_target_position(symbol)
            diff_position = target_position - current_position

            if diff_position > 0:
                total_diff += diff_position
                
        for symbol in self.params['target_weights'].keys():
            data = self.getdatabyname(symbol)
            if data is None:
                continue

            current_position = self.broker.get_value([data])
            target_position = self._calculate_target_position(symbol)
            diff_position = target_position - current_position

            if diff_position > 0:
                # 使用后一日的收盘价
                price = data.open[1]
                buy_size = math.floor((diff_position / total_diff) * cash / price)
                order = self.buy(data=data, size=buy_size, price=price)
                self.order_message[order.ref] = message
                logger.info(f"{symbol}: buy {price:.2f}*{buy_size} = {(price*buy_size):.2f} --- current_position:{current_position} >> target_position:{target_position}")
        

    def next(self):
        """
        主要的策略逻辑
        检查是否满足再平衡条件，如果满足则执行再平衡
        """
        if self.position_closed:
            return
        if self.data0.datetime.get_idx() >= self.data0.datetime.buflen() - 1:
            return

        last_date = self.data0.datetime.date(-1)
        current_date = self.data0.datetime.date(0)
        next_date = self.data0.datetime.date(1)

        # 检查是否有待执行的买入操作
        if self.mark_balance == 2:
            logger.info(f"执行再平衡的第二部分《买入》，日期：{current_date} ")
            self._rebalance_buy("再平衡买入")
            self.mark_balance -= 1
            return
        elif self.mark_balance == 1:
            self.print_positions()
            self.mark_balance -= 1
            return

        # 如果还未开仓且到达开仓日期，执行首次建仓
        if not self.position_opened and next_date is not None and next_date >= self.open_date:
            logger.info(f"达到开仓日期 {current_date}，执行首次建仓")
            self._rebalance_buy("开仓")  # 首次建仓时立即执行所有操作
            self.position_opened = True
            return

        # 如果已经到达平仓日期，执行平仓
        if self.close_date and next_date >= self.close_date and not self.position_closed:
            logger.info(f"达到平仓日期 {current_date}，执行平仓")
            # 遍历所有持仓产品进行平仓
            for symbol in self.params['target_weights'].keys():
                data = self.getdatabyname(symbol)
                if data is None:
                    continue
                pos = self.getposition(data)
                if pos.size > 0:
                    order = self.sell(data=data, size=pos.size)
                    self.order_message[order.ref] = "平仓"
            self.position_closed = True
            return

        # 只有在开仓日期和平仓日期之间才执行再平衡
        if self.position_opened and not self.position_closed:
            # 检查是否需要再平衡
            need_rebalance = False
            
            if self._check_period_trigger():
                logger.info(f"当前日期 {current_date} 触发再平衡")
                need_rebalance = True

            # 检查偏离触发
            if self._check_deviation_trigger():
                logger.info("产品权重偏离触发再平衡")
                need_rebalance = True
            
            # 执行再平衡
            if need_rebalance:
                self.print_positions()
                logger.info(f"执行再平衡的第一部分《卖出》，日期：{current_date} ")
                self._rebalance_sell("再平衡卖出")  # 默认使用分离加减仓的方式
                self.mark_balance = 2