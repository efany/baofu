import sys
import os
import math
from typing import Dict, Any, List
from datetime import datetime, date, timedelta
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from task_backtrader.strategy.base_strategy import BaseStrategy
from task_backtrader.strategy.trigger.trigger_factory import TriggerFactory
from task_backtrader.strategy.trigger.base_trigger import BaseTrigger


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
            }
        }
        """
        super().__init__(params)
        self.params = params
        
        # 验证参数
        if 'target_weights' not in self.params:
            raise ValueError("必须设置目标持仓权重")
        
        self.total_weight = sum(float(weight) for weight in self.params['target_weights'].values())
        
        # 验证triggers参数
        if 'triggers' not in self.params:
            raise ValueError("必须设置触发条件")
        
        # 初始化变量
        self.current_weights = {}  # 记录当前持仓的目标权重
        self.last_rebalance_value = 0  # 记录上次再平衡时的投资组合总价值
        self.mark_balance = 0
        # 初始化触发器
        self.triggers = TriggerFactory.create_triggers(self.params)
        
        # 记录初始配置
        logger.info("策略初始化配置:")
        logger.info(f"目标权重: {self.params['target_weights']}")
        logger.info(f"触发条件: {self.params['triggers']}")
        
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
        
        # 计算目标市值
        target_value = portfolio_value * weight
                    
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
        
        # 获取当前总资产
        total_value = self.get_total_asset()
        # 预留现金
        available_value = total_value * self.total_weight
        current_cash = self.broker.getcash()
        logger.info(f"total_value:{total_value}, available_value:{available_value}, current_cash:{current_cash}")
        
        if current_cash < total_value - available_value:
            logger.info(f"持有现金{current_cash}小于目标现金仓位{total_value - available_value}，不执行再平衡")
            return

        cash = current_cash - (total_value - available_value)
        logger.info(f"可用买入现金 cash:{cash}")

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
            logger.info(f"symbol:{symbol}, current_position:{current_position}, target_position:{target_position}, diff_position:{diff_position}")
            if diff_position > 0:
                # 使用后一日的收盘价
                price = data.open[1]
                buy_size = math.floor((diff_position / total_diff) * cash / price)
                order = self.buy(data=data, size=buy_size, price=price)
                self.order_message[order.ref] = message
                logger.info(f"{symbol}: buy {price:.2f}*{buy_size} = {(price*buy_size):.2f} --- current_position:{current_position} >> target_position:{target_position}")
        
    def open_trade(self):
        """开仓"""
        for trigger in self.triggers:
            need_rebalance, message = trigger.open_trade(self)

        super().open_trade()
        logger.info(f"开仓，日期：{self.data0.datetime.date(0)}")
        self._rebalance_buy("开仓")  # 首次建仓时立即执行所有操作
    
    def close_trade(self):
        """平仓"""
        for trigger in self.triggers:
            need_rebalance, message = trigger.close_trade(self)
    
        super().close_trade()
        logger.info(f"平仓")
        for symbol in self.params['target_weights'].keys():
            data = self.getdatabyname(symbol)
            if data is None:
                continue
            pos = self.getposition(data)
            if pos.size > 0:
                order = self.sell(data=data, size=pos.size)
                self.order_message[order.ref] = "平仓"

    def next(self):
        """主要的策略逻辑"""
        super().next()
    
        if not (self.is_open_traded and not self.is_close_traded):
            return

        # 检查是否有待执行的买入操作
        if self.mark_balance == 2:
            logger.info(f"执行再平衡的第二部分《买入》，日期：{self.data0.datetime.date(0)} ")
            self._rebalance_buy("再平衡买入")
            self.mark_balance -= 1
            return
        elif self.mark_balance == 1:
            self.print_positions()
            self.mark_balance -= 1
            return

        # 检查是否需要再平衡
        need_rebalance = False
        
        # 遍历所有触发器
        for trigger in self.triggers:
            need_rebalance, message = trigger.check(self)
            if need_rebalance:
                logger.info(f"触发器{trigger.__class__.__name__}触发再平衡，原因：{message}")
                break
        
        # 执行再平衡
        if need_rebalance:
            self.print_positions()
            logger.info(f"执行再平衡的第一部分《卖出》，日期：{self.data0.datetime.date(0)} ")
            self._rebalance_sell("再平衡卖出")  # 默认使用分离加减仓的方式
            self.mark_balance = 2