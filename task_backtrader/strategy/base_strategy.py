from datetime import datetime
import backtrader as bt
from loguru import logger
from typing import Dict, Any
import math
from .financing_strategy import FinancingStrategy
from .current_rate_strategy import CurrentRateStrategy
class BaseStrategy(bt.Strategy):
    """处理分红的基础策略"""
    
    def __init__(self, params: Dict[str, Any]):
        """初始化策略"""
        self.is_open_traded = False
        self.is_close_traded = False
        self.params = params
        self.order_message = {}
        self.extend_strategies = []

        self.financing_info = {}
        self.financing_trades = []

        self.current_interests = {}

        # 处理开仓日期
        open_date_param = self.params.get('open_date')
        if open_date_param is None or open_date_param == "":
            # 如果未指定开仓日期，则获取各个产品有效净值的最近日期作为开仓日期
            min_date = None
            for d in self.datas:
                valid_dates = [datetime.fromordinal(int(date)).date() for date in d.datetime.array]
                if not valid_dates:
                    continue
                current_min = min(valid_dates)
                if min_date is None or current_min > min_date:
                    min_date = current_min
            self.open_date = min_date
            self.open_trade_date = min_date
            logger.info(f"开仓日期: {self.open_date}, 开仓交易日期: {self.open_trade_date}, 开仓日期为各产品有效净值的最近日期")
        else:
            self.open_date = datetime.strptime(open_date_param, '%Y-%m-%d').date()
            # 获取首个data的大于等于open_date的最大日期作为开仓交易日期
            min_ordinal = min(date for date in self.datas[0].datetime.array if datetime.fromordinal(int(date)).date() >= self.open_date)
            self.open_trade_date = datetime.fromordinal(int(min_ordinal)).date()
            logger.info(f"开仓日期: {self.open_date}, 开仓交易日期: {self.open_trade_date}, 开仓日期为指定日期")

        # 处理平仓日期
        close_date_param = self.params.get('close_date')
        if close_date_param is None or close_date_param == "":
            self.close_date = None
            self.close_trade_date = None
            logger.info(f"未指定平仓日期，不执行平仓")
        else:
            self.close_date = datetime.strptime(close_date_param, '%Y-%m-%d').date()
            max_ordinal = max(date for date in self.datas[0].datetime.array if datetime.fromordinal(int(date)).date() <= self.close_date)
            self.close_trade_date = datetime.fromordinal(int(max_ordinal)).date()
            logger.info(f"平仓日期: {self.close_date}, 平仓交易日期: {self.close_trade_date}, 平仓日期为指定日期")


        self.dividend_method = self.params.get('dividend_method', 'cash')
        
        # 创建融资策略实例
        if 'forex_financing' in params:
            financing_strategy = FinancingStrategy(params)
            financing_strategy.setBroker(self.broker)
            financing_strategy.setMainStrategy(self)
            self.extend_strategies.append(financing_strategy)

        if 'current_rate' in params:
            current_rate_strategy = CurrentRateStrategy(params)
            current_rate_strategy.setBroker(self.broker)
            current_rate_strategy.setMainStrategy(self)
            self.extend_strategies.append(current_rate_strategy)

    def open_trade(self):
        """开仓"""
        for strategy in self.extend_strategies:
            strategy.open_trade()
    
    def close_trade(self):
        """平仓"""
        for strategy in self.extend_strategies:
            strategy.close_trade()

    def next(self):
        """策略核心逻辑：处理分红和外汇融资"""
        # 处理分红逻辑
        if not self.is_open_traded and self.open_trade_date is not None and self.datas[0].datetime.date(0) >= self.open_trade_date:
            self.open_trade()
            self.is_open_traded = True
        if not self.is_close_traded and self.close_trade_date is not None and self.datas[0].datetime.date(0) >= self.close_trade_date:
            self.close_trade()
            self.is_close_traded = True

        if not (self.is_open_traded and not self.is_close_traded):
            return

        for data in self.datas:
            # 检查当前数据的分红情况
            if data.dividend[0] <= 0:
                continue
            # 获取当前持仓
            position = self.getposition(data)
            if position.size <= 0:
                continue

            # 计算分红金额
            dividend_amount = round(data.dividend[0] * position.size, 4)
            logger.info(f"基金：{data._name} 当前日期: {data.datetime.date(0)}, 下个交易日: {data.datetime.date(1)}, "
                        f"分红: {data.dividend[0]}, 持仓数量: {position.size}, "
                        f"分红金额: {dividend_amount}")

            self.broker.setcash(self.broker.getcash() + dividend_amount)
            logger.info(f"当前现金增加: {dividend_amount}, 新现金总额: {self.broker.getcash()}")

            if self.dividend_method == 'reinvest':
                # 将分红再投资
                size = math.floor(dividend_amount / data.close[1])  # 计算可购买的份额
                if size > 0:
                    order = self.buy(data=data, size=size, price=data.close[1])
                    self.order_message[order.ref] = "分红再投资"
                    logger.info(f"将分红再投资: {size} 份 {data._name}，当前价格: {data.close[1]:.4f}, ref: {order.ref}")
        
        # 处理融资逻辑
        for strategy in self.extend_strategies:
            strategy.next()

    def get_total_asset(self) -> float:
        """获取总资产"""
        total_value = 0
        cash = self.broker.getcash()
        total_value += cash
        for data in self.datas:
            if self.broker.get_value([data]) is not None and not math.isnan(self.broker.get_value([data])):
                total_value += self.broker.get_value([data])
        return total_value

    def print_positions(self, tag="当前持仓"):
        """打印当前持仓情况"""
        # 获取当前现金
        cash = self.broker.getcash()
        
        # 获取当前总资产
        total_value = self.get_total_asset()

        # 获取当前日期
        current_date = self.datas[0].datetime.date(0) if len(self.datas) > 0 else None
        
        logger.info(f"========== {tag} ({current_date}) ==========")
        logger.info(f"现金: {cash:.2f}")

        # 获取融资情况
        financing_info = self.financing_info
        for pair, info in financing_info.items():
            # 获取当前货币对的价格
            data = self.getdatabyname(pair)
            if data is not None:
                # 计算融资份额的当前价值
                current_value = info['shares'] * data.close[0]
                info['current_value'] = current_value
            logger.info(f"{pair} 融资份额: {info['shares']:.2f}, 当前价格: {data.close[0]:.4f}, 当前价值: {info['current_value']:.2f}")

        
        # 统计持仓信息
        positions_value = 0
        positions_info = []
        
        for data in self.datas:
            market_value = self.broker.get_value([data])
            # 计算持仓占比
            weight = market_value / total_value
            positions_value += market_value
            positions_info.append({
                'symbol': data._name,
                'value': market_value,
                'weight': weight
            })
        
        # 按市值排序
        positions_info.sort(key=lambda x: x['value'], reverse=True)
        
        # 打印持仓信息
        for pos in positions_info:
            logger.info(f"{pos['symbol']} = {pos['value']:.2f} ({pos['weight']:.2%})")
        
        # 打印总结信息
        cash_weight = cash / total_value
        logger.info(f"持仓总市值: {positions_value:.2f} ({(1-cash_weight):.2%})")
        logger.info(f"总资产: {total_value:.2f}")
        logger.info("=========================================")