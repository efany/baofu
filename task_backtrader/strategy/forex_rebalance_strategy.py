import backtrader as bt
from typing import List, Dict, Any
from loguru import logger

class ForexRebalanceStrategy(bt.Strategy):
    """
    外汇再平衡策略
    
    该策略根据设定的权重定期调整各个外汇对的持仓比例
    """
    
    params = (
        ('forex_pairs', []),  # 外汇对列表，每个元素包含symbol和weight
        ('base_currency', ''),  # 基础货币
        ('initial_positions', []),  # 初始持仓配置
        ('rebalance_days', 5),  # 再平衡间隔天数
    )
    
    def __init__(self):
        """初始化策略"""
        self.order_list = []  # 订单列表
        self.rebalance_day = 0  # 上次再平衡的日期
        self.initialized = False  # 是否已初始化持仓
        
        # 为每个外汇对创建数据引用
        self.datas_dict = {data._name: data for data in self.datas}
        
        # 验证参数
        if not self.p.forex_pairs:
            raise ValueError("forex_pairs不能为空")
        if not self.p.base_currency:
            raise ValueError("base_currency不能为空")
        if not self.p.initial_positions:
            raise ValueError("initial_positions不能为空")
            
        # 验证所有外汇对是否都有对应的数据
        for pair in self.p.forex_pairs:
            if pair['symbol'] not in self.datas_dict:
                raise ValueError(f"找不到外汇对 {pair['symbol']} 的数据")
                
        # 计算总权重
        total_weight = sum(pair['weight'] for pair in self.p.forex_pairs)
        if abs(total_weight - 1.0) > 0.0001:  # 允许小的浮点数误差
            raise ValueError("所有外汇对的权重之和必须为1")
            
    def initialize_positions(self):
        """初始化持仓"""
        if self.initialized:
            return
            
        # 处理初始持仓
        for position in self.p.initial_positions:
            currency = position['currency']
            amount = position['amount']
            
            if currency == self.p.base_currency:
                # 基础货币不需要交易
                continue
                
            # 构建外汇对代码
            pair_symbol = f"{self.p.base_currency}{currency}"
            if pair_symbol not in self.datas_dict:
                raise ValueError(f"找不到将{currency}转换为{self.p.base_currency}的汇率数据")
                
            # 获取当前汇率
            data = self.datas_dict[pair_symbol]
            rate = data.close[0]
            
            # 计算需要买入的数量
            size = int(amount / rate)
            if size > 0:
                logger.info(f"初始买入 {pair_symbol}: {size} 单位")
                self.buy(data=data, size=size)
                
        self.initialized = True
            
    def next(self):
        """
        策略的主要逻辑，每个交易日调用一次
        """
        # 初始化持仓
        if not self.initialized:
            self.initialize_positions()
            
        # 检查是否需要再平衡
        if len(self) - self.rebalance_day < self.p.rebalance_days:
            return
            
        self.rebalance_day = len(self)
        
        # 获取当前总资产
        total_value = self.broker.getvalue()
        
        # 计算每个外汇对的目标持仓价值
        target_values = {}
        for pair in self.p.forex_pairs:
            symbol = pair['symbol']
            weight = pair['weight']
            target_values[symbol] = total_value * weight
            
        # 调整每个外汇对的持仓
        for pair in self.p.forex_pairs:
            symbol = pair['symbol']
            data = self.datas_dict[symbol]
            
            # 获取当前持仓价值
            current_position = self.getposition(data)
            current_value = current_position.size * data.close[0]
            
            # 计算需要调整的金额
            target_value = target_values[symbol]
            value_diff = target_value - current_value
            
            if abs(value_diff) > 0.01:  # 忽略很小的差异
                # 计算需要买入或卖出的数量
                size = int(value_diff / data.close[0])
                
                if size > 0:
                    logger.info(f"买入 {symbol}: {size} 单位")
                    self.buy(data=data, size=size)
                elif size < 0:
                    logger.info(f"卖出 {symbol}: {abs(size)} 单位")
                    self.sell(data=data, size=abs(size))
                    
    def notify_order(self, order):
        """
        订单状态更新时的回调函数
        """
        if order.status in [order.Submitted, order.Accepted]:
            return
            
        if order.status in [order.Completed]:
            if order.isbuy():
                logger.info(f'买入执行: {order.data._name}, 价格: {order.executed.price:.4f}, 数量: {order.executed.size}')
            else:
                logger.info(f'卖出执行: {order.data._name}, 价格: {order.executed.price:.4f}, 数量: {order.executed.size}')
                
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            logger.warning(f'订单取消/拒绝: {order.data._name}')
            
    def notify_trade(self, trade):
        """
        交易完成时的回调函数
        """
        if not trade.isclosed:
            return
            
        logger.info(f'交易利润: {trade.pnl:.2f}, 毛利润: {trade.pnlcomm:.2f}') 