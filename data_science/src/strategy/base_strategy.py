import backtrader as bt
from typing import Dict, Any
from datetime import datetime

class BaseStrategy(bt.Strategy):
    """
    基础策略类，包含基本的策略框架
    """
    params = (
        ('maperiod', 15),  # MA周期
        ('printlog', False),  # 是否打印日志
        ('min_volume', 100),  # 最小交易金额
        ('size_pct', 0.95),  # 每次交易使用资金比例
        ('start_date', None),  # 回测开始日期，格式：'YYYY-MM-DD'
        ('end_date', None),  # 回测结束日期，格式：'YYYY-MM-DD'
    )

    def __init__(self):
        """初始化策略"""
        self.dataclose = self.datas[0].close  # 收盘价
        self.order = None  # 订单
        self.buyprice = None  # 买入价格
        self.buycomm = None  # 交易费用
        
        # 添加移动平均指标
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.maperiod)
        
        # 添加其他技术指标
        self.rsi = bt.indicators.RSI(self.datas[0], period=14)
        self.atr = bt.indicators.ATR(self.datas[0], period=14)
        
        # 转换日期参数为datetime对象
        self.start_date = (
            datetime.strptime(self.params.start_date, '%Y-%m-%d').date()
            if self.params.start_date else None
        )
        self.end_date = (
            datetime.strptime(self.params.end_date, '%Y-%m-%d').date()
            if self.params.end_date else None
        )

    def notify_order(self, order):
        """订单状态更新通知"""
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入执行: 价格: {order.executed.price:.4f}, '
                        f'数量: {order.executed.size:.4f}, '
                        f'成本: {order.executed.value:.4f}, '
                        f'手续费: {order.executed.comm:.4f}')
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(f'卖出执行: 价格: {order.executed.price:.4f}, '
                        f'数量: {order.executed.size:.4f}, '
                        f'成本: {order.executed.value:.4f}, '
                        f'手续费: {order.executed.comm:.4f}')

        elif order.status in [order.Canceled]:
            self.log('订单取消')
        elif order.status in [order.Margin]:
            self.log('保证金不足')
        elif order.status in [order.Rejected]:
            self.log('订单拒绝')

        self.order = None

    def notify_trade(self, trade):
        """交易状态更新通知"""
        if not trade.isclosed:
            return

        self.log(f'交易完成, 毛利润: {trade.pnl:.4f}, 净利润: {trade.pnlcomm:.4f}')

    def next(self):
        """策略核心逻辑，每个交易日调用一次"""
        # 获取当前日期
        current_date = self.datas[0].datetime.date(0)
        
        # 检查是否在回测时间区间内
        if self.start_date and current_date < self.start_date:
            return
        if self.end_date and current_date > self.end_date:
            return
            
        # 如果有未完成的订单，不执行新的交易
        if self.order:
            return

        # 计算可用资金
        available_cash = self.broker.getcash()
        
        if not self.position:  # 没有持仓
            # 买入条件：价格上穿MA且RSI不超买
            if (self.dataclose[0] > self.sma[0] and 
                self.dataclose[-1] <= self.sma[-1] and 
                self.rsi[0] < 70):
                
                # 计算购买数量
                price = self.dataclose[0]
                target_value = available_cash * self.params.size_pct
                if target_value < self.params.min_volume:
                    self.log(f'可用资金不足最小交易金额: {target_value:.2f} < {self.params.min_volume}')
                    return
                
                size = target_value / price
                
                self.log(f'买入信号, 价格: {price:.4f}, 数量: {size:.4f}')
                self.order = self.buy(size=size)
                
        else:  # 有持仓
            # 卖出条件：价格下穿MA或RSI超买
            if (self.dataclose[0] < self.sma[0] and 
                self.dataclose[-1] >= self.sma[-1]) or self.rsi[0] > 80:
                
                # 计算卖出数量（当前持仓的��量）
                position_size = self.position.size
                price = self.dataclose[0]
                value = position_size * price
                
                if value < self.params.min_volume:
                    self.log(f'持仓价值不足最小交易金额: {value:.2f} < {self.params.min_volume}')
                    return
                
                self.log(f'卖出信号, 价格: {price:.4f}, 数量: {position_size:.4f}')
                self.order = self.sell(size=position_size)

    def log(self, txt, dt=None, doprint=False):
        """日志函数"""
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt.isoformat()}, {txt}')

    def stop(self):
        """策略结束时调用"""
        self.log('策略结束', doprint=True)
        # 打印最终结果
        portfolio_value = self.broker.getvalue()
        initial_value = self.broker.startingcash
        returns = (portfolio_value - initial_value) / initial_value * 100
        self.log(f'初始资金: {initial_value:.2f}', doprint=True)
        self.log(f'最终资金: {portfolio_value:.2f}', doprint=True)
        self.log(f'收益率: {returns:.2f}%', doprint=True) 