import backtrader as bt
from datetime import datetime

class BuyAndHoldStrategy(bt.Strategy):
    """
    买入持有策略：
    1. 在回测区间的第一个交易日买入指定比例资金
    2. 之后持有不再交易
    """
    
    params = (
        ('printlog', False),   # 是否打印日志
        ('start_date', None),  # 回测开始日期，格式：'YYYY-MM-DD'
        ('end_date', None),    # 回测结束日期，格式：'YYYY-MM-DD'
        ('min_volume', 100),   # 最小交易金额
        ('size_pct', 0.95),    # 每次交易使用资金比例
    )

    def __init__(self):
        """初始化策略"""
        self.order = None  # 订单
        self.bought = False  # 是否已买入
        
        # 转换日期参数为datetime对象
        self.start_date = (
            datetime.strptime(self.params.start_date, '%Y-%m-%d').date()
            if self.params.start_date else None
        )
        self.end_date = (
            datetime.strptime(self.params.end_date, '%Y-%m-%d').date()
            if self.params.end_date else None
        )

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
            
        # 如果已经买入，不再交易
        if self.bought:
            return
            
        # 第一个交易日买入指定比例资金
        if not self.position:
            # 计算可以买入的数量
            available_cash = self.broker.getcash()
            price = self.datas[0].close[0]
            target_value = available_cash * self.params.size_pct
            
            # 检查是否满足最小交易金额
            if target_value < self.params.min_volume:
                self.log(f'可用资金不足最小交易金额: {target_value:.2f} < {self.params.min_volume}')
                return
                
            size = target_value / price
            
            self.log(f'买入信号, 价格: {price:.4f}, 数量: {size:.4f}, '
                    f'目标金额: {target_value:.2f} ({self.params.size_pct*100:.0f}%资金)')
            self.order = self.buy(size=size)
            self.bought = True

    def notify_order(self, order):
        """订单状态更新通知"""
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入执行: 价格: {order.executed.price:.4f}, '
                        f'数量: {order.executed.size:.4f}, '
                        f'成本: {order.executed.value:.4f}, '
                        f'手续费: {order.executed.comm:.4f}, '
                        f'剩余现金: {self.broker.getcash():.2f}')

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