import backtrader as bt
from backtrader.utils import AutoOrderedDict, AutoDict
from loguru import logger
class TradeListAnalyzer(bt.Analyzer):
    """分析器，用于记录所有交易记录"""

    def __init__(self):
        self.rets = AutoOrderedDict()
        self.rets.trades = []

    def stop(self):
        super(TradeListAnalyzer, self).stop()
        self.rets._close()

    def notify_order(self, order):
        if order.status == order.Submitted:
            self.rets.trades.append({
                'order_ref': order.ref,
                'fund_code': order.data._name,
                'date': order.data.datetime.date(0),
                'size': order.size,
                'price': order.price,
                'type': 'buy' if order.isbuy() else 'sell'
            })
            logger.info(f"订单 {order.ref} 被提交")
            return
            logger.info(f"订单 {order.ref} 被接受")

    def get_analysis(self):
        """返回交易记录"""
        return self.rets 