import backtrader as bt
from backtrader.utils import AutoOrderedDict, AutoDict
from loguru import logger

class TradeListAnalyzer(bt.Analyzer):
    """分析器，用于记录所有交易记录"""

    def __init__(self):
        super(TradeListAnalyzer, self).__init__()
        self.trades = []  # 使用列表存储交易记录

    def notify_order(self, order):
        """
        订单状态更新时的回调函数
        """
        # 查找已存在的订单记录
        existing_trade = None
        for trade in self.trades:
            if trade['order_ref'] == order.ref:
                existing_trade = trade
                break

        # 如果是新订单，创建新的记录
        if order.status == order.Submitted and not existing_trade:
            trade_record = {
                'order_ref': order.ref,
                'order_message': self.strategy.order_message.get(order.ref, ''),
                'product': order.data._name,
                'date': order.data.datetime.date(0),
                'size': order.size,
                'price': order.price,
                'type': 'buy' if order.isbuy() else 'sell',
                'status': 'submitted'
            }
            self.trades.append(trade_record)
            logger.info(f"订单 {order.ref} 已提交")
            return

        # 更新已存在订单的状态
        if existing_trade:
            if order.status == order.Accepted:
                existing_trade['status'] = 'accepted'
                logger.info(f"订单 {order.ref} 已接受")
            elif order.status == order.Partial:
                existing_trade['status'] = 'partial'
                logger.info(f"订单 {order.ref} 部分成交")
            elif order.status == order.Completed:
                existing_trade['status'] = 'completed'
                existing_trade['executed_size'] = order.executed.size
                existing_trade['executed_price'] = order.executed.price
                existing_trade['executed_value'] = order.executed.value
                existing_trade['executed_comm'] = order.executed.comm
                logger.info(f"订单 {order.ref} 已完成")
            elif order.status == order.Canceled:
                existing_trade['status'] = 'canceled'
                logger.info(f"订单 {order.ref} 已取消")
            elif order.status == order.Expired:
                existing_trade['status'] = 'expired'
                logger.info(f"订单 {order.ref} 已过期")
            elif order.status == order.Margin:
                existing_trade['status'] = 'margin'
                logger.info(f"订单 {order.ref} 保证金不足")
            elif order.status == order.Rejected:
                existing_trade['status'] = 'rejected'
                logger.info(f"订单 {order.ref} 已拒绝")

    def get_analysis(self):
        """返回交易记录"""
        return self.trades 