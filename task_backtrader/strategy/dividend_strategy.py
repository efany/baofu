from datetime import datetime
import backtrader as bt
from loguru import logger

class DividendStrategy(bt.Strategy):
    """处理分红的策略"""

    def __init__(self):
        """初始化策略"""
        pass

    def next(self):
        """
        策略核心逻辑：处理当前持仓产品的分红
        """
        for data in self.datas:
            # 检查当前数据的分红情况
            if data.dividend[0] > 0:
                # 获取当前持仓
                position = self.getposition(data)
                if position.size > 0:
                    # 计算分红金额
                    dividend_amount = data.dividend[0] * position.size
                    logger.info(f"基金：{data._name} 当前日期: {data.datetime.date(0)}, 分红: {data.dividend[0]}, 持仓数量: {position.size}, 分红金额: {dividend_amount}")

                    # 将分红计入现金
                    self.broker.setcash(self.broker.getcash() + dividend_amount)
                    logger.info(f"当前现金增加: {dividend_amount}, 新现金总额: {self.broker.getcash()}")

    def notify_order(self, order):
        """
        监听订单状态变化
        """
        if order.status in [order.Submitted, order.Accepted]:
            # 订单已提交或被接受
            logger.info(f"订单 {order.ref} 被提交或接受")
            return

        # 订单已完成
        if order.status in [order.Completed]:
            if order.isbuy():
                logger.info(f"买入成功: {order.executed.price}, 数量: {order.executed.size}")
            elif order.issell():
                logger.info(f"卖出成功: {order.executed.price}, 数量: {order.executed.size}")

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            logger.error(f"订单 {order.ref} 失败: {order.getstatusname()}") 