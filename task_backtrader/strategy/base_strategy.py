from datetime import datetime
import backtrader as bt
from loguru import logger
from typing import Dict, Any
import math  # 添加此行以导入math模块
import datetime
class BaseStrategy(bt.Strategy):
    """处理分红的策略"""

    # params = (
    #     ('dividend_method', 'cash'),  # 分红处理方式: 'cash' 或 'reinvest'
    # )

    def __init__(self, params: Dict[str, Any]):
        """初始化策略"""
        self.params = params
        self.dividend_method = self.params.get('dividend_method', 'cash')

    def next(self):
        """
        策略核心逻辑：处理当前持仓产品的分红
        """
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
                    self.buy(data=data, size=size, price=data.close[1])
                    logger.info(f"将分红再投资: {size} 份 {data._name}，当前价格: {data.close[1]:.4f}")

    def notify_order(self, order):
        """
        监听订单状态变化
        """
        if order.status == order.Submitted:
            logger.info(f"订单 {order.ref} 被提交")
            return
        elif order.status == order.Accepted:
            logger.info(f"订单 {order.ref} 被接受")

        # 订单已完成
        if order.status in [order.Completed]:
            if order.isbuy():
                logger.info(f"买入成功: {order.executed.price}, 数量: {order.executed.size}")
            elif order.issell():
                logger.info(f"卖出成功: {order.executed.price}, 数量: {order.executed.size}")

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            logger.error(f"订单 {order.ref} 失败: {order}") 