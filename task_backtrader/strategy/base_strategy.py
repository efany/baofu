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
        self.order_message = {}

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
                    order = self.buy(data=data, size=size, price=data.close[1])
                    self.order_message[order.ref] = "分红再投资"
                    logger.info(f"将分红再投资: {size} 份 {data._name}，当前价格: {data.close[1]:.4f}, ref: {order.ref}")
    
    
    def get_total_asset(self) -> int:
        # 获取当前现金
        cash = self.broker.getcash()

        # 获取每个产品的资产值
        product_assets = {}
        total_asset = 0
        
        for i, data in enumerate(self.datas):
            product_value = self.broker.get_value([data])
            product_code = data._name
            product_assets[product_code] = product_value
            total_asset += product_value

        # 计算总资产
        return cash + total_asset
                    
    def print_positions(self, tag="当前持仓"):
        """
        打印当前持仓情况
        
        Args:
            tag: 日志标签，用于区分不同场景下的持仓打印
        """
        # 获取当前现金
        cash = self.broker.getcash()
        
        # 获取当前总资产
        total_value = self.get_total_asset()
        
        # 获取当前日期
        current_date = self.datas[0].datetime.date(0) if len(self.datas) > 0 else None
        
        logger.info(f"========== {tag} ({current_date}) ==========")
        logger.info(f"现金: {cash:.2f}")
        
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