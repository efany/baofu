from datetime import datetime
from loguru import logger
from typing import Dict, Any, List
import math
from .extend_strategy import ExtendStrategy

class CurrentRateStrategy(ExtendStrategy):
    """处理现金和外汇产品的计息策略"""
    
    def __init__(self, params: Dict[str, Any]):
        """
        初始化策略
        
        Args:
            params: 策略参数，包含：
                - bond_rate: 债券利率配置，格式为 {"货币": "债券类型"}，例如：{"CNY": "CN_10Y", "USD": "US_10Y"}
                - forex_pairs: 外汇对配置，格式为 ["USDCNH", "JPYCNH"]
        """
        super().__init__(params)
        
        # 添加债券利率配置
        self.current_rate = self.params.get('current_rate', {})
        self.last_rate = {}
        self.last_trade_date = {}
                
        logger.info(f"现金利率配置: {self.current_rate}")

    def open_trade(self):
        """开仓"""
        pass
    
    def close_trade(self):
        """平仓"""
        pass

    def next(self):
        """执行"""
        
        current_date = self.main_strategy.datas[0].datetime.date(0)

        for currency, bond_type in self.current_rate.items():
            if currency not in self.last_trade_date:
                self.last_trade_date[currency] = current_date
                continue
            # 获取货币持仓
            if currency == "CNY":
                # CNY表示现金持仓
                position = self.broker.getcash()
            else:
                # 其他货币通过外汇对获取持仓
                forex_data = self.get_data(f"{currency}")
                if forex_data is None:
                    logger.error(f"找不到{currency}")
                    continue
                position = self.broker.get_value([forex_data])

            data = self.get_data(bond_type)
            if data is None:
                logger.error(f"找不到债券{bond_type}的数据")
                continue
            rate = data.close[0]
            if (rate is None or math.isnan(rate)) and currency in self.last_rate:
                rate = self.last_rate[currency]

            if rate is not None and not math.isnan(rate):
                days = (current_date - self.last_trade_date[currency]).days
                interest = position * rate / 366 / 100 * days
                current_cash = self.broker.getcash()
                self.broker.setcash(current_cash + interest)
                
                # 更新累计利息记录
                if currency not in self.main_strategy.current_interests:
                    self.main_strategy.current_interests[currency] = interest
                else:
                    self.main_strategy.current_interests[currency] += interest

                logger.info(f"日期：{current_date}, last_trade_date: {self.last_trade_date[currency]}， 计息天数: {days}， {currency}的利率为{rate}, 现金利息为{position}:{interest}, 累计利息: {self.main_strategy.current_interests[currency]}")
                self.last_trade_date[currency] = current_date
                self.last_rate[currency] = rate

    def get_data(self, symbol: str):
        """获取数据"""
        if symbol == "":
            return None
        return self.main_strategy.getdatabyname(symbol)