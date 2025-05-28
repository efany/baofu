import backtrader as bt
from loguru import logger
from datetime import datetime
import math
class DailyAssetAnalyzer(bt.Analyzer):
    def __init__(self):
        self.initial_cash = self.strategy.broker.startingcash
        self.open_date = self.strategy.open_date
        self.daily_assets = []

    def start(self):
        self.daily_assets = []

    def next(self):
        if not (self.strategy.is_open_traded and not self.strategy.is_close_traded):
            return

        # 获取当前现金
        cash = self.strategy.broker.getcash()

        # 获取每个产品的资产值
        product_assets = {}
        total_asset = 0

        for i, data in enumerate(self.datas):
            product_value = self.strategy.broker.get_value([data])
            if product_value is None or math.isnan(product_value):
                product_value = 0
            product_code = data._name
            product_assets[product_code] = product_value
            total_asset += product_value

        # 获取融资信息
        total_financing = 0
        total_financing_interest = 0
        financing_info = self.strategy.financing_info
        for pair, info in financing_info.items():
            product_value = 0
            for i, data in enumerate(self.datas):
                if data._name == pair:
                    product_value = data.close[0] * info['shares']
                    total_financing += product_value
                    total_asset -= product_value
                    total_financing_interest += info['total_interest']

        # 获取现金利息
        product_interests = {}
        total_cash_interest = 0
        current_interests = self.strategy.current_interests
        for currency, interest in current_interests.items():
            product_interests[currency] = interest
            total_cash_interest += interest

        # 计算总资产
        total = cash + total_asset

        # 记录每日资产与净值
        daily_data = {
            'date': self.strategy.datetime.date(),
            'cash': cash,
            'asset': total_asset,
            'total': total,
            'financing': total_financing,
            'financing_interest': total_financing_interest,
            'cash_interest': total_cash_interest,
            'products': product_assets,  # 添加每个产品的资产值
            'product_interests': product_interests  # 添加每个产品的利息
        }
        
        self.daily_assets.append(daily_data)

    def stop(self):
        pass
    
    def get_analysis(self):
        """返回交易记录"""
        return self.daily_assets 