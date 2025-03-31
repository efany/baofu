import backtrader as bt
from loguru import logger
from datetime import datetime

class DailyAssetAnalyzer(bt.Analyzer):
    def __init__(self):
        self.initial_cash = self.strategy.broker.startingcash
        self.open_date = self.strategy.open_date
        self.daily_assets = []

    def start(self):
        self.daily_assets = []

    def next(self):
        if not self.strategy.position_opened and not self.strategy.position_closed:
            return

        # 获取当前现金
        cash = self.strategy.broker.getcash()

        # 获取每个产品的资产值
        product_assets = {}
        total_asset = 0
        
        for i, data in enumerate(self.datas):
            product_value = self.strategy.broker.get_value([data])
            product_code = data._name
            product_assets[product_code] = product_value
            total_asset += product_value

        # 计算总资产
        total = cash + total_asset

        # 记录每日资产与净值
        daily_data = {
            'date': self.strategy.datetime.date(),
            'cash': cash,
            'asset': total_asset,
            'total': total,
            'products': product_assets  # 添加每个产品的资产值
        }
        
        self.daily_assets.append(daily_data)

    def stop(self):
        pass
        # 输出每日资产与净值
        # for asset in self.daily_assets:
        #     logger.info(f"日期: {asset['date']}, 资产: {asset['asset_value']}")
    
    def get_analysis(self):
        """返回交易记录"""
        return self.daily_assets 