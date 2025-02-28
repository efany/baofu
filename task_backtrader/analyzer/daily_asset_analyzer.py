import backtrader as bt
from loguru import logger
from datetime import datetime

class DailyAssetAnalyzer(bt.Analyzer):
    def __init__(self):
        self.initial_cash = self.strategy.broker.startingcash
        self.open_date = datetime.strptime(self.strategy.params['open_date'], '%Y-%m-%d')
        logger.info(f"初始资金: {self.initial_cash}, 开仓日期: {self.open_date}")
        self.daily_assets = []

    def start(self):
        self.daily_assets = []

    def next(self):
        if self.strategy.datetime.datetime() < self.open_date:
            return

        # 记录每日资产与净值
        asset_value = self.strategy.broker.getvalue()
        self.daily_assets.append({'date': self.strategy.datetime.date(),
                                  'cash': self.strategy.broker.getcash(),
                                  'asset': self.strategy.broker.getvalue() - self.strategy.broker.getcash(),
                                  'total': self.strategy.broker.getvalue()})
        logger.info(f"当前日期: {self.strategy.datetime.date()},"
                    f"当前现金: {self.strategy.broker.getcash():.2f},"
                    f"当前资产: {(self.strategy.broker.getvalue() - self.strategy.broker.getcash()):.2f},"
                    f"当前净值: {self.strategy.broker.getvalue():.2f}")

    def stop(self):
        pass
        # 输出每日资产与净值
        # for asset in self.daily_assets:
        #     logger.info(f"日期: {asset['date']}, 资产: {asset['asset_value']}")