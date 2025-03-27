import sys
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
import backtrader as bt
from loguru import logger
import pandas as pd
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError
from database.db_funds import DBFunds
from database.db_funds_nav import DBFundsNav
from database.db_stocks_day_hist import DBStocksDayHist
from database.mysql_database import MySQLDatabase
from task_backtrader.strategy.buy_and_hold_strategy import BuyAndHoldStrategy
from task_backtrader.strategy.rebalance_strategy import RebalanceStrategy
from task_backtrader.feeds.pandas_data_extends import PandasDataExtends

class BacktraderBaseTask(BaseTask):
    """Backtrader任务基类，负责连接数据库获取基金和股票数据"""
    
    def __init__(self, mysql_db: MySQLDatabase, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - data_params: 数据参数
                    - fund_codes: 基金代码列表
                    - stock_symbols: 股票代码列表
        """
        super().__init__(task_config)
        
        # 验证task_config
        required_keys = ['data_params']
        for key in required_keys:
            if key not in self.task_config:
                raise TaskConfigError(f"task_config缺少必要的键: {key}")

        # 验证数据参数
        data_params_str = self.task_config['data_params']
        try:
            data_params = json.loads(data_params_str)  # 将JSON字符串解析为字典
        except json.JSONDecodeError as e:
            raise TaskConfigError(f"data_params格式错误: {str(e)}")

        self.mysql_db = mysql_db
        self.db_funds_nav = DBFundsNav(self.mysql_db)
        self.db_stocks_day_hist = DBStocksDayHist(self.mysql_db)

        # 获取基金数据
        self.funds_code = data_params['fund_codes'] if 'fund_codes' in data_params else []
        self.funds_nav = {}
        for fund_code in self.funds_code:
            self.funds_nav[fund_code] = self.db_funds_nav.get_fund_nav(fund_code)
            logger.debug(f"基金 {fund_code} 的净值数据共计: {len(self.funds_nav[fund_code])} 条")

        # 获取股票数据
        self.stock_symbols = data_params['stock_symbols'] if 'stock_symbols' in data_params else []
        self.stocks_day_hist = {}
        for symbol in self.stock_symbols:
            self.stocks_day_hist[symbol] = self.db_stocks_day_hist.get_stock_hist_data(symbol)
            logger.debug(f"股票 {symbol} 的历史数据共计: {len(self.stocks_day_hist[symbol])} 条")

    def make_data(self) -> Dict[str, bt.feeds.DataBase]:
        """
        准备回测数据
        
        Returns:
            Dict[str, bt.feeds.DataBase]: 数据源字典
        """
        data_feeds = {}

        # 处理基金数据
        for fund_code in self.funds_code:
            df = self.funds_nav[fund_code]
            if df is None or df.empty:
                raise TaskConfigError(f"无法获取基金{fund_code}的净值数据")

            df['nav_date'] = pd.to_datetime(df['nav_date'])
            df['dividend'] = df['dividend'].fillna(0)
            
            data = PandasDataExtends(
                dataname=df,
                datetime='nav_date',
                open='unit_nav',
                high='unit_nav',
                low='unit_nav',
                close='unit_nav',
                volume=-1,
                dividend='dividend'
            )
            data_feeds[fund_code] = data

        # 处理股票数据
        for symbol in self.stock_symbols:
            df = self.stocks_day_hist[symbol]
            if df is None or df.empty:
                raise TaskConfigError(f"无法获取股票{symbol}的历史数据")

            df['date'] = pd.to_datetime(df['date'])
            df['dividend'] = 0
            
            data = PandasDataExtends(
                dataname=df,
                datetime='date',
                open='open',
                high='high',
                low='low',
                close='close',
                volume='volume',
                dividend='dividend'  # 股票数据可能没有分红信息
            )
            data_feeds[symbol] = data

        return data_feeds

    def make_strategy(self, strategy_params: Dict[str, Any]) -> bt.Strategy:
        """
        根据策略参数创建策略
        
        Args:
            strategy_params: 策略参数字典
            
        Returns:
            tuple: (策略类, 策略参数)
        """
        strategy_name = strategy_params['name']
        if strategy_name == 'BuyAndHold':
            return BuyAndHoldStrategy
        elif strategy_name == 'Rebalance':
            return RebalanceStrategy
        return None

    def close(self) -> None:
        """关闭数据库连接"""
        if hasattr(self, 'mysql_db'):
            self.mysql_db.close_connection()
            logger.debug("数据库连接已关闭")

if __name__ == "__main__":
    class TestTask(BacktraderBaseTask):
        def run(self):
            data_feeds = self.make_data()
            for name, feed in data_feeds.items():
                logger.info(f"{name}: {len(feed.dataname)} records")
    
    task_config = {
        "name": "test_task",
        "description": "测试任务",
        "data_params": """{
            "fund_codes": ["003376", "007540"],
            "stock_symbols": ["159949.SZ", "512550.SS"]
        }"""
    }

    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )

    task = TestTask(mysql_db, task_config)
    task.execute()
    task.close()
