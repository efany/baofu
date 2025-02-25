import sys
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
import backtrader as bt
from loguru import logger
import pandas as pd
import json  # 添加json模块导入

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError
from database.db_funds import DBFunds
from database.db_funds_nav import DBFundsNav
from database.mysql_database import MySQLDatabase
from task_backtrader.strategy.buy_and_hold_strategy import BuyAndHoldStrategy
from task_backtrader.feeds.pandas_data_extends import PandasDataExtends

class BacktraderBaseTask(BaseTask):
    """Backtrader任务基类，负责连接数据库获取基金数据"""
    
    def __init__(self, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - db_config: 数据库配置
                    - host: 数据库主机
                    - user: 用户名
                    - password: 密码
                    - database: 数据库名称
                - data_params: 数据参数
                    - start_date: 数据起始日期 (YYYY-MM-DD)
                    - end_date: 数据结束日期 (YYYY-MM-DD)
                    - fund_codes: 基金代码列表
        """
        super().__init__(task_config)
        
        # 验证task_config
        required_keys = ['db_config', 'data_params']
        for key in required_keys:
            if key not in self.task_config:
                raise TaskConfigError(f"task_config缺少必要的键: {key}")
                
        # 验证数据库配置
        db_config = self.task_config['db_config']
        required_db_params = ['host', 'user', 'password', 'database']
        for param in required_db_params:
            if param not in db_config:
                raise TaskConfigError(f"db_config缺少{param}")

        # 验证数据参数
        data_params_str = self.task_config['data_params']
        try:
            data_params = json.loads(data_params_str)  # 将JSON字符串解析为字典
        except json.JSONDecodeError as e:
            raise TaskConfigError(f"data_params格式错误: {str(e)}")
            
        required_data_params = ['fund_codes']
        for param in required_data_params:
            if param not in data_params:
                raise TaskConfigError(f"data_params缺少{param}")

        # 初始化数据库连接
        self.mysql_db = MySQLDatabase(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )

        # 初始化DBFunds和DBFundsNav
        self.db_funds = DBFunds(self.mysql_db)
        self.db_funds_nav = DBFundsNav(self.mysql_db)

        self.funds_code = data_params['fund_codes']
        self.funds_info = {}
        self.funds_nav = {}

        for fund_code in data_params['fund_codes']:
            self.funds_info[fund_code] = self.db_funds.get_fund_info(fund_code)
            self.funds_nav[fund_code] = self.db_funds_nav.get_fund_nav(fund_code, 
                'start_date' in data_params and data_params['start_date'] or None, 
                'end_date' in data_params and data_params['end_date'] or None)
            logger.debug(f"基金 {fund_code} 的净值数据共计: {len(self.funds_nav[fund_code])} 条")
    
    def make_data(self) -> Dict[str, bt.feeds.DataBase]:
        """
        准备回测数据
        
        Returns:
            Dict[str, bt.feeds.DataBase]: 数据源字典
        """
        data_feeds = {}
        
        for fund_code in self.funds_code:
            # 获取基金净值数据
            df = self.funds_nav[fund_code]
            if df is None or df.empty:
                raise TaskConfigError(f"无法获取基金{fund_code}的净值数据")

            df['nav_date'] = pd.to_datetime(df['nav_date'])
            df['dividend'] = df['dividend'].fillna(0)
            
            data = PandasDataExtends(dataname=df,
                                        datetime='nav_date',
                                        open='unit_nav',
                                        high='unit_nav',
                                        low='unit_nav',
                                        close='unit_nav',
                                        volume=-1,
                                        dividend='dividend')
            data_feeds[fund_code] = data

        return data_feeds

    def make_strategy(self, strategy_params: Dict[str, Any]) -> tuple[bt.Strategy, Dict[str, Any]]:
        """
        根据策略参数创建策略
        
        Args:
            strategy_params: 策略参数字典
            
        Returns:
            tuple: (策略类, 策略参数)
        """
        strategy_name = strategy_params['name']
        if strategy_name == 'BuyAndHold':
            return (BuyAndHoldStrategy, strategy_params)
        return None

    def close(self) -> None:
        """关闭数据库连接"""
        if hasattr(self, 'mysql_db'):
            self.mysql_db.close_connection()
            logger.debug("数据库连接已关闭")

if __name__ == "__main__":

    class TestTask(BacktraderBaseTask):
        def run(self):
            for fund_code in self.funds_code:
                logger.info(self.funds_info)
                logger.info(self.funds_nav)
            print("run")
    
    task_config = {
        "db_config": {
            "host": "127.0.0.1",
            "user": "baofu",
            "password": "TYeKmJPfw2b7kxGK",
            "database": "baofu"
        },
        "data_params": """{
            "start_date": "2020-01-01",
            "end_date": "2020-12-31",
            "fund_codes": ["003376","007540"]    
        }"""
    }

    task = TestTask(task_config)
    task.run()
    task.close()
