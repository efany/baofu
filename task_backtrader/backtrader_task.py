import sys
import os
import json
from typing import Dict, Any
from datetime import datetime
import backtrader as bt
from loguru import logger
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from task.exceptions import TaskConfigError
from task_backtrader.backtrader_base_task import BacktraderBaseTask
from task_backtrader.strategy.buy_and_hold_strategy import BuyAndHoldStrategy
from task_backtrader.analyzer.trade_list_analyzer import TradeListAnalyzer
from task_backtrader.analyzer.daily_asset_analyzer import DailyAssetAnalyzer
from task_backtrader.analyzer.pairing_analyzer import PairingAnalyzer
from task_backtrader.commissions.zero_commission import ZeroCommission
from database.mysql_database import MySQLDatabase

class BacktraderTask(BacktraderBaseTask):
    """
    使用Backtrader实现买入持有策略的回测任务
    """

    def __init__(self, mysql_db: MySQLDatabase, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - db_config: 数据库配置
                - data_params: 数据参数JSON字符串
                    - fund_codes: 基金代码列表
                    - stock_symbols: 股票代码列表
                - strategy: 策略
                    {
                        "name": "BuyAndHold",
                        "open_date": "2024-01-01",
                        "close_date": "2024-12-30",
                        "products": ["003376"],
                        "weights": [1.0]
                    }
                - initial_cash: 初始资金，默认1_000_000.0
        """
        super().__init__(mysql_db, task_config)
        
        self.initial_cash = self.task_config.get('initial_cash', 1_000_000.0)

        # 准备数据
        # self.data_feeds = self.make_data(extra_fields=['MA360'])
        self.data_feeds = self.make_data()
        
        # 解析data_params
        self.strategy_param = json.loads(self.task_config['strategy'])
        self.strategy_class = self.make_strategy(self.strategy_param)

    def run(self) -> None:
        """执行回测任务"""
        try:
            # 创建cerebro引擎
            cerebro = bt.Cerebro()
            
            # 设置初始资金
            cerebro.broker.setcash(self.initial_cash)

            # 设置零费率佣金方案
            cerebro.broker.addcommissioninfo(ZeroCommission())
            
            # 添加数据源
            for name, data in self.data_feeds.items():
                cerebro.adddata(data, name=name)
            
            # 添加策略
            cerebro.addstrategy(self.strategy_class, params=self.strategy_param)
            
            # 添加分析器
            cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
            cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
            cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
            cerebro.addanalyzer(TradeListAnalyzer, _name='trade_list')
            cerebro.addanalyzer(DailyAssetAnalyzer, _name='daily_asset')
            cerebro.addanalyzer(PairingAnalyzer, _name='pairing')
            
            # 运行回测
            results = cerebro.run()
            
            # 获取回测结果
            strat = results[0]
            daily_asset = strat.analyzers.daily_asset.get_analysis()
            trade_list = strat.analyzers.trade_list.get_analysis()
            pairing = strat.analyzers.pairing.get_analysis()

            portfolio_value = daily_asset[-1]['total']
            
            # 收集持仓详情
            positions = []
            for data in strat.datas:
                pos = strat.getposition(data)
                if pos.size != 0:  # 如果有持仓
                    # 获取最新净值
                    current_nav = data.close[0]  # 当前净值
                    cost_nav = pos.price         # 持仓成本净值
                    
                    positions.append({
                        'fund_code': data._name,     # 基金代码
                        'size': pos.size,         # 持仓数量
                        'price': pos.price,       # 持仓价格
                        'current_nav': current_nav,  # 当前净值
                    })
            
            # 保存回测结果
            self.task_result = {
                'initial_value': self.initial_cash,
                'final_value': portfolio_value,
                'return_rate': (portfolio_value - self.initial_cash) / self.initial_cash * 100,
                'positions': positions,  # 持仓详情
                'daily_asset': daily_asset,
                'trades': trade_list,
                'pairing': pairing
            }
            
        except Exception as e:
            logger.error(f"回测执行失败: {str(e)}")
            raise TaskConfigError(f"回测执行失败: {str(e)}")

def test_buy_and_hold(mysql_db):
    # 测试配置
    task_config = {
        "name": "buy_and_hold_backtest",
        "description": "买入持有策略回测",
        "data_params": """
            {
                "fund_codes": ["007540","003376"],
                "stock_symbols": ["159949.SZ", "512550.SS", "159633.SZ", "159628.SZ"]
            }
        """,
        "initial_cash": 1000000,
        "strategy": """
            {
                "name": "BuyAndHold",
                "open_date": "2025-01-01",
                "close_date": "",
                "dividend_method": "reinvest",
                "products": ["159949.SZ", "512550.SS"],
                "weights": [0.5,0.5]
            }
        """
    }

    # 执行回测
    task = BacktraderTask(mysql_db, task_config)
    task.execute()

    if task.is_success:
        logger.info("回测完成")
        result = task.result
        logger.info(f"初始资金: {result['initial_value']:.2f}")
        logger.info(f"最终资金: {result['final_value']:.2f}")
        logger.info(f"收益率: {result['return_rate']:.2f}%")
        logger.info(f"持仓详情: {result['positions']}")

    else:
        logger.error(f"回测失败: {task.error}")
def test_rebalance(mysql_db):
                    #     "period": {
                    #     "freq": "month",
                    #     "day": 1,
                    #     "watermark": 0.01
                    # }

                    
                # "triggers": {
                #     "sorting": {
                #         "factor": "MA360",
                #         "top_n": 4,
                #         "weights": [0.3, 0.3, 0.2, 0.2],
                #         "freq": "quarter",
                #         "day": 1
                #     }
                # },
    # 测试配置
    task_config = {
        "name": "rebalance_backtest",
        "description": "再平衡策略回测",
        "data_params": """
            {
                "stock_symbols": ["512480.SS", "515220.SS"]
            }
        """,
        "initial_cash": 1000000,
        "strategy": """
            {
                "name": "Rebalance",
                "open_date": "2025-01-01",
                "dividend_method": "reinvest",
                "triggers": {
                    "r_pairing": {
                        "products": ["512480.SS", "515220.SS"]
                    }
                },
                "target_weights": {
                    "512480.SS": 0.5,
                    "515220.SS": 0.5
                }
            }
        """
    }

    # 执行回测
    task = BacktraderTask(mysql_db, task_config)
    task.execute()

    if task.is_success:
        logger.info("回测完成")
        result = task.result
        logger.info(f"初始资金: {result['initial_value']:.2f}")
        logger.info(f"最终资金: {result['final_value']:.2f}")
        logger.info(f"收益率: {result['return_rate']:.2f}%")
        logger.info(f"持仓详情: {result['positions']}")

    else:
        logger.error(f"回测失败: {task.error}")

if __name__ == "__main__":
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu',
        pool_size=5
    )
    # test_buy_and_hold(mysql_db)
    test_rebalance(mysql_db)


    
