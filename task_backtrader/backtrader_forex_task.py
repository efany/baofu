import sys
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
import backtrader as bt
from loguru import logger
import pandas as pd
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task.exceptions import TaskConfigError, TaskExecutionError
from database.db_forex_day_hist import DBForexDayHist
from database.mysql_database import MySQLDatabase
from task_backtrader.strategy.forex_rebalance_strategy import ForexRebalanceStrategy
from task_backtrader.feeds.pandas_data_extends import PandasDataExtends
from task_backtrader.backtrader_base_task import BacktraderBaseTask

class BacktraderForexTask(BacktraderBaseTask):
    """外汇回测任务，用于测试不同外汇组合的收益情况"""
    
    def __init__(self, mysql_db: MySQLDatabase, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - base_currency: 基础货币代码（如 'USD'）
                - initial_positions: 初始持仓配置，例如：
                    [
                        {"currency": "CNH", "amount": 1000000},
                        {"currency": "USD", "amount": 100000}
                    ]
                - forex_pairs: 外汇对列表，每个元素包含：
                    - symbol: 外汇对代码（如 'USDCNH'）
                    - weight: 权重（0-1之间的小数）
                - start_date: 回测开始日期，格式：YYYY-MM-DD
                - end_date: 回测结束日期，格式：YYYY-MM-DD
        """
        super().__init__(mysql_db, task_config)
        
        # 验证task_config
        required_keys = ['base_currency', 'initial_positions', 'forex_pairs', 'start_date', 'end_date']
        for key in required_keys:
            if key not in self.task_config:
                raise TaskConfigError(f"task_config缺少必要的键: {key}")
                
        if not isinstance(self.task_config['forex_pairs'], list):
            raise TaskConfigError("forex_pairs必须是列表类型")
            
        if not isinstance(self.task_config['initial_positions'], list):
            raise TaskConfigError("initial_positions必须是列表类型")
            
        # 验证初始持仓
        for position in self.task_config['initial_positions']:
            if 'currency' not in position or 'amount' not in position:
                raise TaskConfigError("initial_positions中的每个元素必须包含currency和amount字段")
            if not isinstance(position['amount'], (int, float)) or position['amount'] <= 0:
                raise TaskConfigError("initial_positions中的amount必须是正数")
                
        # 验证权重总和是否为1
        total_weight = sum(pair['weight'] for pair in self.task_config['forex_pairs'])
        if abs(total_weight - 1.0) > 0.0001:  # 允许小的浮点数误差
            raise TaskConfigError("所有外汇对的权重之和必须为1")

        # 准备数据
        self.data_feeds = self.make_data()

        # 解析data_params
        self.strategy_param = json.loads(self.task_config['strategy'])
        self.strategy_class = self.make_strategy(self.strategy_param)
        
    def run(self) -> None:
        """执行回测任务"""
        try:
            # 创建Cerebro引擎
            cerebro = bt.Cerebro()
            
            # 添加数据
            for symbol, data in self.data_feeds.items():
                cerebro.adddata(data)
            
            # 设置初始资金（使用基础货币的总价值）
            initial_value = 0
            for position in self.task_config['initial_positions']:
                currency = position['currency']
                amount = position['amount']
                
                if currency == self.task_config['base_currency']:
                    initial_value += amount
                else:
                    # 获取第一个交易日的汇率
                    pair_symbol = f"{self.task_config['base_currency']}{currency}"
                    if pair_symbol in self.data_feeds:
                        rate = self.data_feeds[pair_symbol].dataname.iloc[0]['close']
                        initial_value += amount / rate
                    else:
                        raise TaskExecutionError(f"无法找到将{currency}转换为{self.task_config['base_currency']}的汇率数据")
            
            logger.info(f"初始资金: {initial_value}")
            cerebro.broker.setcash(initial_value)
            
            # 设置手续费
            cerebro.broker.setcommission(commission=0.0)  # 0.0%的手续费

            # 添加策略
            strategy_params = {
                'forex_pairs': self.task_config['forex_pairs'],
                'base_currency': self.task_config['base_currency'],
                'initial_positions': self.task_config['initial_positions']
            }
            cerebro.addstrategy(self.strategy_class, **strategy_params)
            
            # 添加分析器
            cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
            cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
            cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
            
            # 运行回测
            logger.info("开始回测...")
            results = cerebro.run()
            strategy = results[0]
            
            # 收集回测结果
            self.task_result = {
                'initial_value': initial_value,
                'final_value': cerebro.broker.getvalue(),
                'total_return': strategy.analyzers.returns.get_analysis()['rtot'],
                'annual_return': strategy.analyzers.returns.get_analysis()['rnorm'],
                'sharpe_ratio': strategy.analyzers.sharpe.get_analysis()['sharperatio'],
                'max_drawdown': strategy.analyzers.drawdown.get_analysis()['max']['drawdown'],
                'max_drawdown_length': strategy.analyzers.drawdown.get_analysis()['max']['len']
            }
            
            logger.success("回测完成")
            logger.info(f"初始价值: {initial_value:,.2f} {self.task_config['base_currency']}")
            logger.info(f"最终价值: {self.task_result['final_value']:,.2f} {self.task_config['base_currency']}")
            logger.info(f"总收益率: {self.task_result['total_return']*100:.2f}%")
            logger.info(f"年化收益率: {self.task_result['annual_return']*100:.2f}%")
            logger.info(f"夏普比率: {self.task_result['sharpe_ratio']:.2f}")
            logger.info(f"最大回撤: {self.task_result['max_drawdown']*100:.2f}%")
            logger.info(f"最大回撤持续期: {self.task_result['max_drawdown_length']}天")
            
        except Exception as e:
            logger.error(f"回测过程中发生错误: {str(e)}")
            raise TaskExecutionError(f"回测失败: {str(e)}")


def main():
    """主函数，用于测试"""
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )

    # 测试配置
    task_config = {
        "name": "forex_backtest",
        "description": "外汇组合回测",
        "data_params": """
            {
                "forex_symbols": ["USDCNH", "USDJPY", "USDCHF"]
            }
        """,
        "base_currency": "CNH",
        "initial_positions": [
            {"currency": "CNH", "amount": 1000000},
            {"currency": "USD", "amount": 100000}
        ],
        "strategy": """
            {
                "name": "ForexRebalance",
                "forex_pairs": [
                    {"symbol": "CNH", "weight": 0.8},
                    {"symbol": "USD", "weight": 0.2}
                ],
                "start_date": "2023-01-01",
                "end_date": "2024-03-20"
            }
        """
    }
    
    # 创建并执行任务
    task = BacktraderForexTask(mysql_db, task_config)
    try:
        task.execute()
    finally:
        task.close()


if __name__ == "__main__":
    main() 