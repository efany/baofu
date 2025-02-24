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

class BacktraderBuyAndHoldTask(BacktraderBaseTask):
    """
    使用Backtrader实现买入持有策略的回测任务
    """
    
    def __init__(self, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - db_config: 数据库配置
                - data_params: 数据参数JSON字符串
                    - start_date: 回测开始日期
                    - end_date: 回测结束日期
                    - fund_codes: 基金代码列表
                - strategys: 策略列表
                    [
                        {
                            "name": "BuyAndHold",
                            "open_date": "2023-01-01",
                            "products": ["003376"],
                            "weights": [1.0]
                        }
                    ]
                - initial_cash: 初始资金，默认1_000_000.0
        """
        super().__init__(task_config)
        
        self.initial_cash = self.task_config.get('initial_cash', 1_000_000.0)
        
        self.strategys = []
        # 解析data_params
        strategy_params = json.loads(self.task_config['strategys'])

        for index, strategy_params in enumerate(strategy_params):
            logger.info(f"策略 ## {index}: {strategy_params}")
            self.strategys.append(self.make_strategy(strategy_params))

    def prepare_data(self) -> Dict[str, bt.feeds.DataBase]:
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
            
            data = bt.feeds.PandasData(dataname=df,
                                       datetime='nav_date',
                                       open='unit_nav',
                                       high='unit_nav',
                                       low='unit_nav',
                                       close='unit_nav',
                                       volume=-1)
            data_feeds[fund_code] = data

        return data_feeds

    def run(self) -> None:
        """执行回测任务"""
        try:
            # 创建cerebro引擎
            cerebro = bt.Cerebro()
            
            # 设置初始资金
            cerebro.broker.setcash(self.initial_cash)
            
            # 设置手续费
            cerebro.broker.setcommission(commission=0.0)  # 设置0.0%的手续费
            
            # 添加数据
            data_feeds = self.prepare_data()
            for name, data in data_feeds.items():
                logger.info(f"添加数据: {name}, 条数: {len(data)}")
                cerebro.adddata(data, name=name)
            
            # 添加策略
            for strategy_class, strategy_params in self.strategys:
                strategy_params_json = json.dumps(strategy_params)
                cerebro.addstrategy(strategy_class, params_json=strategy_params_json)
            
            # 添加分析器
            cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
            cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
            cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
            cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
            
            # 运行回测
            logger.info(f'初始资金: {cerebro.broker.getvalue():.2f}')
            results = cerebro.run()
            logger.info(f'最终资金: {cerebro.broker.getvalue():.2f}')
            
            # 获取回测结果
            strat = results[0]
            portfolio_value = cerebro.broker.getvalue()
            
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
                'returns': strat.analyzers.returns.get_analysis(),
                'drawdown': strat.analyzers.drawdown.get_analysis(),
                'trades': strat.analyzers.trades.get_analysis(),
                'sharpe': strat.analyzers.sharpe.get_analysis()
            }
            
            logger.info(f"回测结果: {self.task_result}")
            
        except Exception as e:
            logger.error(f"回测执行失败: {str(e)}")
            raise TaskConfigError(f"回测执行失败: {str(e)}")


if __name__ == "__main__":
    # 测试配置
    task_config = {
        "name": "buy_and_hold_backtest",
        "description": "买入持有策略回测",
        "db_config": {
            "host": "127.0.0.1",
            "user": "baofu",
            "password": "TYeKmJPfw2b7kxGK",
            "database": "baofu"
        },
        "data_params": json.dumps({
            "start_date": "2024-01-01",
            "fund_codes": ["003376","007540"]
        }),
        "initial_cash": 1000000,
        "strategys": """
            [
                {
                    "name": "BuyAndHold",
                    "open_date": "2024-01-01",
                    "products": ["003376","007540"],
                    "weights": [0.5,0.5]
                }
            ]
        """
    }
    
    # 执行回测
    task = BacktraderBuyAndHoldTask(task_config)
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
