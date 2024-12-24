import backtrader as bt
import pandas as pd
from datetime import datetime
from typing import Type, Dict, Any
import os
from .analyzers import PeriodicalReturns
from .recent_returns import RecentReturns
from .trade_recorder import TradeRecorder

class Backtester:
    """回测器类，用于执行回测"""
    
    def __init__(self, 
                 strategy_class: Type[bt.Strategy],
                 data_path: str,
                 cash: float = 100000.0,
                 commission: float = 0.001):
        """
        初始化回测器
        
        Args:
            strategy_class: 策略类
            data_path: 数据文件路径
            cash: 初始资金
            commission: 手续费率
        """
        self.strategy_class = strategy_class
        self.data_path = data_path
        self.cash = cash
        self.commission = commission
        self.cerebro = None
        self.results = None

    def prepare_data(self) -> bt.feeds.PandasData:
        """准备回测数据"""
        # 读取Excel数据
        df = pd.read_excel(self.data_path)
        
        # 打印列名，帮助调试
        print("Available columns:", df.columns.tolist())
        
        # 确保日期列是datetime类型
        date_column = '日期'  # 修改为实际的日期列名
        nav_column = '累计净值'  # 修改为累计净值列名
        
        # 重命名列以符合处理需求
        df = df.rename(columns={
            date_column: 'date',
            nav_column: 'nav'
        })
        
        # 转换日期
        df['date'] = pd.to_datetime(df['date'])
        
        # 按日期升序排序
        df = df.sort_values('date', ascending=True)
        print(f"\nData range: from {df['date'].min()} to {df['date'].max()}")
        print(f"Total records: {len(df)}")
        
        # 设置日期为索引
        df.set_index('date', inplace=True)
        
        # 打印前几行数据，帮助验证
        print("\nFirst few rows after sorting:")
        print(df.head())
        
        # 创建backtrader数据源
        data = bt.feeds.PandasData(
            dataname=df,
            datetime=None,  # 使用索引作为日期
            open='nav',    # 使用累计净值作为open/high/low/close
            high='nav',
            low='nav',
            close='nav',
            volume=-1,      # 不使用交易量
            openinterest=-1 # 不使用持仓量
        )
        
        return data

    def run(self, strategy_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """执行回测"""
        # 创建回测引擎
        self.cerebro = bt.Cerebro()
        
        # 设置初始资金
        self.cerebro.broker.setcash(self.cash)
        
        # 设置手续费
        self.cerebro.broker.setcommission(commission=self.commission)
        
        # 添加数据
        data = self.prepare_data()
        self.cerebro.adddata(data)
        
        # 添加策略
        if strategy_params is None:
            strategy_params = {}
        
        # 确保时间区间参数存在
        if 'start_date' not in strategy_params:
            strategy_params['start_date'] = None
        if 'end_date' not in strategy_params:
            strategy_params['end_date'] = None
        
        self.cerebro.addstrategy(self.strategy_class, **strategy_params)
        
        # 添加分析器
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)
        self.cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        self.cerebro.addanalyzer(PeriodicalReturns, _name='periodical')
        self.cerebro.addanalyzer(RecentReturns, _name='recent')
        self.cerebro.addanalyzer(TradeRecorder, _name='recorder')
        
        # 运行回测
        self.results = self.cerebro.run()
        
        # 收集回测结果
        strat = self.results[0]
        
        # 打印分析报告
        strat.analyzers.periodical.print_report()
        strat.analyzers.recent.print_report()
        
        # 保存分析结果到Excel并绘制交易图表
        strat.analyzers.recorder.save_to_excel(
            output_dir="output",
            recent_analyzer=strat.analyzers.recent,
            periodical_analyzer=strat.analyzers.periodical
        )
        
        # 安全地获取分析结果
        returns_analysis = strat.analyzers.returns.get_analysis()
        sharpe_analysis = strat.analyzers.sharpe.get_analysis()
        drawdown_analysis = strat.analyzers.drawdown.get_analysis()
        periodical_analysis = strat.analyzers.periodical.get_analysis()
        recent_analysis = strat.analyzers.recent.get_analysis()
        
        return {
            'initial': self.cash,
            'final': self.cerebro.broker.getvalue(),
            'returns': returns_analysis.get('rtot', 0.0),
            'sharpe': sharpe_analysis.get('sharperatio', 0.0) or 0.0,
            'drawdown': drawdown_analysis.get('max', {}).get('drawdown', 0.0),
            'periodical': periodical_analysis,
            'recent': recent_analysis
        }

    def plot(self, output_dir: str = "output"):
        """绘制回测结果图表"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        plt_file = os.path.join(output_dir, f'backtest_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png')
        # self.cerebro.plot(style='candlestick', savefig=plt_file) 