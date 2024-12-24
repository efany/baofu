import backtrader as bt
from collections import defaultdict
from datetime import datetime
import pandas as pd
from .base_analyzer import BaseTimeFrameAnalyzer

class PeriodicalReturns(BaseTimeFrameAnalyzer):
    """
    分析每月、每季度、每年的收益情况
    """
    
    params = (
        ('timeframe', 'all'),  # 'monthly', 'quarterly', 'yearly', 'all'
    )
    
    def __init__(self):
        super(PeriodicalReturns, self).__init__()
        self.monthly_returns = defaultdict(float)   # 月度收益
        self.quarterly_returns = defaultdict(float) # 季度收益
        self.yearly_returns = defaultdict(float)    # 年度收益
        self.start_values = defaultdict(float)      # 各周期起始价值
        self.current_value = self.strategy.broker.getvalue()
        
    def notify_cashvalue(self, cash, value):
        """当账户价值发生变化时调用"""
        self.current_value = value
        
    def _get_time_keys(self, dtime):
        """获取时间键值"""
        year = dtime.year
        month = dtime.month
        quarter = (month - 1) // 3 + 1
        
        return (
            f"{year:04d}-{month:02d}",         # 月度键
            f"{year:04d}-Q{quarter}",          # 季度键
            f"{year:04d}"                      # 年度键
        )
        
    def next(self):
        """每个交易日调用一次"""
        dtime = self.strategy.datas[0].datetime.date(0)
        # 只处理指定时间区间内的数据
        if not self.is_in_timeframe(dtime):
            return
            
        month_key, quarter_key, year_key = self._get_time_keys(dtime)
        
        # 记录各周期的起始价值
        if month_key not in self.start_values:
            self.start_values[month_key] = self.current_value
        if quarter_key not in self.start_values:
            self.start_values[quarter_key] = self.current_value
        if year_key not in self.start_values:
            self.start_values[year_key] = self.current_value
            
        # 计算收益率
        self.monthly_returns[month_key] = (self.current_value / self.start_values[month_key] - 1) * 100
        self.quarterly_returns[quarter_key] = (self.current_value / self.start_values[quarter_key] - 1) * 100
        self.yearly_returns[year_key] = (self.current_value / self.start_values[year_key] - 1) * 100
        
    def get_analysis(self):
        """返回分析结果"""
        return {
            'monthly': dict(self.monthly_returns),
            'quarterly': dict(self.quarterly_returns),
            'yearly': dict(self.yearly_returns)
        }
        
    def print_report(self):
        """打印详细报告"""
        print("\n=== 收益率报告 ===")
        
        # 转换为DataFrame便于分析
        monthly_df = pd.Series(self.monthly_returns).sort_index()
        quarterly_df = pd.Series(self.quarterly_returns).sort_index()
        yearly_df = pd.Series(self.yearly_returns).sort_index()
        
        # 月度统计
        print("\n月度收益率:")
        print(f"平均月收益率: {monthly_df.mean():.2f}%")
        print(f"最佳月收益率: {monthly_df.max():.2f}% ({monthly_df.idxmax()})")
        print(f"最差月收益率: {monthly_df.min():.2f}% ({monthly_df.idxmin()})")
        print("\n月度收益率详情:")
        for date, ret in monthly_df.items():
            print(f"{date}: {ret:.2f}%")
            
        # 季度统计
        print("\n季度收益率:")
        print(f"平均季度收益率: {quarterly_df.mean():.2f}%")
        print(f"最佳季度收益率: {quarterly_df.max():.2f}% ({quarterly_df.idxmax()})")
        print(f"最差季度收益率: {quarterly_df.min():.2f}% ({quarterly_df.idxmin()})")
        print("\n季度收益率详情:")
        for date, ret in quarterly_df.items():
            print(f"{date}: {ret:.2f}%")
            
        # 年度统计
        print("\n年度收益率:")
        print(f"平均年收益率: {yearly_df.mean():.2f}%")
        print(f"最佳年收益率: {yearly_df.max():.2f}% ({yearly_df.idxmax()})")
        print(f"最差年收益率: {yearly_df.min():.2f}% ({yearly_df.idxmin()})")
        print("\n年度收益率详情:")
        for date, ret in yearly_df.items():
            print(f"{date}: {ret:.2f}%") 