from datetime import datetime, timedelta
import pandas as pd
from .base_analyzer import BaseTimeFrameAnalyzer

class RecentReturns(BaseTimeFrameAnalyzer):
    def __init__(self):
        super(RecentReturns, self).__init__()
        self.values = {}  # {datetime: value}
        
    def notify_cashvalue(self, cash, value):
        """当账户价值发生变化时调用"""
        dtime = self.strategy.datas[0].datetime.date(0)
        # 只记录指定时间区间内的数据
        if self.is_in_timeframe(dtime):
            self.values[dtime] = value
        
    def _calculate_return(self, current_date: datetime.date, days: int = None) -> float:
        """
        计算指定天数的收益率
        
        Args:
            current_date: 当前日期
            days: 往前推的天数，如果为None则计算成立以来收益率
        
        Returns:
            收益率(%)，如果没有足够的历史数据则返回None
        """
        available_dates = sorted(self.values.keys())
        if not available_dates:
            return None
            
        if days is None:
            # 计算成立以来收益率
            start_value = self.values[min(available_dates)]
            end_value = self.values[current_date]
            return (end_value / start_value - 1) * 100
            
        start_date = current_date - timedelta(days=days)
        
        # 如果历史数据不足，返回None
        if start_date < min(available_dates):
            return None
            
        # 找到最接近start_date的日期
        start_value_date = min(available_dates, key=lambda x: abs((x - start_date).days))
        start_value = self.values[start_value_date]
        end_value = self.values[current_date]
        
        return (end_value / start_value - 1) * 100
        
    def get_analysis(self):
        """返回分析结果"""
        if not self.values:
            return {}
            
        current_date = max(self.values.keys())
        start_date = min(self.values.keys())
        days_since_inception = (current_date - start_date).days
        
        # 计算各期间收益率
        returns = {
            '7d': self._calculate_return(current_date, 7),
            '1m': self._calculate_return(current_date, 30),
            '3m': self._calculate_return(current_date, 90),
            '1y': self._calculate_return(current_date, 365),
            '3y': self._calculate_return(current_date, 365 * 3),
            '5y': self._calculate_return(current_date, 365 * 5),
            '10y': self._calculate_return(current_date, 365 * 10),
            'inception': self._calculate_return(current_date)  # 成立以来收益率
        }
        
        # 添加成立以来天数
        returns['days_since_inception'] = days_since_inception
        
        return returns
        
    def print_report(self):
        """打印详细报告"""
        analysis = self.get_analysis()
        if not analysis:
            print("没有足够的数据进行分析")
            return
            
        print("\n=== 近期收益率报告 ===")
        
        # 定义显示顺序和标签
        periods = [
            ('7d', '近7天'),
            ('1m', '近1个月'),
            ('3m', '近1季度'),
            ('1y', '近1年'),
            ('3y', '近3年'),
            ('5y', '近5年'),
            ('10y', '近10年'),
            ('inception', '成立以来')
        ]
        
        # 打印成立天数
        days = analysis.get('days_since_inception', 0)
        years = days / 365.0
        print(f"成立天数: {days}天 (约{years:.1f}年)")
        
        # 打印收益率
        for key, label in periods:
            value = analysis.get(key)
            if value is not None:
                print(f"{label}收益率: {value:.2f}%")
            else:
                print(f"{label}收益率: 数据不足")
                
        # 计算年化收益率（仅对超过1年的周期）
        print("\n=== 年化收益率 ===")
        annualized = {
            '1y': analysis.get('1y'),
            '3y': analysis.get('3y') / 3 if analysis.get('3y') is not None else None,
            '5y': analysis.get('5y') / 5 if analysis.get('5y') is not None else None,
            '10y': analysis.get('10y') / 10 if analysis.get('10y') is not None else None,
            'inception': analysis.get('inception') * 365.0 / days if analysis.get('inception') is not None and days > 0 else None
        }
        
        periods_annual = [
            ('1y', '近1年'),
            ('3y', '近3年'),
            ('5y', '近5年'),
            ('10y', '近10年'),
            ('inception', '成立以来')
        ]
        
        for key, label in periods_annual:
            value = annualized.get(key)
            if value is not None:
                print(f"{label}年化收益率: {value:.2f}%")
            else:
                print(f"{label}年化收益率: 数据不足") 