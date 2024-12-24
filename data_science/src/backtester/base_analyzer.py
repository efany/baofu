import backtrader as bt
from datetime import datetime

class BaseTimeFrameAnalyzer(bt.Analyzer):
    """基础时间区间分析器"""
    
    def __init__(self):
        super(BaseTimeFrameAnalyzer, self).__init__()
        # 获取策略中的时间区间设置
        self.start_date = (
            datetime.strptime(self.strategy.params.start_date, '%Y-%m-%d').date()
            if self.strategy.params.start_date else None
        )
        self.end_date = (
            datetime.strptime(self.strategy.params.end_date, '%Y-%m-%d').date()
            if self.strategy.params.end_date else None
        )
        
    def is_in_timeframe(self, date) -> bool:
        """
        检查日期是否在回测区间内
        
        Args:
            date: datetime.date 或 datetime.datetime 对象
        """
        # 如果是datetime对象，转换为date
        if isinstance(date, datetime):
            date = date.date()
            
        if self.start_date and date < self.start_date:
            return False
        if self.end_date and date > self.end_date:
            return False
        return True 