from datetime import date
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from loguru import logger

from database.db_stocks_day_hist import DBStocksDayHist
from database.db_stocks import DBStocks
from database.mysql_database import MySQLDatabase
from .data_generator import DataGenerator, TableData, ParamConfig
from task_utils.data_utils import calculate_adjusted_nav, calculate_return_rate
from .data_calculator import DataCalculator

class StockDataGenerator(DataGenerator):
    """股票数据生成器"""
    
    def __init__(self, stock_code: str, mysql_db: MySQLDatabase, start_date: Optional[date] = None, end_date: Optional[date] = None):
        """
        初始化股票数据生成器
        
        Args:
            stock_code: 股票代码
            mysql_db: MySQL数据库连接
            start_date: 开始日期
            end_date: 结束日期
        """
        super().__init__(start_date, end_date)
        self.stock_code = stock_code
        self.db_stocks = DBStocks(mysql_db)
        self.db_stocks_day_hist = DBStocksDayHist(mysql_db)
        self.data = None
    
    def load(self) -> bool:
        """加载股票数据"""
        start_date = self.params['start_date'] if 'start_date' in self.params else None
        end_date = self.params['end_date'] if 'end_date' in self.params else None

        self.stock_info = self.db_stocks.get_stock_info(self.stock_code)

        # 先获取原始日期范围的数据
        original_data = self.db_stocks_day_hist.get_stock_hist_data(self.stock_code)
        if not original_data.empty:
            original_data['date'] = pd.to_datetime(original_data['date'])
            original_data = original_data.sort_values('date')

            # 如果start_date有效，尝试找到前一个有效日期
            if start_date is not None:
                # 将start_date转换为datetime并与数据比较
                start_datetime = pd.to_datetime(start_date)
                
                # 找到所有早于start_date的日期
                earlier_dates = original_data[original_data['date'] < start_datetime]
                
                if not earlier_dates.empty:
                    # 找到最接近start_date的前一个日期
                    previous_date = earlier_dates['date'].max()
                    logger.info(f"找到前一个有效数据日期: {start_date} -> {previous_date.date()}")
                    start_date = previous_date.date()
                else:
                    logger.warning(f"没有找到早于 {start_date} 的 {self.stock_code} 有效数据日期")
        else:    
            logger.warning(f"没有找到股票数据: {self.stock_code}")
            return False

        # 使用调整后的日期范围获取数据
        self.data = self.db_stocks_day_hist.get_stock_hist_data(self.stock_code, start_date, end_date)
        if not self.data.empty:
            self.data['date'] = pd.to_datetime(self.data['date'])
            self.data = self.data.sort_values('date')
            logger.info(f"股票数据加载完成: {self.stock_code}  {start_date}  {end_date} , 共{len(self.data)}条数据")
        else:
            logger.warning(f"未找到股票数据: {self.stock_code}")
            return False

        return True

    def get_summary_data(self) -> List[Tuple[str, Any]]:
        """获取股票摘要数据"""
        if self.data is None or self.data.empty:
            return []

        first_close, last_close, return_rate = calculate_return_rate(self.data, loc_name='close')

        # 获取起止日期
        start_date = self.data.iloc[0]['date'].strftime('%Y-%m-%d')
        end_date = self.data.iloc[-1]['date'].strftime('%Y-%m-%d')
        date_range = f"{start_date} ~ {end_date}"
        
        return [
            ('股票代码', self.stock_info['symbol']),
            ('股票名称', self.stock_info['name']),
            ('统计区间', date_range),
            ('区间收益率', f"{return_rate:+.2f}% ({first_close:.2f} -> {last_close:.2f})")
        ]

    def get_chart_data(self, normalize: bool = False, chart_type: int = 0) -> List[Dict[str, Any]]:
        """获取股票图表数据，显示为K线图"""
        if self.data is None or self.data.empty:
            return []
        
        # 准备数据
        open_price = self.data['open']
        high_price = self.data['high']
        low_price = self.data['low']
        close_price = self.data['close']
        
        if normalize:
            open_price = self.normalize_series(open_price)
            high_price = self.normalize_series(high_price)
            low_price = self.normalize_series(low_price)
            close_price = self.normalize_series(close_price)
        
        # 将日期转换为中文格式
        dates = self.data['date'].tolist()
        if chart_type == 0:
            return [
                {
                    'x': dates,
                    'open': open_price.tolist(),
                    'high': high_price.tolist(),
                    'low': low_price.tolist(),
                    'close': close_price.tolist(),
                    'type': 'candlestick',
                    'name': 'K线图',
                    'increasing': {'line': {'color': 'red'}},  # 上涨为红色
                    'decreasing': {'line': {'color': 'green'}},  # 下跌为绿色
                }
            ]
        elif chart_type == 1:
            return [
                {
                    'x': dates,
                    'y': close_price.tolist(),
                    'type': 'line',
                    'name': '收盘价',
                    'visible': True,
                }
            ]
    
    def get_extra_datas(self) -> List[TableData]:
        """获取股票额外数据"""
        if self.data is None or self.data.empty:
            return []
        
        # 基础指标表格
        basic_table = self._get_basic_indicators()
        
        # 年度统计表格
        yearly_table = self._get_yearly_stats()
        
        # 季度统计表格
        quarterly_table = self._get_quarterly_stats()
        
        return [basic_table, yearly_table, quarterly_table]

    def _get_basic_indicators(self) -> TableData:
        """获取基础指标表格"""
        return super()._get_basic_indicators('date', 'close', '.2f')

    def _get_yearly_stats(self) -> TableData:
        """获取年度统计表格"""
        return super()._get_yearly_stats('date', 'close')

    def _get_quarterly_stats(self) -> TableData:
        """获取季度统计表格"""
        return super()._get_quarterly_stats('date', 'close')

    def get_extra_chart_data(self, data_type: str, normalize: bool = False, **params) -> List[Dict[str, Any]]:
        """获取额外的图表数据"""
        if self.data is None or self.data.empty:
            return []

        if data_type in ['MA5', 'MA20', 'MA60', 'MA120']:
            period = int(data_type.replace('MA', ''))
            return self._get_ma_data(period, 'date', 'close', normalize)
        elif data_type == 'drawdown':
            return self._get_drawdown_data('date', 'close', normalize)
        else:
            raise ValueError(f"Unknown data type: {data_type}")


    def get_value_data(self) -> pd.DataFrame:
        """获取股票收盘价数据"""
        if self.data is None or self.data.empty:
            return pd.DataFrame()
        
        return pd.DataFrame({
            'date': self.data['date'],
            'value': self.data['close']
        })