from datetime import date
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from loguru import logger

from database.db_forex_day_hist import DBForexDayHist
from database.mysql_database import MySQLDatabase
from .data_generator import DataGenerator, TableData, ParamConfig
from task_utils.data_utils import calculate_return_rate
from .data_calculator import DataCalculator

class ForexDataGenerator(DataGenerator):
    """外汇数据生成器"""
    
    def __init__(self, forex_code: str, mysql_db: MySQLDatabase, start_date: Optional[date] = None, end_date: Optional[date] = None):
        """
        初始化外汇数据生成器
        
        Args:
            forex_code: 外汇代码
            mysql_db: MySQL数据库连接
            start_date: 开始日期
            end_date: 结束日期
        """
        super().__init__(start_date, end_date)
        self.forex_code = forex_code
        self.db_forex_hist = DBForexDayHist(mysql_db)
        self.data = None
    
    def load(self) -> bool:
        """加载外汇数据"""
        start_date = self.params['start_date'] if 'start_date' in self.params else None
        end_date = self.params['end_date'] if 'end_date' in self.params else None

        self.data = self.db_forex_hist.get_extend_forex_hist_data(self.forex_code, start_date, end_date)
        
        if not self.data.empty:
            self.data['date'] = pd.to_datetime(self.data['date'])
            self.data = self.data.sort_values('date')
            logger.info(f"外汇数据加载完成: {self.forex_code}  {start_date}  {end_date} , 共{len(self.data)}条数据")
        else:
            logger.warning(f"未找到外汇数据: {self.forex_code}")
            return False
        
        return True

    def get_summary_data(self) -> List[Tuple[str, Any]]:
        """获取外汇摘要数据"""
        if self.data is None or self.data.empty:
            return []

        first_close, last_close, return_rate = calculate_return_rate(self.data, loc_name='close')
        
        # 获取起止日期
        start_date = self.data.iloc[0]['date'].strftime('%Y-%m-%d')
        end_date = self.data.iloc[-1]['date'].strftime('%Y-%m-%d')
        date_range = f"{start_date} ~ {end_date}"

        return [
            ('外汇代码', self.forex_code),
            ('统计区间', date_range),
            ('区间收益率', f"{return_rate:+.2f}% ({first_close:.4f} -> {last_close:.4f})")
        ]

    def get_chart_data(self, normalize: bool = False, chart_type: int = 0) -> List[Dict[str, Any]]:
        """
        获取外汇图表数据
        
        Args:
            normalize: 是否对数据进行归一化处理
            chart_type: 图表类型，0表示K线图，1表示折线图
            
        Returns:
            List[Dict[str, Any]]: 图表数据列表
        """
        if self.data is None or self.data.empty:
            return []

        dates = self.data['date'].tolist()
        chart_data = []

        if chart_type == 0:  # K线图
            chart_data.append({
                'x': dates,
                'open': self.data['open'].tolist(),
                'high': self.data['high'].tolist(),
                'low': self.data['low'].tolist(),
                'close': self.data['close'].tolist(),
                'type': 'candlestick',
                'name': self.forex_code,
                'visible': True
            })
        else:  # 折线图
            values = self.data['close']
            if normalize:
                values = self.normalize_series(values)
                
            chart_data.append({
                'x': dates,
                'y': values.tolist(),
                'type': 'line',
                'name': self.forex_code,
                'visible': True
            })

        return chart_data

    def get_extra_datas(self) -> List[TableData]:
        """获取外汇额外数据"""
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
        """获取用于计算相关系数的主要数据"""
        if self.data is None or self.data.empty:
            return pd.DataFrame()
        
        return pd.DataFrame({
            'date': self.data['date'],
            'value': self.data['close']
        })


def main():
    """主函数，用于测试"""
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )

    # 创建数据生成器实例
    generator = ForexDataGenerator('USDCNY', mysql_db)
    
    try:
        # 加载数据
        if not generator.load():
            print("数据加载失败")
            return

        # 测试获取摘要数据
        print("\n摘要数据：")
        for name, value in generator.get_summary_data():
            print(f"{name}: {value}")

        # 测试获取图表数据
        print("\n图表数据：")
        chart_data = generator.get_chart_data(normalize=True)
        print(f"图表数据条数: {len(chart_data)}")

        # 测试获取额外数据
        print("\n额外数据：")
        extra_datas = generator.get_extra_datas()
        for table in extra_datas:
            print(f"\n{table['name']}:")
            print(f"表头: {table['headers']}")
            print(f"数据行数: {len(table['data'])}")

    finally:
        mysql_db.close_connection()


if __name__ == "__main__":
    main() 