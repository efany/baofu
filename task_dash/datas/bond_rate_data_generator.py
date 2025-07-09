from datetime import date
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from loguru import logger

from database.db_bond_rate import DBBondRate
from database.mysql_database import MySQLDatabase
from .data_generator import DataGenerator, TableData, ParamConfig
from task_utils.data_utils import calculate_return_rate, calculate_max_drawdown
from .data_calculator import DataCalculator

class BondRateDataGenerator(DataGenerator):
    """债券利率数据生成器"""
    
    def __init__(self, bond_type: str, mysql_db: MySQLDatabase, start_date: Optional[date] = None, end_date: Optional[date] = None):
        """
        初始化债券利率数据生成器
        
        Args:
            bond_type: 债券类型（如：CN_10Y, US_10Y等）
            mysql_db: MySQL数据库连接
            start_date: 开始日期
            end_date: 结束日期
        """
        super().__init__(start_date, end_date)
        self.bond_type = bond_type
        self.db_bond_rate = DBBondRate(mysql_db)
        self.data = None
    
    def load(self) -> bool:
        """加载债券利率数据"""
        start_date = self.params['start_date'] if 'start_date' in self.params else None
        end_date = self.params['end_date'] if 'end_date' in self.params else None

        self.data = self.db_bond_rate.get_bond_rate(self.bond_type, start_date, end_date)
        if not self.data.empty:
            self.data['date'] = pd.to_datetime(self.data['date'])
            self.data = self.data.sort_values('date')
            logger.info(f"债券利率数据加载完成: {self.bond_type}  {start_date}  {end_date} , 共{len(self.data)}条数据")
        else:
            logger.warning(f"未找到债券利率数据: {self.bond_type}")
            return False
        
        return True

    def get_summary_data(self) -> List[Tuple[str, Any]]:
        """获取债券利率摘要数据"""
        if self.data is None or self.data.empty:
            return []

        first_rate, last_rate, return_rate = calculate_return_rate(self.data, loc_name='rate')
        
        # 获取起止日期
        start_date = self.data.iloc[0]['date'].strftime('%Y-%m-%d')
        end_date = self.data.iloc[-1]['date'].strftime('%Y-%m-%d')
        date_range = f"{start_date} ~ {end_date}"
        
        return [
            ('债券类型', self.bond_type),
            ('统计区间', date_range),
            ('区间收益率', f"{return_rate:+.2f}% ({first_rate:.2f}% -> {last_rate:.2f}%)")
        ]

    def get_chart_data(self, normalize: bool = False, chart_type: int = 0) -> List[Dict[str, Any]]:
        """
        获取债券利率图表数据
        
        Args:
            normalize: 是否对数据进行归一化处理
            chart_type: 图表类型，0表示折线图，1表示柱状图
            
        Returns:
            List[Dict[str, Any]]: 图表数据列表
        """
        if self.data is None or self.data.empty:
            return []

        dates = self.data['date'].tolist()
        rates = self.data['rate'].tolist()
        
        if normalize:
            rates = self.normalize_series(pd.Series(rates)).tolist()
        
        chart_data = []
        if chart_type == 0:
            chart_data.append({
                'x': dates,
                'y': rates,
                'type': 'line',
                'name': self.bond_type,
                'visible': True
            })
        return chart_data
        

    def get_extra_datas(self) -> List[TableData]:
        """获取债券利率额外数据"""
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
        if self.data is None or self.data.empty:
            return {
                'name': '基础指标',
                'headers': ['指标', '数值'],
                'data': []
            }
        
        return DataCalculator.calculate_basic_indicators(
            df=self.data,
            date_column='date',
            value_column='rate',
        )

    def _get_yearly_stats(self) -> TableData:
        if self.data is None or self.data.empty:
            return {
                'name': '年度统计',
                'headers': ['年份', '收益率', '年化收益率', '最大回撤', '波动率'],
                'data': []
            }
        return DataCalculator.calculate_yearly_stats(
            df=self.data,
            date_column='date',
            value_column='rate',
        )

    def _get_quarterly_stats(self) -> TableData:
        if self.data is None or self.data.empty:
            return {
                'name': '季度统计',
                'headers': ['季度', '收益率', '年化收益率', '最大回撤', '波动率'],
                'data': []
            }
        return DataCalculator.calculate_quarterly_stats(
            df=self.data,
            date_column='date',
            value_column='rate',
        )

    def get_extra_chart_data(self, data_type: str, normalize: bool = False, **params) -> List[Dict[str, Any]]:
        """获取额外的图表数据"""
        if self.data is None or self.data.empty:
            return []
            
        if data_type in ['MA5', 'MA20', 'MA60', 'MA120']:
            period = int(data_type.replace('MA', ''))
            return self._get_ma_data(period, 'rate', normalize)
        elif data_type == 'drawdown':
            return self._get_drawdown_chart_data(normalize)
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    def _get_ma_data(self, period: int, value_column: str, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取移动平均线数据"""
        return DataCalculator.calculate_ma_data(
            df=self.data,
            date_column='date',
            value_column=value_column,
            period=period,
            normalize=normalize
        )

    def _get_drawdown_chart_data(self, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取回撤图表数据"""
        return DataCalculator.calculate_drawdown_chart_data(
            df=self.data,
            date_column='date',
            value_column='rate',
            normalize=normalize
        )

    def get_value_data(self) -> pd.DataFrame:
        """获取用于计算相关系数的主要数据"""
        if self.data is None or self.data.empty:
            return pd.DataFrame()
        
        return pd.DataFrame({
            'date': self.data['date'],
            'value': self.data['rate']
        })