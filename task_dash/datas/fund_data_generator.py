from typing import Dict, List, Optional, Any, Tuple, Literal, TypedDict
from datetime import date
import pandas as pd
from .data_generator import DataGenerator, TableData, ChartDataType, ParamConfig
from database.db_funds import DBFunds
from database.db_funds_nav import DBFundsNav
from database.mysql_database import MySQLDatabase
from task_utils.data_utils import calculate_adjusted_nav, calculate_return_rate, calculate_max_drawdown
from loguru import logger

class FundDataGenerator(DataGenerator):
    """基金数据生成器"""
    
    def __init__(self, fund_code: str, mysql_db: MySQLDatabase, start_date: Optional[date] = None, end_date: Optional[date] = None):
        super().__init__(start_date, end_date)
        self.fund_code = fund_code
        self.db_funds_nav = DBFundsNav(mysql_db)
        self.db_funds = DBFunds(mysql_db)
        self.fund_info = None
        self.fund_nav = None
    
    def load(self) -> bool:
        """加载基金数据"""
        self.fund_info = self.db_funds.get_fund_info(self.fund_code)
        self.fund_nav = self.db_funds_nav.get_fund_nav(self.fund_code)

        if self.start_date and not self.fund_nav.empty:
            # 修正数据start_date到前一个有效日期
            prev_date = self.fund_nav[self.fund_nav['nav_date'] < self.start_date]['nav_date'].max()
            if pd.notna(prev_date):
                self.start_date = prev_date

        if self.start_date:
            self.fund_nav = self.fund_nav[self.fund_nav['nav_date'] >= self.start_date]
        if self.end_date:
            self.fund_nav = self.fund_nav[self.fund_nav['nav_date'] <= self.end_date]
        if not self.fund_nav.empty:
            self.fund_nav['nav_date'] = pd.to_datetime(self.fund_nav['nav_date'])

        calculate_adjusted_nav(self.fund_nav, self.start_date, self.end_date)

        logger.info(f"基金数据加载完成: {self.fund_code}  {self.start_date}  {self.end_date} , 共{len(self.fund_nav)}条数据")
        return True

    def get_params_config(self) -> List[ParamConfig]:
        """获取基金参数配置"""
        return []
    
    def update_params(self, params: Dict[str, Any]) -> bool:
        """更新基金参数"""
        return True
    
    def get_summary_data(self) -> List[Tuple[str, Any]]:
        """获取基金摘要数据"""
        if self.fund_info is None or self.fund_info.empty:
            return []

        first_nav, last_nav, return_rate = calculate_return_rate(self.fund_nav, loc_name='adjusted_nav')
        
        # 获取起止日期
        start_date = self.fund_nav.iloc[0]['nav_date'].strftime('%Y-%m-%d')
        end_date = self.fund_nav.iloc[-1]['nav_date'].strftime('%Y-%m-%d')
        date_range = f"{start_date} ~ {end_date}"
        
        return [
            ('基金代码', self.fund_code),
            ('基金名称', self.fund_info.iloc[0]['name']),
            ('基金公司', self.fund_info.iloc[0]['management']),
            ('统计区间', date_range),
            ('区间收益率', f"{return_rate:+.2f}% ({first_nav:.4f} -> {last_nav:.4f})")
        ]

    def get_chart_data(self, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取基金图表数据"""
        if self.fund_nav is None or self.fund_nav.empty:
            return []
        
        dates = self.fund_nav['nav_date'].tolist()
        
        # 准备数据，确保数据类型
        adjusted_nav = pd.to_numeric(self.fund_nav['adjusted_nav'], errors='coerce')
        accum_nav = pd.to_numeric(self.fund_nav['accum_nav'], errors='coerce')
        unit_nav = pd.to_numeric(self.fund_nav['unit_nav'], errors='coerce')
        
        # 如果需要归一化处理
        if normalize:
            adjusted_nav = self.normalize_series(adjusted_nav)
        
        return [
            {
                'x': dates,
                'y': adjusted_nav.tolist(),
                'type': 'line',
                'name': '再投资净值',
                'visible': True,
            },
            {
                'x': dates,
                'y': accum_nav.tolist(),
                'type': 'line',
                'name': '累计净值',
                'visible': 'legendonly',
            },
            {
                'x': dates,
                'y': unit_nav.tolist(),
                'type': 'line',
                'name': '单位净值',
                'visible': 'legendonly',
            },
            {
                'x': dates,
                'y': self.fund_nav['dividend'],
                'type': 'scatter',
                'name': '分红',
                'visible': 'legendonly',
                'mode': 'markers',
                'marker': {'size': 10, 'color': 'red'}
            }
        ]
    
    def get_extra_datas(self) -> List[TableData]:
        """获取基金额外数据"""
        if self.fund_nav is None or self.fund_nav.empty:
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
        if self.fund_nav is None or self.fund_nav.empty:
            return {
                'name': '基础指标',
                'headers': ['指标', '数值'],
                'data': []
            }
        
        return DataGenerator.calculate_basic_indicators(
            df=self.fund_nav,
            date_column='nav_date',
            value_column='adjusted_nav',
            value_format='.2f'
        )
        

    def _get_yearly_stats(self) -> TableData:
        """获取年度统计表格"""
        if self.fund_nav is None or self.fund_nav.empty:
            return {
                'name': '年度统计',
                'headers': ['年份', '收益率', '年化收益率', '最大回撤', '波动率'],
                'data': []
            }
        
        return DataGenerator.calculate_yearly_stats(
            df=self.fund_nav,
            date_column='nav_date',
            value_column='adjusted_nav'
        )

    def _get_quarterly_stats(self) -> TableData:
        """获取季度统计表格"""
        if self.fund_nav is None or self.fund_nav.empty:
            return {
                'name': '季度统计',
                'headers': ['季度', '收益率', '年化收益率', '最大回撤', '波动率'],
                'data': []
            }
        
        return DataGenerator.calculate_quarterly_stats(
            df=self.fund_nav,
            date_column='nav_date',
            value_column='adjusted_nav'
        )
    
    def get_extra_chart_data(self, data_type: ChartDataType, normalize: bool = False, **params) -> List[Dict[str, Any]]:
        """获取额外的图表数据"""
        if self.fund_nav is None or self.fund_nav.empty:
            return []
            
        if data_type in ['MA5', 'MA20', 'MA60', 'MA120']:
            period = int(data_type.replace('MA', ''))
            return self._get_ma_data(period, 'adjusted_nav', normalize)
        elif data_type == 'drawdown':
            return self._get_drawdown_data('adjusted_nav', normalize)
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    def _get_ma_data(self, period: int, value_column: str, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取移动平均线数据"""
        return DataGenerator.calculate_ma_data(
            df=self.fund_nav,
            date_column='nav_date',
            value_column=value_column,
            period=period,
            normalize=normalize
        )

    def _get_drawdown_data(self, value_column: str, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取回撤数据"""
        return DataGenerator.calculate_drawdown_chart_data(
            df=self.fund_nav,
            date_column='nav_date',
            value_column=value_column,
            normalize=normalize
        )

    def get_value_data(self) -> pd.DataFrame:
        """获取基金净值数据"""
        if self.fund_nav is None or self.fund_nav.empty:
            return pd.DataFrame()
        
        return pd.DataFrame({
            'date': self.fund_nav['nav_date'],
            'value': self.fund_nav['adjusted_nav']
        })