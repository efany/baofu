from datetime import date
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from loguru import logger

from database.db_bond_rate import DBBondRate
from database.mysql_database import MySQLDatabase
from .data_generator import DataGenerator, TableData, ParamConfig
from task_utils.data_utils import calculate_return_rate, calculate_max_drawdown

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
        self.bond_data = None
    
    def load(self) -> bool:
        """加载债券利率数据"""
        self.bond_data = self.db_bond_rate.get_bond_rate(self.bond_type, self.start_date, self.end_date)
        if not self.bond_data.empty:
            self.bond_data['date'] = pd.to_datetime(self.bond_data['date'])
            self.bond_data = self.bond_data.sort_values('date')
            logger.info(f"债券利率数据加载完成: {self.bond_type}  {self.start_date}  {self.end_date} , 共{len(self.bond_data)}条数据")
        else:
            logger.warning(f"未找到债券利率数据: {self.bond_type}")
            return False
        
        return True

    def get_params_config(self) -> List[ParamConfig]:
        """获取债券利率参数配置"""
        return []
    
    def update_params(self, params: Dict[str, Any]) -> bool:
        """更新债券利率参数"""
        return True

    def get_summary_data(self) -> List[Tuple[str, Any]]:
        """获取债券利率摘要数据"""
        if self.bond_data is None or self.bond_data.empty:
            return []

        first_rate, last_rate, return_rate = calculate_return_rate(self.bond_data, loc_name='rate')
        
        # 获取起止日期
        start_date = self.bond_data.iloc[0]['date'].strftime('%Y-%m-%d')
        end_date = self.bond_data.iloc[-1]['date'].strftime('%Y-%m-%d')
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
        if self.bond_data is None or self.bond_data.empty:
            return []

        dates = self.bond_data['date'].tolist()
        rates = self.bond_data['rate'].tolist()
        
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
        if self.bond_data is None or self.bond_data.empty:
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
        # 计算收益率
        first_rate = self.bond_data.iloc[0]['rate']
        last_rate = self.bond_data.iloc[-1]['rate']
        return_rate = (last_rate - first_rate) / first_rate * 100
        
        # 计算年化收益率
        days = (self.bond_data.iloc[-1]['date'] - self.bond_data.iloc[0]['date']).days
        annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
        
        # 计算风险指标
        returns = self.bond_data['rate'].pct_change(fill_method=None)
        volatility = returns.std() * (252 ** 0.5) * 100  # 年化波动率
        
        return {
            'name': '基础指标',
            'headers': ['指标', '数值'],
            'data': [
                ['投资收益率', f'{return_rate:+.2f}% ({first_rate:.2f}% -> {last_rate:.2f}%)'],
                ['年化收益率', f'{annualized_return:+.2f}%'],
                ['投资最大回撤', self._get_max_drawdown()],
                ['年化波动率', f'{volatility:.2f}%'],
            ]
        }

    def _get_yearly_stats(self) -> TableData:
        """获取年度统计表格"""
        # 添加年份列
        df = self.bond_data.copy()
        df['year'] = df['date'].dt.year   
        
        yearly_stats = []
        for year in sorted(df['year'].unique(), reverse=True):
            year_data = df[df['year'] == year]
            
            # 获取年度起止日期
            start_date = year_data.iloc[0]['date'].strftime('%Y-%m-%d')
            end_date = year_data.iloc[-1]['date'].strftime('%Y-%m-%d')
            
            # 计算年度收益率
            start_rate = year_data.iloc[0]['rate']
            end_rate = year_data.iloc[-1]['rate']
            return_rate = (end_rate - start_rate) / start_rate * 100
            
            # 计算年化收益率
            days = (year_data.iloc[-1]['date'] - year_data.iloc[0]['date']).days
            annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
            
            # 计算年度最大回撤
            drawdown_list = calculate_max_drawdown(
                year_data['date'],
                year_data['rate']
            )
            max_drawdown = f"{drawdown_list[0]['value']*100:.2f}%" if drawdown_list else 'N/A'
            
            # 计算年度波动率
            returns = year_data['rate'].pct_change(fill_method=None)
            volatility = returns.std() * (252 ** 0.5) * 100
            
            yearly_stats.append([
                f"{year} ({start_date}~{end_date})",
                f'{return_rate:+.2f}%',
                f'{annualized_return:+.2f}%',
                max_drawdown,
                f'{volatility:.2f}%'
            ])
        
        return {
            'name': '年度统计',
            'headers': ['年份', '收益率', '年化收益率', '最大回撤', '波动率'],
            'data': yearly_stats
        }

    def _get_quarterly_stats(self) -> TableData:
        """获取季度统计表格"""
        # 添加季度列
        df = self.bond_data.copy()
        df['year'] = df['date'].dt.year
        df['quarter'] = df['date'].dt.quarter
        
        quarterly_stats = []
        for year in sorted(df['year'].unique(), reverse=True):
            year_data = df[df['year'] == year]
            for quarter in sorted(year_data['quarter'].unique(), reverse=True):
                quarter_data = year_data[year_data['quarter'] == quarter]
                
                # 获取季度起止日期
                start_date = quarter_data.iloc[0]['date'].strftime('%Y-%m-%d')
                end_date = quarter_data.iloc[-1]['date'].strftime('%Y-%m-%d')
                
                # 计算季度收益率
                start_rate = quarter_data.iloc[0]['rate']
                end_rate = quarter_data.iloc[-1]['rate']
                return_rate = (end_rate - start_rate) / start_rate * 100
                
                # 计算年化收益率
                days = (quarter_data.iloc[-1]['date'] - quarter_data.iloc[0]['date']).days
                annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
                
                # 计算季度最大回撤
                drawdown_list = calculate_max_drawdown(
                    quarter_data['date'],
                    quarter_data['rate']
                )
                max_drawdown = f"{drawdown_list[0]['value']*100:.2f}%" if drawdown_list else 'N/A'
                
                # 计算季度波动率
                returns = quarter_data['rate'].pct_change(fill_method=None)
                volatility = returns.std() * (252 ** 0.5) * 100
                
                quarterly_stats.append([
                    f"{year}Q{quarter} ({start_date}~{end_date})",
                    f'{return_rate:+.2f}%',
                    f'{annualized_return:+.2f}%',
                    max_drawdown,
                    f'{volatility:.2f}%'
                ])
        
        return {
            'name': '季度统计',
            'headers': ['季度', '收益率', '年化收益率', '最大回撤', '波动率'],
            'data': quarterly_stats
        }

    def _get_max_drawdown(self) -> str:
        """计算最大回撤"""
        drawdown_list = calculate_max_drawdown(
            self.bond_data['date'],
            self.bond_data['rate']
        )
        
        if drawdown_list and len(drawdown_list) > 0:
            dd = drawdown_list[0]
            max_dd = dd['value'] * 100
            start_date = dd['start_date'].strftime('%Y-%m-%d')
            end_date = dd['end_date'].strftime('%Y-%m-%d')
            start_value = dd['start_value']
            end_value = dd['end_value']
            
            # 如果有恢复日期，添加恢复信息
            recovery_info = ""
            if dd.get('recovery_date'):
                days_to_recover = (dd['recovery_date'] - dd['end_date']).days
                recovery_info = f", 恢复天数: {days_to_recover}天"
            
            return f"{max_dd:.2f}% ({start_date}~{end_date}{recovery_info}, {start_value:.2f}%->{end_value:.2f}%)"
        
        return 'N/A'

    def get_value_data(self) -> pd.DataFrame:
        """获取用于计算相关系数的主要数据"""
        if self.bond_data is None or self.bond_data.empty:
            return pd.DataFrame()
        
        return pd.DataFrame({
            'date': self.bond_data['date'],
            'value': self.bond_data['rate']
        })

    def get_extra_chart_data(self, data_type: str, normalize: bool = False, **params) -> List[Dict[str, Any]]:
        """获取额外的图表数据"""
        if self.bond_data is None or self.bond_data.empty:
            return []
            
        # 目前只支持基础利率数据
        return self.get_chart_data(normalize=normalize) 