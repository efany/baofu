from typing import Dict, List, Optional, Any, Tuple, Literal, TypedDict
from datetime import date
import pandas as pd
from .data_generator import DataGenerator, TableData, ChartDataType
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
        self._load_data()
    
    def _load_data(self):
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
        # 计算再投资收益率
        first_adjusted_nav = self.fund_nav.iloc[0]['adjusted_nav']
        last_adjusted_nav = self.fund_nav.iloc[-1]['adjusted_nav']
        adjusted_return_rate = (last_adjusted_nav - first_adjusted_nav) / first_adjusted_nav * 100
        
        # 计算年化收益率
        days = (self.fund_nav.iloc[-1]['nav_date'] - self.fund_nav.iloc[0]['nav_date']).days
        annualized_return = ((1 + adjusted_return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
        
        # 计算净值收益率
        first_unit_nav = self.fund_nav.iloc[0]['unit_nav']
        last_unit_nav = self.fund_nav.iloc[-1]['unit_nav']
        unit_return_rate = (last_unit_nav - first_unit_nav) / first_unit_nav * 100
        
        # 计算累计净值收益率
        first_accum_nav = self.fund_nav.iloc[0]['accum_nav']
        last_accum_nav = self.fund_nav.iloc[-1]['accum_nav']
        accum_return_rate = (last_accum_nav - first_accum_nav) / first_accum_nav * 100
        
        # 计算风险指标
        returns = self.fund_nav['adjusted_nav'].pct_change()
        volatility = returns.std() * (252 ** 0.5) * 100  # 年化波动率
        
        return {
            'name': '基础指标',
            'headers': ['指标', '数值'],
            'data': [
                ['投资收益率', f'{adjusted_return_rate:+.2f}% ({first_adjusted_nav:.4f} -> {last_adjusted_nav:.4f})'],
                ['年化收益率', f'{annualized_return:+.2f}%'],
                ['投资最大回撤', self._get_max_drawdown()],
                ['年化波动率', f'{volatility:.2f}%'],
                ['净值收益率', f'{unit_return_rate:+.2f}% ({first_unit_nav:.4f} -> {last_unit_nav:.4f})'],
                ['累计净值收益率', f'{accum_return_rate:+.2f}% ({first_accum_nav:.4f} -> {last_accum_nav:.4f})'],
            ]
        }

    def _get_yearly_stats(self) -> TableData:
        """获取年度统计表格"""
        # 添加年份列
        df = self.fund_nav.copy()
        df['year'] = df['nav_date'].dt.year
        
        yearly_stats = []
        for year in sorted(df['year'].unique(), reverse=True):
            year_data = df[df['year'] == year]
            
            # 获取年度起止日期
            start_date = year_data.iloc[0]['nav_date'].strftime('%Y-%m-%d')
            end_date = year_data.iloc[-1]['nav_date'].strftime('%Y-%m-%d')
            
            # 计算年度收益率
            start_nav = year_data.iloc[0]['adjusted_nav']
            end_nav = year_data.iloc[-1]['adjusted_nav']
            return_rate = (end_nav - start_nav) / start_nav * 100
            
            # 计算年化收益率
            days = (year_data.iloc[-1]['nav_date'] - year_data.iloc[0]['nav_date']).days
            annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
            
            # 计算年度最大回撤
            drawdown_list = calculate_max_drawdown(
                year_data['nav_date'],
                year_data['adjusted_nav']
            )
            max_drawdown = f"{drawdown_list[0]['value']*100:.2f}%" if drawdown_list else 'N/A'
            
            # 计算年度波动率
            returns = year_data['adjusted_nav'].pct_change()
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
        df = self.fund_nav.copy()
        df['year'] = df['nav_date'].dt.year
        df['quarter'] = df['nav_date'].dt.quarter
        
        quarterly_stats = []
        for year in sorted(df['year'].unique(), reverse=True):
            year_data = df[df['year'] == year]
            for quarter in sorted(year_data['quarter'].unique(), reverse=True):
                quarter_data = year_data[year_data['quarter'] == quarter]
                
                # 获取季度起止日期
                start_date = quarter_data.iloc[0]['nav_date'].strftime('%Y-%m-%d')
                end_date = quarter_data.iloc[-1]['nav_date'].strftime('%Y-%m-%d')
                
                # 计算季度收益率
                start_nav = quarter_data.iloc[0]['adjusted_nav']
                end_nav = quarter_data.iloc[-1]['adjusted_nav']
                return_rate = (end_nav - start_nav) / start_nav * 100
                
                # 计算年化收益率
                days = (quarter_data.iloc[-1]['nav_date'] - quarter_data.iloc[0]['nav_date']).days
                annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
                
                # 计算季度最大回撤
                drawdown_list = calculate_max_drawdown(
                    quarter_data['nav_date'],
                    quarter_data['adjusted_nav']
                )
                max_drawdown = f"{drawdown_list[0]['value']*100:.2f}%" if drawdown_list else 'N/A'
                
                # 计算季度波动率
                returns = quarter_data['adjusted_nav'].pct_change()
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
            self.fund_nav['nav_date'],
            self.fund_nav['adjusted_nav']
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
            
            return f"{max_dd:.2f}% ({start_date}~{end_date}{recovery_info}, {start_value:.4f}->{end_value:.4f})"
        
        return 'N/A'
    
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
        dates = self.fund_nav['nav_date'].tolist()
        ma_data = []
        
        # 确保数据类型
        values = pd.to_numeric(self.fund_nav[value_column], errors='coerce')
        if normalize:
            values = self.normalize_series(values)
            
        # 计算移动平均线，并确保数据类型
        ma = values.rolling(window=period).mean().astype('float64')
        ma_data.append({
            'x': dates,
            'y': ma.tolist(),
            'type': 'line',
            'name': f'MA{period}',
            'visible': True,
            'line': {'dash': 'dot'}
        })
        
        return ma_data

    def _get_drawdown_data(self, value_column: str, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取回撤数据"""
        # 确保数据类型
        values = pd.to_numeric(self.fund_nav[value_column], errors='coerce')
        if normalize:
            values = self.normalize_series(values)
            
        drawdown_list = calculate_max_drawdown(
            self.fund_nav['nav_date'],
            values
        )
        
        data = []
        # 绘制回撤区域
        for i in range(len(drawdown_list)):
            if pd.notna(drawdown_list[i]):
                dd = drawdown_list[i]
                drawdown_days = (dd['end_date'] - dd['start_date']).days
                recovery_days = (dd['recovery_date'] - dd['end_date']).days if dd.get('recovery_date') else None
                
                text = f'回撤: {dd["value"]*100:.4f}%({drawdown_days} days)' 
                if recovery_days:
                    text = f'{text}，修复：{recovery_days} days'
                    
                data.append({
                    'type': 'scatter',
                    'x': [dd['start_date'], dd['end_date'], dd['end_date'], dd['start_date'], dd['start_date']],
                    'y': [dd['start_value'], dd['start_value'], dd['end_value'], dd['end_value'], dd['start_value']],
                    'fill': 'toself',
                    'fillcolor': 'rgba(255, 0, 0, 0.2)',
                    'line': {'width': 0},
                    'mode': 'lines+text',
                    'text': [text],
                    'textposition': 'top right',
                    'textfont': {'size': 12, 'color': 'red'},
                    'name': f'TOP{i+1} 回撤',
                    'showlegend': True
                })
                
                if recovery_days:
                    data.append({
                        'type': 'scatter',
                        'x': [dd['end_date'], dd['recovery_date'], dd['recovery_date'], dd['end_date'], dd['end_date']],
                        'y': [dd['end_value'], dd['end_value'], dd['start_value'], dd['start_value'], dd['end_value']],
                        'fill': 'toself',
                        'fillcolor': 'rgba(0, 255, 0, 0.2)',
                        'line': {'width': 0},
                        'mode': 'lines+text',
                        'textfont': {'size': 12, 'color': 'red'},
                        'name': f'TOP{i+1} 回撤修复',
                        'showlegend': True
                    })
        return data

    def get_value_data(self) -> pd.DataFrame:
        """获取基金净值数据"""
        if self.fund_nav is None or self.fund_nav.empty:
            return pd.DataFrame()
        
        return pd.DataFrame({
            'date': self.fund_nav['nav_date'],
            'value': self.fund_nav['adjusted_nav']
        })