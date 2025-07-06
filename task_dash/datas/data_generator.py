from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple, Literal, TypedDict
from datetime import date
import pandas as pd
import numpy as np
from task_utils.data_utils import calculate_max_drawdown

# 定义支持的数据类型
ChartDataType = Literal['MA5', 'MA20', 'MA60', 'MA120', 'drawdown']

# 定义参数类型
class ParamConfig(TypedDict):
    """参数配置类型定义"""
    name: str  # 参数名称
    type: str  # 参数类型 (number, text, select, etc.)
    label: str  # 参数标签
    value: Any  # 当前值
    options: Optional[List[Dict[str, Any]]]  # 选项列表（用于select类型）
    min: Optional[float]  # 最小值（用于number类型）
    max: Optional[float]  # 最大值（用于number类型）
    step: Optional[float]  # 步长（用于number类型）

class TableData(TypedDict):
    """表格数据类型定义"""
    name: str  # 表格名称
    headers: List[str]  # 表头
    data: List[List[Any]]  # 表格数据

class DataGenerator(ABC):
    """数据生成器基类，负责生成各类型数据用于页面展示"""

    def __init__(self, start_date: Optional[date] = None, end_date: Optional[date] = None):
        self.start_date = start_date
        self.end_date = end_date
        self.is_loaded = False  # 添加数据加载状态标记

    def calculate_basic_indicators(
        df: pd.DataFrame,
        date_column: str,
        value_column: str,
        currency_symbol: str = '',
        value_format: str = '.2f',
        extra_indicators: Optional[List[Tuple[str, Any]]] = None
    ) -> TableData:

        if df is None or df.empty:
            return {
                'name': '基础指标',
                'headers': ['指标', '数值'],
                'data': []
            }

        # 确保日期列是datetime类型
        df = df.copy()
        df[date_column] = pd.to_datetime(df[date_column])
        
        # 计算基础收益率
        first_value = df.iloc[0][value_column]
        last_value = df.iloc[-1][value_column]
        return_rate = (last_value - first_value) / first_value * 100
        
        # 计算年化收益率
        days = (df.iloc[-1][date_column] - df.iloc[0][date_column]).days
        annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
        
        # 计算风险指标
        returns = df[value_column].pct_change()
        volatility = returns.std() * (252 ** 0.5) * 100  # 年化波动率
        
        # 格式化显示值
        value_format_str = f"{{:{value_format}}}"
        if currency_symbol:
            value_str = f"{return_rate:+.2f}% ({currency_symbol}{value_format_str} -> {currency_symbol}{value_format_str})".format(first_value, last_value)
        else:
            value_str = f"{return_rate:+.2f}% ({value_format_str} -> {value_format_str})".format(first_value, last_value)
        
        # 基础指标列表
        indicators = [
            ['投资收益率', value_str],
            ['年化收益率', f'{annualized_return:+.2f}%'],
            ['投资最大回撤', DataGenerator.get_max_drawdown(df, date_column, value_column)],
            ['年化波动率', f'{volatility:.2f}%']
        ]
        
        # 添加额外指标
        if extra_indicators:
            indicators.extend(extra_indicators)
        
        return {
            'name': '基础指标',
            'headers': ['指标', '数值'],
            'data': indicators
        }

    def get_max_drawdown(df:pd.DataFrame, date_column: str, value_column: str) -> str:
        """计算最大回撤"""
        drawdown_list = calculate_max_drawdown(df[date_column], df[value_column])
        
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

    def calculate_yearly_stats(
        df: pd.DataFrame,
        date_column: str,
        value_column: str,
        date_format: str = '%Y-%m-%d'
    ) -> TableData:
        """
        计算年度统计数据
        
        Args:
            df: 包含日期和数值的DataFrame
            date_column: 日期列名
            value_column: 数值列名
            date_format: 日期格式化字符串
            
        Returns:
            TableData: 年度统计表格数据
        """
        # 确保日期列是datetime类型
        df = df.copy()
        df[date_column] = pd.to_datetime(df[date_column])
        df['year'] = df[date_column].dt.year
        
        yearly_stats = []
        for year in sorted(df['year'].unique(), reverse=True):
            year_data = df[df['year'] == year]
            
            # 获取年度起止日期
            start_date = year_data.iloc[0][date_column].strftime(date_format)
            end_date = year_data.iloc[-1][date_column].strftime(date_format)
            
            # 计算年度收益率
            start_value = year_data.iloc[0][value_column]
            end_value = year_data.iloc[-1][value_column]
            return_rate = (end_value - start_value) / start_value * 100
            
            # 计算年化收益率
            days = (year_data.iloc[-1][date_column] - year_data.iloc[0][date_column]).days
            annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
            
            # 计算年度最大回撤
            drawdown_list = calculate_max_drawdown(
                year_data[date_column],
                year_data[value_column]
            )
            max_drawdown = f"{drawdown_list[0]['value']*100:.2f}%" if drawdown_list else 'N/A'
            
            # 计算年度波动率
            returns = year_data[value_column].pct_change()
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

    @staticmethod
    def calculate_quarterly_stats(
        df: pd.DataFrame,
        date_column: str,
        value_column: str,
        date_format: str = '%Y-%m-%d'
    ) -> TableData:
        """
        计算季度统计数据
        
        Args:
            df: 包含日期和数值的DataFrame
            date_column: 日期列名
            value_column: 数值列名
            date_format: 日期格式化字符串
            
        Returns:
            TableData: 季度统计表格数据
        """
        if df is None or df.empty:
            return {
                'name': '季度统计',
                'headers': ['季度', '收益率', '年化收益率', '最大回撤', '波动率'],
                'data': []
            }

        # 确保日期列是datetime类型
        df = df.copy()
        df[date_column] = pd.to_datetime(df[date_column])
        df['year'] = df[date_column].dt.year
        df['quarter'] = df[date_column].dt.quarter
        
        quarterly_stats = []
        for year in sorted(df['year'].unique(), reverse=True):
            year_data = df[df['year'] == year]
            for quarter in sorted(year_data['quarter'].unique(), reverse=True):
                quarter_data = year_data[year_data['quarter'] == quarter]
                
                # 获取季度起止日期
                start_date = quarter_data.iloc[0][date_column].strftime(date_format)
                end_date = quarter_data.iloc[-1][date_column].strftime(date_format)
                
                # 计算季度收益率
                start_value = quarter_data.iloc[0][value_column]
                end_value = quarter_data.iloc[-1][value_column]
                return_rate = (end_value - start_value) / start_value * 100
                
                # 计算年化收益率
                days = (quarter_data.iloc[-1][date_column] - quarter_data.iloc[0][date_column]).days
                annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
                
                # 计算季度最大回撤
                drawdown_list = calculate_max_drawdown(
                    quarter_data[date_column],
                    quarter_data[value_column]
                )
                max_drawdown = f"{drawdown_list[0]['value']*100:.2f}%" if drawdown_list else 'N/A'
                
                # 计算季度波动率
                returns = quarter_data[value_column].pct_change()
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

    @staticmethod
    def calculate_ma_data(
        df: pd.DataFrame,
        date_column: str,
        value_column: str,
        period: int,
        normalize: bool = False
    ) -> List[Dict[str, Any]]:
        """
        计算移动平均线数据
        
        Args:
            df: 包含日期和数值的DataFrame
            date_column: 日期列名
            value_column: 数值列名
            period: 移动平均周期
            normalize: 是否对数据进行归一化处理
            
        Returns:
            List[Dict[str, Any]]: 移动平均线图表数据
        """
        if df is None or df.empty:
            return []
            
        dates = df[date_column].tolist()
        ma_data = []
        
        values = df[value_column]
        if normalize:
            values = DataGenerator.normalize_series(values)
            
        ma = values.rolling(window=period).mean()
        ma_data.append({
            'x': dates,
            'y': ma.tolist(),
            'type': 'line',
            'name': f'MA{period}',
            'visible': True,
            'line': {'dash': 'dot'}
        })
        
        return ma_data

    @staticmethod
    def calculate_drawdown_chart_data(
        df: pd.DataFrame,
        date_column: str,
        value_column: str,
        normalize: bool = False
    ) -> List[Dict[str, Any]]:
        """
        计算回撤图表数据
        
        Args:
            df: 包含日期和数值的DataFrame
            date_column: 日期列名
            value_column: 数值列名
            normalize: 是否对数据进行归一化处理
            
        Returns:
            List[Dict[str, Any]]: 回撤图表数据
        """
        if df is None or df.empty:
            return []
            
        values = df[value_column]
        if normalize:
            values = DataGenerator.normalize_series(values)
            
        dates = df[date_column]
        drawdown_list = calculate_max_drawdown(dates, values)
        
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
                    
                start_date = dd['start_date']
                end_date = dd['end_date']
                data.append({
                    'type': 'scatter',
                    'x': [start_date, end_date, end_date, start_date, start_date],
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
                    recovery_date = dd['recovery_date']
                    data.append({
                        'type': 'scatter',
                        'x': [end_date, recovery_date, recovery_date, end_date, end_date],
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

    @abstractmethod
    def load(self) -> bool:
        """
        手动加载数据
        
        Returns:
            bool: 数据加载是否成功
        """
        pass

    @abstractmethod
    def get_params_config(self) -> List[ParamConfig]:
        """
        获取可调节的参数配置

        Returns:
            List[ParamConfig]: 参数配置列表，每个元素描述一个可调节的参数
        """
        pass

    @abstractmethod
    def update_params(self, params: Dict[str, Any]) -> bool:
        """
        更新参数值

        Args:
            params: 参数名称和新值的字典
            
        Returns:
            bool: 更新是否成功
        """
        pass

    @abstractmethod
    def get_summary_data(self) -> List[Tuple[str, Any]]:
        """获取摘要数据，用于展示基本信息"""
        pass

    @abstractmethod
    def get_chart_data(self, normalize: bool = False, chart_type: int = 0) -> List[Dict[str, Any]]:
        """
        获取图表数据，用于绘制图形
        
        Args:
            normalize: 是否对数据进行归一化处理，默认为False
        """
        pass

    @abstractmethod
    def get_extra_datas(self) -> List[TableData]:
        """
        获取额外的表格数据，用于展示各类指标
        
        Returns:
            List[TableData]: 表格数据列表，每个元素包含一个表格的完整信息
        """
        pass

    @abstractmethod
    def get_extra_chart_data(self, data_type: ChartDataType, normalize: bool = False, **params) -> List[Dict[str, Any]]:
        """
        获取额外的图表数据
        
        Args:
            data_type: 数据类型，如 'MA5', 'MA20', 'drawdown' 等
            normalize: 是否对数据进行归一化处理，默认为False
            **params: 数据计算的参数

        Returns:
            List[Dict[str, Any]]: 图表数据列表，每个元素为一个数据系列
        """
        pass

    @abstractmethod
    def get_value_data(self) -> pd.DataFrame:
        """
        获取用于计算相关系数的主要数据
        
        Returns:
            pd.DataFrame: 包含 date 和 value 两列的 DataFrame
        """
        pass

    def normalize_series(self, series: pd.Series) -> pd.Series:
        """归一化数据序列"""
        if series.empty:
            return series
        return series / series.iloc[0]