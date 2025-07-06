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

    def calculate_yearly_stats(
        self,
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