from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple, Literal, TypedDict
from datetime import date
import pandas as pd
import numpy as np
from .data_calculator import DataCalculator

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
    pd_data: pd.DataFrame  # 表格数据

class DataGenerator(ABC):
    """数据生成器基类，负责生成各类型数据用于页面展示"""

    def __init__(self, start_date: Optional[date] = None, end_date: Optional[date] = None):
        self.is_loaded = False  # 添加数据加载状态标记
        self.data = None  # 存储数据的属性

        self.params = {}
        self.params['start_date'] = start_date
        self.params['end_date'] = end_date
        
        self.parameter_configs = []

    def normalize_series(self, series: pd.Series) -> pd.Series:
        """归一化数据序列"""
        return DataCalculator.normalize_series(series)

    def _get_basic_indicators(self, date_column: str, value_column: str, value_format: str = '.2f') -> TableData:
        """获取基础指标表格 - 通用实现"""
        if self.data is None or self.data.empty:
            return {
                'name': '基础指标',
                'headers': ['指标', '数值'],
                'data': []
            }
        
        return DataCalculator.calculate_basic_indicators(
            df=self.data,
            date_column=date_column,
            value_column=value_column,
            value_format=value_format
        )

    def _get_yearly_stats(self, date_column: str, value_column: str) -> TableData:
        """获取年度统计表格 - 通用实现"""
        if self.data is None or self.data.empty:
            return {
                'name': '年度统计',
                'headers': ['年份', '收益率', '年化收益率', '最大回撤', '波动率'],
                'data': []
            }
        
        return DataCalculator.calculate_yearly_stats(
            df=self.data,
            date_column=date_column,
            value_column=value_column
        )

    def _get_quarterly_stats(self, date_column: str, value_column: str) -> TableData:
        """获取季度统计表格 - 通用实现"""
        if self.data is None or self.data.empty:
            return {
                'name': '季度统计',
                'headers': ['季度', '收益率', '年化收益率', '最大回撤', '波动率'],
                'data': []
            }
        
        return DataCalculator.calculate_quarterly_stats(
            df=self.data,
            date_column=date_column,
            value_column=value_column
        )

    def _get_ma_data(self, period: int, date_column: str, value_column: str, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取移动平均线数据 - 通用实现"""
        return DataCalculator.calculate_ma_data(
            df=self.data,
            date_column=date_column,
            value_column=value_column,
            period=period,
            normalize=normalize
        )

    def _get_drawdown_data(self, date_column: str, value_column: str, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取回撤数据 - 通用实现"""
        return DataCalculator.calculate_drawdown_chart_data(
            df=self.data,
            date_column=date_column,
            value_column=value_column,
            normalize=normalize
        )

    @abstractmethod
    def load(self) -> bool:
        """
        手动加载数据
        
        Returns:
            bool: 数据加载是否成功
        """
        pass

    def get_params_config(self) -> List[ParamConfig]:
        """
        获取可调节的参数配置

        Returns:
            List[ParamConfig]: 参数配置列表，每个元素描述一个可调节的参数
        """
        return self.parameter_configs

    def update_params(self, params: Dict[str, Any]) -> bool:
        """
        更新参数值

        Args:
            params: 参数名称和新值的字典
            
        Returns:
            bool: 更新是否成功
        """        """更新策略的参数设置"""
        for param_name, param_value in params.items():
            if param_name in self.params:
                self.params[param_name] = param_value
                return True
        return False

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