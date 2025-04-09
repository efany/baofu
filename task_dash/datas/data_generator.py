from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple, Literal, TypedDict
from datetime import date
import pandas as pd
import numpy as np

# 定义支持的数据类型
ChartDataType = Literal['MA5', 'MA20', 'MA60', 'MA120', 'drawdown']

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