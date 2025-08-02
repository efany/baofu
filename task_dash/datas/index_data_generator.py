from typing import Dict, List, Optional, Any, Tuple
from datetime import date
import pandas as pd
from .data_generator import DataGenerator, TableData, ChartDataType
from database.db_index_hist import DBIndexHist
from database.mysql_database import MySQLDatabase
from task_utils.data_utils import calculate_return_rate
from loguru import logger

class IndexDataGenerator(DataGenerator):
    """指数数据生成器"""
    
    def __init__(self, index_symbol: str, mysql_db: MySQLDatabase, start_date: Optional[date] = None, end_date: Optional[date] = None):
        super().__init__(start_date, end_date)
        self.index_symbol = index_symbol
        self.db_index_hist = DBIndexHist(mysql_db)
        self.index_data = None
        self.data = None
    
    def load(self) -> bool:
        """加载指数数据"""
        start_date = self.params['start_date'] if 'start_date' in self.params else None
        end_date = self.params['end_date'] if 'end_date' in self.params else None

        # 获取指数历史数据
        self.index_data = self.db_index_hist.get_index_hist_data(
            symbol=self.index_symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        if not self.index_data.empty:
            # 确保日期列为datetime类型
            self.index_data['date'] = pd.to_datetime(self.index_data['date'])
            
            # 按日期排序
            self.index_data = self.index_data.sort_values('date')
            
            # 创建数据副本用于计算
            self.data = self.index_data.copy()
            
            # 确保所有数值列为float类型，处理Decimal类型
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'turnover']
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce').astype(float)
            
            # 添加指数名称映射
            index_names = {
                'sh000001': '上证综指',
                'sh000002': '上证A股指数',  
                'sh000016': '上证50',
                'sh000300': '沪深300',
                'sh000905': '中证500',
                'sh000906': '中证800',
                'sz399001': '深证成指',
                'sz399005': '中小板指',
                'sz399006': '创业板指'
            }
            
            self.data['name'] = index_names.get(self.index_symbol, self.index_symbol)
            
        else:
            self.data = None

        logger.info(f"指数数据加载完成: {self.index_symbol} {start_date} {end_date}, 共{len(self.data) if self.data is not None else 0}条数据")
        return self.data is not None and not self.data.empty
    
    def get_summary_data(self) -> List[Tuple[str, Any]]:
        """获取指数摘要数据"""
        if self.data is None or self.data.empty:
            return []

        # 数据已经在load()中转换为float类型
        first_close = self.data.iloc[0]['close']
        last_close = self.data.iloc[-1]['close']
        return_rate = ((last_close - first_close) / first_close) * 100
        
        # 获取起止日期
        start_date = self.data.iloc[0]['date'].strftime('%Y-%m-%d')
        end_date = self.data.iloc[-1]['date'].strftime('%Y-%m-%d')
        date_range = f"{start_date} ~ {end_date}"
        
        # 计算最高价和最低价
        max_close = self.data['close'].max()
        min_close = self.data['close'].min()
        
        # 计算平均成交量
        if 'volume' in self.data.columns:
            avg_volume = self.data['volume'].mean()
        else:
            avg_volume = 0
        
        return [
            ('指数代码', self.index_symbol),
            ('指数名称', self.data.iloc[0]['name']),
            ('统计区间', date_range),
            ('区间收益率', f"{return_rate:+.2f}%"),
            ('指数变化', f"({first_close:.2f} -> {last_close:.2f})"),
            ('期间最高', f"{max_close:.2f}"),
            ('期间最低', f"{min_close:.2f}"),
            ('平均成交量', f"{avg_volume:,.0f}" if avg_volume > 0 else "无数据")
        ]

    def get_chart_data(self, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取指数图表数据"""
        if self.data is None or self.data.empty:
            return []
        
        # 确保日期格式正确，转换为字符串格式用于图表显示
        dates = pd.to_datetime(self.data['date']).dt.strftime('%Y-%m-%d').tolist()
        
        # 数据已经在load()中转换为float类型
        close_prices = self.data['close']
        open_prices = self.data['open']
        high_prices = self.data['high']
        low_prices = self.data['low']
        
        # 如果需要归一化处理
        if normalize:
            close_prices = self.normalize_series(close_prices)
            open_prices = self.normalize_series(open_prices)
            high_prices = self.normalize_series(high_prices)
            low_prices = self.normalize_series(low_prices)
        
        chart_data = [
            {
                'x': dates,
                'y': close_prices.tolist(),
                'type': 'line',
                'name': '收盘价',
                'visible': True
            }
        ]
        
        # 添加开盘价、最高价、最低价（默认隐藏）
        chart_data.extend([
            {
                'x': dates,
                'y': open_prices.tolist(),
                'type': 'line',
                'name': '开盘价',
                'visible': 'legendonly',
                'line': {'color': '#ff7f0e', 'width': 1}
            },
            {
                'x': dates,
                'y': high_prices.tolist(),
                'type': 'line',
                'name': '最高价',
                'visible': 'legendonly',
                'line': {'color': '#2ca02c', 'width': 1}
            },
            {
                'x': dates,
                'y': low_prices.tolist(),
                'type': 'line',
                'name': '最低价',
                'visible': 'legendonly',
                'line': {'color': '#d62728', 'width': 1}
            }
        ])
        
        # 如果有成交量数据，添加成交量图表（但先注释掉，因为可能导致图表显示问题）
        # 成交量需要单独的Y轴配置，暂时禁用避免显示问题
        if False and 'volume' in self.data.columns and not self.data['volume'].isna().all():
            volume = pd.to_numeric(self.data['volume'], errors='coerce').fillna(0).astype(float)
            # 过滤掉无效的成交量数据
            if volume.sum() > 0:  # 只有当有有效成交量数据时才添加
                chart_data.append({
                    'x': dates,
                    'y': volume.tolist(),
                    'type': 'bar',
                    'name': '成交量',
                    'visible': 'legendonly',
                    'yaxis': 'y2',
                    'marker': {'color': 'rgba(158,202,225,0.5)'}
                })
        
        return chart_data
    
    def get_extra_datas(self) -> List[TableData]:
        """获取指数额外数据"""
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
    
    def get_extra_chart_data(self, data_type: ChartDataType, normalize: bool = False, **params) -> List[Dict[str, Any]]:
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
        """获取指数价值数据"""
        if self.data is None or self.data.empty:
            return pd.DataFrame()
        
        return pd.DataFrame({
            'date': pd.to_datetime(self.data['date']),
            'value': pd.to_numeric(self.data['close'], errors='coerce').astype(float)
        })
    
    def get_supported_indices(self) -> List[Dict[str, str]]:
        """获取支持的指数列表"""
        return [
            {'value': 'sh000001', 'label': '上证综指'},
            {'value': 'sh000002', 'label': '上证A股指数'},
            {'value': 'sh000016', 'label': '上证50'},
            {'value': 'sh000300', 'label': '沪深300'},
            {'value': 'sh000905', 'label': '中证500'},
            {'value': 'sh000906', 'label': '中证800'},
            {'value': 'sz399001', 'label': '深证成指'},
            {'value': 'sz399005', 'label': '中小板指'},
            {'value': 'sz399006', 'label': '创业板指'}
        ]