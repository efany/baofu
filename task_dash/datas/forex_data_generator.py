from datetime import date
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from loguru import logger

from database.db_forex_day_hist import DBForexDayHist
from database.mysql_database import MySQLDatabase
from .data_generator import DataGenerator, TableData, ParamConfig
from task_utils.data_utils import calculate_return_rate, calculate_max_drawdown

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
        self.forex_data = None
    
    def load(self) -> bool:
        """加载外汇数据"""
        self.forex_data = self.db_forex_hist.get_extend_forex_hist_data(self.forex_code, self.start_date, self.end_date)
        if not self.forex_data.empty:
            self.forex_data['date'] = pd.to_datetime(self.forex_data['date'])
            self.forex_data = self.forex_data.sort_values('date')
            logger.info(f"外汇数据加载完成: {self.forex_code}  {self.start_date}  {self.end_date} , 共{len(self.forex_data)}条数据")
        else:
            logger.warning(f"未找到外汇数据: {self.forex_code}")
            return False
        
        return True

    def get_params_config(self) -> List[ParamConfig]:
        """获取外汇参数配置"""
        return []
    
    def update_params(self, params: Dict[str, Any]) -> bool:
        """更新外汇参数"""
        return True

    def get_summary_data(self) -> List[Tuple[str, Any]]:
        """获取外汇摘要数据"""
        if self.forex_data is None or self.forex_data.empty:
            return []

        first_close, last_close, return_rate = calculate_return_rate(self.forex_data, loc_name='close')
        
        # 获取起止日期
        start_date = self.forex_data.iloc[0]['date'].strftime('%Y-%m-%d')
        end_date = self.forex_data.iloc[-1]['date'].strftime('%Y-%m-%d')
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
        if self.forex_data is None or self.forex_data.empty:
            return []

        dates = self.forex_data['date'].tolist()
        chart_data = []

        if chart_type == 0:  # K线图
            chart_data.append({
                'x': dates,
                'open': self.forex_data['open'].tolist(),
                'high': self.forex_data['high'].tolist(),
                'low': self.forex_data['low'].tolist(),
                'close': self.forex_data['close'].tolist(),
                'type': 'candlestick',
                'name': self.forex_code,
                'visible': True
            })
        else:  # 折线图
            values = self.forex_data['close']
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
        if self.forex_data is None or self.forex_data.empty:
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
        first_close = self.forex_data.iloc[0]['close']
        last_close = self.forex_data.iloc[-1]['close']
        return_rate = (last_close - first_close) / first_close * 100
        
        # 计算年化收益率
        days = (self.forex_data.iloc[-1]['date'] - self.forex_data.iloc[0]['date']).days
        annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
        
        # 计算风险指标
        returns = self.forex_data['close'].pct_change()
        volatility = returns.std() * (252 ** 0.5) * 100  # 年化波动率
        
        return {
            'name': '基础指标',
            'headers': ['指标', '数值'],
            'data': [
                ['投资收益率', f'{return_rate:+.2f}% ({first_close:.4f} -> {last_close:.4f})'],
                ['年化收益率', f'{annualized_return:+.2f}%'],
                ['投资最大回撤', self._get_max_drawdown()],
                ['年化波动率', f'{volatility:.2f}%'],
            ]
        }

    def _get_yearly_stats(self) -> TableData:
        """获取年度统计表格"""
        # 添加年份列
        df = self.forex_data.copy()
        df['year'] = df['date'].dt.year   
        
        yearly_stats = []
        for year in sorted(df['year'].unique(), reverse=True):
            year_data = df[df['year'] == year]
            
            # 获取年度起止日期
            start_date = year_data.iloc[0]['date'].strftime('%Y-%m-%d')
            end_date = year_data.iloc[-1]['date'].strftime('%Y-%m-%d')
            
            # 计算年度收益率
            start_close = year_data.iloc[0]['close']
            end_close = year_data.iloc[-1]['close']
            return_rate = (end_close - start_close) / start_close * 100
            
            # 计算年化收益率
            days = (year_data.iloc[-1]['date'] - year_data.iloc[0]['date']).days
            annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
            
            # 计算年度最大回撤
            drawdown_list = calculate_max_drawdown(
                year_data['date'],
                year_data['close']
            )
            max_drawdown = f"{drawdown_list[0]['value']*100:.2f}%" if drawdown_list else 'N/A'
            
            # 计算年度波动率
            returns = year_data['close'].pct_change()
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
        df = self.forex_data.copy()
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
                start_close = quarter_data.iloc[0]['close']
                end_close = quarter_data.iloc[-1]['close']
                return_rate = (end_close - start_close) / start_close * 100
                
                # 计算年化收益率
                days = (quarter_data.iloc[-1]['date'] - quarter_data.iloc[0]['date']).days
                annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
                
                # 计算季度最大回撤
                drawdown_list = calculate_max_drawdown(
                    quarter_data['date'],
                    quarter_data['close']
                )
                max_drawdown = f"{drawdown_list[0]['value']*100:.2f}%" if drawdown_list else 'N/A'
                
                # 计算季度波动率
                returns = quarter_data['close'].pct_change()
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
            self.forex_data['date'],
            self.forex_data['close']
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

    def get_extra_chart_data(self, data_type: str, normalize: bool = False, **params) -> List[Dict[str, Any]]:
        """获取额外的图表数据"""
        if self.forex_data is None or self.forex_data.empty:
            return []
            
        if data_type in ['MA5', 'MA20', 'MA60', 'MA120']:
            period = int(data_type.replace('MA', ''))
            return self._get_ma_data(period, 'close', normalize)
        elif data_type == 'drawdown':
            return self._get_drawdown_chart_data(normalize)
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    def _get_ma_data(self, period: int, value_column: str, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取移动平均线数据"""
        dates = self.forex_data['date'].tolist()
        ma_data = []
        
        values = self.forex_data[value_column]
        if normalize:
            values = self.normalize_series(values)
            
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

    def _get_drawdown_chart_data(self, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取回撤图表数据"""
        values = self.forex_data['close']
        if normalize:
            values = self.normalize_series(values)
            
        dates = self.forex_data['date']
        drawdown_list = calculate_max_drawdown(
            self.forex_data['date'],
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

    def get_value_data(self) -> pd.DataFrame:
        """获取用于计算相关系数的主要数据"""
        if self.forex_data is None or self.forex_data.empty:
            return pd.DataFrame()
        
        return pd.DataFrame({
            'date': self.forex_data['date'],
            'value': self.forex_data['close']
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