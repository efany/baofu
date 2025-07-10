from typing import Dict, List, Any, Tuple
import pandas as pd
from task_utils.data_utils import calculate_max_drawdown
from loguru import logger

class DataCalculator:
    """数据计算工具类，提供各种金融数据计算功能"""

    @staticmethod
    def normalize_series(series: pd.Series) -> pd.Series:
        """归一化数据序列"""
        if series.empty:
            return series
        return series / series.iloc[0]

    @staticmethod
    def calculate_basic_indicators(
        df: pd.DataFrame,
        date_column: str,
        value_column: str,
        currency_symbol: str = '',
        value_format: str = '.2f',
        extra_indicators: List[Tuple[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        计算基础指标数据
        
        Args:
            df: 包含日期和数值的DataFrame
            date_column: 日期列名
            value_column: 数值列名
            currency_symbol: 货币符号，用于格式化显示
            value_format: 数值格式化字符串
            extra_indicators: 额外的指标数据
            
        Returns:
            Dict[str, Any]: 基础指标表格数据
        """
        if df is None or df.empty:
            pd_data = pd.DataFrame(columns=['指标', '数值'])
            return {
                'name': '基础指标',
                'pd_data': pd_data
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
            ['投资最大回撤', DataCalculator.get_max_drawdown(df, date_column, value_column)],
            ['年化波动率', f'{volatility:.2f}%']
        ]
        
        # 添加额外指标
        if extra_indicators:
            indicators.extend(extra_indicators)
        
        pd_data = pd.DataFrame(indicators, columns=['指标', '数值'])
        return {
            'name': '基础指标',
            'pd_data': pd_data
        }

    @staticmethod
    def get_max_drawdown(df: pd.DataFrame, date_column: str, value_column: str) -> str:
        """
        计算最大回撤
        
        Args:
            df: 包含日期和数值的DataFrame
            date_column: 日期列名
            value_column: 数值列名
            
        Returns:
            str: 格式化的最大回撤信息
        """
        drawdown_list = calculate_max_drawdown(df[date_column], df[value_column])
        
        if drawdown_list is not None and not drawdown_list.empty:
            dd = drawdown_list.iloc[0]
            max_dd = dd['value'] * 100
            start_date = dd['start_date'].strftime('%Y-%m-%d')
            end_date = dd['end_date'].strftime('%Y-%m-%d')
            start_value = dd['start_value']
            end_value = dd['end_value']
            
            # 如果有恢复日期，添加恢复信息
            recovery_info = ""
            if dd['recovery_date'] is not None and dd['recovery_date'] is not pd.NaT:
                days_to_recover = (dd['recovery_date'] - dd['end_date']).days
                logger.info(f"recovery_date: {dd['recovery_date']}, end_date: {dd['end_date']}, days_to_recover: {days_to_recover}")
                recovery_info = f", 恢复天数: {days_to_recover}天"
            
            return f"{max_dd:.2f}% ({start_date}~{end_date}{recovery_info}, {start_value:.4f}->{end_value:.4f})"
        
        return 'N/A'

    @staticmethod
    def calculate_yearly_stats(
        df: pd.DataFrame,
        date_column: str,
        value_column: str,
        date_format: str = '%Y-%m-%d'
    ) -> Dict[str, Any]:
        """
        计算年度统计数据
        
        Args:
            df: 包含日期和数值的DataFrame
            date_column: 日期列名
            value_column: 数值列名
            date_format: 日期格式化字符串
            
        Returns:
            Dict[str, Any]: 年度统计表格数据
        """
        if df is None or df.empty:
            return {
                'name': '年度统计',
                'pd_data': pd.DataFrame(columns=['年份', '收益率', '年化收益率', '最大回撤', '波动率'])
            }

        # 确保日期列是datetime类型
        df = df.copy()
        df[date_column] = pd.to_datetime(df[date_column])
        df['year'] = df[date_column].dt.year
        
        yearly_stats = []
        years = sorted(df['year'].unique(), reverse=True)
        
        for i, year in enumerate(years):
            year_data = df[df['year'] == year]
            
            # 获取年度起止日期
            end_date = year_data.iloc[-1][date_column].strftime(date_format)
            end_value = year_data.iloc[-1][value_column]
            
            # 获取上一年度最后一个交易日的数据作为起点
            if i < len(years) - 1:  # 如果不是最早的年份
                prev_year = years[i + 1]
                prev_year_data = df[df['year'] == prev_year]
                start_date = prev_year_data.iloc[-1][date_column].strftime(date_format)
                start_value = prev_year_data.iloc[-1][value_column]
            else:  # 最早的年份使用当年第一个交易日
                start_date = year_data.iloc[0][date_column].strftime(date_format)
                start_value = year_data.iloc[0][value_column]

            # 检查有效交易日数量
            if len(year_data) < 7:
                continue
            
            # 计算年度收益率
            return_rate = (end_value - start_value) / start_value * 100
            
            # 计算年化收益率
            days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days
            annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
            
            # 计算年度最大回撤
            drawdown_list = calculate_max_drawdown(
                year_data[date_column],
                year_data[value_column]
            )
            max_drawdown = f"{drawdown_list.iloc[0]['value']*100:.2f}%" if drawdown_list is not None and not drawdown_list.empty else 'N/A'
            
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
            'pd_data': pd.DataFrame(yearly_stats, columns=['年份', '收益率', '年化收益率', '最大回撤', '波动率'])
        }

    @staticmethod
    def calculate_quarterly_stats(
        df: pd.DataFrame,
        date_column: str,
        value_column: str,
        date_format: str = '%Y-%m-%d'
    ) -> Dict[str, Any]:
        """
        计算季度统计数据
        
        Args:
            df: 包含日期和数值的DataFrame
            date_column: 日期列名
            value_column: 数值列名
            date_format: 日期格式化字符串
            
        Returns:
            Dict[str, Any]: 季度统计表格数据
        """
        if df is None or df.empty:
            return {
                'name': '季度统计',
                'pd_data': pd.DataFrame(columns=['季度', '收益率', '年化收益率', '最大回撤', '波动率'])
            }

        # 确保日期列是datetime类型
        df = df.copy()
        df[date_column] = pd.to_datetime(df[date_column])
        df['year'] = df[date_column].dt.year
        df['quarter'] = df[date_column].dt.quarter
        
        quarterly_stats = []
        # 创建年季度组合并排序
        df_grouped = df.groupby(['year', 'quarter']).agg({date_column: ['first', 'last']}).sort_index(ascending=False)
        year_quarters = [(year, quarter) for year, quarter in df_grouped.index]
        
        for i, (year, quarter) in enumerate(year_quarters):
            quarter_data = df[(df['year'] == year) & (df['quarter'] == quarter)]
            
            # 获取季度末数据
            end_date = quarter_data.iloc[-1][date_column]
            end_value = quarter_data.iloc[-1][value_column]
            
            # 获取上一季度最后一个交易日的数据作为起点
            if i < len(year_quarters) - 1:  # 如果不是最早的季度
                prev_year, prev_quarter = year_quarters[i + 1]
                prev_quarter_data = df[(df['year'] == prev_year) & (df['quarter'] == prev_quarter)]
                if not prev_quarter_data.empty:
                    start_date = prev_quarter_data.iloc[-1][date_column]
                    start_value = prev_quarter_data.iloc[-1][value_column]
                else:
                    start_date = quarter_data.iloc[0][date_column]
                    start_value = quarter_data.iloc[0][value_column]
            else:  # 最早的季度使用当季第一个交易日
                start_date = quarter_data.iloc[0][date_column]
                start_value = quarter_data.iloc[0][value_column]
            
            # 检查有效交易日数量
            if len(quarter_data) < 7:
                continue
            
            # 计算季度收益率
            return_rate = (end_value - start_value) / start_value * 100
            
            # 计算年化收益率
            days = (end_date - start_date).days
            annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
            
            # 计算季度最大回撤
            drawdown_list = calculate_max_drawdown(
                quarter_data[date_column],
                quarter_data[value_column]
            )
            max_drawdown = f"{drawdown_list.iloc[0]['value']*100:.2f}%" if drawdown_list is not None and not drawdown_list.empty else 'N/A'
            
            # 计算季度波动率
            returns = quarter_data[value_column].pct_change()
            volatility = returns.std() * (252 ** 0.5) * 100
            
            quarterly_stats.append([
                f"{year}Q{quarter} ({start_date.strftime(date_format)}~{end_date.strftime(date_format)})",
                f'{return_rate:+.2f}%',
                f'{annualized_return:+.2f}%',
                max_drawdown,
                f'{volatility:.2f}%'
            ])
        
        return {
            'name': '季度统计',
            'pd_data': pd.DataFrame(quarterly_stats, columns=['季度', '收益率', '年化收益率', '最大回撤', '波动率'])
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
            values = DataCalculator.normalize_series(values)
            
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
    def calculate_drawdown_data(
        df: pd.DataFrame,
        date_column: str,
        value_column: str,
        normalize: bool = False
    ) -> pd.DataFrame:
        """
        计算回撤数据
        """
        if df is None or df.empty:
            return []
            
        values = df[value_column]
        if normalize:
            values = DataCalculator.normalize_series(values)
            
        dates = df[date_column]
        drawdown_list = calculate_max_drawdown(dates, values)

        return drawdown_list

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
        drawdown_list = DataCalculator.calculate_drawdown_data(df, date_column, value_column, normalize)
        if drawdown_list is None or drawdown_list.empty:
            return []
        
        data = []
        # 绘制回撤区域
        for i in range(len(drawdown_list)):
            dd = drawdown_list.iloc[i]
            if pd.notna(dd['value']):
                drawdown_days = (dd['end_date'] - dd['start_date']).days
                recovery_days = (dd['recovery_date'] - dd['end_date']).days if dd['recovery_date'] is not None and dd['recovery_date'] is not pd.NaT else None
                
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

    @staticmethod
    def calculate_correlation_matrix(
        data_dict: Dict[str, pd.DataFrame], 
        method: str = 'pearson',
        data_processing: str = 'returns'
    ) -> pd.DataFrame:
        """计算相关性矩阵"""
        if not data_dict:
            return pd.DataFrame()
        
        # 合并所有数据
        merged_df = None
        for product_id, df in data_dict.items():
            if df.empty:
                continue
                
            df_copy = df.copy()
            df_copy['date'] = pd.to_datetime(df_copy['date'])
            df_copy = df_copy.sort_values('date')
            
            # 数据处理
            if data_processing == 'returns':
                df_copy['value'] = df_copy['value'].pct_change()
            elif data_processing == 'normalized':
                df_copy['value'] = df_copy['value'] / df_copy['value'].iloc[0]
            
            df_copy = df_copy.rename(columns={'value': product_id})
            
            if merged_df is None:
                merged_df = df_copy[['date', product_id]]
            else:
                merged_df = pd.merge(merged_df, df_copy[['date', product_id]], on='date', how='outer')
        
        if merged_df is None:
            return pd.DataFrame()
        
        # 计算相关性矩阵
        correlation_data = merged_df.drop('date', axis=1)
        correlation_matrix = correlation_data.corr(method=method)
        
        return correlation_matrix 