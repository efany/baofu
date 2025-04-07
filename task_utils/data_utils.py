import pandas as pd
import sys
import os
from datetime import datetime
from loguru import logger

def calculate_return_rate(data, loc_name='accum_nav'):
    first_nav = data.iloc[0][loc_name]
    last_nav = data.iloc[-1][loc_name]
    return_rate = (last_nav - first_nav) / first_nav * 100
    return (first_nav, last_nav, return_rate)

def calculate_adjusted_nav(fund_nav, start_date=None, end_date=None):
    """
    计算考虑分红再投资的单位净值修正数据
    :param fund_nav: DataFrame，包含基金净值数据，需包含'nav_date', 'unit_nav', 'dividend'列
    :param start_date: str，开始日期（可选）
    :param end_date: str，结束日期（可选）
    :return: DataFrame，包含修正后的单位净值数据
    """

    # 初始化修正净值
    fund_nav['adjusted_nav'] = None
    adjustment_factor = 1.0  # 调整因子

    # 遍历数据，计算修正净值
    fund_nav.at[fund_nav.index[0], 'adjusted_nav'] = fund_nav.iloc[0]['unit_nav']
    for i in range(1, len(fund_nav)):
        current_date = fund_nav.iloc[i]['nav_date']
        current_date = pd.to_datetime(current_date).date()

        if start_date and current_date < start_date:
            continue
        if end_date and current_date > end_date:
            break
        # 获取前一日和当前日的净值
        prev_nav = fund_nav.iloc[i-1]['unit_nav']
        current_nav = fund_nav.iloc[i]['unit_nav']
        
        # 获取当前日的分红
        dividend = fund_nav.iloc[i]['dividend']
        
        # 如果有分红，计算调整因子
        if dividend is not None and dividend > 0:
            adjustment_factor *= (prev_nav + dividend) / prev_nav

        # 计算修正净值
        fund_nav.at[fund_nav.index[i], 'adjusted_nav'] = current_nav * adjustment_factor

def calculate_max_drawdown(date_series:pd.Series, value_series:pd.Series, start_date=None, end_date=None):
    """
    计算前三大回撤
    :param date_series: pd.Series，日期数据
    :param value_series: pd.Series，净值数据
    :param start_date: str，开始日期（可选）
    :param end_date: str，结束日期（可选）
    :return: list of tuples，每个tuple包含回撤值、回撤开始日期和结束日期
    """
    if start_date and end_date:
        mask = (date_series.dt.date >= start_date) & (date_series.dt.date  <= end_date)
        date_series_filtered = date_series[mask]
        value_series_filtered = value_series[mask]
    else:
        date_series_filtered = date_series
        value_series_filtered = value_series

    # 如果数据为空，返回默认值
    if len(date_series_filtered) == 0 or len(value_series_filtered) == 0 or len(date_series_filtered) != len(value_series_filtered):
        return [(0, None, None), (0, None, None), (0, None, None)]

    # 找到所有回撤
    drawdowns = []
    peak_value = value_series_filtered.iloc[-1]
    peak_date = date_series_filtered.iloc[-1]
    highest_value = peak_value
    highest_date = peak_date
    
    # 从最后一个元素开始向前遍历
    for i in range(len(date_series_filtered) - 1, -1, -1):
        current_value = value_series_filtered.iloc[i]
        current_date = date_series_filtered.iloc[i]
        if current_value > highest_value:
            highest_value = current_value
            highest_date = current_date
        elif current_value <= peak_value:
            if highest_value > peak_value:
                drawdown = (highest_value - peak_value) / highest_value
                # 查找peak_date之后的日期中最近一个净值超过highest_nav的日期
                recovery_date = None
                recovery_mask = (date_series_filtered > peak_date) & (value_series_filtered > highest_value)
                recovery_dates = date_series_filtered.loc[recovery_mask]
                if not recovery_dates.empty:
                    recovery_date = recovery_dates.iloc[0]  # 取第一个符合条件的日期

                drawdowns.append({
                    'value': abs(drawdown),
                    'start_date': highest_date,
                    'start_value': highest_value,
                    'end_date': peak_date,
                    'end_value': peak_value,
                    'recovery_date': recovery_date
                })
            peak_value = current_value
            peak_date = current_date
            highest_value = current_value
            highest_date = current_date

    # 处理最后一个回撤
    if highest_value > peak_value:
        drawdown = (highest_value - peak_value) / highest_value
         # 查找peak_date之后的日期中最近一个净值超过highest_value的日期
        recovery_date = None
        recovery_mask = (date_series_filtered > peak_date) & (value_series_filtered > highest_value)
        recovery_dates = date_series_filtered.loc[recovery_mask]
        if not recovery_dates.empty:
            recovery_date = recovery_dates.iloc[0]  # 取第一个符合条件的日期
        drawdowns.append({
            'value': abs(drawdown),
            'start_date': highest_date,
            'start_value': highest_value,
            'end_date': peak_date,
            'end_value': peak_value,
            'recovery_date': recovery_date
        })
    
    # 按回撤值排序并取前三个
    sorted_drawdowns = sorted(drawdowns, key=lambda x: x['value'], reverse=True)[:3]
    
    # 转换为百分比并返回
    return sorted_drawdowns

if __name__ == '__main__':
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from database.mysql_database import MySQLDatabase
    from database.db_funds_nav import DBFundsNav

    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )

    db_funds_nav = DBFundsNav(mysql_db)

    # 获取单个基金净值数据
    fund_nav = db_funds_nav.get_fund_nav('003376', start_date='2023-01-01')
    print(fund_nav)

    # 使用date类型而不是datetime
    drawdowns = calculate_max_drawdown(
        fund_nav,
        start_date=datetime.strptime('2024-03-01', '%Y-%m-%d').date(),
        end_date=datetime.strptime('2025-03-02', '%Y-%m-%d').date()
    )
    print(drawdowns)

    mysql_db.close_connection() 