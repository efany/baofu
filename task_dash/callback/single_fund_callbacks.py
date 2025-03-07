import sys
import os
import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_utils.funds_utils import get_fund_data_by_name, calculate_return_rate, calculate_max_drawdown, calculate_adjusted_nav
from database.db_funds_nav import DBFundsNav
from database.db_funds import DBFunds

def get_date_range(time_range):
    """
    根据选择的时间范围和数据的日期范围，计算图表显示的日期范围
    """
    if time_range == 'ALL':
        return None, None

    # 获取数据的最新日期作为结束日期
    end_date = datetime.now().date()
    
    # 根据选择的时间范围计算开始日期
    if time_range == '1M':
        start_date = end_date - relativedelta(months=1)
    elif time_range == '3M':
        start_date = end_date - relativedelta(months=3)
    elif time_range == '6M':
        start_date = end_date - relativedelta(months=6)
    elif time_range == '1Y':
        start_date = end_date - relativedelta(years=1)
    elif time_range == '3Y':
        start_date = end_date - relativedelta(years=3)
    elif time_range == '5Y':
        start_date = end_date - relativedelta(years=5)
    elif time_range == 'CQ':
        # 本季度开始
        start_date = end_date.replace(month=((end_date.month-1)//3)*3+1, day=1)
    elif time_range == 'CY':
        # 本年度开始
        start_date = end_date.replace(month=1, day=1)
    else:
        return None, None

    return start_date, end_date

def register_single_fund_callbacks(app, mysql_db):
    @app.callback(
        Output('fund-value-graph', 'figure'),
        [Input('fund-dropdown', 'value'),
         Input('line-options', 'value'),
         Input('time-range-dropdown', 'value')]
    )
    def update_graph(selected_fund, line_options, time_range):
        
        db_funds_nav = DBFundsNav(mysql_db)
        fund_nav = db_funds_nav.get_fund_nav(selected_fund)

        if fund_nav.empty:
            return go.Figure()


        main_loc_name = 'adjusted_nav'

        # 转换日期列为datetime类型
        fund_nav['nav_date'] = pd.to_datetime(fund_nav['nav_date'])

        # 获取图表显示的日期范围
        start_date, end_date = get_date_range(time_range)

        calculate_adjusted_nav(fund_nav, start_date, end_date)

        # 准备图表数据
        data = [
            {
                'x': fund_nav['nav_date'],
                'y': fund_nav['adjusted_nav'],
                'type': 'line',
                'name': '再投资净值',
                'visible': True if main_loc_name == 'adjusted_nav' else 'legendonly'
            },
            {
                'x': fund_nav['nav_date'],
                'y': fund_nav['accum_nav'],
                'type': 'line',
                'name': '累计净值',
                'visible': True if main_loc_name == 'accum_nav' else 'legendonly'
            },
            {
                'x': fund_nav['nav_date'],
                'y': fund_nav['unit_nav'],
                'type': 'line',
                'name': '单位净值',
                'visible': True if main_loc_name == 'unit_nav' else 'legendonly'
            },
            {
                'x': fund_nav['nav_date'],
                'y': fund_nav['dividend'],
                'type': 'scatter',
                'mode': 'markers',
                'name': '分红',
                'marker': {'size': 10, 'color': 'red'},
                'visible': True if main_loc_name == 'dividend' else 'legendonly'
            }
        ]

        # 添加移动平均线
        for line_option in line_options:
            if line_option in ['MA5', 'MA20', 'MA60', 'MA120']:
                fund_nav[line_option] = get_fund_data_by_name(fund_nav, line_option, main_loc_name)
                data.append({
                    'x': fund_nav['nav_date'],
                    'y': fund_nav[line_option],
                    'type': 'line',
                    'name': line_option
                })
            elif line_option == 'drawdown':
                # 计算回撤
                drawdown_list = calculate_max_drawdown(fund_nav['nav_date'], fund_nav[main_loc_name], start_date, end_date)
                
                # 绘制回撤区域
                for i in range(len(drawdown_list)):
                    if pd.notna(drawdown_list[i]):
                        drawdown_value = drawdown_list[i]['value']
                        drawdown_start_date = drawdown_list[i]['start_date']
                        drawdown_end_date = drawdown_list[i]['end_date']
                        recovery_date = drawdown_list[i]['recovery_date']

                        drawdown_days = (drawdown_end_date - drawdown_start_date).days
                        recovery_days = (recovery_date - drawdown_end_date).days if recovery_date else None
                    
                        # 获取回撤起止日的净值
                        y_min = fund_nav.loc[fund_nav['nav_date'] == drawdown_start_date, main_loc_name].values[0]
                        y_max = fund_nav.loc[fund_nav['nav_date'] == drawdown_end_date, main_loc_name].values[0]
                        
                        text = f'回撤: {drawdown_value*100:.4f}%({drawdown_days} days)' 
                        if recovery_days:
                            text = f'{text}，修复：{recovery_days} days'
                        # 添加矩形区域
                        data.append({
                            'type': 'scatter',
                            'x': [drawdown_start_date, drawdown_end_date, drawdown_end_date, drawdown_start_date, drawdown_start_date],
                            'y': [y_min, y_min, y_max, y_max, y_min],
                            'fill': 'toself',
                            'fillcolor': 'rgba(255, 0, 0, 0.2)',
                            'line': {'width': 0},
                            'mode': 'lines+text',  # 添加文本模式
                            'text': [text],  # 显示回撤值
                            'textposition': 'top right',  # 文本位置
                            'textfont': {'size': 12, 'color': 'red'},  # 文本样式
                            'name': f'TOP{i+1} 回撤',
                            'showlegend': True
                        })
                        if recovery_days:
                            data.append({
                            'type': 'scatter',
                            'x': [drawdown_end_date, recovery_date, recovery_date, drawdown_end_date, drawdown_end_date],
                            'y': [y_min, y_min, y_max, y_max, y_min],
                            'fill': 'toself',
                            'fillcolor': 'rgba(0, 255, 0, 0.2)',
                            'line': {'width': 0},
                            'mode': 'lines+text',  # 添加文本模式
                            'textfont': {'size': 12, 'color': 'red'},  # 文本样式
                            'name': f'TOP{i+1} 回撤修复',
                            'showlegend': True
                        }) 
                        

        # 计算区间内的最大最小值
        filtered_nav = fund_nav
        if start_date:
            filtered_nav = filtered_nav[filtered_nav['nav_date'].dt.date >= start_date]
        if end_date:
            filtered_nav = filtered_nav[filtered_nav['nav_date'].dt.date <= end_date]
        
        # 计算y轴范围
        y_min = None
        y_max = None
        for col in [main_loc_name]:
            y_min = min(y_min, filtered_nav[col].min()) if y_min is not None else filtered_nav[col].min()
            y_max = max(y_max, filtered_nav[col].max()) if y_max is not None else filtered_nav[col].max()
            padding = (y_max - y_min) * 0.05
            y_min = y_min - padding
            y_max = y_max + padding

        # 创建figure
        figure = {
            'data': data,
            'layout': {
                'title': f'基金 {selected_fund} 的净值和分红数据',
                'xaxis': {
                    'title': '日期',
                    'tickformat': '%Y年%m月%d日',
                    'range': [start_date, end_date] if start_date and end_date else None
                },
                'yaxis': {
                    'title': '净值',
                    'range': [y_min, y_max] if y_min is not None and y_max is not None else None,
                    'autorange': True if y_min is None or y_max is None else None
                }
            }
        }

        return figure

    @app.callback(
        Output('fund-summary-table', 'children'),
        [Input('fund-dropdown', 'value'),
         Input('time-range-dropdown', 'value')]
    )
    def update_fund_summary(selected_fund, time_range):
        # 获取基金信息
        db_funds = DBFunds(mysql_db)
        fund_info = db_funds.get_fund_info(selected_fund)

        if fund_info is None or fund_info.empty:
            return html.Div("未找到基金信息", style={'color': 'red'})

        # 获取基金净值数据
        db_funds_nav = DBFundsNav(mysql_db)
        fund_nav = db_funds_nav.get_fund_nav(selected_fund)

        # 获取图表显示的日期范围
        start_date, end_date = get_date_range(time_range)

        # 计算区间收益率
        first_nav, last_nav, return_rate = calculate_return_rate(fund_nav, start_date, end_date)

        # 构建表格数据
        table_data = [
            ('基金代码', selected_fund),
            ('基金名称', fund_info.iloc[0]['name']),
            ('基金公司', fund_info.iloc[0]['management']),
            ('区间收益率', f'{return_rate:.3f}% ({first_nav:.4f} -> {last_nav:.4f})'),
        ]

        # 生成紧凑的布局
        children = []
        for label, value in table_data:
            children.append(
                html.Div([
                    html.Span(label, style={
                        'color': '#666',
                        'fontWeight': 'bold',
                        'marginRight': '5px',
                        'padding': '3px',
                        'backgroundColor': '#e0e0e0',  # 添加背景色
                        'borderRadius': '3px',  # 添加圆角
                    }),
                    html.Span(value, style={
                        'color': '#333',
                        'fontWeight': '500',  # 加粗显示
                        'padding': '3px',
                        'backgroundColor': '#f0f0f0',  # 添加背景色
                        'borderRadius': '3px',  # 添加圆角
                    })
                ], style={
                    'display': 'inline-block',
                    'marginRight': '10px',
                    'padding': '5px',
                    'border': '1px solid #ddd',  # 添加边框
                    'borderRadius': '3px',  # 添加圆角
                    'backgroundColor': '#f5f5f5',  # 添加背景色
                })
            )

        return html.Div(children, style={
            'padding': '2px',
            'border': '1px solid #ddd',
            'borderRadius': '5px',
            'backgroundColor': '#f9f9f9',
        }) 