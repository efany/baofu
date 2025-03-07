import sys
import os
import pandas as pd
from dash import html, dcc
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_backtrader.backtrader_buy_and_hold_task import BacktraderBuyAndHoldTask
from database.db_strategys import DBStrategys
from task_utils.funds_utils import get_fund_data_by_name, calculate_return_rate, calculate_max_drawdown, calculate_adjusted_nav

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

def register_strategy_callbacks(app, mysql_db):

    db_strategys = DBStrategys(mysql_db)

    @app.callback(
        [Output('strategy-value-graph', 'figure'),
         Output('strategy-trades-table', 'children'),
         Output('strategy-summary-table', 'children'),
         Output('strategy-performance-table', 'children')],
        [Input('strategy-dropdown', 'value'),
         Input('strategy-time-range-dropdown', 'value'),
         Input('strategy-line-options', 'value')]
    )
    def update_strategy_graph(strategy_id, time_range, line_options):
        try:
            strategys = db_strategys.get_strategy(strategy_id)
            if strategys.empty:
                return {}, [], [], []
            strategy = strategys.iloc[0]
            print(f"{strategy}")
            # 获取图表显示的日期范围
            open_date, close_date = get_date_range(time_range)
            strategy['strategy'] = strategy['strategy'] \
                .replace("<open_date>", open_date.strftime("%Y-%m-%d") if open_date else "") \
                .replace("<close_date>", close_date.strftime("%Y-%m-%d") if close_date else "")
            task = BacktraderBuyAndHoldTask(mysql_db, strategy)
            task.execute()

            if not task.is_success:
                print(f"Error in BacktraderBuyAndHoldTask execute: {str(e)}")
                return {}, [], [], []
            result = task.result

            return (
                create_strategy_figure(result, line_options, open_date, close_date),
                create_trades_table(result),
                create_summary_table(strategy, result),
                calculate_performance_metrics(result, open_date, close_date)
            )

        except Exception as e:
            print(f"Error in update_strategy_graph: {str(e)}")
            return {}, [], [], []

    def calculate_performance_metrics(result, start_date=None, end_date=None):
        """计算策略表现指标"""
        data = {
            '收益率': f"{result['return_rate']:.3f}%",
        }
        rows = []
        for key, value in data.items():
            rows.append(html.Tr([
                html.Td(key),
                html.Td(value)
            ]))
        return rows

def create_strategy_figure(result, line_options, open_date, close_date):
    daily_asset = result['daily_asset']
    print(daily_asset[:10])

    df = pd.DataFrame(daily_asset)
    df['date'] = pd.to_datetime(df['date'])

    # 创建基础数据列表
    data = []
    
    # 添加总资产曲线
    data.append({
        'x': df['date'],
        'y': df['total'],
        'type': 'line',
        'name': '总资产',
        'line': {'width': 2},  # 加粗总资产线
    })
    
    # 添加每个产品的资产曲线
    if 'products' in df.iloc[0]:
        # 获取所有产品代码
        product_codes = list(df.iloc[0]['products'].keys())
        
        # 为每个产品创建资产序列
        for product_code in product_codes:
            product_values = []
            for _, row in df.iterrows():
                product_values.append(row['products'].get(product_code, 0))
            
            # 添加产品资产曲线
            data.append({
                'x': df['date'],
                'y': product_values,
                'type': 'line',
                'name': f'{product_code}资产',
                'line': {'dash': 'solid'},  # 使用实线
                'visible': 'legendonly'
            })
    
    # 添加移动平均线
    main_loc_name = 'total'
    for line_option in line_options:
        if line_option in ['MA5', 'MA20', 'MA60', 'MA120']:
            df[line_option] = get_fund_data_by_name(df, line_option, main_loc_name)
            data.append({
                'x': df['date'],
                'y': df[line_option],
                'type': 'line',
                'name': line_option
            })
        elif line_option == 'drawdown':
            # 计算回撤
            drawdown_list = calculate_max_drawdown(df['date'], df[main_loc_name], open_date, close_date)
            print(drawdown_list)
            
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
                    y_min = df.loc[df['date'] == drawdown_start_date, main_loc_name].values[0]
                    y_max = df.loc[df['date'] == drawdown_end_date, main_loc_name].values[0]
                    
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
    
    layout = go.Layout(
        title='策略回测结果',
        xaxis=dict(
            title='日期',
            tickformat='%Y-%m-%d',
        ),
        yaxis=dict(
            title='资产值',
            tickformat=',.2f',
            gridcolor='lightgrey'  # 添加网格线
        ),
        legend=dict(
            orientation="h",  # 水平放置图例
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode='x unified',  # 统一显示hover信息
        plot_bgcolor='white'  # 设置白色背景
    )
    
    return {
        'data': data,
        'layout': layout
    }

def create_trades_table(result):
    """创建交易记录表格"""
    if 'trades' not in result:
        return html.Div("无交易记录")
    print(result['trades'])
        
    trades = pd.DataFrame(result['trades'])
    if trades.empty:
        return html.Div("无交易记录")

    # 创建表头
    header = html.Thead(html.Tr([
        html.Th('日期', style={'width': '15%'}),
        html.Th('交易', style={'width': '10%'}),
        html.Th('产品代码', style={'width': '10%'}),
        html.Th('数量', style={'width': '10%'}),
        html.Th('价格', style={'width': '10%'}),
        html.Th('金额', style={'width': '10%'}),
        html.Th('说明', style={'width': '10%'})
    ]))

    # 创建表格行
    rows = []
    for _, trade in trades.iterrows():
        amount = trade['price'] * trade['size']
        type_style = {
            'color': 'green' if trade['type'] == 'buy' else 'red',
            'fontWeight': 'bold'
        }
        
        rows.append(html.Tr([
            html.Td(trade['date'].strftime('%Y-%m-%d')),
            html.Td('买入' if trade['type'] == 'buy' else '卖出', style=type_style),
            html.Td(trade['product']),
            html.Td(f"{trade['size']:,.0f}"),
            html.Td(f"¥{trade['price']:.4f}"),
            html.Td(f"¥{amount:,.2f}"),
            html.Td(trade.get('order_message', ''))
        ]))

    # 创建表格体
    body = html.Tbody(rows)

    # 返回完整的表格
    return html.Table(
        [header, body],
        style={
            'width': '100%',
            'borderCollapse': 'collapse',
            'backgroundColor': 'white',
            'textAlign': 'center',
            'fontSize': '14px'
        }
    )

def create_summary_table(strategys, result):
    """创建策略摘要表格
    
    Args:
        strategys: 策略基本信息
        result: 策略回测结果
    """
    # 计算关键指标
    daily_asset = pd.DataFrame(result['daily_asset'])
    initial_value = daily_asset.iloc[0]['total']
    final_value = daily_asset.iloc[-1]['total']
    total_return = (final_value - initial_value) / initial_value * 100
    
    # 构建表格数据
    table_data = [
        ('策略名称', strategys['name']),
        ('策略描述', strategys['description']),
        ('初始资金', f"¥{initial_value:,.2f}"),
        ('最终资金', f"¥{final_value:,.2f}"),
        ('总收益率', f"{total_return:+.2f}%"),
    ]

    # 生成紧凑的布局
    children = []
    for label, value in table_data:
        children.append(
            html.Div([
                # 标签
                html.Span(label, style={
                    'color': '#666',
                    'fontWeight': 'bold',
                    'padding': '4px 8px',
                    'backgroundColor': '#e0e0e0',
                    'borderRadius': '4px',
                    'marginRight': '8px',
                    'display': 'inline-block',
                    'minWidth': '80px',
                    'textAlign': 'right'
                }),
                # 值
                html.Span(value, style={
                    'color': '#333',
                    'padding': '4px 8px',
                    'backgroundColor': '#f0f0f0',
                    'borderRadius': '4px',
                    'display': 'inline-block',
                    'flex': '1',
                    'fontWeight': '500'
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