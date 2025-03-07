import sys
import os
import dash
import pandas as pd
from dash import dcc, html
import plotly.graph_objs as go
from dash.dependencies import Input, Output
import mysql.connector
from mysql.connector import Error

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.db_funds_nav import DBFundsNav
from database.db_funds import DBFunds

def create_single_fund_value_graph(mysql_db):

    # 获取基金净值数据
    # db_funds_nav = DBFundsNav(mysql_db)
    db_funds = DBFunds(mysql_db)
    funds = db_funds.get_all_funds()  # 假设这个方法返回所有基金的列表

    # 检查 funds 是否为空
    if funds.empty:
        return html.H1("未找到基金数据")  # 返回一个提示信息

    # 默认选择的基金
    default_fund = funds.iloc[0]['ts_code']  # 使用 iloc 获取第一行的 ts_code

    layout = html.Div([
        html.Div([ # 基金选择
            dcc.Dropdown(
                id='fund-dropdown',
                options=[{'label': f"{fund['name']} ({fund['ts_code']})", 'value': fund['ts_code']} for index, fund in funds.iterrows()],
                value=default_fund,  # 默认选择的基金
                clearable=False,
                style={
                    'width': '350px',  # 设置宽度
                    'height': '38px',  # 设置高度
                    'font-size': '14px',  # 设置字体大小
                }
            ),
            dcc.Dropdown(
                id='time-range-dropdown',
                options=[
                    {'label': '近一个月', 'value': '1M'},
                    {'label': '近三个月', 'value': '3M'},
                    {'label': '近半年', 'value': '6M'},
                    {'label': '近一年', 'value': '1Y'},
                    {'label': '近三年', 'value': '3Y'},
                    {'label': '近五年', 'value': '5Y'},
                    {'label': '本季度', 'value': 'CQ'},
                    {'label': '本年度', 'value': 'CY'},
                    {'label': '所有', 'value': 'ALL'}
                ],
                value='1Y',  # 默认显示近一年
                clearable=False,
                style={
                    'width': '120px',  # 设置宽度
                    'height': '38px',  # 设置高度
                    'font-size': '14px',  # 设置字体大小
                    'margin-left': '10px'  # 左边距
                }
            ),
        ], style={
            'margin-bottom': '2px', 
            'display': 'flex', 
            'align-items': 'center',
            'gap': '10px'  # 添加组件之间的间距
        }),
        html.Div([ # 辅助线选择
            dcc.Checklist(
                id='line-options',
                options=[
                {'label': 'MA5', 'value': 'MA5'},
                {'label': 'MA20', 'value': 'MA20'},
                {'label': 'MA60', 'value': 'MA60'},
                {'label': 'MA120', 'value': 'MA120'},
                {'label': '回撤', 'value': 'drawdown'},
                # 可以添加更多的辅助线选项
            ],
            value=[],  # 默认不选中任何选项
            inline=True,  # 使选项在同一行显示
            style={'margin-top': '2px'}  # 添加一些顶部边距
            ),
        ], style={
            'margin-bottom': '2px', 
            'display': 'flex', 
            'align-items': 'center',
            'gap': '10px'  # 添加组件之间的间距
        }),
        html.Div([ # 基金数据汇总表格展示
            html.Table([
                html.Tbody(id='fund-summary-table')
            ], style={
                'width': '100%',
                'borderCollapse': 'collapse',
                'margin': '2px',
                'padding': '2px',
                'backgroundColor': 'white',
                'boxShadow': '0 1px 3px rgba(0,0,0,0.2)',
                'border': '1px solid #ddd'
            })
        ], style={
            'margin': '2px',
            'padding': '2px',
            'backgroundColor': '#f5f5f5',
            'borderRadius': '5px'
        }),
        html.Div([ # 图表
            dcc.Graph(
                id='fund-value-graph',
                config={'displayModeBar': True},  # 显示工具栏
                style={'height': '80vh', 'width': '100%'}  # 设置图表高度为 80% 的视口高度
            ),
        ], style={
            'margin-bottom': '2px', 
            'display': 'flex', 
            'align-items': 'center',
            'width': '100%',  # 设置宽度占满
            'gap': '10px'  # 添加组件之间的间距
        }),
    ], style={'height': '100%', 'display': 'flex', 'flexDirection': 'column'})  # 使用 flexbox 布局

    return layout 