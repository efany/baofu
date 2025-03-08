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

from task_dash.utils import get_data_briefs

def create_single_product_value_graph(mysql_db, data_type):

    layout = html.Div([
        html.Div([ # 数据选择区域
            # 添加类型选择下拉框
            dcc.Dropdown(
                id='type-dropdown',
                options=[
                    {'label': '基金', 'value': 'fund'},
                    {'label': '策略', 'value': 'strategy'},
                    {'label': '股票', 'value': 'stock'}
                ],
                value=data_type,  # 默认选择传入的类型
                clearable=False,
                style={
                    'width': '120px',  # 设置宽度
                    'height': '38px',  # 设置高度
                    'font-size': '14px',  # 设置字体大小
                }
            ),
            # 数据选择下拉框
            dcc.Dropdown(
                id='product-dropdown',
                options=[],
                value='',
                clearable=False,
                style={
                    'width': '350px',  # 设置宽度
                    'height': '38px',  # 设置高度
                    'font-size': '14px',  # 设置字体大小
                    'margin-left': '10px'  # 左边距
                }
            ),
            # 时间范围选择
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
                html.Tbody(id='product-summary-table')
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
                id='product-value-graph',
                config={'displayModeBar': True},
                style={'height': '80vh', 'width': '100%'}
            ),
        ], style={
            'margin-bottom': '10px', 
            'display': 'flex', 
            'align-items': 'center',
            'width': '100%',
            'gap': '10px'
        }),
        
        # 添加两列表格展示区域
        html.Div([
            # 左列
            html.Div(
                id='product-tables-left-column',
                style={
                    'flex': '1',
                    'margin-right': '10px'
                }
            ),
            # 右列
            html.Div(
                id='product-tables-right-column',
                style={
                    'flex': '1',
                    'margin-left': '10px'
                }
            )
        ], style={
            'display': 'flex',
            'justifyContent': 'space-between',
            'marginTop': '20px',
            'width': '100%'
        })
        
    ], style={'height': '100%', 'display': 'flex', 'flexDirection': 'column'})

    return layout 