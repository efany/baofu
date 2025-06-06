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
    """创建单个产品价值图表页面"""
    return html.Div([
        # 数据选择区域
        html.Div([
            html.Div([
                dcc.Dropdown(
                    id='type-dropdown',
                    options=[
                        {'label': '基金', 'value': 'fund'},
                        {'label': '策略', 'value': 'strategy'},
                        {'label': '股票', 'value': 'stock'},
                        {'label': '外汇', 'value': 'forex'},
                        {'label': '国债收益率', 'value': 'bond_yield'}
                    ],
                    value=data_type,
                    style={'width': '150px'}
                )
            ], style={'marginRight': '20px'}),
            
            html.Div([
                dcc.Dropdown(
                    id='product-dropdown',
                    style={'width': '300px'}
                )
            ], style={'marginRight': '20px'}),
            
            html.Div([
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
                        {'label': '所有', 'value': 'ALL'},
                        {'label': '自定义', 'value': 'custom'}  # 添加自定义选项
                    ],
                    value='1Y',  # 默认显示近一年
                    clearable=False,
                    style={'width': '150px'}
                )
            ], style={'marginRight': '10px'}),
            
            # 添加时间控件
            html.Div([
                html.Span('开始时间：', style={'marginRight': '5px'}),
                dcc.DatePickerSingle(
                    id='start-date-picker',
                    style={'width': '200px', 'display': 'inline-block'},
                    disabled=True,  # 默认禁用，当选择"自定义"时启用
                    first_day_of_week=1,  # 设置周一为一周的第一天
                    display_format='YYYY年MM月DD日',  # 设置显示格式
                    month_format='YYYY年MM月',  # 设置月份显示格式
                    clearable=False,  # 禁止清除日期
                )
            ], style={'marginRight': '10px', 'display': 'inline-block'}),
            
            html.Div([
                html.Span('结束时间：', style={'marginRight': '5px'}),
                dcc.DatePickerSingle(
                    id='end-date-picker',
                    style={'width': '150px', 'display': 'inline-block'},
                    disabled=True,  # 默认禁用，当选择"自定义"时启用
                    first_day_of_week=1,  # 设置周一为一周的第一天
                    display_format='YYYY年MM月DD日',  # 设置显示格式
                    month_format='YYYY年MM月',  # 设置月份显示格式
                    clearable=False,  # 禁止清除日期
                )
            ], style={'display': 'inline-block'})
        ], style={
            'display': 'flex',
            'alignItems': 'flex-end',
            'marginBottom': '20px',
            'gap': '10px'
        }),
        
        # 参数配置区域
        html.Div(
            id='params-config-container',
            style={
                'marginBottom': '20px',
                'padding': '15px',
                'border': '1px solid #e8e8e8',
                'borderRadius': '4px',
                'backgroundColor': '#fafafa'
            }
        ),
        
        # 查询按钮和辅助线选项
        html.Div([
            html.Button(
                '查询',
                id='query-button',
                n_clicks=0,
                style={
                    'marginRight': '20px',
                    'padding': '5px 15px',
                    'backgroundColor': '#1890ff',
                    'color': 'white',
                    'border': 'none',
                    'borderRadius': '4px',
                    'cursor': 'pointer'
                }
            ),
            dcc.Checklist(
                id='line-options',
                options=[
                    {'label': 'MA5', 'value': 'MA5'},
                    {'label': 'MA20', 'value': 'MA20'},
                    {'label': 'MA60', 'value': 'MA60'},
                    {'label': 'MA120', 'value': 'MA120'},
                    {'label': '回撤', 'value': 'drawdown'},
                ],
                value=[],
                inline=True,  # 使选项在同一行显示
                style={'margin-top': '2px'}  # 添加一些顶部边距
            ),
        ], style={
            'margin-bottom': '2px', 
            'display': 'flex', 
            'align-items': 'center',
            'gap': '10px'  # 添加组件之间的间距
        }),
        
        # 产品摘要表格
        html.Div(
            id='product-summary-table',
            style={'marginBottom': '20px'}
        ),
        
        # 产品价值图表
        dcc.Graph(
            id='product-value-graph',
            style={'height': '500px'}
        ),
        
        # 产品详细数据表格
        html.Div([
            html.Div(
                id='product-tables-left-column',
                style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'}
            ),
            html.Div(
                id='product-tables-right-column',
                style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top', 'marginLeft': '4%'}
            )
        ])
    ]) 