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

from database.db_strategys import DBStrategys
def create_strategy_graph(mysql_db):
    # 默认选择的基金
    db_strategys = DBStrategys(mysql_db)
    strategies = db_strategys.get_all_strategies()
    default_strategy = strategies.iloc[0]['strategy_id']

    layout = html.Div([
        html.Div([ # 基金选择
            dcc.Dropdown(
                id='strategy-dropdown',
                options=[{'label': f"{strategy['name']} (id:{int(strategy['strategy_id'])})", 
                           'value': int(strategy['strategy_id'])} for index, strategy in strategies.iterrows()],
                value=default_strategy,
                clearable=False,
                style={
                    'width': '350px',  # 设置宽度
                    'height': '38px',  # 设置高度
                    'font-size': '14px',  # 设置字体大小
                }
            ),
            dcc.Dropdown(
                id='strategy-time-range-dropdown',
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
                id='strategy-line-options',
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
                html.Tbody(id='strategy-summary-table')
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
        html.Div([ # 策略净值图表
            dcc.Graph(
                id='strategy-value-graph',
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
        
        html.Div([ # 交易记录和策略表现的容器
            html.Div([ # 交易记录表格
                html.H3('交易记录', style={
                    'margin': '10px 0',
                    'color': '#333',
                    'textAlign': 'center'
                }),
                html.Div(id='strategy-trades-table')
            ], style={
                'margin': '10px',
                'padding': '10px',
                'backgroundColor': 'white',
                'borderRadius': '5px',
                'boxShadow': '0 1px 3px rgba(0,0,0,0.1)',
                'width': '50%'  # 调整宽度以适应两个表格
            }),
            
            html.Div([ # 策略表现表格
                html.H3('策略表现', style={
                    'margin': '10px 0',
                    'color': '#333',
                    'textAlign': 'center'
                }),
                html.Table([
                    html.Thead(
                        html.Tr([
                            html.Th('指标', style={'width': '30%', 'textAlign': 'left', 'padding': '8px'}),
                            html.Th('数值', style={'width': '70%', 'textAlign': 'left', 'padding': '8px'})
                        ])
                    ),
                    html.Tbody(id='strategy-performance-table')
                ], style={
                    'width': '100%',
                    'borderCollapse': 'collapse',
                })
            ], style={
                'margin': '10px',
                'padding': '10px',
                'backgroundColor': 'white',
                'borderRadius': '5px',
                'boxShadow': '0 1px 3px rgba(0,0,0,0.1)',
                'width': '50%'  # 调整宽度以适应两个表格
            })
        ], style={
            'display': 'flex',
            'justifyContent': 'space-between',
            'width': '100%'
        })
        
    ], style={'height': '100%', 'display': 'flex', 'flexDirection': 'column'})

    return layout 