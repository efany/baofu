import sys
import os
import dash
import pandas as pd
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
from dash.dependencies import Input, Output
import mysql.connector
from mysql.connector import Error

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_dash.utils import get_data_briefs

def create_single_product_value_graph(mysql_db, data_type):
    """创建单个产品价值图表页面"""
    return dbc.Container([
        # 页面标题
        dbc.Row([
            dbc.Col([
                html.H1("产品分析", className="text-center mb-4", 
                       style={'color': '#2c3e50', 'fontWeight': 'bold'})
            ])
        ]),
        
        # 产品选择区域
        dbc.Card([
            dbc.CardHeader([
                html.H4("产品选择", className="mb-0", style={'color': '#34495e'})
            ]),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.Label("产品类型:", className="form-label fw-bold"),
                        dcc.Dropdown(
                            id='type-dropdown',
                            options=[
                                {'label': '📈 基金', 'value': 'fund'},
                                {'label': '🎯 策略', 'value': 'strategy'},
                                {'label': '📊 股票', 'value': 'stock'},
                                {'label': '💱 外汇', 'value': 'forex'},
                                {'label': '🏦 国债收益率', 'value': 'bond_yield'},
                                {'label': '📊 指数', 'value': 'index'}
                            ],
                            value=data_type,
                            clearable=False
                        )
                    ], width=3),
                    
                    dbc.Col([
                        html.Label("选择产品:", className="form-label fw-bold"),
                        dcc.Dropdown(
                            id='product-dropdown',
                            placeholder="请先选择产品类型"
                        )
                    ], width=6),
                    
                    dbc.Col([
                        html.Label("时间范围:", className="form-label fw-bold"),
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
                                {'label': '所有数据', 'value': 'ALL'},
                                {'label': '自定义', 'value': 'custom'}
                            ],
                            value='1Y',
                            clearable=False
                        )
                    ], width=3)
                ], className="mb-3"),
                
                # 自定义时间选择区域（默认隐藏）
                dbc.Row([
                    dbc.Col([
                        html.Label("开始日期:", className="form-label fw-bold"),
                        dcc.DatePickerSingle(
                            id='start-date-picker',
                            disabled=True,
                            first_day_of_week=1,
                            display_format='YYYY-MM-DD',
                            clearable=False,
                            style={'width': '100%'}
                        )
                    ], width=6),
                    
                    dbc.Col([
                        html.Label("结束日期:", className="form-label fw-bold"),
                        dcc.DatePickerSingle(
                            id='end-date-picker',
                            disabled=True,
                            first_day_of_week=1,
                            display_format='YYYY-MM-DD',
                            clearable=False,
                            style={'width': '100%'}
                        )
                    ], width=6)
                ], id='custom-date-row', style={'display': 'none'})
            ])
        ], className="mb-4"),
        
        # 参数配置区域
        dbc.Card([
            dbc.CardHeader([
                html.H4("参数配置", className="mb-0", style={'color': '#34495e'})
            ]),
            dbc.CardBody(
                id='params-config-container',
                children=[html.P("请先选择产品", className="text-muted")]
            )
        ], className="mb-4"),
        
        # 分析选项和查询按钮
        dbc.Card([
            dbc.CardHeader([
                html.H4("分析选项", className="mb-0", style={'color': '#34495e'})
            ]),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.Label("技术指标:", className="form-label fw-bold"),
                        dbc.Checklist(
                            id='line-options',
                            options=[
                                {'label': 'MA5 (5日均线)', 'value': 'MA5'},
                                {'label': 'MA20 (20日均线)', 'value': 'MA20'},
                                {'label': 'MA60 (60日均线)', 'value': 'MA60'},
                                {'label': 'MA120 (120日均线)', 'value': 'MA120'},
                                {'label': '回撤分析', 'value': 'drawdown'},
                            ],
                            value=[],
                            inline=True,
                            switch=True
                        )
                    ], width=8),
                    
                    dbc.Col([
                        html.Div([
                            dbc.Button(
                                "🔍 开始分析",
                                id='query-button',
                                color="primary",
                                size="lg",
                                n_clicks=0,
                                className="w-100"
                            )
                        ], className="d-grid")
                    ], width=4, className="d-flex align-items-end")
                ])
            ])
        ], className="mb-4"),
        
        # 产品摘要展示区域
        html.Div(
            id='product-summary-section',
            style={'display': 'none'}
        ),
        
        # 图表展示区域
        dbc.Card([
            dbc.CardHeader([
                html.H4("价格走势图", className="mb-0", style={'color': '#34495e'})
            ]),
            dbc.CardBody([
                dcc.Graph(
                    id='product-value-graph',
                    style={'height': '600px'},
                    config={'displayModeBar': True, 'displaylogo': False}
                )
            ])
        ], id='single-product-chart-section', className="mb-4", style={'display': 'none'}),
        
        # 详细数据表格区域
        html.Div(
            id='single-product-tables-section',
            style={'display': 'none'}
        )
    ], fluid=True) 