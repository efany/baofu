from dash import html, dcc
import dash_bootstrap_components as dbc
from typing import List, Dict, Any
from loguru import logger
from database.db_funds import DBFunds
from database.db_strategys import DBStrategys
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils import get_data_briefs

def create_products_compare_page(mysql_db):
    """创建产品对比页面"""

    layout = dbc.Container([
        # 页面标题
        dbc.Row([
            dbc.Col([
                html.H1("产品对比分析", className="text-center mb-4", 
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
                        html.Label("📈 基金:", className="form-label fw-bold"),
                        dcc.Dropdown(
                            id='fund-dropdown',
                            multi=True,
                            placeholder='选择基金进行对比...',
                            style={'minHeight': '38px'}
                        )
                    ], width=6),
                    
                    dbc.Col([
                        html.Label("📊 股票:", className="form-label fw-bold"),
                        dcc.Dropdown(
                            id='stock-dropdown',
                            multi=True,
                            placeholder='选择股票进行对比...',
                            style={'minHeight': '38px'}
                        )
                    ], width=6)
                ], className="mb-3"),
                
                dbc.Row([
                    dbc.Col([
                        html.Label("💱 外汇:", className="form-label fw-bold"),
                        dcc.Dropdown(
                            id='forex-dropdown',
                            multi=True,
                            placeholder='选择外汇进行对比...',
                            style={'minHeight': '38px'}
                        )
                    ], width=6),
                    
                    dbc.Col([
                        html.Label("🎯 策略:", className="form-label fw-bold"),
                        dcc.Dropdown(
                            id='strategy-dropdown',
                            multi=True,
                            placeholder='选择策略进行对比...',
                            style={'minHeight': '38px'}
                        )
                    ], width=6)
                ])
            ])
        ], className="mb-4"),
        
        # 分析设置区域
        dbc.Card([
            dbc.CardHeader([
                html.H4("分析设置", className="mb-0", style={'color': '#34495e'})
            ]),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.Label("时间范围:", className="form-label fw-bold"),
                        dcc.Dropdown(
                            id='compare-time-range',
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
                    ], width=3),
                    
                    dbc.Col([
                        html.Label("开始日期:", className="form-label fw-bold"),
                        dcc.DatePickerSingle(
                            id='compare-start-date',
                            disabled=True,
                            display_format='YYYY-MM-DD',
                            style={'width': '100%'}
                        )
                    ], width=3, id='start-date-col', style={'display': 'none'}),
                    
                    dbc.Col([
                        html.Label("结束日期:", className="form-label fw-bold"),
                        dcc.DatePickerSingle(
                            id='compare-end-date',
                            disabled=True,
                            display_format='YYYY-MM-DD',
                            style={'width': '100%'}
                        )
                    ], width=3, id='end-date-col', style={'display': 'none'}),
                    
                    dbc.Col([
                        html.Label("技术指标:", className="form-label fw-bold"),
                        dbc.Checklist(
                            id='compare-line-options',
                            options=[
                                {'label': 'MA5', 'value': 'MA5'},
                                {'label': 'MA20', 'value': 'MA20'},
                                {'label': 'MA60', 'value': 'MA60'},
                                {'label': 'MA120', 'value': 'MA120'},
                                {'label': '回撤分析', 'value': 'drawdown'}
                            ],
                            value=[],
                            inline=True,
                            switch=True
                        )
                    ], width=6),
                    
                    dbc.Col([
                        html.Div([
                            dbc.Button(
                                "🔍 开始对比",
                                id='compare-confirm-button',
                                color="primary",
                                size="lg",
                                n_clicks=0,
                                className="w-100"
                            )
                        ], className="d-grid")
                    ], width=3, className="d-flex align-items-end")
                ])
            ])
        ], className="mb-4"),
        
        # 产品摘要区域
        html.Div(
            id='products-summary-section',
            style={'display': 'none'}
        ),

        # 图表区域
        dbc.Card([
            dbc.CardHeader([
                html.H4("价格走势对比", className="mb-0", style={'color': '#34495e'})
            ]),
            dbc.CardBody([
                dcc.Graph(
                    id='compare-value-graph',
                    style={'height': '600px'},
                    config={'displayModeBar': True, 'displaylogo': False}
                )
            ])
        ], id='chart-section', className="mb-4", style={'display': 'none'}),
            
        # 相关性分析区域
        dbc.Card([
            dbc.CardHeader([
                html.H4("相关性分析", className="mb-0", style={'color': '#34495e'})
            ]),
            dbc.CardBody([
                html.Div(id='correlation-matrix-container')
            ])
        ], id='correlation-section', className="mb-4", style={'display': 'none'}),
        
        # 统计指标表格区域
        html.Div(
            id='tables-section',
            style={'display': 'none'}
        )
    ], fluid=True)
    
    return layout