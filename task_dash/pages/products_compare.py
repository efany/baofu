from dash import html, dcc
import dash_bootstrap_components as dbc
from typing import List, Dict, Any
from loguru import logger
from database.db_funds import DBFunds
from database.db_strategys import DBStrategys
from task_dash.utils import get_data_briefs

def create_products_compare_page(mysql_db):
    """创建产品对比页面"""

    layout = html.Div([
        # 顶部选择区域
        html.Div([
            # 产品选择区域
            html.Div([
                # 基金选择
                html.Div([
                    html.Label('基金', style={'width': '50px'}),  # 固定标签宽度
                    dcc.Dropdown(
                        id='fund-dropdown',
                        multi=True,  # 允许多选
                        placeholder='选择基金...',
                        style={'flex': 1}  # 占用剩余空间
                    ),
                ], style={
                    'display': 'flex',
                    'align-items': 'center',
                    'margin-bottom': '10px',
                    'width': '100%'  # 容器占满宽度
                }),
                
                # 股票选择
                html.Div([
                    html.Label('股票', style={'width': '50px'}),  # 固定标签宽度
                    dcc.Dropdown(
                        id='stock-dropdown',
                        multi=True,  # 允许多选
                        placeholder='选择股票...',
                        style={'flex': 1}  # 占用剩余空间
                    ),
                ], style={
                    'display': 'flex',
                    'align-items': 'center',
                    'margin-bottom': '10px',
                    'width': '100%'  # 容器占满宽度
                }),

                # 外汇选择
                html.Div([
                    html.Label('外汇', style={'width': '50px'}),  # 固定标签宽度
                    dcc.Dropdown(
                        id='forex-dropdown',
                        multi=True,  # 允许多选
                        placeholder='选择外汇...',
                        style={'flex': 1}  # 占用剩余空间
                    ),
                ], style={
                    'display': 'flex',
                    'align-items': 'center',
                    'margin-bottom': '10px',
                    'width': '100%'  # 容器占满宽度
                }),

                # 策略选择
                html.Div([
                    html.Label('策略', style={'width': '50px'}),  # 固定标签宽度
                    dcc.Dropdown(
                        id='strategy-dropdown',
                        multi=True,  # 允许多选
                        placeholder='选择策略...',
                        style={'flex': 1}  # 占用剩余空间
                    ),
                ], style={
                    'display': 'flex',
                    'align-items': 'center',
                    'margin-bottom': '10px',
                    'width': '100%'  # 容器占满宽度
                }),
            ], style={
                'width': '100%'  # 产品选择区域占满宽度
            }),
            
            # 时间和指标选择区域
            html.Div([
                # 时间范围选择
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
                        {'label': '所有', 'value': 'ALL'},
                        {'label': '自定义', 'value': 'custom'}  # 添加自定义选项
                    ],
                    value='1Y',
                    clearable=False,
                    style={'width': '120px'}
                ),
                # 添加日期选择器
                html.Div([
                    dcc.DatePickerSingle(
                        id='compare-start-date',
                        placeholder='开始日期',
                        disabled=True,  # 初始状态禁用
                        style={'width': '130px', 'marginLeft': '10px'}
                    ),
                    dcc.DatePickerSingle(
                        id='compare-end-date',
                        placeholder='结束日期',
                        disabled=True,  # 初始状态禁用
                        style={'width': '130px', 'marginLeft': '10px'}
                    ),
                ], style={'display': 'inline-flex', 'alignItems': 'center'}),
                # 辅助线选择
                dcc.Checklist(
                    id='compare-line-options',
                    options=[
                        {'label': 'MA5', 'value': 'MA5'},
                        {'label': 'MA20', 'value': 'MA20'},
                        {'label': 'MA60', 'value': 'MA60'},
                        {'label': 'MA120', 'value': 'MA120'},
                        {'label': '回撤', 'value': 'drawdown'}
                    ],
                    value=[],
                    inline=True,
                    style={'margin-left': '20px'}
                ),
                # 确认按钮
                html.Button(
                    '确认',
                    id='compare-confirm-button',
                    n_clicks=0,
                    style={
                        'marginLeft': '20px',
                        'padding': '5px 15px',
                        'backgroundColor': '#1890ff',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '4px',
                        'cursor': 'pointer',
                        'height': '32px'  # 与下拉框高度保持一致
                    }
                ),
            ], style={
                'display': 'flex',
                'align-items': 'center',
                'margin-bottom': '10px'
            }),
        ], style={
            'width': '100%',  # 顶部选择区域占满宽度
            'padding': '0 20px'  # 添加左右内边距
        }),
        
        # 产品摘要区域
        html.Div(
            id='products-summary',
            style={
                'margin': '0px 0',
                'padding': '0px',
            }
        ),

        # 图表区域
        html.Div([
            dcc.Graph(
                id='compare-value-graph',
                style={'height': '60vh'}
            ),
            
            # 相关系数展示区域
            html.Div([
                html.H5("产品相关性分析", className="text-center mt-4"),
                html.Div(id='correlation-matrix-container', className="mt-2")
            ], style={
                'margin': '20px 0',
                'padding': '0 20px'
            })
        ]),
        
        # 统计指标表格区域
        html.Div([
            html.Div(id='compare-tables-container', style={
                'display': 'flex',
                'flex-wrap': 'wrap',
                'gap': '20px',
                'margin-top': '20px'
            })
        ])
    ])
    
    return layout 