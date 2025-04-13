from dash import html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.db_strategys import DBStrategys
from database.mysql_database import MySQLDatabase

def create_strategy_form():
    """创建策略表单"""
    return dbc.Form([
        dbc.Row([
            dbc.Col([
                dbc.Label("策略名称"),
                dbc.Input(id="strategy-name-input", type="text", placeholder="输入策略名称")
            ], width=6),
            dbc.Col([
                dbc.Label("初始资金"),
                dbc.Input(id="strategy-cash-input", type="number", placeholder="输入初始资金")
            ], width=6),
        ], className="mb-3"),
        
        dbc.Row([
            dbc.Col([
                dbc.Label("策略描述"),
                dbc.Textarea(id="strategy-description-input", placeholder="输入策略描述")
            ], width=12),
        ], className="mb-3"),
        
        dbc.Row([
            dbc.Col([
                dbc.Label("数据参数"),
                dbc.Textarea(
                    id="strategy-data-params-input",
                    value="""
                    {
                        "fund_codes": ["007540","003376"]
                    }
                    """,
                    rows=4
                )
            ], width=12),
        ], className="mb-3"),
        
        dbc.Row([
            dbc.Col([
                dbc.Label("策略参数"),
                dbc.Textarea(
                    id="strategy-parameters-input",
                    value="""
                    {
                        "rebalance_period": 20,
                        "position_size": 50,
                        "ma_periods": ["MA20", "MA60"],
                        "show_drawdown": "top3"
                    }
                    """,
                    rows=6,
                    placeholder="输入策略参数（JSON格式）"
                )
            ], width=12),
        ], className="mb-3"),
        
        dbc.Row([
            dbc.Col([
                dbc.Label("策略配置"),
                dbc.Textarea(
                    id="strategy-config-input",
                    value="""
                    {
                        "name": "BuyAndHold",
                        "open_date": "<open_date>",
                        "close_date": "<close_date>",
                        "dividend_method": "reinvest",
                        "products": ["007540","003376"],
                        "weights": [0.5,0.5]
                    }
                    """,
                    rows=8
                )
            ], width=12),
        ], className="mb-3"),
    ])

def create_strategy_management(mysql_db: MySQLDatabase):
    db_strategys = DBStrategys(mysql_db)
    strategies = db_strategys.get_all_strategies()

    """创建策略管理界面"""
    return html.Div([
        dbc.Row([
            dbc.Col([
                html.H2("策略管理", className="mb-4"),
                dbc.ButtonGroup([
                    dbc.Button("新建策略", id="new-strategy-btn", color="primary", className="me-2"),
                    dbc.Button("删除策略", id="delete-strategy-btn", color="danger", className="me-2"),
                    dbc.Button("保存修改", id="save-strategy-btn", color="success"),
                ], className="mb-3"),
            ])
        ]),
        dbc.Row([
            dbc.Col([
                dbc.Label("选择策略"),
                dcc.Dropdown(
                    id='strategy-selector',
                    placeholder='选择要管理的策略',
                    options=[
                        {'label': '新建策略', 'value': -1},
                        *[{'label': f"{strategy['name']} (id:{int(strategy['strategy_id'])})", 
                           'value': int(strategy['strategy_id'])} 
                          for index, strategy in strategies.iterrows()]
                    ],
                    value='new',
                    className="mb-3"
                ),
            ], width=12),
        ]),
        dbc.Row([
            dbc.Col([
                html.Div(id="strategy-form-container", children=[
                    create_strategy_form()
                ]),
                dbc.Alert(
                    id="strategy-message",
                    is_open=False,
                    duration=3000,  # 3秒后自动关闭
                    className="mt-3"
                ),
            ])
        ]),
    ], className="p-4")
