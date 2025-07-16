from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import json
import sys
import os
import pandas as pd
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.db_strategys import DBStrategys
from database.mysql_database import MySQLDatabase

def create_strategy_form_modal():
    """创建策略表单模态框"""
    return dbc.Modal([
        dbc.ModalHeader([
            html.H4(id="strategy-modal-title", children="新建策略"),
            dbc.Button("×", 
                id="close-strategy-modal", 
                className="btn-close", 
                n_clicks=0,
                style={"background": "none", "border": "none", "font-size": "1.5rem"}
            )
        ], close_button=False),
        
        dbc.ModalBody([
            dbc.Form([
                # 基本信息区域
                html.H5("基本信息", className="mb-3 text-primary"),
                dbc.Row([
                    dbc.Col([
                        dbc.Label("策略名称 *", className="fw-bold"),
                        dbc.Input(
                            id="strategy-name-input", 
                            type="text", 
                            placeholder="请输入策略名称",
                            value="",
                            disabled=False,
                            readonly=False,
                            required=True,
                            className="mb-2",
                            style={"pointerEvents": "auto", "userSelect": "text"}
                        ),
                        html.Div(id="name-feedback", className="invalid-feedback")
                    ], width=6),
                    dbc.Col([
                        dbc.Label("初始资金 *", className="fw-bold"),
                        dbc.InputGroup([
                            dbc.Input(
                                id="strategy-cash-input", 
                                type="number", 
                                placeholder="输入初始资金",
                                value=None,
                                disabled=False,
                                readonly=False,
                                min=1000,
                                step=1000,
                                required=True,
                                className="mb-2",
                                style={"pointerEvents": "auto", "userSelect": "text"}
                            ),
                            dbc.InputGroupText("元")
                        ]),
                        html.Div(id="cash-feedback", className="invalid-feedback")
                    ], width=6),
                ], className="mb-3"),
                
                dbc.Row([
                    dbc.Col([
                        dbc.Label("策略描述", className="fw-bold"),
                        dbc.Textarea(
                            id="strategy-description-input", 
                            placeholder="详细描述策略的目标、特点和适用场景",
                            value="",
                            disabled=False,
                            readonly=False,
                            rows=3,
                            className="mb-2",
                            style={"pointerEvents": "auto", "userSelect": "text"}
                        )
                    ], width=12),
                ], className="mb-4"),
                
                # 配置信息区域
                html.H5("配置信息", className="mb-3 text-primary"),
                
                # 使用标签页来组织复杂的JSON配置
                dbc.Tabs([
                    dbc.Tab(
                        dbc.Card(dbc.CardBody([
                            dbc.Label("数据参数", className="fw-bold"),
                            dbc.Textarea(
                                id="strategy-data-params-input",
                                value="""{
    "fund_codes": ["007540", "003376"],
    "start_date": "2020-01-01",
    "end_date": "2024-12-31"
}""",
                                disabled=False,
                                readonly=False,
                                rows=6,
                                className="strategy-json-field mb-2",
                                style={
                                    "font-family": "monospace",
                                    "pointerEvents": "auto", 
                                    "userSelect": "text"
                                }
                            ),
                            html.Div(id="data-params-feedback", className="invalid-feedback"),
                            dbc.FormText("JSON格式，定义策略所需的数据源参数", color="muted")
                        ]), className="mt-2"),
                        label="数据参数", 
                        tab_id="data-params-tab"
                    ),
                    dbc.Tab(
                        dbc.Card(dbc.CardBody([
                            dbc.Label("策略参数", className="fw-bold"),
                            dbc.Textarea(
                                id="strategy-parameters-input",
                                value="""{
    "rebalance_period": 20,
    "position_size": 50,
    "ma_periods": ["MA20", "MA60"],
    "show_drawdown": "top3",
    "risk_tolerance": "medium"
}""",
                                disabled=False,
                                readonly=False,
                                rows=8,
                                className="strategy-json-field mb-2",
                                style={
                                    "font-family": "monospace",
                                    "pointerEvents": "auto", 
                                    "userSelect": "text"
                                }
                            ),
                            html.Div(id="parameters-feedback", className="invalid-feedback"),
                            dbc.FormText("JSON格式，定义策略运行的具体参数", color="muted")
                        ]), className="mt-2"),
                        label="策略参数", 
                        tab_id="parameters-tab"
                    ),
                    dbc.Tab(
                        dbc.Card(dbc.CardBody([
                            dbc.Label("策略配置 *", className="fw-bold"),
                            dbc.Textarea(
                                id="strategy-config-input",
                                value="""{
    "name": "BuyAndHold",
    "open_date": "<open_date>",
    "close_date": "<close_date>",
    "dividend_method": "reinvest",
    "products": ["007540", "003376"],
    "weights": [0.5, 0.5]
}""",
                                disabled=False,
                                readonly=False,
                                rows=10,
                                className="strategy-json-field mb-2",
                                style={
                                    "font-family": "monospace",
                                    "pointerEvents": "auto", 
                                    "userSelect": "text"
                                },
                                required=True
                            ),
                            html.Div(id="config-feedback", className="invalid-feedback"),
                            dbc.FormText("JSON格式，定义策略的核心配置信息", color="muted")
                        ]), className="mt-2"),
                        label="策略配置", 
                        tab_id="config-tab"
                    )
                ], id="strategy-tabs", active_tab="data-params-tab")
            ])
        ]),
        
        dbc.ModalFooter([
            dbc.Button(
                [html.I(className="fas fa-times me-2"), "取消"], 
                id="cancel-strategy-btn", 
                color="secondary", 
                className="me-2"
            ),
            dbc.Button(
                [html.I(className="fas fa-save me-2"), "保存"], 
                id="save-strategy-btn", 
                color="primary"
            )
        ])
    ], 
    id="strategy-modal", 
    is_open=False, 
    size="xl",  # 使用超大模态框
    backdrop=True,
    keyboard=True,
    className="strategy-modal"
    )

def create_strategy_list_table(strategies_df: pd.DataFrame):
    """创建带操作按钮的策略列表表格"""
    if strategies_df.empty:
        return html.Div(
            dbc.Alert([
                html.I(className="fas fa-info-circle me-2"),
                "暂无策略数据，点击上方「新建策略」按钮创建第一个策略"
            ], color="info", className="text-center"),
            className="mt-3"
        )
    
    # 准备表格数据，添加操作列
    display_data = []
    for index, row in strategies_df.iterrows():
        display_data.append({
            'ID': int(row['strategy_id']),
            '策略名称': row['name'],
            '描述': row['description'][:60] + '...' if len(str(row['description'])) > 60 else str(row['description']) if pd.notna(row['description']) else '-',
            '初始资金': f"{row['initial_cash']:,.0f}元" if pd.notna(row['initial_cash']) else '-',
            '创建时间': row['create_time'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['create_time']) else '-',
            '更新时间': row['update_time'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['update_time']) else '-'
        })
    
    return html.Div([
        dash_table.DataTable(
            id='strategy-table',
            data=display_data,
            columns=[
                {'name': 'ID', 'id': 'ID', 'type': 'numeric'},
                {'name': '策略名称', 'id': '策略名称'},
                {'name': '描述', 'id': '描述'},
                {'name': '初始资金', 'id': '初始资金'},
                {'name': '创建时间', 'id': '创建时间'},
                {'name': '更新时间', 'id': '更新时间'}
            ],
            style_cell={
                'textAlign': 'left',
                'padding': '12px',
                'fontFamily': 'Arial, sans-serif',
                'fontSize': '14px',
                'whiteSpace': 'normal',
                'height': 'auto',
            },
            style_cell_conditional=[
                {
                    'if': {'column_id': 'ID'},
                    'width': '80px',
                    'minWidth': '80px',
                    'maxWidth': '80px',
                },
                {
                    'if': {'column_id': '策略名称'},
                    'width': '200px',
                    'minWidth': '150px',
                    'maxWidth': '250px',
                },
                {
                    'if': {'column_id': '初始资金'},
                    'width': '120px',
                    'minWidth': '100px',
                    'maxWidth': '140px',
                },
                {
                    'if': {'column_id': '创建时间'},
                    'width': '150px',
                    'minWidth': '130px',
                    'maxWidth': '170px',
                },
                {
                    'if': {'column_id': '更新时间'},
                    'width': '150px',
                    'minWidth': '130px',
                    'maxWidth': '170px',
                }
            ],
            style_header={
                'backgroundColor': '#f8f9fa',
                'fontWeight': 'bold',
                'color': '#495057',
                'border': '1px solid #dee2e6'
            },
            style_data={
                'border': '1px solid #dee2e6'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#f8f9fa'
                },
                {
                    'if': {'state': 'selected'},
                    'backgroundColor': '#e3f2fd',
                    'border': '1px solid #2196f3'
                }
            ],
            row_selectable='single',
            selected_rows=[],
            page_size=15,
            sort_action='native',
            filter_action='native',
            css=[{
                'selector': '.dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner table',
                'rule': 'border-collapse: collapse;'
            }]
        ),
        
        # 表格操作说明
        html.Div([
            html.Small([
                html.I(className="fas fa-info-circle me-1"),
                "点击表格行选中策略，然后使用上方按钮进行操作"
            ], className="text-muted")
        ], className="mt-2")
    ])

def create_strategy_management(mysql_db: MySQLDatabase):
    """创建策略管理界面"""
    db_strategys = DBStrategys(mysql_db)
    strategies = db_strategys.get_all_strategies()

    return html.Div([
        # 页面标题和主要操作按钮
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H2([
                        html.I(className="fas fa-chart-line me-3"),
                        "策略管理"
                    ], className="mb-4 text-primary"),
                    
                    # 主要操作按钮
                    html.Div([
                        dbc.Button(
                            [html.I(className="fas fa-plus me-2"), "新建策略"], 
                            id="open-new-strategy-modal", 
                            color="primary", 
                            size="lg"
                        ),
                        dbc.Button(
                            [html.I(className="fas fa-edit me-2"), "编辑策略"], 
                            id="open-edit-strategy-modal", 
                            color="success", 
                            size="lg",
                            disabled=True
                        ),
                        dbc.Button(
                            [html.I(className="fas fa-trash me-2"), "删除策略"], 
                            id="delete-strategy-btn", 
                            color="danger", 
                            size="lg",
                            disabled=True
                        ),
                        dbc.Button(
                            [html.I(className="fas fa-sync me-2"), "刷新"], 
                            id="refresh-strategy-list", 
                            color="info", 
                            size="lg",
                            outline=True
                        )
                    ], className="strategy-main-buttons")
                ])
            ])
        ]),
        
        # 搜索区域
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                dbc.InputGroup([
                                    dbc.Input(
                                        id="strategy-search-input",
                                        placeholder="搜索策略名称或描述...",
                                        type="text"
                                    ),
                                    dbc.Button(
                                        [html.I(className="fas fa-search")],
                                        id="strategy-search-btn",
                                        color="outline-secondary"
                                    )
                                ])
                            ], width=8),
                            dbc.Col([
                                dbc.Button(
                                    [html.I(className="fas fa-times me-2"), "清除搜索"],
                                    id="clear-search-btn",
                                    color="outline-secondary",
                                    className="w-100"
                                )
                            ], width=4)
                        ])
                    ])
                ], className="mb-4 strategy-search-card")
            ])
        ]),
        
        # 策略统计信息
        dbc.Row([
            dbc.Col([
                html.Div(id="strategy-stats", children=[
                    create_strategy_stats(strategies)
                ])
            ], width=12)
        ], className="mb-4"),
        
        # 策略列表区域
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-list me-2"),
                            "策略列表"
                        ], className="mb-0"),
                        html.Div(id="selected-strategy-info", className="text-muted mt-2")
                    ]),
                    dbc.CardBody([
                        html.Div(
                            id="strategy-list-container",
                            children=create_strategy_list_table(strategies)
                        )
                    ])
                ])
            ])
        ]),
        
        # 策略表单模态框
        create_strategy_form_modal(),
        
        # 确认删除模态框
        dbc.Modal([
            dbc.ModalHeader("确认删除"),
            dbc.ModalBody([
                html.Div([
                    html.I(className="fas fa-exclamation-triangle text-warning me-2"),
                    "确定要删除选中的策略吗？此操作不可恢复。"
                ]),
                html.Div(id="delete-strategy-info", className="mt-3 p-3 bg-light rounded")
            ]),
            dbc.ModalFooter([
                dbc.Button("取消", id="cancel-delete-btn", color="secondary", className="me-2"),
                dbc.Button("确认删除", id="confirm-delete-btn", color="danger")
            ])
        ], id="delete-confirm-modal", is_open=False, className="delete-confirm-modal"),
        
        # 消息提示
        html.Div(id="strategy-message-container"),
        
        # 隐藏的数据存储
        dcc.Store(id="selected-strategy-data"),
        dcc.Store(id="current-operation")  # 'new', 'edit'
        
    ], className="strategy-management-container p-4")

def create_strategy_stats(strategies_df: pd.DataFrame):
    """创建策略统计信息"""
    total_count = len(strategies_df)
    
    if total_count == 0:
        return dbc.Alert("还没有创建任何策略", color="info", className="text-center")
    
    # 计算统计信息
    avg_cash = strategies_df['initial_cash'].mean() if 'initial_cash' in strategies_df.columns else 0
    total_cash = strategies_df['initial_cash'].sum() if 'initial_cash' in strategies_df.columns else 0
    
    return dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(str(total_count), className="text-primary mb-1"),
                    html.P("策略总数", className="text-muted mb-0")
                ])
            ], className="text-center h-100 strategy-stats-card")
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(f"{total_cash:,.0f}元", className="text-success mb-1"),
                    html.P("总资金规模", className="text-muted mb-0")
                ])
            ], className="text-center h-100 strategy-stats-card")
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(f"{avg_cash:,.0f}元", className="text-info mb-1"),
                    html.P("平均资金", className="text-muted mb-0")
                ])
            ], className="text-center h-100 strategy-stats-card")
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(strategies_df['create_time'].dt.date.nunique() if 'create_time' in strategies_df.columns else 0, className="text-warning mb-1"),
                    html.P("创建天数", className="text-muted mb-0")
                ])
            ], className="text-center h-100 strategy-stats-card")
        ], width=3),
    ])

# 添加验证函数
def validate_json_field(json_string, field_name):
    """验证JSON字段"""
    try:
        if not json_string or json_string.strip() == '':
            return False, f"{field_name}不能为空"
        
        json.loads(json_string)
        return True, ""
    except json.JSONDecodeError as e:
        return False, f"{field_name}JSON格式错误: {str(e)}"

def validate_strategy_data(name, description, cash, data_params, parameters, strategy_config):
    """验证策略数据"""
    errors = []
    
    # 验证基本字段
    if not name or name.strip() == '':
        errors.append("策略名称不能为空")
    
    if not cash or cash < 1000:
        errors.append("初始资金不能少于1000元")
    
    # 验证JSON字段
    is_valid, error_msg = validate_json_field(data_params, "数据参数")
    if not is_valid:
        errors.append(error_msg)
    
    is_valid, error_msg = validate_json_field(parameters, "策略参数")
    if not is_valid:
        errors.append(error_msg)
    
    is_valid, error_msg = validate_json_field(strategy_config, "策略配置")
    if not is_valid:
        errors.append(error_msg)
    
    return len(errors) == 0, errors