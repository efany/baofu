#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
基金管理页面优化 - 演示版本

这是一个不依赖数据库的演示版本，展示优化后的功能。
"""

import sys
import os
import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from datetime import datetime, timedelta

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 初始化Dash应用
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# 模拟数据
mock_funds_data = {
    'ts_code': ['000001.OF', '000002.OF', '000003.OF', '000004.OF', '000005.OF'],
    'name': ['华夏成长', '华夏大盘', '中欧价值', '嘉实增长', '南方积配'],
    'management': ['华夏基金', '华夏基金', '中欧基金', '嘉实基金', '南方基金'],
    'fund_type': ['股票型', '混合型', '股票型', '混合型', '债券型']
}

mock_stocks_data = {
    'ts_code': ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH'],
    'name': ['平安银行', '万科A', '浦发银行', '招商银行'],
    'industry': ['银行', '房地产', '银行', '银行']
}

mock_forex_data = {
    'symbol': ['USDCNY', 'EURCNY', 'JPYCNY', 'GBPCNY'],
    'name': ['美元人民币', '欧元人民币', '日元人民币', '英镑人民币']
}

def create_demo_overview():
    """创建演示版全局概览"""
    return html.Div([
        html.H3("全局概览", className="mb-4"),
        
        # 数据统计卡片
        dbc.Row([
            dbc.Col([
                create_stat_card("基金总数", len(mock_funds_data['ts_code']), "primary", "📊")
            ], width=3),
            dbc.Col([
                create_stat_card("股票总数", len(mock_stocks_data['ts_code']), "success", "📈")
            ], width=3),
            dbc.Col([
                create_stat_card("外汇对数", len(mock_forex_data['symbol']), "info", "💱")
            ], width=3),
            dbc.Col([
                create_stat_card("数据源数", 7, "warning", "🔗")
            ], width=3)
        ], className="mb-4"),
        
        # 数据更新状态
        html.H4("数据更新状态", className="mb-3"),
        dbc.Row([
            dbc.Col([
                create_update_status_card("基金数据", "2024-01-15 10:30:00")
            ], width=4),
            dbc.Col([
                create_update_status_card("股票数据", "2024-01-14 15:45:00")
            ], width=4),
            dbc.Col([
                create_update_status_card("外汇数据", "2024-01-12 09:15:00")
            ], width=4)
        ], className="mb-4"),
        
        # 数据质量概览
        html.H4("数据质量概览", className="mb-3"),
        dbc.Row([
            dbc.Col([
                create_demo_quality_chart()
            ], width=6),
            dbc.Col([
                create_demo_distribution_chart()
            ], width=6)
        ])
    ])

def create_stat_card(title, value, color, icon):
    """创建统计卡片"""
    return dbc.Card([
        dbc.CardBody([
            html.H2(f"{icon} {value}", className="text-center"),
            html.P(title, className="text-center text-muted")
        ])
    ], color=color, outline=True)

def create_update_status_card(title, last_update):
    """创建更新状态卡片"""
    try:
        last_update_dt = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
        now = datetime.now()
        time_diff = now - last_update_dt
        
        if time_diff.days == 0:
            status_text = "今天更新"
            status_color = "success"
        elif time_diff.days == 1:
            status_text = "昨天更新"
            status_color = "warning"
        elif time_diff.days <= 7:
            status_text = f"{time_diff.days}天前更新"
            status_color = "warning"
        else:
            status_text = f"{time_diff.days}天前更新"
            status_color = "danger"
    except:
        status_text = "状态未知"
        status_color = "secondary"
    
    return dbc.Card([
        dbc.CardBody([
            html.H5(title, className="card-title"),
            dbc.Badge(status_text, color=status_color, className="mb-2"),
            html.P(f"最后更新: {last_update}", className="text-muted small")
        ])
    ])

def create_demo_quality_chart():
    """创建数据质量图表"""
    data = {
        '产品类型': ['基金', '股票', '外汇'],
        '数据完整性': [85, 90, 80]
    }
    
    fig = px.bar(
        data, 
        x='产品类型', 
        y='数据完整性',
        title='数据完整性评分',
        color='数据完整性',
        color_continuous_scale='RdYlGn'
    )
    
    fig.update_layout(
        height=300,
        showlegend=False
    )
    
    return dcc.Graph(figure=fig)

def create_demo_distribution_chart():
    """创建数据分布图表"""
    labels = ['基金', '股票', '外汇']
    values = [len(mock_funds_data['ts_code']), len(mock_stocks_data['ts_code']), len(mock_forex_data['symbol'])]
    
    fig = px.pie(
        values=values,
        names=labels,
        title='产品数据分布'
    )
    
    fig.update_layout(
        height=300
    )
    
    return dcc.Graph(figure=fig)

def create_demo_data_sources():
    """创建数据源管理演示"""
    return html.Div([
        html.H3("数据源管理", className="mb-4"),
        
        # 操作按钮
        dbc.Row([
            dbc.Col([
                dbc.Button("添加新数据源", color="primary", className="me-2"),
                dbc.Button("刷新列表", color="secondary", className="me-2"),
                dbc.Button("批量操作", color="info")
            ], width=12)
        ], className="mb-3"),
        
        # 数据源列表
        dbc.Table([
            html.Thead([
                html.Tr([
                    html.Th("ID"),
                    html.Th("数据源名称"),
                    html.Th("类型"),
                    html.Th("状态"),
                    html.Th("优先级"),
                    html.Th("最后更新"),
                    html.Th("操作")
                ])
            ]),
            html.Tbody([
                html.Tr([
                    html.Td("1"),
                    html.Td("东方财富基金信息"),
                    html.Td("基金"),
                    html.Td(dbc.Badge("活跃", color="success")),
                    html.Td("9"),
                    html.Td("2024-01-15 10:30"),
                    html.Td([
                        dbc.Button("编辑", color="warning", size="sm", className="me-1"),
                        dbc.Button("删除", color="danger", size="sm")
                    ])
                ]),
                html.Tr([
                    html.Td("2"),
                    html.Td("东方财富基金净值"),
                    html.Td("基金"),
                    html.Td(dbc.Badge("活跃", color="success")),
                    html.Td("9"),
                    html.Td("2024-01-15 10:30"),
                    html.Td([
                        dbc.Button("编辑", color="warning", size="sm", className="me-1"),
                        dbc.Button("删除", color="danger", size="sm")
                    ])
                ]),
                html.Tr([
                    html.Td("3"),
                    html.Td("东方财富股票数据"),
                    html.Td("股票"),
                    html.Td(dbc.Badge("活跃", color="success")),
                    html.Td("8"),
                    html.Td("2024-01-14 15:45"),
                    html.Td([
                        dbc.Button("编辑", color="warning", size="sm", className="me-1"),
                        dbc.Button("删除", color="danger", size="sm")
                    ])
                ]),
                html.Tr([
                    html.Td("4"),
                    html.Td("招商银行外汇数据"),
                    html.Td("外汇"),
                    html.Td(dbc.Badge("活跃", color="success")),
                    html.Td("7"),
                    html.Td("2024-01-12 09:15"),
                    html.Td([
                        dbc.Button("编辑", color="warning", size="sm", className="me-1"),
                        dbc.Button("删除", color="danger", size="sm")
                    ])
                ]),
                html.Tr([
                    html.Td("5"),
                    html.Td("新浪财经基金"),
                    html.Td("基金"),
                    html.Td(dbc.Badge("停用", color="danger")),
                    html.Td("5"),
                    html.Td("2024-01-10 14:20"),
                    html.Td([
                        dbc.Button("编辑", color="warning", size="sm", className="me-1"),
                        dbc.Button("删除", color="danger", size="sm")
                    ])
                ])
            ])
        ], striped=True, bordered=True, hover=True)
    ])

# 应用布局
app.layout = dbc.Container([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
], fluid=True)

# 路由回调
@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    if pathname == '/' or pathname == '/home':
        return html.Div([
            html.H1("基金管理系统 - 演示版", className="text-center my-4"),
            html.P("这是一个不依赖数据库的演示版本，展示优化后的功能。", className="text-center text-muted"),
            html.Div([
                dbc.Button("全局概览", href="/overview", className="me-2"),
                dbc.Button("数据源管理", href="/data_sources", className="me-2"),
                dbc.Button("产品管理", href="/products", className="me-2"),
            ], className="text-center")
        ])
    elif pathname == '/overview':
        return html.Div([
            html.H1("基金管理系统 - 全局概览", className="text-center my-4"),
            html.Div([
                dbc.Button("返回首页", href="/", color="secondary", className="mb-3")
            ]),
            create_demo_overview()
        ])
    elif pathname == '/data_sources':
        return html.Div([
            html.H1("基金管理系统 - 数据源管理", className="text-center my-4"),
            html.Div([
                dbc.Button("返回首页", href="/", color="secondary", className="mb-3")
            ]),
            create_demo_data_sources()
        ])
    elif pathname == '/products':
        return html.Div([
            html.H1("基金管理系统 - 产品管理", className="text-center my-4"),
            html.Div([
                dbc.Button("返回首页", href="/", color="secondary", className="mb-3")
            ]),
            create_demo_overview(),
            html.Hr(),
            create_demo_data_sources()
        ])
    else:
        return html.H1("404: 页面未找到")

if __name__ == '__main__':
    print("🚀 启动基金管理系统演示版...")
    print("📱 访问地址: http://localhost:8051")
    print("🏠 首页: http://localhost:8051/")
    print("📊 全局概览: http://localhost:8051/overview")
    print("🔗 数据源管理: http://localhost:8051/data_sources")
    print("📋 产品管理: http://localhost:8051/products")
    print("=" * 50)
    print("按 Ctrl+C 停止服务")
    
    try:
        app.run_server(debug=True, host='0.0.0.0', port=8051)
    except KeyboardInterrupt:
        print("\n👋 服务已停止")