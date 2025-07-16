#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dash_bootstrap_components as dbc
from dash import html, dcc
import plotly.express as px
import pandas as pd
from datetime import datetime

def create_data_sources_management():
    """
    创建数据源管理页面
    """
    return html.Div([
        dbc.Container([
            # 页面标题
            html.H2("数据源管理", className="text-center my-4"),
            
            # 全局概览部分
            html.Div([
                html.H3("全局概览", className="mb-4"),
                
                # 统计卡片
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H2("7", className="text-center text-primary"),
                                html.P("总数据源", className="text-center text-muted")
                            ])
                        ])
                    ], width=3),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H2("5", className="text-center text-success"),
                                html.P("活跃数据源", className="text-center text-muted")
                            ])
                        ])
                    ], width=3),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H2("2", className="text-center text-danger"),
                                html.P("停用数据源", className="text-center text-muted")
                            ])
                        ])
                    ], width=3),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H2("4", className="text-center text-warning"),
                                html.P("数据源类型", className="text-center text-muted")
                            ])
                        ])
                    ], width=3)
                ], className="mb-4"),
                
                # 数据更新状态
                html.H4("数据更新状态", className="mb-3"),
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H5("基金数据", className="card-title"),
                                dbc.Badge("今天更新", color="success", className="mb-2"),
                                html.P("最后更新: 2024-01-15 10:30:00", className="text-muted small")
                            ])
                        ])
                    ], width=4),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H5("股票数据", className="card-title"),
                                dbc.Badge("1天前更新", color="warning", className="mb-2"),
                                html.P("最后更新: 2024-01-14 15:45:00", className="text-muted small")
                            ])
                        ])
                    ], width=4),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H5("外汇数据", className="card-title"),
                                dbc.Badge("3天前更新", color="danger", className="mb-2"),
                                html.P("最后更新: 2024-01-12 09:15:00", className="text-muted small")
                            ])
                        ])
                    ], width=4)
                ], className="mb-4"),
                
                # 示例图表
                html.H4("数据源分布", className="mb-3"),
                dbc.Row([
                    dbc.Col([
                        dcc.Graph(
                            figure=create_sample_pie_chart(),
                            style={'height': '300px'}
                        )
                    ], width=6),
                    dbc.Col([
                        dcc.Graph(
                            figure=create_sample_bar_chart(),
                            style={'height': '300px'}
                        )
                    ], width=6)
                ])
            ], className="mb-5"),
            
            html.Hr(),
            
            # 数据源配置部分
            html.Div([
                html.H3("数据源配置", className="mb-4"),
                
                # 操作按钮
                dbc.Row([
                    dbc.Col([
                        dbc.Button("添加新数据源", color="primary", className="me-2", id="add-source-btn"),
                        dbc.Button("刷新列表", color="secondary", className="me-2", id="refresh-btn"),
                        dbc.Button("批量操作", color="info", id="batch-btn")
                    ], width=12)
                ], className="mb-3"),
                
                # 数据源列表（示例）
                html.Div([
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
            ])
        ], fluid=True)
    ])


def create_sample_pie_chart():
    """创建示例饼图"""
    data = {
        '类型': ['基金', '股票', '外汇', '债券'],
        '数量': [3, 2, 1, 1]
    }
    fig = px.pie(
        data, 
        values='数量', 
        names='类型',
        title='数据源类型分布'
    )
    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    return fig


def create_sample_bar_chart():
    """创建示例柱状图"""
    data = {
        '类型': ['基金', '股票', '外汇', '债券'],
        '活跃': [2, 2, 1, 0],
        '停用': [1, 0, 0, 1]
    }
    df = pd.DataFrame(data)
    
    fig = px.bar(
        df, 
        x='类型', 
        y=['活跃', '停用'],
        title='各类型数据源状态分布',
        barmode='group'
    )
    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    return fig