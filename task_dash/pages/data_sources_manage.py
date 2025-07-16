#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
from datetime import datetime
from database.db_data_sources import DBDataSources

# 注册页面
dash.register_page(__name__, path='/data_sources_manage', name='数据源管理')

# 数据源类型选项
SOURCE_TYPE_OPTIONS = [
    {'label': '基金数据', 'value': 'fund'},
    {'label': '股票数据', 'value': 'stock'},
    {'label': '外汇数据', 'value': 'forex'},
    {'label': '债券数据', 'value': 'bond'},
    {'label': '其他', 'value': 'other'}
]

# 状态选项
STATUS_OPTIONS = [
    {'label': '活跃', 'value': 'active'},
    {'label': '停用', 'value': 'inactive'}
]

def layout():
    return html.Div([
        html.H1("数据源管理", className="page-title"),
        
        # 全局概览部分
        html.Div([
            html.H2("全局概览", className="section-title"),
            html.Div(id='data-sources-overview', children=[]),
            html.Div(id='data-sources-charts', children=[])
        ], className="overview-section"),
        
        html.Hr(),
        
        # 数据源管理部分
        html.Div([
            html.H2("数据源配置", className="section-title"),
            
            # 添加新数据源按钮
            html.Div([
                html.Button("添加新数据源", id="add-data-source-btn", 
                          className="btn btn-primary", n_clicks=0),
                html.Button("刷新列表", id="refresh-data-sources-btn", 
                          className="btn btn-secondary", n_clicks=0, style={'margin-left': '10px'})
            ], className="button-group"),
            
            # 数据源列表
            html.Div(id='data-sources-table-container', children=[]),
            
            # 添加/编辑数据源模态框
            html.Div([
                html.Div([
                    html.Div([
                        html.H3("数据源配置", id="modal-title"),
                        html.Button("×", id="close-modal-btn", 
                                  className="close-btn", n_clicks=0)
                    ], className="modal-header"),
                    
                    html.Div([
                        html.Div([
                            html.Label("数据源名称"),
                            dcc.Input(id="source-name-input", type="text", 
                                    placeholder="请输入数据源名称", className="form-control")
                        ], className="form-group"),
                        
                        html.Div([
                            html.Label("数据源类型"),
                            dcc.Dropdown(id="source-type-dropdown", 
                                       options=SOURCE_TYPE_OPTIONS,
                                       placeholder="请选择数据源类型",
                                       className="form-control")
                        ], className="form-group"),
                        
                        html.Div([
                            html.Label("数据源URL"),
                            dcc.Input(id="source-url-input", type="url", 
                                    placeholder="请输入数据源URL", className="form-control")
                        ], className="form-group"),
                        
                        html.Div([
                            html.Label("优先级"),
                            dcc.Input(id="source-priority-input", type="number", 
                                    value=1, min=1, max=10, className="form-control")
                        ], className="form-group"),
                        
                        html.Div([
                            html.Label("状态"),
                            dcc.Dropdown(id="source-status-dropdown", 
                                       options=STATUS_OPTIONS,
                                       value='active',
                                       className="form-control")
                        ], className="form-group"),
                        
                        html.Div([
                            html.Label("描述"),
                            dcc.Textarea(id="source-description-input", 
                                       placeholder="请输入数据源描述", 
                                       className="form-control", rows=3)
                        ], className="form-group")
                    ], className="modal-body"),
                    
                    html.Div([
                        html.Button("保存", id="save-data-source-btn", 
                                  className="btn btn-primary", n_clicks=0),
                        html.Button("取消", id="cancel-data-source-btn", 
                                  className="btn btn-secondary", n_clicks=0)
                    ], className="modal-footer")
                ], className="modal-content")
            ], id="data-source-modal", className="modal", style={'display': 'none'}),
            
            # 操作结果提示
            html.Div(id='operation-result', children=[])
        ], className="management-section")
    ], className="container")


def create_overview_cards(stats):
    """创建概览卡片"""
    cards = []
    
    # 总数据源数量
    cards.append(
        html.Div([
            html.H3(str(stats.get('total_sources', 0))),
            html.P("总数据源")
        ], className="overview-card total-card")
    )
    
    # 活跃数据源数量
    cards.append(
        html.Div([
            html.H3(str(stats.get('active_sources', 0))),
            html.P("活跃数据源")
        ], className="overview-card active-card")
    )
    
    # 停用数据源数量
    cards.append(
        html.Div([
            html.H3(str(stats.get('inactive_sources', 0))),
            html.P("停用数据源")
        ], className="overview-card inactive-card")
    )
    
    # 数据源类型数量
    cards.append(
        html.Div([
            html.H3(str(stats.get('source_types_count', 0))),
            html.P("数据源类型")
        ], className="overview-card types-card")
    )
    
    return html.Div(cards, className="overview-cards")


def create_data_sources_table(data_sources):
    """创建数据源表格"""
    if not data_sources:
        return html.Div("暂无数据源配置", className="no-data")
    
    # 转换数据格式
    df = pd.DataFrame(data_sources)
    
    # 处理时间格式
    if 'last_update' in df.columns:
        df['last_update'] = df['last_update'].fillna('从未更新')
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
    
    # 处理状态显示
    df['status_display'] = df['status'].map({
        'active': '✅ 活跃',
        'inactive': '❌ 停用'
    })
    
    # 处理数据源类型显示
    type_map = {option['value']: option['label'] for option in SOURCE_TYPE_OPTIONS}
    df['source_type_display'] = df['source_type'].map(type_map)
    
    return dash_table.DataTable(
        id='data-sources-table',
        columns=[
            {'name': 'ID', 'id': 'id', 'type': 'numeric'},
            {'name': '数据源名称', 'id': 'name'},
            {'name': '类型', 'id': 'source_type_display'},
            {'name': 'URL', 'id': 'url'},
            {'name': '状态', 'id': 'status_display'},
            {'name': '优先级', 'id': 'priority', 'type': 'numeric'},
            {'name': '最后更新', 'id': 'last_update'},
            {'name': '创建时间', 'id': 'created_at'},
            {'name': '描述', 'id': 'description'}
        ],
        data=df.to_dict('records'),
        sort_action="native",
        filter_action="native",
        page_action="native",
        page_current=0,
        page_size=10,
        style_cell={'textAlign': 'left', 'padding': '10px'},
        style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold'},
        style_data_conditional=[
            {
                'if': {'filter_query': '{status} = inactive'},
                'backgroundColor': '#f8d7da',
                'color': 'black',
            },
            {
                'if': {'filter_query': '{status} = active'},
                'backgroundColor': '#d4edda',
                'color': 'black',
            }
        ],
        row_selectable='single',
        selected_rows=[],
        export_format='xlsx',
        export_headers='display'
    )


def create_type_distribution_chart(type_stats):
    """创建数据源类型分布图"""
    if not type_stats:
        return html.Div("暂无数据", className="no-data")
    
    df = pd.DataFrame(type_stats)
    
    # 创建饼图
    fig = px.pie(
        df, 
        values='total_count', 
        names='source_type',
        title='数据源类型分布',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    return dcc.Graph(figure=fig)


def create_status_distribution_chart(type_stats):
    """创建状态分布图"""
    if not type_stats:
        return html.Div("暂无数据", className="no-data")
    
    df = pd.DataFrame(type_stats)
    
    # 创建堆积柱状图
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='活跃',
        x=df['source_type'],
        y=df['active_count'],
        marker_color='#28a745'
    ))
    
    fig.add_trace(go.Bar(
        name='停用',
        x=df['source_type'],
        y=df['inactive_count'],
        marker_color='#dc3545'
    ))
    
    fig.update_layout(
        title='各类型数据源状态分布',
        xaxis_title='数据源类型',
        yaxis_title='数量',
        barmode='stack',
        height=300,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    return dcc.Graph(figure=fig)