#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dash
from dash import dcc, html, callback, Input, Output, State
import pandas as pd
from datetime import datetime
import traceback
from database.db_data_sources import DBDataSources
from task_dash.pages.data_sources_manage import (
    create_overview_cards, 
    create_data_sources_table,
    create_type_distribution_chart,
    create_status_distribution_chart
)


@callback(
    [Output('data-sources-overview', 'children'),
     Output('data-sources-charts', 'children'),
     Output('data-sources-table-container', 'children')],
    [Input('refresh-data-sources-btn', 'n_clicks')],
    prevent_initial_call=False
)
def refresh_data_sources_data(n_clicks):
    """刷新数据源数据"""
    try:
        db_sources = DBDataSources()
        
        # 获取统计数据
        stats = db_sources.get_data_source_stats()
        type_stats = db_sources.get_data_source_type_stats()
        all_sources = db_sources.get_all_data_sources()
        
        # 创建概览卡片
        overview_cards = create_overview_cards(stats)
        
        # 创建图表
        charts = html.Div([
            html.Div([
                create_type_distribution_chart(type_stats)
            ], className="chart-container", style={'width': '50%', 'display': 'inline-block'}),
            html.Div([
                create_status_distribution_chart(type_stats)
            ], className="chart-container", style={'width': '50%', 'display': 'inline-block'})
        ], className="charts-row")
        
        # 创建数据源表格
        table = create_data_sources_table(all_sources)
        
        return overview_cards, charts, table
        
    except Exception as e:
        error_msg = f"加载数据源数据时发生错误: {str(e)}"
        print(f"Error in refresh_data_sources_data: {error_msg}")
        return (
            html.Div(error_msg, className="error-message"),
            html.Div(),
            html.Div()
        )


@callback(
    Output('data-source-modal', 'style'),
    [Input('add-data-source-btn', 'n_clicks'),
     Input('close-modal-btn', 'n_clicks'),
     Input('cancel-data-source-btn', 'n_clicks'),
     Input('save-data-source-btn', 'n_clicks')],
    [State('data-source-modal', 'style')]
)
def toggle_modal(add_clicks, close_clicks, cancel_clicks, save_clicks, current_style):
    """切换模态框显示状态"""
    ctx = dash.callback_context
    if not ctx.triggered:
        return {'display': 'none'}
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == 'add-data-source-btn':
        return {'display': 'block'}
    elif button_id in ['close-modal-btn', 'cancel-data-source-btn', 'save-data-source-btn']:
        return {'display': 'none'}
    
    return current_style or {'display': 'none'}


@callback(
    [Output('modal-title', 'children'),
     Output('source-name-input', 'value'),
     Output('source-type-dropdown', 'value'),
     Output('source-url-input', 'value'),
     Output('source-priority-input', 'value'),
     Output('source-status-dropdown', 'value'),
     Output('source-description-input', 'value')],
    [Input('add-data-source-btn', 'n_clicks'),
     Input('data-sources-table', 'selected_rows')],
    [State('data-sources-table', 'data')]
)
def update_modal_content(add_clicks, selected_rows, table_data):
    """更新模态框内容"""
    ctx = dash.callback_context
    if not ctx.triggered:
        return "添加数据源", "", None, "", 1, 'active', ""
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == 'add-data-source-btn':
        return "添加数据源", "", None, "", 1, 'active', ""
    elif button_id == 'data-sources-table' and selected_rows and table_data:
        # 编辑模式
        selected_row = table_data[selected_rows[0]]
        return (
            "编辑数据源",
            selected_row.get('name', ''),
            selected_row.get('source_type', ''),
            selected_row.get('url', ''),
            selected_row.get('priority', 1),
            selected_row.get('status', 'active'),
            selected_row.get('description', '')
        )
    
    return "添加数据源", "", None, "", 1, 'active', ""


@callback(
    Output('operation-result', 'children'),
    [Input('save-data-source-btn', 'n_clicks')],
    [State('source-name-input', 'value'),
     State('source-type-dropdown', 'value'),
     State('source-url-input', 'value'),
     State('source-priority-input', 'value'),
     State('source-status-dropdown', 'value'),
     State('source-description-input', 'value'),
     State('data-sources-table', 'selected_rows'),
     State('data-sources-table', 'data')],
    prevent_initial_call=True
)
def save_data_source(n_clicks, name, source_type, url, priority, status, description, selected_rows, table_data):
    """保存数据源配置"""
    if not n_clicks:
        return ""
    
    try:
        # 验证输入
        if not name or not source_type or not url:
            return html.Div("请填写完整的数据源信息", className="alert alert-danger")
        
        db_sources = DBDataSources()
        
        data_source = {
            'name': name,
            'source_type': source_type,
            'url': url,
            'priority': priority or 1,
            'status': status or 'active',
            'description': description or ''
        }
        
        # 判断是新增还是编辑
        if selected_rows and table_data:
            # 编辑模式
            source_id = table_data[selected_rows[0]]['id']
            success = db_sources.update_data_source(source_id, data_source)
            if success:
                return html.Div("数据源更新成功", className="alert alert-success")
            else:
                return html.Div("数据源更新失败", className="alert alert-danger")
        else:
            # 新增模式
            source_id = db_sources.insert_data_source(data_source)
            if source_id:
                return html.Div(f"数据源添加成功，ID: {source_id}", className="alert alert-success")
            else:
                return html.Div("数据源添加失败", className="alert alert-danger")
                
    except Exception as e:
        error_msg = f"保存数据源时发生错误: {str(e)}"
        print(f"Error in save_data_source: {error_msg}")
        print(traceback.format_exc())
        return html.Div(error_msg, className="alert alert-danger")


@callback(
    Output('data-sources-table', 'selected_rows'),
    [Input('data-sources-table', 'derived_virtual_selected_rows')],
    prevent_initial_call=True
)
def update_selected_rows(selected_rows):
    """更新选中的行"""
    return selected_rows or []


# 双击表格行编辑数据源
@callback(
    Output('data-source-modal', 'style', allow_duplicate=True),
    [Input('data-sources-table', 'active_cell')],
    [State('data-sources-table', 'data'),
     State('data-source-modal', 'style')],
    prevent_initial_call=True
)
def edit_data_source_on_double_click(active_cell, table_data, current_style):
    """双击表格行编辑数据源"""
    if active_cell and table_data:
        return {'display': 'block'}
    return current_style or {'display': 'none'}


# 删除数据源
@callback(
    Output('operation-result', 'children', allow_duplicate=True),
    [Input('data-sources-table', 'data_timestamp')],
    [State('data-sources-table', 'selected_rows'),
     State('data-sources-table', 'data')],
    prevent_initial_call=True
)
def delete_data_source(timestamp, selected_rows, table_data):
    """删除选中的数据源"""
    # 这里可以添加删除按钮的逻辑
    # 为了简化，暂时不实现自动删除功能
    return ""


# 测试数据源连接
@callback(
    Output('operation-result', 'children', allow_duplicate=True),
    [Input('data-sources-table', 'selected_rows')],
    [State('data-sources-table', 'data')],
    prevent_initial_call=True
)
def test_data_source_connection(selected_rows, table_data):
    """测试数据源连接"""
    if not selected_rows or not table_data:
        return ""
    
    try:
        selected_row = table_data[selected_rows[0]]
        source_url = selected_row.get('url', '')
        source_name = selected_row.get('name', '')
        
        # 这里可以添加实际的连接测试逻辑
        # 目前只是简单的示例
        
        return html.Div(
            f"数据源 '{source_name}' 连接测试功能待实现",
            className="alert alert-info"
        )
        
    except Exception as e:
        error_msg = f"测试数据源连接时发生错误: {str(e)}"
        print(f"Error in test_data_source_connection: {error_msg}")
        return html.Div(error_msg, className="alert alert-danger")