#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dash
from dash import dcc, html, dash_table
import json
import os

def load_crawler_scripts_info():
    """加载爬虫脚本描述信息"""
    try:
        # 获取scripts_description.json文件路径
        current_dir = os.path.dirname(__file__)
        scripts_file = os.path.join(current_dir, '..', '..', 'task_crawlers', 'scripts_description.json')
        
        with open(scripts_file, 'r', encoding='utf-8') as f:
            scripts_data = json.load(f)
        
        return scripts_data.get('scripts', {})
    except Exception as e:
        print(f"加载爬虫脚本信息失败: {e}")
        return {}

def layout():
    return html.Div([
        html.H1("数据源管理", className="page-title"),
        
        # 爬虫脚本概览部分
        html.Div([
            html.H2("爬虫脚本概览", className="section-title"),
            create_scripts_overview_cards()
        ], className="overview-section"),
        
        html.Hr(),
        
        # 爬虫脚本详细信息
        html.Div([
            html.H2("可用爬虫脚本", className="section-title"),
            html.Div(id='crawler-scripts-container', children=[
                create_crawler_scripts_table()
            ])
        ], className="scripts-section")
    ], className="container")


def create_scripts_overview_cards():
    """创建爬虫脚本概览卡片"""
    try:
        scripts_info = load_crawler_scripts_info()
        
        # 统计不同类型和状态的脚本数量
        total_scripts = len(scripts_info)
        active_scripts = 0
        inactive_scripts = 0
        type_counts = {}
        
        for script_file, script_info in scripts_info.items():
            # 从JSON获取数据类型
            data_type = get_data_type_display_name(script_info.get('data_type', 'other'))
            type_counts[data_type] = type_counts.get(data_type, 0) + 1
            
            # 统计激活状态
            if script_info.get('is_active', True):
                active_scripts += 1
            else:
                inactive_scripts += 1
        
        cards = []
        
        # 总脚本数
        cards.append(
            html.Div([
                html.H3(str(total_scripts), style={'color': '#1976d2', 'margin': '0'}),
                html.P("总爬虫脚本", style={'margin': '5px 0 0 0'})
            ], className="overview-card", 
            style={
                'border': '2px solid #e3f2fd', 
                'border-radius': '8px', 
                'padding': '20px', 
                'text-align': 'center', 
                'margin': '10px',
                'background': 'white',
                'box-shadow': '0 2px 4px rgba(0,0,0,0.1)'
            })
        )
        
        # 激活脚本数
        cards.append(
            html.Div([
                html.H3(str(active_scripts), style={'color': '#4caf50', 'margin': '0'}),
                html.P("激活脚本", style={'margin': '5px 0 0 0'})
            ], className="overview-card", 
            style={
                'border': '2px solid #4caf50', 
                'border-radius': '8px', 
                'padding': '20px', 
                'text-align': 'center', 
                'margin': '10px',
                'background': 'white',
                'box-shadow': '0 2px 4px rgba(0,0,0,0.1)'
            })
        )
        
        # 未激活脚本数
        cards.append(
            html.Div([
                html.H3(str(inactive_scripts), style={'color': '#f44336', 'margin': '0'}),
                html.P("未激活脚本", style={'margin': '5px 0 0 0'})
            ], className="overview-card", 
            style={
                'border': '2px solid #f44336', 
                'border-radius': '8px', 
                'padding': '20px', 
                'text-align': 'center', 
                'margin': '10px',
                'background': 'white',
                'box-shadow': '0 2px 4px rgba(0,0,0,0.1)'
            })
        )
        
        # 各类型脚本数量
        colors = ['#ff9800', '#9c27b0', '#607d8b', '#795548', '#009688']
        for i, (data_type, count) in enumerate(type_counts.items()):
            color = colors[i % len(colors)]
            cards.append(
                html.Div([
                    html.H3(str(count), style={'color': color, 'margin': '0'}),
                    html.P(f"{data_type}脚本", style={'margin': '5px 0 0 0'})
                ], className="overview-card", 
                style={
                    'border': f'2px solid {color}', 
                    'border-radius': '8px', 
                    'padding': '20px', 
                    'text-align': 'center', 
                    'margin': '10px',
                    'background': 'white',
                    'box-shadow': '0 2px 4px rgba(0,0,0,0.1)'
                })
            )
        
        return html.Div(cards, style={
            'display': 'flex', 
            'flex-wrap': 'wrap', 
            'justify-content': 'space-around',
            'margin-bottom': '20px'
        })
        
    except Exception as e:
        return html.Div(f"加载脚本概览时发生错误: {str(e)}", className="alert alert-danger")


def create_crawler_scripts_table():
    """创建爬虫脚本信息表格"""
    try:
        scripts_info = load_crawler_scripts_info()
        
        if not scripts_info:
            return html.Div("未找到爬虫脚本信息", className="alert alert-warning")
        
        # 准备表格数据
        table_data = []
        for script_file, script_info in scripts_info.items():
            # 从JSON获取激活状态
            is_active = script_info.get('is_active', True)
            status = '✅ 激活' if is_active else '❌ 未激活'
            
            # 从JSON获取数据类型
            data_type = get_data_type_display_name(script_info.get('data_type', 'other'))
            
            table_data.append({
                '脚本文件': script_file,
                '类名': script_info.get('class_name', '-'),
                '功能描述': script_info.get('description', '-'),
                '数据源类型': data_type,
                '状态': status,
                'is_active_raw': is_active  # 用于条件样式
            })
        
        return dash_table.DataTable(
            id='crawler-scripts-table',
            columns=[
                {'name': '脚本文件', 'id': '脚本文件'},
                {'name': '类名', 'id': '类名'},
                {'name': '功能描述', 'id': '功能描述'},
                {'name': '数据源类型', 'id': '数据源类型'},
                {'name': '状态', 'id': '状态'}
            ],
            data=table_data,
            style_cell={
                'textAlign': 'left',
                'padding': '12px',
                'fontFamily': 'Arial, sans-serif',
                'fontSize': '14px',
                'whiteSpace': 'normal',
                'height': 'auto'
            },
            style_header={
                'backgroundColor': '#e3f2fd',
                'fontWeight': 'bold',
                'color': '#1976d2',
                'textAlign': 'center'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#f8f9fa'
                },
                {
                    'if': {
                        'filter_query': '{状态} contains "激活"',
                        'column_id': '状态'
                    },
                    'backgroundColor': '#d4edda',
                    'color': '#155724',
                    'fontWeight': 'bold'
                },
                {
                    'if': {
                        'filter_query': '{状态} contains "未激活"',
                        'column_id': '状态'
                    },
                    'backgroundColor': '#f8d7da',
                    'color': '#721c24',
                    'fontWeight': 'bold'
                }
            ],
            style_table={
                'overflowX': 'auto'
            },
            page_size=10,
            sort_action='native',
            filter_action='native'
        )
        
    except Exception as e:
        return html.Div(f"加载爬虫脚本信息时发生错误: {str(e)}", className="alert alert-danger")


def get_data_type_display_name(data_type):
    """将数据类型转换为显示名称"""
    type_mapping = {
        'fund': '基金',
        'stock': '股票', 
        'forex': '外汇',
        'bond_rate': '债券',
        'etf': 'ETF',
        'other': '其他'
    }
    return type_mapping.get(data_type, '其他')


def get_data_source_type_from_filename(filename):
    """根据文件名推断数据源类型（已弃用，保留兼容性）"""
    if 'fund' in filename:
        return '基金'
    elif 'stock' in filename:
        return '股票'
    elif 'forex' in filename:
        return '外汇'
    elif 'bond' in filename:
        return '债券'
    elif 'etf' in filename:
        return 'ETF'
    else:
        return '其他'