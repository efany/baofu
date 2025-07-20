from dash import callback, Input, Output, State, html, dash_table
import pandas as pd
import plotly.graph_objs as go
import plotly.express as px
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any
import json

from task_dash.pages.correlation_analysis import (
    get_all_products, 
    get_product_data, 
    calculate_correlation_matrix,
    calculate_rolling_correlation
)
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils import get_stock_name


def get_display_name(product_id: str, product_names: Dict[str, str]) -> str:
    """获取产品显示名称，对于股票使用中文名称"""
    if product_id.startswith('stock_'):
        # 获取股票代码
        stock_code = product_id.replace('stock_', '')
        # 获取中文名称
        chinese_name = get_stock_name(stock_code)
        return f"股票: {chinese_name} ({stock_code})"
    else:
        # 其他产品类型使用原始映射
        return product_names.get(product_id, product_id)


def register_correlation_analysis_callbacks(app, mysql_db):
    """注册相关性分析页面的回调函数"""
    
    @callback(
        [Output('benchmark-product-dropdown', 'options'),
         Output('compare-products-dropdown', 'options'),
         Output('product-options-store', 'data')],
        [Input('url', 'pathname')]
    )
    def update_product_options(pathname):
        """更新产品选择选项"""
        if pathname != '/correlation_analysis':
            return [], [], {}
        
        products = get_all_products(mysql_db)
        
        options = []
        for product in products:
            options.append({
                'label': product['label'],
                'value': product['value']
            })
        
        return options, options, {'products': products}
    
    @callback(
        Output('custom-date-div', 'style'),
        [Input('time-window-dropdown', 'value')]
    )
    def toggle_custom_date_picker(time_window):
        """切换自定义时间选择器的显示"""
        if time_window == 'custom':
            return {'display': 'block'}
        return {'display': 'none'}
    
    @callback(
        Output('rolling-window-size-div', 'style'),
        [Input('rolling-window-checkbox', 'value')]
    )
    def toggle_rolling_window_size(rolling_checkbox):
        """切换滚动窗口大小输入的显示"""
        if 'enable' in rolling_checkbox:
            return {'display': 'block'}
        return {'display': 'none'}
    
    @callback(
        [Output('correlation-matrix-table', 'children'),
         Output('correlation-heatmap', 'figure'),
         Output('correlation-scatter', 'figure'),
         Output('rolling-correlation', 'figure'),
         Output('correlation-data-store', 'data')],
        [Input('correlation-analyze-button', 'n_clicks')],
        [State('benchmark-product-dropdown', 'value'),
         State('compare-products-dropdown', 'value'),
         State('time-window-dropdown', 'value'),
         State('custom-date-range', 'start_date'),
         State('custom-date-range', 'end_date'),
         State('correlation-method-dropdown', 'value'),
         State('data-processing-dropdown', 'value'),
         State('rolling-window-checkbox', 'value'),
         State('rolling-window-size', 'value'),
         State('product-options-store', 'data')]
    )
    def analyze_correlation(n_clicks, benchmark_product, compare_products, time_window,
                           custom_start_date, custom_end_date, correlation_method,
                           data_processing, rolling_checkbox, rolling_window_size,
                           product_options_data):
        """执行相关性分析"""
        if not n_clicks or not benchmark_product or not compare_products:
            empty_fig = go.Figure()
            return html.Div("请选择基准产品和对比产品"), empty_fig, empty_fig, empty_fig, {}
        
        # 确定时间范围
        end_date = datetime.now()
        if time_window == 'custom':
            start_date = datetime.strptime(custom_start_date, '%Y-%m-%d')
            end_date = datetime.strptime(custom_end_date, '%Y-%m-%d')
        elif time_window == 'all':
            start_date = None
            end_date = None
        else:
            start_date = end_date - timedelta(days=int(time_window))
        
        # 获取产品数据
        all_products = [benchmark_product] + compare_products
        data_dict = {}
        
        for product_id in all_products:
            df = get_product_data(mysql_db, product_id, start_date, end_date)
            if not df.empty:
                data_dict[product_id] = df
        
        if len(data_dict) < 2:
            empty_fig = go.Figure()
            return html.Div("数据不足，请检查产品选择和时间范围"), empty_fig, empty_fig, empty_fig, {}
        
        # 计算相关性矩阵
        correlation_matrix = calculate_correlation_matrix(data_dict, correlation_method, data_processing)
        
        if correlation_matrix.empty:
            empty_fig = go.Figure()
            return html.Div("无法计算相关性矩阵"), empty_fig, empty_fig, empty_fig, {}
        
        # 创建相关性系数表格
        correlation_table = create_correlation_table(correlation_matrix, product_options_data)
        
        # 创建热力图
        heatmap_fig = create_correlation_heatmap(correlation_matrix, product_options_data)
        
        # 创建散点图
        scatter_fig = create_correlation_scatter(data_dict, benchmark_product, compare_products[0], 
                                               data_processing, product_options_data)
        
        # 创建滚动相关性图
        rolling_fig = go.Figure()
        if 'enable' in rolling_checkbox:
            rolling_corr_df = calculate_rolling_correlation(data_dict, benchmark_product, 
                                                          rolling_window_size, correlation_method,
                                                          data_processing)
            rolling_fig = create_rolling_correlation_chart(rolling_corr_df, product_options_data)
        
        # 存储数据
        correlation_data = {
            'correlation_matrix': correlation_matrix.to_dict(),
            'data_dict': {k: v.to_dict() for k, v in data_dict.items()},
            'settings': {
                'benchmark_product': benchmark_product,
                'compare_products': compare_products,
                'time_window': time_window,
                'correlation_method': correlation_method,
                'data_processing': data_processing
            }
        }
        
        return correlation_table, heatmap_fig, scatter_fig, rolling_fig, correlation_data


def create_correlation_table(correlation_matrix: pd.DataFrame, product_options_data: Dict) -> html.Div:
    """创建相关性系数表格"""
    if correlation_matrix.empty:
        return html.Div("无数据")
    
    # 获取产品名称映射
    product_names = {}
    if product_options_data and 'products' in product_options_data:
        for product in product_options_data['products']:
            product_names[product['value']] = product['label']
    
    # 重命名行列索引，对于股票使用中文名称
    renamed_matrix = correlation_matrix.copy()
    renamed_matrix.index = [get_display_name(idx, product_names) for idx in renamed_matrix.index]
    renamed_matrix.columns = [get_display_name(col, product_names) for col in renamed_matrix.columns]
    
    # 转换为表格数据
    table_data = []
    for i, row_name in enumerate(renamed_matrix.index):
        row_data = {'产品': row_name}
        for j, col_name in enumerate(renamed_matrix.columns):
            value = renamed_matrix.iloc[i, j]
            if pd.notna(value):
                row_data[col_name] = f"{value:.4f}"
            else:
                row_data[col_name] = "N/A"
        table_data.append(row_data)
    
    columns = [{'name': '产品', 'id': '产品'}]
    for col in renamed_matrix.columns:
        columns.append({'name': col, 'id': col})
    
    return dash_table.DataTable(
        data=table_data,
        columns=columns,
        style_cell={'textAlign': 'center'},
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }
        ],
        style_header={
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold'
        }
    )


def create_correlation_heatmap(correlation_matrix: pd.DataFrame, product_options_data: Dict) -> go.Figure:
    """创建相关性热力图"""
    if correlation_matrix.empty:
        return go.Figure()
    
    # 获取产品名称映射
    product_names = {}
    if product_options_data and 'products' in product_options_data:
        for product in product_options_data['products']:
            product_names[product['value']] = product['label']
    
    # 重命名行列索引，对于股票使用中文名称
    renamed_matrix = correlation_matrix.copy()
    renamed_matrix.index = [get_display_name(idx, product_names) for idx in renamed_matrix.index]
    renamed_matrix.columns = [get_display_name(col, product_names) for col in renamed_matrix.columns]
    
    fig = go.Figure(data=go.Heatmap(
        z=renamed_matrix.values,
        x=list(renamed_matrix.columns),
        y=list(renamed_matrix.index),
        colorscale='RdYlBu',
        zmid=0,
        text=renamed_matrix.values,
        texttemplate="%{text:.3f}",
        textfont={"size": 12},
        hoverongaps=False
    ))
    
    fig.update_layout(
        title="产品相关性热力图",
        xaxis_title="产品",
        yaxis_title="产品",
        height=600,
        xaxis=dict(
            tickangle=-45,  # 旋转标签45度
            tickfont=dict(size=10),  # 设置字体大小
            tickmode='linear',  # 线性刻度模式
            showticklabels=True,  # 显示刻度标签
            side='bottom'
        ),
        yaxis=dict(
            tickfont=dict(size=10),  # 设置字体大小
            tickmode='linear',  # 线性刻度模式
            showticklabels=True,  # 显示刻度标签
            side='left'
        ),
        margin=dict(l=150, r=50, t=80, b=150)  # 调整边距为标签留出空间
    )
    
    return fig


def create_correlation_scatter(data_dict: Dict[str, pd.DataFrame], benchmark_id: str, 
                             compare_id: str, data_processing: str,
                             product_options_data: Dict) -> go.Figure:
    """创建相关性散点图"""
    if benchmark_id not in data_dict or compare_id not in data_dict:
        return go.Figure()
    
    benchmark_df = data_dict[benchmark_id].copy()
    compare_df = data_dict[compare_id].copy()
    
    # 数据处理
    if data_processing == 'returns':
        benchmark_df['value'] = benchmark_df['value'].pct_change()
        compare_df['value'] = compare_df['value'].pct_change()
    elif data_processing == 'normalized':
        benchmark_df['value'] = benchmark_df['value'] / benchmark_df['value'].iloc[0]
        compare_df['value'] = compare_df['value'] / compare_df['value'].iloc[0]
    
    # 合并数据
    merged = pd.merge(benchmark_df[['date', 'value']], 
                     compare_df[['date', 'value']], 
                     on='date', how='inner', suffixes=('_benchmark', '_compare'))
    
    if merged.empty:
        return go.Figure()
    
    # 去除空值
    merged = merged.dropna()
    
    # 计算相关系数
    correlation = merged['value_benchmark'].corr(merged['value_compare'])
    
    # 获取产品名称
    product_names = {}
    if product_options_data and 'products' in product_options_data:
        for product in product_options_data['products']:
            product_names[product['value']] = product['label']
    
    benchmark_name = get_display_name(benchmark_id, product_names)
    compare_name = get_display_name(compare_id, product_names)
    
    fig = go.Figure()
    
    # 添加散点
    fig.add_trace(go.Scatter(
        x=merged['value_benchmark'],
        y=merged['value_compare'],
        mode='markers',
        name=f'相关性: {correlation:.4f}',
        text=merged['date'].dt.strftime('%Y-%m-%d'),
        hovertemplate='<b>%{text}</b><br>' +
                     f'{benchmark_name}: %{{x:.4f}}<br>' +
                     f'{compare_name}: %{{y:.4f}}<extra></extra>'
    ))
    
    # 添加趋势线
    if len(merged) > 1:
        z = np.polyfit(merged['value_benchmark'], merged['value_compare'], 1)
        p = np.poly1d(z)
        fig.add_trace(go.Scatter(
            x=merged['value_benchmark'],
            y=p(merged['value_benchmark']),
            mode='lines',
            name=f'趋势线 (R={correlation:.4f})',
            line=dict(color='red', dash='dash')
        ))
    
    # 截断过长的标题以避免图表过度缩小
    def truncate_name(name, max_length=20):
        if len(name) > max_length:
            return name[:max_length] + "..."
        return name
    
    benchmark_name_short = truncate_name(benchmark_name)
    compare_name_short = truncate_name(compare_name)
    
    fig.update_layout(
        title=f"相关性散点图: {benchmark_name_short} vs {compare_name_short}",
        xaxis_title=benchmark_name_short,
        yaxis_title=compare_name_short,
        height=500,
        xaxis=dict(
            tickfont=dict(size=10),
            title_font=dict(size=12)
        ),
        yaxis=dict(
            tickfont=dict(size=10),
            title_font=dict(size=12)
        ),
        margin=dict(l=80, r=50, t=80, b=80)
    )
    
    return fig


def create_rolling_correlation_chart(rolling_corr_df: pd.DataFrame, product_options_data: Dict) -> go.Figure:
    """创建滚动相关性图表"""
    if rolling_corr_df.empty:
        return go.Figure()
    
    # 获取产品名称映射
    product_names = {}
    if product_options_data and 'products' in product_options_data:
        for product in product_options_data['products']:
            product_names[product['value']] = product['label']
    
    fig = go.Figure()
    
    # 截断过长的产品名称
    def truncate_name(name, max_length=25):
        if len(name) > max_length:
            return name[:max_length] + "..."
        return name
    
    # 为每个产品添加滚动相关性线
    for product_id in rolling_corr_df['product'].unique():
        product_data = rolling_corr_df[rolling_corr_df['product'] == product_id]
        product_name = get_display_name(product_id, product_names)
        product_name_short = truncate_name(product_name)
        
        fig.add_trace(go.Scatter(
            x=product_data['date'],
            y=product_data['correlation'],
            mode='lines',
            name=product_name_short,
            hovertemplate=f'<b>{product_name}</b><br>' +
                         '日期: %{x}<br>' +
                         '相关系数: %{y:.4f}<extra></extra>'
        ))
    
    # 添加参考线
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    fig.add_hline(y=0.5, line_dash="dot", line_color="green", opacity=0.5)
    fig.add_hline(y=-0.5, line_dash="dot", line_color="red", opacity=0.5)
    
    fig.update_layout(
        title="滚动相关性分析",
        xaxis_title="日期",
        yaxis_title="相关系数",
        height=500,
        yaxis=dict(range=[-1, 1]),
        xaxis=dict(
            tickfont=dict(size=10),
            title_font=dict(size=12)
        ),
        legend=dict(
            font=dict(size=10),
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        ),
        margin=dict(l=60, r=150, t=80, b=60)  # 为图例留出右侧空间
    )
    
    return fig