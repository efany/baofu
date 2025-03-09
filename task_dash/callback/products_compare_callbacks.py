from dash import Input, Output, html, State, callback_context, no_update
import plotly.graph_objs as go
import dash_bootstrap_components as dbc
from typing import List, Dict, Any
from loguru import logger
import sys
import os
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_dash.datas.data import create_data_generator
from database.db_funds import DBFunds
from database.db_strategys import DBStrategys
from task_dash.utils import get_date_range, get_data_briefs
from task_dash.callback.single_product_callbacks import create_table

def create_summary_table(table_data):
    """创建摘要表格"""
    children = []
    for label, value in table_data:
        children.append(
            html.Div([
                # 标签
                html.Span(label, style={
                    'color': '#666',
                    'fontWeight': 'bold',
                    'padding': '4px 8px',
                    'backgroundColor': '#e0e0e0',
                    'borderRadius': '4px',
                    'marginRight': '8px',
                    'display': 'inline-block',
                    'minWidth': '80px',
                    'textAlign': 'right'
                }),
                # 值
                html.Span(value, style={
                    'color': '#333',
                    'padding': '4px 8px',
                    'backgroundColor': '#f0f0f0',
                    'borderRadius': '4px',
                    'display': 'inline-block',
                    'flex': '1',
                    'fontWeight': '500'
                })
            ], style={
                'display': 'inline-block',
                'marginRight': '10px',
                'padding': '5px',
                'border': '1px solid #ddd',  # 添加边框
                'borderRadius': '3px',  # 添加圆角
                'backgroundColor': '#f5f5f5',  # 添加背景色
            })
        )
    return html.Div(children, style={
        'padding': '2px',
        'border': '1px solid #ddd',
        'borderRadius': '5px',
        'backgroundColor': '#f9f9f9',
    }) 

def register_products_compare_callbacks(app, mysql_db):
    @app.callback(
        [Output('fund-dropdown', 'options'),
         Output('strategy-dropdown', 'options')],
        Input('url', 'pathname')
    )
    def update_dropdowns_options(_):
        """初始化下拉框选项"""
        try:
            # 获取基金选项
            fund_data = DBFunds(mysql_db).get_all_funds()
            fund_options = get_data_briefs('fund', fund_data)
            
            # 获取策略选项
            strategy_data = DBStrategys(mysql_db).get_all_strategies()
            strategy_options = get_data_briefs('strategy', strategy_data)
            
            return fund_options, strategy_options
            
        except Exception as e:
            logger.error(f"Error in update_dropdowns_options: {str(e)}")
            return [], []

    @app.callback(
        [Output('compare-value-graph', 'figure'),
         Output('products-summary', 'children'),
         Output('compare-tables-container', 'children')],
        [Input('fund-dropdown', 'value'),
         Input('strategy-dropdown', 'value'),
         Input('compare-time-range', 'value'),
         Input('compare-line-options', 'value')]
    )
    def update_comparison(fund_values, strategy_values, time_range, line_options):
        """更新对比图表和数据"""
        try:
            # 获取日期范围
            start_date, end_date = get_date_range(time_range)
            
            # 创建图表数据
            figure_data = []
            summary_children = []
            product_tables = []  # 存储每个产品的表格数据
            
            # 计算总产品数量，用于计算每列宽度
            total_products = len(fund_values or []) + len(strategy_values or [])
            if total_products == 0:
                return go.Figure(), [], []
            
            # 计算每列宽度百分比
            column_width = f"{100 / total_products}%"
            
            # 处理基金数据
            if fund_values:
                for fund_id in fund_values:
                    generator = create_data_generator(
                        data_type='fund',
                        data_id=fund_id,
                        mysql_db=mysql_db,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if generator:
                        # 添加摘要信息
                        summary_data = generator.get_summary_data()
                        if summary_data:
                            summary_children.append(
                                html.Div([
                                    create_summary_table(summary_data)
                                ], style={'marginBottom': '15px'})
                            )
                        
                        # 处理图表数据
                        chart_data = generator.get_chart_data(normalize=True)
                        if chart_data:
                            fund_figure_data = []
                            fund_figure_data.append(chart_data[0])
                            
                            for option in line_options:
                                extra_data = generator.get_extra_chart_data(option, normalize=True)
                                fund_figure_data.extend(extra_data)
                            
                            for data in fund_figure_data:
                                if 'name' in data:
                                    data['name'] = f"{data['name']} (f-{fund_id})"
                            
                            figure_data.extend(fund_figure_data)
                            
                            # 获取统计数据
                            extra_datas = generator.get_extra_datas()
                            if extra_datas:
                                # 创建产品列
                                product_tables.append(
                                    html.Div([
                                        html.H6(f"基金 {fund_id}", style={
                                            'color': '#1890ff',
                                            'marginBottom': '10px',
                                            'textAlign': 'center'
                                        }),
                                        html.Div([
                                            create_table(table_data)
                                            for table_data in extra_datas
                                        ], style={
                                            'display': 'flex',
                                            'flexDirection': 'column',
                                            'gap': '10px'
                                        })
                                    ], style={
                                        'width': column_width,  # 使用计算的宽度
                                        'padding': '10px',
                                        'border': '1px solid #e8e8e8',
                                        'borderRadius': '4px',
                                        'backgroundColor': '#fff'
                                    })
                                )
            
            # 处理策略数据
            if strategy_values:
                for strategy_id in strategy_values:
                    generator = create_data_generator(
                        data_type='strategy',
                        data_id=strategy_id,
                        mysql_db=mysql_db,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if generator:
                        # 添加摘要信息
                        summary_data = generator.get_summary_data()
                        if summary_data:
                            summary_children.append(
                                html.Div([
                                    create_summary_table(summary_data)
                                ], style={'marginBottom': '15px'})
                            )
                        
                        # 处理图表数据
                        chart_data = generator.get_chart_data(normalize=True)
                        if chart_data:
                            strategy_figure_data = []
                            strategy_figure_data.append(chart_data[0])
                            
                            for option in line_options:
                                extra_data = generator.get_extra_chart_data(option, normalize=True)
                                strategy_figure_data.extend(extra_data)
                            
                            for data in strategy_figure_data:
                                if 'name' in data:
                                    data['name'] = f"{data['name']} (st-{strategy_id})"

                            figure_data.extend(strategy_figure_data)
                            
                            # 获取统计数据
                            extra_datas = generator.get_extra_datas()
                            if extra_datas:
                                # 创建产品列
                                product_tables.append(
                                    html.Div([
                                        html.H6(f"策略 {strategy_id}", style={
                                            'color': '#52c41a',
                                            'marginBottom': '10px',
                                            'textAlign': 'center'
                                        }),
                                        html.Div([
                                            create_table(table_data)
                                            for table_data in extra_datas
                                        ], style={
                                            'display': 'flex',
                                            'flexDirection': 'column',
                                            'gap': '10px'
                                        })
                                    ], style={
                                        'width': column_width,  # 使用计算的宽度
                                        'padding': '10px',
                                        'border': '1px solid #e8e8e8',
                                        'borderRadius': '4px',
                                        'backgroundColor': '#fff'
                                    })
                                )
            
            # 创建图表
            figure = {
                'data': figure_data,
                'layout': {
                    'title': '产品净值对比',
                    'xaxis': {'title': '日期'},
                    'yaxis': {'title': '净值'},
                    'hovermode': 'x unified',
                    'legend': {
                        'orientation': 'h',
                        'yanchor': 'bottom',
                        'y': 1.02,
                        'xanchor': 'right',
                        'x': 1
                    }
                }
            }
            
            # 创建表格容器
            tables_container = html.Div(
                product_tables,
                style={
                    'display': 'flex',
                    'flexWrap': 'nowrap',  # 不换行
                    'gap': '20px',
                    'width': '100%',  # 占满容器宽度
                    'marginTop': '20px',
                    'padding': '0 20px'  # 添加左右内边距
                }
            ) if product_tables else []
            
            return figure, summary_children if len(summary_children) > 1 else [], tables_container
            
        except Exception as e:
            logger.error(f"Error in update_comparison: {str(e)}")
            return go.Figure(), [], []