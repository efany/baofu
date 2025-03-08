import sys
import os
import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_utils.funds_utils import get_fund_data_by_name, calculate_max_drawdown, calculate_adjusted_nav
from database.db_funds_nav import DBFundsNav
from database.db_funds import DBFunds
from database.db_strategys import DBStrategys
from task_dash.utils import get_date_range, get_data_briefs
from task_dash.datas.data import create_data_generator
from task_dash.datas.data_generator import TableData

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

def create_table(table_data: TableData) -> html.Div:
    """创建表格组件"""
    return html.Div([
        html.H4(table_data['name'], style={
            'margin': '10px 0',
            'padding': '5px 10px',
            'backgroundColor': '#f0f0f0',
            'borderRadius': '4px'
        }),
        html.Table([
            # 表头
            html.Thead(
                html.Tr([
                    html.Th(col, style={
                        'padding': '8px',
                        'backgroundColor': '#e0e0e0',
                        'border': '1px solid #ddd',
                        'textAlign': 'center'
                    }) for col in table_data['headers']
                ])
            ),
            # 数据行
            html.Tbody([
                html.Tr([
                    html.Td(cell, style={
                        'padding': '8px',
                        'border': '1px solid #ddd',
                        'textAlign': 'center'
                    }) for cell in row
                ]) for row in table_data['data']
            ])
        ], style={
            'width': '100%',
            'borderCollapse': 'collapse',
            'marginBottom': '20px',
            'backgroundColor': 'white',
            'boxShadow': '0 1px 3px rgba(0,0,0,0.2)'
        })
    ], style={
        'marginBottom': '20px',
        'padding': '10px',
        'backgroundColor': '#f9f9f9',
        'borderRadius': '5px',
        'border': '1px solid #ddd'
    })

def register_single_fund_callbacks(app, mysql_db):
    # 添加类型切换的回调
    @app.callback(
        [Output('fund-dropdown', 'options'),
         Output('fund-dropdown', 'value')],  # 添加value作为输出
        [Input('type-dropdown', 'value')]
    )
    def update_data_options(selected_type):
        """更新数据选项"""
        try:
            if selected_type == 'fund':
                data = DBFunds(mysql_db).get_all_funds()
            elif selected_type == 'strategy':
                data = DBStrategys(mysql_db).get_all_strategies()
            elif selected_type == 'stock':
                # TODO: 添加股票数据获取逻辑
                data = pd.DataFrame()
            else:
                data = pd.DataFrame()
            
            options = get_data_briefs(selected_type, data)
            # 如果有选项，返回第一个选项的值作为默认值，否则返回空字符串
            default_value = options[0]['value'] if options else ''
            
            return options, default_value
            
        except Exception as e:
            logger.error(f"Error in update_data_options: {str(e)}")
            return [], ''

    @app.callback(
        [Output('fund-value-graph', 'figure'),
         Output('fund-summary-table', 'children'),
         Output('fund-tables-left-column', 'children'),
         Output('fund-tables-right-column', 'children')],
        [Input('type-dropdown', 'value'),
         Input('fund-dropdown', 'value'),
         Input('line-options', 'value'),
         Input('time-range-dropdown', 'value')]
    )
    def update_fund_display(data_type, selected_data, line_options, time_range):
        """更新数据展示"""
        try:
            # 获取日期范围
            start_date, end_date = get_date_range(time_range)
            
            # 创建数据生成器
            generator = create_data_generator(
                data_type=data_type,
                data_id=selected_data,
                mysql_db=mysql_db,
                start_date=start_date,
                end_date=end_date
            )

            if generator is None:
                return go.Figure(), html.Div("创建数据生成器失败", style={'color': 'red'}), [], []

            # 获取数据
            summary_data = generator.get_summary_data()
            logger.info(f"get summary_data")
            chart_data = generator.get_chart_data()
            logger.info(f"get chart_data")
            extra_datas = generator.get_extra_datas()
            logger.info(f"get extra_datas")

            if not chart_data:
                return go.Figure(), html.Div("未找到数据", style={'color': 'red'}), [], []
            
            # 处理图表数据
            for option in line_options:
                extra_chart_data = generator.get_extra_chart_data(option)
                chart_data.extend(extra_chart_data)
            
            # 创建图表
            figure = {
                'data': chart_data,
                'layout': {
                    'title': f'基金 {selected_data} 的净值和分红数据',
                    'xaxis': {'title': '日期'},
                    'yaxis': {'title': '净值'}
                }
            }
            
            # 创建摘要表格
            summary_table = create_summary_table(summary_data)
            
            # 将额外数据表格分配到两列
            left_tables = []
            right_tables = []
            for i, table_data in enumerate(extra_datas):
                if i % 2 == 0:
                    left_tables.append(create_table(table_data))
                else:
                    right_tables.append(create_table(table_data))
            
            return figure, summary_table, left_tables, right_tables
            
        except Exception as e:
            print(f"Error in update_fund_display: {str(e)}")
            return go.Figure(), html.Div(f"发生错误: {str(e)}", style={'color': 'red'}), [], [] 