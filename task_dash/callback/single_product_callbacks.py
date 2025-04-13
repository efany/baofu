import sys
import os
import dash
from dash import html, dcc
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
import pandas as pd
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_utils.data_utils import calculate_return_rate
from database.db_funds import DBFunds
from database.db_strategys import DBStrategys
from database.db_stocks import DBStocks
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

def register_single_product_callbacks(app, mysql_db):
    # 添加类型切换的回调
    @app.callback(
        [Output('product-dropdown', 'options'),
         Output('product-dropdown', 'value')],  # 添加value作为输出
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
                data = DBStocks(mysql_db).get_all_stocks()
            else:
                data = pd.DataFrame()
            
            options = get_data_briefs(selected_type, data)
            # 如果有选项，返回第一个选项的值作为默认值，否则返回空字符串
            default_value = options[0]['value'] if options else ''
            
            return options, default_value
            
        except Exception as e:
            logger.error(f"Error in update_data_options: {str(e)}")
            return [], ''

    # 添加参数配置回调
    @app.callback(
        Output('params-config-container', 'children'),
        [Input('product-dropdown', 'value'),
         Input('type-dropdown', 'value')]
    )
    def update_params_config(selected_data, data_type):
        """更新参数配置界面"""
        if not selected_data or not data_type:
            return html.Div()
        
        try:
            # 创建数据生成器
            generator = create_data_generator(
                data_type=data_type,
                data_id=selected_data,
                mysql_db=mysql_db,
                start_date=None,
                end_date=None
            )
            
            if generator is None:
                return html.Div("无法获取参数配置", style={'color': 'red'})
            
            # 获取参数配置
            params_config = generator.get_params_config()
            if not params_config:
                return html.Div("该产品无可配置参数", style={'color': 'gray', 'padding': '10px'})
            
            # 创建参数输入界面
            params_inputs = []
            for param in params_config:
                param_id = f"param-{param['name']}"
                
                # 根据参数类型创建不同的输入组件
                if param['type'] == 'number':
                    input_component = dcc.Input(
                        id=param_id,
                        type='number',
                        value=param.get('value', param.get('default')),
                        min=param.get('min'),
                        max=param.get('max'),
                        step=param.get('step', 1),
                        style={'width': '150px'}
                    )
                elif param['type'] == 'select':
                    input_component = dcc.Dropdown(
                        id=param_id,
                        options=param.get('options', []),
                        value=param.get('value', param.get('default')),
                        style={'width': '150px'}
                    )
                elif param['type'] == 'float':
                    input_component = dcc.Input(
                        id=param_id,
                        type='number',
                        value=param.get('value', param.get('default')),
                        min=param.get('min'),
                        max=param.get('max'),
                        step=param.get('step', 0.01),
                        style={'width': '150px'}
                    )
                else:  # text类型或其他类型
                    input_component = dcc.Input(
                        id=param_id,
                        type='text',
                        value=param.get('value', param.get('default')),
                        placeholder=param.get('placeholder', ''),
                        style={'width': '150px'}
                    )
                
                params_inputs.append(
                    html.Div([
                        html.Label(
                            param['label'], 
                            style={
                                'marginRight': '10px',
                                'minWidth': '120px',
                                'display': 'inline-block',
                                'textAlign': 'right'
                            }
                        ),
                        input_component,
                        html.Div(
                            param.get('description', ''),
                            style={
                                'marginLeft': '10px',
                                'color': '#666',
                                'fontSize': '12px'
                            }
                        ) if param.get('description') else None
                    ], style={
                        'marginBottom': '10px',
                        'display': 'flex',
                        'alignItems': 'center'
                    })
                )
            
            return html.Div(params_inputs)
            
        except Exception as e:
            logger.error(f"Error in update_params_config: {str(e)}")
            return html.Div(f"参数配置加载失败: {str(e)}", style={'color': 'red'})

    @app.callback(
        [Output('product-value-graph', 'figure'),
         Output('product-summary-table', 'children'),
         Output('product-tables-left-column', 'children'),
         Output('product-tables-right-column', 'children')],
        [Input('query-button', 'n_clicks')],
        [State('type-dropdown', 'value'),
         State('product-dropdown', 'value'),
         State('line-options', 'value'),
         State('time-range-dropdown', 'value'),
         State('params-config-container', 'children')]  # 添加参数配置状态
    )
    def update_product_display(n_clicks, data_type, selected_data, line_options, time_range, params_config):
        """更新数据展示"""
        if not n_clicks:  # 初始加载时不触发更新
            raise dash.exceptions.PreventUpdate
        
        try:
            # 获取日期范围
            start_date, end_date = get_date_range(time_range)

            logger.info(f"更新数据时间范围: {start_date} {end_date}")
            
            # 解析参数配置
            params = {}
            if params_config and isinstance(params_config, dict):
                for param_div in params_config.get('props', {}).get('children', []):
                    if isinstance(param_div, dict) and 'props' in param_div:
                        input_props = param_div['props']
                        for child in input_props.get('children', []):
                            if isinstance(child, dict) and child.get('props', {}).get('id', '').startswith('param-'):
                                param_name = child['props']['id'].replace('param-', '')
                                param_value = child['props'].get('value')
                                if param_value is not None:
                                    params[param_name] = param_value
                                    logger.info(f"更新参数: {param_name} = {param_value}")
            
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

            # 更新参数
            if params:
                generator.update_params(params)

            if not generator.load():
                return go.Figure(), html.Div("数据加载失败", style={'color': 'red'}), [], []

            # 获取数据
            summary_data = generator.get_summary_data()
            chart_data = generator.get_chart_data()
            extra_datas = generator.get_extra_datas()

            if not chart_data:
                return go.Figure(), html.Div("未找到数据", style={'color': 'red'}), [], []
            
            # 处理图表数据
            for option in line_options:
                extra_chart_data = generator.get_extra_chart_data(option)
                chart_data.extend(extra_chart_data)
            
            # 创建图表
            title = '基金净值和分红数据' if data_type == 'fund' else '策略净值数据' if data_type == 'strategy' else '股票K线数据'
            
            # 根据数据类型设置不同的x轴配置
            xaxis_config = {
                'title': '日期',
                'rangeslider': {'visible': False}
            }
            
            if data_type == 'stock':
                xaxis_config.update({
                    'tickmode': 'auto',
                    'nticks': 20,  # 控制显示的刻度数量
                    'tickangle': -45,  # 标签旋转45度
                    'showgrid': True,
                    'gridcolor': '#f0f0f0'
                })
            
            figure = {
                'data': chart_data,
                'layout': {
                    'title': f'{title} - {selected_data}',
                    'xaxis': xaxis_config,
                    'yaxis': {'title': '价格' if data_type == 'stock' else '净值'},
                    'plot_bgcolor': 'white',
                    'hovermode': 'x unified'
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
            print(f"Error in update_product_display: {str(e)}")
            return go.Figure(), html.Div(f"发生错误: {str(e)}", style={'color': 'red'}), [], [] 