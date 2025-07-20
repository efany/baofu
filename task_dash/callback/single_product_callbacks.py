import sys
import os
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
import pandas as pd
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_utils.data_utils import calculate_return_rate
from database.db_funds import DBFunds
from database.db_strategys import DBStrategys
from database.db_stocks import DBStocks
from database.db_forex_day_hist import DBForexDayHist
from database.db_bond_rate import DBBondRate
from database.db_index_hist import DBIndexHist
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils import get_date_range, get_data_briefs
from task_dash.datas.data import create_data_generator
from task_dash.datas.data_generator import TableData
from task_dash.common.cache_manager import cache_manager

def create_summary_table(table_data):
    """创建摘要表格"""
    if not table_data:
        return dbc.Alert("暂无摘要数据", color="info")
    
    # 将数据分组，每行3个指标
    rows = []
    current_row = []
    
    for i, (label, value) in enumerate(table_data):
        current_row.append(
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H6(label, className="card-subtitle mb-2 text-muted"),
                        html.H4(value, className="card-title mb-0", 
                               style={'color': '#2c3e50', 'fontWeight': 'bold'})
                    ], className="text-center")
                ], className="h-100")
            ], width=4)
        )
        
        if (i + 1) % 3 == 0 or i == len(table_data) - 1:
            rows.append(dbc.Row(current_row, className="mb-3"))
            current_row = []
    
    return dbc.Card([
        dbc.CardHeader([
            html.H4("产品摘要", className="mb-0", style={'color': '#34495e'})
        ]),
        dbc.CardBody(rows)
    ], className="mb-4") 

def create_table_from_pd(table_data: TableData) -> dbc.Card:
    """创建表格组件"""
    if table_data['pd_data'].empty:
        return dbc.Card([
            dbc.CardHeader(html.H5(table_data['name'], className="mb-0")),
            dbc.CardBody([
                dbc.Alert("暂无数据", color="info")
            ])
        ], className="mb-3")
    
    # 创建表头
    headers = [html.Th(col, className="text-center") for col in table_data['pd_data'].columns]
    
    # 创建表格数据
    rows = []
    for _, row in table_data['pd_data'].iterrows():
        cells = [html.Td(str(cell), className="text-center") for cell in row]
        rows.append(html.Tr(cells))
    
    table = dbc.Table([
        html.Thead(html.Tr(headers), className="table-dark"),
        html.Tbody(rows)
    ], striped=True, bordered=True, hover=True, responsive=True, size="sm")
    
    return dbc.Card([
        dbc.CardHeader([
            html.H5(table_data['name'], className="mb-0", style={'color': '#34495e'})
        ]),
        dbc.CardBody([table])
    ], className="mb-3")

def create_table(table_data: TableData) -> dbc.Card:
    """创建表格组件"""
    if 'pd_data' in table_data and table_data['pd_data'] is not None:
        return create_table_from_pd(table_data)
    
    if not table_data.get('data') or not table_data.get('headers'):
        return dbc.Card([
            dbc.CardHeader(html.H5(table_data['name'], className="mb-0")),
            dbc.CardBody([
                dbc.Alert("暂无数据", color="info")
            ])
        ], className="mb-3")
    
    # 创建表头
    headers = [html.Th(col, className="text-center") for col in table_data['headers']]
    
    # 创建表格数据
    rows = []
    for row in table_data['data']:
        cells = [html.Td(str(cell), className="text-center") for cell in row]
        rows.append(html.Tr(cells))
    
    table = dbc.Table([
        html.Thead(html.Tr(headers), className="table-dark"),
        html.Tbody(rows)
    ], striped=True, bordered=True, hover=True, responsive=True, size="sm")
    
    return dbc.Card([
        dbc.CardHeader([
            html.H5(table_data['name'], className="mb-0", style={'color': '#34495e'})
        ]),
        dbc.CardBody([table])
    ], className="mb-3")

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
            # 使用缓存减少数据库查询
            cache_key = f"data_options_{selected_type}"
            cached_result = cache_manager.get(cache_key)
            
            if cached_result is not None:
                options, default_value = cached_result
                logger.debug(f"从缓存获取数据选项: {selected_type}")
                return options, default_value
            
            # 缓存未命中，查询数据库
            if selected_type == 'fund':
                data = DBFunds(mysql_db).get_all_funds()
            elif selected_type == 'strategy':
                data = DBStrategys(mysql_db).get_all_strategies()
            elif selected_type == 'stock':
                data = DBStocks(mysql_db).get_all_stocks()
            elif selected_type == 'forex':
                data = DBForexDayHist(mysql_db).get_all_forex(extend=True)
            elif selected_type == 'bond_yield':
                data = DBBondRate(mysql_db).get_all_bond()
            elif selected_type == 'index':
                data = DBIndexHist(mysql_db).get_all_indices()
            else:
                data = pd.DataFrame()
            
            options = get_data_briefs(selected_type, data)
            # 如果有选项，返回第一个选项的值作为默认值，否则返回空字符串
            default_value = options[0]['value'] if options else ''
            
            # 缓存结果5分钟
            cache_manager.set(cache_key, (options, default_value), ttl=300)
            logger.debug(f"缓存数据选项: {selected_type}")
            
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

    # 添加时间控件联动回调
    @app.callback(
        [Output('start-date-picker', 'date'),
         Output('end-date-picker', 'date'),
         Output('start-date-picker', 'disabled'),
         Output('end-date-picker', 'disabled'),
         Output('custom-date-row', 'style')],
        [Input('time-range-dropdown', 'value')]
    )
    def update_date_pickers(time_range):
        """更新时间控件的状态和值"""
        if time_range == 'custom':
            # 如果是自定义时间范围，启用时间控件并显示
            return None, None, False, False, {'display': 'block'}
        
        # 获取日期范围
        start_date, end_date = get_date_range(time_range)
        
        # 将日期转换为字符串格式 (YYYY-MM-DD)
        start_str = start_date.strftime('%Y-%m-%d') if start_date else None
        end_str = end_date.strftime('%Y-%m-%d') if end_date else None
        
        # 非自定义时间范围时禁用时间控件并隐藏
        return start_str, end_str, True, True, {'display': 'none'}

    @app.callback(
        [Output('product-value-graph', 'figure'),
         Output('product-summary-section', 'children'),
         Output('product-summary-section', 'style'),
         Output('single-product-chart-section', 'style'),
         Output('single-product-tables-section', 'children'),
         Output('single-product-tables-section', 'style')],
        [Input('query-button', 'n_clicks')],
        [State('type-dropdown', 'value'),
         State('product-dropdown', 'value'),
         State('line-options', 'value'),
         State('time-range-dropdown', 'value'),
         State('start-date-picker', 'date'),
         State('end-date-picker', 'date'),
         State('params-config-container', 'children')]
    )
    def update_product_display(n_clicks, data_type, selected_data, line_options, time_range, 
                             start_date_str, end_date_str, params_config):
        """更新数据展示"""
        if not n_clicks or not selected_data:  # 初始加载时或未选择产品时不触发更新
            return (go.Figure(), 
                   dbc.Alert("请选择产品并点击开始分析", color="info"),
                   {'display': 'none'}, {'display': 'none'}, [], {'display': 'none'})
        
        try:
            # 获取日期范围
            if time_range == 'custom':
                # 如果选择自定义时间范围，使用时间控件的值
                start_date = pd.to_datetime(start_date_str).date() if start_date_str else None
                end_date = pd.to_datetime(end_date_str).date() if end_date_str else None
            else:
                # 否则使用预设的时间范围
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
                error_alert = dbc.Alert("数据加载失败，请检查产品选择和时间范围", color="danger")
                return (go.Figure(), error_alert, {'display': 'block'}, 
                       {'display': 'none'}, [], {'display': 'none'})

            # 获取数据
            summary_data = generator.get_summary_data()
            chart_data = generator.get_chart_data()
            extra_datas = generator.get_extra_datas()

            if not chart_data:
                error_alert = dbc.Alert("未找到数据，请检查产品选择和时间范围", color="warning")
                return (go.Figure(), error_alert, {'display': 'block'}, 
                       {'display': 'none'}, [], {'display': 'none'})
            
            # 处理图表数据
            for option in line_options:
                extra_chart_data = generator.get_extra_chart_data(option)
                chart_data.extend(extra_chart_data)
            
            # 创建图表
            if data_type == 'fund':
                title = '基金净值和分红数据'
            elif data_type == 'strategy':
                title = '策略净值数据'
            elif data_type == 'stock':
                title = '股票K线数据'
            elif data_type == 'index':
                title = '指数价格走势'
            else:
                title = '价格数据'
            
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
                    'yaxis': {'title': '价格' if data_type in ['stock', 'index'] else '净值'},
                    'plot_bgcolor': 'white',
                    'hovermode': 'x unified'
                }
            }
            
            # 创建摘要表格
            summary_table = create_summary_table(summary_data)
            
            # 创建详细数据表格区域
            tables = [create_table(table_data) for table_data in extra_datas]
            
            # 将表格分为两列显示
            left_tables = tables[::2]  # 偶数索引的表格
            right_tables = tables[1::2]  # 奇数索引的表格
            
            tables_section = dbc.Row([
                dbc.Col(left_tables, width=6),
                dbc.Col(right_tables, width=6)
            ]) if tables else dbc.Alert("暂无详细数据", color="info")
            
            return (figure, summary_table, {'display': 'block'}, 
                   {'display': 'block'}, tables_section, {'display': 'block'})
            
        except Exception as e:
            logger.error(f"Error in update_product_display: {str(e)}")
            error_alert = dbc.Alert(f"分析过程中发生错误: {str(e)}", color="danger")
            return (go.Figure(), error_alert, {'display': 'block'}, 
                   {'display': 'none'}, [], {'display': 'none'}) 