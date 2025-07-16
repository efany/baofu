from dash import Input, Output, html, State, callback_context, no_update
import plotly.graph_objs as go
import dash_bootstrap_components as dbc
from typing import List, Dict, Any
from loguru import logger
import sys
import os
import pandas as pd
import numpy as np
import dash
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_dash.datas.data import create_data_generator
from database.db_funds import DBFunds
from database.db_stocks import DBStocks
from database.db_forex_day_hist import DBForexDayHist
from database.db_strategys import DBStrategys
from task_dash.utils import get_date_range, get_data_briefs
from task_dash.callback.single_product_callbacks import create_table

def create_summary_table(table_data, product_name="产品"):
    """创建摘要表格"""
    if not table_data:
        return dbc.Alert("暂无摘要数据", color="info")
    
    # 将数据分组，每行4个指标
    rows = []
    current_row = []
    
    for i, (label, value) in enumerate(table_data):
        current_row.append(
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H6(label, className="card-subtitle mb-2 text-muted text-center"),
                        html.H5(value, className="card-title mb-0 text-center", 
                               style={'color': '#2c3e50', 'fontWeight': 'bold'})
                    ])
                ], className="h-100 border-0", style={'backgroundColor': '#f8f9fa'})
            ], width=3)
        )
        
        if (i + 1) % 4 == 0 or i == len(table_data) - 1:
            rows.append(dbc.Row(current_row, className="mb-2"))
            current_row = []
    
    return dbc.Card([
        dbc.CardHeader([
            html.H5(f"📈 {product_name} 摘要", className="mb-0", style={'color': '#34495e'})
        ]),
        dbc.CardBody(rows, className="p-3")
    ], className="mb-3")

def create_correlation_table(correlation_df):
    """创建相关系数表格"""
    if correlation_df.empty:
        return dbc.Alert("暂无相关性数据，需要至少选择两个产品", color="info")
        
    # 创建表头
    headers = [html.Th("产品", className="text-center")] + [
        html.Th(col, className="text-center", style={'fontSize': '12px'}) 
        for col in correlation_df.columns
    ]
    
    # 创建表格内容
    rows = []
    for idx, row in correlation_df.iterrows():
        cells = [html.Td(idx, className="fw-bold", style={'fontSize': '12px'})]  # 第一列是产品代码
        for val in row:
            if pd.isna(val):
                cells.append(html.Td("-", className="text-center"))
                continue
                
            # 根据相关系数的值设置不同的颜色
            if abs(val) > 0.8:
                color_class = 'text-white bg-danger' if val > 0 else 'text-white bg-primary'
            elif abs(val) > 0.6:
                color_class = 'text-dark bg-warning' if val > 0 else 'text-white bg-info'
            elif abs(val) > 0.3:
                color_class = 'text-dark bg-light'
            else:
                color_class = 'text-muted bg-light'
                
            cells.append(html.Td(
                f"{val:.3f}",
                className=f"text-center {color_class}",
                style={'fontSize': '11px', 'padding': '8px 4px'}
            ))
        rows.append(html.Tr(cells))
    
    return dbc.Table(
        [html.Thead(html.Tr(headers), className="table-dark"), 
         html.Tbody(rows)],
        striped=True, bordered=True, hover=True, responsive=True, size="sm"
    )

def create_product_tables(product_extra_datas):
    """创建产品表格，将相同name的表格合并显示"""
    
    # 按表格名称分组
    grouped_tables = {}
    for product_id, extra_datas in product_extra_datas.items():
        for table_data in extra_datas:
            name = table_data['name']
            if name not in grouped_tables:
                grouped_tables[name] = []
            # 添加产品标识
            table_data['product_id'] = product_id
            grouped_tables[name].append(table_data)
    
    tables = []
    
    # 处理每组表格
    for table_name, table_group in grouped_tables.items():
        if len(table_group) > 1:  # 多个产品的相同表格，需要合并
            tables.append(
                dbc.Card([
                    dbc.CardHeader([
                        html.H5(table_name, className="mb-0", style={'color': '#34495e'})
                    ]),
                    dbc.CardBody([
                        create_merged_table(table_group)
                    ])
                ], className="mb-3")
            )
        else:  # 单个产品的表格，直接显示
            table_data = table_group[0]
            tables.append(
                dbc.Card([
                    dbc.CardHeader([
                        html.H5(f"{table_name} ({table_data['product_id']})", 
                               className="mb-0", style={'color': '#34495e'})
                    ]),
                    dbc.CardBody([
                        create_table(table_data)
                    ])
                ], className="mb-3")
            )
    
    return html.Div(tables) if tables else dbc.Alert("暂无详细数据", color="info")

def get_table_data_rows(table):
    """从表格中提取数据行，支持不同的数据结构"""
    if 'data' in table and table['data']:
        return table['data']
    elif 'pd_data' in table and not table['pd_data'].empty:
        # 转换 DataFrame 为列表格式
        return table['pd_data'].values.tolist()
    else:
        return []

def create_merged_table(table_group):
    """创建合并后的对比表格"""
    if not table_group:
        return dbc.Alert("暂无数据", color="info")
    
    try:
        # 提取所有指标名称
        all_metrics = set()
        for table in table_group:
            rows = get_table_data_rows(table)
            for row in rows:
                if len(row) > 0:
                    all_metrics.add(str(row[0]))  # 第一列是指标名称
        
        if not all_metrics:
            return dbc.Alert("表格数据为空", color="warning")
        
        # 创建表头
        headers = [html.Th("指标", className="text-center")] + [
            html.Th(f"产品 {table.get('product_id', 'N/A')}", className="text-center") 
            for table in table_group
        ]
        
        # 创建表格内容
        rows = []
        for metric in sorted(all_metrics):
            row_data = [html.Td(metric, className="fw-bold text-center")]
            
            # 获取第一个产品的值作为基准
            base_value = None
            base_table = table_group[0]
            base_rows = get_table_data_rows(base_table)
            base_row = next((row for row in base_rows if len(row) > 1 and str(row[0]) == metric), None)
            if base_row and len(base_row) > 1:
                try:
                    base_value = float(str(base_row[1]).replace(',', '').replace('%', ''))
                except (ValueError, TypeError):
                    base_value = None
            
            # 添加第一列数据
            if base_row and len(base_row) > 1:
                row_data.append(html.Td(str(base_row[1]), className="text-center"))
            else:
                row_data.append(html.Td('-', className="text-center"))
            
            # 添加其他列数据，并与第一列比较
            for table in table_group[1:]:
                table_rows = get_table_data_rows(table)
                matching_row = next((row for row in table_rows if len(row) > 1 and str(row[0]) == metric), None)
                cell_value = str(matching_row[1]) if matching_row and len(matching_row) > 1 else '-'
                
                # 如果基准值存在且当前值可以转换为数值，则计算差异
                if base_value is not None and cell_value != '-':
                    try:
                        current_value = float(str(cell_value).replace(',', '').replace('%', ''))
                        abs_diff = current_value - base_value
                        rel_diff = (abs_diff / abs(base_value)) * 100 if base_value != 0 else float('inf')
                        
                        # 设置颜色
                        if abs_diff > 0:
                            color = 'text-danger'  # 红色表示高于基准
                        elif abs_diff < 0:
                            color = 'text-success'  # 绿色表示低于基准
                        else:
                            color = 'text-dark'  # 黑色表示相等
                        
                        cell_content = html.Div([
                            html.Div(cell_value, className="mb-1"),
                            html.Small(
                                f"Δ: {abs_diff:+.2f} ({rel_diff:+.2f}%)", 
                                className=f"{color} border-top pt-1"
                            )
                        ], className="text-center")
                        
                    except (ValueError, TypeError):
                        cell_content = html.Div(cell_value, className="text-center")
                else:
                    cell_content = html.Div(cell_value, className="text-center")
                
                row_data.append(html.Td(cell_content))
            
            rows.append(html.Tr(row_data))
        
        return dbc.Table(
            [html.Thead(html.Tr(headers), className="table-dark"), 
             html.Tbody(rows)],
            striped=True, bordered=True, hover=True, responsive=True, size="sm"
        )
    
    except Exception as e:
        logger.error(f"Error in create_merged_table: {str(e)}")
        return dbc.Alert(f"创建对比表格时发生错误: {str(e)}", color="danger")

def register_products_compare_callbacks(app, mysql_db):
    @app.callback(
        [Output('fund-dropdown', 'options'),
         Output('stock-dropdown', 'options'),
         Output('forex-dropdown', 'options'),
         Output('strategy-dropdown', 'options')],
        Input('url', 'pathname')
    )
    def update_dropdowns_options(_):
        """初始化下拉框选项"""
        try:
            # 获取基金选项
            fund_data = DBFunds(mysql_db).get_all_funds()
            fund_options = get_data_briefs('fund', fund_data)
            
            # 获取股票选项
            stock_data = DBStocks(mysql_db).get_all_stocks()
            stock_options = get_data_briefs('stock', stock_data)

            # 获取外汇选项
            forex_data = DBForexDayHist(mysql_db).get_all_forex(extend=True)
            forex_options = get_data_briefs('forex', forex_data)
            
            # 获取策略选项
            strategy_data = DBStrategys(mysql_db).get_all_strategies()
            strategy_options = get_data_briefs('strategy', strategy_data)
            
            return fund_options, stock_options, forex_options, strategy_options
            
        except Exception as e:
            logger.error(f"Error in update_dropdowns_options: {str(e)}")
            return [], [], [], []

    @app.callback(
        [Output('compare-start-date', 'date'),
         Output('compare-end-date', 'date'),
         Output('compare-start-date', 'disabled'),
         Output('compare-end-date', 'disabled'),
         Output('start-date-col', 'style'),
         Output('end-date-col', 'style')],
        [Input('compare-time-range', 'value')]
    )
    def update_date_pickers(time_range):
        """更新时间控件的状态和值"""
        if time_range == 'custom':
            # 如果是自定义时间范围，启用时间控件并显示
            return (None, None, False, False, 
                   {'display': 'block'}, {'display': 'block'})
        
        # 获取日期范围
        start_date, end_date = get_date_range(time_range)
        
        # 将日期转换为字符串格式 (YYYY-MM-DD)
        start_str = start_date.strftime('%Y-%m-%d') if start_date else None
        end_str = end_date.strftime('%Y-%m-%d') if end_date else None
        
        # 非自定义时间范围时禁用时间控件并隐藏
        return (start_str, end_str, True, True,
               {'display': 'none'}, {'display': 'none'})

    @app.callback(
        [Output('compare-value-graph', 'figure'),
         Output('products-summary-section', 'children'),
         Output('products-summary-section', 'style'),
         Output('chart-section', 'style'),
         Output('correlation-matrix-container', 'children'),
         Output('correlation-section', 'style'),
         Output('tables-section', 'children'),
         Output('tables-section', 'style')],
        [Input('compare-confirm-button', 'n_clicks')],
        [State('fund-dropdown', 'value'),
         State('stock-dropdown', 'value'),
         State('strategy-dropdown', 'value'),
         State('forex-dropdown', 'value'),
         State('compare-time-range', 'value'),
         State('compare-start-date', 'date'),
         State('compare-end-date', 'date'),
         State('compare-line-options', 'value')]
    )
    def update_comparison(n_clicks, fund_values, stock_values, strategy_values, forex_values, 
                         time_range, start_date_str, end_date_str, line_options):
        """更新对比图表和数据"""
        if not n_clicks:  # 初始加载时不触发更新
            return (go.Figure(), 
                   dbc.Alert("请选择产品并点击开始对比", color="info"),
                   {'display': 'none'}, {'display': 'none'}, 
                   dbc.Alert("请先进行产品对比分析", color="info"),
                   {'display': 'none'}, [], {'display': 'none'})
            
        try:
            # 获取日期范围
            if time_range == 'custom':
                # 如果选择自定义时间范围，使用时间控件的值
                if not start_date_str or not end_date_str:
                    error_alert = dbc.Alert("请选择开始和结束日期", color="warning")
                    return (go.Figure(), error_alert, {'display': 'block'}, {'display': 'none'},
                           dbc.Alert("请先进行产品对比分析", color="info"),
                           {'display': 'none'}, [], {'display': 'none'})
                start_date = pd.to_datetime(start_date_str).date()
                end_date = pd.to_datetime(end_date_str).date()
            else:
                # 否则使用预设的时间范围
                start_date, end_date = get_date_range(time_range)
            
            # 创建图表数据
            figure_data = []
            summary_children = []
            generators = {}  # 存储产品id和generator的映射
            product_extra_datas = {}  # 存储每个产品的统计数据
            
            # 计算总产品数量，用于计算每列宽度
            total_products = len(fund_values or []) + len(stock_values or []) + len(strategy_values or []) + len(forex_values or [])
            if total_products == 0:
                error_alert = dbc.Alert("请选择至少一个产品进行对比", color="warning")
                return (go.Figure(), error_alert, {'display': 'block'}, {'display': 'none'},
                       dbc.Alert("请先进行产品对比分析", color="info"),
                       {'display': 'none'}, [], {'display': 'none'})
            
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
                        generator.load()
                        generators[f"f-{fund_id}"] = generator
                        # 添加摘要信息
                        summary_data = generator.get_summary_data()
                        if summary_data:
                            summary_children.append(
                                create_summary_table(summary_data, f"基金 {fund_id}")
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
                                product_extra_datas[f"f-{fund_id}"] = extra_datas
            
            # 处理股票数据
            if stock_values:
                for stock_id in stock_values:
                    generator = create_data_generator(
                        data_type='stock',
                        data_id=stock_id,
                        mysql_db=mysql_db,
                        start_date=start_date,
                        end_date=end_date
                    )
                    if generator:
                        generator.load()
                        generators[f"s-{stock_id}"] = generator
                        # 添加摘要信息
                        summary_data = generator.get_summary_data()
                        if summary_data:
                            summary_children.append(
                                create_summary_table(summary_data, f"股票 {stock_id}")
                            )
                        
                        # 处理图表数据
                        chart_data = generator.get_chart_data(normalize=True, chart_type=1)
                        if chart_data:
                            stock_figure_data = []
                            stock_figure_data.append(chart_data[0])
                            
                            for option in line_options:
                                extra_data = generator.get_extra_chart_data(option, normalize=True)
                                stock_figure_data.extend(extra_data)
                            
                            for data in stock_figure_data:
                                if 'name' in data:
                                    data['name'] = f"{data['name']} (s-{stock_id})"
                            
                            figure_data.extend(stock_figure_data)
                            
                            # 获取统计数据
                            extra_datas = generator.get_extra_datas()
                            if extra_datas:
                                product_extra_datas[f"s-{stock_id}"] = extra_datas
            
            # 处理外汇数据
            if forex_values:
                for forex_id in forex_values:
                    generator = create_data_generator(
                        data_type='forex',
                        data_id=forex_id,
                        mysql_db=mysql_db,
                        start_date=start_date,
                        end_date=end_date
                    )
                    if generator:
                        generator.load()
                        generators[f"fx-{forex_id}"] = generator
                        # 添加摘要信息
                        summary_data = generator.get_summary_data()
                        if summary_data:
                            summary_children.append(
                                create_summary_table(summary_data, f"外汇 {forex_id}")
                            )
                        
                        # 处理图表数据
                        chart_data = generator.get_chart_data(normalize=True, chart_type=1)
                        if chart_data:
                            forex_figure_data = []
                            forex_figure_data.append(chart_data[0])
                            
                            for option in line_options:
                                extra_data = generator.get_extra_chart_data(option, normalize=True)
                                forex_figure_data.extend(extra_data)
                                
                            for data in forex_figure_data:
                                if 'name' in data:
                                    data['name'] = f"{data['name']} (fx-{forex_id})"
                            
                            figure_data.extend(forex_figure_data)
                            
                            # 获取统计数据
                            extra_datas = generator.get_extra_datas()
                            if extra_datas:
                                product_extra_datas[f"fx-{forex_id}"] = extra_datas
                                
            
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
                        generator.load()
                        generators[f"st-{strategy_id}"] = generator
                        # 添加摘要信息
                        summary_data = generator.get_summary_data()
                        if summary_data:
                            summary_children.append(
                                create_summary_table(summary_data, f"策略 {strategy_id}")
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
                                product_extra_datas[f"st-{strategy_id}"] = extra_datas

            # 创建图表
            figure = {
                'data': figure_data,
                'layout': {
                    'title': {
                        'text': '产品净值对比',
                        'font': {'size': 20, 'color': '#2c3e50'}
                    },
                    'xaxis': {
                        'title': '日期',
                        'tickfont': {'size': 10}
                    },
                    'yaxis': {
                        'title': '净值',
                        'tickfont': {'size': 10}
                    },
                    'hovermode': 'x unified',
                    'legend': {
                        'orientation': 'h',
                        'yanchor': 'bottom',
                        'y': 1.02,
                        'xanchor': 'center',
                        'x': 0.5,
                        'font': {'size': 10}
                    },
                    'plot_bgcolor': 'white',
                    'margin': dict(l=60, r=60, t=80, b=60)
                }
            }
            
            # 创建表格容器
            if product_extra_datas:
                tables_container = create_product_tables(product_extra_datas)
            else:
                tables_container = dbc.Alert("暂无详细数据", color="info")
            
            # 计算相关系数
            all_data = pd.DataFrame()
            date_data = {}  # 用于存储每个产品的日期和涨跌幅数据

            # 收集所有产品的数据
            for product_id, generator in generators.items():
                data = generator.get_value_data()
                if not data.empty:
                    # 确保日期列是datetime类型
                    data['date'] = pd.to_datetime(data['date'])
                    # 确保value列是float类型
                    data['value'] = pd.to_numeric(data['value'], errors='coerce')
                    
                    # 计算涨跌幅并保存
                    pct_change = data['value'].pct_change()
                    date_data[product_id] = pd.DataFrame({
                        'date': data['date'],
                        'pct_change': pct_change.fillna(0).astype('float64')
                    }).set_index('date')
                    logger.info(f"产品{product_id}的数据长度: {len(date_data[product_id])}")

            # 如果有足够的数据进行对比
            if len(date_data) > 1:
                # 合并所有产品的数据，确保数据类型一致
                all_data = pd.concat(
                    [df['pct_change'].rename(pid) for pid, df in date_data.items()], 
                    axis=1
                ).astype('float64')
                
                # 计算相关系数矩阵
                if not all_data.empty and len(all_data.columns) > 1:
                    # 创建一个空的相关系数矩阵
                    correlation_df = pd.DataFrame(index=all_data.columns, columns=all_data.columns)
                    
                    # 对每对产品单独计算相关系数
                    for i, col1 in enumerate(all_data.columns):
                        for j, col2 in enumerate(all_data.columns):
                            if i < j:  # 只计算上三角矩阵
                                # 只使用两个产品同时有数据的日期
                                logger.info(f"计算{col1}和{col2}的相关系数")
                                pair_data = all_data[[col1, col2]].copy().dropna()  # 选择两列数据并删除缺失值
                                if not pair_data.empty and len(pair_data) > 1:  # 确保有足够的数据点
                                    if len(pair_data) > 1:  # 确保还有足够的数据点
                                        # 使用numpy的corrcoef计算相关系数
                                        x = pair_data[col1].values
                                        y = pair_data[col2].values
                                        corr = np.corrcoef(x, y)[0, 1]
                                        logger.info(f"计算得到的相关系数: {corr}, 时间窗口起止: {pair_data.index[0]} - {pair_data.index[-1]}")
                                        correlation_df.loc[col1, col2] = corr
                                        correlation_df.loc[col2, col1] = corr  # 对称矩阵
                                    else:
                                        correlation_df.loc[col1, col2] = None
                                        correlation_df.loc[col2, col1] = None
                            if i == j:
                                correlation_df.loc[col1, col2] = 1
                    
                    correlation_table = create_correlation_table(correlation_df)
                else:
                    correlation_table = dbc.Alert(
                        "数据对齐后无法计算相关性",
                        color="warning"
                    )
            else:
                correlation_table = dbc.Alert(
                    "需要选择至少两个产品进行相关性分析",
                    color="info"
                )
            
            return (figure, 
                   summary_children if summary_children else dbc.Alert("暂无摘要数据", color="info"), 
                   {'display': 'block'}, {'display': 'block'},
                   correlation_table, {'display': 'block'},
                   tables_container, {'display': 'block'})
            
        except Exception as e:
            logger.error(f"Error in update_comparison: {str(e)}")
            error_alert = dbc.Alert(f"分析过程中发生错误: {str(e)}", color="danger")
            return (go.Figure(), error_alert, {'display': 'block'}, {'display': 'none'},
                   dbc.Alert("请先进行产品对比分析", color="info"),
                   {'display': 'none'}, [], {'display': 'none'})