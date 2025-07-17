import dash_bootstrap_components as dbc
from dash import html, dcc, dash_table
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from database.db_funds import DBFunds
from database.db_stocks import DBStocks
from database.db_forex_day_hist import DBForexDayHist
# from database.db_data_sources import DBDataSources

def create_products_overview_content(mysql_db):
    """
    创建产品数据总览内容
    """
    try:
        # 获取各种数据统计
        db_funds = DBFunds(mysql_db)
        db_stocks = DBStocks(mysql_db)
        db_forex = DBForexDayHist(mysql_db)
        # db_sources = DBDataSources()
        
        # 获取基金统计
        funds_stats = get_funds_statistics(db_funds)
        # 获取股票统计
        stocks_stats = get_stocks_statistics(db_stocks)
        # 获取外汇统计
        forex_stats = get_forex_statistics(db_forex)
        # 获取数据源统计（暂时使用模拟数据）
        sources_stats = {'active_sources': 5, 'total_sources': 7}
        
        return html.Div([
            html.H3("数据总览", className="mb-4 text-primary"),
            
            # 数据统计卡片
            dbc.Row([
                dbc.Col([
                    create_stat_card("基金总数", funds_stats.get('total_funds', 0), "primary", "📊", "fund")
                ], width=3),
                dbc.Col([
                    create_stat_card("股票总数", stocks_stats.get('total_stocks', 0), "success", "📈", "stock")
                ], width=3),
                dbc.Col([
                    create_stat_card("外汇对数", forex_stats.get('total_forex', 0), "info", "💱", "forex")
                ], width=3),
                dbc.Col([
                    create_stat_card("数据源数", sources_stats.get('active_sources', 0), "warning", "🔗")
                ], width=3)
            ], className="mb-4"),
            
            # 数据更新状态
            html.H5("数据更新状态", className="mb-3"),
            dbc.Row([
                dbc.Col([
                    create_update_status_card("基金数据", funds_stats.get('latest_update', 'N/A'))
                ], width=4),
                dbc.Col([
                    create_update_status_card("股票数据", stocks_stats.get('latest_update', 'N/A'))
                ], width=4),
                dbc.Col([
                    create_update_status_card("外汇数据", forex_stats.get('latest_update', 'N/A'))
                ], width=4)
            ], className="mb-4"),
            
            # 数据质量概览
            html.H5("数据质量概览", className="mb-3"),
            dbc.Row([
                dbc.Col([
                    create_data_quality_chart(funds_stats, stocks_stats, forex_stats)
                ], width=6),
                dbc.Col([
                    create_data_coverage_chart(funds_stats, stocks_stats, forex_stats)
                ], width=6)
            ])
        ], className="p-4")
        
    except Exception as e:
        return html.Div([
            dbc.Alert(f"加载全局概览时发生错误: {str(e)}", color="danger")
        ], className="p-4")

def create_products_fund_content(mysql_db):
    """创建基金数据内容"""
    try:
        db_funds = DBFunds(mysql_db)
        funds_df = db_funds.get_all_funds()
        
        if funds_df is None or funds_df.empty:
            return html.Div([
                dbc.Alert([
                    html.I(className="fas fa-info-circle me-2"),
                    "暂无基金数据"
                ], color="info", className="text-center")
            ], className="p-4")
        
        return html.Div([
            html.H3("基金数据", className="mb-4 text-primary"),
            
            # 操作按钮
            html.Div([
                dbc.Button([
                    html.I(className="fas fa-plus me-2"),
                    "添加基金"
                ], id="add-fund-btn", color="primary", className="me-2"),
                dbc.Button([
                    html.I(className="fas fa-sync me-2"),
                    "更新数据"
                ], id="update-fund-btn", color="success", className="me-2"),
                dbc.Button([
                    html.I(className="fas fa-download me-2"),
                    "导出数据"
                ], id="export-fund-btn", color="info")
            ], className="mb-3"),
            
            # 添加基金表单
            dbc.Card([
                dbc.CardBody([
                    html.H5("添加新基金", className="mb-3"),
                    dbc.InputGroup([
                        dbc.Input(
                            id="new-fund-code",
                            type="text",
                            placeholder="输入基金代码，多个代码用逗号分隔"
                        ),
                        dbc.Button("添加", id="add-fund-submit", color="primary")
                    ])
                ])
            ], className="mb-3"),
            
            # 基金列表
            html.Div(id="fund-list-container", children=[
                create_fund_list_display(funds_df)
            ])
        ], className="p-4")
        
    except Exception as e:
        return html.Div([
            dbc.Alert([
                html.I(className="fas fa-exclamation-triangle me-2"),
                f"加载基金数据时发生错误: {str(e)}"
            ], color="danger", className="text-center")
        ], className="p-4")

def create_products_stock_content(mysql_db):
    """创建股票数据内容"""
    try:
        db_stocks = DBStocks(mysql_db)
        stocks_df = db_stocks.get_all_stocks()
        
        if stocks_df is None or stocks_df.empty:
            return html.Div([
                dbc.Alert([
                    html.I(className="fas fa-info-circle me-2"),
                    "暂无股票数据"
                ], color="info", className="text-center")
            ], className="p-4")
        
        return html.Div([
            html.H3("股票数据", className="mb-4 text-primary"),
            
            # 操作按钮
            html.Div([
                dbc.Button([
                    html.I(className="fas fa-plus me-2"),
                    "添加股票"
                ], id="add-stock-btn", color="primary", className="me-2"),
                dbc.Button([
                    html.I(className="fas fa-sync me-2"),
                    "更新数据"
                ], id="update-stock-btn", color="success", className="me-2"),
                dbc.Button([
                    html.I(className="fas fa-download me-2"),
                    "导出数据"
                ], id="export-stock-btn", color="info")
            ], className="mb-3"),
            
            # 添加股票表单
            dbc.Card([
                dbc.CardBody([
                    html.H5("添加新股票", className="mb-3"),
                    dbc.InputGroup([
                        dbc.Input(
                            id="new-stock-code",
                            type="text",
                            placeholder="输入股票代码，多个代码用逗号分隔"
                        ),
                        dbc.Button("添加", id="add-stock-submit", color="primary")
                    ])
                ])
            ], className="mb-3"),
            
            # 股票列表
            html.Div(id="stock-list-container", children=[
                create_stock_list_display(stocks_df)
            ])
        ], className="p-4")
        
    except Exception as e:
        return html.Div([
            dbc.Alert([
                html.I(className="fas fa-exclamation-triangle me-2"),
                f"加载股票数据时发生错误: {str(e)}"
            ], color="danger", className="text-center")
        ], className="p-4")

def create_products_forex_content(mysql_db):
    """创建外汇数据内容"""
    try:
        db_forex = DBForexDayHist(mysql_db)
        forex_data = db_forex.get_all_forex()
        
        # 检查是否有数据
        has_data = False
        if isinstance(forex_data, pd.DataFrame) and not forex_data.empty:
            has_data = True
        elif isinstance(forex_data, list) and len(forex_data) > 0:
            has_data = True
        
        if not has_data:
            return html.Div([
                dbc.Alert([
                    html.I(className="fas fa-info-circle me-2"),
                    "暂无外汇数据"
                ], color="info", className="text-center")
            ], className="p-4")
        
        return html.Div([
            html.H3("外汇数据", className="mb-4 text-primary"),
            
            # 操作按钮
            html.Div([
                dbc.Button([
                    html.I(className="fas fa-plus me-2"),
                    "添加外汇"
                ], id="add-forex-btn", color="primary", className="me-2"),
                dbc.Button([
                    html.I(className="fas fa-sync me-2"),
                    "更新数据"
                ], id="update-forex-btn", color="success", className="me-2"),
                dbc.Button([
                    html.I(className="fas fa-download me-2"),
                    "导出数据"
                ], id="export-forex-btn", color="info")
            ], className="mb-3"),
            
            # 添加外汇表单
            dbc.Card([
                dbc.CardBody([
                    html.H5("添加新外汇", className="mb-3"),
                    dbc.InputGroup([
                        dbc.Input(
                            id="new-forex-code",
                            type="text",
                            placeholder="输入外汇代码，多个代码用逗号分隔"
                        ),
                        dbc.Button("添加", id="add-forex-submit", color="primary")
                    ])
                ])
            ], className="mb-3"),
            
            # 外汇列表
            html.Div([
                create_forex_list_display(forex_data)
            ])
        ], className="p-4")
        
    except Exception as e:
        return html.Div([
            dbc.Alert([
                html.I(className="fas fa-exclamation-triangle me-2"),
                f"加载外汇数据时发生错误: {str(e)}"
            ], color="danger", className="text-center")
        ], className="p-4")

def create_fund_list_display(funds_df):
    """创建基金列表显示"""
    if funds_df.empty:
        return dbc.Alert("暂无基金数据", color="info", className="text-center")
    
    # 准备表格数据
    display_data = []
    for index, row in funds_df.iterrows():
        display_data.append({
            '基金代码': row['ts_code'] if 'ts_code' in row else (row['fund_code'] if 'fund_code' in row else '-'),
            '基金名称': row['name'] if 'name' in row else (row['fund_name'] if 'fund_name' in row else '-'),
            '基金公司': row['management'] if 'management' in row else (row['fund_company'] if 'fund_company' in row else '-'),
            '基金类型': row['fund_type'] if 'fund_type' in row else '-',
            '成立日期': row['establishment_date'].strftime('%Y-%m-%d') if 'establishment_date' in row and pd.notna(row['establishment_date']) else '-'
        })
    
    return dash_table.DataTable(
        id='fund-list-table',
        data=display_data,
        columns=[
            {'name': '基金代码', 'id': '基金代码'},
            {'name': '基金名称', 'id': '基金名称'},
            {'name': '基金公司', 'id': '基金公司'},
            {'name': '基金类型', 'id': '基金类型'},
            {'name': '成立日期', 'id': '成立日期'}
        ],
        style_cell={
            'textAlign': 'left',
            'padding': '12px',
            'fontFamily': 'Arial, sans-serif',
            'fontSize': '14px'
        },
        style_header={
            'backgroundColor': '#f8f9fa',
            'fontWeight': 'bold',
            'color': '#495057'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': '#f8f9fa'
            }
        ],
        page_size=15,
        sort_action='native',
        filter_action='native'
    )

def create_stock_list_display(stocks_df):
    """创建股票列表显示"""
    if stocks_df.empty:
        return dbc.Alert("暂无股票数据", color="info", className="text-center")
    
    # 准备表格数据
    display_data = []
    for index, row in stocks_df.iterrows():
        display_data.append({
            '股票代码': row['symbol'] if 'symbol' in row else (row['stock_code'] if 'stock_code' in row else '-'),
            '股票名称': row['name'] if 'name' in row else (row['stock_name'] if 'stock_name' in row else '-'),
            '货币': row['currency'] if 'currency' in row else '-',
            '交易所': row['exchange'] if 'exchange' in row else '-',
            '所属市场': row['market'] if 'market' in row else '-'
        })
    
    return dash_table.DataTable(
        id='stock-list-table',
        data=display_data,
        columns=[
            {'name': '股票代码', 'id': '股票代码'},
            {'name': '股票名称', 'id': '股票名称'},
            {'name': '货币', 'id': '货币'},
            {'name': '交易所', 'id': '交易所'},
            {'name': '所属市场', 'id': '所属市场'}
        ],
        style_cell={
            'textAlign': 'left',
            'padding': '12px',
            'fontFamily': 'Arial, sans-serif',
            'fontSize': '14px'
        },
        style_header={
            'backgroundColor': '#f8f9fa',
            'fontWeight': 'bold',
            'color': '#495057'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': '#f8f9fa'
            }
        ],
        page_size=15,
        sort_action='native',
        filter_action='native'
    )

def create_forex_list_display(forex_data):
    """创建外汇列表显示"""
    # 处理不同的数据类型
    if isinstance(forex_data, pd.DataFrame):
        if forex_data.empty:
            return dbc.Alert("暂无外汇数据", color="info", className="text-center")
        
        # 准备表格数据
        display_data = []
        for index, row in forex_data.iterrows():
            display_data.append({
                '外汇代码': row['symbol'] if 'symbol' in row else '-',
                '货币对': row['symbol'] if 'symbol' in row else '-',
                '状态': '活跃' if 'symbol' in row else '未知'
            })
    
    elif isinstance(forex_data, list):
        if not forex_data:
            return dbc.Alert("暂无外汇数据", color="info", className="text-center")
        
        # 准备表格数据
        display_data = []
        for symbol in forex_data:
            display_data.append({
                '外汇代码': symbol,
                '货币对': symbol,
                '状态': '活跃'
            })
    
    else:
        return dbc.Alert("外汇数据格式错误", color="warning", className="text-center")
    
    return dash_table.DataTable(
        id='forex-list-table',
        data=display_data,
        columns=[
            {'name': '外汇代码', 'id': '外汇代码'},
            {'name': '货币对', 'id': '货币对'},
            {'name': '状态', 'id': '状态'}
        ],
        style_cell={
            'textAlign': 'left',
            'padding': '12px',
            'fontFamily': 'Arial, sans-serif',
            'fontSize': '14px'
        },
        style_header={
            'backgroundColor': '#f8f9fa',
            'fontWeight': 'bold',
            'color': '#495057'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': '#f8f9fa'
            }
        ],
        page_size=15,
        sort_action='native',
        filter_action='native'
    )


def create_stat_card(title, value, color, icon, product_type=None):
    """
    创建统计卡片
    """
    card_props = {
        "color": color,
        "outline": True
    }
    
    if product_type:
        card_props["style"] = {"cursor": "pointer"}
        card_props["id"] = f"stat-card-{product_type}"
    
    return dbc.Card([
        dbc.CardBody([
            html.H2(f"{icon} {value}", className="text-center"),
            html.P(title, className="text-center text-muted")
        ])
    ], **card_props)


def create_update_status_card(title, last_update):
    """
    创建更新状态卡片
    """
    try:
        if last_update and last_update != 'N/A':
            if isinstance(last_update, str):
                last_update_dt = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
            else:
                last_update_dt = last_update
            
            now = datetime.now()
            time_diff = now - last_update_dt
            
            if time_diff.days == 0:
                status_text = "今天更新"
                status_color = "success"
            elif time_diff.days == 1:
                status_text = "昨天更新"
                status_color = "warning"
            elif time_diff.days <= 7:
                status_text = f"{time_diff.days}天前更新"
                status_color = "warning"
            else:
                status_text = f"{time_diff.days}天前更新"
                status_color = "danger"
        else:
            status_text = "未更新"
            status_color = "secondary"
    except:
        status_text = "状态未知"
        status_color = "secondary"
    
    return dbc.Card([
        dbc.CardBody([
            html.H5(title, className="card-title"),
            dbc.Badge(status_text, color=status_color, className="mb-2"),
            html.P(f"最后更新: {last_update}" if last_update != 'N/A' else "暂无数据", 
                   className="text-muted small")
        ])
    ])


def create_data_quality_chart(funds_stats, stocks_stats, forex_stats):
    """
    创建数据质量图表
    """
    try:
        data = {
            '产品类型': ['基金', '股票', '外汇'],
            '数据完整性': [
                funds_stats.get('completeness', 0),
                stocks_stats.get('completeness', 0),
                forex_stats.get('completeness', 0)
            ]
        }
        
        fig = px.bar(
            data, 
            x='产品类型', 
            y='数据完整性',
            title='数据完整性评分',
            color='数据完整性',
            color_continuous_scale='RdYlGn'
        )
        
        fig.update_layout(
            height=300,
            showlegend=False
        )
        
        return dcc.Graph(figure=fig)
    except:
        return html.Div("数据质量图表加载失败", className="text-muted")


def create_data_coverage_chart(funds_stats, stocks_stats, forex_stats):
    """
    创建数据覆盖率图表
    """
    try:
        labels = ['基金', '股票', '外汇']
        values = [
            funds_stats.get('total_funds', 0),
            stocks_stats.get('total_stocks', 0),
            forex_stats.get('total_forex', 0)
        ]
        
        fig = px.pie(
            values=values,
            names=labels,
            title='产品数据分布'
        )
        
        fig.update_layout(
            height=300
        )
        
        return dcc.Graph(figure=fig)
    except:
        return html.Div("数据覆盖率图表加载失败", className="text-muted")


def get_funds_statistics(db_funds):
    """
    获取基金统计信息
    """
    try:
        stats = {}
        # 获取基金总数
        all_funds = db_funds.get_all_funds()
        stats['total_funds'] = len(all_funds) if all_funds is not None and not all_funds.empty else 0
        
        # 获取最新更新时间（示例逻辑）
        stats['latest_update'] = '2024-01-01 12:00:00'  # 这里需要根据实际数据库字段调整
        
        # 数据完整性评分（示例）
        stats['completeness'] = 85  # 这里需要根据实际数据质量计算
        
        return stats
    except Exception as e:
        logger.error(f"获取基金统计信息时发生错误: {e}")
        return {'total_funds': 0, 'latest_update': 'N/A', 'completeness': 0}


def get_stocks_statistics(db_stocks):
    """
    获取股票统计信息
    """
    try:
        stats = {}
        # 获取股票总数
        all_stocks = db_stocks.get_all_stocks()
        stats['total_stocks'] = len(all_stocks) if all_stocks is not None and not all_stocks.empty else 0
        
        # 获取最新更新时间（示例逻辑）
        stats['latest_update'] = '2024-01-01 12:00:00'  # 这里需要根据实际数据库字段调整
        
        # 数据完整性评分（示例）
        stats['completeness'] = 90  # 这里需要根据实际数据质量计算
        
        return stats
    except Exception as e:
        logger.error(f"获取股票统计信息时发生错误: {e}")
        return {'total_stocks': 0, 'latest_update': 'N/A', 'completeness': 0}


def get_forex_statistics(db_forex):
    """
    获取外汇统计信息
    """
    try:
        stats = {}
        # 获取外汇对总数
        all_forex = db_forex.get_all_forex()
        
        # 处理不同的返回类型
        if isinstance(all_forex, pd.DataFrame):
            # 如果返回DataFrame，检查是否为空
            if all_forex.empty:
                stats['total_forex'] = 0
            else:
                # 计算DataFrame的行数
                stats['total_forex'] = len(all_forex)
        elif isinstance(all_forex, list):
            # 如果返回列表，直接计算长度
            stats['total_forex'] = len(all_forex)
        else:
            # 其他情况设为0
            stats['total_forex'] = 0
        
        # 获取最新更新时间（示例逻辑）
        stats['latest_update'] = '2024-01-01 12:00:00'  # 这里需要根据实际数据库字段调整
        
        # 数据完整性评分（示例）
        stats['completeness'] = 80  # 这里需要根据实际数据质量计算
        
        return stats
    except Exception as e:
        logger.error(f"获取外汇统计信息时发生错误: {e}")
        return {'total_forex': 0, 'latest_update': 'N/A', 'completeness': 0}


def create_product_management(mysql_db):
    """
    创建产品管理页面，支持基金和股票的管理
    
    Args:
        mysql_db: MySQL数据库连接
        
    Returns:
        dash.html.Div: 产品管理页面布局
    """
    return html.Div([
        dbc.Row([
            # 左侧导航栏
            dbc.Col([
                html.Div([
                    html.H4([
                        html.I(className="fas fa-cogs me-2"),
                        "产品管理"
                    ], className="mb-4 text-primary"),
                    
                    # 导航菜单
                    dbc.Nav([
                        dbc.NavItem(dbc.NavLink([
                            html.I(className="fas fa-chart-bar me-2"),
                            "数据总览"
                        ], id="nav-products-overview", href="#", active=True, className="products-nav-link")),
                        
                        dbc.NavItem(dbc.NavLink([
                            html.I(className="fas fa-coins me-2"),
                            "基金数据"
                        ], id="nav-products-fund", href="#", className="products-nav-link")),
                        
                        dbc.NavItem(dbc.NavLink([
                            html.I(className="fas fa-chart-line me-2"),
                            "股票数据"
                        ], id="nav-products-stock", href="#", className="products-nav-link")),
                        
                        dbc.NavItem(dbc.NavLink([
                            html.I(className="fas fa-dollar-sign me-2"),
                            "外汇数据"
                        ], id="nav-products-forex", href="#", className="products-nav-link"))
                    ], vertical=True, pills=True, className="products-nav-menu")
                ], className="products-nav-container")
            ], width=3),
            
            # 右侧内容区域
            dbc.Col([
                html.Div(
                    id="products-content-area",
                    children=create_products_overview_content(mysql_db)
                )
            ], width=9)
        ])
    ], className="products-management-container", style={"height": "100vh"}) 