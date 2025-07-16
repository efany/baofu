import dash_bootstrap_components as dbc
from dash import html, dcc
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
from database.db_funds import DBFunds
from database.db_stocks import DBStocks
from database.db_forex_day_hist import DBForexDayHist
# from database.db_data_sources import DBDataSources

def create_global_overview(mysql_db):
    """
    创建全局概览组件
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
            html.H3("全局概览", className="mb-4"),
            
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
            html.H4("数据更新状态", className="mb-3"),
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
            html.H4("数据质量概览", className="mb-3"),
            dbc.Row([
                dbc.Col([
                    create_data_quality_chart(funds_stats, stocks_stats, forex_stats)
                ], width=6),
                dbc.Col([
                    create_data_coverage_chart(funds_stats, stocks_stats, forex_stats)
                ], width=6)
            ])
        ])
        
    except Exception as e:
        return html.Div([
            dbc.Alert(f"加载全局概览时发生错误: {str(e)}", color="danger")
        ])


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
    except:
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
    except:
        return {'total_stocks': 0, 'latest_update': 'N/A', 'completeness': 0}


def get_forex_statistics(db_forex):
    """
    获取外汇统计信息
    """
    try:
        stats = {}
        # 获取外汇对总数
        all_forex = db_forex.get_all_forex()
        # get_all_forex应该返回List[str]，但在没有数据时可能返回DataFrame
        if isinstance(all_forex, list):
            stats['total_forex'] = len(all_forex)
        elif hasattr(all_forex, '__len__') and not (hasattr(all_forex, 'empty') and all_forex.empty):
            stats['total_forex'] = len(all_forex)
        else:
            stats['total_forex'] = 0
        
        # 获取最新更新时间（示例逻辑）
        stats['latest_update'] = '2024-01-01 12:00:00'  # 这里需要根据实际数据库字段调整
        
        # 数据完整性评分（示例）
        stats['completeness'] = 80  # 这里需要根据实际数据质量计算
        
        return stats
    except Exception as e:
        print(f"获取外汇统计信息时发生错误: {e}")
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
        dbc.Container([
            # 页面标题
            html.H2("产品数据管理", className="text-center my-4"),
            
            # 全局概览部分
            html.Div([
                create_global_overview(mysql_db)
            ], className="mb-5"),
            
            html.Hr(),  # 分隔线
            
            # 产品类型选择
            html.H3("产品数据管理", className="mb-4"),
            dbc.Row([
                dbc.Col([
                    html.H4("选择产品类型"),
                    dbc.RadioItems(
                        id="product-type-selector",
                        options=[
                            {"label": "基金", "value": "fund"},
                            {"label": "股票", "value": "stock"},
                            {"label": "外汇", "value": "forex"}
                        ],
                        value="fund",  # 默认选择基金
                        inline=True,
                        className="mb-3"
                    ),
                ], width=12)
            ]),
            
            # 产品列表和操作区域
            dbc.Row([
                # 左侧 - 产品列表
                dbc.Col([
                    html.H4(id="product-list-title", children="基金列表"),
                    dbc.Card(
                        dbc.CardBody([
                            html.Div(id="product-list-container")
                        ])
                    )
                ], width=4),
                
                # 右侧 - 操作区域
                dbc.Col([
                    html.H4("数据操作"),
                    dbc.Card(
                        dbc.CardBody([
                            # 快速操作按钮
                            html.H5("快速操作", className="mb-3"),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Button(
                                        "数据源管理",
                                        id="goto-data-sources-btn",
                                        color="info",
                                        className="mb-2",
                                        href="/data_sources_manage"
                                    )
                                ], width=6),
                                dbc.Col([
                                    dbc.Button(
                                        "全量更新",
                                        id="full-update-btn",
                                        color="warning",
                                        className="mb-2"
                                    )
                                ], width=6)
                            ]),
                            
                            html.Hr(),  # 分隔线
                            
                            # 添加新产品的表单
                            html.H5("添加新产品", className="mb-3"),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label("产品代码"),
                                    dbc.Input(
                                        id="new-product-code",
                                        type="text",
                                        placeholder="输入产品代码，多个代码用逗号分隔"
                                    )
                                ]),
                            ], className="mb-3"),
                            dbc.Button(
                                "添加产品",
                                id="add-product-button",
                                color="primary",
                                className="mb-4"
                            ),
                            
                            html.Hr(),  # 分隔线
                            
                            # 更新按钮
                            html.H5("更新数据", className="mb-3"),
                            dbc.Button(
                                id="update-product-data-button",
                                children="更新基金数据",
                                color="primary",
                                className="mb-3"
                            ),
                            
                            # 更新状态显示
                            html.Div(id="update-status"),
                            
                            # 操作日志
                            html.H5("操作日志", className="mt-3"),
                            dbc.Card(
                                dbc.CardBody(
                                    html.Pre(id="operation-log", 
                                           style={"height": "300px", 
                                                 "overflow-y": "auto"})
                                ),
                                className="mt-2"
                            )
                        ])
                    )
                ], width=8)
            ])
        ], fluid=True)
    ]) 