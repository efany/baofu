from dash import dcc, html, dash_table, Input, Output, State, callback_context
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
import numpy as np
from scipy import stats
from task_dash.utils import get_stock_name


def create_correlation_analysis_page(mysql_db):
    """创建相关性分析页面"""
    
    page_layout = dbc.Container([
        html.H1("产品相关性分析", className="text-center mb-4"),
        
        dbc.Row([
            # 左侧控制面板
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("分析配置", className="card-title"),
                        
                        # 基准产品选择
                        html.Div([
                            html.Label("基准产品选择:", className="form-label"),
                            dcc.Dropdown(
                                id='benchmark-product-dropdown',
                                placeholder="选择基准产品",
                                style={'marginBottom': '10px'}
                            )
                        ]),
                        
                        # 对比产品选择
                        html.Div([
                            html.Label("对比产品选择:", className="form-label"),
                            dcc.Dropdown(
                                id='compare-products-dropdown',
                                placeholder="选择对比产品",
                                multi=True,
                                style={'marginBottom': '10px'}
                            )
                        ]),
                        
                        # 时间窗口选择
                        html.Div([
                            html.Label("时间窗口:", className="form-label"),
                            dcc.Dropdown(
                                id='time-window-dropdown',
                                options=[
                                    {'label': '近1年', 'value': 365},
                                    {'label': '近2年', 'value': 730},
                                    {'label': '近3年', 'value': 1095},
                                    {'label': '近5年', 'value': 1825},
                                    {'label': '全部数据', 'value': 'all'},
                                    {'label': '自定义', 'value': 'custom'}
                                ],
                                value=365,
                                style={'marginBottom': '10px'}
                            )
                        ]),
                        
                        # 自定义时间选择
                        html.Div([
                            html.Label("自定义时间范围:", className="form-label"),
                            dcc.DatePickerRange(
                                id='custom-date-range',
                                start_date=datetime.now() - timedelta(days=365),
                                end_date=datetime.now(),
                                display_format='YYYY-MM-DD',
                                style={'marginBottom': '10px'}
                            )
                        ], id='custom-date-div', style={'display': 'none'}),
                        
                        # 相关性计算方法
                        html.Div([
                            html.Label("相关性计算方法:", className="form-label"),
                            dcc.Dropdown(
                                id='correlation-method-dropdown',
                                options=[
                                    {'label': '皮尔逊相关系数 (Pearson)', 'value': 'pearson'},
                                    {'label': '斯皮尔曼相关系数 (Spearman)', 'value': 'spearman'},
                                    {'label': '肯德尔相关系数 (Kendall)', 'value': 'kendall'}
                                ],
                                value='pearson',
                                style={'marginBottom': '10px'}
                            )
                        ]),
                        
                        # 数据处理方式
                        html.Div([
                            html.Label("数据处理方式:", className="form-label"),
                            dcc.Dropdown(
                                id='data-processing-dropdown',
                                options=[
                                    {'label': '收益率', 'value': 'returns'},
                                    {'label': '价格水平', 'value': 'price'},
                                    {'label': '归一化价格', 'value': 'normalized'}
                                ],
                                value='returns',
                                style={'marginBottom': '10px'}
                            )
                        ]),
                        
                        # 滚动窗口分析
                        html.Div([
                            html.Label("滚动窗口分析:", className="form-label"),
                            dcc.Checklist(
                                id='rolling-window-checkbox',
                                options=[{'label': '启用滚动窗口', 'value': 'enable'}],
                                value=[],
                                style={'marginBottom': '10px'}
                            )
                        ]),
                        
                        # 滚动窗口大小
                        html.Div([
                            html.Label("滚动窗口大小(天):", className="form-label"),
                            dcc.Input(
                                id='rolling-window-size',
                                type='number',
                                value=60,
                                min=30,
                                max=365,
                                step=1,
                                style={'marginBottom': '10px', 'width': '100%'}
                            )
                        ], id='rolling-window-size-div', style={'display': 'none'}),
                        
                        # 分析按钮
                        dbc.Button(
                            "开始分析",
                            id='correlation-analyze-button',
                            color="primary",
                            size="lg",
                            className="d-grid gap-2 col-12 mx-auto"
                        )
                    ])
                ])
            ], width=4),
            
            # 右侧结果展示
            dbc.Col([
                # 相关性系数表格
                dbc.Card([
                    dbc.CardBody([
                        html.H4("相关性系数矩阵", className="card-title"),
                        html.Div(id='correlation-matrix-table')
                    ])
                ], className="mb-3"),
                
                # 相关性热力图
                dbc.Card([
                    dbc.CardBody([
                        html.H4("相关性热力图", className="card-title"),
                        dcc.Graph(id='correlation-heatmap')
                    ])
                ], className="mb-3"),
                
                # 散点图
                dbc.Card([
                    dbc.CardBody([
                        html.H4("散点图分析", className="card-title"),
                        dcc.Graph(id='correlation-scatter')
                    ])
                ], className="mb-3"),
                
                # 滚动相关性图
                dbc.Card([
                    dbc.CardBody([
                        html.H4("滚动相关性分析", className="card-title"),
                        dcc.Graph(id='rolling-correlation')
                    ])
                ], className="mb-3")
            ], width=8)
        ]),
        
        # 存储组件
        dcc.Store(id='correlation-data-store'),
        dcc.Store(id='product-options-store')
    ], fluid=True)
    
    return page_layout


def get_all_products(mysql_db):
    """获取所有产品列表"""
    products = []
    
    # 获取基金数据
    try:
        from database.db_funds import DBFunds
        db_funds = DBFunds(mysql_db)
        funds_df = db_funds.get_all_funds()
        if not funds_df.empty:
            for _, fund in funds_df.iterrows():
                products.append({
                    'label': f"基金: {fund['name']} ({fund['ts_code']})",
                    'value': f"fund_{fund['ts_code']}",
                    'type': 'fund'
                })
    except Exception as e:
        print(f"获取基金数据失败: {e}")
    
    # 获取股票数据  
    try:
        from database.db_stocks import DBStocks
        db_stocks = DBStocks(mysql_db)
        stocks_df = db_stocks.get_all_stocks()
        if not stocks_df.empty:
            for _, stock in stocks_df.iterrows():
                stock_name = get_stock_name(stock['symbol'])
                products.append({
                    'label': f"股票: {stock_name} ({stock['symbol']})",
                    'value': f"stock_{stock['symbol']}",
                    'type': 'stock'
                })
    except Exception as e:
        print(f"获取股票数据失败: {e}")
    
    # 获取策略数据
    try:
        from database.db_strategys import DBStrategys
        db_strategys = DBStrategys(mysql_db)
        strategies_df = db_strategys.get_all_strategies()
        if not strategies_df.empty:
            for _, strategy in strategies_df.iterrows():
                products.append({
                    'label': f"策略: {strategy['name']} ({strategy['strategy_id']})",
                    'value': f"strategy_{strategy['strategy_id']}",
                    'type': 'strategy'
                })
    except Exception as e:
        print(f"获取策略数据失败: {e}")
    
    return products


def get_product_data(mysql_db, product_id: str, start_date=None, end_date=None) -> pd.DataFrame:
    """获取产品数据"""
    product_type, product_code = product_id.split('_', 1)
    
    try:
        if product_type == 'fund':
            from database.db_funds_nav import DBFundsNav
            db_funds_nav = DBFundsNav(mysql_db)
            df = db_funds_nav.get_fund_nav(product_code, start_date, end_date)
            if not df.empty:
                df['date'] = pd.to_datetime(df['nav_date'])
                df = df.sort_values('date')
                return df[['date', 'unit_nav']].rename(columns={'unit_nav': 'value'})
        
        elif product_type == 'stock':
            from database.db_stocks_day_hist import DBStocksDayHist
            db_stocks_hist = DBStocksDayHist(mysql_db)
            df = db_stocks_hist.get_stock_hist_data(product_code, start_date, end_date)
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                return df[['date', 'close']].rename(columns={'close': 'value'})
        
        elif product_type == 'strategy':
            from task_dash.datas.strategy_data_generator import StrategyDataGenerator
            # 将日期字符串转换为datetime.date对象
            date_start = None
            date_end = None
            if start_date:
                if isinstance(start_date, str):
                    date_start = pd.to_datetime(start_date).date()
                else:
                    date_start = start_date
            if end_date:
                if isinstance(end_date, str):
                    date_end = pd.to_datetime(end_date).date()
                else:
                    date_end = end_date
            
            strategy_generator = StrategyDataGenerator(int(product_code), mysql_db, date_start, date_end)
            if strategy_generator.load():
                df = strategy_generator.get_value_data()
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date')
                    return df[['date', 'value']]
    
    except Exception as e:
        print(f"获取产品数据失败: {e}")
    
    return pd.DataFrame()


def calculate_correlation_matrix(data_dict: Dict[str, pd.DataFrame], method: str = 'pearson', 
                                data_processing: str = 'returns') -> pd.DataFrame:
    """计算相关性矩阵"""
    if not data_dict:
        return pd.DataFrame()
    
    # 合并所有数据
    merged_df = None
    for product_id, df in data_dict.items():
        if df.empty:
            continue
            
        df_copy = df.copy()
        
        # 数据处理
        if data_processing == 'returns':
            df_copy['value'] = df_copy['value'].pct_change()
        elif data_processing == 'normalized':
            df_copy['value'] = df_copy['value'] / df_copy['value'].iloc[0]
        
        df_copy = df_copy.rename(columns={'value': product_id})
        
        if merged_df is None:
            merged_df = df_copy[['date', product_id]]
        else:
            merged_df = pd.merge(merged_df, df_copy[['date', product_id]], on='date', how='outer')
    
    if merged_df is None:
        return pd.DataFrame()
    
    # 计算相关性矩阵
    correlation_data = merged_df.drop('date', axis=1)
    correlation_matrix = correlation_data.corr(method=method)
    
    return correlation_matrix


def calculate_rolling_correlation(data_dict: Dict[str, pd.DataFrame], benchmark_id: str, 
                                window_size: int = 60, method: str = 'pearson',
                                data_processing: str = 'returns') -> pd.DataFrame:
    """计算滚动相关性"""
    if benchmark_id not in data_dict or data_dict[benchmark_id].empty:
        return pd.DataFrame()
    
    # 确保window_size是有效的正整数
    if window_size is None or window_size <= 0:
        window_size = 60  # 使用默认值
    
    benchmark_df = data_dict[benchmark_id].copy()
    
    # 数据处理
    if data_processing == 'returns':
        benchmark_df['value'] = benchmark_df['value'].pct_change()
    elif data_processing == 'normalized':
        benchmark_df['value'] = benchmark_df['value'] / benchmark_df['value'].iloc[0]
    
    rolling_corr_data = []
    
    for product_id, df in data_dict.items():
        if product_id == benchmark_id or df.empty:
            continue
            
        df_copy = df.copy()
        
        # 数据处理
        if data_processing == 'returns':
            df_copy['value'] = df_copy['value'].pct_change()
        elif data_processing == 'normalized':
            df_copy['value'] = df_copy['value'] / df_copy['value'].iloc[0]
        
        # 合并数据
        merged = pd.merge(benchmark_df[['date', 'value']], 
                         df_copy[['date', 'value']], 
                         on='date', how='inner', suffixes=('_benchmark', '_compare'))
        
        if len(merged) < window_size:
            continue
            
        # 计算滚动相关性
        rolling_corr = merged['value_benchmark'].rolling(window=window_size).corr(merged['value_compare'])
        
        for i, corr_val in enumerate(rolling_corr):
            if pd.notna(corr_val):
                rolling_corr_data.append({
                    'date': merged.iloc[i]['date'],
                    'product': product_id,
                    'correlation': corr_val
                })
    
    return pd.DataFrame(rolling_corr_data)