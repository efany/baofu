from dash import html
import dash_bootstrap_components as dbc

layout = html.Div([
    html.H1("首页"),
    html.P("欢迎来到首页！"),
    html.Div([
        dbc.Button("单基金", href="/single_fund", style={"margin": "10px"}),
        dbc.Button("单股票", href="/single_stock", style={"margin": "10px"}),
        dbc.Button("单外汇", href="/single_forex", style={"margin": "10px"}),
        dbc.Button("单策略", href="/single_strategy", style={"margin": "10px"}),
        dbc.Button("📈 单指数", href="/single_index", style={"margin": "10px"}),
        dbc.Button("产品对比", href="/products_compare", style={"margin": "10px"}),
        dbc.Button("基金管理", href="/products_manage", style={"margin": "10px"}),
        dbc.Button("策略管理", href="/strategy_manage", style={"margin": "10px"}),
        dbc.Button("相关性分析", href="/correlation_analysis", style={"margin": "10px"}),
        dbc.Button("数据源管理", href="/data_sources_manage", style={"margin": "10px"}),
    ])
]) 