from dash import html
import dash_bootstrap_components as dbc

layout = html.Div([
    html.H1("首页"),
    html.P("欢迎来到首页！"),
    html.Div([
        dbc.Button("单基金", href="/single_fund", style={"margin": "10px"}),
        dbc.Button("单策略", href="/single_strategy", style={"margin": "10px"}),
        dbc.Button("策略管理", href="/strategy_manage", style={"margin": "10px"}),
    ])
]) 