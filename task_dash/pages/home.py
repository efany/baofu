from dash import html
import dash_bootstrap_components as dbc

layout = html.Div([
    html.H1("首页"),
    html.P("欢迎来到首页！"),
    html.Div([
        dbc.Button("单基金", href="/single_fund", style={"margin": "10px"}),
        dbc.Button("页面 2", href="/page2", style={"margin": "10px"}),
        dbc.Button("页面 3", href="/page3", style={"margin": "10px"}),
    ])
]) 