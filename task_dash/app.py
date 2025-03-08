import sys
import os
import dash
from loguru import logger
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase
from database.db_funds import DBFunds
from database.db_strategys import DBStrategys

from pages.single_product import create_single_product_value_graph  # Import the function
from callback.single_product_callbacks import register_single_product_callbacks  # Import the callback registration

from pages.strategy_manage import create_strategy_management
from callback.strategy_manage_callback import register_strategy_manage_callbacks
# 初始化Dash应用
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# 创建数据库连接池
mysql_db = MySQLDatabase(
    host='127.0.0.1',
    user='baofu',
    password='TYeKmJPfw2b7kxGK',
    database='baofu',
    pool_size=5
)

# 应用布局
app.layout = dbc.Container([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content', style={"height": "100vh", "overflow": "auto"}),  # 设置为全屏
], fluid=True)

# 回调函数
@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    if pathname == '/':
        from pages.home import layout
        return layout
    elif pathname == '/single_fund':
        return create_single_product_value_graph(mysql_db, "fund")  # Call the function to get the layout
    elif pathname == '/single_strategy':
        return create_single_product_value_graph(mysql_db, "strategy")  # Call the function to get the layout
    elif pathname == '/strategy_manage':
        return create_strategy_management(mysql_db)
    else:
        return html.H1("404: Not Found")

# 注册基金相关的回调
register_single_product_callbacks(app, mysql_db)

# 注册策略管理相关的回调
register_strategy_manage_callbacks(app, mysql_db)

if __name__ == '__main__':
    try:
        # 设置host为0.0.0.0以便在服务器上可访问
        app.run_server(debug=True, host='0.0.0.0', port=8050)  # 使用8050端口
    finally:
        # 关闭数据库连接池
        mysql_db.close_pool() 