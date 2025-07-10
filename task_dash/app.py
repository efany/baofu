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

from task_dash.pages.single_product import create_single_product_value_graph
from task_dash.callback.single_product_callbacks import register_single_product_callbacks

from task_dash.pages.strategy_manage import create_strategy_management
from task_dash.callback.strategy_manage_callbacks import register_strategy_manage_callbacks

from task_dash.pages.products_compare import create_products_compare_page
from task_dash.callback.products_compare_callbacks import register_products_compare_callbacks

from task_dash.pages.products_manage import create_product_management
from task_dash.callback.products_manage_callbacks import register_product_manage_callbacks

from task_dash.pages.correlation_analysis import create_correlation_analysis_page
from task_dash.callback.correlation_analysis_callbacks import register_correlation_analysis_callbacks

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
    elif pathname == '/single_forex':
        return create_single_product_value_graph(mysql_db, "forex")  # Call the function to get the layout
    elif pathname == '/single_stock':
        return create_single_product_value_graph(mysql_db, "stock")  # Call the function to get the layout
    elif pathname == '/single_strategy':
        return create_single_product_value_graph(mysql_db, "strategy")  # Call the function to get the layout
    elif pathname == '/strategy_manage':
        return create_strategy_management(mysql_db)
    elif pathname == '/products_compare':
        return create_products_compare_page(mysql_db)
    elif pathname == '/products_manage':
        return create_product_management(mysql_db)
    elif pathname == '/correlation_analysis':
        return create_correlation_analysis_page(mysql_db)
    else:
        return html.H1("404: Not Found")

# 注册基金相关的回调
register_single_product_callbacks(app, mysql_db)

# 注册策略管理相关的回调
register_strategy_manage_callbacks(app, mysql_db)

# 注册产品对比相关的回调
register_products_compare_callbacks(app, mysql_db)

# 注册产品管理相关的回调
register_product_manage_callbacks(app, mysql_db)

# 注册相关性分析相关的回调
register_correlation_analysis_callbacks(app, mysql_db)

if __name__ == '__main__':
    try:
        # 设置host为0.0.0.0以便在服务器上可访问
        app.run_server(debug=True, host='0.0.0.0', port=8050)  # 使用8050端口
    finally:
        # 关闭数据库连接池
        mysql_db.close_pool() 