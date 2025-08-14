import sys
import os
import dash
import atexit
import signal
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

from task_dash.pages.data_sources_manage import layout as data_sources_layout
from task_dash.pages.database_monitor import create_database_monitor_layout, register_database_monitor_callbacks
from task_dash.pages.template_editor import create_template_editor_page
from task_dash.callback.template_editor_callbacks import register_template_editor_callbacks

# 初始化Dash应用
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# 创建数据库连接池
mysql_db = MySQLDatabase(
    # host='192.168.0.11',
    # user='root',
    # password='123456',
    host='113.44.90.2',
    user='baofu',
    password='TYeKmJPfw2b7kxGK',
    database='baofu',
    pool_size=30  # 增加连接池大小
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
    elif pathname == '/single_index':
        return create_single_product_value_graph(mysql_db, "index")
    elif pathname == '/strategy_manage':
        return create_strategy_management(mysql_db)
    elif pathname == '/products_compare':
        return create_products_compare_page(mysql_db)
    elif pathname == '/products_manage':
        return create_product_management(mysql_db)
    elif pathname == '/correlation_analysis':
        return create_correlation_analysis_page(mysql_db)
    elif pathname == '/data_sources_manage':
        return data_sources_layout()
    elif pathname == '/database_monitor':
        return create_database_monitor_layout()
    elif pathname == '/template_editor':
        return create_template_editor_page(mysql_db)
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

# 注册数据库监控相关的回调
register_database_monitor_callbacks(app, mysql_db)

# 注册模板编辑器相关的回调
register_template_editor_callbacks(app, mysql_db)

def cleanup_connections():
    """清理数据库连接"""
    logger.info("正在清理数据库连接...")
    mysql_db.close_pool()

def signal_handler(signum, frame):
    """信号处理器"""
    logger.info(f"接收到信号 {signum}，准备关闭应用...")
    cleanup_connections()
    sys.exit(0)

# 注册清理函数
atexit.register(cleanup_connections)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == '__main__':
    try:
        logger.info("启动Dash应用...")
        logger.info(f"数据库连接池状态: {mysql_db.get_pool_status()}")
        
        # 设置host为0.0.0.0以便在服务器上可访问
        app.run(debug=True, host='127.0.0.1', port=8050)  # 使用8050端口
    except KeyboardInterrupt:
        logger.info("应用被手动终止")
    except Exception as e:
        logger.error(f"应用运行出错: {e}")
    finally:
        # 关闭数据库连接池
        cleanup_connections() 