#!/usr/bin/env python3
"""
简化版测试应用，用于验证数据库优化效果
"""
import sys
import os
import dash
import atexit
import signal
from loguru import logger
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

from database.mysql_database import MySQLDatabase
from database.db_funds import DBFunds

# 创建简化版数据库监控页面
def create_simple_monitor():
    return dbc.Container([
        html.H1("数据库连接优化测试", className="text-center mb-4"),
        
        # 数据库状态
        dbc.Card([
            dbc.CardHeader("数据库连接池状态"),
            dbc.CardBody([
                html.Div(id="db-status")
            ])
        ], className="mb-3"),
        
        # 缓存状态  
        dbc.Card([
            dbc.CardHeader("缓存状态"),
            dbc.CardBody([
                html.Div(id="cache-status")
            ])
        ], className="mb-3"),
        
        # 测试按钮
        dbc.Row([
            dbc.Col([
                dbc.Button("测试数据库查询", id="test-db-btn", color="primary", className="me-2"),
                dbc.Button("清理缓存", id="clear-cache-btn", color="warning"),
            ])
        ], className="mb-3"),
        
        html.Div(id="test-result"),
        
        # 自动刷新
        dcc.Interval(id='interval', interval=3000, n_intervals=0)
    ])

# 初始化应用
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# 创建数据库连接池（使用优化后的配置）
mysql_db = MySQLDatabase(
    host='113.44.90.2',
    user='baofu',
    password='TYeKmJPfw2b7kxGK',
    database='baofu',
    pool_size=30  # 优化后的连接池大小
)

app.layout = create_simple_monitor()

@app.callback(
    [Output('db-status', 'children'),
     Output('cache-status', 'children')],
    [Input('interval', 'n_intervals'),
     Input('test-db-btn', 'n_clicks')]
)
def update_status(n_intervals, test_clicks):
    """更新状态显示"""
    try:
        # 数据库连接池状态
        pool_status = mysql_db.get_pool_status()
        usage_percent = (pool_status['active_connections'] / pool_status['pool_size']) * 100
        
        db_content = [
            html.P(f"总连接数: {pool_status['pool_size']}"),
            html.P(f"活跃连接: {pool_status['active_connections']}"),
            html.P(f"可用连接: {pool_status['available_connections']}"),
            html.P(f"使用率: {usage_percent:.1f}%"),
        ]
        
        # 缓存状态
        try:
            from task_dash.common.cache_manager import cache_manager
            cache_stats = cache_manager.get_stats()
            cache_content = [
                html.P(f"总缓存项: {cache_stats['total_items']}"),
                html.P(f"活跃缓存: {cache_stats['active_items']}"),
                html.P(f"过期缓存: {cache_stats['expired_items']}"),
            ]
        except ImportError:
            cache_content = [html.P("缓存管理器未启用")]
        
        return db_content, cache_content
        
    except Exception as e:
        error_msg = html.P(f"错误: {str(e)}", style={'color': 'red'})
        return [error_msg], [error_msg]

@app.callback(
    Output('test-result', 'children'),
    [Input('test-db-btn', 'n_clicks'),
     Input('clear-cache-btn', 'n_clicks')]
)
def handle_buttons(test_clicks, clear_clicks):
    """处理按钮点击"""
    ctx = dash.callback_context
    if not ctx.triggered:
        return ""
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    try:
        if button_id == 'test-db-btn' and test_clicks:
            # 测试数据库查询
            db_funds = DBFunds(mysql_db)
            funds = db_funds.get_all_funds()
            return dbc.Alert(f"查询成功：找到 {len(funds)} 只基金", color="success", dismissable=True)
            
        elif button_id == 'clear-cache-btn' and clear_clicks:
            # 清理缓存
            try:
                from task_dash.common.cache_manager import cache_manager
                cache_manager.clear()
                return dbc.Alert("缓存已清空", color="info", dismissable=True)
            except ImportError:
                return dbc.Alert("缓存管理器未启用", color="warning", dismissable=True)
                
    except Exception as e:
        return dbc.Alert(f"操作失败: {str(e)}", color="danger", dismissable=True)
    
    return ""

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
        logger.info("启动测试应用...")
        logger.info(f"数据库连接池状态: {mysql_db.get_pool_status()}")
        
        print("=" * 60)
        print("数据库连接优化测试应用")
        print("=" * 60)
        print(f"访问地址: http://127.0.0.1:8051")
        print("功能说明:")
        print("- 实时监控数据库连接池状态")
        print("- 显示缓存统计信息")  
        print("- 测试数据库查询性能")
        print("- 缓存管理功能")
        print("=" * 60)
        
        app.run(debug=True, host='127.0.0.1', port=8051)
        
    except KeyboardInterrupt:
        logger.info("应用被手动终止")
    except Exception as e:
        logger.error(f"应用运行出错: {e}")
    finally:
        cleanup_connections()