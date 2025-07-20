"""
数据库连接池监控页面
"""
import dash
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
from datetime import datetime
import pandas as pd
from loguru import logger


def create_database_monitor_layout():
    """创建数据库监控页面布局"""
    return dbc.Container([
        # 页面标题
        dbc.Row([
            dbc.Col([
                html.H2("数据库连接池监控", className="text-center mb-4"),
                html.Hr()
            ])
        ]),
        
        # 实时状态卡片
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5("连接池状态", className="mb-0")
                    ]),
                    dbc.CardBody([
                        html.Div(id="pool-status-content")
                    ])
                ])
            ], width=6),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5("缓存状态", className="mb-0")
                    ]),
                    dbc.CardBody([
                        html.Div(id="cache-status-content")
                    ])
                ])
            ], width=6)
        ], className="mb-4"),
        
        # 操作按钮
        dbc.Row([
            dbc.Col([
                dbc.ButtonGroup([
                    dbc.Button("刷新状态", id="refresh-status-btn", color="primary", n_clicks=0),
                    dbc.Button("清理缓存", id="clear-cache-btn", color="warning", n_clicks=0),
                    dbc.Button("清理过期缓存", id="cleanup-cache-btn", color="info", n_clicks=0)
                ])
            ], className="text-center")
        ], className="mb-4"),
        
        # 操作结果提示
        html.Div(id="operation-result", className="mb-4"),
        
        # 自动刷新间隔组件
        dcc.Interval(
            id='status-interval',
            interval=5*1000,  # 每5秒刷新一次
            n_intervals=0
        )
    ], fluid=True)


def register_database_monitor_callbacks(app, mysql_db):
    """注册数据库监控相关的回调函数"""
    
    @app.callback(
        [Output('pool-status-content', 'children'),
         Output('cache-status-content', 'children')],
        [Input('status-interval', 'n_intervals'),
         Input('refresh-status-btn', 'n_clicks')]
    )
    def update_status(n_intervals, refresh_clicks):
        """更新连接池和缓存状态"""
        try:
            # 获取连接池状态
            pool_status = mysql_db.get_pool_status()
            
            pool_content = [
                dbc.Row([
                    dbc.Col([
                        html.H6("连接池大小", className="text-muted"),
                        html.H4(str(pool_status['pool_size']), className="text-primary")
                    ], width=4),
                    dbc.Col([
                        html.H6("活跃连接", className="text-muted"),
                        html.H4(str(pool_status['active_connections']), className="text-warning")
                    ], width=4),
                    dbc.Col([
                        html.H6("可用连接", className="text-muted"),
                        html.H4(str(pool_status['available_connections']), className="text-success")
                    ], width=4)
                ]),
                html.Hr(),
                html.P(f"使用率: {(pool_status['active_connections'] / pool_status['pool_size'] * 100):.1f}%", 
                       className="text-center mb-0")
            ]
            
            # 获取缓存状态
            try:
                from task_dash.common.cache_manager import cache_manager
                cache_stats = cache_manager.get_stats()
                
                cache_content = [
                    dbc.Row([
                        dbc.Col([
                            html.H6("总缓存项", className="text-muted"),
                            html.H4(str(cache_stats['total_items']), className="text-info")
                        ], width=4),
                        dbc.Col([
                            html.H6("活跃缓存", className="text-muted"),
                            html.H4(str(cache_stats['active_items']), className="text-success")
                        ], width=4),
                        dbc.Col([
                            html.H6("过期缓存", className="text-muted"),
                            html.H4(str(cache_stats['expired_items']), className="text-danger")
                        ], width=4)
                    ]),
                    html.Hr(),
                    html.P(f"默认TTL: {cache_stats['default_ttl']}秒", 
                           className="text-center mb-0")
                ]
            except ImportError:
                cache_content = [
                    dbc.Alert("缓存管理器未启用", color="warning")
                ]
            
            return pool_content, cache_content
            
        except Exception as e:
            logger.error(f"更新状态时出错: {e}")
            error_msg = dbc.Alert(f"获取状态失败: {str(e)}", color="danger")
            return [error_msg], [error_msg]
    
    @app.callback(
        Output('operation-result', 'children'),
        [Input('clear-cache-btn', 'n_clicks'),
         Input('cleanup-cache-btn', 'n_clicks')]
    )
    def handle_cache_operations(clear_clicks, cleanup_clicks):
        """处理缓存操作"""
        ctx = dash.callback_context
        if not ctx.triggered:
            return ""
        
        try:
            from task_dash.common.cache_manager import cache_manager
            
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]
            
            if button_id == 'clear-cache-btn' and clear_clicks > 0:
                cache_manager.clear()
                return dbc.Alert("缓存已清空", color="success", dismissable=True)
            
            elif button_id == 'cleanup-cache-btn' and cleanup_clicks > 0:
                removed_count = cache_manager.cleanup_expired()
                return dbc.Alert(f"已清理 {removed_count} 个过期缓存项", color="info", dismissable=True)
                
        except ImportError:
            return dbc.Alert("缓存管理器未启用", color="warning", dismissable=True)
        except Exception as e:
            logger.error(f"处理缓存操作时出错: {e}")
            return dbc.Alert(f"操作失败: {str(e)}", color="danger", dismissable=True)
        
        return ""