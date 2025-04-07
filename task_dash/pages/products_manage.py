import dash_bootstrap_components as dbc
from dash import html, dcc

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
            
            # 产品类型选择
            dbc.Row([
                dbc.Col([
                    html.H4("选择产品类型"),
                    dbc.RadioItems(
                        id="product-type-selector",
                        options=[
                            {"label": "基金", "value": "fund"},
                            {"label": "股票", "value": "stock"}
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
                            # 产品列表将在这里动态加载
                            html.Div(id="product-list-container")
                        ])
                    )
                ], width=4),
                
                # 右侧 - 操作区域
                dbc.Col([
                    html.H4("数据操作"),
                    dbc.Card(
                        dbc.CardBody([
                            # 更新按钮
                            dbc.Button(
                                id="update-product-data-button",
                                children="更新基金数据",
                                color="primary",
                                className="me-2"
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