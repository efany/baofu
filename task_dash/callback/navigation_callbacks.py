from dash.dependencies import Input, Output
from dash import html
import dash

def register_navigation_callbacks(app, pages):
    """
    注册导航栏的回调函数
    
    Args:
        app: Dash应用实例
        pages: 页面组件字典，格式：{路径:组件}
    """
    
    # 导航回调
    @app.callback(
        Output("page-content", "children"),
        [Input("url", "pathname")],
    )
    def display_page(pathname):
        if pathname in pages:
            return pages[pathname]
        return html.Div("404 - 页面不存在")

    # 导航链接激活状态回调
    @app.callback(
        [Output("home-link", "active"),
         Output("fund-details-link", "active"),
         Output("stock-details-link", "active"),
         Output("product-manage-link", "active"),
         Output("strategy-backtest-link", "active")],
        [Input("url", "pathname")],
    )
    def set_active_link(pathname):
        return (pathname == "/",
                pathname == "/fund-details",
                pathname == "/stock-details",
                pathname == "/product-manage",
                pathname == "/strategy-backtest") 