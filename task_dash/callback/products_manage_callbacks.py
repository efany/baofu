from dash.dependencies import Input, Output, State
from dash import html, dash_table, dcc
import dash
import dash_bootstrap_components as dbc
import pandas as pd
from loguru import logger
from database.db_funds import DBFunds
from database.db_stocks import DBStocks
from database.db_forex_day_hist import DBForexDayHist
from task_dash.utils import get_data_briefs
import json
from task_data.update_funds_task import UpdateFundsTask
from task_data.update_stocks_task import UpdateStocksTask
from task_data.update_forex_task import UpdateForexTask
from task_data.update_stocks_info_task import UpdateStocksInfoTask
from task_data.update_stocks_day_hist_task import UpdateStocksDayHistTask
from database.mysql_database import MySQLDatabase

def register_product_manage_callbacks(app, mysql_db):
    """
    注册产品管理页面的回调函数
    
    Args:
        app: Dash应用实例
        mysql_db: MySQL数据库连接
    """
    
    # 导航栏切换回调
    @app.callback(
        [Output('products-content-area', 'children'),
         Output('nav-products-overview', 'active'),
         Output('nav-products-fund', 'active'),
         Output('nav-products-stock', 'active'),
         Output('nav-products-forex', 'active')],
        [Input('nav-products-overview', 'n_clicks'),
         Input('nav-products-fund', 'n_clicks'),
         Input('nav-products-stock', 'n_clicks'),
         Input('nav-products-forex', 'n_clicks')],
        prevent_initial_call=True
    )
    def update_products_content(overview_clicks, fund_clicks, stock_clicks, forex_clicks):
        """更新产品管理内容区域"""
        ctx = dash.callback_context
        if not ctx.triggered:
            from task_dash.pages.products_manage import create_products_overview_content
            return (create_products_overview_content(mysql_db), True, False, False, False)
        
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if button_id == 'nav-products-overview':
            from task_dash.pages.products_manage import create_products_overview_content
            return (create_products_overview_content(mysql_db), True, False, False, False)
        elif button_id == 'nav-products-fund':
            from task_dash.pages.products_manage import create_products_fund_content
            return (create_products_fund_content(mysql_db), False, True, False, False)
        elif button_id == 'nav-products-stock':
            from task_dash.pages.products_manage import create_products_stock_content
            return (create_products_stock_content(mysql_db), False, False, True, False)
        elif button_id == 'nav-products-forex':
            from task_dash.pages.products_manage import create_products_forex_content
            return (create_products_forex_content(mysql_db), False, False, False, True)
        
        from task_dash.pages.products_manage import create_products_overview_content
        return (create_products_overview_content(mysql_db), True, False, False, False)
    
    # 统计卡片点击跳转回调
    @app.callback(
        Output("url", "pathname"),
        [Input("stat-card-fund", "n_clicks"),
         Input("stat-card-stock", "n_clicks"),
         Input("stat-card-forex", "n_clicks")],
        prevent_initial_call=True
    )
    def navigate_from_stat_cards(fund_clicks, stock_clicks, forex_clicks):
        """响应统计卡片点击事件，跳转到对应的单产品页面"""
        ctx = dash.callback_context
        if not ctx.triggered:
            return dash.no_update
        
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]
        
        if button_id == "stat-card-fund" and fund_clicks:
            return "/single_fund"
        elif button_id == "stat-card-stock" and stock_clicks:
            return "/single_stock"
        elif button_id == "stat-card-forex" and forex_clicks:
            return "/single_forex"
        
        return dash.no_update
    
    # 更新产品列表标题
    @app.callback(
        Output("product-list-title", "children"),
        Input("product-type-selector", "value")
    )
    def update_product_list_title(product_type):
        """根据选择的产品类型更新列表标题"""
        if product_type == "fund":
            return "基金列表"
        elif product_type == "stock":
            return "股票列表"
        elif product_type == "forex":
            return "外汇列表"
        return "产品列表"
    
    # 更新按钮文字
    @app.callback(
        Output("update-product-data-button", "children"),
        Input("product-type-selector", "value")
    )
    def update_button_text(product_type):
        """根据选择的产品类型更新按钮文字"""
        if product_type == "fund":
            return "更新基金数据"
        elif product_type == "stock":
            return "更新股票数据"
        elif product_type == "forex":
            return "更新外汇数据"
        return "更新产品数据"
    
    # 页面加载时初始化产品列表
    @app.callback(
        Output("product-list-container", "children"),
        [Input("product-type-selector", "value"),
         Input("update-product-data-button", "n_clicks"),
         Input("add-product-button", "n_clicks")],
        [State("new-product-code", "value")],
        prevent_initial_call=True
    )
    def update_product_list(product_type, update_clicks, add_clicks, new_product_code):
        """更新产品列表：响应产品类型切换、更新产品和添加产品"""
        ctx = dash.callback_context
        if not ctx.triggered:
            return load_fund_list(mysql_db) if product_type == "fund" else load_stock_list(mysql_db)
        
        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
        
        # 处理添加新产品
        if trigger_id == "add-product-button" and add_clicks:
            if not new_product_code:
                if product_type == "forex":
                    return load_forex_list(mysql_db)
                elif product_type == "stock":
                    return load_stock_list(mysql_db)
                elif product_type == "fund":
                    return load_fund_list(mysql_db)
                else:
                    return html.Div("未知的产品类型", style={"color": "red"})

            try:
                # 处理输入的代码，支持多个代码，去除空格
                product_codes = [code.strip() for code in new_product_code.split(",") if code.strip()]
                
                if not product_codes:
                    logger.warning("未输入有效的产品代码")
                    if product_type == "forex":
                        return load_forex_list(mysql_db)
                    elif product_type == "stock":
                        return load_stock_list(mysql_db)
                    elif product_type == "fund":
                        return load_fund_list(mysql_db)
                    else:
                        return html.Div("未知的产品类型", style={"color": "red"})
                
                logger.info(f"开始添加新{product_type}: {product_codes}")
                
                if product_type == "fund":
                    # 创建更新基金任务
                    task_config = {
                        "name": "update_funds",
                        "description": "添加新基金并更新数据",
                        "fund_codes": product_codes,
                        "update_all": False
                    }
                    task = UpdateFundsTask(task_config)
                elif product_type == "stock":
                    # 创建更新股票任务
                    task_config = {
                        "name": "update_stocks",
                        "description": "添加新股票并更新数据",
                        "stock_symbols": product_codes,
                        "proxy": "http://127.0.0.1:7890",
                        "update_info": True,
                        "update_hist": True
                    }
                    task = UpdateStocksTask(mysql_db, task_config)
                elif product_type == "forex":
                    # 创建更新外汇任务
                    task_config = {
                        "name": "update_forex",
                        "description": "添加新外汇并更新数据",
                        "symbols": product_codes,
                    }
                    task = UpdateForexTask(mysql_db, task_config)
                
                # 执行任务
                task.execute()
                
                if task.is_success:
                    logger.success(f"成功添加新{product_type}: {', '.join(product_codes)}")
                else:
                    logger.error(f"添加失败: {task.error}")
                    
            except Exception as e:
                logger.error(f"添加产品失败: {str(e)}")
        
        # 根据产品类型加载列表
        if product_type == "forex":
            return load_forex_list(mysql_db)
        elif product_type == "stock":
            return load_stock_list(mysql_db)
        elif product_type == "fund":
            return load_fund_list(mysql_db)
        else:
            return html.Div("未知的产品类型", style={"color": "red"})

    def load_fund_list(mysql_db):
        """加载基金列表"""
        # 创建基金数据库操作对象
        db_funds = DBFunds(mysql_db)
        
        # 获取所有基金数据
        funds_df = db_funds.get_all_funds()
        
        if funds_df is None or funds_df.empty:
            return html.Div("暂无基金数据", style={"color": "gray", "padding": "20px"})
        
        # 使用get_data_briefs函数获取简要信息
        fund_briefs = get_data_briefs("fund", funds_df)
        
        # 创建可勾选的基金列表
        fund_list = html.Div([
            html.Div(f"共 {len(funds_df)} 只基金", 
                     style={"margin": "10px 0", "fontWeight": "bold"}),
            
            # 全选/取消全选按钮
            html.Div([
                dbc.Button("全选", id="select-all-products", color="secondary", size="sm", className="me-2"),
                dbc.Button("取消全选", id="deselect-all-products", color="secondary", size="sm"),
            ], style={"margin": "10px 0"}),
            
            # 搜索框
            dbc.Input(
                id="product-search-input",
                type="text",
                placeholder="搜索基金...",
                style={"margin": "10px 0"}
            ),
            
            # 可勾选的基金列表
            dbc.Checklist(
                id="product-checklist",
                options=[
                    {"label": item['label'], "value": item['value']} 
                    for item in fund_briefs
                ],
                value=[],  # 默认不选中任何基金
                style={"maxHeight": "400px", "overflowY": "auto"}
            ),
            
            # 隐藏的div用于存储选中的产品
            html.Div(id="selected-products-store", style={"display": "none"}),
            
            # 隐藏的div用于存储所有产品选项
            html.Div(
                id="all-products-options-store", 
                children=json.dumps([item['value'] for item in fund_briefs]),
                style={"display": "none"}
            )
        ])
        
        return fund_list

    def load_stock_list(mysql_db):
        """加载股票列表"""
        # 创建股票数据库操作对象
        db_stocks = DBStocks(mysql_db)
        
        # 获取所有股票数据
        stocks_df = db_stocks.get_all_stocks()
        
        if stocks_df is None or stocks_df.empty:
            return html.Div("暂无股票数据", style={"color": "gray", "padding": "20px"})
        
        # 使用get_data_briefs函数获取简要信息
        stock_briefs = get_data_briefs("stock", stocks_df)
        
        # 创建可勾选的股票列表
        stock_list = html.Div([
            html.Div(f"共 {len(stocks_df)} 只股票", 
                     style={"margin": "10px 0", "fontWeight": "bold"}),
            
            # 全选/取消全选按钮
            html.Div([
                dbc.Button("全选", id="select-all-products", color="secondary", size="sm", className="me-2"),
                dbc.Button("取消全选", id="deselect-all-products", color="secondary", size="sm"),
            ], style={"margin": "10px 0"}),
            
            # 搜索框
            dbc.Input(
                id="product-search-input",
                type="text",
                placeholder="搜索股票...",
                style={"margin": "10px 0"}
            ),
            
            # 可勾选的股票列表
            dbc.Checklist(
                id="product-checklist",
                options=[
                    {"label": item['label'], "value": item['value']} 
                    for item in stock_briefs
                ],
                value=[],  # 默认不选中任何股票
                style={"maxHeight": "400px", "overflowY": "auto"}
            ),
            
            # 隐藏的div用于存储选中的产品
            html.Div(id="selected-products-store", style={"display": "none"}),
            
            # 隐藏的div用于存储所有产品选项
            html.Div(
                id="all-products-options-store", 
                children=json.dumps([item['value'] for item in stock_briefs]),
                style={"display": "none"}
            )
        ])
        
        return stock_list

    def load_forex_list(mysql_db):
        """加载外汇列表"""
        # 创建外汇数据库操作对象
        db_forex_day_hist = DBForexDayHist(mysql_db)
        
        # 获取所有外汇数据
        forex_df = db_forex_day_hist.get_all_forex()
        
        if forex_df is None or forex_df.empty:
            return html.Div("暂无外汇数据", style={"color": "gray", "padding": "20px"})
        
        # 使用get_data_briefs函数获取简要信息
        forex_briefs = get_data_briefs("forex", forex_df)
        
        # 创建可勾选的外汇列表
        forex_list = html.Div([
            html.Div(f"共 {len(forex_df)} 只外汇", 
                     style={"margin": "10px 0", "fontWeight": "bold"}),
            
            # 全选/取消全选按钮
            html.Div([
                dbc.Button("全选", id="select-all-products", color="secondary", size="sm", className="me-2"),
                dbc.Button("取消全选", id="deselect-all-products", color="secondary", size="sm"),
            ], style={"margin": "10px 0"}),
            
            # 搜索框
            dbc.Input(
                id="product-search-input",
                type="text",
                placeholder="搜索外汇...",
                style={"margin": "10px 0"}
            ),
            
            # 可勾选的外汇列表
            dbc.Checklist(
                id="product-checklist",
                options=[
                    {"label": item['label'], "value": item['value']} 
                    for item in forex_briefs
                ],
                value=[],  # 默认不选中任何外汇
                style={"maxHeight": "400px", "overflowY": "auto"}
            ),  
            
            # 隐藏的div用于存储选中的产品
            html.Div(id="selected-products-store", style={"display": "none"}),
            
            # 隐藏的div用于存储所有产品选项
            html.Div(
                id="all-products-options-store", 
                children=json.dumps([item['value'] for item in forex_briefs]),
                style={"display": "none"}
            )
        ])
        
        return forex_list

    # 全选/取消全选回调
    @app.callback(
        Output("product-checklist", "value"),
        [Input("select-all-products", "n_clicks"),
         Input("deselect-all-products", "n_clicks")],
        [State("all-products-options-store", "children"),
         State("product-checklist", "value")]
    )
    def select_deselect_all(select_clicks, deselect_clicks, all_options_json, current_values):
        """全选或取消全选产品"""
        ctx = dash.callback_context
        if not ctx.triggered:
            return current_values
        
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]
        
        try:
            all_options = json.loads(all_options_json) if all_options_json else []
        except:
            all_options = []
        
        if button_id == "select-all-products" and select_clicks:
            return all_options
        elif button_id == "deselect-all-products" and deselect_clicks:
            return []
        
        return current_values

    # 存储选中的产品
    @app.callback(
        Output("selected-products-store", "children"),
        [Input("product-checklist", "value")],
        prevent_initial_call=True
    )
    def store_selected_products(selected_products):
        """存储选中的产品"""
        if selected_products is None:
            return "[]"
        return json.dumps(selected_products)

    # 搜索过滤回调
    @app.callback(
        Output("product-checklist", "options"),
        [Input("product-search-input", "value"),
         Input("product-type-selector", "value")],
        prevent_initial_call=True
    )
    def filter_products(search_term, product_type):
        """根据搜索词过滤产品列表"""
        if product_type == "fund":
            # 获取所有基金
            db_funds = DBFunds(mysql_db)
            products_df = db_funds.get_all_funds()
            product_briefs = get_data_briefs("fund", products_df)
            placeholder = "搜索基金..."
        elif product_type == "stock":
            # 获取所有股票
            db_stocks = DBStocks(mysql_db)
            products_df = db_stocks.get_all_stocks()
            product_briefs = get_data_briefs("stock", products_df)
            placeholder = "搜索股票..."
        else:
            return []
        
        # 如果没有搜索词，返回所有产品
        if not search_term:
            return [{"label": item['label'], "value": item['value']} for item in product_briefs]
        
        # 过滤包含搜索词的产品
        filtered_options = [
            {"label": item['label'], "value": item['value']}
            for item in product_briefs
            if search_term.lower() in item['label'].lower()
        ]
        return filtered_options

    # 更新搜索框占位符
    @app.callback(
        Output("product-search-input", "placeholder"),
        Input("product-type-selector", "value")
    )
    def update_search_placeholder(product_type):
        """更新搜索框占位符"""
        if product_type == "fund":
            return "搜索基金..."
        elif product_type == "stock":
            return "搜索股票..."
        return "搜索产品..."

    @app.callback(
        [Output("update-status", "children"),
         Output("operation-log", "children")],
        Input("update-product-data-button", "n_clicks"),
        [State("selected-products-store", "children"),
         State("product-type-selector", "value")],
        prevent_initial_call=True
    )
    def update_product_data(n_clicks, selected_products_json, product_type):
        """更新产品数据"""
        if n_clicks is None:
            return "", "等待操作..."
        
        try:
            selected_products = json.loads(selected_products_json) if selected_products_json else []
        except:
            selected_products = []
        
        if not selected_products:
            return html.Div(
                "请先选择要更新的产品",
                style={"color": "red"}
            ), "请先选择要更新的产品\n"

        # 根据产品类型执行不同的更新操作
        if product_type == "fund":
            return update_fund_data(selected_products)
        elif product_type == "stock":
            return update_stock_data(selected_products)
        elif product_type == "forex":
            return update_forex_data(selected_products)
        else:
            return html.Div(
                "未知的产品类型",
                style={"color": "red"}
            ), "未知的产品类型\n"

    def update_fund_data(selected_funds):
        """更新基金数据"""
        # 创建任务配置
        task_config = {
            "name": "update_funds",
            "description": "更新基金信息和净值数据",
            "fund_codes": selected_funds,  # 使用选择的基金代码
            "update_all": False  # 只更新选择的基金
        }

        # 创建并执行更新任务
        try:
            task = UpdateFundsTask(task_config)
            task.execute()
            
            if task.is_success:
                return html.Div(
                    f"成功更新 {len(selected_funds)} 只基金",
                    style={"color": "green"}
                ), f"成功更新 {len(selected_funds)} 只基金：\n" + "\n".join(selected_funds)
            else:
                return html.Div(
                    f"更新失败: {task.error}",
                    style={"color": "red"}
                ), f"更新失败: {task.error}\n"
                
        except Exception as e:
            logger.error(f"更新基金数据失败: {str(e)}")
            return html.Div(
                f"更新过程中发生错误: {str(e)}",
                style={"color": "red"}
            ), f"更新过程中发生错误: {str(e)}\n"
            
    def update_stock_data(selected_stocks):
        """更新股票数据"""
        # 更新股票历史数据
        task_config = {
            "name": "update_stocks",
            "description": "更新股票日线历史数据",
            "stock_symbols": selected_stocks,
            "proxy": "http://127.0.0.1:7890",  # 根据实际情况调整代理
            "update_info": True,  # 更新基本信息
            "update_hist": True,  # 更新历史数据
        }
        
        info_result = ""
        hist_result = ""
        
        try:
            # 执行股票信息更新任务
            task = UpdateStocksTask(mysql_db, task_config)
            task.execute()
            
            if task.is_success:
                info_result = "股票信息更新成功\n"
                logger.info(f"成功更新{len(selected_stocks)}只股票的基本信息")
            else:
                info_result = f"股票信息更新失败: {task.error}\n"
                logger.error(f"更新股票信息失败: {task.error}")

            # 合并结果
            if task.is_success:
                return html.Div(
                    f"成功更新 {len(selected_stocks)} 只股票的基本信息和历史数据",
                    style={"color": "green"}
                ), f"更新 {len(selected_stocks)} 只股票完成：\n" + "\n".join(selected_stocks) + "\n\n" + info_result + hist_result
            else:
                return html.Div(
                    "部分更新失败，请查看日志",
                    style={"color": "orange"}
                ), f"更新 {len(selected_stocks)} 只股票结果：\n" + info_result + hist_result
                
        except Exception as e:
            error_msg = f"更新股票数据时发生错误: {str(e)}"
            logger.error(error_msg)
            return html.Div(
                error_msg,
                style={"color": "red"}
            ), error_msg + "\n" + info_result + hist_result 

    def update_forex_data(selected_forex):
        """更新外汇数据"""
        # 创建任务配置
        task_config = {
            "name": "update_forex",
            "description": "更新外汇历史数据",
            "symbols": selected_forex,
        }
        
        # 创建并执行更新任务
        try:
            task = UpdateForexTask(mysql_db, task_config)
            task.execute()

            if task.is_success:
                return html.Div(
                    f"成功更新 {len(selected_forex)} 只外汇",
                    style={"color": "green"}
                ), f"成功更新 {len(selected_forex)} 只外汇：\n" + "\n".join(selected_forex)
            else:
                return html.Div(
                    f"更新失败: {task.error}",
                    style={"color": "red"}
                ), f"更新失败: {task.error}\n"
                
        except Exception as e:
            logger.error(f"更新外汇数据失败: {str(e)}")
            return html.Div(
                f"更新过程中发生错误: {str(e)}",
                style={"color": "red"}
            ), f"更新过程中发生错误: {str(e)}\n"
