import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, ALL
import json
import sys
import os
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.db_strategys import DBStrategys
from database.mysql_database import MySQLDatabase
from pages.strategy_manage import create_strategy_list_table, validate_strategy_data, create_strategy_stats

def register_strategy_manage_callbacks(app, mysql_db: MySQLDatabase):
    """注册策略管理相关的回调"""
    db_strategys = DBStrategys(mysql_db)

    # 表格行选择回调 - 控制按钮状态
    @app.callback(
        [Output('open-edit-strategy-modal', 'disabled'),
         Output('delete-strategy-btn', 'disabled'),
         Output('selected-strategy-info', 'children'),
         Output('selected-strategy-data', 'data')],
        [Input('strategy-table', 'selected_rows'),
         Input('strategy-table', 'data')]
    )
    def update_button_states(selected_rows, table_data):
        """根据表格选择状态更新按钮"""
        if not selected_rows or not table_data:
            return True, True, "", {}
        
        selected_strategy = table_data[selected_rows[0]]
        strategy_id = selected_strategy['ID']
        strategy_name = selected_strategy['策略名称']
        
        # 获取完整的策略数据
        strategy_data = db_strategys.get_strategy(strategy_id)
        strategy_info = {}
        if not strategy_data.empty:
            strategy_info = {
                'strategy_id': strategy_id,
                'name': strategy_data.iloc[0]['name'],
                'description': strategy_data.iloc[0]['description'],
                'initial_cash': strategy_data.iloc[0]['initial_cash'],
                'data_params': strategy_data.iloc[0]['data_params'],
                'parameters': strategy_data.iloc[0]['parameters'],
                'strategy': strategy_data.iloc[0]['strategy']
            }
        
        info_text = f"已选择策略: {strategy_name} (ID: {strategy_id})"
        
        return False, False, info_text, strategy_info

    # 打开新建策略模态框
    @app.callback(
        [Output('strategy-modal', 'is_open'),
         Output('strategy-modal-title', 'children'),
         Output('current-operation', 'data')],
        [Input('open-new-strategy-modal', 'n_clicks'),
         Input('open-edit-strategy-modal', 'n_clicks'),
         Input('close-strategy-modal', 'n_clicks'),
         Input('cancel-strategy-btn', 'n_clicks')],
        [State('strategy-modal', 'is_open'),
         State('selected-strategy-data', 'data')]
    )
    def toggle_strategy_modal(new_clicks, edit_clicks, close_clicks, cancel_clicks, is_open, selected_data):
        """控制策略模态框的打开和关闭"""
        ctx = dash.callback_context
        if not ctx.triggered:
            return False, "新建策略", "new"
        
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if button_id == "open-new-strategy-modal":
            return True, "新建策略", "new"
        elif button_id == "open-edit-strategy-modal" and selected_data:
            return True, f"编辑策略 - {selected_data.get('name', '')}", "edit"
        elif button_id in ["close-strategy-modal", "cancel-strategy-btn"]:
            return False, "新建策略", "new"
        
        return is_open, dash.no_update, dash.no_update

    # 加载策略数据到表单并确保可编辑
    @app.callback(
        [Output('strategy-name-input', 'value'),
         Output('strategy-description-input', 'value'),
         Output('strategy-cash-input', 'value'),
         Output('strategy-data-params-input', 'value'),
         Output('strategy-parameters-input', 'value'),
         Output('strategy-config-input', 'value'),
         Output('strategy-name-input', 'disabled'),
         Output('strategy-description-input', 'disabled'),
         Output('strategy-cash-input', 'disabled'),
         Output('strategy-data-params-input', 'disabled'),
         Output('strategy-parameters-input', 'disabled'),
         Output('strategy-config-input', 'disabled')],
        [Input('open-new-strategy-modal', 'n_clicks'),
         Input('open-edit-strategy-modal', 'n_clicks')],
        [State('selected-strategy-data', 'data')],
        prevent_initial_call=True
    )
    def load_strategy_form_data(new_clicks, edit_clicks, selected_data):
        """加载策略数据到表单并确保所有字段可编辑"""
        ctx = dash.callback_context
        if not ctx.triggered:
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, \
                   dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
        
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        # 所有字段都应该可编辑
        disabled_states = (False, False, False, False, False, False)
        
        if button_id == "open-new-strategy-modal":
            # 新建策略的默认值
            values = ("", "", None, 
                     """{
    "fund_codes": ["007540", "003376"],
    "start_date": "2020-01-01",
    "end_date": "2024-12-31"
}""", 
                     """{
    "rebalance_period": 20,
    "position_size": 50,
    "ma_periods": ["MA20", "MA60"],
    "show_drawdown": "top3",
    "risk_tolerance": "medium"
}""", 
                     """{
    "name": "BuyAndHold",
    "open_date": "<open_date>",
    "close_date": "<close_date>",
    "dividend_method": "reinvest",
    "products": ["007540", "003376"],
    "weights": [0.5, 0.5]
}""")
            return values + disabled_states
        
        elif button_id == "open-edit-strategy-modal" and selected_data:
            # 编辑策略，加载现有数据
            try:
                # 格式化JSON数据
                strategy_config = json.loads(selected_data['strategy'])
                strategy_json = json.dumps(strategy_config, indent=4, ensure_ascii=False)
                
                parameters_json = "{}"
                if selected_data.get('parameters'):
                    parameters = json.loads(selected_data['parameters'])
                    parameters_json = json.dumps(parameters, indent=4, ensure_ascii=False)
                
                values = (
                    selected_data.get('name', ''),
                    selected_data.get('description', ''),
                    selected_data.get('initial_cash'),
                    selected_data.get('data_params', '{}'),
                    parameters_json,
                    strategy_json
                )
                return values + disabled_states
            except (json.JSONDecodeError, KeyError):
                # 如果数据有问题，返回空值
                values = ("", "", None, "{}", "{}", "{}")
                return values + disabled_states
        
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, \
               dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # 搜索策略
    @app.callback(
        [Output('strategy-list-container', 'children'),
         Output('strategy-stats', 'children')],
        [Input('strategy-search-btn', 'n_clicks'),
         Input('clear-search-btn', 'n_clicks'),
         Input('refresh-strategy-list', 'n_clicks')],
        [State('strategy-search-input', 'value')]
    )
    def search_and_refresh_strategies(search_clicks, clear_clicks, refresh_clicks, search_term):
        """搜索和刷新策略列表"""
        ctx = dash.callback_context
        if not ctx.triggered:
            strategies = db_strategys.get_all_strategies()
        else:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]
            
            if button_id == "clear-search-btn":
                strategies = db_strategys.get_all_strategies()
            elif button_id == "strategy-search-btn" and search_term and search_term.strip():
                strategies = db_strategys.search_strategies(search_term.strip())
            else:
                strategies = db_strategys.get_all_strategies()
        
        table = create_strategy_list_table(strategies)
        stats = create_strategy_stats(strategies)
        
        return table, stats

    # 清除搜索输入
    @app.callback(
        Output('strategy-search-input', 'value'),
        [Input('clear-search-btn', 'n_clicks')]
    )
    def clear_search_input(n_clicks):
        """清除搜索输入框"""
        if n_clicks:
            return ""
        return dash.no_update

    # 保存策略
    @app.callback(
        [Output('strategy-message-container', 'children'),
         Output('strategy-modal', 'is_open', allow_duplicate=True)],
        [Input('save-strategy-btn', 'n_clicks')],
        [State('current-operation', 'data'),
         State('selected-strategy-data', 'data'),
         State('strategy-name-input', 'value'),
         State('strategy-description-input', 'value'),
         State('strategy-cash-input', 'value'),
         State('strategy-data-params-input', 'value'),
         State('strategy-parameters-input', 'value'),
         State('strategy-config-input', 'value')],
        prevent_initial_call=True
    )
    def save_strategy(n_clicks, operation, selected_data, name, description, cash, 
                     data_params, parameters, strategy_config):
        """保存策略（新建或编辑）"""
        if not n_clicks:
            return "", dash.no_update
        
        try:
            # 验证数据
            is_valid, errors = validate_strategy_data(name, description, cash, data_params, parameters, strategy_config)
            if not is_valid:
                error_message = dbc.Alert([
                    html.H6("数据验证失败：", className="mb-2"),
                    html.Ul([html.Li(error) for error in errors])
                ], color="danger", dismissable=True, duration=5000)
                return error_message, True
            
            strategy_data = {
                'name': name,
                'description': description,
                'initial_cash': cash,
                'data_params': data_params,
                'strategy': strategy_config,
                'parameters': parameters
            }
            
            success = False
            message = ""
            
            if operation == "new":
                # 新建策略
                success = db_strategys.add_strategy(strategy_data)
                message = "策略创建成功！" if success else "策略创建失败，请检查数据格式"
            elif operation == "edit" and selected_data:
                # 编辑策略
                success = db_strategys.update_strategy(selected_data['strategy_id'], strategy_data)
                message = "策略更新成功！" if success else "策略更新失败，请检查数据格式"
            
            if success:
                success_message = dbc.Alert([
                    html.I(className="fas fa-check-circle me-2"),
                    message
                ], color="success", dismissable=True, duration=3000)
                return success_message, False  # 关闭模态框
            else:
                error_message = dbc.Alert([
                    html.I(className="fas fa-exclamation-triangle me-2"),
                    message
                ], color="danger", dismissable=True, duration=5000)
                return error_message, True  # 保持模态框打开
                
        except Exception as e:
            error_message = dbc.Alert([
                html.I(className="fas fa-exclamation-triangle me-2"),
                f"操作失败：{str(e)}"
            ], color="danger", dismissable=True, duration=5000)
            return error_message, True

    # 删除策略模态框
    @app.callback(
        [Output('delete-confirm-modal', 'is_open'),
         Output('delete-strategy-info', 'children')],
        [Input('delete-strategy-btn', 'n_clicks'),
         Input('cancel-delete-btn', 'n_clicks'),
         Input('confirm-delete-btn', 'n_clicks')],
        [State('delete-confirm-modal', 'is_open'),
         State('selected-strategy-data', 'data')]
    )
    def toggle_delete_modal(delete_clicks, cancel_clicks, confirm_clicks, is_open, selected_data):
        """控制删除确认模态框"""
        ctx = dash.callback_context
        if not ctx.triggered:
            return False, ""
        
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if button_id == "delete-strategy-btn" and selected_data:
            strategy_info = html.Div([
                html.P([html.Strong("策略名称："), selected_data.get('name', 'N/A')]),
                html.P([html.Strong("策略ID："), str(selected_data.get('strategy_id', 'N/A'))]),
                html.P([html.Strong("初始资金："), f"{selected_data.get('initial_cash', 0):,.0f}元"]),
                html.P([html.Strong("描述："), selected_data.get('description', 'N/A')[:100] + ('...' if len(str(selected_data.get('description', ''))) > 100 else '')])
            ])
            return True, strategy_info
        elif button_id in ["cancel-delete-btn", "confirm-delete-btn"]:
            return False, ""
        
        return is_open, dash.no_update

    # 确认删除策略
    @app.callback(
        [Output('strategy-message-container', 'children', allow_duplicate=True),
         Output('delete-confirm-modal', 'is_open', allow_duplicate=True)],
        [Input('confirm-delete-btn', 'n_clicks')],
        [State('selected-strategy-data', 'data')],
        prevent_initial_call=True
    )
    def confirm_delete_strategy(n_clicks, selected_data):
        """确认删除策略"""
        if not n_clicks or not selected_data:
            return "", dash.no_update
        
        try:
            success = db_strategys.delete_strategy(selected_data['strategy_id'])
            
            if success:
                success_message = dbc.Alert([
                    html.I(className="fas fa-check-circle me-2"),
                    f"策略 '{selected_data.get('name', '')}' 删除成功！"
                ], color="success", dismissable=True, duration=3000)
                return success_message, False
            else:
                error_message = dbc.Alert([
                    html.I(className="fas fa-exclamation-triangle me-2"),
                    "策略删除失败，请稍后重试"
                ], color="danger", dismissable=True, duration=5000)
                return error_message, False
                
        except Exception as e:
            error_message = dbc.Alert([
                html.I(className="fas fa-exclamation-triangle me-2"),
                f"删除失败：{str(e)}"
            ], color="danger", dismissable=True, duration=5000)
            return error_message, False

    # JSON字段实时验证回调
    @app.callback(
        [Output('data-params-feedback', 'children'),
         Output('parameters-feedback', 'children'),
         Output('config-feedback', 'children')],
        [Input('strategy-data-params-input', 'value'),
         Input('strategy-parameters-input', 'value'),
         Input('strategy-config-input', 'value')]
    )
    def validate_json_fields(data_params, parameters, config):
        """实时验证JSON字段格式"""
        def validate_json(json_str):
            if not json_str or json_str.strip() == '':
                return ""
            try:
                json.loads(json_str)
                return ""
            except json.JSONDecodeError as e:
                return f"JSON格式错误: {str(e)}"
        
        data_params_error = validate_json(data_params)
        parameters_error = validate_json(parameters)
        config_error = validate_json(config)
        
        return data_params_error, parameters_error, config_error

    # 简化的输入测试回调
    @app.callback(
        Output('name-feedback', 'children'),
        [Input('strategy-name-input', 'value')],
        prevent_initial_call=True
    )
    def test_input_response(value):
        """测试输入框是否响应用户输入"""
        if value:
            return f"✅ 输入检测到: {value}"
        return "等待输入..."
    
    # 测试现金输入框
    @app.callback(
        Output('cash-feedback', 'children'),
        [Input('strategy-cash-input', 'value')],
        prevent_initial_call=True
    )
    def test_cash_input_response(value):
        """测试现金输入框是否响应"""
        if value:
            return f"✅ 现金输入检测到: {value}"
        return "等待输入..."

