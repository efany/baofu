import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.db_strategys import DBStrategys
from database.mysql_database import MySQLDatabase

def register_strategy_manage_callbacks(app, mysql_db: MySQLDatabase):
    """注册策略管理相关的回调"""
    db_strategys = DBStrategys(mysql_db)

    @app.callback(
        [Output('strategy-name-input', 'value'),
         Output('strategy-description-input', 'value'),
         Output('strategy-cash-input', 'value'),
         Output('strategy-data-params-input', 'value'),
         Output('strategy-parameters-input', 'value'),
         Output('strategy-config-input', 'value')],
        Input('strategy-selector', 'value')
    )
    def load_strategy_data(strategy_id):
        """加载选中策略的数据"""
        if not strategy_id:
            return "", "", None, "", "", ""
        if strategy_id == -1:
            return "", "", None, \
                   """{\n\t"fund_codes": ["007540","003376"]\n}""", \
                   """{\n\t"rebalance_period": 20,\n\t"position_size": 50,\n\t"ma_periods": ["MA20", "MA60"],\n\t"show_drawdown": "top3"\n}""", \
                   """{\n\t"name": "BuyAndHold",\n\t"open_date": "<open_date>",\n\t"close_date": "<close_date>",\n\t"dividend_method": "reinvest",\n\t"products": ["007540","003376"],\n\t"weights": [0.5,0.5]\n}"""
        
        strategy = db_strategys.get_strategy(strategy_id)
        if strategy.empty:
            return "", "", None, "", "", ""
            
        strategy_config = json.loads(strategy.iloc[0]['strategy'])
        strategy_json = json.dumps(strategy_config, indent=2)
        if 'parameters' in strategy.iloc[0]:
            parameters = json.loads(strategy.iloc[0]['parameters'])
            parameters_json = json.dumps(parameters, indent=2)
        else:
            parameters_json = '{}'

        return (
            strategy.iloc[0]['name'],
            strategy.iloc[0]['description'],
            strategy.iloc[0]['initial_cash'],
            strategy.iloc[0]['data_params'],
            parameters_json,
            strategy_json
        )

    @app.callback(
        [Output('strategy-message', 'children'),
         Output('strategy-message', 'is_open'),
         Output('strategy-message', 'color'),
         Output('strategy-selector', 'options'),
         Output('strategy-selector', 'value')],
        [Input('save-strategy-btn', 'n_clicks'),
         Input('new-strategy-btn', 'n_clicks'),
         Input('delete-strategy-btn', 'n_clicks')],
        [State('strategy-selector', 'value'),
         State('strategy-name-input', 'value'),
         State('strategy-description-input', 'value'),
         State('strategy-cash-input', 'value'),
         State('strategy-data-params-input', 'value'),
         State('strategy-parameters-input', 'value'),
         State('strategy-config-input', 'value')]
    )
    def handle_strategy_actions(save_clicks, new_clicks, delete_clicks,
                              strategy_id, name, description, cash, 
                              data_params, parameters, strategy_config):
        """处理策略的增删改操作"""
        ctx = dash.callback_context
        if not ctx.triggered:
            return "", False, "primary", dash.no_update, dash.no_update
        
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        message = ""
        is_open = False
        color = "primary"
        new_value = strategy_id  # 默认保持当前选中值
        
        try:
            strategy_data = {
                'name': name,
                'description': description,
                'initial_cash': cash,
                'data_params': data_params,
                'strategy': strategy_config,
                'parameters': parameters
            }
            
            if button_id == "save-strategy-btn" and strategy_id != -1:
                # 更新策略
                if db_strategys.update_strategy(strategy_id, strategy_data):
                    message = "策略更新成功！"
                    is_open = True
                    color = "success"
                else:
                    message = "策略更新失败！"
                    is_open = True
                    color = "danger"
                
            elif button_id == "new-strategy-btn":
                # 新建策略
                if db_strategys.add_strategy(strategy_data):
                    message = "策略创建成功！"
                    is_open = True
                    color = "success"
                    new_value = -1  # 创建成功后重置为新建策略
                else:
                    message = "策略创建失败！"
                    is_open = True
                    color = "danger"
                
            elif button_id == "delete-strategy-btn" and strategy_id != -1:
                # 删除策略
                if db_strategys.delete_strategy(strategy_id):
                    message = "策略删除成功！"
                    is_open = True
                    color = "success"
                    new_value = -1  # 删除成功后重置为新建策略
                else:
                    message = "策略删除失败！"
                    is_open = True
                    color = "danger"
                
        except Exception as e:
            message = f"操作失败：{str(e)}"
            is_open = True
            color = "danger"
        
        # 获取更新后的策略列表
        strategies = db_strategys.get_all_strategies()
        options = [
            {'label': '新建策略', 'value': -1},
            *[{'label': f"{strategy['name']} (id:{int(strategy['strategy_id'])})", 
                'value': int(strategy['strategy_id'])} 
                for index, strategy in strategies.iterrows()]
        ]
        
        return message, is_open, color, options, new_value