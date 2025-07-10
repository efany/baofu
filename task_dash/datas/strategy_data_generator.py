from typing import Dict, List, Optional, Any, Tuple, Literal, TypedDict
from datetime import date
import pandas as pd
from loguru import logger
from .data_generator import DataGenerator, TableData, ChartDataType, ParamConfig
from database.mysql_database import MySQLDatabase
from database.db_strategys import DBStrategys
from task_backtrader.backtrader_task import BacktraderTask
import json
from .data_calculator import DataCalculator

class StrategyDataGenerator(DataGenerator):
    """策略数据生成器"""
    
    def __init__(self, strategy_id: int, mysql_db: MySQLDatabase, start_date: Optional[date] = None, end_date: Optional[date] = None):
        super().__init__(start_date, end_date)
        self.strategy_id = strategy_id
        self.db_strategys = DBStrategys(mysql_db)
        self.mysql_db = mysql_db
        
        self.backtest_result = None
        self.strategys = self.db_strategys.get_strategy(self.strategy_id)
        self.data = None

        if not self.strategys.empty:
            self.strategy = self.strategys.iloc[0]
        else:
            self.strategy = None
        
        if self.strategy is not None:
            self.parse_params(self.strategy['parameters'])
            

    def parse_params(self, params_str: str) -> bool:
        """解析策略参数"""
        parameter_json = json.loads(params_str)
        if not parameter_json:
            return []
        parameter_json = parameter_json['parameters']
        
        for parameter in parameter_json:
            if parameter['type'] == 'float':
                param = ParamConfig(
                    type=parameter['type'],
                    name=parameter['name'],
                    label=parameter['label'],
                    value=float(parameter['value']),
                    min=float(parameter['min']),
                    max=float(parameter['max']),
                    step=float(parameter['step'])
                )
                self.params[parameter['name']] = parameter['value']
                self.parameter_configs.append(param)
            elif parameter['type'] == 'select':
                param = ParamConfig(
                    type=parameter['type'],
                    name=parameter['name'],
                    label=parameter['label'],
                    value=parameter['value'],
                    options=parameter['options']
                )
                self.params[parameter['name']] = parameter['value']
                self.parameter_configs.append(param)
    
    def load(self) -> bool:
        """加载策略数据"""
        if self.strategy is None:
            return False
        
        strategy_info = {
            'name': self.strategy['name'],
            'description': self.strategy['description'],
            'initial_cash': self.strategy['initial_cash'],
            'data_params': self.strategy['data_params'],
            'strategy': self.strategy['strategy']
        }
        
        # 获取策略参数
        try:
            for param_name, param_value in self.params.items():
                logger.info(f"param_name: {param_name}, param_value: {param_value}")
                if param_name == 'start_date' or param_name == 'end_date':
                    param_name = 'open_date' if param_name == 'start_date' else 'close_date'
                    param_value = param_value.strftime("%Y-%m-%d") if param_value is not None else ""
                strategy_info['strategy'] = strategy_info['strategy'] \
                    .replace(f"<{param_name}>", str(param_value))
            
            # 创建回测任务
            task = BacktraderTask(self.mysql_db, strategy_info)
            task.execute()
            if task.is_success:
                logger.info(f"回测任务执行成功")
                self.backtest_result = task.result
                # 转换日期格式
                if 'daily_asset' in self.backtest_result:
                    daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
                    daily_asset['date'] = pd.to_datetime(daily_asset['date'])
                    self.data = daily_asset
                return True
            else:
                return False
            
        except Exception as e:
            logger.error(f"加载策略数据失败: {str(e)}")
            return False
    
    def get_summary_data(self) -> List[Tuple[str, Any]]:
        """获取策略摘要数据"""
        if self.data is None or self.data.empty:
            return []

        # 计算收益率
        initial_value = self.data.iloc[0]['total']
        final_value = self.data.iloc[-1]['total']
        return_rate = (final_value - initial_value) / initial_value * 100

        # 获取起止日期
        start_date = self.data.iloc[0]['date'].strftime('%Y-%m-%d')
        end_date = self.data.iloc[-1]['date'].strftime('%Y-%m-%d')
        date_range = f"{start_date} ~ {end_date}"

        return [
            ('策略ID', self.strategy_id), 
            ('策略名称', self.strategy['name']),
            ('策略描述', self.strategy['description']),
            ('统计区间', date_range),
            ('区间收益率', f"{return_rate:+.2f}% (¥{initial_value:,.2f} -> ¥{final_value:,.2f})")
        ]
    
    def get_chart_data(self, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取策略图表数据"""
        if not self.backtest_result:
            return []
        
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        dates = daily_asset['date'].tolist()
        
        # 准备数据
        total = daily_asset['total']
        cash = daily_asset['cash']
        
        # 如果需要归一化处理
        if normalize:
            total = self.normalize_series(total)
        
        # 基础数据
        chart_data = [{
            'x': dates,
            'y': total.tolist(),
            'type': 'line',
            'name': '总资产',
            'visible': True
        }, {
            'x': dates,
            'y': cash.tolist(),
            'type': 'line',
            'name': '现金',
            'visible': 'legendonly'
        }]
        
        # 添加各个产品的资产数据
        if 'products' in daily_asset.iloc[0]:
            product_codes = list(daily_asset.iloc[0]['products'].keys())
            for product_code in product_codes:
                product_data = pd.Series([
                    row['products'].get(product_code, 0) 
                    for _, row in daily_asset.iterrows()
                ])
                
                if normalize:
                    product_data = self.normalize_series(product_data)
                
                # 如果产品数据为空，或Y轴均为0值，则不添加到图表中
                if product_data.empty:
                    continue
                if product_data.tolist().count(0) == len(product_data):
                    continue
                
                chart_data.append({
                    'x': dates,
                    'y': product_data.tolist(),
                    'type': 'line',
                    'name': f'{product_code}持仓',
                    'visible': 'legendonly'
                })
        
        # 添加融资数据
        if 'financing' in daily_asset.columns:
            financing = daily_asset['financing']
            chart_data.append({
                'x': dates,
                'y': financing.tolist(),
                'type': 'line',
                'name': '融资',
                'visible': 'legendonly'
            })
            financing_interest = daily_asset['financing_interest']
            chart_data.append({
                'x': dates,
                'y': financing_interest.tolist(),
                'type': 'line',
                'name': '融资利息',
                'visible': 'legendonly'
            })
        
        if 'cash_interest' in daily_asset.columns:
            cash_interest = daily_asset['cash_interest']
            chart_data.append({
                'x': dates,
                'y': cash_interest.tolist(),
                'type': 'line',
                'name': '现金利息',
                'visible': 'legendonly'
            })

        # 添加每个产品的利息数据
        if 'product_interests' in daily_asset.iloc[0]:
            product_codes = []
            product_interests = daily_asset['product_interests']
            # 遍历所有日期的product_interests，收集所有计息产品代码
            product_codes = set()
            for _, row in daily_asset.iterrows():
                if 'product_interests' in row and isinstance(row['product_interests'], dict):
                    product_codes.update(row['product_interests'].keys())
            product_codes = list(product_codes)
            for product_code in product_codes:
                product_interest_data = pd.Series([
                    row['product_interests'].get(product_code, 0) 
                    for _, row in daily_asset.iterrows()
                ])
                if product_interest_data.empty:
                    continue
                if product_interest_data.tolist().count(0) == len(product_interest_data):
                    continue
                chart_data.append({
                    'x': dates,
                    'y': product_interest_data.tolist(),
                    'type': 'line',
                    'name': f'{product_code}利息',
                    'visible': 'legendonly'
                    })
        
        # 添加配对数据
        if 'pairing' in self.backtest_result:
            pairing = pd.DataFrame(self.backtest_result['pairing'])
            pairing['date'] = pd.to_datetime(pairing['date'])

            for pair_key in pairing.keys():
                if pair_key == 'date':
                    continue
                pair_value = pairing[pair_key]
                chart_data.append({
                    'x': dates,
                    'y': pair_value.tolist(),
                    'type': 'line',
                    'name': pair_key,
                    'visible': 'legendonly'
                })
            
        return chart_data
    
    def get_extra_datas(self) -> List[TableData]:
        """获取策略额外数据"""
        if self.data is None or self.data.empty:
            return []
        
        # 交易记录表格
        trade_table = self._get_trade_table()
        logger.info(f"get trade_table")
        # 基础指标表格
        basic_table = self._get_basic_indicators()
        logger.info(f"get basic_table")
        # 年度统计表格
        yearly_table = self._get_yearly_stats()
        logger.info(f"get yearly_table")
        # 季度统计表格
        quarterly_table = self._get_quarterly_stats()
        logger.info(f"get quarterly_table")
        return [trade_table, basic_table, yearly_table, quarterly_table]
    
    def _get_basic_indicators(self) -> TableData:
        """获取基础指标表格"""
        return super()._get_basic_indicators('date', 'total', '.2f')
    
    def _get_yearly_stats(self) -> TableData:
        """获取年度统计表格"""
        return super()._get_yearly_stats('date', 'total')
    
    def _get_quarterly_stats(self) -> TableData:
        """获取季度统计表格"""
        return super()._get_quarterly_stats('date', 'total')
    
    def get_extra_chart_data(self, data_type: ChartDataType, normalize: bool = False, **params) -> List[Dict[str, Any]]:
        """获取额外的图表数据"""
        if self.data is None or self.data.empty:
            return []
            
        if data_type in ['MA5', 'MA20', 'MA60', 'MA120']:
            period = int(data_type.replace('MA', ''))
            return self._get_ma_data(period, 'date', 'total', normalize)
        elif data_type == 'drawdown':
            return self._get_drawdown_data('date', 'total', normalize)
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    
    def _get_trade_table(self):
        """获取交易记录表格"""
        if not self.backtest_result or 'trades' not in self.backtest_result:
            return {
                'name': '交易记录',
                'pd_data': pd.DataFrame(columns=['日期', '类型', '产品', '数量', '价格', '金额', '备注'])
            }
        
        trades = pd.DataFrame(self.backtest_result['trades'])
        if trades.empty:
            return {
                'name': '交易记录',
                'pd_data': pd.DataFrame(columns=['日期', '类型', '产品', '数量', '价格', '金额', '备注'])
            }
        
        # 转换为表格数据
        trade_data = []
        for _, trade in trades.iterrows():
            amount = trade['executed_size'] * trade['executed_price']
            trade_data.append([
                trade['date'].strftime('%Y-%m-%d'),
                trade['type'],
                trade['product'],
                f"{trade['executed_size']:.0f}",
                f"¥{trade['executed_price']:.4f}",
                f"¥{amount:.2f}",
                trade.get('order_message', '')
            ])
        
        return {
            'name': '交易记录',
            'pd_data': pd.DataFrame(trade_data, columns=['日期', '类型', '产品', '数量', '价格', '金额', '备注'])
        }

    def get_value_data(self) -> pd.DataFrame:
        """获取策略总资产数据"""
        if self.data is None or self.data.empty:
            return pd.DataFrame()

        return pd.DataFrame({
            'date': self.data['date'],
            'value': self.data['total']
        }) 