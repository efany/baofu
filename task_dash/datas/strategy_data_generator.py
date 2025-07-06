from typing import Dict, List, Optional, Any, Tuple, Literal, TypedDict
from datetime import date
import pandas as pd
from loguru import logger
from .data_generator import DataGenerator, TableData, ChartDataType, ParamConfig
from database.mysql_database import MySQLDatabase
from database.db_strategys import DBStrategys
from task_utils.data_utils import calculate_max_drawdown
from task_backtrader.backtrader_task import BacktraderTask
import json

class StrategyDataGenerator(DataGenerator):
    """策略数据生成器"""
    
    def __init__(self, strategy_id: int, mysql_db: MySQLDatabase, start_date: Optional[date] = None, end_date: Optional[date] = None):
        super().__init__(start_date, end_date)
        self.strategy_id = strategy_id
        self.db_strategys = DBStrategys(mysql_db)
        self.mysql_db = mysql_db
        self.backtest_result = None
        self.strategys = self.db_strategys.get_strategy(self.strategy_id)
        self.parameter_configs = []
        self.params = {}
        if not self.strategys.empty:
            self.strategy = self.strategys.iloc[0]
        else:
            self.strategy = None
        
        if self.strategy is not None:
            parameters = json.loads(self.strategy['parameters'])
            if not parameters:
                return []
            parameters = parameters['parameters']
            for parameter in parameters:
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
            # 替换日期占位符
            strategy_info['strategy'] = strategy_info['strategy'] \
                .replace("<open_date>", self.start_date.strftime("%Y-%m-%d") if self.start_date else "") \
                .replace("<close_date>", self.end_date.strftime("%Y-%m-%d") if self.end_date else "")

            for param_name, param_value in self.params.items():
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
                    # 过滤日期范围
                    self.backtest_result['daily_asset'] = daily_asset.to_dict('records')
                return True
            else:
                return False
            
        except Exception as e:
            logger.error(f"加载策略数据失败: {str(e)}")
            return False
        
    def get_params_config(self) -> List[ParamConfig]:
        """获取策略的可调节参数配置"""
        return self.parameter_configs
    
    def update_params(self, params: Dict[str, Any]) -> bool:
        """更新策略的参数设置"""
        for param_name, param_value in params.items():
            if param_name in self.params:
                self.params[param_name] = param_value
        return True
    
    def get_summary_data(self) -> List[Tuple[str, Any]]:
        """获取策略摘要数据"""
        if self.strategy is None or not self.backtest_result:
            return []
        
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        
        # 计算收益率
        initial_value = daily_asset.iloc[0]['total']
        final_value = daily_asset.iloc[-1]['total']
        return_rate = (final_value - initial_value) / initial_value * 100
        
        # 获取起止日期
        start_date = daily_asset.iloc[0]['date'].strftime('%Y-%m-%d')
        end_date = daily_asset.iloc[-1]['date'].strftime('%Y-%m-%d')
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
        if not self.backtest_result:
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
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        
        # 计算收益率
        initial_value = daily_asset.iloc[0]['total']
        final_value = daily_asset.iloc[-1]['total']
        return_rate = (final_value - initial_value) / initial_value * 100
        
        # 计算年化收益率
        days = (daily_asset.iloc[-1]['date'] - daily_asset.iloc[0]['date']).days
        annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
        
        # 计算风险指标
        returns = daily_asset['total'].pct_change()
        volatility = returns.std() * (252 ** 0.5) * 100  # 年化波动率
        
        return {
            'name': '基础指标',
            'headers': ['指标', '数值'],
            'data': [
                ['投资收益率', f'{return_rate:+.2f}% (¥{initial_value:,.2f} -> ¥{final_value:,.2f})'],
                ['年化收益率', f'{annualized_return:+.2f}%'],
                ['投资最大回撤', self._get_max_drawdown()],
                ['年化波动率', f'{volatility:.2f}%']
            ]
        }
    
    def _get_yearly_stats(self) -> TableData:
        """获取年度统计表格"""
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        daily_asset['year'] = daily_asset['date'].dt.year
        
        yearly_stats = []
        for year in sorted(daily_asset['year'].unique(), reverse=True):
            year_data = daily_asset[daily_asset['year'] == year]
            
            # 获取年度起止日期
            start_date = year_data.iloc[0]['date'].strftime('%Y-%m-%d')
            end_date = year_data.iloc[-1]['date'].strftime('%Y-%m-%d')
            
            # 计算年度收益率
            start_value = year_data.iloc[0]['total']
            end_value = year_data.iloc[-1]['total']
            return_rate = (end_value - start_value) / start_value * 100
            
            # 计算年化收益率
            days = (year_data.iloc[-1]['date'] - year_data.iloc[0]['date']).days
            annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
            
            # 计算年度最大回撤
            drawdown_list = calculate_max_drawdown(
                year_data['date'],
                year_data['total']
            )
            max_drawdown = f"{drawdown_list[0]['value']*100:.2f}%" if drawdown_list else 'N/A'
            
            # 计算年度波动率
            returns = year_data['total'].pct_change()
            volatility = returns.std() * (252 ** 0.5) * 100
            
            yearly_stats.append([
                f"{year} ({start_date}~{end_date})",
                f'{return_rate:+.2f}%',
                f'{annualized_return:+.2f}%',
                max_drawdown,
                f'{volatility:.2f}%'
            ])
        
        return {
            'name': '年度统计',
            'headers': ['年份', '收益率', '年化收益率', '最大回撤', '波动率'],
            'data': yearly_stats
        }
    
    def _get_quarterly_stats(self) -> TableData:
        """获取季度统计表格"""
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        daily_asset['year'] = daily_asset['date'].dt.year
        daily_asset['quarter'] = daily_asset['date'].dt.quarter
        
        quarterly_stats = []
        for year in sorted(daily_asset['year'].unique(), reverse=True):
            year_data = daily_asset[daily_asset['year'] == year]
            for quarter in sorted(year_data['quarter'].unique(), reverse=True):
                quarter_data = year_data[year_data['quarter'] == quarter]
                
                # 获取季度起止日期
                start_date = quarter_data.iloc[0]['date'].strftime('%Y-%m-%d')
                end_date = quarter_data.iloc[-1]['date'].strftime('%Y-%m-%d')
                
                # 计算季度收益率
                start_value = quarter_data.iloc[0]['total']
                end_value = quarter_data.iloc[-1]['total']
                return_rate = (end_value - start_value) / start_value * 100
                
                # 计算年化收益率
                days = (quarter_data.iloc[-1]['date'] - quarter_data.iloc[0]['date']).days
                annualized_return = ((1 + return_rate/100) ** (365/days) - 1) * 100 if days > 0 else 0
                
                # 计算季度最大回撤
                drawdown_list = calculate_max_drawdown(
                    quarter_data['date'],
                    quarter_data['total']
                )
                max_drawdown = f"{drawdown_list[0]['value']*100:.2f}%" if drawdown_list else 'N/A'
                
                # 计算季度波动率
                returns = quarter_data['total'].pct_change()
                volatility = returns.std() * (252 ** 0.5) * 100
                
                quarterly_stats.append([
                    f"{year}Q{quarter} ({start_date}~{end_date})",
                    f'{return_rate:+.2f}%',
                    f'{annualized_return:+.2f}%',
                    max_drawdown,
                    f'{volatility:.2f}%'
                ])
        
        return {
            'name': '季度统计',
            'headers': ['季度', '收益率', '年化收益率', '最大回撤', '波动率'],
            'data': quarterly_stats
        }
    
    def _get_max_drawdown(self) -> str:
        """计算最大回撤"""
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        drawdown_list = calculate_max_drawdown(
            daily_asset['date'],
            daily_asset['total']
        )
        
        if drawdown_list and len(drawdown_list) > 0:
            dd = drawdown_list[0]
            max_dd = dd['value'] * 100
            start_date = dd['start_date'].strftime('%Y-%m-%d')
            end_date = dd['end_date'].strftime('%Y-%m-%d')
            start_value = dd['start_value']
            end_value = dd['end_value']
            
            # 如果有恢复日期，添加恢复信息
            recovery_info = ""
            if dd.get('recovery_date'):
                days_to_recover = (dd['recovery_date'] - dd['end_date']).days
                recovery_info = f", 恢复天数: {days_to_recover}天"
            
            return f"{max_dd:.2f}% ({start_date}~{end_date}{recovery_info}, ¥{start_value:,.2f}->¥{end_value:,.2f})"
        
        return 'N/A'
    
    def get_extra_chart_data(self, data_type: ChartDataType, normalize: bool = False, **params) -> List[Dict[str, Any]]:
        """获取额外的图表数据"""
        if not self.backtest_result:
            return []
            
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        
        if data_type in ['MA5', 'MA20', 'MA60', 'MA120']:
            period = int(data_type.replace('MA', ''))
            return self._get_ma_data(period, daily_asset, normalize)
        elif data_type == 'drawdown':
            return self._get_drawdown_data(daily_asset, normalize)
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    def _get_ma_data(self, period: int, daily_asset: pd.DataFrame, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取移动平均线数据"""
        dates = daily_asset['date'].tolist()
        ma_data = []
        
        # 计算总资产的移动平均线
        values = daily_asset['total']
        if normalize:
            values = self.normalize_series(values)
            
        ma = values.rolling(window=period).mean()
        ma_data.append({
            'x': dates,
            'y': ma.tolist(),
            'type': 'line',
            'name': f'MA{period}',
            'visible': True,
            'line': {'dash': 'dot'}
        })
        
        return ma_data

    def _get_drawdown_data(self, daily_asset: pd.DataFrame, normalize: bool = False) -> List[Dict[str, Any]]:
        """获取回撤数据"""
        values = daily_asset['total']
        if normalize:
            values = self.normalize_series(values)
            
        drawdown_list = calculate_max_drawdown(
            daily_asset['date'],
            values
        )
        
        data = []
        # 绘制回撤区域
        for i in range(len(drawdown_list)):
            if pd.notna(drawdown_list[i]):
                dd = drawdown_list[i]
                drawdown_days = (dd['end_date'] - dd['start_date']).days
                recovery_days = (dd['recovery_date'] - dd['end_date']).days if dd.get('recovery_date') else None
                
                text = f'回撤: {dd["value"]*100:.4f}%({drawdown_days} days)' 
                if recovery_days:
                    text = f'{text}，修复：{recovery_days} days'
                    
                data.append({
                    'type': 'scatter',
                    'x': [dd['start_date'], dd['end_date'], dd['end_date'], dd['start_date'], dd['start_date']],
                    'y': [dd['start_value'], dd['start_value'], dd['end_value'], dd['end_value'], dd['start_value']],
                    'fill': 'toself',
                    'fillcolor': 'rgba(255, 0, 0, 0.2)',
                    'line': {'width': 0},
                    'mode': 'lines+text',
                    'text': [text],
                    'textposition': 'top right',
                    'textfont': {'size': 12, 'color': 'red'},
                    'name': f'TOP{i+1} 回撤',
                    'showlegend': True
                })
                
                if recovery_days:
                    data.append({
                        'type': 'scatter',
                        'x': [dd['end_date'], dd['recovery_date'], dd['recovery_date'], dd['end_date'], dd['end_date']],
                        'y': [dd['end_value'], dd['end_value'], dd['start_value'], dd['start_value'], dd['end_value']],
                        'fill': 'toself',
                        'fillcolor': 'rgba(0, 255, 0, 0.2)',
                        'line': {'width': 0},
                        'mode': 'lines+text',
                        'textfont': {'size': 12, 'color': 'red'},
                        'name': f'TOP{i+1} 回撤修复',
                        'showlegend': True
                    })
        return data
    
    def _get_trade_table(self):
        """获取交易记录表格"""
        if not self.backtest_result or 'trades' not in self.backtest_result:
            return {
                'name': '交易记录',
                'headers': ['日期', '类型', '产品', '数量', '价格', '金额', '备注'],
                'data': []
            }
        
        trades = pd.DataFrame(self.backtest_result['trades'])
        if trades.empty:
            return {
                'name': '交易记录',
                'headers': ['日期', '类型', '产品', '数量', '价格', '金额', '备注'],
                'data': []
            }
        
        # 过滤日期范围
        # if self.start_date:
        #     trades = trades[trades['date'] >= self.start_date]
        # if self.end_date:
        #     trades = trades[trades['date'] <= self.end_date]
        
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
            'headers': ['日期', '类型', '产品', '数量', '价格', '金额', '备注'],
            'data': trade_data
        }

    def get_value_data(self) -> pd.DataFrame:
        """获取策略总资产数据"""
        if not self.backtest_result or 'daily_asset' not in self.backtest_result:
            return pd.DataFrame()
        
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        return pd.DataFrame({
            'date': daily_asset['date'],
            'value': daily_asset['total']
        }) 