from typing import Dict, List, Optional, Any, Tuple, Literal, TypedDict
from datetime import date
import pandas as pd
from loguru import logger
from .data_generator import DataGenerator, TableData
from database.mysql_database import MySQLDatabase
from database.db_strategys import DBStrategys
from task_utils.funds_utils import calculate_max_drawdown
from task_backtrader.backtrader_buy_and_hold_task import BacktraderBuyAndHoldTask

class StrategyDataGenerator(DataGenerator):
    """策略数据生成器"""
    
    def __init__(self, strategy_id: int, mysql_db: MySQLDatabase, start_date: Optional[date] = None, end_date: Optional[date] = None):
        super().__init__(start_date, end_date)
        self.strategy_id = strategy_id
        self.db_strategys = DBStrategys(mysql_db)
        self.mysql_db = mysql_db
        self.strategy_info = None
        self.backtest_result = None
        self._load_data()
    
    def _load_data(self):
        """加载策略数据"""
        
        strategy = self.db_strategys.get_strategy(self.strategy_id)
        if not strategy.empty:
            self.strategy_info = strategy.iloc[0]
            self.strategy_info['strategy'] = self.strategy_info['strategy'] \
                .replace("<open_date>", self.start_date.strftime("%Y-%m-%d") if self.start_date else "") \
                .replace("<close_date>", self.end_date.strftime("%Y-%m-%d") if self.end_date else "")
            task = BacktraderBuyAndHoldTask(self.mysql_db, self.strategy_info)
            task.execute()
            if task.is_success:
                self.backtest_result = task.result
                # 转换日期格式
                if 'daily_asset' in self.backtest_result:
                    daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
                    daily_asset['date'] = pd.to_datetime(daily_asset['date'])
                    # 过滤日期范围
                    if self.start_date:
                        daily_asset = daily_asset[daily_asset['date'].dt.date >= self.start_date]
                    if self.end_date:
                        daily_asset = daily_asset[daily_asset['date'].dt.date <= self.end_date]
                    self.backtest_result['daily_asset'] = daily_asset.to_dict('records')
    
    def get_summary_data(self) -> List[Tuple[str, Any]]:
        """获取策略摘要数据"""
        if self.strategy_info is None or not self.backtest_result:
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
            ('策略名称', self.strategy_info['name']),
            ('策略描述', self.strategy_info['description']),
            ('统计区间', date_range),
            ('区间收益率', f"{return_rate:+.2f}% (¥{initial_value:,.2f} -> ¥{final_value:,.2f})")
        ]
    
    def get_chart_data(self) -> List[Dict[str, Any]]:
        """获取策略图表数据"""
        if not self.backtest_result:
            return []
        
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        dates = daily_asset['date'].tolist()
        
        # 基础数据
        chart_data = [{
            'x': dates,
            'y': daily_asset['total'].tolist(),
            'type': 'line',
            'name': '总资产',
            'visible': True
        }, {
            'x': dates,
            'y': daily_asset['cash'].tolist(),
            'type': 'line',
            'name': '现金',
            'visible': 'legendonly'
        }]
        
        # 添加各个产品的资产数据
        if 'products' in daily_asset.iloc[0]:
            product_codes = list(daily_asset.iloc[0]['products'].keys())
            for product_code in product_codes:
                product_data = [
                    row['products'].get(product_code, 0) 
                    for _, row in daily_asset.iterrows()
                ]
                chart_data.append({
                    'x': dates,
                    'y': product_data,
                    'type': 'line',
                    'name': f'产品{product_code}',
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
    
    def get_extra_chart_data(self, data_type: Literal['MA5', 'MA20', 'MA60', 'MA120', 'drawdown'], **params) -> List[Dict[str, Any]]:
        """获取额外的图表数据"""
        if not self.backtest_result:
            return []
        
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        
        if data_type == 'MA5':
            return self._get_ma_data(5)
        elif data_type == 'MA20':
            return self._get_ma_data(20)
        elif data_type == 'MA60':
            return self._get_ma_data(60)
        elif data_type == 'MA120':
            return self._get_ma_data(120)
        elif data_type == 'drawdown':
            return self._get_drawdown_data()
        else:
            raise ValueError(f"Unknown data type: {data_type}")
    
    def _get_ma_data(self, period: int) -> List[Dict[str, Any]]:
        """获取移动平均线数据"""
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        dates = daily_asset['date'].tolist()
        ma_data = []
        
        ma = daily_asset['total'].rolling(window=period).mean()
        ma_data.append({
            'x': dates,
            'y': ma.tolist(),
            'type': 'line',
            'name': f'MA{period}',
            'visible': True,
            'line': {'dash': 'dot'}
        })
        
        return ma_data

    def _get_drawdown_data(self) -> List[Dict[str, Any]]:
        """获取回撤数据"""
        daily_asset = pd.DataFrame(self.backtest_result['daily_asset'])
        drawdown_list = calculate_max_drawdown(
            daily_asset['date'],
            daily_asset['total']
        )
        
        data = []
        # 绘制回撤区域
        for i in range(len(drawdown_list)):
            if pd.notna(drawdown_list[i]):
                drawdown_value = drawdown_list[i]['value']
                drawdown_start_date = drawdown_list[i]['start_date']
                drawdown_start_value = drawdown_list[i]['start_value']
                drawdown_end_date = drawdown_list[i]['end_date']
                drawdown_end_value = drawdown_list[i]['end_value']
                recovery_date = drawdown_list[i]['recovery_date']

                drawdown_days = (drawdown_end_date - drawdown_start_date).days
                recovery_days = (recovery_date - drawdown_end_date).days if recovery_date else None
                
                text = f'回撤: {drawdown_value*100:.4f}%({drawdown_days} days)' 
                if recovery_days:
                    text = f'{text}，修复：{recovery_days} days'
                # 添加矩形区域
                data.append({
                    'type': 'scatter',
                    'x': [drawdown_start_date, drawdown_end_date, drawdown_end_date, drawdown_start_date, drawdown_start_date],
                    'y': [drawdown_start_value, drawdown_start_value, drawdown_end_value, drawdown_end_value, drawdown_start_value],
                    'fill': 'toself',
                    'fillcolor': 'rgba(255, 0, 0, 0.2)',
                    'line': {'width': 0},
                    'mode': 'lines+text',  # 添加文本模式
                    'text': [text],  # 显示回撤值
                    'textposition': 'top right',  # 文本位置
                    'textfont': {'size': 12, 'color': 'red'},  # 文本样式
                    'name': f'TOP{i+1} 回撤',
                    'showlegend': True
                })
                if recovery_days:
                    data.append({
                        'type': 'scatter',
                        'x': [drawdown_end_date, recovery_date, recovery_date, drawdown_end_date, drawdown_end_date],
                        'y': [drawdown_end_value, drawdown_end_value, drawdown_start_value, drawdown_start_value, drawdown_end_value],
                        'fill': 'toself',
                        'fillcolor': 'rgba(0, 255, 0, 0.2)',
                        'line': {'width': 0},
                        'mode': 'lines+text',  # 添加文本模式
                        'textfont': {'size': 12, 'color': 'red'},  # 文本样式
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
        if self.start_date:
            trades = trades[trades['date'] >= self.start_date]
        if self.end_date:
            trades = trades[trades['date'] <= self.end_date]
        
        # 转换为表格数据
        trade_data = []
        for _, trade in trades.iterrows():
            amount = trade['price'] * trade['size']
            trade_data.append([
                trade['date'].strftime('%Y-%m-%d'),
                trade['type'],
                trade['product'],
                f"{trade['size']:.0f}",
                f"¥{trade['price']:.4f}",
                f"¥{amount:.2f}",
                trade.get('order_message', '')
            ])
        
        return {
            'name': '交易记录',
            'headers': ['日期', '类型', '产品', '数量', '价格', '金额', '备注'],
            'data': trade_data
        } 