from datetime import datetime
from loguru import logger
from typing import Dict, Any, List
import math
from .extend_strategy import ExtendStrategy

class FinancingStrategy(ExtendStrategy):
    """处理外汇融资的策略"""
    
    def __init__(self, params: Dict[str, Any]):
        """
        初始化策略
        
        Args:
            params: 策略参数，包含：
                - forex_financing: 外汇融资配置，格式为 {"货币对": 融资金额}
                - financing_leverage: 融资杠杆倍数，默认为1
        """
        super().__init__(params)
        
        # 添加外汇融资配置
        self.forex_financing = self.params.get('forex_financing', {})  # 例如: {"USDCNH": 50000}
        self.is_initialized = False
        self.last_trade_date = None  # 记录上一个交易日

        logger.info(f"外汇融资配置: {self.forex_financing}")
        

    def record_financing_trade(self, pair: str, amount: float, shares: float, price: float, 
                             trade_type: str = 'open', order_message: str = None) -> None:
        """
        记录融资交易
        
        Args:
            pair: 货币对
            amount: 融资金额
            shares: 融资份额
            price: 融资价格
            trade_type: 交易类型 ('open' 或 'close')
            order_message: 订单消息
        """
        trade_record = {
            'trade_id': len(self.main_strategy.financing_trades) + 1,
            'date': self.main_strategy.datetime.date(),
            'product': pair,
            'type': trade_type,
            'amount': amount,
            'executed_size': shares,
            'executed_price': price,
            'rate': self.get_financing_rate(self.get_data(pair)),
            'status': 'completed',
            'order_message': order_message
        }
        self.main_strategy.financing_trades.append(trade_record)
        logger.info(f"记录融资交易: {trade_record}")

    def open_trade(self):
        """开仓"""
        self.initialize()
    
    def close_trade(self):
        """平仓"""
        pass
            
    def next(self):
        """执行策略逻辑"""
        if not self.is_initialized:
            return

        current_date = self.main_strategy.datas[0].datetime.date(0)
        
        # 如果是第一个交易日，初始化 last_trade_date
        if self.last_trade_date is None:
            self.last_trade_date = current_date
            return

        # 计算交易日之间的天数
        days_diff = (current_date - self.last_trade_date).days
        if days_diff <= 0:
            return

        # 计算并扣除每个货币对的融资利息
        total_financing_amount = 0
        total_interest = 0
        for pair, info in self.main_strategy.financing_info.items():
            # 获取当前货币对的数据
            data = self.get_data(pair)
            if data is None:
                continue

            # 获取当前价值
            current_value = info['shares'] * data.close[0]
            total_financing_amount += current_value
            # 计算融资利率（年化）
            rate = self.get_financing_rate(data)
            
            # 计算每日利息（按366天计算）
            daily_interest = current_value * rate / 366
            
            # 计算总利息
            interest = daily_interest * days_diff
            total_interest += interest

            # 更新累计融资利息
            self.main_strategy.financing_info[pair]['total_interest'] += interest

        # 扣除总利息
        if total_interest > 0:
            current_cash = self.broker.getcash()
            self.broker.setcash(current_cash - total_interest)
        
        # 检查每个货币对的融资价值是否需要调整
        for pair, ratio_str in self.forex_financing.items():
            ratio = float(ratio_str)
            # 获取当前货币对的数据
            data = self.get_data(pair)
            if data is None:
                continue

            # 获取当前价格和份额
            current_price = data.close[0]
            current_shares = self.main_strategy.financing_info[pair]['shares']
            
            # 计算当前融资价值和目标融资价值
            current_value = current_shares * current_price
            target_value = (self.main_strategy.get_total_asset() - total_financing_amount) * ratio
            
            # 计算价值偏离比例
            value_deviation = abs(current_value - target_value) / target_value
            
            # 如果偏离超过1%，进行调整
            if value_deviation > 0.05:
                logger.info(f"货币对 {pair} 的融资价值偏离超过1%，进行调整, current_value: {current_value}, target_value: {target_value}")
                # 计算目标份额
                target_shares = math.floor(target_value / current_price)
                shares_diff = target_shares - current_shares
                
                if shares_diff != 0:
                    # 更新融资份额
                    self.main_strategy.financing_info[pair]['shares'] = target_shares
                    self.broker.setcash(self.broker.getcash() + (shares_diff * current_price))
                    
                    # 记录融资调整交易
                    trade_type = 'financing_increase' if shares_diff > 0 else 'financing_decrease'
                    self.record_financing_trade(
                        pair=pair,
                        amount=abs(shares_diff * current_price),
                        shares=abs(shares_diff),
                        price=current_price,
                        trade_type=trade_type,
                        order_message=f"融资增持" if shares_diff > 0 else f"融资减少"
                    )

        # 更新上一个交易日
        self.last_trade_date = current_date

    def initialize(self) -> bool:
        """初始化外汇融资"""
        # 获取初始资金
        current_cash = self.broker.getcash()
        logger.info(f"当前现金: {current_cash}")
        
        total_financing_amount = 0
        # 遍历外汇融资配置
        for pair, ratio_str in self.forex_financing.items():
            ratio = float(ratio_str)
            # 计算融资金额
            financing_amount = current_cash * ratio

            # 获取当前货币对的价格
            data = self.get_data(pair)
            if data is None:
                logger.warning(f"无法获取 {pair} 的价格数据")
                continue
                
            # 计算可融资的份额
            price = data.close[0]  # 获取当前价格
            if price <= 0:
                logger.warning(f"{pair} 的价格无效: {price}")
                continue

            # 计算融资份额
            financing_shares = math.floor(financing_amount / price)
            
            # 更新融资金额
            total_financing_amount += financing_amount

            self.main_strategy.financing_info[pair] = {
                'shares': financing_shares,
                'total_interest': 0.0
            }

            # 记录融资交易
            self.record_financing_trade(
                pair=pair,
                amount=financing_amount,
                shares=financing_shares,
                price=price,
                trade_type='financing_open',
                order_message=f"融资开仓"
            )

            # 记录日志
            logger.info(f"为 {pair} 配资融资: {financing_amount:.2f}, 融资价格: {price:.4f}, 融资份额: {financing_shares:.2f}, 当前现金: {self.broker.getcash():.2f}")

        # 更新现金
        self.broker.setcash(current_cash + total_financing_amount)
        # 标记初始化完成
        self.is_initialized = True
        
        return True
    
    def get_data(self, pair: str):
        """获取数据"""
        return self.main_strategy.getdatabyname(pair)

    def get_financing_rate(self, data) -> float:
        """获取融资利率"""
        # 这里可以根据实际情况实现融资利率的计算
        # 例如：根据货币对、市场利率等计算
        if data._name == 'JPYCNH':
            return 0.01843
        elif data._name == 'CHFCNH':
            return 0.01567
        else:
            return 0.05
