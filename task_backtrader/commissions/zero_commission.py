import backtrader as bt
from loguru import logger

class ZeroCommission(bt.CommInfoBase):
    """
    零费率佣金方案
    
    特点:
    - 不收取任何交易费用
    - 支持股票和基金交易
    - 支持做多和做空
    """
    
    params = (
        ('stocklike', True),  # 类似股票的资产
        ('commtype', bt.CommInfoBase.COMM_FIXED),  # 固定费用模式
    )

    def _getcommission(self, size, price, pseudoexec):
        """
        计算佣金
        
        Args:
            size: 交易数量
            price: 交易价格
            pseudoexec: 是否是虚拟执行
            
        Returns:
            float: 佣金金额，始终返回0
        """
        return 0.0

    def get_margin(self, price):
        """
        计算保证金
        
        Args:
            price: 交易价格
            
        Returns:
            float: 保证金金额，等于价格（全额保证金）
        """
        return price

    def getsize(self, price, cash):
        """
        计算可以购买的数量
        
        Args:
            price: 交易价格
            cash: 可用资金
            
        Returns:
            float: 可以购买的数量
        """
        # 由于没有手续费，直接用现金除以价格
        return cash / price if price else 0

    def get_leverage(self):
        """
        获取杠杆率
        
        Returns:
            float: 杠杆率，始终为1（不支持杠杆）
        """
        return 1.0

    def getvaluesize(self, size, price):
        """
        计算持仓市值
        
        Args:
            size: 持仓数量
            price: 当前价格
            
        Returns:
            float: 持仓市值
        """
        return size * price

    def profitandloss(self, size, price, newprice):
        """
        计算盈亏
        
        Args:
            size: 持仓数量
            price: 建仓价格
            newprice: 当前价格
            
        Returns:
            float: 盈亏金额
        """
        return size * (newprice - price) 