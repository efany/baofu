import pandas as pd
from typing import Dict, Any, Optional
from loguru import logger
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase

class DBStocks:
    """股票数据库操作类"""

    def __init__(self, mysql_db: MySQLDatabase):
        """
        初始化
        
        Args:
            mysql_db: MySQL数据库实例
        """
        self.mysql_db = mysql_db

    def get_stock_info(self, symbol: str) -> pd.DataFrame:
        """
        获取股票基本信息
        
        Args:
            symbol: 股票代码
            
        Returns:
            pd.DataFrame: 股票信息，如果不存在返回空DataFrame
        """
        sql = """
            SELECT 
                symbol, name, currency, exchange, market
            FROM stocks_info 
            WHERE symbol = %s
        """
        result = self.mysql_db.execute_query(sql, (symbol,))
        if not result:
            logger.warning(f"未找到股票{symbol}的基本信息")
            return pd.DataFrame()
        
        return pd.DataFrame(result)

    def insert_stock_info(self, stock_info: Dict[str, Any]) -> bool:
        """
        插入股票信息
        
        Args:
            stock_info: 股票信息字典，包含以下字段：
                - symbol: 股票代码
                - name: 股票名称
                - currency: 交易货币
                - exchange: 交易所
                - market: 市场
                
        Returns:
            bool: 插入是否成功
        """
        sql = """
            INSERT INTO stocks_info (
                symbol, name, currency, exchange, market
            ) VALUES (
                %(symbol)s, %(name)s, %(currency)s, %(exchange)s, %(market)s
            )
        """

        return self.mysql_db.execute_query(sql, stock_info) is not None

    def update_stock_info(self, symbol: str, stock_info: Dict[str, Any]) -> bool:
        """
        更新股票信息
        
        Args:
            symbol: 股票代码
            stock_info: 股票信息字典，包含要更新的字段
            
        Returns:
            bool: 更新是否成功
        """
        # 构建UPDATE语句
        set_clause = ", ".join([f"{k} = %s" for k in stock_info.keys()])
        sql = f"""
            UPDATE stocks_info 
            SET {set_clause}
            WHERE symbol = %s
        """
        
        # 构建参数tuple
        params = tuple(stock_info.values()) + (symbol,)
        
        return self.mysql_db.execute_query(sql, params) is not None

    def get_all_stocks(self) -> pd.DataFrame:
        """
        获取所有股票信息
        
        Returns:
            pd.DataFrame: 所有股票信息
        """
        sql = """
            SELECT 
                symbol, name, currency, exchange, market
            FROM stocks_info
            ORDER BY symbol
        """
        result = self.mysql_db.execute_query(sql)
        if not result:
            return pd.DataFrame()
        
        return pd.DataFrame(result)

    def delete_stock_info(self, symbol: str) -> bool:
        """
        删除股票信息
        
        Args:
            symbol: 股票代码
            
        Returns:
            bool: 删除是否成功
        """
        sql = "DELETE FROM stocks_info WHERE symbol = %s"
        return self.mysql_db.execute_query(sql, (symbol,)) is not None

if __name__ == "__main__":
    # 初始化数据库连接
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )

    # 创建DBStocks实例
    db_stocks = DBStocks(mysql_db)

    # 测试插入股票信息
    test_stock = {
        'symbol': 'AAPL',
        'name': 'Apple Inc.',
        'currency': 'USD',
        'exchange': 'NASDAQ',
        'market': 'US'
    }
    print("插入股票信息结果:", db_stocks.insert_stock_info(test_stock))

    # 测试获取单个股票信息
    print("获取单个股票信息:")
    print(db_stocks.get_stock_info('AAPL'))

    # 测试更新股票信息
    print("更新股票信息结果:", db_stocks.update_stock_info('AAPL', {'name': 'Apple Inc. (Updated)'}))

    # 测试获取所有股票信息
    print("获取所有股票信息:")
    print(db_stocks.get_all_stocks())

    # 测试删除股票信息
    print("删除股票信息结果:", db_stocks.delete_stock_info('AAPL'))

    # 测试获取所有股票信息
    print("获取所有股票信息:")
    print(db_stocks.get_all_stocks())
    # 关闭数据库连接
    mysql_db.close_connection()