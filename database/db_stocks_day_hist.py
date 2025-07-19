import pandas as pd
from typing import Dict, Any, Optional, List
from loguru import logger
import os
import sys
from datetime import datetime, date

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase

class DBStocksDayHist:
    """股票日线历史数据库操作类"""

    def __init__(self, mysql_db: MySQLDatabase):
        """
        初始化
        
        Args:
            mysql_db: MySQL数据库实例
        """
        self.mysql_db = mysql_db

    def get_stock_hist_data(self, symbol: str, start_date: date = None, end_date: date = None) -> pd.DataFrame:
        """
        获取股票历史数据
        
        Args:
            symbol: 股票代码
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            pd.DataFrame: 股票历史数据，如果不存在返回空DataFrame
        """
        sql = """
            SELECT 
                symbol, date, open, high, low, close,
                volume, dividends, stock_splits
            FROM stocks_day_hist_data 
            WHERE symbol = %s
        """
        params = [symbol]
        
        if start_date:
            sql += " AND date >= %s"
            params.append(start_date)
        if end_date:
            sql += " AND date <= %s"
            params.append(end_date)
            
        sql += " ORDER BY date"
        
        result = self.mysql_db.execute_query(sql, tuple(params))
        if not result:
            logger.warning(f"未找到股票{symbol}的历史数据")
            return pd.DataFrame()
        
        return pd.DataFrame(result)

    def insert_stock_hist_data(self, hist_data: Dict[str, Any]) -> bool:
        """
        插入股票历史数据
        
        Args:
            hist_data: 股票历史数据字典，包含以下字段：
                - symbol: 股票代码
                - date: 交易日期
                - open: 开盘价
                - high: 最高价
                - low: 最低价
                - close: 收盘价
                - volume: 成交量
                - dividends: 分红
                - stock_splits: 拆股
                
        Returns:
            bool: 插入是否成功
        """
        sql = """
            INSERT INTO stocks_day_hist_data (
                symbol, date, open, high, low, close,
                volume, dividends, stock_splits
            ) VALUES (
                %(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s,
                %(volume)s, %(dividends)s, %(stock_splits)s
            )
        """
        return self.mysql_db.execute_query(sql, hist_data) is not None

    def batch_insert_stock_hist_data(self, hist_data_list: List[Dict[str, Any]]) -> bool:
        """
        批量插入股票历史数据
        
        Args:
            hist_data_list: 股票历史数据字典列表
                
        Returns:
            bool: 插入是否全部成功
        """
        if not hist_data_list:
            logger.warning("没有数据需要插入")
            return False

        sql = """
            INSERT INTO stocks_day_hist_data (
                symbol, date, open, high, low, close,
                volume, dividends, stock_splits
            ) VALUES (
                %(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s,
                %(volume)s, %(dividends)s, %(stock_splits)s
            )
        """
        try:
            for hist_data in hist_data_list:
                self.mysql_db.execute_query(sql, hist_data)
            logger.debug(f"批量插入{len(hist_data_list)}条历史数据成功")
            return True
        except Exception as e:
            logger.error(f"批量插入股票历史数据失败: {str(e)}")
            raise

    def update_stock_hist_data(self, symbol: str, date_str: str, hist_data: Dict[str, Any]) -> bool:
        """
        更新股票历史数据
        
        Args:
            symbol: 股票代码
            date_str: 交易日期
            hist_data: 股票历史数据字典，包含要更新的字段
            
        Returns:
            bool: 更新是否成功
        """
        # 构建UPDATE语句
        set_clause = ", ".join([f"{k} = %s" for k in hist_data.keys()])
        sql = f"""
            UPDATE stocks_day_hist_data 
            SET {set_clause}
            WHERE symbol = %s AND date = %s
        """
        
        # 构建参数tuple
        params = tuple(hist_data.values()) + (symbol, date_str)
        return self.mysql_db.execute_query(sql, params) is not None

    def delete_stock_hist_data(self, symbol: str, start_date: date = None, end_date: date = None) -> bool:
        """
        删除股票历史数据
        
        Args:
            symbol: 股票代码
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            bool: 删除是否成功
        """
        sql = "DELETE FROM stocks_day_hist_data WHERE symbol = %s"
        params = [symbol]
        
        if start_date:
            sql += " AND date >= %s"
            params.append(start_date)
        if end_date:
            sql += " AND date <= %s"
            params.append(end_date)
            
        return self.mysql_db.execute_query(sql, tuple(params)) is not None

    def get_latest_hist_date(self, symbol: str) -> Optional[str]:
        """
        获取指定股票的最新历史数据日期
        
        Args:
            symbol: 股票代码
            
        Returns:
            Optional[str]: 最新历史数据日期 (YYYY-MM-DD)，如果没有数据返回None
        """
        sql = """
            SELECT MAX(date) as latest_date
            FROM stocks_day_hist_data
            WHERE symbol = %s
        """
        results = self.mysql_db.execute_query(sql, (symbol,))
        if not results or not results[0] or results[0]['latest_date'] is None:
            return None
        
        latest_date = results[0]['latest_date']
        # 处理不同的日期格式
        if hasattr(latest_date, 'strftime'):
            return latest_date.strftime('%Y-%m-%d')
        else:
            return str(latest_date)

    def get_all_stocks_latest_hist_date(self) -> Dict[str, Optional[str]]:
        """
        获取所有股票的最新历史数据日期
        
        Returns:
            Dict[str, Optional[str]]: 股票代码到最新历史数据日期的映射
        """
        sql = """
            SELECT symbol, MAX(date) as latest_date
            FROM stocks_day_hist_data
            GROUP BY symbol
        """
        results = self.mysql_db.execute_query(sql)
        if not results:
            return {}
        
        latest_dates = {}
        for result in results:
            symbol = result['symbol']
            latest_date = result['latest_date']
            
            if latest_date is None:
                latest_dates[symbol] = None
            elif hasattr(latest_date, 'strftime'):
                latest_dates[symbol] = latest_date.strftime('%Y-%m-%d')
            else:
                latest_dates[symbol] = str(latest_date)
        
        return latest_dates

    def get_stocks_hist_summary(self, symbols: List[str] = None) -> pd.DataFrame:
        """
        获取股票历史数据摘要（包含最新日期、数据条数等）
        
        Args:
            symbols: 可选，指定股票代码列表，如果为None则获取所有股票
            
        Returns:
            pd.DataFrame: 股票历史数据摘要DataFrame，包含symbol, latest_date, record_count
        """
        base_sql = """
            SELECT 
                symbol,
                MAX(date) as latest_date,
                COUNT(*) as record_count,
                MIN(date) as earliest_date
            FROM stocks_day_hist_data
        """
        
        if symbols:
            placeholders = ", ".join(["%s"] * len(symbols))
            sql = f"{base_sql} WHERE symbol IN ({placeholders}) GROUP BY symbol"
            params = symbols
        else:
            sql = f"{base_sql} GROUP BY symbol"
            params = []
        
        results = self.mysql_db.execute_query(sql, params)
        if not results:
            return pd.DataFrame(columns=['symbol', 'latest_date', 'record_count', 'earliest_date'])
        
        return pd.DataFrame(results)


if __name__ == "__main__":
    # 初始化数据库连接
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )

    # 创建DBStocksDayHist实例
    db_stocks_hist = DBStocksDayHist(mysql_db)

    # 测试插入股票历史数据
    test_hist_data = {
        'symbol': 'AAPL',
        'date': '2024-03-20',
        'open': 172.50,
        'high': 175.20,
        'low': 171.80,
        'close': 174.90,
        'volume': 1000000,
        'dividends': 0.0,
        'stock_splits': 0.0
    }
    print("插入股票历史数据结果:", db_stocks_hist.insert_stock_hist_data(test_hist_data))

    # 测试批量插入
    test_hist_data_list = [
        {
            'symbol': 'AAPL',
            'date': '2024-03-21',
            'open': 174.90,
            'high': 176.30,
            'low': 174.50,
            'close': 175.80,
            'volume': 1100000,
            'dividends': 0.0,
            'stock_splits': 0.0
        },
        {
            'symbol': 'AAPL',
            'date': '2024-03-22',
            'open': 175.80,
            'high': 177.10,
            'low': 175.20,
            'close': 176.90,
            'volume': 1200000,
            'dividends': 0.0,
            'stock_splits': 0.0
        }
    ]
    print("批量插入股票历史数据结果:", db_stocks_hist.batch_insert_stock_hist_data(test_hist_data_list))

    # 测试获取股票历史数据
    print("获取股票历史数据:")
    hist_data = db_stocks_hist.get_stock_hist_data('AAPL', 
                                                  start_date='2024-03-01',
                                                  end_date='2024-03-31')
    print(hist_data)

    # 测试更新股票历史数据
    update_data = {
        'close': 175.00,
        'volume': 1100000
    }
    print("更新股票历史数据结果:", 
          db_stocks_hist.update_stock_hist_data('AAPL', '2024-03-20', update_data))

    # 测试删除股票历史数据
    print("删除股票历史数据结果:", 
          db_stocks_hist.delete_stock_hist_data('AAPL', 
                                              start_date='2024-03-20',
                                              end_date='2024-03-22'))

    # 测试获取股票历史数据
    print("获取股票历史数据:")
    hist_data = db_stocks_hist.get_stock_hist_data('AAPL')
    print(hist_data)

    # 关闭数据库连接
    mysql_db.close_connection() 