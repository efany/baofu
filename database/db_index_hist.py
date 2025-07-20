import pandas as pd
from typing import Dict, Any, Optional, List
from loguru import logger
import os
import sys
from datetime import datetime, date

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase

class DBIndexHist:
    """指数历史数据库操作类"""

    def __init__(self, mysql_db: MySQLDatabase):
        """
        初始化
        
        Args:
            mysql_db: MySQL数据库实例
        """
        self.mysql_db = mysql_db

    def get_index_hist_data(self, symbol: str, start_date: date = None, end_date: date = None) -> pd.DataFrame:
        """
        获取指数历史数据
        
        Args:
            symbol: 指数代码
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            pd.DataFrame: 指数历史数据，如果不存在返回空DataFrame
        """
        sql = """
            SELECT 
                symbol, date, open, high, low, close,
                volume, turnover, nav, change_pct
            FROM index_hist_data 
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
            logger.warning(f"未找到指数{symbol}的历史数据")
            return pd.DataFrame()
        
        return pd.DataFrame(result)

    def insert_index_hist_data(self, hist_data: Dict[str, Any]) -> bool:
        """
        插入指数历史数据
        
        Args:
            hist_data: 指数历史数据字典，包含以下字段：
                - symbol: 指数代码
                - date: 交易日期
                - open: 开盘价
                - high: 最高价
                - low: 最低价
                - close: 收盘价
                - volume: 成交量
                - turnover: 成交额
                - nav: 净值（基金指数用）
                - change_pct: 涨跌幅（%）
                
        Returns:
            bool: 插入是否成功
        """
        sql = """
            INSERT INTO index_hist_data (
                symbol, date, open, high, low, close,
                volume, turnover, nav, change_pct
            ) VALUES (
                %(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s,
                %(volume)s, %(turnover)s, %(nav)s, %(change_pct)s
            )
        """
        return self.mysql_db.execute_query(sql, hist_data) is not None

    def batch_insert_index_hist_data(self, hist_data_list: List[Dict[str, Any]]) -> bool:
        """
        批量插入指数历史数据
        
        Args:
            hist_data_list: 指数历史数据字典列表
                
        Returns:
            bool: 插入是否全部成功
        """
        if not hist_data_list:
            logger.warning("没有数据需要插入")
            return False

        sql = """
            INSERT INTO index_hist_data (
                symbol, date, open, high, low, close,
                volume, turnover, nav, change_pct
            ) VALUES (
                %(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s,
                %(volume)s, %(turnover)s, %(nav)s, %(change_pct)s
            )
        """
        try:
            for hist_data in hist_data_list:
                self.mysql_db.execute_query(sql, hist_data)
            logger.debug(f"批量插入{len(hist_data_list)}条历史数据成功")
            return True
        except Exception as e:
            logger.error(f"批量插入指数历史数据失败: {str(e)}")
            raise

    def update_index_hist_data(self, symbol: str, date_str: str, hist_data: Dict[str, Any]) -> bool:
        """
        更新指数历史数据
        
        Args:
            symbol: 指数代码
            date_str: 交易日期
            hist_data: 指数历史数据字典，包含要更新的字段
            
        Returns:
            bool: 更新是否成功
        """
        # 构建UPDATE语句
        set_clause = ", ".join([f"{k} = %s" for k in hist_data.keys()])
        sql = f"""
            UPDATE index_hist_data 
            SET {set_clause}
            WHERE symbol = %s AND date = %s
        """
        
        # 构建参数tuple
        params = tuple(hist_data.values()) + (symbol, date_str)
        return self.mysql_db.execute_query(sql, params) is not None

    def delete_index_hist_data(self, symbol: str, start_date: date = None, end_date: date = None) -> bool:
        """
        删除指数历史数据
        
        Args:
            symbol: 指数代码
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            bool: 删除是否成功
        """
        sql = "DELETE FROM index_hist_data WHERE symbol = %s"
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
        获取指定指数的最新历史数据日期
        
        Args:
            symbol: 指数代码
            
        Returns:
            Optional[str]: 最新历史数据日期 (YYYY-MM-DD)，如果没有数据返回None
        """
        sql = """
            SELECT MAX(date) as latest_date
            FROM index_hist_data
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

    def get_all_indices_latest_hist_date(self) -> Dict[str, Optional[str]]:
        """
        获取所有指数的最新历史数据日期
        
        Returns:
            Dict[str, Optional[str]]: 指数代码到最新历史数据日期的映射
        """
        sql = """
            SELECT symbol, MAX(date) as latest_date
            FROM index_hist_data
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

    def get_indices_hist_summary(self, symbols: List[str] = None) -> pd.DataFrame:
        """
        获取指数历史数据摘要（包含最新日期、数据条数等）
        
        Args:
            symbols: 可选，指定指数代码列表，如果为None则获取所有指数
            
        Returns:
            pd.DataFrame: 指数历史数据摘要DataFrame，包含symbol, latest_date, record_count
        """
        base_sql = """
            SELECT 
                symbol,
                MAX(date) as latest_date,
                COUNT(*) as record_count,
                MIN(date) as earliest_date
            FROM index_hist_data
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

    def get_all_index_symbols(self) -> List[str]:
        """
        获取所有指数代码列表
        
        Returns:
            List[str]: 指数代码列表
        """
        sql = """
            SELECT DISTINCT symbol
            FROM index_hist_data
            ORDER BY symbol
        """
        results = self.mysql_db.execute_query(sql)
        if not results:
            logger.warning("未找到任何指数数据")
            return []
        
        return [result['symbol'] for result in results]

    def get_all_indices(self) -> pd.DataFrame:
        """
        获取所有指数基本信息
        
        Returns:
            pd.DataFrame: 指数信息DataFrame，包含symbol和name列
        """
        # 指数名称映射
        index_names = {
            'sh000001': '上证综指',
            'sh000002': '上证A股指数',  
            'sh000016': '上证50',
            'sh000300': '沪深300',
            'sh000905': '中证500',
            'sh000906': '中证800',
            'sz399001': '深证成指',
            'sz399005': '中小板指',
            'sz399006': '创业板指'
        }
        
        # 获取数据库中实际存在的指数代码
        symbols = self.get_all_index_symbols()
        
        indices_data = []
        for symbol in symbols:
            name = index_names.get(symbol, symbol)
            indices_data.append({
                'symbol': symbol,
                'name': name
            })
        
        return pd.DataFrame(indices_data)

if __name__ == "__main__":
    # 初始化数据库连接
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )

    # 创建DBIndexHist实例
    db_index_hist = DBIndexHist(mysql_db)

    # 测试插入指数历史数据
    test_hist_data = {
        'symbol': 'sh000001',
        'date': '2025-07-19',
        'open': 3200.50,
        'high': 3250.20,
        'low': 3180.80,
        'close': 3234.90,
        'volume': 123456789,
        'turnover': 987654321.12,
        'nav': None,
        'change_pct': 0.38
    }
    print("插入指数历史数据结果:", db_index_hist.insert_index_hist_data(test_hist_data))

    # 测试批量插入
    test_hist_data_list = [
        {
            'symbol': 'sh000001',
            'date': '2025-07-18',
            'open': 3220.30,
            'high': 3245.60,
            'low': 3200.10,
            'close': 3222.80,
            'volume': 110000000,
            'turnover': 980000000.00,
            'nav': None,
            'change_pct': -0.25
        },
        {
            'symbol': 'sz399001',
            'date': '2025-07-19',
            'open': 11500.20,
            'high': 11650.40,
            'low': 11480.80,
            'close': 11620.30,
            'volume': 98765432,
            'turnover': 876543210.50,
            'nav': None,
            'change_pct': 1.05
        }
    ]
    print("批量插入指数历史数据结果:", db_index_hist.batch_insert_index_hist_data(test_hist_data_list))

    # 测试获取指数历史数据
    print("获取指数历史数据:")
    hist_data = db_index_hist.get_index_hist_data('sh000001', 
                                                 start_date='2025-07-01',
                                                 end_date='2025-07-31')
    print(hist_data)

    # 测试获取最新日期
    print("获取最新历史数据日期:")
    latest_date = db_index_hist.get_latest_hist_date('sh000001')
    print(f"sh000001最新日期: {latest_date}")

    # 测试获取所有指数的最新日期
    print("获取所有指数的最新日期:")
    all_latest_dates = db_index_hist.get_all_indices_latest_hist_date()
    print(all_latest_dates)

    # 关闭数据库连接
    mysql_db.close_connection()