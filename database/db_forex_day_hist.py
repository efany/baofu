import pandas as pd
from typing import Dict, Any, Optional, List
from loguru import logger
import os
import sys
from datetime import datetime, date

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase

class DBForexDayHist:
    """外汇日线历史数据库操作类"""

    def __init__(self, mysql_db: MySQLDatabase):
        """
        初始化
        
        Args:
            mysql_db: MySQL数据库实例
        """
        self.mysql_db = mysql_db

    def get_all_forex(self, extend: bool = False) -> List[str]:
        """
        获取所有外汇代码列表
        
        Returns:
            List[str]: 外汇代码列表
        """
        sql = """
            SELECT DISTINCT symbol
            FROM forex_day_hist_data
            ORDER BY symbol
        """
        results = self.mysql_db.execute_query(sql)
        if not results:
            logger.warning("未找到任何外汇数据")
            return pd.DataFrame()
        pd_results = pd.DataFrame(results)
        if extend:
            # 定义虚拟数据与真实数据的依赖关系
            virtual_data_dependencies = {
                'JPYUSD': ['USDJPY'],  # JPYUSD依赖于USDJPY
                'CNHUSD': ['USDCNH'],  # CNHUSD依赖于USDCNH
                'CHFUSD': ['USDCHF'],   # CHFUSD依赖于USDCHF
                'CNHJPY': ['USDCNH', 'USDJPY'],  # CNHJPY依赖于USDCNH和USDJPY
                'CNHCHF': ['USDCNH', 'USDCHF'],  # CNHCHF依赖于USDCNH和USDCHF
                'JPYCNH': ['USDJPY', 'USDCNH'],  # JPYCNH依赖于USDJPY和USDCNH
                'CHFCNH': ['USDCHF', 'USDCNH']  # CHFCNH依赖于USDCHF和USDCNH
            }
            
            # 遍历所有虚拟数据依赖关系
            for virtual_symbol, real_symbols in virtual_data_dependencies.items():
                # 检查所有依赖的真实数据是否存在
                if all(symbol in pd_results['symbol'].values for symbol in real_symbols):
                    # 创建虚拟数据，仅包含symbol字段
                    virtual_data = pd.DataFrame({'symbol': [virtual_symbol]})
                    pd_results = pd.concat([pd_results, virtual_data], ignore_index=True)
        return pd_results

    def get_last_forex_hist_date(self, symbol: str) -> Optional[date]:
        """
        获取指定外汇代码的最新数据日期
        
        Args:
            symbol: 外汇代码
            
        Returns:
            Optional[date]: 最新数据日期，如果不存在则返回None
        """
        sql = """
            SELECT MAX(date) as last_date
            FROM forex_day_hist_data 
            WHERE symbol = %s
        """
        result = self.mysql_db.execute_query(sql, (symbol,))
        if not result:
            logger.warning(f"未找到外汇{symbol}的历史数据")
            return None
            
        # 获取第一行第一列的值
        last_date = result[0]['last_date'] if result[0] else None
        if not last_date:
            logger.warning(f"未找到外汇{symbol}的历史数据")
            return None
        
        return last_date

    def get_forex_hist_data(self, symbol: str, start_date: date = None, end_date: date = None) -> pd.DataFrame:
        """
        获取外汇历史数据
        
        Args:
            symbol: 外汇代码
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            pd.DataFrame: 外汇历史数据，如果不存在返回空DataFrame
        """
        sql = """
            SELECT 
                symbol, date, open, high, low, close, change_pct
            FROM forex_day_hist_data 
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
            logger.warning(f"未找到外汇{symbol}的历史数据")
            return pd.DataFrame()
        
        return pd.DataFrame(result)
    
    def get_extend_forex_hist_data(self, symbol: str, start_date: date = None, end_date: date = None) -> pd.DataFrame:
        # 使用调整后的日期范围获取数据
        if symbol in ["USDCNH", "USDJPY", "USDCHF"]:
            return self.get_forex_hist_data(symbol, start_date, end_date)
        elif symbol in ["CNHUSD", "JPYUSD", "CHFUSD"]:
            if symbol == "CNHUSD":
                deps_forex_code = "USDCNH"
            elif symbol == "JPYUSD":
                deps_forex_code = "USDJPY"
            elif symbol == "CHFUSD":
                deps_forex_code = "USDCHF"
            forex_data = self.get_forex_hist_data(deps_forex_code, start_date, end_date)
            forex_data['close'] = 1 / forex_data['close']
            forex_data['open'] = 1 / forex_data['open']
            forex_data['high'] = 1 / forex_data['high']
            forex_data['low'] = 1 / forex_data['low']
            return forex_data
        elif symbol == "CNHJPY":
            usdcnh_data = self.get_forex_hist_data("USDCNH", start_date, end_date)
            usdjpy_data = self.get_forex_hist_data("USDJPY", start_date, end_date)
            
            # 合并两个数据集
            merged_data = pd.merge(usdcnh_data, usdjpy_data, on='date', suffixes=('_usdcnh', '_usdjpy'))
            # 过滤掉任一数据源中为NaN的行
            merged_data = merged_data.dropna(subset=['open_usdcnh', 'high_usdcnh', 'low_usdcnh', 'close_usdcnh', 
                                                    'open_usdjpy', 'high_usdjpy', 'low_usdjpy', 'close_usdjpy'])
            
            # 计算CNHJPY数据
            return pd.DataFrame({
                'date': merged_data['date'],
                'open': (1 / merged_data['open_usdcnh']) * merged_data['open_usdjpy'],
                'high': (1 / merged_data['high_usdcnh']) * merged_data['high_usdjpy'],
                'low': (1 / merged_data['low_usdcnh']) * merged_data['low_usdjpy'],
                'close': (1 / merged_data['close_usdcnh']) * merged_data['close_usdjpy']
            })
        elif symbol == "JPYCNH":
            usdjpy_data = self.get_forex_hist_data("USDJPY", start_date, end_date)
            usdcnh_data = self.get_forex_hist_data("USDCNH", start_date, end_date)
            merged_data = pd.merge(usdjpy_data, usdcnh_data, on='date', suffixes=('_usdjpy', '_usdcnh'))
            return pd.DataFrame({
                'date': merged_data['date'],
                'open': merged_data['open_usdcnh'] / merged_data['open_usdjpy'],
                'high': merged_data['high_usdcnh'] / merged_data['high_usdjpy'],
                'low': merged_data['low_usdcnh'] / merged_data['low_usdjpy'],
                'close': merged_data['close_usdcnh'] / merged_data['close_usdjpy']
            })
        elif symbol == "CNHCHF":
            usdcnh_data = self.get_forex_hist_data("USDCNH", start_date, end_date)
            usdchf_data = self.get_forex_hist_data("USDCHF", start_date, end_date)
            
            # 合并两个数据集
            merged_data = pd.merge(usdcnh_data, usdchf_data, on='date', suffixes=('_usdcnh', '_usdchf'))
            # 过滤掉任一数据源中为NaN的行
            merged_data = merged_data.dropna(subset=['open_usdcnh', 'high_usdcnh', 'low_usdcnh', 'close_usdcnh', 
                                                    'open_usdchf', 'high_usdchf', 'low_usdchf', 'close_usdchf'])
            
            # 计算CNHCHF数据
            return pd.DataFrame({
                'date': merged_data['date'],
                'open': (1 / merged_data['open_usdcnh']) * merged_data['open_usdchf'],
                'high': (1 / merged_data['high_usdcnh']) * merged_data['high_usdchf'],
                'low': (1 / merged_data['low_usdcnh']) * merged_data['low_usdchf'],
                'close': (1 / merged_data['close_usdcnh']) * merged_data['close_usdchf']
            })
        elif symbol == "CHFCNH":
            usdchf_data = self.get_forex_hist_data("USDCHF", start_date, end_date)
            usdcnh_data = self.get_forex_hist_data("USDCNH", start_date, end_date)
            merged_data = pd.merge(usdchf_data, usdcnh_data, on='date', suffixes=('_usdchf', '_usdcnh'))
            return pd.DataFrame({
                'date': merged_data['date'],
                'open': merged_data['open_usdcnh'] / merged_data['open_usdchf'],
                'high': merged_data['high_usdcnh'] / merged_data['high_usdchf'],
                'low': merged_data['low_usdcnh'] / merged_data['low_usdchf'],
                'close': merged_data['close_usdcnh'] / merged_data['close_usdchf']
            })
        else:
            return self.get_forex_hist_data(symbol, start_date, end_date)

    def insert_forex_hist_data(self, hist_data: Dict[str, Any]) -> bool:
        """
        插入外汇历史数据
        
        Args:
            hist_data: 外汇历史数据字典，包含以下字段：
                - symbol: 外汇代码
                - date: 交易日期
                - open: 开盘价
                - high: 最高价
                - low: 最低价
                - close: 收盘价
                - change_pct: 涨跌幅
                
        Returns:
            bool: 插入是否成功
        """
        sql = """
            INSERT INTO forex_day_hist_data (
                symbol, date, open, high, low, close, change_pct
            ) VALUES (
                %(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(change_pct)s
            )
        """
        return self.mysql_db.execute_query(sql, hist_data) is not None

    def batch_insert_forex_hist_data(self, hist_data_list: List[Dict[str, Any]]) -> bool:
        """
        批量插入外汇历史数据
        
        Args:
            hist_data_list: 外汇历史数据字典列表
                
        Returns:
            bool: 插入是否全部成功
        """
        if not hist_data_list:
            logger.warning("没有数据需要插入")
            return False

        sql = """
            INSERT INTO forex_day_hist_data (
                symbol, date, open, high, low, close, change_pct
            ) VALUES (
                %(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(change_pct)s
            )
        """
        try:
            for hist_data in hist_data_list:
                self.mysql_db.execute_query(sql, hist_data)
            logger.debug(f"批量插入{len(hist_data_list)}条历史数据成功")
            return True
        except Exception as e:
            logger.error(f"批量插入外汇历史数据失败: {str(e)}")
            raise

    def update_forex_hist_data(self, symbol: str, date_str: str, hist_data: Dict[str, Any]) -> bool:
        """
        更新外汇历史数据
        
        Args:
            symbol: 外汇代码
            date_str: 交易日期
            hist_data: 外汇历史数据字典，包含要更新的字段
            
        Returns:
            bool: 更新是否成功
        """
        # 构建UPDATE语句
        set_clause = ", ".join([f"{k} = %s" for k in hist_data.keys()])
        sql = f"""
            UPDATE forex_day_hist_data 
            SET {set_clause}
            WHERE symbol = %s AND date = %s
        """
        
        # 构建参数tuple
        params = tuple(hist_data.values()) + (symbol, date_str)
        return self.mysql_db.execute_query(sql, params) is not None

    def delete_forex_hist_data(self, symbol: str, start_date: date = None, end_date: date = None) -> bool:
        """
        删除外汇历史数据
        
        Args:
            symbol: 外汇代码
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            bool: 删除是否成功
        """
        sql = "DELETE FROM forex_day_hist_data WHERE symbol = %s"
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
        获取指定外汇的最新历史数据日期
        
        Args:
            symbol: 外汇代码
            
        Returns:
            Optional[str]: 最新历史数据日期 (YYYY-MM-DD)，如果没有数据返回None
        """
        sql = """
            SELECT MAX(date) as latest_date
            FROM forex_day_hist_data
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

    def get_all_forex_latest_hist_date(self) -> Dict[str, Optional[str]]:
        """
        获取所有外汇的最新历史数据日期
        
        Returns:
            Dict[str, Optional[str]]: 外汇代码到最新历史数据日期的映射
        """
        sql = """
            SELECT symbol, MAX(date) as latest_date
            FROM forex_day_hist_data
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

    def get_forex_hist_summary(self, symbols: List[str] = None) -> pd.DataFrame:
        """
        获取外汇历史数据摘要（包含最新日期、数据条数等）
        
        Args:
            symbols: 可选，指定外汇代码列表，如果为None则获取所有外汇
            
        Returns:
            pd.DataFrame: 外汇历史数据摘要DataFrame，包含symbol, latest_date, record_count
        """
        base_sql = """
            SELECT 
                symbol,
                MAX(date) as latest_date,
                COUNT(*) as record_count,
                MIN(date) as earliest_date
            FROM forex_day_hist_data
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

    # 创建DBForexDayHist实例
    db_forex_hist = DBForexDayHist(mysql_db)

    # 测试插入外汇历史数据
    test_hist_data = {
        'symbol': 'USDCNY',
        'date': '2024-03-20',
        'open': 7.2345,
        'high': 7.2456,
        'low': 7.2234,
        'close': 7.2389,
        'change_pct': 0.12
    }
    print("插入外汇历史数据结果:", db_forex_hist.insert_forex_hist_data(test_hist_data))

    # 测试批量插入
    test_hist_data_list = [
        {
            'symbol': 'USDCNY',
            'date': '2024-03-21',
            'open': 7.2389,
            'high': 7.2567,
            'low': 7.2345,
            'close': 7.2456,
            'change_pct': 0.09
        },
        {
            'symbol': 'USDCNY',
            'date': '2024-03-22',
            'open': 7.2456,
            'high': 7.2678,
            'low': 7.2456,
            'close': 7.2567,
            'change_pct': 0.15
        }
    ]
    print("批量插入外汇历史数据结果:", db_forex_hist.batch_insert_forex_hist_data(test_hist_data_list))

    # 测试获取外汇历史数据
    print("获取外汇历史数据:")
    hist_data = db_forex_hist.get_forex_hist_data('USDCNY', 
                                                 start_date='2024-03-01',
                                                 end_date='2024-03-31')
    print(hist_data)

    # 测试更新外汇历史数据
    update_data = {
        'close': 7.2400,
        'change_pct': 0.10
    }
    print("更新外汇历史数据结果:", 
          db_forex_hist.update_forex_hist_data('USDCNY', '2024-03-20', update_data))

    # 测试删除外汇历史数据
    print("删除外汇历史数据结果:", 
          db_forex_hist.delete_forex_hist_data('USDCNY', 
                                             start_date='2024-03-20',
                                             end_date='2024-03-22'))

    # 测试获取外汇历史数据
    print("获取外汇历史数据:")
    hist_data = db_forex_hist.get_forex_hist_data('USDCNY')
    print(hist_data)

    # 关闭数据库连接
    mysql_db.close_connection() 