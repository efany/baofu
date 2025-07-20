from typing import Dict, List, Optional
import pandas as pd
import sys
import os
from typing import Dict, Any, List
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase  # 导入 MySQLDatabase 类

# 尝试导入缓存管理器，如果失败则定义一个简单的装饰器
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'task_dash'))
    from common.cache_manager import cached
except ImportError:
    # 如果无法导入缓存管理器，定义一个简单的装饰器
    def cached(key, ttl=None):
        def decorator(func):
            return func
        return decorator

class DBFunds:
    """提供对funds表的便捷操作"""
    
    def __init__(self, mysql_db: MySQLDatabase):
        """
        初始化
        
        Args:
            mysql_db: MySQLDatabase实例
        """
        self.mysql_db = mysql_db

    def get_fund_info(self, fund_code: str) -> Optional[pd.DataFrame]:
        """
        获取基金基本信息
        
        Args:
            fund_code: 基金代码
            
        Returns:
            Optional[pd.DataFrame]: 基金信息DataFrame，如果未找到则返回None
        """
        sql = """
            SELECT ts_code, name, management
            FROM funds
            WHERE ts_code = %s
        """
        result = self.mysql_db.execute_query(sql, (fund_code,))
        if not result:
            logger.warning(f"未找到基金{fund_code}的基本信息")
            return None
        
        # 使用列名创建DataFrame
        return pd.DataFrame([result[0]])

    def get_funds_info(self, fund_codes: List[str]) -> pd.DataFrame:
        """
        批量获取基金基本信息
        
        Args:
            fund_codes: 基金代码列表
            
        Returns:
            pd.DataFrame: 基金信息DataFrame
        """
        if not fund_codes:
            return pd.DataFrame()
            
        sql = """
            SELECT ts_code, name, management
            FROM funds
            WHERE ts_code IN (%s)
        """ % ','.join(['%s'] * len(fund_codes))

        results = self.mysql_db.execute_query(sql, tuple(fund_codes))
        if not results:
            logger.warning(f"未找到基金{fund_codes}的基本信息")
            return pd.DataFrame()
        return pd.DataFrame(results)

    def insert_fund_info(self, fund_info: Dict[str, Any]) -> bool:
        placeholders = ", ".join(["%s"] * len(fund_info))
        columns = ", ".join(fund_info.keys())
        sql = f"INSERT INTO funds ({columns}) VALUES ({placeholders})"
        return self.mysql_db.execute_query(sql, list(fund_info.values())) is not None

    def update_fund_info(self, fund_code: str, fund_info: Dict[str, Any]) -> bool:
        """
        更新基金信息
        
        Args:
            fund_code: 基金代码
            fund_info: 要更新的字段字典
            
        Returns:
            bool: 是否更新成功
        """
        if not fund_info:
            raise ValueError("fund_info不能为空")
    
        set_clause = ", ".join([f"{k}=%s" for k in fund_info.keys()])
        where_clause = "WHERE ts_code=%s"
        sql = f"UPDATE funds SET {set_clause} {where_clause}"
        print(sql)
        return self.mysql_db.execute_query(sql, list(fund_info.values()) + [fund_code]) is not None

    @cached("all_funds", ttl=600)  # 缓存10分钟
    def get_all_funds(self) -> pd.DataFrame:
        """
        获取所有基金信息
        
        Returns:
            pd.DataFrame: 基金信息DataFrame
        """
        sql = "SELECT ts_code, name, management FROM funds"
        results = self.mysql_db.execute_query(sql)
        if not results:
            logger.warning("未找到任何基金信息")
            return pd.DataFrame()
        return pd.DataFrame(results) 

if __name__ == "__main__":
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )

    db_funds = DBFunds(mysql_db)

    # 获取单个基金信息
    fund_info = db_funds.get_fund_info('003376')
    print(fund_info)

    # 批量获取基金信息
    funds_info = db_funds.get_funds_info(['003376', '007540'])
    print(funds_info)

    # 获取所有基金信息
    all_funds = db_funds.get_all_funds()
    print(all_funds)

    # 更新基金信息
    db_funds.update_fund_info('003376', {'name': 'test'})
    print(db_funds.get_fund_info('003376'))

    mysql_db.close_connection()