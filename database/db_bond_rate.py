from typing import Dict, List, Optional, Any
import pandas as pd
from loguru import logger
import sys
import os
from datetime import datetime, date

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase

class DBBondRate:
    """债券利率历史数据库操作类"""
    
    def __init__(self, mysql_db: MySQLDatabase):
        """
        初始化
        
        Args:
            mysql_db: MySQL数据库实例
        """
        self.mysql_db = mysql_db
    
    def get_all_bond(self) -> pd.DataFrame:
        """
        获取所有债券品种
        
        Returns:
            pd.DataFrame: 所有债券品种
        """ 
        sql = """
            SELECT DISTINCT bond_type
            FROM bond_rate_history
        """
        result = self.mysql_db.execute_query(sql)
        return pd.DataFrame(result)

    def get_bond_rate(self, bond_type: str, start_date: date = None, end_date: date = None) -> pd.DataFrame:
        """
        获取债券利率历史数据
        
        Args:
            bond_type: 债券类型（如：CN_10Y, US_10Y等）
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            pd.DataFrame: 债券利率历史数据，如果不存在返回空DataFrame
        """
        sql = """
            SELECT 
                bond_type, date, rate
            FROM bond_rate_history 
            WHERE bond_type = %s
        """
        params = [bond_type]
        
        if start_date:
            sql += " AND date >= %s"
            params.append(start_date)
        if end_date:
            sql += " AND date <= %s"
            params.append(end_date)
            
        sql += " ORDER BY date"
        
        result = self.mysql_db.execute_query(sql, tuple(params))
        if not result:
            logger.warning(f"未找到债券{bond_type}的利率历史数据")
            return pd.DataFrame()
        
        return pd.DataFrame(result)

    def get_bond_rate_by_date(self, bond_type: str, date_str: str) -> pd.DataFrame:
        """
        获取指定日期的债券利率数据
        
        Args:
            bond_type: 债券类型
            date_str: 日期字符串 (YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: 债券利率数据DataFrame
        """
        sql = """
            SELECT bond_type, date, rate
            FROM bond_rate_history
            WHERE bond_type = %s AND date = %s
        """
        params = [bond_type, date_str]
        results = self.mysql_db.execute_query(sql, tuple(params))   
        if not results:
            return pd.DataFrame()
        return pd.DataFrame(results)

    def insert_bond_rate(self, bond_rate: Dict[str, Any]) -> bool:
        """
        插入债券利率数据
        
        Args:
            bond_rate: 债券利率数据字典，包含以下字段：
                - bond_type: 债券类型
                - date: 日期
                - rate: 利率值
                
        Returns:
            bool: 插入是否成功
        """
        required_fields = ['bond_type', 'date', 'rate']
        for field in required_fields:
            if field not in bond_rate:
                raise ValueError(f"bond_rate缺少{field}字段")

        sql = """
            INSERT INTO bond_rate_history (
                bond_type, date, rate
            ) VALUES (
                %(bond_type)s, %(date)s, %(rate)s
            )
        """
        return self.mysql_db.execute_query(sql, bond_rate) is not None

    def batch_insert_bond_rate(self, bond_rate_list: List[Dict[str, Any]]) -> bool:
        """
        批量插入债券利率数据
        
        Args:
            bond_rate_list: 债券利率数据字典列表
                
        Returns:
            bool: 插入是否全部成功
        """
        if not bond_rate_list:
            logger.warning("没有数据需要插入")
            return False

        sql = """
            INSERT INTO bond_rate_history (
                bond_type, date, rate
            ) VALUES (
                %(bond_type)s, %(date)s, %(rate)s
            )
        """
        try:
            for bond_rate in bond_rate_list:
                self.mysql_db.execute_query(sql, bond_rate)
            logger.debug(f"批量插入{len(bond_rate_list)}条债券利率数据成功")
            return True
        except Exception as e:
            logger.error(f"批量插入债券利率数据失败: {str(e)}")
            raise

    def update_bond_rate(self, bond_type: str, date_str: str, bond_rate: Dict[str, Any]) -> bool:
        """
        更新债券利率数据
        
        Args:
            bond_type: 债券类型
            date_str: 日期字符串
            bond_rate: 债券利率数据字典，包含要更新的字段
            
        Returns:
            bool: 更新是否成功
        """
        if not bond_rate:
            raise ValueError("bond_rate不能为空")

        set_clause = ", ".join([f"{k}=%s" for k in bond_rate.keys()])
        where_clause = "WHERE bond_type=%s AND date=%s"
    
        sql = f"UPDATE bond_rate_history SET {set_clause} {where_clause}"
        return self.mysql_db.execute_query(sql, list(bond_rate.values()) + [bond_type, date_str]) is not None

    def delete_bond_rate(self, bond_type: str = None, start_date: date = None, end_date: date = None) -> bool:
        """
        删除债券利率数据
        
        Args:
            bond_type: 债券类型，为None时删除所有类型
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            
        Returns:
            bool: 删除是否成功
        """
        sql = "DELETE FROM bond_rate_history"
        params = []
        conditions = []
        
        if bond_type is not None:
            conditions.append("bond_type = %s")
            params.append(bond_type)
            
        if start_date:
            conditions.append("date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("date <= %s")
            params.append(end_date)
            
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
            
        return self.mysql_db.execute_query(sql, tuple(params)) is not None

    def get_latest_date(self, bond_type: str = None, date: Optional[date] = None) -> Optional[date]:
        """
        获取债券利率数据的最新日期
        
        Args:
            bond_type: 债券类型，为None时查询所有类型的最新日期
            date: 可选，如果提供则查询在该日期前的最新有效数据日期
            
        Returns:
            Optional[date]: 最新日期，如果没有数据返回None
        """
        sql = "SELECT MAX(date) as latest_date FROM bond_rate_history"
        params = []
        conditions = []
        
        if bond_type is not None:
            conditions.append("bond_type = %s")
            params.append(bond_type)
            
        if date is not None:
            conditions.append("date < %s")
            params.append(date)
            
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
            
        logger.info(f"查询SQL: {sql}, 参数: {tuple(params)}")
            
        result = self.mysql_db.execute_query(sql, tuple(params))
        logger.info(f"查询结果: {result[0]}")
        if not result or not result[0]:
            logger.warning(f"未找到{'指定类型' if bond_type else '任何'}债券利率数据")
            return None
        
        return result[0]['latest_date']

if __name__ == "__main__":
    # 初始化数据库连接
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )

    # 创建DBBondRate实例
    db_bond_rate = DBBondRate(mysql_db)

    # 测试插入债券利率数据
    test_bond_rate = {
        'bond_type': 'CN_10Y',
        'date': '2024-01-01',
        'rate': 2.5
    }
    print("插入债券利率数据结果:", db_bond_rate.insert_bond_rate(test_bond_rate))

    # 测试获取债券利率数据
    print("获取债券利率数据:")
    print(db_bond_rate.get_bond_rate('CN_10Y'))

    # 测试更新债券利率数据
    print("更新债券利率数据结果:", db_bond_rate.update_bond_rate('CN_10Y', '2024-01-01', {'rate': 2.6}))

    # 测试删除所有类型的债券利率数据
    print("删除所有类型的债券利率数据结果:", db_bond_rate.delete_bond_rate(
        start_date='2024-01-01',
        end_date='2024-01-31'
    ))

    # 测试删除特定类型的债券利率数据
    print("删除特定类型的债券利率数据结果:", db_bond_rate.delete_bond_rate(
        bond_type='CN_10Y',
        start_date='2024-01-01',
        end_date='2024-01-31'
    ))

    # 测试获取所有类型的最新日期
    latest_date = db_bond_rate.get_latest_date()
    print(f"所有债券类型的最新数据日期: {latest_date}")

    # 测试获取特定类型的最新日期
    latest_date = db_bond_rate.get_latest_date('CN_10Y')
    print(f"CN_10Y债券的最新数据日期: {latest_date}")

    # 关闭数据库连接
    mysql_db.close_connection() 