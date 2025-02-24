from typing import Dict, List, Optional, Any
import pandas as pd
from database.mysql_database import MySQLDatabase
from loguru import logger

class DBFundsNav:
    """提供对funds_nav表的便捷操作"""
    
    def __init__(self, mysql_db: MySQLDatabase):
        """
        初始化
        
        Args:
            mysql_db: MySQLDatabase实例
        """
        self.mysql_db = mysql_db

    def get_fund_nav(self, fund_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取基金净值数据
        
        Args:
            fund_code: 基金代码
            start_date: 起始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: 基金净值数据DataFrame
        """
        sql = """
            SELECT ts_code, nav_date, unit_nav, accum_nav, dividend
            FROM funds_nav
            WHERE ts_code = %s
        """
        params = [fund_code]
        
        # 添加日期条件
        if start_date:
            sql += " AND nav_date >= %s"
            params.append(start_date)
        if end_date:
            sql += " AND nav_date <= %s"
            params.append(end_date)
            
        sql += " ORDER BY nav_date ASC"
        
        results = self.mysql_db.run_sql(sql, tuple(params))
        if not results:
            logger.warning(f"未找到基金{fund_code}的净值数据")
            return pd.DataFrame()
        return pd.DataFrame(results)

    def insert_fund_nav(self, fund_nav: Dict[str, Any]) -> bool:
        """
        插入基金净值数据
        
        Args:
            fund_nav: 基金净值数据字典，包含ts_code, nav_date, unit_nav, accum_nav字段
            
        Returns:
            bool: 是否插入成功
        """
        required_fields = ['ts_code', 'nav_date', 'unit_nav', 'accum_nav', 'dividend']
        for field in required_fields:
            if field not in fund_nav:
                raise ValueError(f"fund_nav缺少{field}字段")
                
        return self.mysql_db.insert_data('funds_nav', fund_nav)

    def update_fund_nav(self, fund_code: str, nav_date: str, fund_nav: Dict[str, Any]) -> bool:
        """
        更新基金净值数据
        
        Args:
            fund_code: 基金代码
            nav_date: 净值日期
            fund_nav: 要更新的字段字典
            
        Returns:
            bool: 是否更新成功
        """
        if not fund_nav:
            raise ValueError("fund_nav不能为空")
            
        return self.mysql_db.update_data('funds_nav', fund_nav, {'ts_code': fund_code, 'nav_date': nav_date})

    def delete_fund_nav(self, fund_code: str, nav_date: str = None) -> bool:
        """
        删除基金净值数据
        
        Args:
            fund_code: 基金代码
            nav_date: 净值日期 (如果为None则删除该基金所有净值数据)
            
        Returns:
            bool: 是否删除成功
        """
        sql = "DELETE FROM funds_nav WHERE ts_code = %s"
        params = [fund_code]
        
        if nav_date:
            sql += " AND nav_date = %s"
            params.append(nav_date)
            
        return self.mysql_db.run_sql(sql, tuple(params)) is not None

if __name__ == "__main__":
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )

    db_funds_nav = DBFundsNav(mysql_db)

    # 获取单个基金净值数据
    fund_nav = db_funds_nav.get_fund_nav('003376', start_date='2023-01-01')
    print(fund_nav)

    mysql_db.close_connection() 