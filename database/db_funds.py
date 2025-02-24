from typing import Dict, List, Optional
import pandas as pd
from database.mysql_database import MySQLDatabase  # 导入 MySQLDatabase 类
from loguru import logger

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
        result = self.mysql_db.run_sql(sql, (fund_code,))
        if not result:
            logger.warning(f"未找到基金{fund_code}的基本信息")
            return None
        return pd.DataFrame([result])

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

        results = self.mysql_db.run_sql(sql, tuple(fund_codes))
        if not results:
            logger.warning(f"未找到基金{fund_codes}的基本信息")
            return pd.DataFrame()
        return pd.DataFrame(results)

    # def insert_fund_info(self, fund_info: Dict[str, Any]) -> bool:
    #     """
    #     插入基金信息
        
    #     Args:
    #         fund_info: 基金信息字典，包含ts_code, name, management字段
            
    #     Returns:
    #         bool: 是否插入成功
    #     """
    #     required_fields = ['ts_code', 'name', 'management']
    #     for field in required_fields:
    #         if field not in fund_info:
    #             raise ValueError(f"fund_info缺少{field}字段")
                
    #     return self.mysql_db.insert_data('funds', fund_info)

    # def update_fund_info(self, fund_code: str, fund_info: Dict[str, Any]) -> bool:
    #     """
    #     更新基金信息
        
    #     Args:
    #         fund_code: 基金代码
    #         fund_info: 要更新的字段字典
            
    #     Returns:
    #         bool: 是否更新成功
    #     """
    #     if not fund_info:
    #         raise ValueError("fund_info不能为空")
            
    #     return self.mysql_db.update_data('funds', fund_info, {'ts_code': fund_code})

    # def delete_fund_info(self, fund_code: str) -> bool:
    #     """
    #     删除基金信息
        
    #     Args:
    #         fund_code: 基金代码
            
    #     Returns:
    #         bool: 是否删除成功
    #     """
    #     sql = "DELETE FROM funds WHERE ts_code = %s"
    #     return self.mysql_db.run_sql(sql, (fund_code,)) is not None

    def get_all_funds(self) -> pd.DataFrame:
        """
        获取所有基金信息
        
        Returns:
            pd.DataFrame: 基金信息DataFrame
        """
        sql = "SELECT ts_code, name, management FROM funds"
        results = self.mysql_db.run_sql(sql)
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

    mysql_db.close_connection()