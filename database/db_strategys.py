from typing import Dict, List, Optional

import sys
import os
import pandas as pd
from loguru import logger
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase


class DBStrategys:
    def __init__(self, db: MySQLDatabase):
        """
        初始化策略数据表操作类
        
        Args:
            db: MySQLDatabase实例
        """
        self.db = db

    def add_strategy(self, strategy_data: Dict) -> bool:
        """
        添加新策略
        
        Args:
            strategy_data: 策略数据字典，包含：
                - name: 策略名称
                - description: 策略描述
                - data_params: 数据参数
                - initial_cash: 初始资金
                - strategy: 策略配置
                - parameters: 策略参数
                
        Returns:
            bool: 是否添加成功
        """
        try:
            sql = """
                INSERT INTO strategys 
                (name, description, data_params, initial_cash, strategy, parameters)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            params = (
                strategy_data.get('name'),
                strategy_data.get('description'),
                strategy_data.get('data_params'),
                strategy_data.get('initial_cash'),
                strategy_data.get('strategy'),
                strategy_data.get('parameters')
            )
            self.db.execute_query(sql, params)
            return True
        except Exception as e:
            print(f"Error adding strategy: {e}")
            return False

    def get_strategy(self, strategy_id: int) -> Optional[pd.DataFrame]:
        """
        获取指定策略的详细信息
        
        Args:
            strategy_id: 策略ID
            
        Returns:
            pd.DataFrame: 策略信息DataFrame，如果不存在返回空DataFrame
        """
        sql = "SELECT * FROM strategys WHERE strategy_id = %s"
        result = self.db.execute_query(sql, (strategy_id,))
        
        if result and len(result) > 0:
            df = pd.DataFrame([result[0]])
            return df
        return pd.DataFrame()

    def get_all_strategies(self) -> pd.DataFrame:
        """
        获取所有策略的列表
        
        Returns:
            pd.DataFrame: 策略信息DataFrame
        """
        sql = "SELECT * FROM strategys ORDER BY create_time DESC"
        result = self.db.execute_query(sql)
        
        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()

    def update_strategy(self, strategy_id: int, strategy_data: Dict) -> bool:
        logger.info(f"Updating strategy with ID: {strategy_id}")
        logger.info(f"Strategy data: {strategy_data}")
        logger.info(f"Strategy: {strategy_data.get('strategy')}")
        logger.info(f"Parameters: {strategy_data.get('parameters')}")
        """
        更新策略信息
        
        Args:
            strategy_id: 策略ID
            strategy_data: 要更新的策略数据
            
        Returns:
            bool: 是否更新成功
        """
        try:
            sql = """
                UPDATE strategys 
                SET name = %s,
                    description = %s,
                    data_params = %s,
                    initial_cash = %s,
                    strategy = %s,
                    parameters = %s
                WHERE strategy_id = %s
            """
            params = (
                strategy_data.get('name'),
                strategy_data.get('description'),
                strategy_data.get('data_params'),
                strategy_data.get('initial_cash'),
                strategy_data.get('strategy'),
                strategy_data.get('parameters'),
                strategy_id
            )
            self.db.execute_query(sql, params)
            return True
        except Exception as e:
            print(f"Error updating strategy: {e}")
            return False

    def delete_strategy(self, strategy_id: int) -> bool:
        """
        删除策略
        
        Args:
            strategy_id: 策略ID
            
        Returns:
            bool: 是否删除成功
        """
        try:
            sql = "DELETE FROM strategys WHERE strategy_id = %s"
            self.db.execute_query(sql, (strategy_id,))
            return True
        except Exception as e:
            print(f"Error deleting strategy: {e}")
            return False

    def search_strategies(self, keyword: str) -> pd.DataFrame:
        """
        搜索策略
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            pd.DataFrame: 匹配的策略DataFrame
        """
        sql = """
            SELECT * FROM strategys 
            WHERE name LIKE %s OR description LIKE %s 
            ORDER BY create_time DESC
        """
        search_term = f"%{keyword}%"
        result = self.db.execute_query(sql, (search_term, search_term))
        
        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()

if __name__ == "__main__":
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )

    db_strategys = DBStrategys(mysql_db)

    # 测试添加策略
    test_strategy = {
        'name': 'Test Strategy',
        'description': 'This is a test strategy',
        'data_params': """{'start_date': '2023-01-01', 'end_date': '2023-12-31'}""",
        'initial_cash': 1000000,
        'strategy': """{'type': 'buy_and_hold', 'assets': ['AAPL', 'MSFT']}"""
    }
    add_result = db_strategys.add_strategy(test_strategy)
    print(f"Add strategy result: {add_result}")

    # 测试获取策略
    strategies = db_strategys.get_all_strategies()
    print(f"All strategies: {strategies}")

    # 测试更新策略
    if not strategies.empty:
        strategy_id = int(strategies.iloc[0]['strategy_id'])
        update_data = {
            'name': 'Updated Test Strategy',
            'description': 'This is an updated test strategy',
            'data_params': """{'start_date': '2023-01-01', 'end_date': '2023-12-31'}""",
            'initial_cash': 1000000,
            'strategy': """{'type': 'buy_and_hold', 'assets': ['AAPL', 'MSFT']}"""
        }
        update_result = db_strategys.update_strategy(strategy_id, update_data)
        print(f"Update strategy result: {update_result}")

    # 测试删除策略
    if not strategies.empty:
        delete_result = db_strategys.delete_strategy(strategy_id)
        print(f"Delete strategy result: {delete_result}")

    # 测试搜索策略
    search_result = db_strategys.search_strategies('test')
    print(f"Search result: {search_result}")

    mysql_db.close_connection()
