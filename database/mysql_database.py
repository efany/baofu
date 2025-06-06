import mysql.connector
from mysql.connector import errorcode
from mysql.connector import pooling
from typing import Dict, Optional

class MySQLDatabase:
    def __init__(self, host, user, password, database, pool_size=5):
        """
        初始化数据库连接池
        
        Args:
            host: 数据库主机地址
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称
            pool_size: 连接池大小
        """
        self.pool = pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=pool_size,
            host=host,
            user=user,
            password=password,
            database=database
        )
        print("数据库连接池初始化成功")

    def get_connection(self):
        """
        从连接池中获取一个数据库连接
        """
        try:
            connection = self.pool.get_connection()
            return connection
        except mysql.connector.Error as err:
            print(f"获取数据库连接失败: {err}")
            return None

    def execute_query(self, sql, params=None):
        """
        执行SQL查询语句
        
        Args:
            sql: SQL查询语句
            params: 查询参数
            
        Returns:
            Optional[Dict]: 查询结果，如果是SELECT语句则返回结果字典，其他语句返回None
        """
        connection = self.get_connection()
        if not connection:
            return None

        try:
            cursor = connection.cursor(dictionary=True, buffered=True)
            cursor.execute(sql, params or ())
            
            if sql.strip().upper().startswith('SELECT') or sql.strip().upper().startswith('SHOW'):
                result = cursor.fetchall()
            else:
                connection.commit()
                result = None
            
            cursor.close()
            return result
        except mysql.connector.Error as err:
            print(f"执行SQL出错: {err}")
            return None
        finally:
            if connection:
                connection.close()

    def create_table(self, table_name, table_schema):
        """
        创建表
        
        Args:
            table_name: 表名
            table_schema: 表结构
        """
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})"
        self.execute_query(sql)

    def check_table_exists(self, table_name):
        """
        检查表是否存在
        
        Args:
            table_name: 要检查的表名
            
        Returns:
            bool: 表是否存在
        """
        sql = f"SHOW TABLES LIKE '{table_name}'"
        result = self.execute_query(sql)
        return bool(result)

    def close_connection(self):
        print("数据库连接池已关闭")

    def close_pool(self):
        """
        关闭连接池
        """
        print("数据库连接池已关闭")