import mysql.connector
from mysql.connector import errorcode
from mysql.connector import pooling
from typing import Dict, Optional
import threading
import time
from loguru import logger

class MySQLDatabase:
    def __init__(self, host, user, password, database, pool_size=20, **kwargs):
        """
        初始化数据库连接池
        
        Args:
            host: 数据库主机地址
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称
            pool_size: 连接池大小，默认20
            **kwargs: 其他连接参数
        """
        pool_config = {
            'pool_name': "mypool",
            'pool_size': pool_size,
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'pool_reset_session': True,
            'autocommit': True,
            'connection_timeout': 30,
            'use_unicode': True,
            'charset': 'utf8mb4',
            **kwargs
        }
        
        self.pool = pooling.MySQLConnectionPool(**pool_config)
        self._pool_size = pool_size
        self._active_connections = 0
        self._lock = threading.Lock()
        self._closed = False
        
        logger.info(f"数据库连接池初始化成功，连接池大小: {pool_size}")

    def get_connection(self):
        """
        从连接池中获取一个数据库连接
        """
        if self._closed:
            logger.error("连接池已关闭，无法获取连接")
            return None
            
        try:
            with self._lock:
                self._active_connections += 1
            
            connection = self.pool.get_connection()
            return connection
        except mysql.connector.Error as err:
            with self._lock:
                self._active_connections -= 1
            logger.error(f"获取数据库连接失败: {err}")
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
            logger.error(f"执行SQL出错: {err}")
            return None
        finally:
            if connection:
                connection.close()
                with self._lock:
                    self._active_connections -= 1

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

    def get_pool_status(self) -> Dict[str, int]:
        """
        获取连接池状态信息
        
        Returns:
            Dict[str, int]: 包含连接池大小和活跃连接数的字典
        """
        with self._lock:
            return {
                'pool_size': self._pool_size,
                'active_connections': self._active_connections,
                'available_connections': self._pool_size - self._active_connections
            }
    
    def close_connection(self):
        """保持向后兼容的方法"""
        self.close_pool()

    def close_pool(self):
        """
        关闭连接池
        """
        if self._closed:
            logger.warning("连接池已经关闭")
            return
            
        try:
            with self._lock:
                self._closed = True
                
            # 等待所有活跃连接释放
            max_wait = 30  # 最大等待30秒
            wait_count = 0
            while self._active_connections > 0 and wait_count < max_wait:
                logger.info(f"等待 {self._active_connections} 个活跃连接释放...")
                time.sleep(1)
                wait_count += 1
            
            # 强制关闭连接池中的所有连接
            if hasattr(self.pool, '_cnx_queue'):
                while not self.pool._cnx_queue.empty():
                    try:
                        conn = self.pool._cnx_queue.get_nowait()
                        if conn.is_connected():
                            conn.close()
                    except:
                        pass
            
            logger.info("数据库连接池已成功关闭")
            
        except Exception as e:
            logger.error(f"关闭连接池时发生错误: {e}")