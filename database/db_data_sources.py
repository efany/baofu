#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymysql
from database.db_pool import DBPool


class DBDataSources:
    """数据源管理数据库操作类"""
    
    def __init__(self):
        self.db_pool = DBPool()
    
    def get_all_data_sources(self):
        """获取所有数据源配置"""
        conn = self.db_pool.get_connection()
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sql = """
                SELECT id, name, source_type, url, status, priority, 
                       last_update, created_at, updated_at, description
                FROM data_sources 
                ORDER BY priority DESC, name ASC
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            self.db_pool.return_connection(conn)
    
    def get_data_source_by_id(self, source_id):
        """根据ID获取数据源配置"""
        conn = self.db_pool.get_connection()
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sql = """
                SELECT id, name, source_type, url, status, priority, 
                       last_update, created_at, updated_at, description
                FROM data_sources 
                WHERE id = %s
            """
            cursor.execute(sql, (source_id,))
            result = cursor.fetchone()
            return result
        finally:
            cursor.close()
            self.db_pool.return_connection(conn)
    
    def get_data_sources_by_type(self, source_type):
        """根据类型获取数据源配置"""
        conn = self.db_pool.get_connection()
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sql = """
                SELECT id, name, source_type, url, status, priority, 
                       last_update, created_at, updated_at, description
                FROM data_sources 
                WHERE source_type = %s AND status = 'active'
                ORDER BY priority DESC, name ASC
            """
            cursor.execute(sql, (source_type,))
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            self.db_pool.return_connection(conn)
    
    def insert_data_source(self, data_source):
        """插入新的数据源配置"""
        conn = self.db_pool.get_connection()
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO data_sources (name, source_type, url, status, priority, description)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                data_source['name'],
                data_source['source_type'],
                data_source['url'],
                data_source.get('status', 'active'),
                data_source.get('priority', 1),
                data_source.get('description', '')
            ))
            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            self.db_pool.return_connection(conn)
    
    def update_data_source(self, source_id, data_source):
        """更新数据源配置"""
        conn = self.db_pool.get_connection()
        try:
            cursor = conn.cursor()
            sql = """
                UPDATE data_sources 
                SET name = %s, source_type = %s, url = %s, status = %s, 
                    priority = %s, description = %s, updated_at = NOW()
                WHERE id = %s
            """
            cursor.execute(sql, (
                data_source['name'],
                data_source['source_type'],
                data_source['url'],
                data_source.get('status', 'active'),
                data_source.get('priority', 1),
                data_source.get('description', ''),
                source_id
            ))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            self.db_pool.return_connection(conn)
    
    def delete_data_source(self, source_id):
        """删除数据源配置"""
        conn = self.db_pool.get_connection()
        try:
            cursor = conn.cursor()
            sql = "DELETE FROM data_sources WHERE id = %s"
            cursor.execute(sql, (source_id,))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            self.db_pool.return_connection(conn)
    
    def update_last_update_time(self, source_id):
        """更新数据源最后更新时间"""
        conn = self.db_pool.get_connection()
        try:
            cursor = conn.cursor()
            sql = "UPDATE data_sources SET last_update = NOW() WHERE id = %s"
            cursor.execute(sql, (source_id,))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            self.db_pool.return_connection(conn)
    
    def get_data_source_stats(self):
        """获取数据源统计信息"""
        conn = self.db_pool.get_connection()
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sql = """
                SELECT 
                    COUNT(*) as total_sources,
                    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_sources,
                    SUM(CASE WHEN status = 'inactive' THEN 1 ELSE 0 END) as inactive_sources,
                    COUNT(DISTINCT source_type) as source_types_count
                FROM data_sources
            """
            cursor.execute(sql)
            result = cursor.fetchone()
            return result
        finally:
            cursor.close()
            self.db_pool.return_connection(conn)
    
    def get_data_source_type_stats(self):
        """获取按类型分组的数据源统计"""
        conn = self.db_pool.get_connection()
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sql = """
                SELECT 
                    source_type,
                    COUNT(*) as total_count,
                    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_count,
                    SUM(CASE WHEN status = 'inactive' THEN 1 ELSE 0 END) as inactive_count
                FROM data_sources
                GROUP BY source_type
                ORDER BY total_count DESC
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            self.db_pool.return_connection(conn)