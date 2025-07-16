#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymysql
from database.db_pool import DBPool


def create_data_sources_table():
    """创建数据源配置表"""
    
    # 创建数据源配置表的SQL
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS data_sources (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL COMMENT '数据源名称',
        source_type VARCHAR(50) NOT NULL COMMENT '数据源类型(fund/stock/forex/bond/other)',
        url VARCHAR(500) NOT NULL COMMENT '数据源URL',
        status VARCHAR(20) DEFAULT 'active' COMMENT '状态(active/inactive)',
        priority INT DEFAULT 1 COMMENT '优先级(1-10)',
        last_update DATETIME NULL COMMENT '最后更新时间',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        description TEXT COMMENT '描述信息',
        INDEX idx_source_type (source_type),
        INDEX idx_status (status),
        INDEX idx_priority (priority)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据源配置表'
    """
    
    db_pool = DBPool()
    conn = db_pool.get_connection()
    
    try:
        cursor = conn.cursor()
        
        # 创建表
        cursor.execute(create_table_sql)
        print("数据源配置表创建成功")
        
        # 插入初始数据
        insert_initial_data(cursor)
        
        conn.commit()
        print("初始数据插入成功")
        
    except Exception as e:
        print(f"创建数据源配置表时发生错误: {str(e)}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        db_pool.return_connection(conn)


def insert_initial_data(cursor):
    """插入初始数据源配置"""
    
    initial_data = [
        {
            'name': '东方财富基金信息',
            'source_type': 'fund',
            'url': 'http://fund.eastmoney.com',
            'status': 'active',
            'priority': 9,
            'description': '东方财富网基金基本信息数据源'
        },
        {
            'name': '东方财富基金净值',
            'source_type': 'fund',
            'url': 'http://fund.eastmoney.com',
            'status': 'active',
            'priority': 9,
            'description': '东方财富网基金净值数据源'
        },
        {
            'name': '东方财富股票数据',
            'source_type': 'stock',
            'url': 'http://quote.eastmoney.com',
            'status': 'active',
            'priority': 8,
            'description': '东方财富网股票数据源'
        },
        {
            'name': '招商银行外汇数据',
            'source_type': 'forex',
            'url': 'https://www.cmbchina.com',
            'status': 'active',
            'priority': 7,
            'description': '招商银行外汇牌价数据源'
        },
        {
            'name': '中国债券信息网',
            'source_type': 'bond',
            'url': 'https://www.chinabond.com.cn',
            'status': 'active',
            'priority': 6,
            'description': '中国债券信息网债券数据源'
        },
        {
            'name': '新浪财经基金',
            'source_type': 'fund',
            'url': 'https://finance.sina.com.cn',
            'status': 'inactive',
            'priority': 5,
            'description': '新浪财经基金数据源（备用）'
        },
        {
            'name': '腾讯财经股票',
            'source_type': 'stock',
            'url': 'https://stock.qq.com',
            'status': 'inactive',
            'priority': 4,
            'description': '腾讯财经股票数据源（备用）'
        }
    ]
    
    insert_sql = """
        INSERT INTO data_sources (name, source_type, url, status, priority, description)
        VALUES (%(name)s, %(source_type)s, %(url)s, %(status)s, %(priority)s, %(description)s)
    """
    
    for data in initial_data:
        cursor.execute(insert_sql, data)
    
    print(f"插入了 {len(initial_data)} 条初始数据源配置")


if __name__ == "__main__":
    create_data_sources_table()