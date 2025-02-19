from mysql_database import MySQLDatabase  # 导入 MySQLDatabase 类

if __name__ == "__main__":
    db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )
    
    table_schema = """
    ts_code VARCHAR(20),
    name VARCHAR(100),
    management VARCHAR(100),
    custodian VARCHAR(100),
    fund_type VARCHAR(50),
    found_date DATE,
    due_date DATE,
    list_date DATE,
    issue_date DATE,
    delist_date DATE,
    issue_amount FLOAT,
    m_fee FLOAT,
    c_fee FLOAT,
    duration_year FLOAT,
    p_value FLOAT,
    min_amount FLOAT,
    exp_return FLOAT,
    benchmark VARCHAR(100),
    status VARCHAR(10),
    invest_type VARCHAR(50),
    type VARCHAR(50),
    trustee VARCHAR(100),
    purc_startdate DATE,
    redm_startdate DATE,
    market VARCHAR(10)
    """
    
    db.create_table('funds', table_schema)
    
    if db.check_table_exists('funds'):
        print("数据表 'funds' 已存在并已校验。")
    else:
        print("数据表 'funds' 创建失败。")
    
    db.close_connection() 