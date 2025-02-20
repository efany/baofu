from mysql_database import MySQLDatabase  # 导入 MySQLDatabase 类

if __name__ == "__main__":
    db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )
    
    if db.check_table_exists('funds'):
        print("数据表 'funds' 已存在")
    else:
        # ts_code        | str    | 基金代码
        # name           | str    | 简称
        # management     | str    | 管理人
        # custodian      | str    | 托管人
        # fund_type      | str    | 投资类型
        # found_date     | str    | 成立日期
        # due_date       | str    | 到期日期
        # list_date      | str    | 上市时间
        # issue_date     | str    | 发行日期
        # delist_date    | str    | 退市日期
        # issue_amount   | float  | 发行份额(亿)
        # m_fee          | float  | 管理费
        # c_fee          | float  | 托管费
        # duration_year  | float  | 存续期
        # p_value        | float  | 面值
        # min_amount     | float  | 起点金额(万元)
        # exp_return     | float  | 预期收益率
        # benchmark      | str    | 业绩比较基准
        # status         | str    | 存续状态D摘牌 I发行 L已上市
        # invest_type    | str    | 投资风格
        # type           | str    | 基金类型
        # trustee        | str    | 受托人
        # purc_startdate | str    | 日常申购起始日
        # redm_startdate | str    | 日常赎回起始日
        # market         | str    | E场内O场外
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
            print("数据表 'funds' 校验。")
        else:
            print("数据表 'funds' 创建失败。")

    if db.check_table_exists('funds_nav'):
        print("数据表 'funds_nav' 已存在")
    else:
        # ts_code	str	Y	TS代码  
        # nav_date	str	Y	净值日期
        # unit_nav	float	Y	单位净值
        # accum_nav	float	Y	累计净值
        # dividend	float	Y	分红配送
        table_schema = """
        ts_code VARCHAR(20),
        nav_date DATE,
        unit_nav FLOAT,
        accum_nav FLOAT,
        dividend FLOAT
        """
        db.create_table('funds_nav', table_schema)
        
        if db.check_table_exists('funds_nav'):
            print("数据表 'funds_nav' 校验。")
        else:
            print("数据表 'funds_nav' 创建失败。")
    
    db.close_connection() 