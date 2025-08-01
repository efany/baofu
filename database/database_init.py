from mysql_database import MySQLDatabase  # 导入 MySQLDatabase 类

if __name__ == "__main__":
    db = MySQLDatabase(
        host='113.44.90.2',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu',
        pool_size=5
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
    
    if db.check_table_exists('strategys'):
        print("数据表 'strategys' 已存在")      
    else:
        # strategy_id	int	Y	策略ID
        # name	str	Y	名称   
        # description	str	Y	描述
        # data_params	str	Y	数据参数
        # initial_cash	str	Y	初始资金
        # strategy	str	Y	策略
        # create_time	timestamp	Y	创建时间
        # update_time	timestamp	Y	更新时间
        table_schema = """
        strategy_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100),
        description VARCHAR(500),
        data_params VARCHAR(500),
        initial_cash FLOAT,
        strategy VARCHAR(2000),
        parameters VARCHAR(2000),
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        """
        db.create_table('strategys', table_schema)

        if db.check_table_exists('strategys'):
            print("数据表 'strategys' 校验。")
        else:
            print("数据表 'strategys' 创建失败。")

    if db.check_table_exists('stocks_info'):
        print("数据表 'stocks_info' 已存在")
    else:
        table_schema = """
        symbol VARCHAR(20),
        name VARCHAR(100),
        currency VARCHAR(10),
        exchange VARCHAR(10),
        market VARCHAR(10),
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        """
        db.create_table('stocks_info', table_schema)
        if db.check_table_exists('stocks_info'):
            print("数据表 'stocks_info' 校验。")
        else:
            print("数据表 'stocks_info' 创建失败。")

    if db.check_table_exists('stocks_day_hist_data'):
        print("数据表 'stocks_day_hist_data' 已存在")
    else:
        # Date   Open   High    Low  Close    Volume  Dividends  Stock Splits
        table_schema = """
        symbol VARCHAR(20),
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume FLOAT,
        dividends FLOAT,
        stock_splits FLOAT, 
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        """
        db.create_table('stocks_day_hist_data', table_schema)
        if db.check_table_exists('stocks_day_hist_data'):
            print("数据表 'stocks_day_hist_data' 校验。")
        else:
            print("数据表 'stocks_day_hist_data' 创建失败。")

    if db.check_table_exists('forex_day_hist_data'):
        print("数据表 'forex_day_hist_data' 已存在")
    else:
        table_schema = """
        symbol VARCHAR(20),
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        change_pct FLOAT, 
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        """
        db.create_table('forex_day_hist_data', table_schema)
        if db.check_table_exists('forex_day_hist_data'):
            print("数据表 'forex_day_hist_data' 校验。")
        else:
            print("数据表 'forex_day_hist_data' 创建失败。")

    if db.check_table_exists('bond_rate_history'):
        print("数据表 'bond_rate_history' 已存在")
    else:
        # bond_type  | str    | 债券类型（如：CN_10Y, US_10Y等）
        # date       | date   | 日期
        # rate       | float  | 利率值
        table_schema = """
        bond_type VARCHAR(20),
        date DATE,
        rate FLOAT,
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (bond_type, date)
        """
        db.create_table('bond_rate_history', table_schema)
        if db.check_table_exists('bond_rate_history'):
            print("数据表 'bond_rate_history' 校验。")
        else:
            print("数据表 'bond_rate_history' 创建失败。")

    if db.check_table_exists('index_hist_data'):
        print("数据表 'index_hist_data' 已存在")
    else:
        # symbol      | str     | 指数代码（如：sh000001, sz399001等）
        # date        | date    | 交易日期
        # open        | decimal | 开盘价
        # high        | decimal | 最高价
        # low         | decimal | 最低价
        # close       | decimal | 收盘价
        # volume      | bigint  | 成交量
        # turnover    | decimal | 成交额
        # nav         | decimal | 净值（基金指数用）
        # change_pct  | decimal | 涨跌幅（%）
        table_schema = """
        id INT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL COMMENT '指数代码',
        date DATE NOT NULL COMMENT '交易日期',
        open DECIMAL(10,4) DEFAULT NULL COMMENT '开盘价',
        high DECIMAL(10,4) DEFAULT NULL COMMENT '最高价',
        low DECIMAL(10,4) DEFAULT NULL COMMENT '最低价',
        close DECIMAL(10,4) DEFAULT NULL COMMENT '收盘价',
        volume BIGINT DEFAULT NULL COMMENT '成交量',
        turnover DECIMAL(15,2) DEFAULT NULL COMMENT '成交额',
        nav DECIMAL(10,4) DEFAULT NULL COMMENT '净值（基金指数用）',
        change_pct DECIMAL(8,4) DEFAULT NULL COMMENT '涨跌幅（%）',
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        UNIQUE KEY unique_symbol_date (symbol, date),
        INDEX idx_symbol (symbol),
        INDEX idx_date (date),
        INDEX idx_symbol_date (symbol, date)
        """
        db.create_table('index_hist_data', table_schema)
        if db.check_table_exists('index_hist_data'):
            print("数据表 'index_hist_data' 校验。")
        else:
            print("数据表 'index_hist_data' 创建失败。")

    db.close_connection()