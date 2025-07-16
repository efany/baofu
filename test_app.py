#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试应用启动脚本
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

# 模拟数据库类（用于测试）
class MockDatabase:
    def __init__(self):
        pass
    
    def close_pool(self):
        pass

# 模拟数据库操作类
class MockDBFunds:
    def __init__(self, mysql_db=None):
        self.mysql_db = mysql_db
    
    def get_all_funds(self):
        # 返回示例数据
        import pandas as pd
        data = {
            'ts_code': ['000001.OF', '000002.OF', '000003.OF'],
            'name': ['华夏成长', '华夏大盘', '中欧价值'],
            'management': ['华夏基金', '华夏基金', '中欧基金'],
            'fund_type': ['股票型', '混合型', '股票型']
        }
        return pd.DataFrame(data)

class MockDBStocks:
    def __init__(self, mysql_db=None):
        self.mysql_db = mysql_db
    
    def get_all_stocks(self):
        import pandas as pd
        data = {
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
            'name': ['平安银行', '万科A', '浦发银行'],
            'industry': ['银行', '房地产', '银行']
        }
        return pd.DataFrame(data)

class MockDBForex:
    def __init__(self, mysql_db=None):
        self.mysql_db = mysql_db
    
    def get_all_forex(self):
        import pandas as pd
        data = {
            'symbol': ['USDCNY', 'EURCNY', 'JPYCNY'],
            'name': ['美元人民币', '欧元人民币', '日元人民币']
        }
        return pd.DataFrame(data)

class MockDBForexDayHist:
    def __init__(self, mysql_db=None):
        self.mysql_db = mysql_db
    
    def get_all_forex(self):
        import pandas as pd
        data = {
            'symbol': ['USDCNY', 'EURCNY', 'JPYCNY'],
            'name': ['美元人民币', '欧元人民币', '日元人民币']
        }
        return pd.DataFrame(data)

# 替换数据库操作类
sys.modules['database.mysql_database'] = type('MockModule', (), {
    'MySQLDatabase': MockDatabase
})()

sys.modules['database.db_funds'] = type('MockModule', (), {
    'DBFunds': MockDBFunds
})()

sys.modules['database.db_stocks'] = type('MockModule', (), {
    'DBStocks': MockDBStocks
})()

sys.modules['database.db_forex'] = type('MockModule', (), {
    'DBForex': MockDBForex
})()

sys.modules['database.db_forex_day_hist'] = type('MockModule', (), {
    'DBForexDayHist': MockDBForexDayHist
})()

# 模拟任务类
class MockTask:
    def __init__(self, config):
        self.config = config
        self.is_success = True
        self.error = None
    
    def execute(self):
        pass

sys.modules['task_data.update_funds_task'] = type('MockModule', (), {
    'UpdateFundsTask': MockTask
})()

sys.modules['task_data.update_stocks_task'] = type('MockModule', (), {
    'UpdateStocksTask': MockTask
})()

sys.modules['task_data.update_forex_task'] = type('MockModule', (), {
    'UpdateForexTask': MockTask
})()

# 模拟工具函数
def mock_get_data_briefs(data_type, df):
    """模拟的数据摘要函数"""
    if df is None or df.empty:
        return []
    
    briefs = []
    for _, row in df.iterrows():
        if data_type == 'fund':
            label = f"{row['name']} ({row['ts_code']})"
            value = row['ts_code']
        elif data_type == 'stock':
            label = f"{row['name']} ({row['ts_code']})"
            value = row['ts_code']
        elif data_type == 'forex':
            label = f"{row['name']} ({row['symbol']})"
            value = row['symbol']
        else:
            label = str(row.iloc[0])
            value = str(row.iloc[0])
        
        briefs.append({'label': label, 'value': value})
    
    return briefs

sys.modules['task_dash.utils'] = type('MockModule', (), {
    'get_data_briefs': mock_get_data_briefs
})()

if __name__ == '__main__':
    # 现在导入并启动应用
    from task_dash.app import app
    
    print("启动测试应用...")
    print("请访问: http://localhost:8050")
    print("按 Ctrl+C 停止应用")
    
    app.run_server(debug=True, host='0.0.0.0', port=8050)