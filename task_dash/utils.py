from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict

# 股票代码到中文简称的映射
STOCK_NAME_MAP: Dict[str, str] = {
    '159949.SZ': '创业板50ETF',
    '512550.SS': 'A50ETF',
    '159633.SZ': '中证1000ETF',
    '159628.SZ': '国证2000ETF',
    # 可以继续添加更多股票代码和简称的映射
}

def get_stock_name(symbol: str) -> str:
    """
    获取股票的中文简称
    
    Args:
        symbol: 股票代码，例如 '159949.SZ'
        
    Returns:
        str: 股票的中文简称，如果没有找到则返回股票代码
    """
    return STOCK_NAME_MAP.get(symbol, symbol)

def get_data_briefs(data_type, data) -> list:
    if data_type == "fund":
        return [{'label': f"{item['name']}({item['ts_code']})", 'value': item['ts_code']} for index, item in data.iterrows()]
    elif data_type == "strategy":
        return [{'label': f"{item['name']}({item['strategy_id']})", 'value': item['strategy_id']} for index, item in data.iterrows()]
    elif data_type == "stock":
        return [{'label': f"{get_stock_name(item['symbol'])}({item['symbol']})", 'value': item['symbol']} for index, item in data.iterrows()]


def get_date_range(time_range):
    """
    根据选择的时间范围和数据的日期范围，计算图表显示的日期范围
    """
    if time_range == 'ALL':
        return None, None

    # 获取数据的最新日期作为结束日期
    end_date = datetime.now().date()
    
    # 根据选择的时间范围计算开始日期
    if time_range == '1M':
        start_date = end_date - relativedelta(months=1)
    elif time_range == '3M':
        start_date = end_date - relativedelta(months=3)
    elif time_range == '6M':
        start_date = end_date - relativedelta(months=6)
    elif time_range == '1Y':
        start_date = end_date - relativedelta(years=1)
    elif time_range == '3Y':
        start_date = end_date - relativedelta(years=3)
    elif time_range == '5Y':
        start_date = end_date - relativedelta(years=5)
    elif time_range == 'CQ':
        # 本季度开始
        start_date = end_date.replace(month=((end_date.month-1)//3)*3+1, day=1)
    elif time_range == 'CY':
        # 本年度开始
        start_date = end_date.replace(month=1, day=1)
    else:
        return None, None

    return start_date, end_date