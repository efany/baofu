from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict

# 股票代码到中文简称的映射
STOCK_NAME_MAP: Dict[str, str] = {
    '159949.SZ': '创业板50ETF',
    '512550.SS': 'A50ETF',
    '159633.SZ': '中证1000ETF',
    '159628.SZ': '国证2000ETF',
    '515030.SS': '新能源车ETF',
    '513050.SS': '中概互联网ETF',
    '515950.SS': '医药龙头ETF',
    '515000.SS': '科技ETF',
    '515220.SS': '煤炭ETF',
    '512400.SS': '有色金属ETF',
    '515880.SS': '通信ETF',
    '159825.SZ': '农业ETF',
    '512800.SS': '银行ETF',
    '159766.SZ': '旅游ETF',
    '515450.SS': '红利低波ETF',
    '512660.SS': '军工ETF',
    '512980.SS': '传媒ETF',
    '159996.SZ': '家电ETF',
    '512480.SS': '半导体ETF',
    '512000.SS': '券商ETF'

    # 515030.SS,513050.SS,515950.SS,515000.SS,515220.SS,512400.SS,515880.SS,159825.SZ,512800.SS,159766.SZ,515450.SS,512660.SS,512980.SS,159996.SZ,512480.SS,512000.SS
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

FOREX_NAME_MAP: Dict[str, str] = {
    'USDCNH': '美元兑人民币',
    'USDJPY': '美元兑日元',
    'USDCHF': '美元兑瑞郎',
    'JPYUSD': '日元兑美元',
    'CNHUSD': '人民币兑美元',
    'CHFUSD': '瑞郎兑美元',
    'CNHJPY': '人民币兑日元',
    'CNHCHF': '人民币兑瑞郎',
    'JPYCNH': '日元兑人民币',
    'CHFCNH': '瑞郎兑人民币'
}

def get_forex_name(symbol: str) -> str:
    """
    获取外汇的中文简称
    
    Args:
        symbol: 外汇代码，例如 'USDCNH'
        
    Returns:
        str: 外汇的中文简称，如果没有找到则返回外汇代码
    """
    return FOREX_NAME_MAP.get(symbol, symbol)

BOND_NAME_MAP: Dict[str, str] = {
    'CN_30Y': '中国30年期国债收益率',
    'US_30Y': '美国30年期国债收益率',
    'CN_10Y': '中国10年期国债收益率',
    'US_10Y': '美国10年期国债收益率',
    'CN_5Y': '中国5年期国债收益率',
    'US_5Y': '美国5年期国债收益率',
    'CN_2Y': '中国2年期国债收益率',
    'US_2Y': '美国2年期国债收益率'
}

def get_bond_name(symbol: str) -> str:
    """
    获取债券的中文简称
    
    Args:
        symbol: 债券代码，例如 'CN_30Y'
        
    Returns:
        str: 债券的中文简称，如果没有找到则返回债券代码
    """
    return BOND_NAME_MAP.get(symbol, symbol)


def get_data_briefs(data_type, data) -> list:
    if data_type == "fund":
        return [{'label': f"{item['name']}({item['ts_code']})", 'value': item['ts_code']} for index, item in data.iterrows()]
    elif data_type == "strategy":
        return [{'label': f"{item['name']}({item['strategy_id']})", 'value': item['strategy_id']} for index, item in data.iterrows()]
    elif data_type == "stock":
        return [{'label': f"{get_stock_name(item['symbol'])}({item['symbol']})", 'value': item['symbol']} for index, item in data.iterrows()]
    elif data_type == "forex":
        return [{'label': f"{get_forex_name(item['symbol'])}({item['symbol']})", 'value': item['symbol']} for index, item in data.iterrows()]
    elif data_type == "bond_yield":
        return [{'label': f"{get_bond_name(item['bond_type'])}({item['bond_type']})", 'value': item['bond_type']} for index, item in data.iterrows()]
    elif data_type == "index":
        return [{'label': f"{item['name']}({item['symbol']})", 'value': item['symbol']} for index, item in data.iterrows()]
    else:
        return []

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