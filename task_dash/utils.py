from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Any
import plotly.graph_objs as go
import plotly.io as pio
import os
import hashlib

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
    '512000.SS': '券商ETF',
    '512690.SS': '酒ETF'

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


def generate_chart_image(chart_data: List[Dict[str, Any]], data_type: str, data_id: str, 
                        cache_dir: str = "./task_dash/assets/chart_cache", return_absolute_path: bool = False) -> str:
    """
    根据DataGenerator的get_chart_data返回值生成图片并保存到缓存目录
    
    该函数将各种类型的DataGenerator的get_chart_data()返回值转换为Plotly图表，
    然后生成PNG图片文件，确保与single_product.py页面的展示效果一致。
    
    需要安装kaleido包才能生成图片: pip install kaleido
    如果kaleido不可用，函数将返回空字符串。
    
    Args:
        chart_data: DataGenerator.get_chart_data()的返回值，包含Plotly图表数据
        data_type: 数据类型 ('fund', 'strategy', 'stock', 'forex', 'bond_yield', 'index')
        data_id: 数据标识符，如基金代码、股票代码等
        cache_dir: 缓存目录路径，默认为"./task_dash/assets/chart_cache"
        return_absolute_path: 是否返回绝对路径，False时返回Web路径(/assets/...)，True时返回文件系统路径
        
    Returns:
        str: 图片路径，Web路径或绝对文件系统路径，如果生成失败则返回空字符串
        
    Example:
        >>> from task_dash.datas.fund_data_generator import FundDataGenerator
        >>> generator = FundDataGenerator('000001.OF', mysql_db)
        >>> generator.load()
        >>> chart_data = generator.get_chart_data()
        >>> image_path = generate_chart_image(chart_data, 'fund', '000001.OF')
        >>> if image_path:
        ...     print(f"图片已生成: {image_path}")
    """
    if not chart_data:
        return ""
    
    # 确保缓存目录存在
    os.makedirs(cache_dir, exist_ok=True)
    
    # 生成唯一的文件名，处理特殊字符
    chart_hash = hashlib.md5(str(chart_data).encode()).hexdigest()[:8]
    # 清理data_id中的特殊字符，确保文件名安全
    safe_data_id = data_id.replace('.', '_').replace('/', '_').replace('\\', '_')
    filename = f"{data_type}_{safe_data_id}_{chart_hash}.png"
    file_path = os.path.join(cache_dir, filename)
    
    # 如果文件已存在，直接返回路径
    if os.path.exists(file_path):
        if return_absolute_path:
            return os.path.abspath(file_path)
        else:
            # 返回相对于assets目录的路径，以便Dash能正确提供静态文件服务
            return f"/assets/chart_cache/{filename}"
    
    try:
        # 根据数据类型设置图表标题
        if data_type == 'fund':
            title = '基金净值和分红数据'
        elif data_type == 'strategy':
            title = '策略净值数据'
        elif data_type == 'stock':
            title = '股票K线数据'
        elif data_type == 'index':
            title = '指数价格走势'
        elif data_type == 'forex':
            title = '外汇汇率走势'
        elif data_type == 'bond_yield':
            title = '债券收益率走势'
        else:
            title = '价格数据'
        
        # 根据数据类型设置不同的x轴配置
        xaxis_config = {
            'title': '日期',
            'rangeslider': {'visible': False}
        }
        
        if data_type == 'stock':
            xaxis_config.update({
                'tickmode': 'auto',
                'nticks': 20,
                'tickangle': -45,
                'showgrid': True,
                'gridcolor': '#f0f0f0'
            })
        
        # 创建图表布局
        layout = {
            'title': f'{title} - {data_id}',
            'xaxis': xaxis_config,
            'yaxis': {'title': '价格' if data_type in ['stock', 'index', 'forex'] else '净值/收益率'},
            'plot_bgcolor': 'white',
            'hovermode': 'x unified',
            'width': 800,
            'height': 500
        }
        
        # 转换chart_data为正确的Plotly格式
        plotly_traces = []
        for trace_data in chart_data:
            trace_copy = trace_data.copy()
            
            # 转换type字段为Plotly兼容格式
            if trace_copy.get('type') == 'line':
                trace_copy['type'] = 'scatter'
                trace_copy['mode'] = 'lines'
            elif trace_copy.get('type') == 'scatter' and 'mode' not in trace_copy:
                trace_copy['mode'] = 'markers'
            
            plotly_traces.append(trace_copy)
        
        # 生成图片
        fig = go.Figure(data=plotly_traces, layout=layout)
        
        # 检查是否可用kaleido引擎
        try:
            pio.write_image(fig, file_path, format='png', width=800, height=500, engine='kaleido')
        except ValueError as ve:
            if "kaleido" in str(ve).lower():
                print(f"Kaleido引擎不可用，请安装: pip install kaleido")
                return ""
            else:
                raise ve
        
        # 根据参数决定返回Web路径还是绝对文件系统路径
        if return_absolute_path:
            return os.path.abspath(file_path)
        else:
            # 返回相对于assets目录的路径，以便Dash能正确提供静态文件服务
            return f"/assets/chart_cache/{filename}"
        
    except Exception as e:
        # 如果生成失败，返回空字符串
        print(f"生成图表图片失败: {str(e)}")
        return ""