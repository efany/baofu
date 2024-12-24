import requests
import pandas as pd
from datetime import datetime
import os
import time
import json

class XueqiuStockCrawler:
    """
    从雪球网爬取股票数据的爬虫
    
    功能：
    1. 获取股票的日K线数据
    2. 支持指定时间范围
    3. 导出数据到Excel
    4. 自动处理复权数据
    
    数据字段：
    - date: 交易日期
    - open: 开盘价
    - high: 最高价
    - low: 最低价
    - close: 收盘价
    - volume: 成交量
    - amount: 成交额
    """
    
    def __init__(self, cookies: str = None, auto_cookies: bool = False):
        """
        初始化爬虫
        
        Args:
            cookies: 可选的cookies字符串，如果不提供则使用默认cookies
            auto_cookies: 是否自动获取cookies，当为True时忽略cookies参数
        """
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Referer': 'https://xueqiu.com/',
            'Origin': 'https://xueqiu.com'
        }
        self.session = requests.Session()
        self.stock_data = None
        
        # 设置cookies
        if auto_cookies:
            cookies = self.get_cookies()
            if not cookies:
                print("自动获取cookies失败，使用默认cookies")
                
        self.cookies = self._parse_cookies(cookies) if cookies else {
            'cookiesu': '701719421794334',
            's': 'bd11e1i13l',
            'device_id': '71cd437bb93d78af0b4922e65b6c6596',
            'xq_is_login': '1',
            'u': '7941706469',
            'xq_a_token': '56458ee8178e981e42ab97fead87676e4c3052ef',
            'xqat': '56458ee8178e981e42ab97fead87676e4c3052ef',
            'xq_r_token': '2b9191b6f5ea8a8f4d0e8e50524f05f5260d9ccd'
        }
        
    def _parse_cookies(self, cookie_str: str) -> dict:
        """
        解析cookie字符串为字典
        
        Args:
            cookie_str: 浏览器复制的cookie字符串
            
        Returns:
            解析后的cookie字典
        """
        cookies = {}
        try:
            for item in cookie_str.split(';'):
                if not item.strip():
                    continue
                if '=' not in item:
                    continue
                name, value = item.strip().split('=', 1)
                cookies[name.strip()] = value.strip()
            return cookies
        except Exception as e:
            print(f"解析cookies失败: {str(e)}")
            return {}
            
    def _init_session(self):
        """初始化会话，设置cookies"""
        try:
            # 设置cookies
            for name, value in self.cookies.items():
                self.session.cookies.set(name, value)
                
            # 打印cookie状态
            print("\n=== Cookie Status ===")
            print(f"Total cookies: {len(self.session.cookies)}")
            for cookie in self.session.cookies:
                print(f"Name: {cookie.name}")
                print(f"Value: {cookie.value}")
                print("---")
            
            return True
        except Exception as e:
            print(f"初始化会话失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return False
            
    def fetch_stock_data(self, 
                        stock_code: str,
                        start_date: str = None,
                        end_date: str = None,
                        adjust: str = 'qfq') -> pd.DataFrame:
        """获取股票数据"""
        try:
            # 初始化会话
            if not self._init_session():
                return None
                
            # 处理股票代码格式
            if not stock_code.startswith(('SH', 'SZ')):
                if stock_code.startswith(('6', '5')):
                    stock_code = f"SH{stock_code}"
                else:
                    stock_code = f"SZ{stock_code}"
                    
            # 转换日期格式
            if start_date:
                start_timestamp = int(datetime.strptime(start_date, '%Y%m%d').timestamp() * 1000)
            else:
                start_timestamp = int((datetime.now().timestamp() - 365*24*3600) * 1000)
                
            if end_date:
                end_timestamp = int(datetime.strptime(end_date, '%Y%m%d').timestamp() * 1000)
            else:
                end_timestamp = int(datetime.now().timestamp() * 1000)
                
            # 设置请求参数
            period = 'day'
            count = 200  # 每次请求200条数据
            adjust_flag = {
                'qfq': 'before',
                'hfq': 'after',
                None: 'normal'
            }.get(adjust, 'before')
            
            # 用于存储所有数据
            all_items = []
            current_timestamp = None  # 初始化为None
            retry_count = 0
            max_retries = 3
            
            while True:  # 改为无限循环，由内部逻辑控制退出
                # 设置请求的时间戳
                request_timestamp = current_timestamp if current_timestamp else start_timestamp
                
                # 构建API URL
                url = (f'https://stock.xueqiu.com/v5/stock/chart/kline.json?'
                      f'symbol={stock_code}&begin={request_timestamp}&period={period}&'
                      f'type={adjust_flag}&count={count}&indicator=kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance')
                
                # 打印请求信息
                print(f"\n=== Requesting data before {datetime.fromtimestamp(request_timestamp/1000)} ===")
                print(f"URL: {url}")
                
                try:
                    # 发送请求
                    response = self.session.get(
                        url, 
                        headers=self.headers,
                        cookies=self.cookies,
                        timeout=10
                    )
                    
                    # 检查响应状态码
                    if response.status_code != 200:
                        print(f"Error: HTTP {response.status_code}")
                        if retry_count < max_retries:
                            retry_count += 1
                            print(f"Retrying... ({retry_count}/{max_retries})")
                            time.sleep(2)  # 等待2秒后重试
                            continue
                        else:
                            print("Max retries reached, exiting...")
                            break
                    
                    data = response.json()
                    
                    # 检查数据结构
                    if 'data' not in data:
                        print("Error: No 'data' field in response")
                        print(f"Response: {data}")
                        break
                    
                    if 'item' not in data['data']:
                        print("Error: No 'item' field in data")
                        print(f"Data: {data['data']}")
                        break
                    
                    items = data['data']['item']
                    
                    # 检查是否获取到数据
                    if not items:
                        print("No more data available")
                        break
                    
                    # 验证数据格式
                    if not isinstance(items, list) or not isinstance(items[0], list):
                        print("Error: Invalid data format")
                        print(f"Items: {items}")
                        break
                    
                    # 检查数据时间范围
                    item_timestamps = [item[0] for item in items]  # 获取所有数据的时间戳
                    earliest_timestamp = max(item_timestamps)  # 获取最早的时间戳
                    
                    # 过滤掉超出目标时间范围的数据
                    valid_items = [item for item in items if start_timestamp <= item[0] <= end_timestamp]

                    # 如果本次获取的数据时间戳与上次重叠，可能存在重复数据
                    if len(valid_items) == 0:
                        print("No data available")
                        break
                    
                    # 如果已经到达或超过开始时间
                    if earliest_timestamp >= end_timestamp:
                        print(f"Reached target start date: {datetime.fromtimestamp(end_timestamp/1000)}")
                        all_items.extend(valid_items)
                        break
                    
                    # 添加数据并更新时间戳
                    all_items.extend(valid_items)
                    current_timestamp = earliest_timestamp  # 更新为最早数据的时间戳
                    retry_count = 0  # 重置重试计数
                    
                    print(f"Retrieved {len(items)} records")
                    print(f"Current earliest date: {datetime.fromtimestamp(current_timestamp/1000)}")
                    
                    time.sleep(1)  # 添加延迟避免请求过快
                    
                except Exception as e:
                    print(f"Error processing request: {str(e)}")
                    if retry_count < max_retries:
                        retry_count += 1
                        print(f"Retrying... ({retry_count}/{max_retries})")
                        time.sleep(2)
                        continue
                    else:
                        print("Max retries reached, exiting...")
                        break
            
            if not all_items:
                print("No data retrieved")
                return None
            
            # 打印数据结构
            print("\nData structure:")
            print(f"Total records: {len(all_items)}")
            print(f"Sample data: {all_items[0]}")
            
            # 设置列名
            columns = [
                'timestamp',    # 时间戳
                'volume',       # 成交量
                'open',         # 开盘价
                'high',         # 最高价
                'low',          # 最低价
                'close',        # 收盘价
                'chg',          # 涨跌额
                'percent',      # 涨跌幅
                'turnoverrate', # 换手率
                'amount',       # 成交额
                'volume_post',  # 盘后成交量
                'amount_post',  # 盘后成交额
                'pe',          # 市盈率
                'pb',          # 市净率
                'ps',          # 市销率
                'pcf',         # 市现率
                'market_capital', # 总市值
                'balance',     # 资金流向
                'hold_volume', # 持仓量
                'hold_value',  # 持仓市值
                'position_amount', # 持金额
                'position_quantity', # 持仓数量
                'position_value',   # 持仓市值
                'position_price'    # 持仓成本
            ]
            
            # 创建DataFrame
            df = pd.DataFrame(all_items, columns=columns)
            
            # 处理时间戳
            df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # 选择需要的列
            selected_columns = [
                'date', 'open', 'high', 'low', 'close', 
                'volume', 'amount', 'percent', 'turnoverrate',
                'pe', 'pb', 'market_capital'
            ]
            df = df[selected_columns]
            
            # 按日期排序并去重
            df = df.sort_values('date').drop_duplicates(subset=['date'])
            
            # 过滤日期范围
            if start_date:
                df = df[df['date'] >= pd.Timestamp(start_date)]
            if end_date:
                df = df[df['date'] <= pd.Timestamp(end_date)]
            
            self.stock_data = df
            print(f"\nFinal dataset:")
            print(f"Date range: {df['date'].min()} to {df['date'].max()}")
            print(f"Total records: {len(df)}")
            print("\nData preview:")
            print(df.head())
            return df
            
        except Exception as e:
            print(f"Error fetching data: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None
            
    def export_to_excel(self, output_dir: str = "output") -> str:
        """
        将数据导出到Excel文件
        
        Args:
            output_dir: 输出目录
            
        Returns:
            导出的文件路径
        """
        if self.stock_data is None or self.stock_data.empty:
            print("No data to export")
            return ""
            
        # 创建输出目录
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"xueqiu_stock_data_{timestamp}.xlsx"
        filepath = os.path.join(output_dir, filename)
        
        try:
            # 导出到Excel
            self.stock_data.to_excel(filepath, index=False)
            print(f"\nData exported to: {filepath}")
            return filepath
            
        except Exception as e:
            print(f"Error exporting data: {str(e)}")
            return "" 
            
    def get_cookies(self) -> str:
        """
        自动获取雪球网的有效cookies
        
        Returns:
            str: 有效的cookie字符串
        """
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            import time
            
            # 设置Chrome选项
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # 无头模式
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            
            # 创建Chrome浏览器实例
            driver = webdriver.Chrome(options=chrome_options)
            
            try:
                # 访问雪球网首页
                print("正在访问雪球网...")
                driver.get('https://xueqiu.com/')
                time.sleep(2)  # 等待页面加载
                
                # 获取所有cookies
                cookies = driver.get_cookies()
                
                # 转换cookies为字符串格式
                cookie_str = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])
                
                print("成功获取cookies")
                return cookie_str
                
            finally:
                driver.quit()
                
        except Exception as e:
            print(f"获取cookies失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return "" 