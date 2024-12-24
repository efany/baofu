import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import time
from typing import List, Dict, Any, Optional
import urllib3
import json
import hashlib
from PIL import Image
import io
import base64
import re
import openpyxl

class MorningstarFundCrawler:
    """
    晨星网基金爬虫
    
    功能：
    1. 爬取三年评级三星以上的纯债基金
    2. 支持数据导出
    3. 支持自动翻页
    4. 实现礼貌爬取
    
    数据字段：
    - fund_code: 基金代码
    - fund_name: 基金名称
    - fund_type: 基金类型
    - rating_3year: 三年评级
    - rating_5year: 五年评级
    - nav: 最新净值
    - nav_date: 净值日期
    - return_3year: 三年回报(%)
    - return_5year: 五年回报(%)
    - risk_level: 风险等级
    """
    
    # 基金类型对应的checkbox ID映射
    FUND_TYPE_MAP = {
        "大盘成长股票": "0",
        "大盘平衡股票": "1",
        "大盘价值股票": "2",
        "中盘成长股票": "3",
        "中盘平衡股票": "4",
        "香港股": "5",
        "沪深股票": "6",
        "行业股票-医药": "7",
        "行业股票-消费": "8",
        "行业股票-金融地产": "9",
        "行业股票-其它": "10",
        "积极配置-大盘成长": "11",
        "积极配置-大盘平衡": "12",
        "积极配置-中小盘": "13",
        "标准混合": "14",
        "保守混合": "15",
        "灵活配置": "16",
        "港股积极配置": "17",
        "沪港深积极配置": "18",
        "沪港深保守混合": "19",
        "沪港深灵活配置": "20",
        "行业混合-医药": "21",
        "行业混合-消费": "22",
        "目标日期": "23",
        "基础设施REITs": "24",
        "可转债": "25",
        "积极债券": "26",
        "普通债券": "27",
        "纯债": "28",
        "利率债": "29",
        "信用债": "30",
        "短债": "31",
        "货币市场": "32",
        "市场中性": "33",
        "商品-贵金属": "34",
        "商品-其它": "35",
        "其它": "36",
        "QDII环球债券": "37",
        "QDII商品": "38",
        "QDII大中华区股票": "39",
        "QDII新兴市场股票": "40",
        "QDII环球票": "41",
        "QDII美国股票": "42",
        "QDII行业股票": "43",
        "QDII环球股债混合": "44",
        "QDII亚洲股债混合": "45",
        "北上基金": "46",
        "QDII其它": "47",
        "行业混合-科技传媒及通讯": "48",
        "行业股票-科技传媒及通讯": "49",
        "QDII亚太区不包括日本股票": "50",
        "QDII全球新兴市场股债混合": "51",
        "QDII大中华股债混合": "52"
    }
    
    DEFAULT_CONFIG = {
        "fund_type": {
            "enabled": True,
            "categories": ["债券型"]  # 默认只选择债券型
        },
        "rating": {
            "enabled": True,
            "three_year": "3",  # 三年评级: 0-3表示三星以下, 3-5表示三星以上
            "five_year": ""     # 五年评级: 0-3表示三星以下, 3-5表示三星以上
        },
        "fund_status": {
            "enabled": False,
            "status": "O"  # 开放申购
        },
        "max_records": 0,  # 最大记录数,0表示不限制
        "page_size": 25,  # 每页显示数量
        "delay": 2.0  # 请求延迟(秒)
    }
    
    def __init__(self, config_file: Optional[str] = None, cache_dir: str = "cache/morningstar", use_cache: bool = True):
        """
        初始化爬虫
        
        Args:
            config_file: 配置文件路径，JSON格式
            cache_dir: 缓存目录路径
            use_cache: 是否允许使用缓存
        """
        # 加载配置
        self.config = self.load_config(config_file)
        self.cache_dir = cache_dir
        self.use_cache = use_cache
        self.is_cache_used = False
        
        # 计算配置的hash值
        self.config_hash = self._calculate_config_hash()
        print(f"配置Hash: {self.config_hash}")
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www.morningstar.cn',
            'Referer': 'https://www.morningstar.cn/fundselect/default.aspx',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1'
        }
        self.session = requests.Session()
        self.fund_data: List[Dict] = []
        
        # 设置重试策略
        retry_strategy = urllib3.util.Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
    def load_config(self, config_file: Optional[str]) -> Dict[str, Any]:
        """
        加载配置文件
        
        Args:
            config_file: 配置文件路径
            
        Returns:
            Dict: 配置字典
        """
        config = self.DEFAULT_CONFIG.copy()
        
        if config_file:
            try:
                if not os.path.exists(config_file):
                    print(f"配置文件不存在: {config_file}")
                    print("使用默认配置")
                    return config
                    
                with open(config_file, 'r', encoding='utf-8') as f:
                    try:
                        content = f.read()
                        
                        user_config = json.loads(content)
                    except json.JSONDecodeError as e:
                        print(f"配置��件格式错误: {str(e)} config_file: {config_file} content: {content}")
                        print("使用默认配置")
                        return config
                        
                    # 更新配置，保持默认值
                    self._update_config(config, user_config)
                    
            except Exception as e:
                print(f"加载配置文件失败: {str(e)}")
                print("使用默认配置")
    
        print("当前配置:")
        print(json.dumps(config, indent=4, ensure_ascii=False))
        return config
        
    def _update_config(self, base_config: Dict, new_config: Dict) -> None:
        """递归更新配置字典"""
        for key, value in new_config.items():
            if key in base_config:
                if isinstance(value, dict) and isinstance(base_config[key], dict):
                    self._update_config(base_config[key], value)
                else:
                    base_config[key] = value
                    
    def _get_viewstate(self, html: str) -> str:
        """获取ASP.NET的ViewState值"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # 尝试不同的方式查找ViewState
            viewstate = (
                soup.find('input', {'name': '__VIEWSTATE'}) or
                soup.find('input', {'id': '__VIEWSTATE'}) or
                soup.find('input', {'type': 'hidden', 'name': '__VIEWSTATE'})
            )
            
            if viewstate:
                return viewstate.get('value', '')
            
            # 如果找不到，尝试从页面源码中直接提取
            match = re.search(r'id="__VIEWSTATE"\s+value="([^"]+)"', html)
            if match:
                return match.group(1)
            
            return ''
            
        except Exception as e:
            print(f"获取 ViewState 失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return ''
        
    def _get_eventvalidation(self, html: str) -> str:
        """获取ASP.NET的EventValidation值"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # 尝试不同的方式查找 EventValidation
            eventvalidation = (
                soup.find('input', {'name': '__EVENTVALIDATION'}) or
                soup.find('input', {'id': '__EVENTVALIDATION'}) or
                soup.find('input', {'type': 'hidden', 'name': '__EVENTVALIDATION'})
            )
            
            if eventvalidation:
                return eventvalidation.get('value', '')
            
            # 如果找不到，尝试从页面源码中直接提取
            match = re.search(r'id="__EVENTVALIDATION"\s+value="([^"]+)"', html)
            if match:
                return match.group(1)
            
            return ''
            
        except Exception as e:
            print(f"获取 EventValidation 失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return ''
        
    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """发送请求并处理超时重试"""
        max_retries = 3
        retry_count = 0
        timeout = (10, 30)  # (连接时, 读取超时)
        
        while retry_count < max_retries:
            try:
                if method.upper() == 'GET':
                    response = self.session.get(url, timeout=timeout, **kwargs)
                else:
                    response = self.session.post(url, timeout=timeout, **kwargs)
                response.raise_for_status()
                return response
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                retry_count += 1
                wait_time = retry_count * 2
                print(f"\n请求超时，{wait_time}秒后进行第{retry_count}次重试...")
                time.sleep(wait_time)
                if retry_count == max_retries:
                    raise e
            except requests.exceptions.RequestException as e:
                print(f"请求失败: {str(e)}")
                raise e
        
    def _get_fund_type_map(self, html: str) -> Dict[str, str]:
        """从网页中获取基金分类映射关系"""
        soup = BeautifulSoup(html, 'html.parser')
        fund_types = {}
        
        # 查找基金分类的checkbox列表
        category_list = soup.find('div', {'id': 'ctl00_cphMain_cblCategory'})
        if category_list:
            for item in category_list.find_all('input', type='checkbox'):
                label = item.find_next('label')
                if label:
                    fund_type = label.get_text(strip=True)
                    # 从input的id中提取序号
                    checkbox_id = item.get('id', '').split('_')[-1]
                    fund_types[fund_type] = checkbox_id
                    
        if fund_types:
            return fund_types
        
        print("警告: 未能从网页获取基金分类信息，使用默认映射")
        return self.FUND_TYPE_MAP
        
    def _get_pager_params(self, soup, target_page: int) -> tuple:
        """从分页控件中获取事件参数"""
        pager = soup.find('div', {'id': 'ctl00_cphMain_AspNetPager1'})
        if not pager:
            return None, None
        
        # 查找目标页码的链接
        for link in pager.find_all('a', href=True):
            text = link.get_text(strip=True)
            if text.isdigit() and int(text) == target_page:
                href = link['href']
                # 从href中提取事件参数
                # 格式: javascript:__doPostBack('target','argument')
                if "javascript:__doPostBack(" in href:
                    parts = href.split("'")
                    if len(parts) >= 4:
                        return parts[1], parts[3]
        
        return None, None

    def _make_business_request(self, page: int = 1) -> requests.Response:
        """发送业务请求"""
        try:
            url = 'https://www.morningstar.cn/fundselect/default.aspx'
            
            # 获取上一页的状态用于翻页
            response = self._make_request('GET', url, headers=self.headers)
            viewstate = self._get_viewstate(response.text)
            if not viewstate:
                raise Exception("无法获取页面状态参数")
            
            # 构建请求参数
            data = {
                '__EVENTTARGET': '',
                '__EVENTARGUMENT': '',
                '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': '9E46DF1C',
                'ctl00$cphMain$hfStylebox': '0,0,0,0,0,0,0,0,0',
                'ctl00$cphMain$hfRisk': '0,0,0,0',
                'ctl00$cphMain$ucPerformance$YTD': 'rbYtd2',
                'ctl00$cphMain$ucPerformance$txtYtd': '',
                'ctl00$cphMain$ucPerformance$Month3': 'rbM32',
                'ctl00$cphMain$ucPerformance$txtM3': '',
                'ctl00$cphMain$ucPerformance$Month6': 'rbM62',
                'ctl00$cphMain$ucPerformance$txtM6': '',
                'ctl00$cphMain$ucPerformance$Year1': 'rbY12',
                'ctl00$cphMain$ucPerformance$txtY1': '',
                'ctl00$cphMain$ucPerformance$Year2': 'rbY22',
                'ctl00$cphMain$ucPerformance$txtY2': '',
                'ctl00$cphMain$ucPerformance$Year3': 'rbY32',
                'ctl00$cphMain$ucPerformance$txtY3': '',
                'ctl00$cphMain$ucPerformance$Year5': 'rbY52',
                'ctl00$cphMain$ucPerformance$txtY5': '',
                'ctl00$cphMain$ucPerformance$Year10': 'rbY102',
                'ctl00$cphMain$ucPerformance$txtY10': '',
                'ctl00$cphMain$ddlEffectiveDate': 'G',
                'ctl00$cphMain$txtEffectiveDate': '',
                'ctl00$cphMain$ddlFundStatus': '',
                'ctl00$cphMain$hfTNA': '0~5',
                'ctl00$cphMain$hfMinInvest': '0~5',
                'ctl00$cphMain$txtFund': '',
                'ctl00$cphMain$hfMoreOptions': 'close',
                'ctl00$cphMain$ddlPageSite': '25'
            }

            # 基金类型
            if self.config['fund_type']['enabled']:
                for fund_type in self.config['fund_type']['categories']:
                    if fund_type in self.FUND_TYPE_MAP:
                        key = f'ctl00$cphMain$cblCategory${self.FUND_TYPE_MAP[fund_type]}'
                        data[key] = 'on'

            # 评级条件
            if self.config['rating']['enabled']:
                # 三年评级
                if self.config['rating']['three_year']:
                    rating = int(self.config['rating']['three_year'])
                    if 3 <= rating <= 5:
                        data['ctl00$cphMain$cblStarRating$0'] = 'on'  # 三星以上
                
                # 五年评级
                if self.config['rating']['five_year']:
                    rating = int(self.config['rating']['five_year'])
                    if 3 <= rating <= 5:
                        data['ctl00$cphMain$cblStarRating5$0'] = 'on'  # 三星以上

            # 基金状态
            if self.config['fund_status']['enabled']:
                data['ctl00$cphMain$ddlFundStatus'] = self.config['fund_status']['status']

            if page > 1:
                # 下一页请求
                data['__EVENTTARGET'] = 'ctl00$cphMain$AspNetPager1'
                data['__EVENTARGUMENT'] = str(page)
            else:
                # 第一页需要点击查询按钮
                data['ctl00$cphMain$btnGo'] = '查询'
            # 打印请求详情
            print("\n请求详情:")
            print(f"URL: {url}")
            # print("\nHeaders:")
            # print(json.dumps(self.headers, indent=4, ensure_ascii=False))
            # print("\nData:")
            # print(json.dumps(data, indent=4, ensure_ascii=False))
            # 发送请求
            response = self._make_request(
                'POST',
                url,
                data=data,
                headers=self.headers,
                allow_redirects=True
            )

            return response

        except Exception as e:
            print(f"请求失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            raise

    def _get_basic_info(self, soup: BeautifulSoup) -> Dict[str, str]:
        """从基金页面获取基本信息"""
        details = {
            'morningstar_classify': '',  # 晨星分类
            'found_date': '',      # 成立日期
            'fund_size': '',       # 总净资产
        }
        
        qt_base = soup.find('div', {'id': 'qt_base'})
        if qt_base:
            info_list = qt_base.find('ul', {'class': 'info'})
            if info_list:
                for item in info_list.find_all('li'):
                    label = item.find('b')
                    value = item.find('span')
                    
                    if label and value:
                        label_text = label.get_text(strip=True)
                        value_text = value.get_text(strip=True)
                        
                        if '晨星分类' in label_text:
                            details['morningstar_classify'] = value_text
                        elif '成立日期' in label_text:
                            details['found_date'] = value_text
                        elif '总净资产' in label_text:
                            details['fund_size'] = value_text
        
        return details

    def _get_current_returns(self, fcid: str) -> Dict[str, str]:
        """获取当前回报数据"""
        details = {
            'ytd_return': '',     # 今年以来回报
            'm1_return': '',      # 近1月回报
            'm3_return': '',      # 近3月回报
            'm6_return': '',      # 近6月回报
            'y1_return': '',      # 近1年回报
            'y2_return': '',      # 近2年回报
            'y3_return': '',      # 近3年回报
            'y5_return': '',      # 近5年回报
            'y10_return': '',     # 近10年回报
        }
        
        try:
            url = f'https://www.morningstar.cn/handler/quicktake.ashx?command=return&fcid={fcid}'
            response = self._make_request('GET', url, headers=self.headers)
            data = response.json()
            
            if 'CurrentReturn' in data and 'Return' in data['CurrentReturn']:
                for item in data['CurrentReturn']['Return']:
                    name = item.get('Name', '')
                    return_value = item.get('Return', '')
                    
                    if name == '今年以来回报':
                        details['ytd_return'] = return_value
                    elif name == '一个月回报':
                        details['m1_return'] = return_value
                    elif name == '三个月回报':
                        details['m3_return'] = return_value
                    elif name == '六个月回报':
                        details['m6_return'] = return_value
                    elif name == '一年回报':
                        details['y1_return'] = return_value
                    elif name == '二年回报（年化）':
                        details['y2_return'] = return_value
                    elif name == '三年回报（年化）':
                        details['y3_return'] = return_value
                    elif name == '五年回报（年化）':
                        details['y5_return'] = return_value
                    elif name == '十年回报（年化）':
                        details['y10_return'] = return_value
        
        except Exception as e:
            print(f"获取当前回报数据失败: {str(e)}")
        
        return details

    def _get_annual_returns(self, fcid: str) -> Dict[str, str]:
        """获取年度回报数据"""
        details = {}
        
        try:
            url = f'https://www.morningstar.cn/handler/quicktake.ashx?command=performance&fcid={fcid}'
            response = self._make_request('GET', url, headers=self.headers)
            data = response.json()
            
            # 从响应数据中获取年度键列表
            year_keys = [key for key in data.keys() if isinstance(data[key], dict) and 'Year' in data[key]]
            
            # 处理每个年度的数据
            for key in year_keys:
                year_data = data[key]
                year = year_data.get('Year', '')
                if year:
                    # 年度回报
                    return_year = year_data.get('ReturnYear', '')
                    return_year_ind = year_data.get('ReturnYear_Ind', '')
                    return_year_cat = year_data.get('ReturnYear_Cat', '')
                    
                    if return_year:
                        details[f'year_{year}'] = return_year
                        details[f'year_{year}_vs_benchmark'] = return_year_ind
                        details[f'year_{year}_vs_category'] = return_year_cat
                    
                    # 季度回报
                    for q in range(1, 5):
                        q_return = year_data.get(f'ReturnQ{q}', '')
                        q_cat = year_data.get(f'ReturnQ{q}_Cat', '')
                        q_ind = year_data.get(f'ReturnQ{q}_Ind', '')
                        
                        if q_return:
                            details[f'year_{year}_q{q}'] = q_return
                            details[f'year_{year}_q{q}_vs_benchmark'] = q_ind
                            details[f'year_{year}_q{q}_vs_category'] = q_cat
            
            # 获取最差回报
            details['worst_3mon_return'] = data.get('Worst3MonReturn', '')
            details['worst_6mon_return'] = data.get('Worst6MonReturn', '')
        
        except Exception as e:
            print(f"获取年度回报数据失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
        
        return details

    def _get_fund_detail(self, fund_code: str, fund_link: str) -> Dict[str, str]:
        """获取基金详细信息"""
        try:
            # 访问基金链接获取基本信息
            response = self._make_request('GET', fund_link, headers=self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 获取fcid
            fcid = fund_code
            if '/quicktake/' in fund_link:
                fcid = fund_link.split('/quicktake/')[-1]
            
            # 获取各类数据
            basic_info = self._get_basic_info(soup)
            current_returns = self._get_current_returns(fcid)
            annual_returns = self._get_annual_returns(fcid)
            portfolio = self._get_portfolio(fcid)
            manage = self._get_manage(fcid)
            
            # 合并所有数据
            details = {**basic_info, **current_returns, **annual_returns, **portfolio, **manage}
            
            return details
            
        except Exception as e:
            print(f"获取基金详情失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return {}

    def _find_latest_cache(self) -> Optional[str]:
        """
        查找最新的缓存文件
        
        Returns:
            str: 缓存文件路径，如果没有找到则返回None
        """
        try:
            if not os.path.exists(self.cache_dir):
                return None
            
            # 查找所有匹配的缓存文件
            cache_pattern = f"morningstar_funds_{self.config_hash}_*.xlsx"
            cache_files = []
            
            for file in os.listdir(self.cache_dir):
                if file.startswith(f"morningstar_funds_{self.config_hash}_") and file.endswith(".xlsx"):
                    cache_path = os.path.join(self.cache_dir, file)
                    # 获取文件日期（文件名格式：morningstar_funds_<hash>_YYYYMMDD.xlsx）
                    date_str = file.split('_')[-1].split('.')[0]
                    cache_files.append((cache_path, date_str))
            
            if not cache_files:
                return None
            
            # 按日期排序，返回最新的
            latest_cache = sorted(cache_files, key=lambda x: x[1], reverse=True)[0][0]
            print(f"找到最新缓存: {latest_cache}")
            return latest_cache
            
        except Exception as e:
            print(f"查找缓存失败: {str(e)}")
            return None
        
    def __request_fund_data(self) -> List[Dict]:
        """获取满足条件的基金数据"""
        fund_datas = []
        try:
            print("正在访问晨星网...")
            
            # 获取配置参数
            max_records = self.config.get('max_records', 0)
            delay = float(self.config.get('delay', 2.0))  # 确保是浮点数
            
            page = 1
            while True:
                print(f"\n正在获取第 {page} 页数据...")
                
                try:
                    response = self._make_business_request(page)
                except Exception as e:
                    print(f"请求失败: {str(e)}")
                    break
                
                # 解析数据
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 查找数据表格
                table = soup.find('table', {'id': 'ctl00_cphMain_gridResult'})
                if not table:
                    print("未找到数据表格")
                    break
                
                # 处理当前页数据
                rows = table.find_all('tr')[1:]  # 跳过表头
                if not rows:
                    print("没有更多数据")
                    break
                
                # 检查是否达到最大数条数限制
                remaining = max_records - len(fund_datas) if max_records > 0 else len(rows)
                if remaining <= 0:
                    print(f"\n已达到最大数据条数限制: {max_records}")
                    break
                
                # 处理数据
                for row in rows[:remaining]:
                    cells = row.find_all('td')
                    if len(cells) >= 10:
                        # 获取基金代码和链接
                        fund_code = cells[1].get_text(strip=True)
                        fund_link = cells[2].find('a')['href'] if cells[2].find('a') else None
                        
                        if not fund_link:
                            print(f"未找到基金 {fund_code} 的链接")
                            continue
                        
                        # 确保链接是完整的URL
                        if not fund_link.startswith('http'):
                            fund_link = f"https://www.morningstar.cn{fund_link}"
                        
                        # 获取评级
                        rating_3year = self._parse_rating(cells[4].find('img'))
                        rating_5year = self._parse_rating(cells[5].find('img'))
                        
                        # 清理数据，移除百分号和其他非数字字符
                        nav = cells[7].get_text(strip=True).replace(',', '')
                        nav_diff = cells[8].get_text(strip=True).replace(',', '').replace('%', '')
                        year_return = cells[9].get_text(strip=True).replace(',', '').replace('%', '')
                        
                        # 获取基金详情
                        fund_detail = self._get_fund_detail(fund_code, fund_link)
                        
                        # 合并基础数据和详情数据
                        fund_data = {
                            'fund_code': fund_code,
                            'fund_name': cells[2].get_text(strip=True),
                            'fund_share_type': cells[2].get_text(strip=True)[-1] if re.match(r'[A-Z]', cells[2].get_text(strip=True)[-1]) else '',
                            'fund_type': cells[3].get_text(strip=True),
                            'rating_3year': rating_3year,
                            'rating_5year': rating_5year,
                            'nav_date': cells[6].get_text(strip=True),
                            'nav': nav,
                            'nav_diff': nav_diff,
                            'year_return': year_return,
                            **fund_detail  # 展开详情数据
                        }
                        
                        fund_datas.append(fund_data)
                        time.sleep(0.5)  # 添加延迟避免请求过快
                
                print(f"已取 {len(fund_datas)} 条记录")
                
                # 检查是否有下一页
                pager = soup.find('div', {'id': 'ctl00_cphMain_AspNetPager1'})
                if not pager:
                    print("未找到分页控件")
                    break
                
                # 查找下一页链接
                next_link = pager.find('a', text='>')
                if not next_link:
                    print("已到达最后一页")
                    break
                
                # 检查是否已达到最大记录数
                if max_records > 0 and len(fund_datas) >= max_records:
                    print(f"\n已达到最大数据条数限制: {max_records}")
                    break
                
                page += 1
                time.sleep(delay)  # 使用配置中的延迟时间
            
            print(f"\n共获取 {len(fund_datas)} 条基金数据")
            if max_records > 0:
                print(f"最大限制: {max_records} 条")
            return fund_datas
            
        except Exception as e:
            print(f"获取数据失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return fund_datas

    def fetch_fund_data(self) -> List[Dict[str, Any]]:
        """获取基金数据"""
        # 如果允许使用缓存，先尝试读取缓存
        if self.use_cache:
            cache_file = self._find_latest_cache()
            if cache_file:
                try:
                    print(f"正在读取缓存: {cache_file}")
                    df = pd.read_excel(cache_file, dtype={'fund_code': str})
                    self.fund_data = df.to_dict('records')
                    self.is_cache_used = True
                    print(f"从缓存读取了 {len(self.fund_data)} 条数据")
                    return self.fund_data
                except Exception as e:
                    print(f"读取缓存失败: {str(e)}")
                    print("将重新获取数据")
        self.fund_data = self.__request_fund_data()
        self.export_to_cache()
        return self.fund_data
    
    def export_to_cache(self) -> str:
        """
        将数据导出到缓存目录
        
        Returns:
            str: 缓存文件路径
        """
        if not self.fund_data:
            print("没有数据可导出到缓存")
            return ""
        
        try:
            # 确保缓存目录存在
            os.makedirs(self.cache_dir, exist_ok=True)
            
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d")
            filename = f"morningstar_funds_{self.config_hash}_{timestamp}.xlsx"
            cache_path = os.path.join(self.cache_dir, filename)
            
            # 导出数据
            with pd.ExcelWriter(cache_path, engine='openpyxl') as writer:
                # 转换为DataFrame
                df = pd.DataFrame(self.fund_data)
                # 导出到Excel
                df.to_excel(writer, sheet_name='基金列表', index=False)

            print(f"\n数据已缓存到: {cache_path}")
            print(f"总记录数: {len(df)}")
            print(f"配置Hash: {self.config_hash}")
            
            return cache_path
            
        except Exception as e:
            print(f"导出缓存失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return ""
    
    def export_to_excel(self, output_dir: str = "output") -> str:
        """导出数据到Excel文件"""
        if not self.fund_data:
            print("没有数据可导出")
            return ""
        
        try:
            # 确保输出目录存在
            os.makedirs(output_dir, exist_ok=True)
            
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"morningstar_funds_{timestamp}.xlsx"
            filepath = os.path.join(output_dir, filename)
            
            # 导出到Excel
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # 转换为DataFrame
                df = pd.DataFrame(self.fund_data)
                
                # 导出到Excel
                df.to_excel(writer, sheet_name='基金列表', index=False)
                
                # 调整列宽
                worksheet = writer.sheets['基金列表']
                for idx, column in enumerate(df.columns, 1):
                    max_length = max(
                        df[column].astype(str).apply(len).max(),
                        len(str(column))
                    ) + 2
                    col_letter = openpyxl.utils.get_column_letter(idx)
                    worksheet.column_dimensions[col_letter].width = max_length
            
            print(f"\n数据已导出到: {filepath}")
            print(f"总记录数: {len(df)}")
            return filepath
            
        except Exception as e:
            print(f"导出数据失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return ""
        
    def _parse_rating(self, img_element) -> str:
        """
        通过比对本地评级图片件确定评级值
        
        Args:
            img_element: BeautifulSoup的img标签元素
            
        Returns:
            str: 评级值(0-5)，空字符串表示无评级
        """
        if not img_element:
            return ''
        
        try:
            # 获取图片URL
            src = img_element.get('src', '')
            if not src:
                return ''
            
            # 下载图片
            response = self.session.get(src, headers=self.headers)
            if response.status_code != 200:
                print(f"下载图片失败: {response.status_code}")
                return ''
            
            img_bytes = response.content
            
            # 计算图片哈希值
            img_hash = hashlib.md5(img_bytes).hexdigest()
            
            # 如果是第一次运行，计算地片哈希值
            if not hasattr(self, '_rating_hash_map'):
                self._rating_hash_map = {}
                rating_dir = "C:/Users/yanyifan/code/baofu/web_crawler/res/morningstar/rating/"
                
                if os.path.exists(rating_dir):
                    for filename in os.listdir(rating_dir):
                        if filename.endswith('.gif'):
                            file_path = os.path.join(rating_dir, filename)
                            with open(file_path, 'rb') as f:
                                file_hash = hashlib.md5(f.read()).hexdigest()
                            
                            # 从文件名中提取评级值
                            try:
                                # 尝试不同的件名格式
                                if '_' in filename:
                                    # 格式: rating_X.gif 或 X_star.gif
                                    parts = filename.split('_')
                                    if parts[0].isdigit():
                                        rating = parts[0]
                                    else:
                                        rating = parts[1].split('.')[0]
                                else:
                                    # 格式: X.gif
                                    rating = filename.split('.')[0]
                                    
                                # 确保评级是有效的字
                                rating = ''.join(filter(str.isdigit, rating))
                                if rating and 0 <= int(rating) <= 5:
                                    self._rating_hash_map[file_hash] = rating
                                else:
                                    print(f"跳过无效评级图片: {filename}")
                                    
                            except Exception as e:
                                print(f"无法解析文件名 {filename}: {str(e)}")
                                continue
                else:
                    print(f"评级图片目录不存在: {rating_dir}")
                    print("请创建目录并添加评级图片，格式如: 3_star.gif 或 rating_3.gif")
            
            # 查找配的评级
            rating = self._rating_hash_map.get(img_hash, '')
            
            if not rating:
                # 保存未识别的图片用于分析
                unknown_dir = os.path.join('res', 'morningstar', 'rating', 'unknown')
                os.makedirs(unknown_dir, exist_ok=True)
                unknown_file = os.path.join(unknown_dir, f'unknown_{img_hash[:8]}.gif')
                with open(unknown_file, 'wb') as f:
                    f.write(img_bytes)
                print(f"\n未别的评级图片已保存: {unknown_file}")
                print(f"URL: {src}")
                print(f"Hash: {img_hash}")
                if self._rating_hash_map:
                    print("\n已知的评级图片:")
                    for h, r in self._rating_hash_map.items():
                        print(f"{h}: {r}星")
            
            return rating
            
        except Exception as e:
            print(f"解析评级图片失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return '' 

    def _get_portfolio(self, fcid: str) -> Dict[str, str]:
        """获取投资组合数据"""
        details = {}
        
        try:
            url = f'https://www.morningstar.cn/handler/quicktake.ashx?command=portfolio&fcid={fcid}'
            response = self._make_request('GET', url, headers=self.headers)
            data = response.json()
            
            # 债券品种占比映射
            bond_cat_map = {
                '1': '国家债券',
                '2': '央行票据',
                '3': '金融债券',
                '4': '企业债券',
                '5': '企业短期融资券',
                '6': '中期票据',
                '7': '可转债（可交换债）',
                '8': '公司债券',
                '9': '资产支持证券',
                '10': '同业存单',
                '11': '地方政府债',
                '11': '其他'
            }
            
            # 解析债券品种占比
            if 'BondCat' in data:
                for bond_cat in data['BondCat']:
                    cat_id = bond_cat.get('Id', '')
                    if cat_id in bond_cat_map:
                        cat_name = bond_cat_map[cat_id]
                        weight = bond_cat.get('NetAssetWeight', 0)
                        cat_weight = bond_cat.get('CatAvgWeight', 0)
                        
                        details[f'bond_{cat_id}_name'] = cat_name
                        details[f'bond_{cat_id}_weight'] = str(weight or '0')  # 如果weight为None或0，则设为'0'
                        details[f'bond_{cat_id}_cat_weight'] = str(cat_weight or '0')  # 如果cat_weight为None或0，则设为'0'
            
            # 解析资产配置
            asset_types = {
                'Cash': '现金',
                'Bond': '债券',
                'Stock': '股票',
                'Other': '其他'
            }
            
            for eng_name, chn_name in asset_types.items():
                weight = data.get(eng_name, 0)
                cat_weight = data.get(f'Cat{eng_name}', 0)
                details[f'asset_{eng_name.lower()}_weight'] = str(weight)
                details[f'asset_{eng_name.lower()}_cat_weight'] = str(cat_weight)
            
            # 解析前5大债券持仓
            if 'Top5BondHoldings' in data:
                for i, holding in enumerate(data['Top5BondHoldings'], 1):
                    details[f'top_bond_{i}_code'] = holding.get('Symbol', '')
                    details[f'top_bond_{i}_name'] = holding.get('HoldingName', '')
                    details[f'top_bond_{i}_weight'] = str(holding.get('Percent', 0))
        
        except Exception as e:
            print(f"获取投资组合数据失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
        
        return details
    
    def _get_manage(self, fcid: str) -> Dict[str, str]:
        """获取基金经理和管理信息"""
        details = {}
        
        try:
            url = f'https://www.morningstar.cn/handler/quicktake.ashx?command=manage&fcid={fcid}'
            response = self._make_request('GET', url, headers=self.headers)
            data = response.json()
            
            # 基本信息
            details['inception_date'] = data.get('InceptionDate', '')  # 成立日期
            details['profile'] = data.get('Profile', '')  # 基金简介
            details['custodian'] = data.get('CustodianName', '')  # 托管人
            details['company'] = data.get('CompanyName', '')  # 基金公司
            details['company_tel'] = data.get('Tel', '')  # 公司电话
            details['company_address'] = data.get('Address', '')  # 公司地址
            
            # 获取当前基金经理信息
            if 'Managers' in data:
                current_managers = []
                max_duration = ''  # 最长任期
                
                for manager in data['Managers']:
                    management_range = manager.get('ManagementRange', '')
                    # 如果管理期限以'- '结尾，说明是当前基金经理
                    if management_range.endswith('- '):
                        duration = manager.get('ManagementTime', '')
                        manager_info = {
                            'name': manager.get('ManagerName', ''),
                            'id': manager.get('ManagerId', ''),
                            'start_date': management_range.split(' - ')[0],
                            'duration': duration,
                            'resume': manager.get('Resume', '').strip()
                        }
                        current_managers.append(manager_info)
                        
                        # 更新最长任期
                        if duration:
                            # 解析任期时间（格式如："3年83天"）
                            years = 0
                            days = 0
                            if '年' in duration:
                                years = int(duration.split('年')[0])
                                if '天' in duration:
                                    days = int(duration.split('年')[1].split('天')[0])
                            elif '天' in duration:
                                days = int(duration.split('天')[0])
                            
                            # 比较任期长度
                            if not max_duration:
                                max_duration = duration
                            else:
                                curr_years = 0
                                curr_days = 0
                                if '年' in max_duration:
                                    curr_years = int(max_duration.split('年')[0])
                                    if '天' in max_duration:
                                        curr_days = int(max_duration.split('年')[1].split('天')[0])
                                elif '天' in max_duration:
                                    curr_days = int(max_duration.split('天')[0])
                                
                                # 比较总天数
                                if (years * 365 + days) > (curr_years * 365 + curr_days):
                                    max_duration = duration
                
                # 更新基金经理信息
                if current_managers:
                    details['manager_count'] = str(len(current_managers))
                    details['longest_tenure'] = max_duration  # 添加最长任期字段
                    for i, manager in enumerate(current_managers, 1):
                        details[f'manager_{i}_name'] = manager['name']
                        details[f'manager_{i}_id'] = manager['id']
                        details[f'manager_{i}_start_date'] = manager['start_date']
                        details[f'manager_{i}_duration'] = manager['duration']
                        details[f'manager_{i}_resume'] = manager['resume']
        
        except Exception as e:
            print(f"获取基金管理信息失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
        
        return details
    
    def _calculate_config_hash(self) -> str:
        """计算配置的hash值"""
        try:
            # 将配置转换为JSON字符串
            config_str = json.dumps(self.config, sort_keys=True)
            # 计算MD5
            return hashlib.md5(config_str.encode()).hexdigest()[:8]
        except Exception as e:
            print(f"计算配置hash失败: {str(e)}")
            return "default"
    
    def export_to_cache(self) -> str:
        """
        将数据导出到缓存目录
        
        Returns:
            str: 缓存文件路径
        """
        if not self.fund_data:
            print("没有数据可导出到缓存")
            return ""
        
        try:
            # 确保缓存目录存在
            os.makedirs(self.cache_dir, exist_ok=True)
            
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d")
            filename = f"morningstar_funds_{self.config_hash}_{timestamp}.xlsx"
            cache_path = os.path.join(self.cache_dir, filename)
            
            # 导出数据
            with pd.ExcelWriter(cache_path, engine='openpyxl') as writer:
                # 转换为DataFrame
                df = pd.DataFrame(self.fund_data)
                # 导出到Excel
                df.to_excel(writer, sheet_name='基金列表', index=False)

            print(f"\n数据已缓存到: {cache_path}")
            print(f"总记录数: {len(df)}")
            print(f"配置Hash: {self.config_hash}")
            
            return cache_path
            
        except Exception as e:
            print(f"导出缓存失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return ""
    
    