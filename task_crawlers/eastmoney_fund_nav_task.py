import sys
import os
import re
import json
import time
import requests
from bs4 import BeautifulSoup
from typing import Dict, Any, List, Optional
from datetime import datetime, date
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'task'))
from base_task import BaseTask
from exceptions import TaskConfigError, TaskExecutionError, TaskDataError

class EastMoneyFundNavTask(BaseTask):
    """
    东方财富基金净值数据爬取任务
    """
    
    def __init__(self, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - fund_code: 基金代码
                - start_date: 开始日期，格式：'YYYY-MM-DD'
                - per_page: 每页条数（可选，默认40）
        """
        super().__init__(task_config)
        self.fund_code = task_config['fund_code']
        self.per_page = task_config.get('per_page', 40)
        
        # 处理开始日期
        start_date = task_config.get('start_date')
        if start_date:
            if isinstance(start_date, str):
                self.start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            elif isinstance(start_date, (datetime, date)):
                self.start_date = start_date.date() if isinstance(start_date, datetime) else start_date
            else:
                raise ValueError("start_date must be a string 'YYYY-MM-DD' or datetime/date object")
        else:
            self.start_date = None
        logger.info(f"fund_code: {self.fund_code}, start_date: {self.start_date}")
            
        self.base_url = "https://fundf10.eastmoney.com/F10DataApi.aspx"
    
    # var apidata={ content:"
    # <table class='w782 comm lsjz'>
    #     <thead>
    #         <tr>
    #             <th class='first'>净值日期</th>
    #             <th>单位净值</th>
    #             <th>累计净值</th>
    #             <th>日增长率</th>
    #             <th>申购状态</th>
    #             <th>赎回状态</th>
    #             <th class='tor last'>分红送配</th>
    #         </tr>
    #     </thead>
    #     <tbody>
    #         <tr>
    #             <td>2025-02-21</td>
    #             <td class='tor bold'>1.3326</td>
    #             <td class='tor bold'>1.4514</td>
    #             <td class='tor bold grn'>-0.31%</td>
    #             <td>开放申购</td>
    #             <td>开放赎回</td>
    #             <td class='red unbold'></td>
    #         </tr>
    #         <tr>
    #             <td>2025-02-20</td>
    #             <td class='tor bold'>1.3367</td>
    #             <td class='tor bold'>1.4555</td>
    #             <td class='tor bold grn'>-0.28%</td>
    #             <td>开放申购</td>
    #             <td>开放赎回</td>
    #             <td class='red unbold'></td>
    #         </tr>
    #         <tr>
    #             <td>2025-02-19</td>
    #             <td class='tor bold'>1.3404</td>
    #             <td class='tor bold'>1.4592</td>
    #             <td class='tor bold red'>0.16%</td>
    #             <td>开放申购</td>
    #             <td>开放赎回</td>
    #             <td class='red unbold'></td>
    #         </tr>
    #         <tr>
    #             <td>2025-02-18</td>
    #             <td class='tor bold'>1.3383</td>
    #             <td class='tor bold'>1.4571</td>
    #             <td class='tor bold grn'>-0.14%</td>
    #             <td>开放申购</td>
    #             <td>开放赎回</td>
    #             <td class='red unbold'></td>
    #         </tr>
    #         <tr>
    #             <td>2025-02-17</td>
    #             <td class='tor bold'>1.3402</td>
    #             <td class='tor bold'>1.4590</td>
    #             <td class='tor bold grn'>-0.30%</td>
    #             <td>开放申购</td>
    #             <td>开放赎回</td>
    #             <td class='red unbold'></td>
    #         </tr>
    #         <tr>
    #             <td>2025-02-14</td>
    #             <td class='tor bold'>1.3443</td>
    #             <td class='tor bold'>1.4631</td>
    #             <td class='tor bold grn'>-0.20%</td>
    #             <td>开放申购</td>
    #             <td>开放赎回</td>
    #             <td class='red unbold'></td>
    #         </tr>
    #         <tr>
    #             <td>2025-02-13</td>
    #             <td class='tor bold'>1.3470</td>
    #             <td class='tor bold'>1.4658</td>
    #             <td class='tor bold grn'>-0.02%</td>
    #             <td>开放申购</td>
    #             <td>开放赎回</td>
    #             <td class='red unbold'></td>
    #         </tr>
    #         <tr>
    #             <td>2025-02-12</td>
    #             <td class='tor bold'>1.3473</td>
    #             <td class='tor bold'>1.4661</td>
    #             <td class='tor bold grn'>-0.05%</td>
    #             <td>开放申购</td>
    #             <td>开放赎回</td>
    #             <td class='red unbold'></td>
    #         </tr>
    #         <tr>
    #             <td>2025-02-11</td>
    #             <td class='tor bold'>1.3480</td>
    #             <td class='tor bold'>1.4668</td>
    #             <td class='tor bold red'>0.08%</td>
    #             <td>开放申购</td>
    #             <td>开放赎回</td>
    #             <td class='red unbold'></td>
    #         </tr>
    #         <tr>
    #             <td>2025-02-10</td>
    #             <td class='tor bold'>1.3469</td>
    #             <td class='tor bold'>1.4657</td>
    #             <td class='tor bold grn'>-0.21%</td>
    #             <td>开放申购</td>
    #             <td>开放赎回</td>
    #             <td class='red unbold'></td>
    #         </tr>
    #     </tbody>
    # </table>
    # ",records:2047,pages:205,curpage:1};

    def _parse_api_data(self, response_text: str) -> Dict[str, Any]:
        """
        解析API返回的数据文本
        
        Args:
            response_text: API返回的文本内容
            
        Returns:
            Dict包含:
            - nav_data: 净值数据列表
            - pages: 总页数
            - cur_page: 当前页码
            - records: 总记录数
        """
        try:
            # 提取JSON格式的数据部分
            data_start = response_text.find('{')
            data_end = response_text.rfind('}') + 1
            if data_start == -1 or data_end == 0:
                raise ValueError("无法找到有效的数据内容")

            data_text = response_text[data_start:data_end]
    
            # 解析字段部分
            # 将JavaScript对象格式转换为标准JSON格式
            data_text = data_text.replace("content:", '"content":')
            data_text = data_text.replace("records:", '"records":')
            data_text = data_text.replace("pages:", '"pages":')
            data_text = data_text.replace("curpage:", '"curpage":')
            
            # 解析JSON数据
            data = json.loads(data_text)
            content = data['content']
            records = data['records']
            pages = data['pages'] 
            curpage = data['curpage']
            
            # 解析表格数据
            soup = BeautifulSoup(content, 'html.parser')
            table = soup.find('table')
            if not table:
                return {'nav_data': [], 'pages': 0, 'cur_page': 1, 'records': 0}
                
            nav_data = []
            rows = table.find_all('tr')[1:]  # 跳过表头
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 7:
                    nav_data.append({
                        'nav_date': cells[0].get_text(strip=True),
                        'nav': cells[1].get_text(strip=True),
                        'acc_nav': cells[2].get_text(strip=True),
                        'growth_rate': cells[3].get_text(strip=True).replace('%', ''),
                        'buy_status': cells[4].get_text(strip=True),
                        'sell_status': cells[5].get_text(strip=True),
                        'dividend': cells[6].get_text(strip=True).split('每份派现金')[-1].strip('元')
                    })
            
            return {
                'nav_data': nav_data,
                'pages': int(pages),
                'cur_page': int(curpage),
                'records': int(records)
            }
            
        except Exception as e:
            raise ValueError(f"解析数据失败: {str(e)}")
        

    def _fetch_page(self, page: int) -> Optional[Dict[str, Any]]:
        """
        获取指定页码的数据
        
        Args:
            page: 页码
            
        Returns:
            解析后的数据字典，如果没有更多数据则返回None
        """
        params = {
            'type': 'lsjz',
            'code': self.fund_code,
            'page': page,
            'per': self.per_page
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return self._parse_api_data(response.text)
        except requests.RequestException as e:
            raise Exception(f"请求失败: {str(e)}")
        except (ValueError, KeyError, IndexError) as e:
            raise Exception(f"数据解析失败: {str(e)}")
    
    def run(self) -> None:
        """
        执行任务：分页获取基金净值数据，直到无数据或达到日期限制
        """
        all_nav_data = []
        current_page = 1
        reached_date_limit = False
        
        while not reached_date_limit:
            # 获取当前页数据
            result = self._fetch_page(current_page)
            
            # 检查是否还有数据
            if not result or not result['nav_data']:
                break
                
            nav_data = result['nav_data']
            
            # 如果设置了开始日期，检查是否已经达到日期限制
            if self.start_date and nav_data:
                last_date = datetime.strptime(nav_data[-1]['nav_date'], '%Y-%m-%d').date()
                if last_date < self.start_date:
                    # 找到最后一个符合日期要求的数据的索引
                    valid_data = [item for item in nav_data if datetime.strptime(item['nav_date'], '%Y-%m-%d').date() >= self.start_date]
                    if valid_data:
                        all_nav_data.extend(valid_data)
                    reached_date_limit = True
                    break
            
            # 添加当前页的数据
            all_nav_data.extend(nav_data)
            
            # 检查是否还有下一页
            if result['pages'] and current_page >= result['pages']:
                break
                
            current_page += 1

            # 每页请求后暂停一下，避免请求过快
            time.sleep(0.5)
        
        # 保存所有获取到的数据
        self.task_result = {
            'nav_data': all_nav_data,
            'total_records': len(all_nav_data)
        }


if __name__ == "__main__":
    task_config = {
        "name": "eastmoney_fund_nav",
        "description": "获取基金净值数据",
        "fund_code": "008163",
        "start_date": "2024-01-01",  # 只获取2024年1月1日之后的数据
    }
    task = EastMoneyFundNavTask(task_config)
    task.execute()
    if not task.is_success:
        logger.error(f"Error: {task.error}")
    else:
        logger.info(f"获取到{len(task.result['nav_data'])}条净值数据")
        for nav_data in task.result['nav_data']:
            logger.info(f"nav_data: {nav_data}")    

