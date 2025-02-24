import sys
import os
from typing import Dict, Any
from bs4 import BeautifulSoup
import re
import requests
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'task'))
from base_task import BaseTask
from exceptions import TaskConfigError, TaskExecutionError, TaskDataError

"""
task_config 规范:
{
    "name": str,  # 任务名称
    "description": str,  # 任务描述
    "fund_code": str,  # 基金代码
    "url": str  # 爬取数据的URL
}
"""
class EastmoneyTask(BaseTask):
    """爬取天天基金网基金信息的任务"""
    
    def __init__(self, task_config=None):
        super().__init__(task_config)

    def fetch_url(self, url: str, timeout: int = 30) -> str:
        """
        获取URL内容的通用方法
        
        Args:
            url: 要获取的URL
            timeout: 超时时间（秒）
            
        Returns:
            str: 网页内容
            
        Raises:
            TaskExecutionError: 当网络请求失败时
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            # 1. 首先尝试从Content-Type头部获取编码
            if 'charset' in response.headers.get('content-type', '').lower():
                response.encoding = response.headers.get('content-type').split('charset=')[-1]
            # 2. 如果没有，则尝试从网页meta标签获取
            else:
                response.encoding = response.apparent_encoding
                
            return response.text
        except requests.exceptions.RequestException as e:
            raise TaskExecutionError(f"获取URL内容失败: {str(e)}")
        
    def parse_html(self, html: str) -> Dict[str, Any]:
        """解析HTML内容提取基金信息
        
        Args:
            html: 网页HTML内容
            
        Returns:
            包含基金信息的字典
        """
        logger.info("开始解析基金HTML内容")
        soup = BeautifulSoup(html, 'html.parser')
        
        # 提取基金基本信息
        result = {}

        result['fund_code'] = self.task_config['fund_code']
        
        # 基金名称
        fund_title = soup.find('div', {'class': 'fundDetail-tit'})
        if fund_title:
            title_text = fund_title.text.strip()
            name_match = re.search(r'(.*?)\(', title_text)
            if name_match:
                result['fund_name'] = name_match.group(1).strip()
                logger.debug(f"提取到基金名称: {result['fund_name']}")
            
        # 提取管理人
        # <td><span class="letterSpace01">管 理 人</span>：<a href="http://fund.eastmoney.com/company/80000248.html">广发基金</a></td>
        company = soup.find('span', class_='letterSpace01', text='管 理 人').find_parent('td').find('a')
        if company:
            result['fund_company'] = company.text.strip()
            logger.debug(f"提取到基金公司: {result['fund_company']}")
            
        logger.info(f"基金信息解析完成, 共提取到 {len(result)} 个字段")
        return result

    def run(self) -> None:
        # 校验task_config
        if not isinstance(self.task_config, dict):
            raise TaskConfigError("task_config必须是字典类型")
        
        required_keys = ["name", "description", "fund_code", "url"]
        for key in required_keys:
            if key not in self.task_config:
                raise TaskConfigError(f"task_config缺少必要的键: {key}")
        
        # 获取基金URL
        url = self.task_config["url"]
        if len(url) == 0:
            raise TaskConfigError("task_config中的url不能为空")
        
        try:
            logger.info(f"开始爬取基金信息: {url}")
            
            # 获取网页内容
            html = self.fetch_url(url)
            logger.debug("成功获取网页内容")
            
            # 解析网页内容并保存结果
            self.task_result = self.parse_html(html)
            
            logger.success(f"基金信息爬取完成: {self.task_result.get('fund_name', url)}")
            
        except Exception as e:
            if isinstance(e, TaskConfigError):
                raise
            elif "Connection" in str(e):
                raise TaskExecutionError(f"网络连接错误: {str(e)}")
            elif "解析" in str(e):
                raise TaskDataError(f"数据解析错误: {str(e)}")
            else:
                raise TaskExecutionError(f"爬取过程出错: {str(e)}")

if __name__ == "__main__": 
    task_config = {
        "name": "eastmoney_fund",
        "description": "爬取天天基金网基金信息",
        "fund_code": "003376",
        "url": "https://fund.eastmoney.com/003376.html"
    }
    task = EastmoneyTask(task_config)
    task.run()
    if task.is_success:
        for key, value in task.result.items():
            logger.debug(f"{key}: {value}")
    else:
        logger.error(task.error)