from datetime import datetime
from typing import Dict, Any, Optional
import yfinance as yf
from loguru import logger
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task.base_task import BaseTask
from task.exceptions import TaskExecutionError

class YFinanceStockInfoTask(BaseTask):
    """
    使用yfinance获取股票基本信息的任务类
    """
    
    def __init__(self, task_config=None):
        """
        初始化任务
        
        Args:
            stock_symbol: 股票代码，例如 'AAPL' 表示苹果公司
            proxy: 代理服务器地址，默认为本地代理
        """
        super().__init__(task_config)
        self.stock_symbol = task_config['stock_symbol']
        self.proxy = task_config['proxy']
        logger.info(f"初始化任务，股票代码: {self.stock_symbol}, 代理: {self.proxy}")
        self._proxy()
        
    def _proxy(self):
        """
        设置代理
        """
        if self.proxy:
            os.environ['HTTP_PROXY'] = self.proxy
            os.environ['HTTPS_PROXY'] = self.proxy
            
    def run(self) -> None:
        """
        执行任务，获取股票信息
        """
        try:
            ticker = yf.Ticker(self.stock_symbol)
            info = ticker.info
            
            # 提取关键信息
            self.task_result = {
                'symbol': self.stock_symbol,
                'name': info.get('longName', ''),
                'currency': info.get('currency', ''),
                'exchange': info.get('exchange', ''),
                'market': info.get('market', '')
            }
            logger.info(f"成功获取{self.stock_symbol}的基本信息")
        except Exception as e:
            logger.error(f"获取{self.stock_symbol}的基本信息时发生错误: {str(e)}")
            self._error = e
            raise TaskExecutionError(f"获取{self.stock_symbol}的基本信息时发生错误: {str(e)}")

def main():
    """
    主函数，用于测试任务功能
    """
    task_config = {
        "name": "yfinance_stock_info",
        "description": "爬取yfinance股票信息",
        "stock_symbol": "512550.SS",
        "proxy": "http://127.0.0.1:7890"
    }
    
    task = YFinanceStockInfoTask(task_config)
    task.execute()
    if task.is_success:
        logger.info(f"股票基本信息: {task.result}")
    else:
        logger.error("获取股票信息失败")

if __name__ == '__main__':
    main() 