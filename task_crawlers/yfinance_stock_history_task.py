from datetime import datetime
from typing import Optional
import yfinance as yf
from loguru import logger
import pandas as pd
import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task.base_task import BaseTask
from task.exceptions import TaskExecutionError

class YFinanceStockHistoryTask(BaseTask):
    """
    使用yfinance获取股票历史数据的任务类
    """
    
    def __init__(self, task_config=None):
        """
        初始化任务
        
        Args:
            task_config: 任务配置，包含以下字段：
                - stock_symbol: 股票代码
                - proxy: 代理服务器地址
                - start_date: 开始日期（可选）
                - end_date: 结束日期（可选）
        """
        super().__init__(task_config)
        self.stock_symbol = task_config['stock_symbol']
        self.proxy = task_config.get('proxy')
        self.start_date = task_config.get('start_date')
        self.end_date = task_config.get('end_date')
        
        logger.info(f"初始化任务，股票代码: {self.stock_symbol}, 代理: {self.proxy}, "
                   f"时间范围: {self.start_date} - {self.end_date}")
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
        执行任务，获取股票历史数据
        
        每次获取3个月的数据，如果指定了end_date，则从end_date开始往前推；
        如果指定了start_date，则获取到start_date为止；
        否则一直获取到没有数据为止。
        """
        try:
            ticker = yf.Ticker(self.stock_symbol)
            all_data = pd.DataFrame()
            
            # 确定初始结束日期
            current_end = pd.to_datetime(self.end_date) if self.end_date else pd.Timestamp.now()
            start_date = pd.to_datetime(self.start_date) if self.start_date else None
            
            while True:
                # 计算当前批次的开始日期（往前推3个月）
                current_start = current_end - pd.DateOffset(months=3)
                
                # 如果有start_date且current_start小于start_date，则调整current_start
                if start_date and current_start < start_date:
                    current_start = start_date
                
                # 获取当前批次的数据
                logger.info(f"获取{self.stock_symbol}从{current_start.strftime('%Y-%m-%d')}到{current_end.strftime('%Y-%m-%d')}的数据")
                df = ticker.history(start=current_start, end=current_end, actions=True)
                logger.info(f"获取到的数据长度: {len(df)}")

                # df_actions = ticker.actions
                # logger.info(f"获取到的分红数据:\n{df_actions}")
                
                # 如果没有获取到数据，退出循环
                if df.empty:
                    break
                
                # 合并数据
                all_data = pd.concat([df, all_data])
                
                # 如果已经达到start_date或者获取的数据量小于预期，退出循环
                if (start_date and current_start <= start_date):
                    break

                # 更新结束日期为当前开始日期
                current_end = current_start
    
                # 每页请求后暂停一下，避免请求过快
                time.sleep(0.5)
                
            if not all_data.empty:
                # 去除重复数据
                all_data = all_data.reset_index()
                all_data = all_data.drop_duplicates(subset=['Date'])
                all_data = all_data.sort_values('Date')

                # 格式化日期
                all_data['Date'] = all_data['Date'].dt.strftime('%Y-%m-%d')

                # 重命名列名
                all_data.columns = [col.lower() for col in all_data.columns]
                all_data = all_data.rename(columns={
                    'stock splits': 'stock_splits'
                })
                
                # 添加股票代码列
                all_data['symbol'] = self.stock_symbol
                
                self.task_result = {
                    'hist_data': all_data.to_dict('records'),
                    'total_records': len(all_data)
                }
                logger.info(f"成功获取{self.stock_symbol}的历史数据，共{len(all_data)}条记录")

            else:
                return {
                    'hist_data': [],
                    'total_records': 0
                }
                
        except Exception as e:
            logger.error(f"获取{self.stock_symbol}的历史数据时发生错误: {str(e)}")
            self._error = e
            raise TaskExecutionError(f"获取{self.stock_symbol}的历史数据时发生错误: {str(e)}")

def main():
    """
    主函数，用于测试任务功能
    """
    task_config = {
        "name": "yfinance_stock_history",
        "description": "爬取yfinance股票历史数据",
        "stock_symbol": "515220.SS",
        "proxy": "http://127.0.0.1:7890",
        "start_date": "2024-03-01",
        "end_date": "2024-05-01"
    }
    
    task = YFinanceStockHistoryTask(task_config)
    task.execute()
    if task.is_success:
        logger.info(f"历史数据前5条:\n{task.result['hist_data'][:5]}")
    else:
        logger.error("获取历史数据失败")

if __name__ == '__main__':
    main() 