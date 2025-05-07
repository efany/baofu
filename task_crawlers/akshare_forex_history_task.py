import akshare as ak
from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
from typing import Optional, Dict, Any
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError

class AKShareETFForexTask(BaseTask):
    """AKShare外汇历史数据爬取任务"""
    
    def __init__(self, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 任务配置字典，包含以下字段：
                - symbol: 外汇代码，例如 'USDJPY'
                - start_date: 开始日期，格式为 'YYYY-MM-DD'，默认为30天前
                - end_date: 结束日期，格式为 'YYYY-MM-DD'，默认为今天
        """
        super().__init__(task_config)
        self.symbol = task_config.get('symbol')
        if not self.symbol:
            raise TaskConfigError("symbol不能为空")
        
        self.start_date = None
        self.end_date = None
        if task_config.get('start_date'):
            self.start_date = task_config.get('start_date')
        
        if task_config.get('end_date'):
            self.end_date = task_config.get('end_date')
        
    def run(self) -> Dict[str, Any]:
        """
        执行爬取任务
        
        Returns:
            Dict[str, Any]: 包含任务执行结果的字典
        """
        try:
            logger.info(f"开始爬取 {self.symbol} 从 {self.start_date} 到 {self.end_date} 的历史数据")
            
            # 调用AKShare接口获取数据
            df = ak.forex_hist_em(symbol=self.symbol)
            
            if df.empty:
                logger.warning(f"未获取到 {self.symbol} 的历史数据")
                return {
                    'status': 'warning',
                    'message': f"未获取到 {self.symbol} 的历史数据",
                    'data': None
                }
            
            # 数据预处理
            df = self._preprocess_data(df)
            
            # 按日期范围过滤
            if self.start_date and self.end_date:   
                df = df[(df['date'] >= self.start_date) & (df['date'] <= self.end_date)]
            elif self.start_date:
                df = df[df['date'] >= self.start_date]
            elif self.end_date:
                df = df[df['date'] <= self.end_date]
            
            logger.info(f"成功爬取 {self.symbol} 的历史数据，共 {len(df)} 条记录")
            
            self.task_result = {
                'hist_data': df.to_dict('records'),
                'total_records': len(df)
            }
            
        except Exception as e:
            logger.error(f"爬取 {self.symbol} 历史数据时发生错误: {str(e)}")
            return {
                'status': 'error',
                'message': f"爬取 {self.symbol} 历史数据时发生错误: {str(e)}",
                'data': None
            }
    
    def _preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据预处理
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            pd.DataFrame: 处理后的DataFrame
        """
        # 重命名列
        df = df.rename(columns={
            '日期': 'date',
            '代码': 'symbol',
            '名称': 'name',
            '今开': 'open',
            '最新价': 'close',
            '最高': 'high',
            '最低': 'low',
            '振幅': 'change_pct'
        })
        
        # 转换日期格式
        df['date'] = pd.to_datetime(df['date'])
        
        # 转换数值类型
        numeric_columns = ['open', 'close', 'high', 'low', 'change_pct']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 添加symbol列
        df['symbol'] = self.symbol
        
        return df

if __name__ == '__main__':
    # 测试代码
    task_config = { 
        'symbol': 'USDJPY',
        'start_date': '2024-02-01',
        'end_date': '2024-02-21'
    }
    task = AKShareETFForexTask(task_config)
    task.run()
    logger.info(task.task_result)