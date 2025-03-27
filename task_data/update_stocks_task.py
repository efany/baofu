import sys
import os
from typing import Dict, Any, List
from loguru import logger
from datetime import datetime, date

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase
from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError
from task_data.update_stocks_info_task import UpdateStocksInfoTask
from task_data.update_stocks_day_hist_task import UpdateStocksDayHistTask

class UpdateStocksTask(BaseTask):
    """更新股票数据的任务，包括基本信息和历史数据"""
    
    def __init__(self, mysql_db: MySQLDatabase, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - stock_symbols: 股票代码列表
                - start_date: 开始日期（可选）
                - end_date: 结束日期（可选）
                - proxy: 代理服务器地址（可选）
                - update_info: 是否更新基本信息（可选，默认True）
                - update_hist: 是否更新历史数据（可选，默认True）
        """
        super().__init__(task_config)
        self.mysql_db = mysql_db
        
        # 验证task_config
        if 'stock_symbols' not in self.task_config:
            raise TaskConfigError("task_config必须包含stock_symbols字段")
        if not isinstance(self.task_config['stock_symbols'], list):
            raise TaskConfigError("stock_symbols必须是列表类型")
        if not self.task_config['stock_symbols']:
            raise TaskConfigError("stock_symbols不能为空")

    def run(self) -> None:
        """执行更新股票数据的任务"""
        try:
            update_info = self.task_config.get('update_info', True)
            update_hist = self.task_config.get('update_hist', True)
            
            # 更新股票基本信息
            if update_info:
                logger.info("开始更新股票基本信息...")
                info_task_config = {
                    "name": "update_stocks_info",
                    "description": "更新股票基础信息",
                    "stock_symbols": self.task_config['stock_symbols'],
                    "proxy": self.task_config.get('proxy')
                }
                
                info_task = UpdateStocksInfoTask(self.mysql_db, info_task_config)
                info_task.execute()
                
                if not info_task.is_success:
                    logger.error(f"更新股票基本信息失败: {info_task.error}")
                else:
                    logger.success("股票基本信息更新完成")
            
            # 更新股票历史数据
            if update_hist:
                logger.info("开始更新股票历史数据...")
                hist_task_config = {
                    "name": "update_stocks_day_hist",
                    "description": "更新股票日线历史数据",
                    "stock_symbols": self.task_config['stock_symbols'],
                    "start_date": self.task_config.get('start_date'),
                    "end_date": self.task_config.get('end_date'),
                    "proxy": self.task_config.get('proxy')
                }
                logger.info(hist_task_config)
                
                hist_task = UpdateStocksDayHistTask(self.mysql_db, hist_task_config)
                hist_task.execute()
                
                if not hist_task.is_success:
                    logger.error(f"更新股票历史数据失败: {hist_task.error}")
                else:
                    logger.success("股票历史数据更新完成")
            
            if not update_info and not update_hist:
                logger.warning("未指定需要更新的数据类型")
                
        except Exception as e:
            raise TaskExecutionError(f"更新股票数据任务失败: {str(e)}")

def main():
    """主函数，用于测试"""
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )
    
    # 测试配置
    task_config = {
        "name": "update_stocks",
        "description": "更新股票数据",
        "stock_symbols": ["159949.SZ", "512550.SS", "159633.SZ", "159628.SZ"],  # 示例股票代码列表
        "proxy": "http://127.0.0.1:7890",  # 代理设置
        "update_info": True,  # 更新基本信息
        "update_hist": True,  # 更新历史数据
    }
    
    # 执行任务
    task = UpdateStocksTask(mysql_db, task_config)
    task.execute()
    
    if task.is_success:
        logger.success("股票数据更新任务完成")
    else:
        logger.error(f"股票数据更新任务失败: {task.error}")

if __name__ == "__main__":
    main() 