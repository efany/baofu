import sys
import os
from typing import Dict, Any, List
from datetime import datetime, date, timedelta
from loguru import logger
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_crawlers.akshare_forex_history_task import AKShareETFForexTask
from database.db_forex_day_hist import DBForexDayHist
from database.mysql_database import MySQLDatabase
from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError

class UpdateForexTask(BaseTask):
    """外汇历史数据更新任务"""
    
    def __init__(self, mysql_db: MySQLDatabase, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - symbols: 外汇代码列表
                - start_date: 开始日期，格式：YYYY-MM-DD（可选）
                - end_date: 结束日期，格式：YYYY-MM-DD（可选）
        """
        super().__init__(task_config)
        
        # 验证task_config
        if 'symbols' not in self.task_config:
            raise TaskConfigError("task_config必须包含symbols字段")
        if not isinstance(self.task_config['symbols'], list):
            raise TaskConfigError("symbols必须是列表类型")
        if not self.task_config['symbols']:
            raise TaskConfigError("symbols不能为空")
            
        self.symbols = self.task_config['symbols']
        self.start_date = self.task_config.get('start_date')
        self.end_date = self.task_config.get('end_date')
        
        # 初始化数据库连接
        self.mysql_db = mysql_db
        self.db_forex_hist = DBForexDayHist(self.mysql_db)
        
    def update_forex_data(self, symbol: str, start_date: date = None, end_date: date = None) -> bool:
        """
        更新单个外汇的历史数据
        
        Args:
            symbol: 外汇代码
            
        Returns:
            bool: 更新是否成功
        """
        try:
            # 创建爬虫任务配置
            crawler_config = {
                'name': 'update_forex_hist',
                'description': f'更新外汇{symbol}历史数据',
                'symbol': symbol,
                'start_date': start_date,
                'end_date': end_date
            }
            
            # 执行爬虫任务
            crawler = AKShareETFForexTask(crawler_config)
            crawler.execute()
            
            if not crawler.is_success:
                logger.error(f"获取外汇{symbol}历史数据失败: {crawler.error}")
                return False
                
            # 获取历史数据
            hist_data = crawler.task_result.get('hist_data', [])
            if not hist_data:
                logger.warning(f"外汇{symbol}没有历史数据")
                return True
            
            # 获取要插入的日期列表
            dates_to_insert = [data['date'] for data in hist_data]
            
            # 删除这些日期的旧数据
            self.db_forex_hist.delete_forex_hist_data(
                symbol=symbol,
                start_date=min(dates_to_insert),
                end_date=max(dates_to_insert)
            )
            
            # 批量插入数据
            success = self.db_forex_hist.batch_insert_forex_hist_data(hist_data)
            if success:
                logger.success(f"成功更新外汇{symbol}的历史数据，共{len(hist_data)}条记录")
            else:
                logger.error(f"更新外汇{symbol}的历史数据失败")
                
            return success
            
        except Exception as e:
            logger.error(f"更新外汇{symbol}历史数据时发生错误: {str(e)}")
            return False
            
    def run(self) -> None:
        """执行更新任务"""
        if not self.symbols:
            logger.warning("没有指定要更新的外汇代码")
            return
            
        success_count = 0
        failed_symbols = []
        
        for symbol in self.symbols:
            logger.info(f"开始更新外汇{symbol}的历史数据...")
            # 获取数据库中该外汇的最后数据日期
            start_date = self.start_date
            end_date = self.end_date
            last_date = self.db_forex_hist.get_last_forex_hist_date(symbol)
            
            # 如果数据库中有数据且start_date小于最后日期
            if last_date and (start_date is None or datetime.strptime(start_date, '%Y-%m-%d').date() < last_date):
                # 更新start_date为最后日期的十日前
                new_start_date = (pd.to_datetime(last_date) - timedelta(days=10)).strftime('%Y-%m-%d')
                logger.info(f"调整{symbol}的start_date从{start_date}到{new_start_date}")
                start_date = new_start_date

            if self.update_forex_data(symbol, start_date, end_date):
                success_count += 1
            else:
                failed_symbols.append(symbol)
                
        # 输出更新结果
        logger.info(f"更新完成，成功: {success_count}个，失败: {len(failed_symbols)}个")
        if failed_symbols:
            logger.warning(f"以下外汇更新失败: {failed_symbols}")
            
    def close(self):
        """关闭数据库连接"""
        self.mysql_db.close_connection()


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
        "name": "update_forex",
        "description": "更新外汇历史数据",
        "symbols": ["USDCNH", "USDJPY", "USDCHF"]
    }
    
    # 创建并执行任务
    task = UpdateForexTask(mysql_db, task_config)
    try:
        task.execute()
    finally:
        task.close()


if __name__ == "__main__":
    main() 