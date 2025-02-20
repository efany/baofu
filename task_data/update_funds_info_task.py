import sys
import os
from typing import Dict, Any, List
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase
from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError
from task_crawlers.eastmoney_task import EastmoneyTask

class UpdateFundsInfoTask(BaseTask):
    """更新基金信息的任务"""
    
    def __init__(self, task_config=None):
        if task_config is None:
            task_config = {
                "name": "update_funds_info",
                "description": "更新基金基础信息"
            }
        super().__init__(task_config)

    def get_fund_codes(self) -> List[str]:
        """
        获取需要更新的基金代码列表
        
        Returns:
            List[str]: 基金代码列表
        """
        # 从数据库获取所有基金代码
        
        return ['003376']

    def update_fund_info(self, fund_code: str) -> Dict[str, Any]:
        """
        更新单个基金的信息
        
        Args:
            fund_code: 基金代码
            
        Returns:
            Dict[str, Any]: 更新后的基金信息
        """
        # 构建天天基金URL
        url = f"https://fund.eastmoney.com/{fund_code}.html"
        
        # 创建爬虫任务配置
        crawler_config = {
            "name": "eastmoney_fund",
            "description": f"爬取基金{fund_code}信息",
            "fund_code": fund_code,
            "url": url
        }
        
        # 执行爬虫任务
        crawler = EastmoneyTask(crawler_config)
        crawler.execute()
        
        if not crawler.is_success:
            raise TaskExecutionError(f"爬取基金{fund_code}信息失败: {crawler.error}")

        return crawler.result

    def update_funds_database(self, mysql_db: MySQLDatabase, fund_info: Dict[str, Any]) -> None:
        """
        更新数据库中的基金信息
        
        Args:
            fund_info: 基金信息字典
        """
        fund_code = fund_info['fund_code']

        logger.debug(f"更新基金{fund_info['fund_code']}信息")
        for key, value in fund_info.items():
            logger.debug(f"{key}: {value}")

    def run(self) -> None:
        """执行更新基金信息的任务"""
        mysql_db = MySQLDatabase(
            host='127.0.0.1',
            user='baofu',
            password='TYeKmJPfw2b7kxGK',
            database='baofu'
        )
        try:
            # 获取需要更新的基金代码列表
            fund_codes = self.get_fund_codes()
            logger.info(f"开始更新{len(fund_codes)}个基金的信息")
            
            # 遍历更新每个基金的信息
            for fund_code in fund_codes:
                try:
                    logger.debug(f"开始更新基金{fund_code}信息")
                    fund_info = self.update_fund_info(fund_code)
                    self.update_funds_database(mysql_db, fund_info)
                except Exception as e:
                    logger.error(f"更新基金{fund_code}信息失败: {str(e)}")
                    continue
                    
            logger.success("基金信息更新任务完成")
            
        except Exception as e:
            raise TaskExecutionError(f"更新基金信息任务失败: {str(e)}")

if __name__ == "__main__":
    task = UpdateFundsInfoTask()
    task.execute()
    if not task.is_success:
        logger.error(task.error) 