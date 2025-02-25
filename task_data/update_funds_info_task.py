import sys
import os
import time
from typing import Dict, Any, List
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase
from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError
from task_crawlers.eastmoney_fund_info_task import EastmoneyTask

class UpdateFundsInfoTask(BaseTask):
    """更新基金信息的任务"""
    
    def __init__(self, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - fund_codes: 基金代码列表
        """
        super().__init__(task_config)
        
        # 验证task_config
        if 'fund_codes' not in self.task_config:
            raise TaskConfigError("task_config必须包含fund_codes字段")
        if not isinstance(self.task_config['fund_codes'], list):
            raise TaskConfigError("fund_codes必须是列表类型")
        if not self.task_config['fund_codes']:
            raise TaskConfigError("fund_codes不能为空")

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

        sql_fund_info = mysql_db.fetch_data('funds', {'ts_code': fund_code})
        if sql_fund_info:
            logger.debug(f"基金{fund_code}已存在")
            mysql_db.update_data('funds', {'name': fund_info['fund_name'], 
                                           'management': fund_info['fund_company']}, 
                                           {'ts_code': fund_code})
        else:
            logger.debug(f"基金{fund_code}不存在")
            mysql_db.insert_data('funds', {'ts_code': fund_info['fund_code'], 
                                           'name': fund_info['fund_name'], 
                                           'management': fund_info['fund_company']})

    def run(self) -> None:
        """执行更新基金信息的任务"""
        
        mysql_db = MySQLDatabase(
            host='127.0.0.1',
            user='baofu',
            password='TYeKmJPfw2b7kxGK',
            database='baofu'
        )
        try:
            fund_codes = self.task_config['fund_codes']
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
            
                # 每次请求后暂停一下，避免请求过快
                time.sleep(0.5)
                    
            logger.success("基金信息更新任务完成")
            
        except Exception as e:
            raise TaskExecutionError(f"更新基金信息任务失败: {str(e)}")
        finally:
            # 确保在任务完成或出错时都能关闭数据库连接
            mysql_db.close_connection()
            logger.debug("数据库连接已关闭")


if __name__ == "__main__":
    task_config = {
        "name": "update_funds_info",
        "description": "更新基金基础信息",
        "fund_codes": ["008163","007540","003376","011062","400030","011983","007744","010353","006635","003156","162715","003547","003157","007745","485119","010232","000914","007828","006645","006484","006485","009560","008583"]  # 示例基金代码列表
    }
    task = UpdateFundsInfoTask(task_config)
    task.execute()
    if not task.is_success:
        logger.error(task.error) 



