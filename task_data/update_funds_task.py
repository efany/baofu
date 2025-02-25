import sys
import os
from typing import Dict, Any, List
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase
from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError
from task_data.update_funds_info_task import UpdateFundsInfoTask
from task_data.update_funds_nav_task import UpdateFundsNavTask

class UpdateFundsTask(BaseTask):
    """更新基金信息和净值数据的任务"""
    
    def __init__(self, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - start_date: 可选，净值数据的开始日期，格式：'YYYY-MM-DD'
        """
        super().__init__(task_config)
        
    def get_fund_codes(self) -> List[str]:
        """
        从数据库获取需要更新的基金代码列表
        
        Returns:
            List[str]: 基金代码列表
        """
        mysql_db = MySQLDatabase(
            host='127.0.0.1',
            user='baofu',
            password='TYeKmJPfw2b7kxGK',
            database='baofu'
        )
        try:
            # 查询所有基金代码
            sql = "SELECT ts_code FROM funds"
            result = mysql_db.fetch_data('funds')
            
            if not result:
                logger.warning("数据库中没有基金数据")
                return []
                
            fund_codes = [row['ts_code'] for row in result]
            logger.info(f"从数据库获取到{len(fund_codes)}个基金代码")
            return fund_codes
            
        except Exception as e:
            logger.error(f"获取基金代码列表失败: {str(e)}")
            raise TaskExecutionError(f"获取基金代码列表失败: {str(e)}")
        finally:
            mysql_db.close_connection()

    def run(self) -> None:
        """执行更新基金数据的任务"""
        try:
            # 获取基金代码列表
            fund_codes = self.get_fund_codes()
            if not fund_codes:
                logger.warning("没有需要更新的基金")
                return
                
            # 1. 更新基金基本信息
            info_task_config = {
                "name": "update_funds_info",
                "description": "更新基金基本信息",
                "fund_codes": fund_codes
            }
            
            logger.info("开始更新基金基本信息")
            info_task = UpdateFundsInfoTask(info_task_config)
            info_task.execute()
            
            if not info_task.is_success:
                logger.error(f"更新基金基本信息失败: {info_task.error}")
                raise TaskExecutionError(f"更新基金基本信息失败: {info_task.error}")
            
            # 2. 更新基金净值数据
            nav_task_config = {
                "name": "update_funds_nav",
                "description": "更新基金净值数据",
                "fund_codes": fund_codes
            }
            
            # 如果配置中指定了开始日期，添加到净值更新任务的配置中
            if 'start_date' in self.task_config:
                nav_task_config['start_date'] = self.task_config['start_date']
            
            logger.info("开始更新基金净值数据")
            nav_task = UpdateFundsNavTask(nav_task_config)
            nav_task.execute()
            
            if not nav_task.is_success:
                logger.error(f"更新基金净值数据失败: {nav_task.error}")
                raise TaskExecutionError(f"更新基金净值数据失败: {nav_task.error}")
            
            logger.success("基金数据更新任务完成")
            
        except Exception as e:
            logger.error(f"更新基金数据失败: {str(e)}")
            raise TaskExecutionError(f"更新基金数据失败: {str(e)}")


if __name__ == "__main__":
    task_config = {
        "name": "update_funds",
        "description": "更新基金信息和净值数据"
    }
    task = UpdateFundsTask(task_config)
    task.execute()
    if not task.is_success:
        logger.error(task.error)
