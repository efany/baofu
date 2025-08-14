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
from database.db_funds import DBFunds


class UpdateFundsTask(BaseTask):
    """更新基金信息和净值数据的任务"""
    
    def __init__(self, task_config: Dict[str, Any], mysql_db=None):
        """
        初始化任务

        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - fund_codes: 可选，待更新的基金代码列表
                - update_all: 可选，是否更新所有已有基金，默认为False
                - start_date: 可选，净值数据的开始日期，格式：'YYYY-MM-DD'
                - host: 可选，MySQL主机地址
                - user: 可选，MySQL用户名
                - password: 可选，MySQL密码
                - database: 可选，MySQL数据库名
        """
        if mysql_db is None:
            # 从task_config获取数据库配置参数
            db_config = {
                'host': task_config.get('host', '127.0.0.1'),
                'user': task_config.get('user', 'baofu'),
                'password': task_config.get('password', 'TYeKmJPfw2b7kxGK'),
                'database': task_config.get('database', 'baofu')
            }
            self.mysql_db = MySQLDatabase(**db_config)
        else:
            self.mysql_db = mysql_db
        self.db_funds = DBFunds(self.mysql_db)
        super().__init__(task_config)

    def get_fund_codes(self) -> List[str]:
        """
        从数据库获取需要更新的基金代码列表
        
        Returns:
            List[str]: 基金代码列表
        """
        try:
            funds = self.db_funds.get_all_funds()
            fund_codes = funds['ts_code'].tolist()
            return fund_codes
            
        except Exception as e:
            logger.error(f"获取基金代码列表失败: {str(e)}")
            raise TaskExecutionError(f"获取基金代码列表失败: {str(e)}")

    def run(self) -> None:
        """执行更新基金数据的任务"""
        try:
            # 获取待更新的基金代码列表
            fund_codes = self.task_config.get('fund_codes', [])
            update_all = self.task_config.get('update_all', False)

            if update_all:
                logger.info("更新所有已有基金")
                fund_codes = self.get_fund_codes()
            elif not fund_codes:
                logger.warning("没有待更新的基金代码")
                return
            
            # 1. 更新基金基本信息
            info_task_config = {
                "name": "update_funds_info",
                "description": "更新基金基本信息",
                "fund_codes": fund_codes
            }
            
            logger.info("开始更新基金基本信息")
            info_task = UpdateFundsInfoTask(self.mysql_db, info_task_config)
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
            nav_task = UpdateFundsNavTask(self.mysql_db, nav_task_config)
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
        "description": "更新基金信息和净值数据",
        "update_all": False,  # 设置为True以更新所有基金
        "fund_codes": ["010232"],  # 可选，指定待更新的基金代码 ,
        "host": "113.44.90.2",
        "user": "baofu",
        "password": "TYeKmJPfw2b7kxGK",
        "database": "baofu",
    }
    task = UpdateFundsTask(task_config)
    task.execute()
    if not task.is_success:
        logger.error(task.error)
