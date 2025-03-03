import sys
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase
from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError
from task_crawlers.eastmoney_fund_nav_task import EastMoneyFundNavTask
from database.db_funds_nav import DBFundsNav
class UpdateFundsNavTask(BaseTask):
    """更新基金净值数据的任务"""
    
    def __init__(self, mysql_db: MySQLDatabase, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - fund_codes: 基金代码列表
        """
        super().__init__(task_config)
        self.mysql_db = mysql_db
        self.db_funds_nav = DBFundsNav(self.mysql_db)
        
        # 验证task_config
        if 'fund_codes' not in self.task_config:
            raise TaskConfigError("task_config必须包含fund_codes字段")
        if not isinstance(self.task_config['fund_codes'], list):
            raise TaskConfigError("fund_codes必须是列表类型")
        if not self.task_config['fund_codes']:
            raise TaskConfigError("fund_codes不能为空")

    def fetch_latest_nav_date(self, fund_code: str) -> Optional[datetime.date]:
        """
        获取数据库中基金的最新净值日期
        
        Args:
            fund_code: 基金代码
            
        Returns:
            Optional[datetime.date]: 最新净值日期，如果没有数据则返回None
        """
        try:
            sql = """
                SELECT nav_date 
                FROM funds_nav 
                WHERE ts_code = %(ts_code)s 
                ORDER BY nav_date DESC 
                LIMIT 1
            """
            result = self.mysql_db.execute_query(sql, {'ts_code': fund_code})
            
            if len(result) > 0:
                latest_date = result[0]['nav_date']
                logger.debug(f"基金{fund_code}的最新净值日期为: {latest_date}")
                return latest_date
            else:
                logger.debug(f"基金{fund_code}在数据库中没有净值数据")
                return None
            
        except Exception as e:
            logger.error(f"获取基金{fund_code}最新净值日期失败: {str(e)}")
            return None

    def crawle_fund_nav(self, fund_code: str, start_date: Optional[datetime.date] = None) -> Dict[str, Any]:
        """
        更新单个基金的净值数据
        
        Args:
            fund_code: 基金代码
            start_date: 开始日期
            
        Returns:
            Dict[str, Any]: 更新后的基金净值数据
        """
        # 创建爬虫任务配置
        crawler_config = {
            "name": "eastmoney_fund_nav",
            "description": f"爬取基金{fund_code}净值数据",
            "fund_code": fund_code,
            "per_page": 40
        }
        
        # 如果有开始日期，添加到配置中
        if start_date:
            crawler_config['start_date'] = start_date.strftime('%Y-%m-%d')
            logger.debug(f"设置基金{fund_code}的净值获取起始日期为: {start_date}")
        
        # 执行爬虫任务
        crawler = EastMoneyFundNavTask(crawler_config)
        crawler.execute()
        
        if not crawler.is_success:
            raise TaskExecutionError(f"爬取基金{fund_code}净值数据失败: {crawler.error}")

        return crawler.result

    def update_nav_database(self, fund_code: str, nav_data: List[Dict[str, Any]]) -> None:
        """
        更新数据库中的基金净值数据
        
        Args:
            mysql_db: 数据库连接
            fund_code: 基金代码
            nav_data: 净值数据列表
        """
        for nav_item in nav_data:
            # 转换日期字符串为datetime对象
            nav_date = datetime.strptime(nav_item['nav_date'], '%Y-%m-%d').date()
            
            # 准备数据
            data = {
                'ts_code': fund_code,
                'nav_date': nav_date,
                'unit_nav': float(nav_item['nav']),
                'accum_nav': float(nav_item['acc_nav']),
                'dividend': nav_item['dividend'] if nav_item['dividend'] else None
            }
            
            # 检查是否已存在该日期的数据
            db_funds_nav = self.db_funds_nav.get_fund_nav_by_date(fund_code, nav_date)
            
            if not db_funds_nav.empty:
                # 更新现有数据
                self.db_funds_nav.update_fund_nav(fund_code, nav_date, data)
            else:
                # 插入新数据
                self.db_funds_nav.insert_fund_nav(data)

    def run(self) -> None:
        """执行更新基金净值数据的任务"""
        try:
            fund_codes = self.task_config['fund_codes']
            logger.info(f"开始更新{len(fund_codes)}个基金的净值数据")
            
            # 遍历更新每个基金的净值数据
            for fund_code in fund_codes:
                try:
                    logger.debug(f"开始更新基金{fund_code}净值数据")
                    
                    # 获取最新净值日期
                    latest_date = self.fetch_latest_nav_date(fund_code)
                    if latest_date:
                        # 如果有历史数据，从最新日期的下一天开始获取
                        start_date = latest_date + timedelta(days=1)
                        logger.info(f"基金{fund_code}从{start_date}开始更新净值数据")
                    else:
                        start_date = None
                        logger.info(f"基金{fund_code}没有历史净值数据，将获取全部数据")
                    
                    nav_result = self.crawle_fund_nav(fund_code, start_date)
                    if nav_result['nav_data']:
                        self.update_nav_database(fund_code, nav_result['nav_data'])
                        logger.info(f"基金{fund_code}更新了{len(nav_result['nav_data'])}条净值数据")
                    else:
                        logger.info(f"基金{fund_code}没有新的净值数据需要更新")
                    
                except Exception as e:
                    logger.error(f"更新基金{fund_code}净值数据失败: {str(e)}")
                    continue
                    
            logger.success("基金净值数据更新任务完成")
            
        except Exception as e:
            raise TaskExecutionError(f"更新基金净值数据任务失败: {str(e)}")


if __name__ == "__main__":
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )
    
    task_config = {
        "name": "update_funds_nav",
        "description": "更新基金净值数据",
        "fund_codes": ["006484"]  # 示例基金代码列表
    }
    task = UpdateFundsNavTask(mysql_db, task_config)
    task.execute()
    if not task.is_success:
        logger.error(task.error)
