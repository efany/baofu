import sys
import os
import time
from typing import Dict, Any, List
from loguru import logger
from datetime import datetime, date, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase
from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError
from task_crawlers.yfinance_stock_history_task import YFinanceStockHistoryTask
from database.db_stocks_day_hist import DBStocksDayHist

class UpdateStocksDayHistTask(BaseTask):
    """更新股票日线历史数据的任务"""
    
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
        """
        super().__init__(task_config)

        self.mysql_db = mysql_db
        self.db_stocks_hist = DBStocksDayHist(self.mysql_db)

        # 验证task_config
        if 'stock_symbols' not in self.task_config:
            raise TaskConfigError("task_config必须包含stock_symbols字段")
        if not isinstance(self.task_config['stock_symbols'], list):
            raise TaskConfigError("stock_symbols必须是列表类型")
        if not self.task_config['stock_symbols']:
            raise TaskConfigError("stock_symbols不能为空")

    def get_last_update_date(self, symbol: str) -> date:
        """
        获取股票在数据库中的最新数据日期
        
        Args:
            symbol: 股票代码
            
        Returns:
            date: 最新数据日期，如果没有数据则返回None
        """
        hist_data = self.db_stocks_hist.get_stock_hist_data(symbol)
        if hist_data.empty:
            return None
        
        # 获取最新日期
        last_date = hist_data['date'].max()
        if isinstance(last_date, str):
            return datetime.strptime(last_date, '%Y-%m-%d').date()
        return last_date.date() if isinstance(last_date, datetime) else last_date

    def crawl_stock_history(self, symbol: str) -> List[Dict[str, Any]]:
        """
        获取单个股票的历史数据
        
        Args:
            symbol: 股票代码
            
        Returns:
            List[Dict[str, Any]]: 股票历史数据列表
        """
        # 检查日期范围
        start_date = datetime.strptime(self.task_config.get('start_date'), '%Y-%m-%d').date() if self.task_config.get('start_date') else None
        end_date = datetime.strptime(self.task_config.get('end_date'), '%Y-%m-%d').date() if self.task_config.get('end_date') else None

        last_date = self.get_last_update_date(symbol)
        if last_date:
            if start_date is None:
                start_date = (last_date + timedelta(days=1))
                # 从最新数据的下一天开始更新
                logger.info(f"股票{symbol}从{start_date}开始更新数据")
            else:
                start_date = max(start_date, last_date + timedelta(days=1))
        else:
            logger.info(f"股票{symbol}没有历史数据，将获取所有可用数据")

        if start_date and end_date and end_date <= start_date:
            logger.info(f"股票{symbol}的结束日期{end_date}小于等于开始日期{start_date}，无需更新数据")
            return {"total_records": 0, "hist_data": []}
        if start_date and start_date > date.today():
            logger.info(f"股票{symbol}的开始日期{start_date}大于当前日期，无需更新数据")
            return {"total_records": 0, "hist_data": []}

        # 创建爬虫任务配置
        crawler_config = {
            "name": "yfinance_stock_history",
            "description": f"爬取股票{symbol}历史数据",
            "stock_symbol": symbol,
            "start_date": start_date.strftime('%Y-%m-%d') if start_date else None,
            "end_date": end_date.strftime('%Y-%m-%d') if end_date else None,
            "proxy": self.task_config.get('proxy')  # 如果配置中有代理，则使用代理
        }
        
        # 执行爬虫任务
        crawler = YFinanceStockHistoryTask(crawler_config)
        crawler.execute()
        
        if not crawler.is_success:
            raise TaskExecutionError(f"爬取股票{symbol}历史数据失败: {crawler.error}")

        return crawler.result

    def update_stock_history_database(self, symbol: str, hist_data_list: Dict[str, Any]) -> None:
        """
        更新数据库中的股票历史数据
        
        Args:
            symbol: 股票代码
            hist_data_list: 股票历史数据列表
        """
        if not hist_data_list or hist_data_list['total_records'] == 0:
            logger.warning(f"股票{symbol}没有历史数据需要更新")
            return
        hist_data = hist_data_list['hist_data']

        # 将字符串日期转换为日期对象
        def parse_date(date_str: str) -> date:
            return datetime.strptime(date_str, '%Y-%m-%d').date()

        # 删除指定日期范围内的旧数据
        start_date = min(datetime.strptime(data['date'], '%Y-%m-%d').date() for data in hist_data)
        end_date = max(datetime.strptime(data['date'], '%Y-%m-%d').date() for data in hist_data)
        
        logger.debug(f"删除股票{symbol}从{start_date}到{end_date}的历史数据")
        self.db_stocks_hist.delete_stock_hist_data(symbol, start_date, end_date)
        
        # 插入新数据
        logger.debug(f"开始插入股票{symbol}的{len(hist_data)}条历史数据")
        self.db_stocks_hist.batch_insert_stock_hist_data(hist_data)

    def run(self) -> None:
        """执行更新股票历史数据的任务"""
        try:
            stock_symbols = self.task_config['stock_symbols']
            logger.info(f"开始更新{len(stock_symbols)}个股票的历史数据")
            
            # 遍历更新每个股票的历史数据
            for symbol in stock_symbols:
                try:
                    logger.debug(f"开始更新股票{symbol}历史数据")
                    hist_data_list = self.crawl_stock_history(symbol)
                    self.update_stock_history_database(symbol, hist_data_list)
                except Exception as e:
                    logger.error(f"更新股票{symbol}历史数据失败: {str(e)}")
                    continue
            
                # 每次请求后暂停一下，避免请求过快
                time.sleep(1)
                    
            logger.success("股票历史数据更新任务完成")
            
        except Exception as e:
            raise TaskExecutionError(f"更新股票历史数据任务失败: {str(e)}")


if __name__ == "__main__":
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )
    
    task_config = {
        "name": "update_stocks_day_hist",
        "description": "更新股票日线历史数据",
        "stock_symbols": ["512500.SS"],  # 示例股票代码列表
        "proxy": "http://127.0.0.1:7890"  # 可选的代理设置
    }
    
    task = UpdateStocksDayHistTask(mysql_db, task_config)
    task.execute()
    if not task.is_success:
        logger.error(task.error) 