import sys
import os
import time
from typing import Dict, Any, List
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase
from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError
from task_crawlers.yfinance_stock_info_task import YFinanceStockInfoTask
from database.db_stocks import DBStocks

class UpdateStocksInfoTask(BaseTask):
    """更新股票信息的任务"""
    
    def __init__(self, mysql_db: MySQLDatabase, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - stock_symbols: 股票代码列表
                - proxy: 代理服务器地址（可选）
        """
        super().__init__(task_config)

        self.mysql_db = mysql_db
        self.db_stocks = DBStocks(self.mysql_db)

        # 验证task_config
        if 'stock_symbols' not in self.task_config:
            raise TaskConfigError("task_config必须包含stock_symbols字段")
        if not isinstance(self.task_config['stock_symbols'], list):
            raise TaskConfigError("stock_symbols必须是列表类型")
        if not self.task_config['stock_symbols']:
            raise TaskConfigError("stock_symbols不能为空")

    def crawl_stock_info(self, stock_symbol: str) -> Dict[str, Any]:
        """
        更新单个股票的信息
        
        Args:
            stock_symbol: 股票代码
            
        Returns:
            Dict[str, Any]: 更新后的股票信息
        """
        # 创建爬虫任务配置
        crawler_config = {
            "name": "yfinance_stock_info",
            "description": f"爬取股票{stock_symbol}信息",
            "stock_symbol": stock_symbol,
            "proxy": self.task_config.get('proxy')  # 如果配置中有代理，则使用代理
        }
        
        # 执行爬虫任务
        crawler = YFinanceStockInfoTask(crawler_config)
        crawler.execute()
        
        if not crawler.is_success:
            raise TaskExecutionError(f"爬取股票{stock_symbol}信息失败: {crawler.error}")

        return crawler.result

    def update_stocks_database(self, stock_info: Dict[str, Any]) -> None:
        """
        更新数据库中的股票信息
        
        Args:
            stock_info: 股票信息字典
        """
        symbol = stock_info['symbol']

        logger.debug(f"更新股票{symbol}信息")
        for key, value in stock_info.items():
            logger.debug(f"{key}: {value}")

        db_stock_info = self.db_stocks.get_stock_info(symbol)

        if not db_stock_info.empty:
            logger.debug(f"股票{symbol}已存在, 将执行更新")
            self.db_stocks.update_stock_info(symbol, {
                'name': stock_info['name'],
                'currency': stock_info['currency'],
                'exchange': stock_info['exchange'],
                'market': stock_info['market']
            })
        else:
            logger.debug(f"股票{symbol}不存在, 将执行插入")
            self.db_stocks.insert_stock_info({
                'symbol': stock_info['symbol'],
                'name': stock_info['name'],
                'currency': stock_info['currency'],
                'exchange': stock_info['exchange'],
                'market': stock_info['market']
            })

    def run(self) -> None:
        """执行更新股票信息的任务"""
        try:
            stock_symbols = self.task_config['stock_symbols']
            logger.info(f"开始更新{len(stock_symbols)}个股票的信息")
            
            # 遍历更新每个股票的信息
            for stock_symbol in stock_symbols:
                try:
                    logger.debug(f"开始更新股票{stock_symbol}信息")
                    stock_info = self.crawl_stock_info(stock_symbol)
                    self.update_stocks_database(stock_info)
                except Exception as e:
                    logger.error(f"更新股票{stock_symbol}信息失败: {str(e)}")
                    continue
            
                # 每次请求后暂停一下，避免请求过快
                time.sleep(0.5)
                    
            logger.success("股票信息更新任务完成")
            
        except Exception as e:
            raise TaskExecutionError(f"更新股票信息任务失败: {str(e)}")

if __name__ == "__main__":
    mysql_db = MySQLDatabase(
        host='127.0.0.1',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu'
    )
    task_config = {
        "name": "update_stocks_info",
        "description": "更新股票基础信息",
        "stock_symbols": ["159949.SZ", "512550.SS"],  # 示例股票代码列表
        "proxy": "http://127.0.0.1:7890"  # 可选的代理设置
    }
    task = UpdateStocksInfoTask(mysql_db, task_config)
    task.execute()
    if not task.is_success:
        logger.error(task.error) 