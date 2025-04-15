from typing import Dict, Any, List, Optional
from datetime import datetime, date
import pandas as pd
from loguru import logger
import akshare as ak
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError

class AKShareETFHistoryTask(BaseTask):
    """通过AKShare获取东方财富ETF历史数据的任务"""
    
    def __init__(self, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 包含以下字段：
                - name: 任务名称
                - description: 任务描述
                - stock_symbol: 股票代码
                - start_date: 开始日期，格式：YYYYMMDD（可选）
                - end_date: 结束日期，格式：YYYYMMDD（可选）
        """
        super().__init__(task_config)
        
        # 验证task_config
        if 'stock_symbol' not in self.task_config:
            raise TaskConfigError("task_config必须包含stock_symbol字段")
        if not isinstance(self.task_config['stock_symbol'], str):
            raise TaskConfigError("stock_symbol必须是字符串类型")
        if not self.task_config['stock_symbol']:
            raise TaskConfigError("stock_symbol不能为空")

    def _get_stock_history(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        获取单个股票的历史数据
        
        Args:
            symbol: 股票代码
            
        Returns:
            Optional[pd.DataFrame]: 股票历史数据，如果获取失败则返回None
        """
        try:
            stock_symbol = symbol
            # 处理股票代码，确保符合AKShare的格式要求
            if stock_symbol.endswith('.SS'):
                stock_symbol = stock_symbol.replace('.SS', '')  # 上海交易所
            elif stock_symbol.endswith('.SZ'):
                stock_symbol = stock_symbol.replace('.SZ', '')  # 深圳交易所

            start_date = self.task_config.get('start_date')
            if start_date:
                start_date = start_date.replace('-', '')
            end_date = self.task_config.get('end_date')
            if end_date:
                end_date = end_date.replace('-', '')
            if start_date and end_date:
                # 获取历史数据
                df = ak.fund_etf_hist_em(
                    symbol=stock_symbol,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"  # 前复权
                )
            elif start_date:
                # 获取历史数据
                df = ak.fund_etf_hist_em(
                    symbol=stock_symbol,
                    period="daily",
                    start_date=start_date,
                    adjust="qfq"  # 前复权
                )
            elif end_date:
                # 获取历史数据
                df = ak.fund_etf_hist_em(
                    symbol=stock_symbol,
                    period="daily",
                    end_date=end_date,
                    adjust="qfq"  # 前复权
                )
            else:
                # 获取历史数据
                df = ak.fund_etf_hist_em(
                    symbol=stock_symbol,
                    period="daily",
                    adjust="qfq"  # 前复权
                )

            if df.empty:
                logger.warning(f"股票{symbol}没有历史数据")
                return None
                
            # 重命名列
            df = df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_chg',
                '涨跌额': 'change',
                '换手率': 'turnover'
            })
            
            # 添加股票代码列
            df['symbol'] = symbol

            df['dividends'] = 0
            df['stock_splits'] = 0
            
            # 调整列顺序
            columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'amplitude', 'pct_chg', 'change', 'turnover', 'dividends', 'stock_splits']
            df = df[columns]
            
            return df
            
        except Exception as e:
            logger.error(f"获取股票{symbol}历史数据失败: {str(e)}")
            return None

    def run(self) -> None:
        """执行任务"""
        try:
            symbol = self.task_config['stock_symbol']
            logger.info(f"正在获取股票{symbol}的历史数据...")
            
            df = self._get_stock_history(symbol)
            if df is None:
                raise TaskExecutionError(f"获取股票{symbol}历史数据失败")
            
            self.task_result = {
                'hist_data': df.to_dict('records'),
                'total_records': len(df)
            }
            logger.success(f"成功获取股票{symbol}的历史数据")
        except Exception as e:
            raise TaskExecutionError(f"获取股票历史数据失败: {str(e)}")

def main():
    """主函数，用于测试"""
    # 测试配置
    task_config = {
        "name": "akshare_stock_history",
        "description": "获取股票历史数据",
        "stock_symbol": "515220.SS",  # 测试股票代码
        # "start_date": "2024-01-01",
        # "end_date": "2024-06-15"
    }
    
    # 执行任务
    task = AKShareStockHistoryTask(task_config)
    task.execute()
    
    if task.is_success:
        logger.success(f"股票历史数据获取成功，共{task.task_result['total_records']}条记录")
        logger.info(f"股票前5条历史数据: {task.task_result['hist_data'][:5]}")
        logger.info(f"股票后5条历史数据: {task.task_result['hist_data'][-5:]}")
    else:
        logger.error(f"股票历史数据获取失败: {task.error}")

if __name__ == "__main__":
    main() 