import akshare as ak
from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
from typing import Optional, Dict, Any, List
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError

class AKShareIndexHistoryTask(BaseTask):
    """AKShare指数历史数据爬取任务
    
    使用AKShare的index_zh_a_hist函数获取中国股票指数历史数据
    支持日期范围筛选和多种主流指数
    """
    
    def __init__(self, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 任务配置字典，包含以下字段：
                - symbol: 指数代码，例如 'sh000001' (上证综指), 'sz399001' (深证成指)
                - start_date: 开始日期，格式为 'YYYY-MM-DD'，可选
                - end_date: 结束日期，格式为 'YYYY-MM-DD'，可选
                - get_all_data: 是否获取所有历史数据，默认False
                
        默认行为:
            - 如果未指定任何日期参数，则获取所有可用的历史数据
            - 如果指定了日期范围，则只获取该范围内的数据
            - 如果显式设置get_all_data=True，则忽略日期范围获取所有数据
        """
        super().__init__(task_config)
        self.symbol = task_config.get('symbol')
        if not self.symbol:
            raise TaskConfigError("symbol不能为空")
        
        # 处理日期参数
        self.end_date = task_config.get('end_date')
        self.start_date = task_config.get('start_date')
        self.get_all_data = task_config.get('get_all_data', False)
        
        # 如果没有指定日期范围且没有设置获取全部数据，则默认获取全部数据
        if not self.start_date and not self.end_date and not self.get_all_data:
            self.get_all_data = True
        
        # 设置默认日期（用于非全量获取时）
        if not self.get_all_data:
            if not self.end_date:
                self.end_date = datetime.now().strftime('%Y-%m-%d')
            if not self.start_date:
                # 默认获取30天的数据
                start_datetime = datetime.now() - timedelta(days=30)
                self.start_date = start_datetime.strftime('%Y-%m-%d')
    
    def _validate_params(self) -> None:
        """
        验证任务参数
        
        Raises:
            TaskConfigError: 参数验证失败时抛出
        """
        if not self.get_all_data:
            try:
                # 验证日期格式
                datetime.strptime(self.start_date, '%Y-%m-%d')
                datetime.strptime(self.end_date, '%Y-%m-%d')
                
                # 验证日期范围
                if self.start_date > self.end_date:
                    raise TaskConfigError("开始日期不能大于结束日期")
                    
            except ValueError as e:
                raise TaskConfigError(f"日期格式错误: {str(e)}")
    
    def run(self) -> None:
        """
        执行爬取任务
        
        Raises:
            TaskExecutionError: 任务执行失败时抛出
        """
        try:
            self._validate_params()
            
            if self.get_all_data:
                logger.info(f"开始爬取指数 {self.symbol} 的所有历史数据")
            else:
                logger.info(f"开始爬取指数 {self.symbol} 从 {self.start_date} 到 {self.end_date} 的历史数据")
            
            # 获取股票指数数据
            hist_data = self._get_stock_index_data()
            
            if hist_data is None or len(hist_data) == 0:
                logger.warning(f"未获取到指数 {self.symbol} 的历史数据")
                self.task_result = {
                    'status': 'success',
                    'message': f"未获取到指数 {self.symbol} 的历史数据",
                    'hist_data': []
                }
                return
            
            logger.info(f"成功爬取指数 {self.symbol} 的历史数据，共 {len(hist_data)} 条记录")
            
            self.task_result = {
                'status': 'success',
                'message': f"成功获取指数 {self.symbol} 历史数据",
                'hist_data': hist_data,
                'symbol': self.symbol,
                'start_date': self.start_date if not self.get_all_data else None,
                'end_date': self.end_date if not self.get_all_data else None,
                'get_all_data': self.get_all_data,
                'record_count': len(hist_data)
            }
            
        except TaskConfigError as e:
            error_msg = f"任务配置错误: {str(e)}"
            logger.error(error_msg)
            self.task_result = {
                'status': 'error',
                'message': error_msg,
                'hist_data': []
            }
            raise TaskExecutionError(error_msg) from e
            
        except Exception as e:
            error_msg = f"爬取指数 {self.symbol} 历史数据失败: {str(e)}"
            logger.error(error_msg)
            self.task_result = {
                'status': 'error',
                'message': error_msg,
                'hist_data': []
            }
            raise TaskExecutionError(error_msg) from e
    
    def _get_stock_index_data(self) -> List[Dict[str, Any]]:
        """
        获取股票指数历史数据
        
        Returns:
            List[Dict[str, Any]]: 历史数据列表
        """
        try:
            # 转换指数代码格式：从 sh000001 -> 000001, sz399001 -> 399001
            if self.symbol.startswith(('sh', 'sz')):
                index_code = self.symbol[2:]  # 去掉 sh/sz 前缀
            else:
                index_code = self.symbol
            
            # 使用akshare的index_zh_a_hist获取指数历史数据
            if self.get_all_data:
                # 获取所有历史数据（不指定日期范围）
                df = ak.index_zh_a_hist(
                    symbol=index_code, 
                    period="daily"
                )
                logger.info(f"获取指数 {index_code} 的所有历史数据")
            else:
                # 转换日期格式：从 YYYY-MM-DD -> YYYYMMDD
                start_date_fmt = self.start_date.replace('-', '')
                end_date_fmt = self.end_date.replace('-', '')
                
                # 获取指定日期范围的数据
                df = ak.index_zh_a_hist(
                    symbol=index_code, 
                    period="daily",
                    start_date=start_date_fmt,
                    end_date=end_date_fmt
                )
                logger.info(f"获取指数 {index_code} 从 {start_date_fmt} 到 {end_date_fmt} 的历史数据")
            
            if df is None or df.empty:
                logger.warning(f"AKShare返回空数据: {self.symbol}")
                return []
            
            # 重命名列名，统一数据格式
            # index_zh_a_hist 返回的列名可能是英文或中文，需要处理两种情况
            column_mapping = {
                # 中文列名映射
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close', 
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'turnover',
                # 英文列名映射（保持原样）
                'date': 'date',
                'open': 'open',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'volume': 'volume',
                'turnover': 'turnover'
            }
            
            # 只重命名存在的列
            existing_columns = {col: column_mapping.get(col, col) for col in df.columns if col in column_mapping}
            df = df.rename(columns=existing_columns)
            
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            
            # 转换数值类型
            numeric_columns = ['open', 'close', 'high', 'low', 'volume', 'turnover']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 过滤日期范围（仅在非全量获取时）
            if not self.get_all_data:
                df = df[(df['date'] >= self.start_date) & (df['date'] <= self.end_date)]
            
            # 转换为字典列表
            hist_data = []
            for _, row in df.iterrows():
                data_point = {
                    'symbol': self.symbol,
                    'date': row['date'],
                    'open': row.get('open', 0.0),
                    'close': row.get('close', 0.0),
                    'high': row.get('high', 0.0), 
                    'low': row.get('low', 0.0),
                    'volume': row.get('volume', 0),
                    'turnover': row.get('turnover', 0.0),
                    'nav': None,  # 股票指数不使用净值
                    'change_pct': None  # 可以后续计算涨跌幅
                }
                hist_data.append(data_point)
            
            return hist_data
            
        except Exception as e:
            logger.error(f"获取股票指数 {self.symbol} 数据失败: {str(e)}")
            logger.error(f"使用的指数代码: {index_code if 'index_code' in locals() else 'N/A'}")
            if self.get_all_data:
                logger.error("获取模式: 全量数据")
            else:
                logger.error(f"日期范围: {start_date_fmt if 'start_date_fmt' in locals() else 'N/A'} 到 {end_date_fmt if 'end_date_fmt' in locals() else 'N/A'}")
            raise
    
    def get_supported_stock_indices(self) -> List[Dict[str, str]]:
        """
        获取支持的股票指数列表
        
        Returns:
            List[Dict[str, str]]: 支持的指数列表
        """
        return [
            {'symbol': 'sh000001', 'name': '上证综指', 'market': '上海'},
            {'symbol': 'sh000002', 'name': '上证A股指数', 'market': '上海'},
            {'symbol': 'sh000003', 'name': '上证B股指数', 'market': '上海'},
            {'symbol': 'sh000016', 'name': '上证50', 'market': '上海'},
            {'symbol': 'sh000300', 'name': '沪深300', 'market': '沪深'},
            {'symbol': 'sh000905', 'name': '中证500', 'market': '沪深'},
            {'symbol': 'sh000906', 'name': '中证800', 'market': '沪深'},
            {'symbol': 'sz399001', 'name': '深证成指', 'market': '深圳'},
            {'symbol': 'sz399002', 'name': '深成指R', 'market': '深圳'},
            {'symbol': 'sz399003', 'name': '成份B指', 'market': '深圳'},
            {'symbol': 'sz399004', 'name': '深证100R', 'market': '深圳'},
            {'symbol': 'sz399005', 'name': '中小板指', 'market': '深圳'},
            {'symbol': 'sz399006', 'name': '创业板指', 'market': '深圳'},
            {'symbol': 'sz399102', 'name': '创业板综', 'market': '深圳'}
        ]

def main():
    """测试用例"""
    print("=== AKShare指数历史数据爬取测试 ===\n")
    
    # 测试1: 获取所有历史数据（默认模式）
    print("1. 测试获取所有历史数据（默认模式）:")
    test_params_all = {
        'name': 'test_all_data',
        'description': '测试获取所有历史数据',
        'symbol': 'sh000001'  # 上证综指，不指定日期范围
    }
    
    task1 = AKShareIndexHistoryTask(test_params_all)
    task1.execute()
    
    if task1.is_success:
        result = task1.task_result
        print(f"✅ 成功获取所有历史数据，共 {result['record_count']} 条记录")
        print(f"   获取模式: {'全量数据' if result['get_all_data'] else '范围数据'}")
        if result['hist_data']:
            print("   最早3条数据:")
            for data in result['hist_data'][:3]:
                print(f"     {data['date']}: 开盘={data['open']}, 收盘={data['close']}")
            print("   最新3条数据:")
            for data in result['hist_data'][-3:]:
                print(f"     {data['date']}: 开盘={data['open']}, 收盘={data['close']}")
    else:
        print(f"❌ 获取失败: {task1.error}")
    
    print("\n" + "-"*50 + "\n")
    
    # 测试2: 获取指定日期范围的数据
    print("2. 测试获取指定日期范围数据:")
    test_params_range = {
        'name': 'test_range_data',
        'description': '测试获取指定范围数据',
        'symbol': 'sh000001',  # 上证综指
        'start_date': '2025-01-01',
        'end_date': '2025-01-31'
    }
    
    task2 = AKShareIndexHistoryTask(test_params_range)
    task2.execute()
    
    if task2.is_success:
        result = task2.task_result
        print(f"✅ 成功获取范围数据，共 {result['record_count']} 条记录")
        print(f"   获取模式: {'全量数据' if result['get_all_data'] else '范围数据'}")
        print(f"   日期范围: {result['start_date']} 到 {result['end_date']}")
        if result['hist_data']:
            print("   前3条数据:")
            for data in result['hist_data'][:3]:
                print(f"     {data['date']}: 开盘={data['open']}, 收盘={data['close']}")
    else:
        print(f"❌ 获取失败: {task2.error}")
    
    print("\n" + "-"*50 + "\n")
    
    # 测试3: 显式设置获取所有数据
    print("3. 测试显式设置获取所有数据:")
    test_params_explicit = {
        'name': 'test_explicit_all',
        'description': '显式设置获取所有数据',
        'symbol': 'sz399001',  # 深证成指
        'get_all_data': True
    }
    
    task3 = AKShareIndexHistoryTask(test_params_explicit)
    task3.execute()
    
    if task3.is_success:
        result = task3.task_result
        print(f"✅ 成功获取所有数据，共 {result['record_count']} 条记录")
        print(f"   获取模式: {'全量数据' if result['get_all_data'] else '范围数据'}")
    else:
        print(f"❌ 获取失败: {task3.error}")
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    main()