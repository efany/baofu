import sys
import os
from typing import Dict, Any, List
from datetime import datetime, timedelta
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_crawlers.akshare_index_history_task import AKShareIndexHistoryTask
from database.db_index_hist import DBIndexHist
from database.mysql_database import MySQLDatabase
from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError

class UpdateIndexTask(BaseTask):
    """更新指数数据任务"""
    
    def __init__(self, config: Dict[str, Any], mysql_db: MySQLDatabase):
        """
        初始化更新指数数据任务
        
        Args:
            config: 任务配置
            mysql_db: MySQL数据库连接
        """
        super().__init__(config)
        self.mysql_db = mysql_db
        self.db_index_hist = DBIndexHist(mysql_db)
        
        # 解析配置参数
        self.index_symbols = config.get('index_symbols', [])
        self.start_date = config.get('start_date')
        self.end_date = config.get('end_date')
        self.update_all = config.get('update_all', False)
        self.days_back = config.get('days_back', 30)  # 默认获取30天数据
        
    def _validate_params(self) -> None:
        """验证参数"""
        if not self.index_symbols and not self.update_all:
            raise TaskConfigError("必须指定index_symbols或设置update_all=True")
    
    def run(self) -> None:
        """执行更新任务"""
        try:
            self._validate_params()
            
            # 确保数据表存在
            self._ensure_table_exists()
            
            if self.update_all:
                # 更新所有支持的股票指数
                symbols_to_update = self._get_default_stock_indices()
            else:
                symbols_to_update = self.index_symbols
            
            logger.info(f"开始更新{len(symbols_to_update)}个指数的历史数据")
            
            success_count = 0
            error_count = 0
            updated_symbols = []
            
            for symbol in symbols_to_update:
                try:
                    success = self._update_single_index(symbol)
                    if success:
                        success_count += 1
                        updated_symbols.append(symbol)
                        logger.success(f"成功更新指数 {symbol}")
                    else:
                        error_count += 1
                        logger.error(f"更新指数 {symbol} 失败")
                except Exception as e:
                    error_count += 1
                    logger.error(f"更新指数 {symbol} 时发生异常: {str(e)}")
            
            # 设置任务结果
            self.task_result = {
                'status': 'success' if error_count == 0 else 'partial_success',
                'message': f"更新完成: 成功{success_count}个, 失败{error_count}个",
                'success_count': success_count,
                'error_count': error_count,
                'updated_symbols': updated_symbols,
                'total_symbols': len(symbols_to_update)
            }
            
            if error_count == len(symbols_to_update):
                # 全部失败
                raise TaskExecutionError("所有指数更新都失败了")
            
            logger.info(f"指数数据更新完成: 成功{success_count}个, 失败{error_count}个")
            
        except TaskConfigError as e:
            error_msg = f"任务配置错误: {str(e)}"
            logger.error(error_msg)
            self.task_result = {
                'status': 'error',
                'message': error_msg,
                'success_count': 0,
                'error_count': 0
            }
            raise TaskExecutionError(error_msg) from e
            
        except Exception as e:
            error_msg = f"更新指数数据失败: {str(e)}"
            logger.error(error_msg)
            self.task_result = {
                'status': 'error', 
                'message': error_msg,
                'success_count': 0,
                'error_count': 0
            }
            raise TaskExecutionError(error_msg) from e
    
    def _ensure_table_exists(self) -> None:
        """
        检查指数历史数据表是否存在
        
        如果表不存在，会抛出TaskConfigError要求用户先初始化数据库
        不会自动创建表，需要用户手动运行database_init.py
        
        Raises:
            TaskConfigError: 当数据表不存在时
            TaskExecutionError: 当数据库检查失败时
        """
        try:
            if not self.mysql_db.check_table_exists('index_hist_data'):
                error_msg = (
                    "指数历史数据表 'index_hist_data' 不存在。"
                    "请先运行 database/database_init.py 初始化数据库表结构，"
                    "或者手动创建该数据表。"
                )
                logger.error(error_msg)
                raise TaskConfigError(error_msg)
            
            logger.debug("指数历史数据表存在，检查通过")
        except Exception as e:
            if isinstance(e, TaskConfigError):
                raise
            logger.error(f"检查指数历史数据表失败: {str(e)}")
            raise TaskExecutionError(f"数据库表检查失败: {str(e)}") from e
    
    def _get_default_stock_indices(self) -> List[str]:
        """获取默认的股票指数列表"""
        return [
            'sh000001',  # 上证综指
            'sh000002',  # 上证A股指数
            'sh000016',  # 上证50
            'sh000300',  # 沪深300
            'sh000905',  # 中证500
            'sh000906',  # 中证800
            'sz399001',  # 深证成指
            'sz399005',  # 中小板指
            'sz399006'   # 创业板指
        ]
    
    def _update_single_index(self, symbol: str) -> bool:
        """
        更新单个指数的历史数据
        
        Args:
            symbol: 指数代码
            
        Returns:
            bool: 是否更新成功
        """
        try:
            # 确定更新的日期范围
            start_date, end_date, get_all_data = self._determine_date_range(symbol)
            
            if not start_date and not get_all_data:
                logger.info(f"指数 {symbol} 无需更新")
                return True
            
            # 创建爬虫任务配置
            crawler_config = {
                'name': 'update_index_hist',
                'description': f'更新指数{symbol}历史数据',
                'symbol': symbol
            }
            
            # 根据策略添加日期参数
            if get_all_data:
                crawler_config['get_all_data'] = True
                logger.info(f"指数 {symbol} 将获取所有历史数据")
            else:
                crawler_config['start_date'] = start_date
                crawler_config['end_date'] = end_date
                logger.info(f"指数 {symbol} 将获取 {start_date} 到 {end_date} 的数据")
            
            # 执行爬虫任务
            crawler = AKShareIndexHistoryTask(crawler_config)
            crawler.execute()
            
            if not crawler.is_success:
                logger.error(f"获取指数{symbol}历史数据失败: {crawler.error}")
                return False
                
            # 获取历史数据
            hist_data = crawler.task_result.get('hist_data', [])
            if not hist_data:
                logger.warning(f"指数{symbol}没有历史数据")
                return True
            
            # 获取要插入的日期列表
            dates_to_insert = [data['date'] for data in hist_data]
            
            if get_all_data:
                # 如果是获取所有数据，先删除该指数的所有历史数据
                logger.info(f"删除指数 {symbol} 的所有历史数据，准备重新插入")
                self.db_index_hist.delete_index_hist_data(symbol=symbol)
            else:
                # 如果是范围更新，只删除要更新的日期范围内的数据
                if dates_to_insert:
                    min_date = min(dates_to_insert)
                    max_date = max(dates_to_insert)
                    logger.info(f"删除指数 {symbol} 在 {min_date} 到 {max_date} 范围内的旧数据")
                    self.db_index_hist.delete_index_hist_data(
                        symbol=symbol,
                        start_date=min_date,
                        end_date=max_date
                    )
            
            # 批量插入新数据
            insert_success = self.db_index_hist.batch_insert_index_hist_data(hist_data)
            
            if insert_success:
                if get_all_data:
                    logger.success(f"成功完整更新指数 {symbol} 历史数据，共 {len(hist_data)} 条记录")
                else:
                    logger.success(f"成功增量更新指数 {symbol} 历史数据，共 {len(hist_data)} 条记录")
                return True
            else:
                logger.error(f"插入指数{symbol}历史数据失败")
                return False
                
        except Exception as e:
            logger.error(f"更新指数{symbol}失败: {str(e)}")
            return False
    
    def _determine_date_range(self, symbol: str) -> tuple:
        """
        确定更新的日期范围
        
        智能日期范围策略:
        1. 如果用户指定了完整的日期范围，直接使用
        2. 如果数据库中无该指数数据，获取所有历史数据 
        3. 如果数据库中有数据，从(最新日期-30天)开始更新到今天
        
        Args:
            symbol: 指数代码
            
        Returns:
            tuple: (start_date, end_date, get_all_data) 
                   如果不需要更新返回 (None, None, False)
                   如果获取全部数据返回 (None, None, True)
        """
        try:
            # 如果用户指定了完整的日期范围，直接使用
            if self.start_date and self.end_date:
                logger.info(f"使用用户指定的日期范围: {self.start_date} 到 {self.end_date}")
                return self.start_date, self.end_date, False
            
            # 获取数据库中该指数的最新日期
            latest_date = self.db_index_hist.get_latest_hist_date(symbol)
            end_date = self.end_date or datetime.now().strftime('%Y-%m-%d')
            
            if latest_date:
                logger.info(f"指数 {symbol} 数据库中最新日期: {latest_date}")
                
                # 有历史数据，从最新日期-30天开始更新
                latest_datetime = datetime.strptime(latest_date, '%Y-%m-%d')
                start_datetime = latest_datetime - timedelta(days=30)
                start_date = start_datetime.strftime('%Y-%m-%d')
                
                logger.info(f"基于最新数据日期，设置更新范围: {start_date} 到 {end_date}")
                
                # 检查是否需要更新（避免重复的历史数据获取）
                if start_date > end_date:
                    logger.info(f"指数 {symbol} 数据已经是最新的，无需更新")
                    return None, None, False
                    
                return start_date, end_date, False
                
            else:
                # 数据库中无该指数数据，获取所有历史数据
                logger.info(f"指数 {symbol} 数据库中无历史数据，将获取所有可用数据")
                return None, None, True
            
        except Exception as e:
            logger.error(f"确定日期范围失败: {str(e)}")
            # 异常情况下，使用保守策略：获取最近30天数据
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            logger.warning(f"使用默认日期范围: {start_date} 到 {end_date}")
            return start_date, end_date, False
    

def main():
    """测试用例"""
    print("=== UpdateIndexTask 测试 ===\n")
    
    # 初始化数据库连接
    mysql_db = MySQLDatabase(
        host='113.44.90.2',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu',
        pool_size=5
    )
    
    try:
        # 首先检查数据表是否存在
        table_exists = mysql_db.check_table_exists('index_hist_data')
        print(f"数据表 'index_hist_data' 存在: {table_exists}")
        
        if not table_exists:
            print("\n⚠️  数据表不存在!")
            print("请先运行以下命令初始化数据库:")
            print("  cd database/")
            print("  python database_init.py")
            print("\n或手动创建 'index_hist_data' 表")
            return
        
        # 测试配置 - 使用智能更新策略
        test_config = {
            'name': 'test_update_index',
            'description': '测试智能更新指数数据',
            'index_symbols': ['sh000001', 'sz399001'],  # 上证综指和深证成指
            # 不指定日期范围，让任务自动判断更新策略
        }
        
        print("\n预览更新策略...")
        task = UpdateIndexTask(test_config, mysql_db)
        
        # 先预览每个指数的更新策略
        for symbol in test_config['index_symbols']:
            try:
                start_date, end_date, get_all_data = task._determine_date_range(symbol)
                print(f"指数 {symbol}:")
                if get_all_data:
                    print(f"  策略: 获取所有历史数据 (数据库中无数据)")
                elif start_date and end_date:
                    print(f"  策略: 增量更新 ({start_date} 到 {end_date})")
                else:
                    print(f"  策略: 无需更新 (数据已是最新)")
            except Exception as e:
                print(f"  策略判断失败: {str(e)}")
        
        print(f"\n开始执行更新任务...")
        
        try:
            task.execute()
            
            if task.is_success:
                result = task.task_result
                print(f"✅ 更新成功: {result['message']}")
                print(f"成功更新的指数: {result['updated_symbols']}")
                print(f"成功数量: {result['success_count']}, 失败数量: {result['error_count']}")
            else:
                print(f"❌ 更新失败: {task.error}")
                
        except TaskConfigError as e:
            print(f"❌ 配置错误: {str(e)}")
        except TaskExecutionError as e:
            print(f"❌ 执行错误: {str(e)}")
    
    except Exception as e:
        print(f"❌ 测试过程中发生异常: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 关闭数据库连接
        mysql_db.close_connection()

if __name__ == "__main__":
    main()