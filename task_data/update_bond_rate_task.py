"""
更新债券利率历史数据的任务
"""
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError
from task_crawlers.akshare_bond_rate_history_task import AKShareBondRateHistoryTask
from database.db_bond_rate import DBBondRate
from database.mysql_database import MySQLDatabase

class UpdateBondRateTask(BaseTask):
    """更新债券利率历史数据的任务"""
    
    def __init__(self, task_config: Dict = None):
        """
        初始化任务
        
        Args:
            task_config: 任务参数，包含：
                - start_date: 开始日期，格式：YYYYMMDD，默认为30天前
                - end_date: 结束日期，格式：YYYYMMDD，默认为今天
                - bond_types: 债券类型列表，可选，为空时更新所有类型
        """
        super().__init__(task_config)
        self.start_date = self.task_config.get('start_date', 
            (datetime.now() - timedelta(days=3650 * 2)).strftime('%Y%m%d'))
        self.end_date = self.task_config.get('end_date', 
            datetime.now().strftime('%Y%m%d'))
        self.bond_types = self.task_config.get('bond_types', [])  # 默认为空列表，表示更新所有类型
        
        # 初始化数据库连接
        self.mysql_db = MySQLDatabase(
            host='127.0.0.1',
            user='baofu',
            password='TYeKmJPfw2b7kxGK',
            database='baofu'
        )
        self.db_bond_rate = DBBondRate(self.mysql_db)

    def run(self) -> Dict:
        """
        实现基类的抽象方法，执行任务
        
        Returns:
            Dict: 任务执行结果
        """
        try:
            # 验证参数
            self._validate_params()
            
            # 获取债券利率数据
            logger.info(f"开始获取债券利率数据，时间范围：{self.start_date} 至 {self.end_date}")
            crawler_task = AKShareBondRateHistoryTask({
                'start_date': self.start_date,
                'end_date': self.end_date
            })
            result = crawler_task.run()
            
            if result['status'] != 'success':
                raise TaskExecutionError(f"获取债券利率数据失败：{result['message']}")
            
            # 处理并存储数据
            data = result['data']
            if not data:
                raise TaskExecutionError('未获取到数据')
            
            # 将数据转换为DataFrame进行处理
            df = pd.DataFrame(data)
            
            # 处理NaN值
            df = df.replace({np.nan: None})  # 将numpy的nan替换为None
            
            # 过滤债券类型（如果指定了）
            if self.bond_types:
                df = df[df['bond_type'].isin(self.bond_types)]
            
            # 转换回列表格式
            data = df.to_dict('records')

            # 删除指定日期范围内的旧数据
            for bond_type in df['bond_type'].unique():
                self.db_bond_rate.delete_bond_rate(
                    bond_type=bond_type,
                    start_date=self.start_date,
                    end_date=self.end_date
                )
            # 批量插入数据
            success = self.db_bond_rate.batch_insert_bond_rate(data)
            
            if not success:
                raise TaskExecutionError('数据写入数据库失败')
            
            # 设置任务结果
            self.task_result = {
                'status': 'success',
                'message': f'成功更新{len(data)}条债券利率数据',
                'data': {
                    'total_records': len(data),
                    'bond_types': list(set(item['bond_type'] for item in data)),
                    'date_range': {
                        'start_date': self.start_date,
                        'end_date': self.end_date
                    }
                }
            }
            
            return self.task_result
            
        except Exception as e:
            error_msg = f"更新债券利率数据失败：{str(e)}"
            logger.error(error_msg)
            self.task_result = {
                'status': 'error',
                'message': error_msg,
                'data': None
            }
            raise TaskExecutionError(error_msg) from e
        finally:
            # 关闭数据库连接
            self.mysql_db.close_connection()

    def _validate_params(self) -> None:
        """
        验证任务参数
        
        Raises:
            TaskConfigError: 参数验证失败时抛出
        """
        try:
            # 验证日期格式
            datetime.strptime(self.start_date, '%Y%m%d')
            datetime.strptime(self.end_date, '%Y%m%d')
            
            # 验证日期范围
            start = datetime.strptime(self.start_date, '%Y%m%d')
            end = datetime.strptime(self.end_date, '%Y%m%d')
            if start > end:
                raise TaskConfigError('开始日期不能晚于结束日期')

            # 获取数据库中最新数据日期
            latest_date = self.db_bond_rate.get_latest_date(bond_type='CN_10Y')
            logger.info(f"数据库中最新数据日期：{latest_date}")
            
            # 如果最新日期存在且大于start_date，则将start_date调整为start_date-30
            if latest_date:
                if latest_date > start.date():
                    start = latest_date - timedelta(days=30)
                    self.start_date = start.strftime('%Y%m%d')
                    logger.info(f"调整start_date为{self.start_date}，以确保数据连续性")
            
            # 验证债券类型
            if not isinstance(self.bond_types, list):
                raise TaskConfigError('bond_types必须是列表类型')
                
        except ValueError as e:
            raise TaskConfigError(f'日期格式错误：{str(e)}')
        except Exception as e:
            raise TaskConfigError(f'参数验证失败：{str(e)}')

def main():
    """测试用例"""
    # 测试参数
    test_params = {
        # 'start_date': '20240101',
        # 'end_date': '20240131',
        # bond_types 为空列表，将更新所有类型
    }
    
    try:
        # 创建任务实例
        task = UpdateBondRateTask(test_params)
        
        # 执行任务
        result = task.run()
        
        # 打印结果
        print("数据更新成功！")
        print("\n更新结果：")
        print(f"状态: {result['status']}")
        print(f"消息: {result['message']}")
        if result['data']:
            print(f"总记录数: {result['data']['total_records']}")
            print(f"债券类型: {result['data']['bond_types']}")
            print(f"日期范围: {result['data']['date_range']}")
    except (TaskConfigError, TaskExecutionError) as e:
        print(f"任务执行失败：{str(e)}")
    except Exception as e:
        print(f"发生未知错误：{str(e)}")

if __name__ == "__main__":
    main() 