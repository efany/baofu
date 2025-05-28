"""
使用 akshare 获取债券利率历史数据的任务
"""
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
from loguru import logger
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task.base_task import BaseTask
from task.exceptions import TaskConfigError, TaskExecutionError

class AKShareBondRateHistoryTask(BaseTask):
    """获取债券收益率历史数据的任务"""
    
    def __init__(self, task_config: Dict = None):
        """
        初始化任务
        
        Args:
            params: 任务参数，包含：
                - start_date: 开始日期，格式：YYYYMMDD，默认为30天前
                - end_date: 结束日期，格式：YYYYMMDD，默认为今天
        """
        super().__init__(task_config)
        self.start_date = self.task_config.get('start_date', 
            (datetime.now() - timedelta(days=30)).strftime('%Y%m%d'))
        self.end_date = self.task_config.get('end_date', 
            datetime.now().strftime('%Y%m%d'))
        
        # 定义收益率类型映射
        self.rate_types = {
            '中国国债收益率10年': 'CN_10Y',
            '中国国债收益率2年': 'CN_2Y',
            '中国国债收益率5年': 'CN_5Y',
            '中国国债收益率30年': 'CN_30Y',
            '美国国债收益率10年': 'US_10Y',
            '美国国债收益率2年': 'US_2Y',
            '美国国债收益率5年': 'US_5Y',
            '美国国债收益率30年': 'US_30Y'
        }

    def run(self) -> Dict:
        """
        实现基类的抽象方法，执行任务
        
        Returns:
            Dict: 任务执行结果
        """
        try:
            # 验证参数
            self._validate_params()
            
            # 获取原始数据
            logger.info(f"开始获取债券收益率数据，时间范围：{self.start_date} 至 {self.end_date}")
            df = ak.bond_zh_us_rate(start_date=self.start_date)
            
            if df.empty:
                raise TaskExecutionError('未获取到数据')
            
            # 处理数据
            result_data = self._process_data(df)
            
            # 设置任务结果
            self.task_result = {
                'status': 'success',
                'message': '数据获取成功',
                'data': result_data
            }
            
            return self.task_result
            
        except Exception as e:
            error_msg = f"获取债券收益率数据失败：{str(e)}"
            logger.error(error_msg)
            self.task_result = {
                'status': 'error',
                'message': error_msg,
                'data': None
            }
            raise TaskExecutionError(error_msg) from e

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
                
        except ValueError as e:
            raise TaskConfigError(f'日期格式错误：{str(e)}')
        except Exception as e:
            raise TaskConfigError(f'参数验证失败：{str(e)}')

    def _process_data(self, df) -> List[Dict]:
        """
        处理原始数据，将数据转换为按日期、债券类型、利率的格式
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            List[Dict]: 处理后的数据列表，每行包含：
                - date: 日期，格式：YYYY-MM-DD
                - bond_type: 债券类型（如：CN_10Y, US_10Y等）
                - rate: 利率值
        """
        result = []
        
        # 确保日期列是datetime类型
        df['日期'] = pd.to_datetime(df['日期'])
        
        # 过滤日期范围
        mask = (df['日期'] >= pd.to_datetime(self.start_date)) & \
               (df['日期'] <= pd.to_datetime(self.end_date))
        df = df[mask]
        
        # 处理每种收益率类型
        for cn_name, en_name in self.rate_types.items():
            if cn_name in df.columns:
                # 遍历每一行数据
                for _, row in df.iterrows():
                    try:
                        rate = float(row[cn_name])
                    except (ValueError, TypeError):
                        rate = None
                        
                    result.append({
                        'date': row['日期'].strftime('%Y-%m-%d'),
                        'bond_type': en_name,
                        'rate': rate
                    })
        
        # 按日期和债券类型排序
        result.sort(key=lambda x: (x['date'], x['bond_type']))
        
        return result

def main():
    """测试用例"""
    # 测试参数
    test_params = {
        'start_date': '20240101'
    }
    
    try:
        # 创建任务实例
        task = AKShareBondRateHistoryTask(test_params)
        
        # 执行任务
        result = task.run()
        
        # 打印结果
        print("数据获取成功！")
        print("\n数据示例：")
        if result['data']:
            print(f"总数据条数: {len(result['data'])}")
            print("\n前5条数据:")
            for item in result['data'][:5]:
                print(f"日期: {item['date']}, 债券类型: {item['bond_type']}, 利率: {item['rate']}")
            print("\n后5条数据:")
            for item in result['data'][-5:]:
                print(f"日期: {item['date']}, 债券类型: {item['bond_type']}, 利率: {item['rate']}")
    except (TaskConfigError, TaskExecutionError) as e:
        print(f"任务执行失败：{str(e)}")
    except Exception as e:
        print(f"发生未知错误：{str(e)}")

if __name__ == "__main__":
    import pandas as pd
    main()