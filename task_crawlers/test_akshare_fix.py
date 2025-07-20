#!/usr/bin/env python3
"""
测试修复后的AKShare指数历史数据爬取任务
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from akshare_index_history_task import AKShareIndexHistoryTask

def test_index_crawler():
    """测试指数爬虫修复"""
    
    print("=== 测试AKShare指数历史数据爬取 ===\n")
    
    # 测试配置 - 获取最近5天的数据
    end_date = datetime.now()
    start_date = end_date - timedelta(days=10)  # 获取10天数据，确保有交易日
    
    test_config = {
        'name': 'test_index_fix',
        'description': '测试修复后的指数历史数据获取',
        'symbol': 'sh000001',  # 上证综指
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d')
    }
    
    print(f"测试配置:")
    print(f"  指数代码: {test_config['symbol']}")
    print(f"  开始日期: {test_config['start_date']}")
    print(f"  结束日期: {test_config['end_date']}")
    print()
    
    try:
        # 创建并执行任务
        task = AKShareIndexHistoryTask(test_config)
        task.execute()
        
        if task.is_success:
            result = task.task_result
            print("✅ 任务执行成功!")
            print(f"状态: {result['status']}")
            print(f"消息: {result['message']}")
            print(f"数据条数: {result['record_count']}")
            
            if result['hist_data']:
                print(f"\n前3条数据示例:")
                for i, data in enumerate(result['hist_data'][:3], 1):
                    print(f"  {i}. 日期: {data['date']}")
                    print(f"     开盘: {data['open']}, 收盘: {data['close']}")
                    print(f"     最高: {data['high']}, 最低: {data['low']}")
                    print(f"     成交量: {data['volume']}")
                    print()
        else:
            print("❌ 任务执行失败!")
            print(f"错误: {task.error}")
            
    except Exception as e:
        print(f"❌ 测试过程中发生异常: {str(e)}")
        import traceback
        traceback.print_exc()

def test_multiple_indices():
    """测试多个指数"""
    
    print("\n=== 测试多个指数代码 ===\n")
    
    # 测试的指数列表
    test_indices = [
        ('sh000001', '上证综指'),
        ('sz399001', '深证成指'),
        ('sh000016', '上证50')
    ]
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5)
    
    for symbol, name in test_indices:
        print(f"测试 {symbol} ({name})...")
        
        test_config = {
            'name': f'test_{symbol}',
            'description': f'测试{name}数据获取',
            'symbol': symbol,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d')
        }
        
        try:
            task = AKShareIndexHistoryTask(test_config)
            task.execute()
            
            if task.is_success:
                result = task.task_result
                print(f"  ✅ 成功获取 {result['record_count']} 条数据")
            else:
                print(f"  ❌ 失败: {task.error}")
                
        except Exception as e:
            print(f"  ❌ 异常: {str(e)}")
        
        print()

if __name__ == "__main__":
    # 测试基本功能
    test_index_crawler()
    
    # 测试多个指数
    test_multiple_indices()
    
    print("=== 测试完成 ===")