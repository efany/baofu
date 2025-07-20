#!/usr/bin/env python3
"""
测试智能指数更新策略
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase
from database.db_index_hist import DBIndexHist
from update_index_task import UpdateIndexTask

def test_intelligent_update_strategy():
    """测试智能更新策略"""
    
    print("=== 测试智能指数更新策略 ===\n")
    
    # 数据库配置
    mysql_db = MySQLDatabase(
        host='113.44.90.2',
        user='baofu',
        password='TYeKmJPfw2b7kxGK',
        database='baofu',
        pool_size=5
    )
    
    try:
        # 检查数据表是否存在
        if not mysql_db.check_table_exists('index_hist_data'):
            print("❌ 数据表 'index_hist_data' 不存在")
            print("请先运行 database/database_init.py 初始化数据库")
            return
        
        db_index_hist = DBIndexHist(mysql_db)
        
        # 测试指数列表
        test_symbols = ['sh000001', 'sz399001', 'sh000016']
        
        print("当前数据库状态:")
        print("-" * 50)
        
        # 检查每个指数的当前状态
        for symbol in test_symbols:
            latest_date = db_index_hist.get_latest_hist_date(symbol)
            if latest_date:
                print(f"{symbol}: 最新数据日期 = {latest_date}")
            else:
                print(f"{symbol}: 无历史数据")
        
        print(f"\n当前日期: {datetime.now().strftime('%Y-%m-%d')}")
        print("\n" + "=" * 60)
        
        # 测试不同场景的更新策略
        print("\n1. 测试智能更新策略:")
        print("-" * 50)
        
        for symbol in test_symbols:
            print(f"\n测试指数: {symbol}")
            
            # 创建单个指数的更新任务
            test_config = {
                'name': f'test_intelligent_{symbol}',
                'description': f'测试智能更新策略 - {symbol}',
                'index_symbols': [symbol]
            }
            
            task = UpdateIndexTask(test_config, mysql_db)
            
            # 模拟date range determination
            try:
                start_date, end_date, get_all_data = task._determine_date_range(symbol)
                
                print(f"  更新策略判断结果:")
                if get_all_data:
                    print(f"    策略: 获取所有历史数据")
                    print(f"    原因: 数据库中无该指数历史数据")
                elif start_date and end_date:
                    print(f"    策略: 增量更新")
                    print(f"    日期范围: {start_date} 到 {end_date}")
                    print(f"    原因: 基于最新数据日期向前回溯30天")
                else:
                    print(f"    策略: 无需更新")
                    print(f"    原因: 数据已经是最新的")
                    
            except Exception as e:
                print(f"    ❌ 策略判断失败: {str(e)}")
        
        print("\n" + "=" * 60)
        
        # 询问是否执行实际更新
        print("\n2. 是否执行实际数据更新? (输入 'yes' 确认, 其他键取消)")
        user_input = input("请选择: ").strip().lower()
        
        if user_input == 'yes':
            print("\n开始执行实际数据更新...")
            print("-" * 50)
            
            # 执行实际更新
            update_config = {
                'name': 'intelligent_update_test',
                'description': '智能更新策略实际测试',
                'index_symbols': test_symbols[:1],  # 只更新第一个指数以节省时间
            }
            
            task = UpdateIndexTask(update_config, mysql_db)
            task.execute()
            
            if task.is_success:
                result = task.task_result
                print(f"\n✅ 更新完成!")
                print(f"状态: {result['status']}")
                print(f"消息: {result['message']}")
                print(f"成功数量: {result['success_count']}")
                print(f"失败数量: {result['error_count']}")
                print(f"更新的指数: {result['updated_symbols']}")
                
                # 显示更新后的数据状态
                print(f"\n更新后的数据状态:")
                for symbol in result['updated_symbols']:
                    latest_date = db_index_hist.get_latest_hist_date(symbol)
                    print(f"  {symbol}: 最新数据日期 = {latest_date}")
                    
            else:
                print(f"❌ 更新失败: {task.error}")
        else:
            print("取消实际更新操作")
        
        # 测试用户指定日期范围的场景
        print("\n" + "=" * 60)
        print("\n3. 测试用户指定日期范围:")
        print("-" * 50)
        
        # 指定日期范围的配置
        custom_config = {
            'name': 'custom_date_test',
            'description': '用户指定日期范围测试',
            'index_symbols': [test_symbols[0]],
            'start_date': '2025-01-01',
            'end_date': '2025-01-10'
        }
        
        custom_task = UpdateIndexTask(custom_config, mysql_db)
        start_date, end_date, get_all_data = custom_task._determine_date_range(test_symbols[0])
        
        print(f"用户指定日期范围: 2025-01-01 到 2025-01-10")
        print(f"策略判断结果:")
        print(f"  开始日期: {start_date}")
        print(f"  结束日期: {end_date}")
        print(f"  获取全部数据: {get_all_data}")
        print(f"  说明: 用户指定了完整日期范围，直接使用用户配置")
        
    except Exception as e:
        print(f"❌ 测试过程中发生异常: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        mysql_db.close_connection()

def simulate_different_database_states():
    """模拟不同的数据库状态来演示策略"""
    
    print("\n" + "=" * 60)
    print("=== 策略说明和示例场景 ===\n")
    
    scenarios = [
        {
            'description': '场景1: 数据库中无指数数据',
            'latest_date': None,
            'expected_strategy': '获取所有历史数据',
            'reasoning': '首次导入，需要完整的历史数据'
        },
        {
            'description': '场景2: 数据库中有较新的数据',
            'latest_date': '2025-07-15',
            'expected_strategy': '从 2025-06-15 到今天',
            'reasoning': '最新日期-30天作为起点，确保数据连续性'
        },
        {
            'description': '场景3: 数据库中有较老的数据',
            'latest_date': '2024-12-31',
            'expected_strategy': '从 2024-12-01 到今天',
            'reasoning': '最新日期-30天作为起点，填补数据缺口'
        },
        {
            'description': '场景4: 用户指定日期范围',
            'latest_date': '2025-07-15',
            'user_range': '2025-01-01 到 2025-01-31',
            'expected_strategy': '严格按用户指定范围',
            'reasoning': '用户明确需求，优先级最高'
        }
    ]
    
    for scenario in scenarios:
        print(f"{scenario['description']}:")
        print(f"  数据库最新日期: {scenario['latest_date'] or '无数据'}")
        if 'user_range' in scenario:
            print(f"  用户指定范围: {scenario['user_range']}")
        print(f"  预期策略: {scenario['expected_strategy']}")
        print(f"  策略原因: {scenario['reasoning']}")
        print()

if __name__ == "__main__":
    # 显示策略说明
    simulate_different_database_states()
    
    # 执行实际测试
    test_intelligent_update_strategy()
    
    print("\n=== 测试完成 ===")