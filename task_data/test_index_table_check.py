#!/usr/bin/env python3
"""
测试指数更新任务的数据表检查功能
"""

import sys
import os
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.mysql_database import MySQLDatabase
from update_index_task import UpdateIndexTask
from task.exceptions import TaskConfigError, TaskExecutionError

def test_table_check():
    """测试数据表检查功能"""
    
    print("=== 测试指数更新任务的数据表检查 ===\n")
    
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
        table_exists = mysql_db.check_table_exists('index_hist_data')
        print(f"数据表 'index_hist_data' 存在: {table_exists}")
        
        # 测试任务配置
        test_config = {
            'name': 'test_table_check',
            'description': '测试数据表检查',
            'index_symbols': ['sh000001'],  # 上证综指
            'days_back': 7
        }
        
        print("创建UpdateIndexTask实例...")
        task = UpdateIndexTask(test_config, mysql_db)
        
        if table_exists:
            print("✅ 预期行为: 数据表存在，任务应该能正常执行表检查")
            try:
                # 只测试表检查，不执行完整任务
                task._ensure_table_exists()
                print("✅ 数据表检查通过")
            except Exception as e:
                print(f"❌ 数据表检查失败: {str(e)}")
        else:
            print("⚠️  预期行为: 数据表不存在，任务应该报错而不是自动创建")
            try:
                task._ensure_table_exists()
                print("❌ 意外: 数据表检查没有报错（应该报错）")
            except TaskConfigError as e:
                print(f"✅ 正确行为: 抛出TaskConfigError - {str(e)}")
            except Exception as e:
                print(f"❌ 意外错误类型: {type(e).__name__} - {str(e)}")
    
    except Exception as e:
        print(f"❌ 测试过程中发生异常: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 关闭数据库连接
        mysql_db.close_connection()

def test_full_task_execution():
    """测试完整任务执行（当表存在时）"""
    
    print("\n" + "="*50)
    print("=== 测试完整任务执行 ===\n")
    
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
        table_exists = mysql_db.check_table_exists('index_hist_data')
        
        if not table_exists:
            print("⚠️  数据表不存在，跳过完整任务测试")
            print("   请先运行 database/database_init.py 创建数据表")
            return
        
        # 测试任务配置 - 获取少量数据以避免长时间等待
        test_config = {
            'name': 'test_full_execution',
            'description': '测试完整任务执行',
            'index_symbols': ['sh000001'],  # 只测试上证综指
            'days_back': 3  # 只获取3天数据
        }
        
        print("开始执行完整的指数更新任务...")
        task = UpdateIndexTask(test_config, mysql_db)
        
        try:
            task.execute()
            
            if task.is_success:
                result = task.task_result
                print("✅ 任务执行成功!")
                print(f"   状态: {result['status']}")
                print(f"   消息: {result['message']}")
                print(f"   成功数量: {result['success_count']}")
                print(f"   失败数量: {result['error_count']}")
                print(f"   更新的指数: {result['updated_symbols']}")
            else:
                print("❌ 任务执行失败!")
                print(f"   错误: {task.error}")
                
        except TaskConfigError as e:
            print(f"❌ 配置错误: {str(e)}")
        except TaskExecutionError as e:
            print(f"❌ 执行错误: {str(e)}")
        except Exception as e:
            print(f"❌ 其他错误: {str(e)}")
    
    except Exception as e:
        print(f"❌ 测试过程中发生异常: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 关闭数据库连接
        mysql_db.close_connection()

def show_database_init_help():
    """显示数据库初始化帮助信息"""
    
    print("\n" + "="*50)
    print("=== 数据库初始化帮助 ===\n")
    
    print("如果数据表不存在，请按以下步骤初始化:")
    print("1. 运行数据库初始化脚本:")
    print("   cd database/")
    print("   python database_init.py")
    print()
    print("2. 或者手动创建数据表:")
    print("   执行 database/verify_index_table.py 中的SQL语句")
    print()
    print("3. 验证表创建成功:")
    print("   检查数据库中是否存在 'index_hist_data' 表")
    print()
    print("创建成功后即可正常使用指数更新任务。")

if __name__ == "__main__":
    # 测试数据表检查
    test_table_check()
    
    # 测试完整任务执行（如果表存在）
    test_full_task_execution()
    
    # 显示帮助信息
    show_database_init_help()
    
    print("\n=== 测试完成 ===")