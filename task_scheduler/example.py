#!/usr/bin/env python3
"""
定时任务模块使用示例
"""

import sys
import os
import time
import signal
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_scheduler import create_task_manager
from loguru import logger


def example_task(message: str = "Hello"):
    """示例任务函数"""
    logger.info(f"执行示例任务: {message} at {datetime.now()}")


def main():
    # 创建任务管理器
    task_manager = create_task_manager()
    
    # 添加一个测试任务 - 每2分钟执行一次
    task_manager.add_custom_task(
        name="test_task",
        task_func=example_task,
        cron="*/1",  # 每2分钟
        message="定时任务测试"
    )
    
    # 启动调度器
    task_manager.start()
    
    # 设置优雅退出
    def signal_handler(sig, frame):
        logger.info("收到退出信号，正在停止任务调度器...")
        task_manager.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info("任务调度器已启动，按 Ctrl+C 退出")
        
        # 每30秒打印一次任务状态
        while task_manager.is_running():
            time.sleep(30)
            
            logger.info("=== 当前任务状态 ===")
            tasks = task_manager.list_all_tasks()
            for task in tasks:
                logger.info(f"任务: {task['name']}")
                logger.info(f"  状态: {task['status']}")
                logger.info(f"  下次执行: {task['next_run']}")
                logger.info(f"  上次执行: {task['last_run']}")
                if task['error_msg']:
                    logger.error(f"  错误: {task['error_msg']}")
                logger.info("-" * 30)
    
    except KeyboardInterrupt:
        logger.info("收到键盘中断，正在停止...")
    finally:
        task_manager.stop()


if __name__ == "__main__":
    main()