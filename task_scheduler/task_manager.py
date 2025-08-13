import sys
import os
from typing import Dict, Any, List, Optional
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from task_scheduler.scheduler import TaskScheduler
from loguru import logger


class DataUpdateTaskManager:
    """数据更新任务管理器"""
    
    def __init__(self):
        self.scheduler = TaskScheduler()
        self._setup_default_tasks()
    
    def _setup_default_tasks(self):
        """设置默认的数据更新任务"""
        
        # 基金净值更新 - 每天早上8点
        self.scheduler.add_task(
            name="更新基金净值",
            task_func=self._update_funds_nav,
            cron="08:00",
            kwargs={}
        )
        
        # 股票数据更新 - 每天晚上6点
        self.scheduler.add_task(
            name="update_stocks_data",
            task_func=self._update_stocks_data,
            cron="18:00"
        )
        
        # 债券利率更新 - 每天中午12点
        self.scheduler.add_task(
            name="update_bond_rates",
            task_func=self._update_bond_rates,
            cron="12:00"
        )
        
        # 外汇数据更新 - 每4小时
        self.scheduler.add_task(
            name="update_forex_data",
            task_func=self._update_forex_data,
            cron="*/240"  # 每240分钟
        )
    
    def _update_funds_nav(self):
        """更新基金净值数据"""
        try:
            logger.info(f"基金净值更新任务执行完成，基金代码: {fund_codes}")
 
        except Exception as e:
            logger.error(f"定时更新基金净值失败: {e}")
            raise
    
    def _update_stocks_data(self):
        """更新股票数据"""
        try:
            logger.info("股票数据更新任务执行完成")
            
        except Exception as e:
            logger.error(f"定时更新股票数据失败: {e}")
            raise
    
    def _update_bond_rates(self):
        """更新债券利率数据"""
        try:
            logger.info("债券利率数据更新任务执行完成")
            
        except Exception as e:
            logger.error(f"定时更新债券利率失败: {e}")
            raise
    
    def _update_forex_data(self):
        """更新外汇数据"""
        try:
            logger.info("外汇数据更新任务执行完成")
            
        except Exception as e:
            logger.error(f"定时更新外汇数据失败: {e}")
            raise
    
    def add_custom_task(self, name: str, task_func, cron: str, **kwargs):
        """添加自定义任务"""
        self.scheduler.add_task(name, task_func, cron, kwargs=kwargs)
    
    def remove_task(self, name: str) -> bool:
        """移除任务"""
        return self.scheduler.remove_task(name)
    
    def get_task_status(self, name: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        return self.scheduler.get_task_status(name)
    
    def list_all_tasks(self) -> List[Dict[str, Any]]:
        """列出所有任务"""
        return self.scheduler.list_tasks()
    
    def start(self):
        """启动任务调度器"""
        self.scheduler.start()
        logger.info("数据更新任务管理器已启动")
    
    def stop(self):
        """停止任务调度器"""
        self.scheduler.stop()
        logger.info("数据更新任务管理器已停止")
    
    def is_running(self) -> bool:
        """检查是否运行中"""
        return self.scheduler.is_running()


def create_task_manager() -> DataUpdateTaskManager:
    """创建任务管理器实例"""
    return DataUpdateTaskManager()