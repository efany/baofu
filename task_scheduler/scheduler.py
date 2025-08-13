import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional
from dataclasses import dataclass
from loguru import logger
from enum import Enum


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class ScheduledTask:
    """定时任务数据类"""
    name: str
    task_func: Callable
    cron: str  # 简化的cron表达式: "H:M" 或 "*/N" (每N分钟)
    args: tuple = ()
    kwargs: Dict[str, Any] = None
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    status: TaskStatus = TaskStatus.PENDING
    error_msg: Optional[str] = None
    
    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}
        self._calculate_next_run()
    
    def _calculate_next_run(self):
        """计算下次执行时间"""
        now = datetime.now()
        
        if self.cron.startswith("*/"):
            # 每N分钟执行一次
            minutes = int(self.cron[2:])
            self.next_run = now + timedelta(minutes=minutes)
        elif ":" in self.cron:
            # 每天指定时间执行
            hour, minute = map(int, self.cron.split(":"))
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            self.next_run = next_run
        else:
            raise ValueError(f"不支持的cron格式: {self.cron}")
    
    def should_run(self) -> bool:
        """检查是否应该执行"""
        return datetime.now() >= self.next_run
    
    def update_next_run(self):
        """更新下次执行时间"""
        self.last_run = datetime.now()
        self._calculate_next_run()


class TaskScheduler:
    """精简的任务调度器"""
    
    def __init__(self):
        self.tasks: Dict[str, ScheduledTask] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
    
    def add_task(self, name: str, task_func: Callable, cron: str, 
                 args: tuple = (), kwargs: Dict[str, Any] = None) -> None:
        """添加定时任务"""
        with self._lock:
            task = ScheduledTask(name, task_func, cron, args, kwargs or {})
            self.tasks[name] = task
            logger.info(f"添加定时任务: {name}, 下次执行时间: {task.next_run}")
    
    def remove_task(self, name: str) -> bool:
        """移除定时任务"""
        with self._lock:
            if name in self.tasks:
                del self.tasks[name]
                logger.info(f"移除定时任务: {name}")
                return True
            return False
    
    def get_task_status(self, name: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        task = self.tasks.get(name)
        if not task:
            return None
        
        return {
            "name": task.name,
            "cron": task.cron,
            "status": task.status.value,
            "next_run": task.next_run,
            "last_run": task.last_run,
            "error_msg": task.error_msg
        }
    
    def list_tasks(self) -> List[Dict[str, Any]]:
        """列出所有任务状态"""
        return [self.get_task_status(name) for name in self.tasks.keys()]
    
    def _run_task(self, task: ScheduledTask) -> None:
        """执行单个任务"""
        try:
            task.status = TaskStatus.RUNNING
            logger.info(f"开始执行任务: {task.name}")
            
            task.task_func(*task.args, **task.kwargs)
            
            task.status = TaskStatus.SUCCESS
            task.error_msg = None
            logger.success(f"任务执行成功: {task.name}")
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error_msg = str(e)
            logger.error(f"任务执行失败: {task.name}, 错误: {e}")
        
        finally:
            task.update_next_run()
    
    def _scheduler_loop(self) -> None:
        """调度器主循环"""
        while self._running:
            try:
                with self._lock:
                    for task in list(self.tasks.values()):
                        if task.should_run() and task.status != TaskStatus.RUNNING:
                            # 在新线程中执行任务，避免阻塞调度器
                            thread = threading.Thread(
                                target=self._run_task, 
                                args=(task,),
                                daemon=True
                            )
                            thread.start()
                
                # 每10秒检查一次
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"调度器异常: {e}")
                time.sleep(10)
    
    def start(self) -> None:
        """启动调度器"""
        if self._running:
            logger.warning("调度器已在运行")
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._thread.start()
        logger.info("定时任务调度器已启动")
    
    def stop(self) -> None:
        """停止调度器"""
        if not self._running:
            return
        
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info("定时任务调度器已停止")
    
    def is_running(self) -> bool:
        """检查调度器是否运行中"""
        return self._running