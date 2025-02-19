from abc import ABC, abstractmethod
from typing import Any, Dict

class BaseTask(ABC):
    """
    任务基类，用于派生各种具体任务类（如爬虫任务、数据处理任务等）
    """
    
    def __init__(self, task_config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_config: 任务配置字典，包含任务所需的参数
        """
        self.task_config = task_config
        self.task_name = self.__class__.__name__
    
    @abstractmethod
    def run(self) -> None:
        """
        执行任务的抽象方法，需要被子类实现
        """
        pass
    
    def pre_run(self) -> None:
        """
        任务执行前的准备工作，可以被子类重写
        """
        pass
    
    def post_run(self) -> None:
        """
        任务执行后的清理工作，可以被子类重写
        """
        pass
    
    def execute(self) -> None:
        """
        任务执行的主流程
        """
        try:
            self.pre_run()
            self.run()
            self.post_run()
        except Exception as e:
            self.handle_error(e)
    
    def handle_error(self, error: Exception) -> None:
        """
        处理任务执行过程中的错误
        
        Args:
            error: 捕获到的异常
        """
        print(f"任务 {self.task_name} 执行出错: {str(error)}")
        raise error 