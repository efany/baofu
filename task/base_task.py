from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

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
        self.task_result: Dict[str, Any] = {}
        self._error: Optional[Exception] = None
        self._success: bool = False
    
    @property
    def result(self) -> Dict[str, Any]:
        """
        获取任务执行结果
        
        Returns:
            Dict[str, Any]: 任务执行结果字典
        """
        return self.task_result
    
    @property
    def has_error(self) -> bool:
        """
        检查任务执行是否有错误
        
        Returns:
            bool: 如果有错误返回True，否则返回False
        """
        return self._error is not None
    
    @property
    def error(self) -> Optional[Exception]:
        """
        获取任务执行的错误信息
        
        Returns:
            Optional[Exception]: 如果有错误返回异常对象，否则返回None
        """
        return self._error
    
    @property
    def is_success(self) -> bool:
        """
        检查任务是否执行成功
        
        Returns:
            bool: 如果成功返回True，否则返回False
        """
        return self._success
    
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
        # 重置状态
        self._error = None
        self._success = False
    
    def post_run(self) -> None:
        """
        任务执行后的清理工作，可以被子类重写
        """
        # 如果执行到这里没有异常，标记为成功
        if not self.has_error:
            self._success = True
    
    def execute(self) -> None:
        """
        任务执行的主流程
        """
        try:
            self.pre_run()
            self.run()
            self.post_run()
        except Exception as e:
            self._error = e
            self._success = False
            self.handle_error(e)
    
    def handle_error(self, error: Exception) -> None:
        """
        处理任务执行过程中的错误
        
        Args:
            error: 捕获到的异常
        """
        print(f"任务 {self.task_name} 执行出错: {str(error)}")
        raise error 