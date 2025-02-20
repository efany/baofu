

class TaskError(Exception):
    """任务执行相关的基础异常类"""
    pass

class TaskConfigError(TaskError):
    """任务配置相关的异常"""
    pass

class TaskExecutionError(TaskError):
    """任务执行过程中的异常"""
    pass

class TaskDataError(TaskError):
    """任务数据处理相关的异常"""
    pass 