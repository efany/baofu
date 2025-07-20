"""
数据缓存管理器
用于缓存频繁查询的数据，减少数据库访问
"""
import time
import threading
from typing import Any, Dict, Optional, Callable
from loguru import logger


class CacheManager:
    """简单的内存缓存管理器"""
    
    def __init__(self, default_ttl: int = 300):
        """
        初始化缓存管理器
        
        Args:
            default_ttl: 默认缓存过期时间（秒），默认5分钟
        """
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._default_ttl = default_ttl
        self._lock = threading.RLock()
        
    def get(self, key: str) -> Optional[Any]:
        """
        从缓存中获取数据
        
        Args:
            key: 缓存键
            
        Returns:
            Optional[Any]: 缓存的数据，如果不存在或已过期则返回None
        """
        with self._lock:
            if key not in self._cache:
                return None
                
            cache_entry = self._cache[key]
            current_time = time.time()
            
            # 检查是否过期
            if current_time > cache_entry['expires_at']:
                del self._cache[key]
                logger.debug(f"缓存键 {key} 已过期，已删除")
                return None
                
            logger.debug(f"从缓存获取数据: {key}")
            return cache_entry['data']
    
    def set(self, key: str, data: Any, ttl: Optional[int] = None) -> None:
        """
        设置缓存数据
        
        Args:
            key: 缓存键
            data: 要缓存的数据
            ttl: 缓存过期时间（秒），如果为None则使用默认值
        """
        if ttl is None:
            ttl = self._default_ttl
            
        expires_at = time.time() + ttl
        
        with self._lock:
            self._cache[key] = {
                'data': data,
                'expires_at': expires_at,
                'created_at': time.time()
            }
            
        logger.debug(f"设置缓存: {key}, TTL: {ttl}秒")
    
    def delete(self, key: str) -> bool:
        """
        删除缓存数据
        
        Args:
            key: 缓存键
            
        Returns:
            bool: 是否成功删除
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                logger.debug(f"删除缓存: {key}")
                return True
            return False
    
    def clear(self) -> None:
        """清空所有缓存"""
        with self._lock:
            cache_count = len(self._cache)
            self._cache.clear()
            logger.info(f"清空缓存，共删除 {cache_count} 个缓存项")
    
    def cleanup_expired(self) -> int:
        """
        清理过期的缓存项
        
        Returns:
            int: 清理的缓存项数量
        """
        current_time = time.time()
        expired_keys = []
        
        with self._lock:
            for key, cache_entry in self._cache.items():
                if current_time > cache_entry['expires_at']:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self._cache[key]
                
        if expired_keys:
            logger.info(f"清理过期缓存，共删除 {len(expired_keys)} 个缓存项")
            
        return len(expired_keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息
        
        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        with self._lock:
            current_time = time.time()
            active_count = 0
            expired_count = 0
            
            for cache_entry in self._cache.values():
                if current_time > cache_entry['expires_at']:
                    expired_count += 1
                else:
                    active_count += 1
            
            return {
                'total_items': len(self._cache),
                'active_items': active_count,
                'expired_items': expired_count,
                'default_ttl': self._default_ttl
            }

    def cache_result(self, key: str, func: Callable, *args, ttl: Optional[int] = None, **kwargs) -> Any:
        """
        缓存函数执行结果的装饰器方法
        
        Args:
            key: 缓存键
            func: 要执行的函数
            *args: 函数参数
            ttl: 缓存过期时间
            **kwargs: 函数关键字参数
            
        Returns:
            Any: 函数执行结果
        """
        # 尝试从缓存获取
        result = self.get(key)
        if result is not None:
            return result
            
        # 缓存未命中，执行函数
        try:
            result = func(*args, **kwargs)
            self.set(key, result, ttl)
            return result
        except Exception as e:
            logger.error(f"执行函数时出错: {e}")
            raise


# 创建全局缓存管理器实例
cache_manager = CacheManager(default_ttl=300)  # 默认5分钟过期


def cached(key_pattern: str, ttl: Optional[int] = None):
    """
    缓存装饰器
    
    Args:
        key_pattern: 缓存键模式，可以使用 {arg_name} 占位符
        ttl: 缓存过期时间（秒）
        
    Usage:
        @cached("user_data_{user_id}")
        def get_user_data(user_id):
            return fetch_user_from_db(user_id)
    """
    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            # 生成缓存键
            import inspect
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            try:
                cache_key = key_pattern.format(**bound_args.arguments)
            except KeyError:
                # 如果键模式中的占位符不存在，使用函数名和参数生成键
                cache_key = f"{func.__name__}_{hash(str(bound_args.arguments))}"
            
            return cache_manager.cache_result(cache_key, func, *args, ttl=ttl, **kwargs)
            
        return wrapper
    return decorator