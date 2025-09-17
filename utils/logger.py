#!/usr/bin/env python3
"""
数据库系统日志模块
提供统一的日志记录功能
"""
import logging
import os
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum


class LogLevel(Enum):
    """日志级别"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class DatabaseLogger:
    """数据库系统日志记录器"""
    
    def __init__(self, name: str = "database", log_file: Optional[str] = None, 
                 level: LogLevel = LogLevel.INFO):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.value))
        
        # 避免重复添加处理器
        if not self.logger.handlers:
            # 创建格式器
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            # 控制台处理器
            # console_handler = logging.StreamHandler()
            # console_handler.setLevel(logging.INFO)
            # console_handler.setFormatter(formatter)
            # self.logger.addHandler(console_handler)
            
            # 文件处理器
            if log_file:
                file_handler = logging.FileHandler(log_file, encoding='utf-8')
                file_handler.setLevel(logging.DEBUG)
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
    
    def debug(self, message: str, **kwargs):
        """记录调试信息"""
        self.logger.debug(self._format_message(message, **kwargs))
    
    def info(self, message: str, **kwargs):
        """记录信息"""
        self.logger.info(self._format_message(message, **kwargs))
    
    def warning(self, message: str, **kwargs):
        """记录警告"""
        self.logger.warning(self._format_message(message, **kwargs))
    
    def error(self, message: str, **kwargs):
        """记录错误"""
        self.logger.error(self._format_message(message, **kwargs))
    
    def critical(self, message: str, **kwargs):
        """记录严重错误"""
        self.logger.critical(self._format_message(message, **kwargs))
    
    def _format_message(self, message: str, **kwargs) -> str:
        """格式化消息"""
        if kwargs:
            return f"{message} | {kwargs}"
        return message
    
    def log_sql_execution(self, sql: str, success: bool, duration: float, 
                         rows_affected: int = 0, error: Optional[str] = None):
        """记录SQL执行信息"""
        status = "SUCCESS" if success else "FAILED"
        self.info(f"SQL执行 {status}", 
                 sql=sql[:100] + "..." if len(sql) > 100 else sql,
                 duration=f"{duration:.3f}s",
                 rows_affected=rows_affected,
                 error=error)
    
    def log_cache_operation(self, operation: str, page_id: int, hit: bool):
        """记录缓存操作"""
        self.debug(f"缓存{operation}", page_id=page_id, hit=hit)
    
    def log_storage_operation(self, operation: str, table_name: str, 
                            record_count: int = 0, page_count: int = 0):
        """记录存储操作"""
        self.debug(f"存储{operation}", 
                  table_name=table_name, 
                  record_count=record_count,
                  page_count=page_count)
    
    def log_performance(self, operation: str, duration: float, 
                       details: Optional[Dict[str, Any]] = None):
        """记录性能信息"""
        self.info(f"性能统计 {operation}", 
                 duration=f"{duration:.3f}s",
                 details=details)


# 全局日志记录器实例
logger = DatabaseLogger("database", "database.log", LogLevel.DEBUG)


def get_logger(name: str = "database") -> DatabaseLogger:
    """获取日志记录器"""
    return DatabaseLogger(name)


def log_function_call(func):
    """函数调用日志装饰器"""
    def wrapper(*args, **kwargs):
        logger.debug(f"调用函数 {func.__name__}", args=args, kwargs=kwargs)
        try:
            result = func(*args, **kwargs)
            logger.debug(f"函数 {func.__name__} 执行成功")
            return result
        except Exception as e:
            logger.error(f"函数 {func.__name__} 执行失败", error=str(e))
            raise
    return wrapper


def log_sql_operation(sql: str, success: bool, duration: float, 
                     rows_affected: int = 0, error: Optional[str] = None):
    """记录SQL操作"""
    logger.log_sql_execution(sql, success, duration, rows_affected, error)


def log_performance_metric(operation: str, duration: float, 
                          details: Optional[Dict[str, Any]] = None):
    """记录性能指标"""
    logger.log_performance(operation, duration, details)


if __name__ == "__main__":
    # 测试日志功能
    logger.info("数据库系统启动")
    logger.debug("调试信息", module="test")
    logger.warning("警告信息", code="W001")
    logger.error("错误信息", error_code="E001")
    
    # 测试SQL执行日志
    logger.log_sql_execution("SELECT * FROM test", True, 0.001, 5)
    logger.log_sql_execution("INSERT INTO test VALUES (1)", False, 0.002, 0, "语法错误")
    
    # 测试性能日志
    logger.log_performance("查询操作", 0.005, {"table": "test", "rows": 100})






