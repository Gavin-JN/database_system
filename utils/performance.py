#!/usr/bin/env python3
"""
数据库系统性能监控模块
提供性能统计和监控功能
"""
import time
# import psutil  # 可选依赖
import threading
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
from datetime import datetime, timedelta
from enum import Enum


class MetricType(Enum):
    """指标类型"""
    SQL_EXECUTION = "sql_execution"
    CACHE_OPERATION = "cache_operation"
    STORAGE_OPERATION = "storage_operation"
    MEMORY_USAGE = "memory_usage"
    CPU_USAGE = "cpu_usage"
    DISK_IO = "disk_io"


@dataclass
class PerformanceMetric:
    """性能指标"""
    metric_type: MetricType
    operation: str
    duration: float
    timestamp: datetime
    details: Dict[str, Any] = field(default_factory=dict)
    success: bool = True
    error: Optional[str] = None


@dataclass
class SystemStats:
    """系统统计信息"""
    memory_usage: float  # MB
    cpu_usage: float     # %
    disk_usage: float    # MB
    timestamp: datetime


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self, max_history: int = 1000):
        self.max_history = max_history
        self.metrics: deque = deque(maxlen=max_history)
        self.system_stats: List[SystemStats] = []
        self.counters: Dict[str, int] = defaultdict(int)
        self.timers: Dict[str, float] = {}
        self.lock = threading.Lock()
        
        # 启动系统监控线程
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_system, daemon=True)
        self.monitor_thread.start()
    
    def record_metric(self, metric: PerformanceMetric):
        """记录性能指标"""
        with self.lock:
            self.metrics.append(metric)
            self.counters[f"{metric.metric_type.value}_{metric.operation}"] += 1
    
    def start_timer(self, operation: str) -> str:
        """开始计时"""
        timer_id = f"{operation}_{int(time.time() * 1000000)}"
        self.timers[timer_id] = time.time()
        return timer_id
    
    def end_timer(self, timer_id: str, metric_type: MetricType, 
                  operation: str, success: bool = True, 
                  error: Optional[str] = None, **details):
        """结束计时并记录指标"""
        if timer_id in self.timers:
            duration = time.time() - self.timers[timer_id]
            del self.timers[timer_id]
            
            metric = PerformanceMetric(
                metric_type=metric_type,
                operation=operation,
                duration=duration,
                timestamp=datetime.now(),
                details=details,
                success=success,
                error=error
            )
            self.record_metric(metric)
            return duration
        return 0.0
    
    def get_metrics_summary(self, metric_type: Optional[MetricType] = None,
                           operation: Optional[str] = None,
                           time_window: Optional[timedelta] = None) -> Dict[str, Any]:
        """获取指标摘要"""
        with self.lock:
            filtered_metrics = list(self.metrics)
            
            # 按类型过滤
            if metric_type:
                filtered_metrics = [m for m in filtered_metrics if m.metric_type == metric_type]
            
            # 按操作过滤
            if operation:
                filtered_metrics = [m for m in filtered_metrics if m.operation == operation]
            
            # 按时间窗口过滤
            if time_window:
                cutoff_time = datetime.now() - time_window
                filtered_metrics = [m for m in filtered_metrics if m.timestamp >= cutoff_time]
            
            if not filtered_metrics:
                return {
                    "count": 0,
                    "avg_duration": 0.0,
                    "min_duration": 0.0,
                    "max_duration": 0.0,
                    "success_rate": 0.0
                }
            
            durations = [m.duration for m in filtered_metrics]
            success_count = sum(1 for m in filtered_metrics if m.success)
            
            return {
                "count": len(filtered_metrics),
                "avg_duration": sum(durations) / len(durations),
                "min_duration": min(durations),
                "max_duration": max(durations),
                "success_rate": success_count / len(filtered_metrics),
                "total_duration": sum(durations)
            }
    
    def get_top_operations(self, metric_type: Optional[MetricType] = None,
                          limit: int = 10) -> List[Dict[str, Any]]:
        """获取最频繁的操作"""
        with self.lock:
            filtered_metrics = list(self.metrics)
            if metric_type:
                filtered_metrics = [m for m in filtered_metrics if m.metric_type == metric_type]
            
            operation_counts = defaultdict(int)
            operation_durations = defaultdict(list)
            
            for metric in filtered_metrics:
                operation_counts[metric.operation] += 1
                operation_durations[metric.operation].append(metric.duration)
            
            top_operations = []
            for operation, count in sorted(operation_counts.items(), 
                                        key=lambda x: x[1], reverse=True)[:limit]:
                durations = operation_durations[operation]
                top_operations.append({
                    "operation": operation,
                    "count": count,
                    "avg_duration": sum(durations) / len(durations),
                    "total_duration": sum(durations)
                })
            
            return top_operations
    
    def get_system_stats(self, time_window: Optional[timedelta] = None) -> List[SystemStats]:
        """获取系统统计信息"""
        with self.lock:
            if time_window:
                cutoff_time = datetime.now() - time_window
                return [s for s in self.system_stats if s.timestamp >= cutoff_time]
            return list(self.system_stats)
    
    def _monitor_system(self):
        """系统监控线程"""
        while self.monitoring:
            try:
                # 简化的系统信息获取
                import os
                import sys
                
                # 获取内存使用情况（简化实现）
                try:
                    if hasattr(os, 'getrusage'):
                        import resource
                        memory_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # KB to MB
                    else:
                        memory_usage = 0
                except:
                    memory_usage = 0
                
                # 简化的CPU使用率（基于时间）
                cpu_usage = 0.0  # 简化实现
                
                # 简化的磁盘使用情况
                try:
                    disk_usage = os.path.getsize(sys.executable) / 1024 / 1024  # MB
                except:
                    disk_usage = 0
                
                stats = SystemStats(
                    memory_usage=memory_usage,
                    cpu_usage=cpu_usage,
                    disk_usage=disk_usage,
                    timestamp=datetime.now()
                )
                
                with self.lock:
                    self.system_stats.append(stats)
                    # 保持最近1000条记录
                    if len(self.system_stats) > 1000:
                        self.system_stats = self.system_stats[-1000:]
                
                time.sleep(5)  # 每5秒监控一次
            except Exception as e:
                print(f"系统监控错误: {e}")
                time.sleep(10)
    
    def stop_monitoring(self):
        """停止监控"""
        self.monitoring = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=1)
    
    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        with self.lock:
            # 最近1小时的指标
            recent_metrics = self.get_metrics_summary(time_window=timedelta(hours=1))
            
            # 各类型指标统计
            sql_metrics = self.get_metrics_summary(MetricType.SQL_EXECUTION)
            cache_metrics = self.get_metrics_summary(MetricType.CACHE_OPERATION)
            storage_metrics = self.get_metrics_summary(MetricType.STORAGE_OPERATION)
            
            # 最频繁的操作
            top_sql_ops = self.get_top_operations(MetricType.SQL_EXECUTION, 5)
            top_cache_ops = self.get_top_operations(MetricType.CACHE_OPERATION, 5)
            
            # 系统统计
            recent_system_stats = self.get_system_stats(timedelta(minutes=10))
            avg_memory = sum(s.memory_usage for s in recent_system_stats) / len(recent_system_stats) if recent_system_stats else 0
            avg_cpu = sum(s.cpu_usage for s in recent_system_stats) / len(recent_system_stats) if recent_system_stats else 0
            
            return {
                "summary": {
                    "total_operations": recent_metrics["count"],
                    "avg_duration": recent_metrics["avg_duration"],
                    "success_rate": recent_metrics["success_rate"]
                },
                "sql_operations": {
                    "count": sql_metrics["count"],
                    "avg_duration": sql_metrics["avg_duration"],
                    "success_rate": sql_metrics["success_rate"],
                    "top_operations": top_sql_ops
                },
                "cache_operations": {
                    "count": cache_metrics["count"],
                    "avg_duration": cache_metrics["avg_duration"],
                    "success_rate": cache_metrics["success_rate"],
                    "top_operations": top_cache_ops
                },
                "storage_operations": {
                    "count": storage_metrics["count"],
                    "avg_duration": storage_metrics["avg_duration"],
                    "success_rate": storage_metrics["success_rate"]
                },
                "system_resources": {
                    "avg_memory_mb": avg_memory,
                    "avg_cpu_percent": avg_cpu,
                    "monitoring_duration_minutes": len(recent_system_stats) * 5
                }
            }


# 全局性能监控器实例
performance_monitor = PerformanceMonitor()


def record_sql_execution(operation: str, duration: float, success: bool = True, 
                        error: Optional[str] = None, **details):
    """记录SQL执行性能"""
    metric = PerformanceMetric(
        metric_type=MetricType.SQL_EXECUTION,
        operation=operation,
        duration=duration,
        timestamp=datetime.now(),
        details=details,
        success=success,
        error=error
    )
    performance_monitor.record_metric(metric)


def record_cache_operation(operation: str, duration: float, hit: bool, **details):
    """记录缓存操作性能"""
    metric = PerformanceMetric(
        metric_type=MetricType.CACHE_OPERATION,
        operation=operation,
        duration=duration,
        timestamp=datetime.now(),
        details={**details, "hit": hit},
        success=True
    )
    performance_monitor.record_metric(metric)


def record_storage_operation(operation: str, duration: float, table_name: str, 
                           record_count: int = 0, **details):
    """记录存储操作性能"""
    metric = PerformanceMetric(
        metric_type=MetricType.STORAGE_OPERATION,
        operation=operation,
        duration=duration,
        timestamp=datetime.now(),
        details={**details, "table_name": table_name, "record_count": record_count},
        success=True
    )
    performance_monitor.record_metric(metric)


def performance_timer(metric_type: MetricType, operation: str):
    """性能计时装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            timer_id = performance_monitor.start_timer(operation)
            try:
                result = func(*args, **kwargs)
                performance_monitor.end_timer(timer_id, metric_type, operation, True)
                return result
            except Exception as e:
                performance_monitor.end_timer(timer_id, metric_type, operation, False, str(e))
                raise
        return wrapper
    return decorator


if __name__ == "__main__":
    # 测试性能监控
    import random
    
    # 模拟一些操作
    for i in range(10):
        record_sql_execution("SELECT", random.uniform(0.001, 0.01), True)
        record_cache_operation("GET", random.uniform(0.0001, 0.001), random.choice([True, False]))
        record_storage_operation("INSERT", random.uniform(0.01, 0.1), "test_table", 1)
    
    # 生成性能报告
    report = performance_monitor.get_performance_report()
    print("性能报告:")
    print(f"总操作数: {report['summary']['total_operations']}")
    print(f"平均耗时: {report['summary']['avg_duration']:.3f}s")
    print(f"成功率: {report['summary']['success_rate']:.2%}")
    print(f"SQL操作数: {report['sql_operations']['count']}")
    print(f"缓存操作数: {report['cache_operations']['count']}")
    print(f"存储操作数: {report['storage_operations']['count']}")
    
    performance_monitor.stop_monitoring()
