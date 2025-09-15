#!/usr/bin/env python3
"""
数据库事务模块
提供ACID事务支持
"""
import time
import threading
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import uuid


class TransactionState(Enum):
    """事务状态"""
    ACTIVE = "active"
    COMMITTED = "committed"
    ABORTED = "aborted"
    PREPARING = "preparing"


class LockType(Enum):
    """锁类型"""
    SHARED = "shared"      # 共享锁
    EXCLUSIVE = "exclusive"  # 排他锁


class LockMode(Enum):
    """锁模式"""
    READ = "read"
    WRITE = "write"


@dataclass
class Lock:
    """锁对象"""
    resource: str  # 资源标识符（表名或页ID）
    lock_type: LockType
    transaction_id: str
    timestamp: float


@dataclass
class Transaction:
    """事务对象"""
    transaction_id: str
    state: TransactionState
    start_time: float
    read_set: Set[str] = field(default_factory=set)  # 读集
    write_set: Set[str] = field(default_factory=set)  # 写集
    locks: List[Lock] = field(default_factory=list)  # 持有的锁
    operations: List[Dict[str, Any]] = field(default_factory=list)  # 操作日志


class LockManager:
    """锁管理器"""
    
    def __init__(self):
        self.locks: Dict[str, List[Lock]] = defaultdict(list)  # resource -> locks
        self.wait_queue: Dict[str, List[Lock]] = defaultdict(list)  # resource -> waiting locks
        self.lock = threading.Lock()
    
    def acquire_lock(self, transaction_id: str, resource: str, 
                    lock_type: LockType) -> bool:
        """获取锁"""
        with self.lock:
            # 检查是否已持有兼容锁
            if self._has_compatible_lock(transaction_id, resource, lock_type):
                return True
            
            # 检查是否可以立即获取锁
            if self._can_acquire_lock(resource, lock_type):
                lock = Lock(resource, lock_type, transaction_id, time.time())
                self.locks[resource].append(lock)
                return True
            
            # 加入等待队列
            lock = Lock(resource, lock_type, transaction_id, time.time())
            self.wait_queue[resource].append(lock)
            return False
    
    def release_lock(self, transaction_id: str, resource: str) -> bool:
        """释放锁"""
        with self.lock:
            # 移除锁
            locks_to_remove = [lock for lock in self.locks[resource] 
                             if lock.transaction_id == transaction_id]
            for lock in locks_to_remove:
                self.locks[resource].remove(lock)
            
            # 处理等待队列
            self._process_wait_queue(resource)
            return True
    
    def release_all_locks(self, transaction_id: str) -> bool:
        """释放事务的所有锁"""
        with self.lock:
            for resource in list(self.locks.keys()):
                self.release_lock(transaction_id, resource)
            
            # 从等待队列中移除
            for resource in list(self.wait_queue.keys()):
                self.wait_queue[resource] = [
                    lock for lock in self.wait_queue[resource]
                    if lock.transaction_id != transaction_id
                ]
            return True
    
    def _has_compatible_lock(self, transaction_id: str, resource: str, 
                           lock_type: LockType) -> bool:
        """检查是否已持有兼容锁"""
        for lock in self.locks[resource]:
            if lock.transaction_id == transaction_id:
                if lock.lock_type == LockType.EXCLUSIVE:
                    return True
                elif lock.lock_type == LockType.SHARED and lock_type == LockType.SHARED:
                    return True
        return False
    
    def _can_acquire_lock(self, resource: str, lock_type: LockType) -> bool:
        """检查是否可以立即获取锁"""
        existing_locks = self.locks[resource]
        
        if not existing_locks:
            return True
        
        if lock_type == LockType.SHARED:
            # 共享锁：如果没有排他锁就可以获取
            return not any(lock.lock_type == LockType.EXCLUSIVE for lock in existing_locks)
        else:
            # 排他锁：必须没有任何锁
            return len(existing_locks) == 0
    
    def _process_wait_queue(self, resource: str):
        """处理等待队列"""
        if resource not in self.wait_queue:
            return
        
        waiting_locks = self.wait_queue[resource]
        locks_to_acquire = []
        locks_to_remove = []
        
        # 先找出可以获取的锁
        for lock in waiting_locks:
            if self._can_acquire_lock(resource, lock.lock_type):
                locks_to_acquire.append(lock)
                locks_to_remove.append(lock)
        
        # 移除已处理的锁
        for lock in locks_to_remove:
            waiting_locks.remove(lock)
        
        # 添加可获取的锁
        for lock in locks_to_acquire:
            self.locks[resource].append(lock)
    
    def detect_deadlock(self) -> Optional[List[str]]:
        """检测死锁（简化实现）"""
        # 这里应该实现死锁检测算法，如等待图检测
        # 简化实现，返回None表示无死锁
        return None


class TransactionManager:
    """事务管理器"""
    
    def __init__(self):
        self.transactions: Dict[str, Transaction] = {}
        self.lock_manager = LockManager()
        self.lock = threading.Lock()
    
    def begin_transaction(self) -> str:
        """开始事务"""
        transaction_id = str(uuid.uuid4())
        transaction = Transaction(
            transaction_id=transaction_id,
            state=TransactionState.ACTIVE,
            start_time=time.time()
        )
        
        with self.lock:
            self.transactions[transaction_id] = transaction
        
        return transaction_id
    
    def commit_transaction(self, transaction_id: str) -> bool:
        """提交事务"""
        with self.lock:
            if transaction_id not in self.transactions:
                return False
            
            transaction = self.transactions[transaction_id]
            if transaction.state != TransactionState.ACTIVE:
                return False
            
            # 检查死锁
            deadlock = self.lock_manager.detect_deadlock()
            if deadlock and transaction_id in deadlock:
                return self.abort_transaction(transaction_id)
            
            # 提交事务
            transaction.state = TransactionState.COMMITTED
            
            # 释放所有锁
            self.lock_manager.release_all_locks(transaction_id)
            
            # 清理事务
            del self.transactions[transaction_id]
            
            return True
    
    def abort_transaction(self, transaction_id: str) -> bool:
        """回滚事务"""
        with self.lock:
            if transaction_id not in self.transactions:
                return False
            
            transaction = self.transactions[transaction_id]
            if transaction.state != TransactionState.ACTIVE:
                return False
            
            # 回滚操作
            self._rollback_operations(transaction)
            
            # 标记为中止
            transaction.state = TransactionState.ABORTED
            
            # 释放所有锁
            self.lock_manager.release_all_locks(transaction_id)
            
            # 清理事务
            del self.transactions[transaction_id]
            
            return True
    
    def _rollback_operations(self, transaction: Transaction):
        """回滚事务操作"""
        # 按相反顺序回滚操作
        for operation in reversed(transaction.operations):
            self._rollback_operation(operation)
    
    def _rollback_operation(self, operation: Dict[str, Any]):
        """回滚单个操作"""
        op_type = operation.get("type")
        
        if op_type == "INSERT":
            # 回滚插入：删除记录
            table_name = operation.get("table_name")
            record_id = operation.get("record_id")
            # 这里应该调用存储引擎删除记录
            pass
        elif op_type == "UPDATE":
            # 回滚更新：恢复原值
            table_name = operation.get("table_name")
            record_id = operation.get("record_id")
            old_values = operation.get("old_values")
            # 这里应该调用存储引擎恢复记录
            pass
        elif op_type == "DELETE":
            # 回滚删除：恢复记录
            table_name = operation.get("table_name")
            record_id = operation.get("record_id")
            old_record = operation.get("old_record")
            # 这里应该调用存储引擎恢复记录
            pass
    
    def acquire_read_lock(self, transaction_id: str, resource: str) -> bool:
        """获取读锁"""
        return self.lock_manager.acquire_lock(transaction_id, resource, LockType.SHARED)
    
    def acquire_write_lock(self, transaction_id: str, resource: str) -> bool:
        """获取写锁"""
        return self.lock_manager.acquire_lock(transaction_id, resource, LockType.EXCLUSIVE)
    
    def log_operation(self, transaction_id: str, operation: Dict[str, Any]):
        """记录操作日志"""
        with self.lock:
            if transaction_id in self.transactions:
                self.transactions[transaction_id].operations.append(operation)
    
    def get_transaction_info(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """获取事务信息"""
        with self.lock:
            if transaction_id not in self.transactions:
                return None
            
            transaction = self.transactions[transaction_id]
            return {
                "transaction_id": transaction_id,
                "state": transaction.state.value,
                "start_time": transaction.start_time,
                "duration": time.time() - transaction.start_time,
                "read_set": list(transaction.read_set),
                "write_set": list(transaction.write_set),
                "lock_count": len(transaction.locks),
                "operation_count": len(transaction.operations)
            }
    
    def get_all_transactions(self) -> List[Dict[str, Any]]:
        """获取所有事务信息"""
        with self.lock:
            return [
                self.get_transaction_info(tid) 
                for tid in self.transactions.keys()
            ]


class TransactionContext:
    """事务上下文管理器"""
    
    def __init__(self, transaction_manager: TransactionManager):
        self.transaction_manager = transaction_manager
        self.transaction_id: Optional[str] = None
    
    def __enter__(self):
        self.transaction_id = self.transaction_manager.begin_transaction()
        return self.transaction_id
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.transaction_id:
            if exc_type is None:
                # 没有异常，提交事务
                self.transaction_manager.commit_transaction(self.transaction_id)
            else:
                # 有异常，回滚事务
                self.transaction_manager.abort_transaction(self.transaction_id)


if __name__ == "__main__":
    # 测试事务管理器
    print("测试事务管理器:")
    
    tm = TransactionManager()
    
    # 开始事务
    txn_id = tm.begin_transaction()
    print(f"开始事务: {txn_id}")
    
    # 获取锁
    print(f"获取读锁: {tm.acquire_read_lock(txn_id, 'table1')}")
    print(f"获取写锁: {tm.acquire_write_lock(txn_id, 'table2')}")
    
    # 记录操作
    tm.log_operation(txn_id, {
        "type": "INSERT",
        "table_name": "student",
        "record_id": 1,
        "values": {"id": 1, "name": "Alice"}
    })
    
    # 获取事务信息
    info = tm.get_transaction_info(txn_id)
    print(f"事务信息: {info}")
    
    # 提交事务
    print(f"提交事务: {tm.commit_transaction(txn_id)}")
    
    # 测试事务上下文
    print("\n测试事务上下文:")
    with TransactionContext(tm) as ctx_txn_id:
        print(f"上下文事务ID: {ctx_txn_id}")
        tm.acquire_read_lock(ctx_txn_id, "table3")
        tm.log_operation(ctx_txn_id, {
            "type": "SELECT",
            "table_name": "student",
            "condition": "id = 1"
        })
    
    print("事务上下文测试完成")






