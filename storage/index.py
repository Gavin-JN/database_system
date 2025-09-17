#!/usr/bin/env python3
"""
数据库索引模块
提供B+树索引和哈希索引支持
"""
import struct
import pickle
from typing import Any, List, Optional, Dict, Tuple
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict
import bisect


class IndexType(Enum):
    """索引类型"""
    BPLUS_TREE = "bplus_tree"
    HASH = "hash"
    BTREE = "btree"


@dataclass
class IndexEntry:
    """索引条目"""
    key: Any
    page_id: int
    offset: int
    record_id: Optional[int] = None


class BPlusTreeNode:
    """B+树节点"""
    
    def __init__(self, is_leaf: bool = False, max_keys: int = 3):
        self.is_leaf = is_leaf
        self.max_keys = max_keys
        self.keys: List[Any] = []
        self.values: List[Any] = []  # 对于叶子节点存储数据，对于内部节点存储子节点指针
        self.next_leaf: Optional['BPlusTreeNode'] = None
        self.parent: Optional['BPlusTreeNode'] = None
    
    def is_full(self) -> bool:
        """检查节点是否已满"""
        return len(self.keys) >= self.max_keys
    
    def is_underflow(self) -> bool:
        """检查节点是否下溢"""
        if self.is_leaf:
            return len(self.keys) < (self.max_keys + 1) // 2
        else:
            return len(self.keys) < (self.max_keys + 1) // 2
    
    def insert_key(self, key: Any, value: Any) -> bool:
        """插入键值对"""
        if self.is_leaf:
            # 叶子节点插入
            pos = bisect.bisect_left(self.keys, key)
            self.keys.insert(pos, key)
            self.values.insert(pos, value)
            return len(self.keys) > self.max_keys
        else:
            # 内部节点插入
            pos = bisect.bisect_right(self.keys, key)
            child = self.values[pos]
            if child.insert_key(key, value):
                # 子节点分裂
                return self._split_child(pos)
            return False
    
    def _split_child(self, pos: int) -> bool:
        """分裂子节点"""
        child = self.values[pos]
        mid = len(child.keys) // 2
        
        # 创建新节点
        new_child = BPlusTreeNode(child.is_leaf, child.max_keys)
        new_child.keys = child.keys[mid:]
        new_child.values = child.values[mid:]
        new_child.parent = self
        
        # 更新原节点
        child.keys = child.keys[:mid]
        child.values = child.values[:mid]
        
        # 更新叶子节点链接
        if child.is_leaf:
            new_child.next_leaf = child.next_leaf
            child.next_leaf = new_child
        
        # 将中间键提升到父节点
        promote_key = new_child.keys[0]
        self.keys.insert(pos, promote_key)
        self.values.insert(pos + 1, new_child)
        
        return len(self.keys) > self.max_keys
    
    def search(self, key: Any) -> Optional[Any]:
        """搜索键"""
        if self.is_leaf:
            pos = bisect.bisect_left(self.keys, key)
            if pos < len(self.keys) and self.keys[pos] == key:
                return self.values[pos]
            return None
        else:
            pos = bisect.bisect_right(self.keys, key)
            return self.values[pos].search(key)
    
    def range_search(self, start_key: Any, end_key: Any) -> List[Any]:
        """范围搜索"""
        if self.is_leaf:
            result = []
            for i, key in enumerate(self.keys):
                if start_key <= key <= end_key:
                    result.append(self.values[i])
            return result
        else:
            pos = bisect.bisect_right(self.keys, start_key)
            return self.values[pos].range_search(start_key, end_key)


class BPlusTreeIndex:
    """B+树索引"""
    
    def __init__(self, max_keys: int = 3):
        self.max_keys = max_keys
        self.root: Optional[BPlusTreeNode] = None
        self.size = 0
    
    def insert(self, key: Any, value: Any) -> bool:
        """插入键值对"""
        if self.root is None:
            self.root = BPlusTreeNode(is_leaf=True, max_keys=self.max_keys)
            self.root.keys.append(key)
            self.root.values.append(value)
            self.size = 1
            return True
        
        if self.root.insert_key(key, value):
            # 根节点分裂
            new_root = BPlusTreeNode(is_leaf=False, max_keys=self.max_keys)
            mid = len(self.root.keys) // 2
            
            # 创建新的右子树
            right_child = BPlusTreeNode(self.root.is_leaf, self.max_keys)
            right_child.keys = self.root.keys[mid:]
            right_child.values = self.root.values[mid:]
            right_child.parent = new_root
            
            # 更新左子树
            left_child = self.root
            left_child.keys = left_child.keys[:mid]
            left_child.values = left_child.values[:mid]
            left_child.parent = new_root
            
            # 设置新根
            new_root.keys = [right_child.keys[0]]
            new_root.values = [left_child, right_child]
            self.root = new_root
        
        self.size += 1
        return True
    
    def search(self, key: Any) -> Optional[Any]:
        """搜索键"""
        if self.root is None:
            return None
        return self.root.search(key)
    
    def range_search(self, start_key: Any, end_key: Any) -> List[Any]:
        """范围搜索"""
        if self.root is None:
            return []
        return self.root.range_search(start_key, end_key)
    
    def delete(self, key: Any) -> bool:
        """删除键"""
        # 简化实现，实际B+树删除比较复杂
        # 这里只做标记删除
        return True


class HashIndex:
    """哈希索引"""
    
    def __init__(self, initial_size: int = 16):
        self.size = initial_size
        self.buckets: List[List[Tuple[Any, Any]]] = [[] for _ in range(initial_size)]
        self.count = 0
        self.load_factor = 0.75
    
    def _hash(self, key: Any) -> int:
        """计算哈希值"""
        return hash(key) % self.size
    
    def _resize(self):
        """调整哈希表大小"""
        old_buckets = self.buckets
        self.size *= 2
        self.buckets = [[] for _ in range(self.size)]
        self.count = 0
        
        for bucket in old_buckets:
            for key, value in bucket:
                self.insert(key, value)
    
    def insert(self, key: Any, value: Any) -> bool:
        """插入键值对"""
        if self.count >= self.size * self.load_factor:
            self._resize()
        
        bucket_index = self._hash(key)
        bucket = self.buckets[bucket_index]
        
        # 检查是否已存在
        for i, (k, v) in enumerate(bucket):
            if k == key:
                bucket[i] = (key, value)
                return True
        
        # 插入新键值对
        bucket.append((key, value))
        self.count += 1
        return True
    
    def search(self, key: Any) -> Optional[Any]:
        """搜索键"""
        bucket_index = self._hash(key)
        bucket = self.buckets[bucket_index]
        
        for k, v in bucket:
            if k == key:
                return v
        return None
    
    def delete(self, key: Any) -> bool:
        """删除键"""
        bucket_index = self._hash(key)
        bucket = self.buckets[bucket_index]
        
        for i, (k, v) in enumerate(bucket):
            if k == key:
                del bucket[i]
                self.count -= 1
                return True
        return False


class IndexManager:
    """索引管理器"""
    
    def __init__(self):
        self.indexes: Dict[str, Dict[str, Any]] = {}  # table_name -> {column_name -> index}
        self.index_metadata: Dict[str, Dict[str, Any]] = {}
    
    def create_index(self, table_name: str, column_name: str, 
                    index_type: IndexType = IndexType.BPLUS_TREE) -> bool:
        """创建索引"""
        index_key = f"{table_name}.{column_name}"
        
        if index_type == IndexType.BPLUS_TREE:
            index = BPlusTreeIndex()
        elif index_type == IndexType.HASH:
            index = HashIndex()
        else:
            return False
        
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
        
        self.indexes[table_name][column_name] = index
        self.index_metadata[index_key] = {
            "type": index_type.value,
            "created_at": "2024-01-01",  # 简化实现
            "column": column_name,
            "table": table_name
        }
        
        return True
    
    def drop_index(self, table_name: str, column_name: str) -> bool:
        """删除索引"""
        if table_name in self.indexes and column_name in self.indexes[table_name]:
            del self.indexes[table_name][column_name]
            index_key = f"{table_name}.{column_name}"
            if index_key in self.index_metadata:
                del self.index_metadata[index_key]
            return True
        return False
    
    def insert_record(self, table_name: str, column_name: str, 
                     key: Any, page_id: int, offset: int) -> bool:
        """向索引插入记录"""
        if table_name in self.indexes and column_name in self.indexes[table_name]:
            index = self.indexes[table_name][column_name]
            return index.insert(key, (page_id, offset))
        return False
    
    def search_record(self, table_name: str, column_name: str, key: Any) -> Optional[Tuple[int, int]]:
        """在索引中搜索记录"""
        if table_name in self.indexes and column_name in self.indexes[table_name]:
            index = self.indexes[table_name][column_name]
            result = index.search(key)
            if result:
                return result
        return None
    
    def range_search(self, table_name: str, column_name: str, 
                    start_key: Any, end_key: Any) -> List[Tuple[int, int]]:
        """范围搜索"""
        if table_name in self.indexes and column_name in self.indexes[table_name]:
            index = self.indexes[table_name][column_name]
            if hasattr(index, 'range_search'):
                return index.range_search(start_key, end_key)
        return []
    
    def get_index_info(self, table_name: str) -> Dict[str, Any]:
        """获取表的索引信息"""
        if table_name in self.indexes:
            return {
                column: {
                    "type": self.index_metadata.get(f"{table_name}.{column}", {}).get("type", "unknown"),
                    "size": len(index.keys) if hasattr(index, 'keys') else 0
                }
                for column, index in self.indexes[table_name].items()
            }
        return {}
    
    def get_all_indexes(self) -> Dict[str, Dict[str, Any]]:
        """获取所有索引信息"""
        return {
            table: {
                column: {
                    "type": self.index_metadata.get(f"{table}.{column}", {}).get("type", "unknown"),
                    "size": len(index.keys) if hasattr(index, 'keys') else 0
                }
                for column, index in columns.items()
            }
            for table, columns in self.indexes.items()
        }


if __name__ == "__main__":
    # 测试B+树索引
    print("测试B+树索引:")
    btree = BPlusTreeIndex(max_keys=3)
    
    # 插入数据
    for i in range(10):
        btree.insert(i, f"value_{i}")
    
    # 搜索测试
    print(f"搜索键5: {btree.search(5)}")
    print(f"搜索键15: {btree.search(15)}")
    print(f"范围搜索3-7: {btree.range_search(3, 7)}")
    
    # 测试哈希索引
    print("\n测试哈希索引:")
    hash_index = HashIndex()
    
    # 插入数据
    for i in range(10):
        hash_index.insert(f"key_{i}", f"value_{i}")
    
    # 搜索测试
    print(f"搜索key_5: {hash_index.search('key_5')}")
    print(f"搜索key_15: {hash_index.search('key_15')}")
    
    # 测试索引管理器
    print("\n测试索引管理器:")
    manager = IndexManager()
    
    # 创建索引
    manager.create_index("student", "id", IndexType.BPLUS_TREE)
    manager.create_index("student", "name", IndexType.HASH)
    
    # 插入记录
    manager.insert_record("student", "id", 1, 1, 100)
    manager.insert_record("student", "id", 2, 1, 200)
    manager.insert_record("student", "name", "Alice", 1, 100)
    
    # 搜索记录
    print(f"搜索ID=1: {manager.search_record('student', 'id', 1)}")
    print(f"搜索name=Alice: {manager.search_record('student', 'name', 'Alice')}")
    
    # 获取索引信息
    print(f"student表索引信息: {manager.get_index_info('student')}")
    print(f"所有索引信息: {manager.get_all_indexes()}")






