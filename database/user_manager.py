"""
用户数据库绑定管理器
管理用户ID与数据库文件的绑定关系
"""
import os
import json
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class UserDatabaseBinding:
    """用户数据库绑定信息"""
    user_id: str
    db_file: str
    created_at: str
    last_accessed: str


class UserManager:
    """用户数据库绑定管理器"""
    
    def __init__(self, binding_file: str = "user_bindings.json"):
        self.binding_file = binding_file
        self.bindings: Dict[str, UserDatabaseBinding] = {}  # db_file -> binding
        self.user_databases: Dict[str, Set[str]] = {}  # user_id -> set of db_files
        self._load_bindings()
    
    def _load_bindings(self):
        """从文件加载绑定信息"""
        if os.path.exists(self.binding_file):
            try:
                with open(self.binding_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                for binding_data in data.get('bindings', []):
                    binding = UserDatabaseBinding(**binding_data)
                    self.bindings[binding.db_file] = binding
                    
                    # 更新用户数据库映射
                    if binding.user_id not in self.user_databases:
                        self.user_databases[binding.user_id] = set()
                    self.user_databases[binding.user_id].add(binding.db_file)
                    
            except (json.JSONDecodeError, KeyError) as e:
                print(f"加载用户绑定文件失败: {e}")
                self._initialize_empty_bindings()
        else:
            self._initialize_empty_bindings()
    
    def _initialize_empty_bindings(self):
        """初始化空的绑定数据"""
        self.bindings = {}
        self.user_databases = {}
        self._save_bindings()
    
    def _save_bindings(self):
        """保存绑定信息到文件"""
        data = {
            'bindings': [asdict(binding) for binding in self.bindings.values()]
        }
        
        try:
            with open(self.binding_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"保存用户绑定文件失败: {e}")
    
    def bind_database(self, user_id: str, db_file: str) -> bool:
        """绑定数据库文件到用户"""
        import datetime
        
        # 检查数据库文件是否已被其他用户绑定
        if db_file in self.bindings:
            existing_binding = self.bindings[db_file]
            if existing_binding.user_id != user_id:
                print(f"数据库文件 {db_file} 已被用户 {existing_binding.user_id} 绑定")
                return False
        
        now = datetime.datetime.now().isoformat()
        
        # 创建或更新绑定
        binding = UserDatabaseBinding(
            user_id=user_id,
            db_file=db_file,
            created_at=now,
            last_accessed=now
        )
        
        self.bindings[db_file] = binding
        
        # 更新用户数据库映射
        if user_id not in self.user_databases:
            self.user_databases[user_id] = set()
        self.user_databases[user_id].add(db_file)
        
        self._save_bindings()
        print(f"成功将数据库 {db_file} 绑定到用户 {user_id}")
        return True
    
    def unbind_database(self, user_id: str, db_file: str) -> bool:
        """解绑数据库文件"""
        if db_file not in self.bindings:
            print(f"数据库文件 {db_file} 未被绑定")
            return False
        
        binding = self.bindings[db_file]
        if binding.user_id != user_id:
            print(f"数据库文件 {db_file} 不属于用户 {user_id}")
            return False
        
        # 删除绑定
        del self.bindings[db_file]
        self.user_databases[user_id].discard(db_file)
        
        # 如果用户没有其他数据库，删除用户记录
        if not self.user_databases[user_id]:
            del self.user_databases[user_id]
        
        self._save_bindings()
        print(f"成功解绑用户 {user_id} 的数据库 {db_file}")
        return True
    
    def get_user_databases(self, user_id: str) -> List[str]:
        """获取用户的所有数据库文件"""
        return list(self.user_databases.get(user_id, []))
    
    def get_database_owner(self, db_file: str) -> Optional[str]:
        """获取数据库文件的所有者"""
        binding = self.bindings.get(db_file)
        return binding.user_id if binding else None
    
    def is_database_bound(self, db_file: str) -> bool:
        """检查数据库文件是否已被绑定"""
        return db_file in self.bindings
    
    def update_last_accessed(self, db_file: str):
        """更新数据库最后访问时间"""
        if db_file in self.bindings:
            import datetime
            self.bindings[db_file].last_accessed = datetime.datetime.now().isoformat()
            self._save_bindings()
    
    def get_binding_info(self, db_file: str) -> Optional[UserDatabaseBinding]:
        """获取数据库绑定信息"""
        return self.bindings.get(db_file)
    
    def list_all_bindings(self) -> List[UserDatabaseBinding]:
        """列出所有绑定信息"""
        return list(self.bindings.values())
    
    def get_user_stats(self, user_id: str) -> Dict[str, any]:
        """获取用户统计信息"""
        databases = self.get_user_databases(user_id)
        return {
            'user_id': user_id,
            'database_count': len(databases),
            'databases': databases
        }


def main():
    """测试用户管理器"""
    manager = UserManager("test_user_bindings.json")
    
    print("用户管理器测试:")
    
    # 测试绑定
    print("\n1. 测试绑定数据库:")
    manager.bind_database("user1", "test1.db")
    manager.bind_database("user1", "test2.db")
    manager.bind_database("user2", "test3.db")
    
    # 测试重复绑定
    print("\n2. 测试重复绑定:")
    manager.bind_database("user1", "test1.db")  # 应该成功（更新）
    manager.bind_database("user2", "test1.db")  # 应该失败（已被绑定）
    
    # 测试获取用户数据库
    print("\n3. 获取用户数据库:")
    user1_dbs = manager.get_user_databases("user1")
    print(f"用户1的数据库: {user1_dbs}")
    
    user2_dbs = manager.get_user_databases("user2")
    print(f"用户2的数据库: {user2_dbs}")
    
    # 测试获取数据库所有者
    print("\n4. 获取数据库所有者:")
    owner1 = manager.get_database_owner("test1.db")
    print(f"test1.db的所有者: {owner1}")
    
    # 测试解绑
    print("\n5. 测试解绑:")
    manager.unbind_database("user1", "test2.db")
    user1_dbs_after = manager.get_user_databases("user1")
    print(f"解绑后用户1的数据库: {user1_dbs_after}")
    
    # 测试统计信息
    print("\n6. 用户统计信息:")
    stats = manager.get_user_stats("user1")
    print(f"用户1统计: {stats}")
    
    # 清理测试文件
    if os.path.exists("test_user_bindings.json"):
        os.remove("test_user_bindings.json")


if __name__ == "__main__":
    main()
