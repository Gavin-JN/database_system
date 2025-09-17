"""
系统目录管理
维护数据库的元数据（表名、列名、列类型等）
系统目录本身作为一张特殊的表进行存储和管理
"""
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
from storage.storage_engine import StorageEngine, ColumnInfo, DataType, Record


@dataclass
class TableMetadata:
    """表元数据"""
    table_name: str
    columns: List[Dict[str, str]]  # [{"name": "col1", "type": "INT"}, ...]
    created_at: str
    page_count: int = 0


class SystemCatalog:
    """系统目录"""
    
    CATALOG_TABLE_NAME = "pg_catalog"
    
    def __init__(self, storage_engine: StorageEngine):
        self.storage_engine = storage_engine
        self.tables: Dict[str, TableMetadata] = {}
        self._initialize_catalog()
    
    def _initialize_catalog(self):
        """初始化系统目录"""
        # 检查目录表是否存在
        if not self.storage_engine.table_exists(self.CATALOG_TABLE_NAME):
            self._create_catalog_table()
        else:
            self._load_catalog_from_storage()
        
        # 确保系统目录表在存储引擎中存在
        if self.CATALOG_TABLE_NAME not in self.storage_engine.tables:
            self._create_catalog_table()
        
        # 再次尝试从存储加载目录信息
        self._load_catalog_from_storage()
    
    def _create_catalog_table(self):
        """创建系统目录表"""
        columns = [
            ColumnInfo("table_name", DataType.VARCHAR),
            ColumnInfo("column_info", DataType.VARCHAR),  # JSON格式存储列信息
            ColumnInfo("created_at", DataType.VARCHAR),
            ColumnInfo("page_count", DataType.INT)
        ]
        
        self.storage_engine.create_table(self.CATALOG_TABLE_NAME, columns)
        # 刷新到磁盘
        self.storage_engine.flush_all()
    
    def _load_catalog_from_storage(self):
        """从存储加载目录信息"""
        records = self.storage_engine.select_records(self.CATALOG_TABLE_NAME)
        
        for record in records:
            if record.is_deleted:
                continue
            
            table_name = record.get_value("table_name")
            column_info_json = record.get_value("column_info")
            created_at = record.get_value("created_at")
            page_count = record.get_value("page_count") or 0
            
            try:
                columns = json.loads(column_info_json) if column_info_json else []
                self.tables[table_name] = TableMetadata(
                    table_name=table_name,
                    columns=columns,
                    created_at=created_at,
                    page_count=page_count
                )
            except json.JSONDecodeError as e:
                print(f"解析表 {table_name} 的列信息失败: {e}")
    
    def create_table(self, table_name: str, columns: List[Dict[str, str]]) -> bool:
        """在目录中注册新表"""
        if table_name in self.tables:
            return False
        
        import datetime
        created_at = datetime.datetime.now().isoformat()
        
        metadata = TableMetadata(
            table_name=table_name,
            columns=columns,
            created_at=created_at,
            page_count=0
        )
        
        self.tables[table_name] = metadata
        print(f"在目录中注册新表: {table_name}")
        
        # 保存到存储
        return self._save_table_metadata(metadata)
    
    def add_table(self, table_name: str, columns: List[ColumnInfo]) -> bool:
        """添加表（兼容性方法）"""
        # 转换ColumnInfo为字典格式
        column_dicts = []
        for col in columns:
            column_dicts.append({
                'name': col.name,
                'type': col.data_type.name
            })
        
        return self.create_table(table_name, column_dicts)
    
    def drop_table(self, table_name: str) -> bool:
        """从目录中删除表"""
        if table_name not in self.tables:
            return False
        
        # 从存储中删除记录
        deleted_count = self.storage_engine.delete_records(
            self.CATALOG_TABLE_NAME,
            {"column": "table_name", "operator": "=", "value": table_name}
        )
        
        if deleted_count > 0:
            del self.tables[table_name]
            return True
        
        return False
    
    def get_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """获取表元数据"""
        return self.tables.get(table_name)
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.tables
    
    def get_all_tables(self) -> List[str]:
        """获取所有表名"""
        return list(self.tables.keys())
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """获取表的列信息"""
        metadata = self.get_table_metadata(table_name)
        if metadata:
            return metadata.columns
        return []
    
    def update_table_page_count(self, table_name: str, page_count: int) -> bool:
        """更新表的页数"""
        if table_name not in self.tables:
            return False
        
        self.tables[table_name].page_count = page_count
        
        # 更新存储中的记录
        return self._save_table_metadata(self.tables[table_name])
    
    def _save_table_metadata(self, metadata: TableMetadata) -> bool:
        """保存表元数据到存储"""
        # 先删除旧记录
        self.storage_engine.delete_records(
            self.CATALOG_TABLE_NAME,
            {"column": "table_name", "operator": "=", "value": metadata.table_name}
        )
        
        # 插入新记录
        record_data = {
            "table_name": metadata.table_name,
            "column_info": json.dumps(metadata.columns),
            "created_at": metadata.created_at,
            "page_count": metadata.page_count
        }
        
        print(f"准备保存表元数据 {metadata.table_name}: {record_data}")
        result = self.storage_engine.insert_record(self.CATALOG_TABLE_NAME, record_data)
        print(f"保存表元数据 {metadata.table_name}: {result}")
        
        # 强制刷新
        self.storage_engine.flush_all()
        
        # 验证数据是否真的被写入
        records = self.storage_engine.select_records(self.CATALOG_TABLE_NAME)
        print(f"验证pg_catalog表记录数: {len(records)}")
        for record in records:
            print(f"  - {record.data}")
        
        return result
    
    def get_catalog_info(self) -> Dict[str, Any]:
        """获取目录信息"""
        return {
            "total_tables": len(self.tables),
            "tables": [
                {
                    "name": metadata.table_name,
                    "columns": metadata.columns,
                    "created_at": metadata.created_at,
                    "page_count": metadata.page_count
                }
                for metadata in self.tables.values()
            ]
        }
    
    def print_catalog(self):
        """打印目录信息"""
        info = self.get_catalog_info()
        print("系统目录信息:")
        print(f"  总表数: {info['total_tables']}")
        
        for table_info in info['tables']:
            print(f"\n表: {table_info['name']}")
            print(f"  创建时间: {table_info['created_at']}")
            print(f"  页数: {table_info['page_count']}")
            print(f"  列信息:")
            for col in table_info['columns']:
                print(f"    {col['name']}: {col['type']}")


def main():
    """测试系统目录"""
    from ..storage.storage_engine import StorageEngine
    
    # 创建存储引擎和系统目录
    storage_engine = StorageEngine("test_catalog.dat")
    catalog = SystemCatalog(storage_engine)
    
    print("系统目录测试:")
    
    # 创建一些表
    tables = [
        {
            "name": "student",
            "columns": [
                {"name": "id", "type": "INT"},
                {"name": "name", "type": "VARCHAR"},
                {"name": "age", "type": "INT"}
            ]
        },
        {
            "name": "course",
            "columns": [
                {"name": "id", "type": "INT"},
                {"name": "title", "type": "VARCHAR"},
                {"name": "credits", "type": "INT"}
            ]
        }
    ]
    
    for table in tables:
        success = catalog.create_table(table["name"], table["columns"])
        print(f"创建表 {table['name']}: {'成功' if success else '失败'}")
    
    # 打印目录信息
    catalog.print_catalog()
    
    # 测试查询
    print(f"\n表 'student' 是否存在: {catalog.table_exists('student')}")
    print(f"表 'teacher' 是否存在: {catalog.table_exists('teacher')}")
    
    columns = catalog.get_table_columns("student")
    print(f"表 'student' 的列: {columns}")
    
    # 更新页数
    catalog.update_table_page_count("student", 5)
    print(f"更新表 'student' 的页数为 5")
    
    # 删除表
    success = catalog.drop_table("course")
    print(f"删除表 'course': {'成功' if success else '失败'}")
    
    # 再次打印目录
    print("\n删除后的目录信息:")
    catalog.print_catalog()
    
    # 刷新数据
    storage_engine.flush_all()


if __name__ == "__main__":
    main()
