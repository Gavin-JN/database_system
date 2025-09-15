"""
主数据库系统
集成SQL编译器、存储引擎、执行引擎和系统目录
提供统一的数据库接口
"""
import os
from typing import List, Dict, Any, Optional
from sql_compiler.lexer import SQLLexer
from sql_compiler.parser import SQLParser
from sql_compiler.semantic import SemanticAnalyzer, Catalog as SemanticCatalog
from sql_compiler.planner import PlanGenerator
from storage.storage_engine import StorageEngine, ColumnInfo, DataType
from .execution_engine import ExecutionEngine
from .catalog import SystemCatalog
from utils.logger import DatabaseLogger, LogLevel, logger
import time

class Database:
    """主数据库类"""
    
    def __init__(self, data_file: str = "database.db"):
        self.data_file = data_file
        self.storage_engine = StorageEngine(data_file)
        self.execution_engine = ExecutionEngine(self.storage_engine)
        self.system_catalog = SystemCatalog(self.storage_engine)
        
        # SQL编译器组件
        self.lexer = SQLLexer()
        self.parser = SQLParser([])
        self.semantic_catalog = SemanticCatalog()
        self.semantic_analyzer = SemanticAnalyzer(self.semantic_catalog)
        self.planner = PlanGenerator()
        
        # 同步语义目录和系统目录
        self._sync_catalogs()
        
        # 确保数据持久化
        self.storage_engine.flush_all()
    
    def _sync_catalogs(self):
        """同步语义目录和系统目录"""
        # 从系统目录加载表信息到语义目录
        for table_name in self.system_catalog.get_all_tables():
            columns_info = self.system_catalog.get_table_columns(table_name)
            column_infos = []
            for col_info in columns_info:
                column_infos.append(ColumnInfo(
                    name=col_info['name'],
                    data_type=DataType(col_info['type'])
                ))
            
            # 创建表（如果不存在）
            if not self.semantic_catalog.table_exists(table_name):
                self.semantic_catalog.create_table(table_name, column_infos)
    
    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """执行SQL语句"""
        now=time.time()
        try:
            # 1. 词法分析
            tokens = self.lexer.tokenize(sql)
            
            # 2. 语法分析
            self.parser = SQLParser(tokens)
            ast_nodes = self.parser.parse()
            
            # 3. 语义分析
            semantic_results = self.semantic_analyzer.analyze(ast_nodes)
            
            # 检查语义错误
            errors = [result for result in semantic_results if result.startswith('[')]
            if errors:
                return {
                    'sql':sql,
                    'success': False,
                    'message': '语义分析错误',
                    'errors': errors,
                    'data': [],
                    'duration':time.time()-now,
                    'row_affected':0
                }
            
            # 4. 生成执行计划
            plans = self.planner.generate_plan(ast_nodes)
            print(f"生成的执行计划数量: {len(plans)}")
            for i, plan in enumerate(plans):
                print(f"  计划 {i}: {plan.operator_type.value}")
            
            # 5. 执行计划
            results = []
            for plan in plans:
                result = self.execution_engine.execute_plan(plan)
                results.append(result)
                
                # 如果执行失败，返回错误
                if not result.success:
                    return {
                        'sql':sql,
                        'success': False,
                        'message': result.message,
                        'data': [],
                        'duration': time.time() - now,
                        'row_affected':0
                    }
            
            # 6. 更新系统目录
            self._update_catalog_from_plans(plans)
            
            # 7. 同步目录
            self._sync_catalogs()
            
            # 8. 刷新数据
            self.storage_engine.flush_all()
            
            # 返回结果
            if results:
                last_result = results[-1]
                return {
                    'sql':sql,
                    'success': True,
                    'message': last_result.message,
                    'data': last_result.data,
                    'rows_affected': last_result.rows_affected,
                    'duration': time.time() - now
                }
            else:
                return {
                    'sql':sql,
                    'success': True,
                    'message': '执行完成',
                    'data': [],
                    'duration': time.time() - now,
                    'rows_affected': 0
                }
                
        except Exception as e:
            return {
                'sql':sql,
                'success': False,
                'message': f'执行错误: {str(e)}',
                'data': [],
                'duration': time.time() - now,
                'rows_affected': 0
            }
    
    def _update_catalog_from_plans(self, plans: List):
        """从执行计划更新系统目录"""
        for plan in plans:
            if plan.operator_type.value == "CreateTable":
                # 创建表时，更新系统目录
                table_name = plan.table_name
                columns = plan.columns
                
                print(f"准备更新系统目录，表名: {table_name}, 列: {columns}")
                
                if not self.system_catalog.table_exists(table_name):
                    result = self.system_catalog.create_table(table_name, columns)
                    print(f"系统目录已更新：添加表 {table_name}, 结果: {result}")
                else:
                    print(f"表 {table_name} 已存在，跳过")
    
    def get_tables(self) -> List[str]:
        """获取所有表名"""
        return self.system_catalog.get_all_tables()
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取表信息"""
        metadata = self.system_catalog.get_table_metadata(table_name)
        if metadata:
            return {
                'name': metadata.table_name,
                'columns': metadata.columns,
                'created_at': metadata.created_at,
                'page_count': metadata.page_count
            }
        return None
    
    def get_database_info(self) -> Dict[str, Any]:
        """获取数据库信息"""
        storage_stats = self.storage_engine.get_stats()
        catalog_info = self.system_catalog.get_catalog_info()
        
        return {
            'database_file': self.data_file,
            'tables': catalog_info['total_tables'],
            'storage': storage_stats,
            'catalog': catalog_info
        }
    
    def close(self):
        """关闭数据库"""
        self.storage_engine.flush_all()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class DatabaseCLI:
    """数据库命令行界面"""
    
    def __init__(self, database: Database, user_id: str = None):
        self.database = database
        self.user_id = user_id
        self.running = True
    
    def start(self):
        """启动CLI"""
        print("欢迎使用数据库系统!")
        if self.user_id:
            print(f"当前用户: {self.user_id}")
        print("输入 'help' 查看帮助，输入 'exit' 退出")
        print("=" * 50)
        logger.info("数据库系统启动")
        while self.running:
            try:
                prompt = f"db({self.user_id})> " if self.user_id else "db> "
                command = input(prompt).strip()
                
                if command.lower() in ['exit', 'quit']:
                    self.running = False
                    logger.info("退出数据库系统")
                    break
                elif command.lower() == 'help':
                    self._show_help()
                    logger.info("查看帮助")
                elif command.lower() == 'tables':
                    self._show_tables()
                    logger.info("查看所有表格")
                elif command.lower() == 'info':
                    self._show_database_info()
                    logger.info("展示数据库详细信息")
                elif command.lower().startswith('desc '):
                    table_name = command[5:].strip()
                    self._describe_table(table_name)
                    logger.info("查看表"+table_name+"的详细信息")
                elif command.lower() == 'userinfo':
                    self._show_user_info()
                    logger.info("查看用户信息")
                elif command.lower() == 'listdbs':
                    self._list_user_databases()
                    logger.info("列出用户数据库")
                else:
                    # 执行SQL
                    result = self.database.execute_sql(command)
                    self._print_result(result)
                    if result.get('success'):
                        logger.log_sql_execution(result.get('sql'),True,result.get('duration'),result.get('rows_affected'))
                    else:
                        logger.log_sql_execution(result.get('sql'),False,result.get('duration'),result.get('rows_affected'),result.get('message'))
            except KeyboardInterrupt:
                print("\n使用 'exit' 命令退出")
            except Exception as e:
                print(f"错误: {e}")
        
        print("再见!")
        self.database.close()
    
    def _show_help(self):
        """显示帮助信息"""
        print("可用命令:")
        print("  help                    - 显示此帮助信息")
        print("  tables                  - 显示所有表")
        print("  desc <table_name>       - 显示表结构")
        print("  info                    - 显示数据库信息")
        print("  userinfo                - 显示用户信息")
        print("  listdbs                 - 列出用户的所有数据库")
        print("  exit/quit               - 退出程序")
        print("\nSQL语句:")
        print("  CREATE TABLE table_name (col1 type1, col2 type2, ...);")
        print("  INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...);")
        print("  SELECT col1, col2 FROM table_name [WHERE condition];")
        print("  DELETE FROM table_name [WHERE condition];")
    
    def _show_tables(self):
        """显示所有表"""
        tables = self.database.get_tables()
        if tables:
            print("表列表:")
            for table in tables:
                print(f"  {table}")
        else:
            print("没有表")
    
    def _describe_table(self, table_name: str):
        """显示表结构"""
        table_info = self.database.get_table_info(table_name)
        if table_info:
            print(f"表: {table_info['name']}")
            print(f"创建时间: {table_info['created_at']}")
            print(f"页数: {table_info['page_count']}")
            print("列信息:")
            for col in table_info['columns']:
                print(f"  {col['name']}: {col['type']}")
        else:
            print(f"表 '{table_name}' 不存在")
    
    def _show_database_info(self):
        """显示数据库信息"""
        info = self.database.get_database_info()
        print("数据库信息:")
        print(f"  数据库文件: {info['database_file']}")
        print(f"  表数量: {info['tables']}")
        
        storage = info['storage']
        print(f"  存储统计:")
        print(f"    缓存命中率: {storage['cache']['hit_rate']:.2%}")
        print(f"    总页数: {storage['pages']['total_pages']}")
        print(f"    空闲页数: {storage['pages']['free_pages']}")
    
    def _print_result(self, result: Dict[str, Any]):
        """打印执行结果"""
        if result['success']:
            print(f"✓ {result['message']}")
            
            if result['data']:
                print("查询结果:")
                for row in result['data']:
                    print(f"  {row}")
            
            if result.get('rows_affected', 0) > 0:
                print(f"影响行数: {result['rows_affected']}")
        else:
            print(f"✗ {result['message']}")
            
            if 'errors' in result:
                for error in result['errors']:
                    print(f"  {error}")
    
    def _show_user_info(self):
        """显示用户信息"""
        if not self.user_id:
            print("当前未指定用户")
            return
        
        from database.user_manager import UserManager
        user_manager = UserManager()
        
        print(f"用户信息:")
        print(f"  用户ID: {self.user_id}")
        print(f"  当前数据库: {self.database.data_file}")
        
        # 获取绑定信息
        binding_info = user_manager.get_binding_info(self.database.data_file)
        if binding_info:
            print(f"  绑定时间: {binding_info.created_at}")
            print(f"  最后访问: {binding_info.last_accessed}")
        
        # 获取用户统计
        stats = user_manager.get_user_stats(self.user_id)
        print(f"  总数据库数: {stats['database_count']}")
    
    def _list_user_databases(self):
        """列出用户的所有数据库"""
        if not self.user_id:
            print("当前未指定用户")
            return
        
        from database.user_manager import UserManager
        user_manager = UserManager()
        
        databases = user_manager.get_user_databases(self.user_id)
        
        print(f"用户 {self.user_id} 的数据库列表:")
        print("=" * 50)
        
        if not databases:
            print("该用户没有绑定的数据库")
            return
        
        for i, db_file in enumerate(databases, 1):
            binding_info = user_manager.get_binding_info(db_file)
            current_mark = " (当前)" if db_file == self.database.data_file else ""
            print(f"{i}. {db_file}{current_mark}")
            if binding_info:
                print(f"   创建时间: {binding_info.created_at}")
                print(f"   最后访问: {binding_info.last_accessed}")
                print(f"   文件存在: {'是' if os.path.exists(db_file) else '否'}")
            print()


def main():
    """测试数据库系统"""
    # 创建数据库
    db = Database("test_database.db")
    
    print("数据库系统测试:")
    
    # 测试SQL语句
    test_sqls = [
        "CREATE TABLE student(id INT, name VARCHAR, age INT);",
        "INSERT INTO student(id,name,age) VALUES (1,'Alice',20);",
        "INSERT INTO student(id,name,age) VALUES (2,'Bob',22);",
        "INSERT INTO student(id,name,age) VALUES (3,'Charlie',19);",
        "SELECT id,name FROM student WHERE age > 18;",
        "DELETE FROM student WHERE id = 2;",
        "SELECT * FROM student;"
    ]
    
    for sql in test_sqls:
        print(f"\n执行: {sql}")
        result = db.execute_sql(sql)
        if result['success']:
            print(f"结果: {result['message']}")
            if result['data']:
                for row in result['data']:
                    print(f"  {row}")
        else:
            print(f"错误: {result['message']}")
    
    # 显示数据库信息
    print(f"\n数据库信息:")
    info = db.get_database_info()
    print(f"  表数量: {info['tables']}")
    print(f"  缓存命中率: {info['storage']['cache']['hit_rate']:.2%}")
    
    # 关闭数据库
    db.close()


if __name__ == "__main__":
    main()
