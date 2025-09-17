#!/usr/bin/env python3
"""
数据库系统主程序入口
提供命令行界面和交互式数据库操作
支持用户ID与数据库文件绑定
支持SQL自动补全功能
"""
import sys
import os
from database.database import Database, DatabaseCLI
from database.enhanced_database import EnhancedDatabase
from database.user_manager import UserManager
from utils.sql_autocomplete import SQLCompleter

# 跨平台兼容性处理
try:
    import readline
    HAS_READLINE = True
except ImportError:
    try:
        import pyreadline3 as readline
        HAS_READLINE = True
    except ImportError:
        HAS_READLINE = False


class EnhancedDatabaseCLI:
    """增强版数据库命令行界面，支持SQL自动补全"""
    
    def __init__(self, database, user_id: str = None):
        self.database = database
        self.user_id = user_id
        self.running = True
        self.sql_completer = None
        
        # 初始化SQL自动补全器
        if HAS_READLINE:
            try:
                self.sql_completer = SQLCompleter(database)
                readline.set_completer(self._completer)
                readline.parse_and_bind("tab: complete")
                print("⌨️  SQL自动补全功能已启用")
            except Exception as e:
                print(f"⚠️  自动补全功能初始化失败: {e}")
                self.sql_completer = None
        else:
            print("⚠️  readline模块不可用，自动补全功能已禁用")
    
    def start(self):
        """启动CLI"""
        print("欢迎使用数据库系统!")
        if self.user_id:
            print(f"当前用户: {self.user_id}")
        print("输入 'help' 查看帮助，输入 'exit' 退出")
        if self.sql_completer:
            print("按Tab键自动补全SQL关键字")
        print("=" * 50)
        
        while self.running:
            try:
                prompt = f"db({self.user_id})> " if self.user_id else "db> "
                command = input(prompt).strip()
                
                if command.lower() in ['exit', 'quit']:
                    self.running = False
                    break
                elif command.lower() == 'help':
                    self._show_help()
                elif command.lower() == 'tables':
                    self._show_tables()
                elif command.lower() == 'info':
                    self._show_database_info()
                elif command.lower().startswith('desc '):
                    table_name = command[5:].strip()
                    self._describe_table(table_name)
                elif command.lower() == 'userinfo':
                    self._show_user_info()
                elif command.lower() == 'listdbs':
                    self._list_user_databases()
                else:
                    # 执行SQL
                    result = self.database.execute_sql(command)
                    self._print_result(result)
            except KeyboardInterrupt:
                print("\n使用 'exit' 命令退出")
            except Exception as e:
                print(f"错误: {e}")
        
        print("再见!")
        self.database.close()
    
    def _completer(self, text, state):
        """自动补全函数"""
        if not self.sql_completer:
            return None
        
        # 获取当前输入行
        line = readline.get_line_buffer()
        
        # 获取补全建议
        completions = self.sql_completer.get_completions(line)
        
        # 过滤匹配的补全建议
        matches = []
        for completion in completions:
            if completion.upper().startswith(text.upper()):
                matches.append(completion)
        
        # 返回第state个匹配项
        if state < len(matches):
            return matches[state]
        else:
            return None
    
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
        if self.sql_completer:
            print("\n快捷键:")
            print("  Tab                     - SQL自动补全")
            print("  ↑↓箭头                  - 浏览命令历史")
    
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
        print(f"数据库信息:")
        print(f"  数据库文件: {info['database_file']}")
        print(f"  表数量: {info['tables']}")
        
        storage = info['storage']
        print(f"  存储统计:")
        print(f"    缓存命中率: {storage['cache']['hit_rate']:.2%}")
        print(f"    总页数: {storage['pages']['total_pages']}")
        print(f"    空闲页数: {storage['pages']['free_pages']}")
    
    def _show_user_info(self):
        """显示用户信息"""
        if self.user_id:
            print(f"当前用户: {self.user_id}")
        else:
            print("未指定用户")
    
    def _list_user_databases(self):
        """列出用户数据库"""
        if self.user_id:
            user_manager = UserManager()
            databases = user_manager.get_user_databases(self.user_id)
            print(f"用户 {self.user_id} 的数据库:")
            for db in databases:
                print(f"  {db}")
        else:
            print("未指定用户")
    
    def _print_result(self, result):
        """打印执行结果"""
        if result['success']:
            print(f"✓ {result['message']}")
            if result.get('data'):
                print("查询结果:")
                for row in result['data']:
                    print(f"  {row}")
            if result.get('rows_affected', 0) > 0:
                print(f"影响行数: {result['rows_affected']}")
        else:
            print(f"✗ {result['message']}")


def main():
    """主函数"""
    # 检查命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help" or sys.argv[1] == "-h":
            print("数据库系统")
            print("用法: python main.py <用户ID> [数据库文件] [--enhanced]")
            print("  用户ID: 必需，指定用户标识符")
            print("  数据库文件: 可选，指定要打开的数据库文件")
            print("  --enhanced: 可选，使用增强版数据库（支持聚合函数等高级功能）")
            print("  如果不指定数据库文件，将列出该用户的所有数据库")
            print("  示例: python main.py user1 mydb.db")
            print("  示例: python main.py user1 mydb.db --enhanced  # 使用增强版")
            print("  示例: python main.py user1  # 列出用户1的所有数据库")
            return
        elif len(sys.argv) == 2:
            # 只有用户ID，列出该用户的所有数据库
            user_id = sys.argv[1]
            list_user_databases(user_id)
            return
        elif len(sys.argv) == 3:
            # 用户ID和数据库文件
            user_id = sys.argv[1]
            db_file = sys.argv[2]
            open_database(user_id, db_file)
            return
        elif len(sys.argv) == 4:
            # 用户ID、数据库文件和增强版标志
            user_id = sys.argv[1]
            db_file = sys.argv[2]
            enhanced = sys.argv[3] == "--enhanced"
            open_database(user_id, db_file, enhanced)
            return
        else:
            print("参数错误。使用 --help 查看帮助信息")
            return
    else:
        print("缺少用户ID参数。使用 --help 查看帮助信息")
        return


def list_user_databases(user_id: str):
    """列出用户的所有数据库"""
    user_manager = UserManager()
    
    print(f"用户 {user_id} 的数据库列表:")
    print("=" * 50)
    
    databases = user_manager.get_user_databases(user_id)
    
    if not databases:
        print("该用户没有绑定的数据库")
        print("\n要绑定数据库，请使用:")
        print(f"  python main.py {user_id} <数据库文件>")
        return
    
    for i, db_file in enumerate(databases, 1):
        binding_info = user_manager.get_binding_info(db_file)
        if binding_info:
            print(f"{i}. {db_file}")
            print(f"   创建时间: {binding_info.created_at}")
            print(f"   最后访问: {binding_info.last_accessed}")
            print(f"   文件存在: {'是' if os.path.exists(db_file) else '否'}")
            print()
    
    print(f"总共 {len(databases)} 个数据库")
    print("\n要打开特定数据库，请使用:")
    print(f"  python main.py {user_id} <数据库文件>")


def open_database(user_id: str, db_file: str, enhanced: bool = False):
    """打开指定的数据库"""
    user_manager = UserManager()
    
    # 检查数据库文件是否已被其他用户绑定
    owner = user_manager.get_database_owner(db_file)
    if owner and owner != user_id:
        print(f"错误: 数据库文件 {db_file} 已被用户 {owner} 绑定")
        print("一个数据库文件只能被一个用户绑定")
        return
    
    # 如果数据库文件不存在，创建它
    if not os.path.exists(db_file):
        print(f"数据库文件 {db_file} 不存在，将创建新文件")
    
    # 绑定数据库到用户（如果尚未绑定）
    if not user_manager.is_database_bound(db_file):
        if not user_manager.bind_database(user_id, db_file):
            print("绑定数据库失败")
            return
    
    # 更新最后访问时间
    user_manager.update_last_accessed(db_file)
    
    # 创建数据库连接
    try:
        if enhanced:
            db = EnhancedDatabase(db_file)
            print(f"🚀 增强版数据库已连接到: {db_file}")
            print(f"用户: {user_id}")
            print("✨ 支持聚合函数、JOIN、子查询等高级功能")
            if HAS_READLINE:
                print("⌨️  SQL自动补全功能已启用")
            # 启动增强版CLI（支持自动补全）
            cli = EnhancedDatabaseCLI(db, user_id)
            cli.start()
        else:
            db = Database(db_file)
            print(f"数据库已连接到: {db_file}")
            print(f"用户: {user_id}")
            if HAS_READLINE:
                print("⌨️  SQL自动补全功能已启用")
            # 启动增强版CLI（支持自动补全）
            cli = EnhancedDatabaseCLI(db, user_id)
            cli.start()
        
    except Exception as e:
        print(f"启动数据库失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
