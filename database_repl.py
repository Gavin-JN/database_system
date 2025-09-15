#!/usr/bin/env python3
"""
数据库REPL - 交互式数据库编程接口
类似Python REPL的数据库编程环境
"""
import sys
import os
from database.database import Database

# 跨平台兼容性处理
try:
    import readline
    import rlcompleter
    HAS_READLINE = True
except ImportError:
    HAS_READLINE = False


class DatabaseREPL:
    """数据库REPL类"""
    
    def __init__(self, db_file="repl_database.db"):
        self.db_file = db_file
        self.db = None
        self.history_file = ".database_history"
        self.commands = {
            'help': self.help_command,
            'tables': self.tables_command,
            'desc': self.desc_command,
            'info': self.info_command,
            'clear': self.clear_command,
            'history': self.history_command,
            'load': self.load_command,
            'save': self.save_command,
            'exit': self.exit_command,
            'quit': self.exit_command
        }
        
        # 设置readline自动补全（如果可用）
        if HAS_READLINE:
            readline.set_completer(rlcompleter.Completer().complete)
            readline.parse_and_bind("tab: complete")
        
        # 加载历史记录
        self.load_history()
    
    def start(self):
        """启动REPL"""
        print("=" * 60)
        print("数据库REPL - 交互式数据库编程接口")
        print("=" * 60)
        print("输入 'help' 查看帮助，输入 'exit' 退出")
        print("支持多行SQL语句，以分号结尾")
        print("=" * 60)
        
        # 初始化数据库
        self.init_database()
        
        # 主循环
        self.main_loop()
    
    def init_database(self):
        """初始化数据库"""
        try:
            self.db = Database(self.db_file)
            print(f"✓ 数据库已连接到: {self.db_file}")
        except Exception as e:
            print(f"✗ 数据库初始化失败: {e}")
            sys.exit(1)
    
    def main_loop(self):
        """主循环"""
        buffer = ""
        line_count = 0
        
        while True:
            try:
                # 获取输入
                if buffer:
                    prompt = f"db[{line_count+1}]> "
                else:
                    prompt = "db> "
                
                line = input(prompt).strip()
                
                # 处理特殊命令
                if line in self.commands:
                    self.commands[line]()
                    continue
                
                # 添加到缓冲区
                if line:
                    buffer += line + "\n"
                    line_count += 1
                    
                    # 检查是否以分号结尾
                    if line.endswith(';'):
                        # 执行SQL
                        self.execute_sql(buffer.strip())
                        buffer = ""
                        line_count = 0
                    else:
                        # 继续输入
                        continue
                else:
                    # 空行，清空缓冲区
                    if buffer:
                        self.execute_sql(buffer.strip())
                        buffer = ""
                        line_count = 0
                
            except KeyboardInterrupt:
                print("\n使用 'exit' 命令退出")
                buffer = ""
                line_count = 0
            except EOFError:
                print("\n再见!")
                break
            except Exception as e:
                print(f"错误: {e}")
                buffer = ""
                line_count = 0
    
    def execute_sql(self, sql):
        """执行SQL语句"""
        if not sql.strip():
            return
        
        try:
            result = self.db.execute_sql(sql)
            
            if result['success']:
                print(f"✓ {result['message']}")
                
                # 显示查询结果
                if result['data']:
                    self.display_result(result['data'])
                
                # 显示影响的行数
                if result.get('rows_affected', 0) > 0:
                    print(f"影响行数: {result['rows_affected']}")
            else:
                print(f"✗ {result['message']}")
                if 'errors' in result:
                    for error in result['errors']:
                        print(f"  {error}")
        
        except Exception as e:
            print(f"✗ 执行错误: {e}")
    
    def display_result(self, data):
        """显示查询结果"""
        if not data:
            print("查询结果为空")
            return
        
        print(f"\n查询结果 ({len(data)} 条记录):")
        print("-" * 50)
        
        for i, row in enumerate(data, 1):
            print(f"{i:3d}. {row}")
        
        print("-" * 50)
    
    def help_command(self):
        """帮助命令"""
        print("\n可用命令:")
        print("  help                    - 显示此帮助信息")
        print("  tables                  - 显示所有表")
        print("  desc <table_name>       - 显示表结构")
        print("  info                    - 显示数据库信息")
        print("  clear                   - 清屏")
        print("  history                 - 显示命令历史")
        print("  load <file>             - 加载SQL文件")
        print("  save <file>             - 保存当前会话到文件")
        print("  exit/quit               - 退出程序")
        print("\nSQL语句:")
        print("  CREATE TABLE table_name (col1 type1, col2 type2, ...);")
        print("  INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...);")
        print("  SELECT col1, col2 FROM table_name [WHERE condition];")
        print("  DELETE FROM table_name [WHERE condition];")
        print("\n快捷键:")
        print("  Ctrl+C                  - 取消当前输入")
        print("  Tab                     - 自动补全")
        print("  上下箭头                - 浏览命令历史")
    
    def tables_command(self):
        """显示所有表"""
        tables = self.db.get_tables()
        if tables:
            print("\n表列表:")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table}")
        else:
            print("没有表")
    
    def desc_command(self, table_name=None):
        """显示表结构"""
        if not table_name:
            table_name = input("请输入表名: ").strip()
        
        if not table_name:
            print("表名不能为空")
            return
        
        table_info = self.db.get_table_info(table_name)
        if table_info:
            print(f"\n表: {table_info['name']}")
            print(f"创建时间: {table_info['created_at']}")
            print(f"页数: {table_info['page_count']}")
            print("列信息:")
            for col in table_info['columns']:
                print(f"  {col['name']}: {col['type']}")
        else:
            print(f"表 '{table_name}' 不存在")
    
    def info_command(self):
        """显示数据库信息"""
        info = self.db.get_database_info()
        print(f"\n数据库信息:")
        print(f"  数据库文件: {info['database_file']}")
        print(f"  表数量: {info['tables']}")
        
        storage = info['storage']
        print(f"  存储统计:")
        print(f"    缓存命中率: {storage['cache']['hit_rate']:.2%}")
        print(f"    总页数: {storage['pages']['total_pages']}")
        print(f"    空闲页数: {storage['pages']['free_pages']}")
    
    def clear_command(self):
        """清屏"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def history_command(self):
        """显示命令历史"""
        try:
            with open(self.history_file, 'r') as f:
                lines = f.readlines()
            
            print(f"\n命令历史 (最近 {len(lines)} 条):")
            for i, line in enumerate(lines[-20:], 1):  # 显示最近20条
                print(f"  {i:3d}. {line.strip()}")
        except FileNotFoundError:
            print("没有命令历史")
    
    def load_command(self, filename=None):
        """加载SQL文件"""
        if not filename:
            filename = input("请输入文件名: ").strip()
        
        if not filename:
            print("文件名不能为空")
            return
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
            
            print(f"加载文件: {filename}")
            
            # 按分号分割SQL语句
            statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
            
            for i, sql in enumerate(statements, 1):
                print(f"\n执行语句 {i}:")
                print(f"  {sql}")
                self.execute_sql(sql)
                
        except FileNotFoundError:
            print(f"文件 '{filename}' 不存在")
        except Exception as e:
            print(f"加载文件失败: {e}")
    
    def save_command(self, filename=None):
        """保存当前会话到文件"""
        if not filename:
            filename = input("请输入文件名: ").strip()
        
        if not filename:
            print("文件名不能为空")
            return
        
        try:
            # 获取历史记录
            with open(self.history_file, 'r') as f:
                history = f.read()
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("-- 数据库会话保存\n")
                f.write(f"-- 保存时间: {__import__('datetime').datetime.now()}\n\n")
                f.write(history)
            
            print(f"会话已保存到: {filename}")
            
        except Exception as e:
            print(f"保存文件失败: {e}")
    
    def exit_command(self):
        """退出程序"""
        print("再见!")
        if self.db:
            self.db.close()
        sys.exit(0)
    
    def load_history(self):
        """加载历史记录"""
        if HAS_READLINE:
            try:
                readline.read_history_file(self.history_file)
            except (FileNotFoundError, PermissionError):
                pass
    
    def save_history(self):
        """保存历史记录"""
        if HAS_READLINE:
            try:
                readline.write_history_file(self.history_file)
            except (PermissionError, Exception):
                pass


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据库REPL - 交互式数据库编程接口')
    parser.add_argument('database', nargs='?', default='repl_database.db',
                       help='数据库文件路径 (默认: repl_database.db)')
    
    args = parser.parse_args()
    
    # 创建并启动REPL
    repl = DatabaseREPL(args.database)
    
    try:
        repl.start()
    finally:
        repl.save_history()


if __name__ == "__main__":
    main()
