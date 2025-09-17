#!/usr/bin/env python3
"""
增强版数据库REPL - 支持高级SQL功能
"""
import sys
import os
from database.enhanced_database import EnhancedDatabase

# 跨平台兼容性处理
try:
    import readline
    import rlcompleter
    HAS_READLINE = True
except ImportError:
    HAS_READLINE = False


class EnhancedDatabaseREPL:
    """增强版数据库REPL类"""
    
    def __init__(self, db_file="enhanced_database.db"):
        self.db_file = db_file
        self.db = None
        self.history_file = ".enhanced_database_history"
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
        print("🚀 增强版数据库REPL - 支持高级SQL功能")
        print("=" * 60)
        print("✨ 支持功能:")
        print("  • 聚合函数: SUM, COUNT, AVG, MAX, MIN")
        print("  • JOIN操作: INNER, LEFT, RIGHT, FULL, CROSS")
        print("  • 子查询: 嵌套查询, EXISTS, IN, ANY, ALL")
        print("  • 集合操作: UNION, INTERSECT, EXCEPT")
        print("  • 窗口函数: ROW_NUMBER, RANK, DENSE_RANK")
        print("  • 递归查询: WITH RECURSIVE (CTE)")
        print("  • 排序分页: ORDER BY, LIMIT, OFFSET")
        print("  • 索引管理: CREATE INDEX, DROP INDEX")
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
            self.db = EnhancedDatabase(self.db_file)
            print(f"✅ 增强版数据库已连接到: {self.db_file}")
        except Exception as e:
            print(f"❌ 数据库初始化失败: {e}")
            sys.exit(1)
    
    def main_loop(self):
        """主循环"""
        while True:
            try:
                # 获取用户输入
                line = input("db(enhanced)> ").strip()
                
                # 处理空输入
                if not line:
                    continue
                
                # 处理内置命令
                if line in self.commands:
                    self.commands[line]()
                    continue
                
                # 处理SQL语句
                self.execute_sql(line)
                
            except KeyboardInterrupt:
                print("\n\n👋 再见！")
                break
            except EOFError:
                print("\n\n👋 再见！")
                break
            except Exception as e:
                print(f"❌ 意外错误: {e}")
    
    def execute_sql(self, sql):
        """执行SQL语句"""
        try:
            result = self.db.execute_sql(sql)
            
            if result['success']:
                print("✅ 执行成功")
                if 'data' in result and result['data']:
                    print("查询结果:")
                    for row in result['data']:
                        print(f"  {row}")
                elif 'message' in result:
                    print(result['message'])
            else:
                print(f"❌ 执行错误: {result['message']}")
                
        except Exception as e:
            print(f"❌ 执行异常: {e}")
    
    def help_command(self):
        """显示帮助信息"""
        print("\n📚 增强版数据库REPL帮助")
        print("=" * 40)
        print("🔧 内置命令:")
        for cmd, func in self.commands.items():
            if cmd not in ['help', 'exit', 'quit']:
                print(f"  {cmd:<10} - {func.__doc__ or '无描述'}")
        
        print("\n💡 SQL示例:")
        print("  -- 基本查询")
        print("  SELECT * FROM students;")
        print("  SELECT name, age FROM students WHERE age > 20;")
        
        print("\n  -- 聚合函数")
        print("  SELECT COUNT(*) FROM students;")
        print("  SELECT SUM(age) FROM students;")
        print("  SELECT AVG(age) FROM students;")
        print("  SELECT MAX(age), MIN(age) FROM students;")
        
        print("\n  -- 分组查询")
        print("  SELECT grade, COUNT(*) FROM students GROUP BY grade;")
        print("  SELECT grade, AVG(age) FROM students GROUP BY grade;")
        
        print("\n  -- 排序和分页")
        print("  SELECT * FROM students ORDER BY age DESC;")
        print("  SELECT * FROM students LIMIT 5;")
        print("  SELECT * FROM students LIMIT 5 OFFSET 10;")
        
        print("\n  -- 索引管理")
        print("  CREATE INDEX idx_age ON students(age);")
        print("  DROP INDEX idx_age;")
        
        print("\n  -- 表管理")
        print("  CREATE TABLE test(id INT, name VARCHAR);")
        print("  INSERT INTO test VALUES (1, 'Hello');")
        print("  UPDATE test SET name = 'World' WHERE id = 1;")
        print("  DELETE FROM test WHERE id = 1;")
        print("  DROP TABLE test;")
        print()
    
    def tables_command(self):
        """显示所有表"""
        try:
            tables = self.db.get_tables()
            if tables:
                print("📋 数据库中的表:")
                for table in tables:
                    print(f"  • {table}")
            else:
                print("📋 数据库中没有表")
        except Exception as e:
            print(f"❌ 获取表列表失败: {e}")
    
    def desc_command(self, table_name=None):
        """显示表结构"""
        if not table_name:
            print("用法: desc <表名>")
            return
        
        try:
            info = self.db.get_table_info(table_name)
            if info:
                print(f"📊 表 '{table_name}' 结构:")
                print(f"  列数: {info['column_count']}")
                print("  列信息:")
                for col in info['columns']:
                    print(f"    • {col['name']} ({col['type']})")
            else:
                print(f"❌ 表 '{table_name}' 不存在")
        except Exception as e:
            print(f"❌ 获取表结构失败: {e}")
    
    def info_command(self):
        """显示数据库信息"""
        try:
            info = self.db.get_database_info()
            print("📊 数据库信息:")
            print(f"  文件: {self.db_file}")
            print(f"  表数: {info['table_count']}")
            print(f"  总记录数: {info['total_records']}")
        except Exception as e:
            print(f"❌ 获取数据库信息失败: {e}")
    
    def clear_command(self):
        """清屏"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def history_command(self):
        """显示命令历史"""
        try:
            with open(self.history_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                if lines:
                    print("📜 命令历史:")
                    for i, line in enumerate(lines[-10:], 1):  # 显示最近10条
                        print(f"  {i:2d}. {line.strip()}")
                else:
                    print("📜 没有命令历史")
        except FileNotFoundError:
            print("📜 没有命令历史")
        except Exception as e:
            print(f"❌ 获取历史记录失败: {e}")
    
    def load_command(self, filename=None):
        """加载SQL文件"""
        if not filename:
            print("用法: load <文件名>")
            return
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
                print(f"📁 加载文件: {filename}")
                
                # 分割SQL语句
                statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
                
                for i, sql in enumerate(statements, 1):
                    print(f"\n执行语句 {i}: {sql}")
                    self.execute_sql(sql)
                    
        except FileNotFoundError:
            print(f"❌ 文件不存在: {filename}")
        except Exception as e:
            print(f"❌ 加载文件失败: {e}")
    
    def save_command(self, filename=None):
        """保存当前会话到文件"""
        if not filename:
            print("用法: save <文件名>")
            return
        
        try:
            with open(self.history_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"💾 会话已保存到: {filename}")
        except Exception as e:
            print(f"❌ 保存失败: {e}")
    
    def exit_command(self):
        """退出程序"""
        print("👋 再见！")
        sys.exit(0)
    
    def load_history(self):
        """加载历史记录"""
        try:
            with open(self.history_file, 'r', encoding='utf-8') as f:
                for line in f:
                    readline.add_history(line.strip())
        except FileNotFoundError:
            pass
        except Exception:
            pass
    
    def save_history(self):
        """保存历史记录"""
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                for i in range(readline.get_current_history_length()):
                    f.write(readline.get_history_item(i + 1) + '\n')
        except Exception:
            pass


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='增强版数据库REPL - 支持高级SQL功能')
    parser.add_argument('database', nargs='?', default='enhanced_database.db',
                       help='数据库文件路径 (默认: enhanced_database.db)')
    
    args = parser.parse_args()
    
    # 创建并启动REPL
    repl = EnhancedDatabaseREPL(args.database)
    
    try:
        repl.start()
    finally:
        repl.save_history()


if __name__ == "__main__":
    main()
