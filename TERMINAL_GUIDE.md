# 数据库系统终端使用指南

## 快速开始

### 1. 命令行版本 (推荐)

启动交互式数据库REPL：
```bash
python3 database_repl.py
```

#### 基本使用
```bash
# 启动REPL
python3 database_repl.py

# 在REPL中输入SQL语句
db> CREATE TABLE students(id INT, name VARCHAR, age INT);
db> INSERT INTO students(id,name,age) VALUES (1,'Alice',20);
db> SELECT * FROM students;
db> exit
```

#### 内置命令
- `help` - 显示帮助信息
- `tables` - 显示所有表
- `desc <table_name>` - 显示表结构
- `info` - 显示数据库信息
- `clear` - 清屏
- `history` - 显示命令历史
- `load <file>` - 加载SQL文件
- `save <file>` - 保存当前会话到文件
- `exit`/`quit` - 退出程序

### 2. Web版本

启动Web界面：
```bash
python3 database_web.py
```

然后访问：http://localhost:5000

### 3. 直接执行SQL文件

```bash
# 创建SQL文件
echo "CREATE TABLE test(id INT, name VARCHAR); INSERT INTO test VALUES (1,'Hello'); SELECT * FROM test;" > test.sql

# 在REPL中加载执行
python3 database_repl.py
db> load test.sql
```

## 支持的SQL语句

### 数据定义语言 (DDL)
```sql
-- 创建表
CREATE TABLE students(id INT, name VARCHAR, age INT);
```

### 数据操作语言 (DML)
```sql
-- 插入数据
INSERT INTO students(id,name,age) VALUES (1,'Alice',20);
INSERT INTO students(id,name,age) VALUES (2,'Bob',22);

-- 查询数据
SELECT * FROM students;
SELECT id,name FROM students WHERE age > 20;
SELECT * FROM students WHERE name = 'Alice';

-- 删除数据
DELETE FROM students WHERE age < 20;

-- 更新数据
UPDATE students SET age = 21 WHERE name = 'Alice';
```

## 示例会话

```bash
$ python3 database_repl.py
============================================================
数据库REPL - 交互式数据库编程接口
============================================================
输入 'help' 查看帮助，输入 'exit' 退出
支持多行SQL语句，以分号结尾
============================================================
✓ 数据库已连接到: repl_database.db
db> CREATE TABLE students(id INT, name VARCHAR, age INT);
✓ 表 'students' 创建成功

db> INSERT INTO students(id,name,age) VALUES (1,'Alice',20);
✓ 成功插入1条记录到表 'students'

db> INSERT INTO students(id,name,age) VALUES (2,'Bob',22);
✓ 成功插入1条记录到表 'students'

db> SELECT * FROM students;
✓ 投影操作完成

查询结果 (2 条记录):
--------------------------------------------------
  1. {'id': 1, 'name': 'Alice', 'age': 20}
  2. {'id': 2, 'name': 'Bob', 'age': 22}
--------------------------------------------------

db> SELECT * FROM students WHERE age > 20;
✓ 过滤操作完成

查询结果 (1 条记录):
--------------------------------------------------
  1. {'id': 2, 'name': 'Bob', 'age': 22}
--------------------------------------------------

db> tables
表列表:
  1. students

db> desc students
表: students
创建时间: 2025-09-08T20:00:00.000000
页数: 0
列信息:
  id: INT
  name: VARCHAR
  age: INT

db> info
数据库信息:
  数据库文件: repl_database.db
  表数量: 1
  存储统计:
    缓存命中率: 75.00%
    总页数: 2
    空闲页数: 0

db> exit
再见!
```

## 快捷键

- `↑`/`↓` - 浏览命令历史
- `Tab` - 自动补全
- `Ctrl+C` - 取消当前输入

## 故障排除

### 常见问题
1. **数据库连接失败**: 检查文件权限
2. **SQL执行失败**: 检查SQL语法
3. **命令历史无法保存**: 检查文件权限

### 调试方法
1. 使用 `info` 命令查看数据库状态
2. 使用 `tables` 命令查看表列表
3. 检查错误信息中的具体提示

## 项目文件

- `database_repl.py` - 命令行REPL版本
- `database_web.py` - Web版本
- `main.py` - 主程序入口
- `demo.py` - 功能演示
- `examples/` - 示例SQL文件

---

**注意**: 所有图形化界面代码已删除，只保留终端和Web版本。



