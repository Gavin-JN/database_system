-- 数据库系统错误测试用例
-- 用于验证词法分析、语法分析、语义分析的错误处理能力

-- ============================================
-- 1. 词法分析错误测试
-- ============================================

-- 1.1 非法字符错误
CREATE TABLE test@table(id INT, name VARCHAR);
-- 错误类型: 非法字符 '@'

-- 1.2 未闭合字符串错误
INSERT INTO test VALUES (1, 'unclosed string);
-- 错误类型: 字符串未闭合

-- 1.3 未闭合字符串错误（双引号）
INSERT INTO test VALUES (1, "unclosed string);
-- 错误类型: 字符串未闭合

-- ============================================
-- 2. 语法分析错误测试
-- ============================================

-- 2.1 缺少分号错误
CREATE TABLE test(id INT, name VARCHAR)
-- 错误类型: 期望分号

-- 2.2 缺少关键字错误
CREATE test(id INT, name VARCHAR);
-- 错误类型: 期望TABLE关键字

-- 2.3 缺少括号错误
CREATE TABLE test id INT, name VARCHAR;
-- 错误类型: 期望左括号

-- 2.4 缺少右括号错误
CREATE TABLE test(id INT, name VARCHAR;
-- 错误类型: 期望右括号

-- 2.5 INSERT语句语法错误
INSERT test(id,name) VALUES (1,'Alice');
-- 错误类型: 期望INTO关键字

-- 2.6 SELECT语句语法错误
SELECT * test WHERE id = 1;
-- 错误类型: 期望FROM关键字

-- 2.7 DELETE语句语法错误
DELETE test WHERE id = 1;
-- 错误类型: 期望FROM关键字

-- ============================================
-- 3. 语义分析错误测试
-- ============================================

-- 3.1 表不存在错误
SELECT * FROM nonexistent_table;
-- 错误类型: TABLE_NOT_EXISTS

-- 3.2 列不存在错误
SELECT invalid_column FROM student;
-- 错误类型: COLUMN_NOT_EXISTS

-- 3.3 类型不匹配错误
INSERT INTO student(id,name,age) VALUES ('invalid_id', 'Alice', 20);
-- 错误类型: TYPE_MISMATCH

-- 3.4 列数不匹配错误
INSERT INTO student(id,name,age) VALUES (1, 'Alice');
-- 错误类型: COLUMN_COUNT_MISMATCH

-- 3.5 重复创建表错误
CREATE TABLE student(id INT, name VARCHAR);
CREATE TABLE student(id INT, name VARCHAR);
-- 错误类型: TABLE_EXISTS

-- 3.6 列名重复错误
CREATE TABLE test(id INT, id VARCHAR);
-- 错误类型: DUPLICATE_COLUMN

-- ============================================
-- 4. 执行时错误测试
-- ============================================

-- 4.1 插入到不存在的表
INSERT INTO nonexistent_table(id,name) VALUES (1,'Alice');
-- 错误类型: 表不存在

-- 4.2 查询不存在的表
SELECT * FROM nonexistent_table;
-- 错误类型: 表不存在

-- 4.3 删除不存在的表
DELETE FROM nonexistent_table WHERE id = 1;
-- 错误类型: 表不存在

-- ============================================
-- 5. 边界条件测试
-- ============================================

-- 5.1 空表名
CREATE TABLE (id INT, name VARCHAR);
-- 错误类型: 期望表名

-- 5.2 空列名
CREATE TABLE test( INT, name VARCHAR);
-- 错误类型: 期望列名

-- 5.3 空值列表
INSERT INTO test(id,name) VALUES ();
-- 错误类型: 期望值

-- 5.4 空列列表
INSERT INTO test() VALUES (1, 'Alice');
-- 错误类型: 期望列名

-- ============================================
-- 6. 复杂错误组合测试
-- ============================================

-- 6.1 多个语法错误
CREATE TABLE test(id INT, name VARCHAR
-- 错误类型: 缺少右括号和分号

-- 6.2 语法错误后跟语义错误
CREATE TABLE test(id INT, name VARCHAR);
INSERT INTO test(id,name) VALUES (1, 'Alice', 20);
-- 第一个语句语法正确，第二个语句列数不匹配

-- 6.3 嵌套错误
SELECT * FROM (SELECT * FROM nonexistent_table);
-- 错误类型: 表不存在（嵌套查询暂不支持，但可以测试错误处理）

-- ============================================
-- 7. 正确语句（用于对比）
-- ============================================

-- 7.1 正确的CREATE TABLE语句
CREATE TABLE student(id INT, name VARCHAR, age INT);

-- 7.2 正确的INSERT语句
INSERT INTO student(id,name,age) VALUES (1,'Alice',20);

-- 7.3 正确的SELECT语句
SELECT id,name FROM student WHERE age > 18;

-- 7.4 正确的DELETE语句
DELETE FROM student WHERE id = 1;

-- ============================================
-- 8. 特殊字符测试
-- ============================================

-- 8.1 包含特殊字符的表名和列名
CREATE TABLE "test_table"(id INT, "column_name" VARCHAR);

-- 8.2 包含数字的表名和列名
CREATE TABLE table123(id INT, col1 VARCHAR);

-- 8.3 包含下划线的表名和列名
CREATE TABLE test_table(id INT, column_name VARCHAR);

-- ============================================
-- 9. 数据类型测试
-- ============================================

-- 9.1 整数类型测试
CREATE TABLE int_test(id INT, value INT);

-- 9.2 字符串类型测试
CREATE TABLE string_test(id INT, name VARCHAR, description VARCHAR);

-- 9.3 混合类型测试
CREATE TABLE mixed_test(id INT, name VARCHAR, age INT, grade VARCHAR);

-- ============================================
-- 10. 运算符测试
-- ============================================

-- 10.1 等号运算符
SELECT * FROM student WHERE id = 1;

-- 10.2 大于运算符
SELECT * FROM student WHERE age > 18;

-- 10.3 小于运算符
SELECT * FROM student WHERE age < 25;

-- 10.4 大于等于运算符
SELECT * FROM student WHERE age >= 20;

-- 10.5 小于等于运算符
SELECT * FROM student WHERE age <= 22;

-- 10.6 不等于运算符
SELECT * FROM student WHERE id != 1;
