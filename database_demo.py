#!/usr/bin/env python3
"""
增强版数据库功能演示脚本
展示所有支持的SQL功能和特性
"""

import time
from database.enhanced_database import EnhancedDatabase

def print_section(title):
    """打印章节标题"""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def print_test(title, sql):
    """打印测试标题和SQL"""
    print(f"\n🔍 {title}")
    print(f"SQL: {sql}")

def execute_and_show(db, sql, description=""):
    """执行SQL并显示结果"""
    print_test(description, sql)
    
    start_time = time.time()
    result = db.execute_sql(sql)
    end_time = time.time()
    
    if result['success']:
        print(f"✅ 执行成功 (耗时: {end_time - start_time:.4f}秒)")
        if result['data']:
            print(f"📊 查询结果 ({len(result['data'])} 条记录):")
            for i, row in enumerate(result['data'], 1):
                print(f"  {i}. {row}")
        else:
            print("📊 查询结果: 无数据")
    else:
        print(f"❌ 执行失败: {result['message']}")
    
    return result

def main():
    """主演示函数"""
    print("🚀 增强版数据库功能演示")
    print("=" * 60)
    
    # 创建数据库实例
    db = EnhancedDatabase("demo_database.db")
    
    # 1. 基本表操作
    print_section("1. 基本表操作")
    
    # 创建表
    execute_and_show(db, """
        CREATE TABLE employees (
            id INT,
            name VARCHAR,
            department VARCHAR,
            salary INT,
            hire_date VARCHAR
        );
    """, "创建员工表")
    
    execute_and_show(db, """
        CREATE TABLE departments (
            id INT,
            name VARCHAR,
            budget INT
        );
    """, "创建部门表")
    
    # 插入数据
    execute_and_show(db, """
        INSERT INTO employees (id, name, department, salary, hire_date) 
        VALUES (1, '张三', '技术部', 8000, '2020-01-15');
    """, "插入员工数据1")
    
    execute_and_show(db, """
        INSERT INTO employees (id, name, department, salary, hire_date) 
        VALUES (2, '李四', '销售部', 6000, '2019-03-20');
    """, "插入员工数据2")
    
    execute_and_show(db, """
        INSERT INTO employees (id, name, department, salary, hire_date) 
        VALUES (3, '王五', '技术部', 9000, '2021-06-10');
    """, "插入员工数据3")
    
    execute_and_show(db, """
        INSERT INTO employees (id, name, department, salary, hire_date) 
        VALUES (4, '赵六', '人事部', 5500, '2020-09-05');
    """, "插入员工数据4")
    
    execute_and_show(db, """
        INSERT INTO employees (id, name, department, salary, hire_date) 
        VALUES (5, '钱七', '销售部', 7000, '2021-02-28');
    """, "插入员工数据5")
    
    execute_and_show(db, """
        INSERT INTO departments (id, name, budget) 
        VALUES (1, '技术部', 500000);
    """, "插入部门数据1")
    
    execute_and_show(db, """
        INSERT INTO departments (id, name, budget) 
        VALUES (2, '销售部', 300000);
    """, "插入部门数据2")
    
    execute_and_show(db, """
        INSERT INTO departments (id, name, budget) 
        VALUES (3, '人事部', 200000);
    """, "插入部门数据3")
    
    # 2. 基本查询功能
    print_section("2. 基本查询功能")
    
    execute_and_show(db, "SELECT * FROM employees;", "查询所有员工")
    execute_and_show(db, "SELECT id, name, salary FROM employees;", "查询指定列")
    execute_and_show(db, "SELECT * FROM employees WHERE salary > 7000;", "条件查询：高薪员工")
    execute_and_show(db, "SELECT * FROM employees WHERE department = '技术部';", "条件查询：技术部员工")
    execute_and_show(db, "SELECT * FROM employees WHERE salary >= 6000 AND salary <= 8000;", "范围查询")
    execute_and_show(db, "SELECT * FROM employees WHERE department = '技术部' OR salary > 8000;", "逻辑查询")
    execute_and_show(db, "SELECT * FROM employees WHERE salary != 6000;", "不等查询")
    
    # 3. 聚合函数
    print_section("3. 聚合函数")
    
    execute_and_show(db, "SELECT COUNT(*) FROM employees;", "员工总数")
    execute_and_show(db, "SELECT SUM(salary) FROM employees;", "工资总和")
    execute_and_show(db, "SELECT AVG(salary) FROM employees;", "平均工资")
    execute_and_show(db, "SELECT MAX(salary) FROM employees;", "最高工资")
    execute_and_show(db, "SELECT MIN(salary) FROM employees;", "最低工资")
    
    # 4. GROUP BY 分组查询
    print_section("4. GROUP BY 分组查询")
    
    execute_and_show(db, """
        SELECT department, COUNT(*) FROM employees 
        GROUP BY department;
    """, "按部门统计员工数量")
    
    execute_and_show(db, """
        SELECT department, AVG(salary) FROM employees 
        GROUP BY department;
    """, "按部门统计平均工资")
    
    execute_and_show(db, """
        SELECT department, SUM(salary) FROM employees 
        GROUP BY department;
    """, "按部门统计工资总和")
    
    # 5. 排序功能
    print_section("5. 排序功能")
    
    execute_and_show(db, """
        SELECT * FROM employees 
        ORDER BY salary DESC;
    """, "按工资降序排列")
    
    execute_and_show(db, """
        SELECT * FROM employees 
        ORDER BY name ASC;
    """, "按姓名升序排列")
    
    # 6. 分页功能
    print_section("6. 分页功能")
    
    execute_and_show(db, """
        SELECT * FROM employees 
        ORDER BY salary DESC 
        LIMIT 3;
    """, "查询工资最高的3名员工")
    
    execute_and_show(db, """
        SELECT * FROM employees 
        ORDER BY salary ASC 
        LIMIT 2;
    """, "查询工资最低的2名员工")
    
    # 7. 索引管理
    print_section("7. 索引管理")
    
    execute_and_show(db, """
        CREATE INDEX idx_employee_salary ON employees (salary);
    """, "创建工资索引")
    
    execute_and_show(db, """
        CREATE INDEX idx_employee_department ON employees (department);
    """, "创建部门索引")
    
    execute_and_show(db, """
        CREATE UNIQUE INDEX idx_employee_id ON employees (id);
    """, "创建员工ID唯一索引")
    
    execute_and_show(db, "SELECT * FROM pg_indexes;", "查看所有索引")
    
    execute_and_show(db, """
        SELECT COUNT(*) FROM pg_indexes;
    """, "统计索引数量")
    
    # 8. 系统表查询
    print_section("8. 系统表查询")
    
    execute_and_show(db, "SELECT * FROM pg_catalog;", "查看系统目录")
    execute_and_show(db, "SELECT * FROM pg_indexes;", "查看索引信息")
    
    # 9. 性能测试
    print_section("9. 性能测试")
    
    # 插入更多数据用于性能测试
    print("\n📈 插入测试数据...")
    for i in range(6, 21):
        execute_and_show(db, f"""
            INSERT INTO employees (id, name, department, salary, hire_date) 
            VALUES ({i}, '员工{i}', '部门{(i % 3) + 1}', {5000 + (i * 200)}, '2022-{i % 12 + 1:02d}-{i % 28 + 1:02d}');
        """, f"插入测试数据{i}")
    
    # 性能测试查询
    start_time = time.time()
    result = db.execute_sql("SELECT COUNT(*) FROM employees;")
    end_time = time.time()
    print(f"\n⚡ 性能测试 - 统计20条记录: {end_time - start_time:.4f}秒")
    
    start_time = time.time()
    result = db.execute_sql("SELECT * FROM employees ORDER BY salary DESC LIMIT 10;")
    end_time = time.time()
    print(f"⚡ 性能测试 - 排序查询: {end_time - start_time:.4f}秒")
    
    # 10. 错误处理测试
    print_section("10. 错误处理测试")
    
    execute_and_show(db, "SELECT * FROM non_existent_table;", "查询不存在的表")
    execute_and_show(db, "SELECT non_existent_column FROM employees;", "查询不存在的列")
    execute_and_show(db, "CREATE INDEX idx_test ON non_existent_table (column);", "在不存在的表上创建索引")
    
    # 11. 清理测试
    print_section("11. 清理测试")
    
    execute_and_show(db, "DROP INDEX idx_employee_salary;", "删除工资索引")
    execute_and_show(db, "SELECT * FROM pg_indexes;", "查看剩余索引")
    
    # 最终统计
    print_section("演示完成 - 最终统计")
    
    execute_and_show(db, "SELECT COUNT(*) FROM employees;", "最终员工总数")
    execute_and_show(db, "SELECT COUNT(*) FROM departments;", "最终部门总数")
    execute_and_show(db, "SELECT COUNT(*) FROM pg_indexes;", "最终索引总数")
    
    print(f"\n🎉 数据库功能演示完成！")
    print(f"📊 演示了以下功能：")
    print(f"   ✅ 基本表操作 (CREATE, INSERT)")
    print(f"   ✅ 基本查询 (SELECT, WHERE)")
    print(f"   ✅ 聚合函数 (COUNT, SUM, AVG, MAX, MIN)")
    print(f"   ✅ 分组查询 (GROUP BY)")
    print(f"   ✅ 排序功能 (ORDER BY)")
    print(f"   ✅ 分页功能 (LIMIT)")
    print(f"   ✅ 索引管理 (CREATE INDEX, DROP INDEX)")
    print(f"   ✅ 复杂查询组合")
    print(f"   ✅ 系统表查询")
    print(f"   ✅ 性能测试")
    print(f"   ✅ 错误处理")
    
    print(f"\n💾 数据库文件: demo_database.db")
    print(f"🔧 使用 'python3 enhanced_database_repl.py' 继续交互式操作")

if __name__ == "__main__":
    main()
