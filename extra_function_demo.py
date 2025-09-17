#!/usr/bin/env python3
"""
数据库操作演示脚本
展示如何使用增强版数据库进行各种操作
"""

from database.enhanced_database import EnhancedDatabase

def demo_database_operations():
    """演示数据库操作"""
    print("🎯 数据库操作演示")
    print("=" * 50)
    
    # 创建增强版数据库
    db = EnhancedDatabase("extra_function_demo.db")
    
    # 1. 创建表
    print("\n1️⃣ 创建表...")
    result = db.execute_sql("""
        CREATE TABLE student (
            id INT,
            name VARCHAR,
            age INT,
            grade VARCHAR,
            score INT
        );
    """)
    print(f"CREATE TABLE: {'✅' if result['success'] else '❌'} - {result['message']}")
    
    # 2. 插入数据
    print("\n2️⃣ 插入数据...")
    test_data = [
        (1, 'Alice', 20, 'A', 95),
        (2, 'Bob', 22, 'B', 87),
        (3, 'Charlie', 19, 'A', 92),
        (4, 'David', 21, 'C', 78),
        (5, 'Eve', 23, 'B', 89),
        (6, 'Frank', 20, 'A', 94),
        (7, 'Grace', 22, 'B', 85),
        (8, 'Henry', 19, 'C', 76)
    ]
    
    for data in test_data:
        result = db.execute_sql(f"""
            INSERT INTO student (id, name, age, grade, score) 
            VALUES ({data[0]}, '{data[1]}', {data[2]}, '{data[3]}', {data[4]});
        """)
        print(f"INSERT {data[1]}: {'✅' if result['success'] else '❌'}")
    
    # 3. 基本查询
    print("\n3️⃣ 基本查询...")
    
    # 查询所有数据
    print("\n   a) 查询所有数据:")
    result = db.execute_sql("SELECT * FROM student;")
    if result['success']:
        print("   查询结果:")
        for row in result['data']:
            print(f"     {row}")
    else:
        print(f"   ❌ 错误: {result['message']}")
    
    # 条件查询
    print("\n   b) 条件查询 (age > 20):")
    result = db.execute_sql("SELECT name, age, grade FROM student WHERE age > 20;")
    if result['success']:
        print("   查询结果:")
        for row in result['data']:
            print(f"     {row}")
    else:
        print(f"   ❌ 错误: {result['message']}")
    
    # 4. 聚合函数
    print("\n4️⃣ 聚合函数...")
    
    # 基本聚合
    print("\n   a) 基本聚合函数:")
    result = db.execute_sql("SELECT COUNT(*), SUM(age), AVG(age), MAX(age), MIN(age) FROM student;")
    if result['success']:
        print(f"   COUNT(*), SUM(age), AVG(age), MAX(age), MIN(age) = {result['data']}")
    else:
        print(f"   ❌ 错误: {result['message']}")
    
    # 带别名的聚合
    print("\n   b) 带别名的聚合函数:")
    result = db.execute_sql("SELECT COUNT(*) AS total, AVG(score) AS avg_score FROM student;")
    if result['success']:
        print(f"   total, avg_score = {result['data']}")
    else:
        print(f"   ❌ 错误: {result['message']}")
    
    # 5. 分组查询
    print("\n5️⃣ 分组查询...")
    
    # 按年级分组
    print("\n   a) 按年级分组统计:")
    result = db.execute_sql("SELECT grade, COUNT(*), AVG(age), AVG(score) FROM student GROUP BY grade;")
    if result['success']:
        print("   查询结果:")
        for row in result['data']:
            print(f"     {row}")
    else:
        print(f"   ❌ 错误: {result['message']}")
    
    # 6. 排序和分页
    print("\n6️⃣ 排序和分页...")
    
    # 按分数排序
    print("\n   a) 按分数降序排序:")
    result = db.execute_sql("SELECT name, score FROM student ORDER BY score DESC;")
    if result['success']:
        print("   查询结果:")
        for row in result['data']:
            print(f"     {row}")
    else:
        print(f"   ❌ 错误: {result['message']}")
    
    # 分页查询
    print("\n   b) 分页查询 (LIMIT 3):")
    result = db.execute_sql("SELECT name, score FROM student ORDER BY score DESC LIMIT 3;")
    if result['success']:
        print("   查询结果:")
        for row in result['data']:
            print(f"     {row}")
    else:
        print(f"   ❌ 错误: {result['message']}")
    
    # 7. 更新数据
    print("\n7️⃣ 更新数据...")
    
    # 更新分数
    print("\n   a) 更新分数 (A级学生100分):")
    result = db.execute_sql("UPDATE student SET score = 100 WHERE grade = 'A';")
    if result['success']:
        print(f"   ✅ 更新成功: {result['message']}")
    else:
        print(f"   ❌ 错误: {result['message']}")
    
    # 查看更新结果
    result = db.execute_sql("SELECT name, grade, score FROM student WHERE grade = 'A';")
    if result['success']:
        print("   更新后的A级学生:")
        for row in result['data']:
            print(f"     {row}")
    
    # 8. 删除数据
    print("\n8️⃣ 删除数据...")
    
    # 删除低分学生
    print("\n   a) 删除分数低于80的学生:")
    result = db.execute_sql("DELETE FROM student WHERE score < 80;")
    if result['success']:
        print(f"   ✅ 删除成功: {result['message']}")
    else:
        print(f"   ❌ 错误: {result['message']}")
    
    # 查看剩余学生
    result = db.execute_sql("SELECT name, score FROM student ORDER BY score DESC;")
    if result['success']:
        print("   剩余学生:")
        for row in result['data']:
            print(f"     {row}")
    
    # 9. 索引操作
    print("\n9️⃣ 索引操作...")
    
    # 创建索引
    print("\n   a) 创建索引:")
    result = db.execute_sql("CREATE INDEX idx_score ON student(score);")
    if result['success']:
        print(f"   ✅ 索引创建成功: {result['message']}")
    else:
        print(f"   ❌ 错误: {result['message']}")
    
    # 查看索引
    result = db.execute_sql("SELECT * FROM pg_indexes;")
    if result['success']:
        print("   当前索引:")
        for row in result['data']:
            print(f"     {row}")
    
    # 10. 数据库信息
    print("\n🔟 数据库信息...")
    
    # 表信息
    tables = db.get_tables()
    print(f"   数据库中的表: {tables}")
    
    # 表结构
    if tables:
        table_info = db.get_table_info(tables[0])
        print(f"   表 '{tables[0]}' 结构:")
        print(f"     列数: {table_info['column_count']}")
        for col in table_info['columns']:
            print(f"     • {col['name']} ({col['type']})")
    
    print("\n🎉 演示完成！")

if __name__ == "__main__":
    demo_database_operations()
