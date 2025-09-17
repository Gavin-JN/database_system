#!/usr/bin/env python3
"""
数据库系统演示脚本
展示SQL编译器、存储系统、执行引擎的完整功能
"""
from database.database import Database


def main():
    """主演示函数"""
    print("=" * 60)
    print("数据库系统演示")
    print("=" * 60)
    
    # 创建数据库
    db = Database("demo.db")
    
    # 演示SQL语句
    demo_sqls = [
        "CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);",
        "INSERT INTO student(id,name,age,grade) VALUES (1,'Alice',20,'A');",
        "INSERT INTO student(id,name,age,grade) VALUES (2,'Bob',22,'B');",
        "INSERT INTO student(id,name,age,grade) VALUES (3,'Charlie',19,'A');",
        "INSERT INTO student(id,name,age,grade) VALUES (4,'David',21,'C');",
        "SELECT * FROM student;",
        "SELECT id,name FROM student WHERE age > 20;",
        "SELECT name,grade FROM student WHERE grade = 'A';",
        "DELETE FROM student WHERE age < 20;",
        "SELECT * FROM student;"
    ]
    
    for i, sql in enumerate(demo_sqls, 1):
        print(f"\n步骤 {i}: {sql}")
        result = db.execute_sql(sql)
        
        if result['success']:
            print(f"✓ {result['message']}")
            if result['data']:
                print("查询结果:")
                for row in result['data']:
                    print(f"  {row}")
        else:
            print(f"✗ {result['message']}")
    
    # 显示数据库信息
    print(f"\n数据库信息:")
    info = db.get_database_info()
    print(f"  表数量: {info['tables']}")
    print(f"  缓存命中率: {info['storage']['cache']['hit_rate']:.2%}")
    print(f"  总页数: {info['storage']['pages']['total_pages']}")
    
    # 关闭数据库
    db.close()
    print(f"\n数据库已关闭，数据已保存到 demo.db")


if __name__ == "__main__":
    main()
