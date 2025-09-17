#!/usr/bin/env python3
"""
基本增删改查功能测试 Demo
测试数据库的基本 CRUD 操作和条件查询功能
"""

import os
from database.enhanced_database import EnhancedDatabase

def test_basic_crud():
    """测试基本增删改查功能"""
    print("=" * 60)
    print("🚀 基本增删改查功能测试 Demo")
    print("=" * 60)
    
    # 使用临时数据库文件
    db_file = "basic_function_demo.db"
    
    # 如果文件存在，删除它以确保测试的干净性
    if os.path.exists(db_file):
        os.remove(db_file)
        print(f"🗑️  删除旧的测试数据库文件: {db_file}")
    
    try:
        # 创建数据库连接
        print(f"\n📊 创建数据库连接: {db_file}")
        db = EnhancedDatabase(db_file)
        
        # 1. 测试创建表 (CREATE)
        print("\n" + "=" * 40)
        print("1️⃣  测试创建表 (CREATE)")
        print("=" * 40)
        
        create_table_sql = """
        CREATE TABLE students (
            id INT,
            name VARCHAR,
            age INT,
            grade VARCHAR,
            score INT
        );
        """
        
        print(f"📝 执行SQL: {create_table_sql.strip()}")
        result = db.execute_sql(create_table_sql)
        print(f"创建表结果: {'✅ 成功' if result['success'] else '❌ 失败'}")
        if not result['success']:
            print(f"错误信息: {result['message']}")
            return
        
        # 2. 测试插入数据 (INSERT)
        print("\n" + "=" * 40)
        print("2️⃣  测试插入数据 (INSERT)")
        print("=" * 40)
        
        test_data = [
            (1, 'Alice', 20, 'A', 95),
            (2, 'Bob', 19, 'B', 87),
            (3, 'Charlie', 21, 'A', 92),
            (4, 'David', 20, 'C', 78),
            (5, 'Eve', 22, 'B', 88),
            (6, 'Frank', 19, 'A', 96),
            (7, 'Grace', 21, 'C', 75),
            (8, 'Henry', 20, 'B', 85)
        ]
        
        for i, (id, name, age, grade, score) in enumerate(test_data, 1):
            insert_sql = f"INSERT INTO students (id, name, age, grade, score) VALUES ({id}, '{name}', {age}, '{grade}', {score});"
            print(f"📝 执行SQL: {insert_sql}")
            result = db.execute_sql(insert_sql)
            print(f"插入学生 {i}: {'✅' if result['success'] else '❌'} - {name}")
            if not result['success']:
                print(f"  错误: {result['message']}")
        
        # 3. 测试查询所有数据 (SELECT)
        print("\n" + "=" * 40)
        print("3️⃣  测试查询所有数据 (SELECT)")
        print("=" * 40)
        
        select_all_sql = "SELECT * FROM students;"
        print(f"📝 执行SQL: {select_all_sql}")
        result = db.execute_sql(select_all_sql)
        print(f"查询所有学生: {'✅ 成功' if result['success'] else '❌ 失败'}")
        if result['success'] and result['data']:
            print("查询结果:")
            for row in result['data']:
                print(f"  {row}")
        else:
            print(f"错误信息: {result['message']}")
        
        # 4. 测试条件查询 (WHERE)
        print("\n" + "=" * 40)
        print("4️⃣  测试条件查询 (WHERE)")
        print("=" * 40)
        
        # 查询成绩大于90的学生
        print("\n📈 查询成绩大于90的学生:")
        high_score_sql = "SELECT * FROM students WHERE score > 90;"
        print(f"📝 执行SQL: {high_score_sql}")
        result = db.execute_sql(high_score_sql)
        print(f"查询结果: {'✅ 成功' if result['success'] else '❌ 失败'}")
        if result['success'] and result['data']:
            for row in result['data']:
                print(f"  {row}")
        else:
            print(f"错误信息: {result['message']}")
        
        # 查询A等级的学生
        print("\n🏆 查询A等级的学生:")
        grade_a_sql = "SELECT * FROM students WHERE grade = 'A';"
        print(f"📝 执行SQL: {grade_a_sql}")
        result = db.execute_sql(grade_a_sql)
        print(f"查询结果: {'✅ 成功' if result['success'] else '❌ 失败'}")
        if result['success'] and result['data']:
            for row in result['data']:
                print(f"  {row}")
        else:
            print(f"错误信息: {result['message']}")
        
        # 查询年龄在20-21之间的学生
        print("\n👥 查询年龄在20-21之间的学生:")
        age_range_sql = "SELECT * FROM students WHERE age >= 20 AND age <= 21;"
        print(f"📝 执行SQL: {age_range_sql}")
        result = db.execute_sql(age_range_sql)
        print(f"查询结果: {'✅ 成功' if result['success'] else '❌ 失败'}")
        if result['success'] and result['data']:
            for row in result['data']:
                print(f"  {row}")
        else:
            print(f"错误信息: {result['message']}")
        
        # 5. 测试更新数据 (UPDATE)
        print("\n" + "=" * 40)
        print("5️⃣  测试更新数据 (UPDATE)")
        print("=" * 40)
        
        # 更新Bob的成绩
        print("\n📝 更新Bob的成绩从87到90:")
        update_sql = "UPDATE students SET score = 90 WHERE name = 'Bob';"
        print(f"📝 执行SQL: {update_sql}")
        result = db.execute_sql(update_sql)
        print(f"更新结果: {'✅ 成功' if result['success'] else '❌ 失败'}")
        if not result['success']:
            print(f"错误信息: {result['message']}")
        
        # 更新所有C等级学生的成绩
        print("\n📝 更新所有C等级学生的成绩为85分:")
        update_grade_c_sql = "UPDATE students SET score = 85 WHERE grade = 'C';"
        print(f"📝 执行SQL: {update_grade_c_sql}")
        result = db.execute_sql(update_grade_c_sql)
        print(f"更新结果: {'✅ 成功' if result['success'] else '❌ 失败'}")
        if not result['success']:
            print(f"错误信息: {result['message']}")
        
        # 验证更新结果
        print("\n🔍 验证更新后的数据:")
        verify_sql = "SELECT * FROM students WHERE name = 'Bob' OR grade = 'C';"
        print(f"📝 执行SQL: {verify_sql}")
        result = db.execute_sql(verify_sql)
        if result['success']:
            if result.get('data') and len(result['data']) > 0:
                for row in result['data']:
                    print(f"  {row}")
            else:
                print("  没有找到匹配的记录")
        else:
            print(f"错误信息: {result['message']}")
        
        # 6. 测试删除数据 (DELETE)
        print("\n" + "=" * 40)
        print("6️⃣  测试删除数据 (DELETE)")
        print("=" * 40)
        
        # 删除成绩低于80的学生
        print("\n🗑️  删除成绩低于80的学生:")
        delete_sql = "DELETE FROM students WHERE score < 80;"
        print(f"📝 执行SQL: {delete_sql}")
        result = db.execute_sql(delete_sql)
        print(f"删除结果: {'✅ 成功' if result['success'] else '❌ 失败'}")
        if not result['success']:
            print(f"错误信息: {result['message']}")
        
        # 验证删除结果
        print("\n🔍 验证删除后的数据:")
        verify_delete_sql = "SELECT * FROM students ORDER BY id;"
        print(f"📝 执行SQL: {verify_delete_sql}")
        result = db.execute_sql(verify_delete_sql)
        if result['success']:
            if result.get('data') and len(result['data']) > 0:
                print("剩余学生:")
                for row in result['data']:
                    print(f"  {row}")
            else:
                print("  没有剩余学生")
        else:
            print(f"错误信息: {result['message']}")
        
        # 7. 测试复杂条件查询
        print("\n" + "=" * 40)
        print("7️⃣  测试复杂条件查询")
        print("=" * 40)
        
        # 查询A等级且成绩大于90的学生
        print("\n🎯 查询A等级且成绩大于90的学生:")
        complex_sql = "SELECT * FROM students WHERE grade = 'A' AND score > 90;"
        print(f"📝 执行SQL: {complex_sql}")
        result = db.execute_sql(complex_sql)
        print(f"查询结果: {'✅ 成功' if result['success'] else '❌ 失败'}")
        if result['success']:
            if result.get('data') and len(result['data']) > 0:
                for row in result['data']:
                    print(f"  {row}")
            else:
                print("  没有找到匹配的记录")
        else:
            print(f"错误信息: {result['message']}")
        
        # 查询年龄为20或21的学生
        print("\n👥 查询年龄为20或21的学生:")
        or_sql = "SELECT * FROM students WHERE age = 20 OR age = 21;"
        print(f"📝 执行SQL: {or_sql}")
        result = db.execute_sql(or_sql)
        print(f"查询结果: {'✅ 成功' if result['success'] else '❌ 失败'}")
        if result['success']:
            if result.get('data') and len(result['data']) > 0:
                for row in result['data']:
                    print(f"  {row}")
            else:
                print("  没有找到匹配的记录")
        else:
            print(f"错误信息: {result['message']}")
        
        
        print("\n" + "=" * 60)
        print("🎉 基本增删改查功能测试完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 关闭数据库连接
        try:
            db.close()
            print(f"\n🔒 数据库连接已关闭")
        except:
            pass
        
        # 清理测试文件
        if os.path.exists(db_file):
            os.remove(db_file)
            print(f"🗑️  清理测试数据库文件: {db_file}")

if __name__ == "__main__":
    test_basic_crud()
