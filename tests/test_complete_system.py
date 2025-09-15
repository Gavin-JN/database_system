#!/usr/bin/env python3
"""
完整系统测试套件
测试所有模块的集成功能
"""
import unittest
import tempfile
import os
import time
from database.database import Database
from database.transaction import TransactionManager, TransactionContext
from storage.index import IndexManager, IndexType
from utils.logger import get_logger
from utils.performance import performance_monitor


class TestCompleteSystem(unittest.TestCase):
    """完整系统测试"""
    
    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_file = os.path.join(self.temp_dir, "test_complete.db")
        self.db = Database(self.db_file)
        self.logger = get_logger("test")
    
    def tearDown(self):
        """测试后清理"""
        self.db.close()
        # 清理临时文件
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_basic_crud_operations(self):
        """测试基本CRUD操作"""
        self.logger.info("测试基本CRUD操作")
        
        # 创建表
        result = self.db.execute_sql("""
            CREATE TABLE student(
                id INT, 
                name VARCHAR, 
                age INT, 
                grade VARCHAR
            );
        """)
        self.assertTrue(result['success'], f"创建表失败: {result['message']}")
        
        # 插入数据
        test_data = [
            (1, "Alice", 20, "A"),
            (2, "Bob", 22, "B"),
            (3, "Charlie", 19, "A"),
            (4, "David", 21, "C"),
            (5, "Eve", 20, "B")
        ]
        
        for student in test_data:
            sql = f"INSERT INTO student(id,name,age,grade) VALUES ({student[0]},'{student[1]}',{student[2]},'{student[3]}');"
            result = self.db.execute_sql(sql)
            self.assertTrue(result['success'], f"插入数据失败: {result['message']}")
        
        # 查询数据
        result = self.db.execute_sql("SELECT * FROM student;")
        self.assertTrue(result['success'], f"查询数据失败: {result['message']}")
        self.assertGreater(len(result['data']), 0, "查询结果为空")
        
        # 条件查询
        result = self.db.execute_sql("SELECT * FROM student WHERE age > 20;")
        self.assertTrue(result['success'], f"条件查询失败: {result['message']}")
        
        # 更新数据
        result = self.db.execute_sql("UPDATE student SET age = 25 WHERE grade = 'A';")
        self.assertTrue(result['success'], f"更新数据失败: {result['message']}")
        
        # 删除数据
        result = self.db.execute_sql("DELETE FROM student WHERE id = 1;")
        self.assertTrue(result['success'], f"删除数据失败: {result['message']}")
    
    def test_error_handling(self):
        """测试错误处理"""
        self.logger.info("测试错误处理")
        
        # 语法错误
        result = self.db.execute_sql("CREATE TABLE test(id INT, name VARCHAR")
        self.assertFalse(result['success'], "应该捕获语法错误")
        
        # 语义错误 - 表不存在
        result = self.db.execute_sql("SELECT * FROM nonexistent_table;")
        self.assertFalse(result['success'], "应该捕获表不存在错误")
        
        # 语义错误 - 列不存在
        self.db.execute_sql("CREATE TABLE test(id INT, name VARCHAR);")
        result = self.db.execute_sql("SELECT invalid_column FROM test;")
        self.assertFalse(result['success'], "应该捕获列不存在错误")
    
    def test_data_persistence(self):
        """测试数据持久化"""
        self.logger.info("测试数据持久化")
        
        # 创建表并插入数据
        self.db.execute_sql("CREATE TABLE test(id INT, name VARCHAR);")
        self.db.execute_sql("INSERT INTO test(id,name) VALUES (1,'Alice');")
        self.db.execute_sql("INSERT INTO test(id,name) VALUES (2,'Bob');")
        
        # 关闭数据库
        self.db.close()
        
        # 重新打开数据库
        self.db = Database(self.db_file)
        
        # 验证数据是否持久化
        result = self.db.execute_sql("SELECT * FROM test;")
        self.assertTrue(result['success'], "重启后查询失败")
        self.assertGreater(len(result['data']), 0, "数据未持久化")
    
    def test_performance(self):
        """测试性能"""
        self.logger.info("测试性能")
        
        # 创建表
        self.db.execute_sql("CREATE TABLE perf_test(id INT, name VARCHAR, value INT);")
        
        # 插入性能测试
        start_time = time.time()
        for i in range(100):
            self.db.execute_sql(f"INSERT INTO perf_test(id,name,value) VALUES ({i},'Name{i}',{i*10});")
        insert_time = time.time() - start_time
        
        self.assertLess(insert_time, 5.0, f"插入100条记录耗时过长: {insert_time:.3f}秒")
        
        # 查询性能测试
        start_time = time.time()
        for _ in range(10):
            self.db.execute_sql("SELECT * FROM perf_test;")
        query_time = time.time() - start_time
        
        self.assertLess(query_time, 2.0, f"查询性能过慢: {query_time:.3f}秒")
    
    def test_concurrent_operations(self):
        """测试并发操作"""
        self.logger.info("测试并发操作")
        
        # 创建表
        self.db.execute_sql("CREATE TABLE concurrent_test(id INT, name VARCHAR);")
        
        # 模拟并发插入
        import threading
        
        def insert_worker(worker_id, count):
            for i in range(count):
                try:
                    result = self.db.execute_sql(
                        f"INSERT INTO concurrent_test(id,name) VALUES ({worker_id*100+i},'Worker{worker_id}');"
                    )
                    if not result['success']:
                        self.logger.warning(f"Worker {worker_id} 插入失败: {result['message']}")
                except Exception as e:
                    self.logger.error(f"Worker {worker_id} 异常: {e}")
        
        # 启动多个线程
        threads = []
        for i in range(3):
            thread = threading.Thread(target=insert_worker, args=(i, 10))
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证结果
        result = self.db.execute_sql("SELECT COUNT(*) FROM concurrent_test;")
        self.assertTrue(result['success'], "并发操作后查询失败")
        # 注意：由于并发问题，可能不是所有插入都成功
    
    def test_transaction_support(self):
        """测试事务支持"""
        self.logger.info("测试事务支持")
        
        # 创建表
        self.db.execute_sql("CREATE TABLE txn_test(id INT, name VARCHAR, balance INT);")
        self.db.execute_sql("INSERT INTO txn_test(id,name,balance) VALUES (1,'Alice',1000);")
        self.db.execute_sql("INSERT INTO txn_test(id,name,balance) VALUES (2,'Bob',500);")
        
        # 测试事务管理器
        tm = TransactionManager()
        
        # 开始事务
        txn_id = tm.begin_transaction()
        self.assertIsNotNone(txn_id, "开始事务失败")
        
        # 获取锁
        self.assertTrue(tm.acquire_read_lock(txn_id, "txn_test"), "获取读锁失败")
        self.assertTrue(tm.acquire_write_lock(txn_id, "txn_test"), "获取写锁失败")
        
        # 记录操作
        tm.log_operation(txn_id, {
            "type": "UPDATE",
            "table_name": "txn_test",
            "record_id": 1,
            "old_values": {"balance": 1000},
            "new_values": {"balance": 900}
        })
        
        # 提交事务
        self.assertTrue(tm.commit_transaction(txn_id), "提交事务失败")
    
    def test_index_support(self):
        """测试索引支持"""
        self.logger.info("测试索引支持")
        
        # 创建索引管理器
        im = IndexManager()
        
        # 创建索引
        self.assertTrue(im.create_index("student", "id", IndexType.BPLUS_TREE), "创建B+树索引失败")
        self.assertTrue(im.create_index("student", "name", IndexType.HASH), "创建哈希索引失败")
        
        # 插入记录到索引
        self.assertTrue(im.insert_record("student", "id", 1, 1, 100), "插入记录到索引失败")
        self.assertTrue(im.insert_record("student", "id", 2, 1, 200), "插入记录到索引失败")
        self.assertTrue(im.insert_record("student", "name", "Alice", 1, 100), "插入记录到索引失败")
        
        # 搜索记录
        result = im.search_record("student", "id", 1)
        self.assertIsNotNone(result, "索引搜索失败")
        self.assertEqual(result, (1, 100), "索引搜索结果不正确")
        
        # 获取索引信息
        info = im.get_index_info("student")
        self.assertIn("id", info, "索引信息中缺少id索引")
        self.assertIn("name", info, "索引信息中缺少name索引")
    
    def test_logging_and_monitoring(self):
        """测试日志和监控"""
        self.logger.info("测试日志和监控")
        
        # 测试日志记录
        self.logger.info("测试信息日志")
        self.logger.warning("测试警告日志")
        self.logger.error("测试错误日志")
        
        # 测试性能监控
        start_time = time.time()
        time.sleep(0.001)  # 模拟操作
        duration = time.time() - start_time
        
        performance_monitor.log_sql_execution("SELECT * FROM test", True, duration, 5)
        performance_monitor.log_cache_operation("GET", duration, True)
        performance_monitor.log_storage_operation("INSERT", duration, "test_table", 1)
        
        # 获取性能报告
        report = performance_monitor.get_performance_report()
        self.assertIn("summary", report, "性能报告缺少摘要信息")
        self.assertIn("sql_operations", report, "性能报告缺少SQL操作信息")
        self.assertIn("cache_operations", report, "性能报告缺少缓存操作信息")
    
    def test_complex_queries(self):
        """测试复杂查询"""
        self.logger.info("测试复杂查询")
        
        # 创建表
        self.db.execute_sql("""
            CREATE TABLE employee(
                id INT, 
                name VARCHAR, 
                department VARCHAR, 
                salary INT, 
                manager_id INT
            );
        """)
        
        # 插入测试数据
        employees = [
            (1, "Alice", "Engineering", 80000, None),
            (2, "Bob", "Engineering", 75000, 1),
            (3, "Charlie", "Marketing", 70000, None),
            (4, "David", "Engineering", 90000, 1),
            (5, "Eve", "Marketing", 65000, 3)
        ]
        
        for emp in employees:
            sql = f"INSERT INTO employee(id,name,department,salary,manager_id) VALUES ({emp[0]},'{emp[1]}','{emp[2]}',{emp[3]},{emp[4] if emp[4] else 'NULL'});"
            result = self.db.execute_sql(sql)
            self.assertTrue(result['success'], f"插入员工数据失败: {result['message']}")
        
        # 复杂查询测试
        queries = [
            "SELECT * FROM employee WHERE department = 'Engineering';",
            "SELECT * FROM employee WHERE salary > 75000;",
            "SELECT name, salary FROM employee WHERE manager_id IS NOT NULL;",
            "SELECT COUNT(*) FROM employee;",
            "SELECT department, COUNT(*) FROM employee GROUP BY department;"
        ]
        
        for query in queries:
            result = self.db.execute_sql(query)
            self.assertTrue(result['success'], f"复杂查询失败: {query} - {result['message']}")
    
    def test_system_integration(self):
        """测试系统集成"""
        self.logger.info("测试系统集成")
        
        # 创建多个表
        tables = [
            "CREATE TABLE users(id INT, username VARCHAR, email VARCHAR);",
            "CREATE TABLE posts(id INT, user_id INT, title VARCHAR, content VARCHAR);",
            "CREATE TABLE comments(id INT, post_id INT, user_id INT, content VARCHAR);"
        ]
        
        for table_sql in tables:
            result = self.db.execute_sql(table_sql)
            self.assertTrue(result['success'], f"创建表失败: {result['message']}")
        
        # 插入关联数据
        self.db.execute_sql("INSERT INTO users(id,username,email) VALUES (1,'alice','alice@example.com');")
        self.db.execute_sql("INSERT INTO users(id,username,email) VALUES (2,'bob','bob@example.com');")
        
        self.db.execute_sql("INSERT INTO posts(id,user_id,title,content) VALUES (1,1,'Hello World','This is my first post');")
        self.db.execute_sql("INSERT INTO posts(id,user_id,title,content) VALUES (2,2,'Second Post','Another post here');")
        
        self.db.execute_sql("INSERT INTO comments(id,post_id,user_id,content) VALUES (1,1,2,'Great post!');")
        self.db.execute_sql("INSERT INTO comments(id,post_id,user_id,content) VALUES (2,2,1,'Nice work!');")
        
        # 测试关联查询
        result = self.db.execute_sql("SELECT u.username, p.title FROM users u, posts p WHERE u.id = p.user_id;")
        self.assertTrue(result['success'], "关联查询失败")
        
        # 测试数据完整性
        result = self.db.execute_sql("SELECT COUNT(*) FROM users;")
        self.assertTrue(result['success'], "用户表查询失败")
        
        result = self.db.execute_sql("SELECT COUNT(*) FROM posts;")
        self.assertTrue(result['success'], "文章表查询失败")
        
        result = self.db.execute_sql("SELECT COUNT(*) FROM comments;")
        self.assertTrue(result['success'], "评论表查询失败")


def run_performance_benchmark():
    """运行性能基准测试"""
    print("运行性能基准测试...")
    
    db = Database("benchmark.db")
    
    # 创建测试表
    db.execute_sql("CREATE TABLE benchmark(id INT, name VARCHAR, value INT, data VARCHAR);")
    
    # 插入性能测试
    print("测试插入性能...")
    start_time = time.time()
    for i in range(1000):
        db.execute_sql(f"INSERT INTO benchmark(id,name,value,data) VALUES ({i},'Name{i}',{i*10},'Data{i}');")
    insert_time = time.time() - start_time
    print(f"插入1000条记录耗时: {insert_time:.3f}秒")
    print(f"平均插入时间: {insert_time/1000*1000:.3f}毫秒/条")
    
    # 查询性能测试
    print("测试查询性能...")
    start_time = time.time()
    for _ in range(100):
        db.execute_sql("SELECT * FROM benchmark WHERE value > 500;")
    query_time = time.time() - start_time
    print(f"执行100次条件查询耗时: {query_time:.3f}秒")
    print(f"平均查询时间: {query_time/100*1000:.3f}毫秒/次")
    
    # 更新性能测试
    print("测试更新性能...")
    start_time = time.time()
    for i in range(100):
        db.execute_sql(f"UPDATE benchmark SET value = {i*20} WHERE id = {i};")
    update_time = time.time() - start_time
    print(f"执行100次更新耗时: {update_time:.3f}秒")
    print(f"平均更新时间: {update_time/100*1000:.3f}毫秒/次")
    
    # 删除性能测试
    print("测试删除性能...")
    start_time = time.time()
    for i in range(100):
        db.execute_sql(f"DELETE FROM benchmark WHERE id = {i};")
    delete_time = time.time() - start_time
    print(f"执行100次删除耗时: {delete_time:.3f}秒")
    print(f"平均删除时间: {delete_time/100*1000:.3f}毫秒/次")
    
    # 获取性能报告
    report = performance_monitor.get_performance_report()
    print(f"\n性能报告:")
    print(f"总操作数: {report['summary']['total_operations']}")
    print(f"平均耗时: {report['summary']['avg_duration']:.3f}秒")
    print(f"成功率: {report['summary']['success_rate']:.2%}")
    
    db.close()
    
    # 清理测试文件
    import os
    if os.path.exists("benchmark.db"):
        os.remove("benchmark.db")


if __name__ == "__main__":
    # 运行单元测试
    print("运行完整系统测试...")
    unittest.main(verbosity=2, exit=False)
    
    # 运行性能基准测试
    print("\n" + "="*50)
    run_performance_benchmark()
    
    print("\n所有测试完成！")






