"""
数据库系统测试
"""
import os
import tempfile
import unittest
from database.database import Database


class TestDatabase(unittest.TestCase):
    """数据库测试类"""
    
    def setUp(self):
        """测试前准备"""
        # 创建临时数据库文件
        self.temp_file = tempfile.mktemp(suffix='.db')
        self.db = Database(self.temp_file)
    
    def tearDown(self):
        """测试后清理"""
        self.db.close()
        if os.path.exists(self.temp_file):
            os.remove(self.temp_file)
    
    def test_create_table(self):
        """测试创建表"""
        sql = "CREATE TABLE test(id INT, name VARCHAR, age INT);"
        result = self.db.execute_sql(sql)
        
        self.assertTrue(result['success'])
        self.assertIn('创建成功', result['message'])
        
        # 检查表是否存在
        tables = self.db.get_tables()
        self.assertIn('test', tables)
    
    def test_insert_data(self):
        """测试插入数据"""
        # 先创建表
        self.db.execute_sql("CREATE TABLE test(id INT, name VARCHAR, age INT);")
        
        # 插入数据
        sql = "INSERT INTO test(id,name,age) VALUES (1,'Alice',20);"
        result = self.db.execute_sql(sql)
        
        self.assertTrue(result['success'])
        self.assertIn('成功插入', result['message'])
    
    def test_select_data(self):
        """测试查询数据"""
        # 创建表并插入数据
        self.db.execute_sql("CREATE TABLE test(id INT, name VARCHAR, age INT);")
        self.db.execute_sql("INSERT INTO test(id,name,age) VALUES (1,'Alice',20);")
        self.db.execute_sql("INSERT INTO test(id,name,age) VALUES (2,'Bob',22);")
        
        # 查询所有数据
        sql = "SELECT * FROM test;"
        result = self.db.execute_sql(sql)
        
        self.assertTrue(result['success'])
        self.assertEqual(len(result['data']), 2)
        
        # 查询特定列
        sql = "SELECT id,name FROM test;"
        result = self.db.execute_sql(sql)
        
        self.assertTrue(result['success'])
        self.assertEqual(len(result['data']), 2)
        self.assertIn('id', result['data'][0])
        self.assertIn('name', result['data'][0])
    
    def test_where_condition(self):
        """测试WHERE条件"""
        # 创建表并插入数据
        self.db.execute_sql("CREATE TABLE test(id INT, name VARCHAR, age INT);")
        self.db.execute_sql("INSERT INTO test(id,name,age) VALUES (1,'Alice',20);")
        self.db.execute_sql("INSERT INTO test(id,name,age) VALUES (2,'Bob',22);")
        self.db.execute_sql("INSERT INTO test(id,name,age) VALUES (3,'Charlie',19);")
        
        # 查询年龄大于20的记录
        sql = "SELECT * FROM test WHERE age > 20;"
        result = self.db.execute_sql(sql)
        
        self.assertTrue(result['success'])
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['name'], 'Bob')
    
    def test_delete_data(self):
        """测试删除数据"""
        # 创建表并插入数据
        self.db.execute_sql("CREATE TABLE test(id INT, name VARCHAR, age INT);")
        self.db.execute_sql("INSERT INTO test(id,name,age) VALUES (1,'Alice',20);")
        self.db.execute_sql("INSERT INTO test(id,name,age) VALUES (2,'Bob',22);")
        
        # 删除特定记录
        sql = "DELETE FROM test WHERE id = 1;"
        result = self.db.execute_sql(sql)
        
        self.assertTrue(result['success'])
        self.assertIn('删除了 1 条记录', result['message'])
        
        # 验证删除结果
        sql = "SELECT * FROM test;"
        result = self.db.execute_sql(sql)
        
        self.assertTrue(result['success'])
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['name'], 'Bob')
    
    def test_error_handling(self):
        """测试错误处理"""
        # 测试语法错误
        sql = "CREATE TABLE test(id INT, name VARCHAR, age INT)";  # 缺少分号
        result = self.db.execute_sql(sql)
        
        # 这里应该能处理语法错误
        self.assertIsNotNone(result)
        
        # 测试表不存在错误
        sql = "SELECT * FROM nonexistent_table;"
        result = self.db.execute_sql(sql)
        
        # 应该返回错误或空结果
        self.assertIsNotNone(result)
    
    def test_data_persistence(self):
        """测试数据持久化"""
        # 创建表并插入数据
        self.db.execute_sql("CREATE TABLE test(id INT, name VARCHAR, age INT);")
        self.db.execute_sql("INSERT INTO test(id,name,age) VALUES (1,'Alice',20);")
        
        # 关闭数据库
        self.db.close()
        
        # 重新打开数据库
        self.db = Database(self.temp_file)
        
        # 查询数据，应该仍然存在
        sql = "SELECT * FROM test;"
        result = self.db.execute_sql(sql)
        
        self.assertTrue(result['success'])
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['name'], 'Alice')


if __name__ == '__main__':
    unittest.main()
