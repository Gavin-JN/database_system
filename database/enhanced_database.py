#!/usr/bin/env python3
"""
增强版数据库
集成增强版解析器和执行引擎
"""

import os
from typing import Dict, Any, List
from storage.storage_engine import StorageEngine
from database.catalog import SystemCatalog
from sql_compiler.enhanced_parser import EnhancedSQLParser
from sql_compiler.enhanced_execution_engine import EnhancedExecutionEngine, ExecutionResult
from utils.logger import DatabaseLogger, LogLevel, logger
import time
class EnhancedDatabase:
    """增强版数据库"""
    
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.data_file = db_file  # 为了兼容性
        self.storage_engine = StorageEngine(db_file)
        self.catalog = SystemCatalog(self.storage_engine)
        self.parser = EnhancedSQLParser()
        self.execution_engine = EnhancedExecutionEngine(self.storage_engine, self.catalog)
        
        # 创建系统表
        self._create_system_tables()
    
    def _create_system_tables(self):
        """创建系统表"""
        # 创建pg_catalog表
        if not self.storage_engine.table_exists('pg_catalog'):
            from storage.storage_engine import ColumnInfo, DataType
            columns = [
                ColumnInfo('table_name', DataType.VARCHAR),
                ColumnInfo('column_info', DataType.VARCHAR),
                ColumnInfo('created_at', DataType.VARCHAR),
                ColumnInfo('page_count', DataType.VARCHAR)
            ]
            self.storage_engine.create_table('pg_catalog', columns)
        
        # 创建pg_indexes表
        if not self.storage_engine.table_exists('pg_indexes'):
            from storage.storage_engine import ColumnInfo, DataType
            columns = [
                ColumnInfo('index_name', DataType.VARCHAR),
                ColumnInfo('table_name', DataType.VARCHAR),
                ColumnInfo('column_name', DataType.VARCHAR),
                ColumnInfo('unique', DataType.VARCHAR),
                ColumnInfo('created_at', DataType.VARCHAR)
            ]
            self.storage_engine.create_table('pg_indexes', columns)
    
    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """执行SQL语句"""
        now = time.time()
        try:
            # 解析SQL
            plan = self.parser.parse(sql)
            
            # 执行计划
            result = self.execution_engine.execute(plan)
            
            # 刷新数据
            self.storage_engine.flush_all()
            
            # 记录执行结果
            duration = time.time() - now
            if result.success:
                logger.log_sql_execution(sql, True, duration, result.rows_affected)
            else:
                logger.log_sql_execution(sql, False, duration, 0, result.message)
            
            return {
                'sql': sql,
                'success': result.success,
                'message': result.message,
                'data': result.data,
                'rows_affected': result.rows_affected,
                'duration': duration
            }
        except Exception as e:
            duration = time.time() - now
            logger.log_sql_execution(sql, False, duration, 0, str(e))
            return {
                'sql': sql,
                'success': False,
                'message': f"执行错误: {str(e)}",
                'data': [],
                'rows_affected': 0,
                'duration': duration
            }
    
    def get_tables(self) -> List[str]:
        """获取所有表名"""
        logger.info("查看所有表格")
        return self.catalog.get_all_tables()
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息"""
        metadata = self.catalog.get_table_metadata(table_name)
        if metadata:
            return {
                'name': metadata.table_name,
                'columns': metadata.columns,
                'created_at': metadata.created_at,
                'page_count': metadata.page_count,
                'column_count': len(metadata.columns) if metadata.columns else 0
            }
        return None
    
    def get_database_info(self) -> Dict[str, Any]:
        """获取数据库信息"""
        storage_stats = self.storage_engine.get_stats()
        catalog_info = self.catalog.get_catalog_info()
        
        return {
            'database_file': self.data_file,
            'tables': catalog_info['total_tables'],
            'storage': storage_stats,
            'catalog': catalog_info
        }
    
    def close(self):
        """关闭数据库连接"""
        self.storage_engine.flush_all()
        if hasattr(self.storage_engine, 'close'):
            self.storage_engine.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
