#!/usr/bin/env python3
"""
SQL自动补全器
提供SQL关键字、表名、列名的自动补全功能
"""

import re
from typing import List, Set, Optional, Tuple


class SQLCompleter:
    """SQL自动补全器"""
    
    def __init__(self, database=None):
        self.database = database
        
        # SQL关键字列表（按使用频率排序）
        self.keywords = [
            # 查询相关
            'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'HAVING',
            'DISTINCT', 'TOP', 'LIMIT', 'OFFSET',
            
            # 数据操作
            'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
            
            # 表操作
            'CREATE', 'TABLE', 'ALTER', 'DROP', 'TRUNCATE',
            'INDEX', 'UNIQUE', 'PRIMARY', 'KEY', 'FOREIGN',
            
            # 数据类型
            'INT', 'INTEGER', 'VARCHAR', 'CHAR', 'TEXT', 'DECIMAL',
            'FLOAT', 'DOUBLE', 'DATE', 'TIME', 'DATETIME', 'TIMESTAMP',
            'BOOLEAN', 'BOOL',
            
            # 聚合函数
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE',
            
            # 字符串函数
            'CONCAT', 'SUBSTRING', 'LENGTH', 'UPPER', 'LOWER', 'TRIM',
            'REPLACE', 'LEFT', 'RIGHT',
            
            # 日期函数
            'NOW', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
            'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
            
            # 逻辑操作符
            'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS',
            
            # 比较操作符
            '=', '!=', '<>', '<', '>', '<=', '>=',
            
            # 排序
            'ASC', 'DESC',
            
            # 连接
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'ON',
            
            # 别名
            'AS',
            
            # 其他
            'NULL', 'DEFAULT', 'AUTO_INCREMENT', 'UNIQUE', 'NOT', 'NULL',
            'CHECK', 'CONSTRAINT', 'REFERENCES', 'CASCADE', 'RESTRICT'
        ]
        
        # 内置函数列表
        self.functions = [
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE',
            'CONCAT', 'SUBSTRING', 'LENGTH', 'UPPER', 'LOWER', 'TRIM',
            'REPLACE', 'LEFT', 'RIGHT', 'NOW', 'CURRENT_DATE', 'CURRENT_TIME',
            'CURRENT_TIMESTAMP', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'
        ]
    
    def get_completions(self, text: str) -> List[str]:
        """
        获取补全建议
        
        Args:
            text: 当前输入的文本
            
        Returns:
            补全建议列表
        """
        # 获取当前单词
        current_word = self._get_current_word(text)
        current_word_upper = current_word.upper()
        
        # 分析上下文
        context = self._analyze_context(text)
        
        completions = []
        
        # 根据上下文提供不同的补全建议
        if context['in_from_clause']:
            # 在FROM子句中，优先建议表名
            completions.extend(self._get_table_completions(current_word_upper))
            completions.extend(self._get_keyword_completions(current_word_upper, ['JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER']))
        elif context['in_where_clause']:
            # 在WHERE子句中，建议列名、操作符和函数
            completions.extend(self._get_column_completions(current_word_upper))
            completions.extend(self._get_operator_completions(current_word_upper))
            completions.extend(self._get_function_completions(current_word_upper))
            completions.extend(self._get_keyword_completions(current_word_upper, ['AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS']))
        elif context['in_create_table']:
            # 在CREATE TABLE中，建议数据类型和约束
            completions.extend(self._get_keyword_completions(current_word_upper, ['INT', 'VARCHAR', 'CHAR', 'TEXT', 'DECIMAL', 'FLOAT', 'DOUBLE', 'DATE', 'TIME', 'DATETIME', 'TIMESTAMP', 'BOOLEAN']))
            completions.extend(self._get_keyword_completions(current_word_upper, ['PRIMARY', 'KEY', 'UNIQUE', 'NOT', 'NULL', 'DEFAULT', 'AUTO_INCREMENT']))
        elif context['in_select_clause']:
            # 在SELECT子句中，优先建议列名和函数
            completions.extend(self._get_column_completions(current_word_upper))
            completions.extend(self._get_function_completions(current_word_upper))
            completions.extend(self._get_keyword_completions(current_word_upper, ['DISTINCT', 'TOP', 'LIMIT']))
        else:
            # 默认情况下，建议所有关键字
            completions.extend(self._get_keyword_completions(current_word_upper))
        
        # 去重并排序
        completions = list(set(completions))
        completions.sort()
        
        return completions
    
    def _get_current_word(self, text: str) -> str:
        """获取当前光标位置的单词"""
        # 找到最后一个单词
        words = re.findall(r'\b\w+\b', text)
        return words[-1] if words else ""
    
    def _analyze_context(self, text: str) -> dict:
        """分析当前上下文"""
        text_upper = text.upper()
        
        context = {
            'in_select_clause': False,
            'in_from_clause': False,
            'in_where_clause': False,
            'in_create_table': False,
            'in_insert_values': False,
            'in_update_set': False
        }
        
        # 简单的上下文分析
        if 'CREATE TABLE' in text_upper:
            context['in_create_table'] = True
        elif 'INSERT INTO' in text_upper and 'VALUES' in text_upper:
            context['in_insert_values'] = True
        elif 'UPDATE' in text_upper and 'SET' in text_upper:
            context['in_update_set'] = True
        elif 'WHERE' in text_upper:
            context['in_where_clause'] = True
        elif 'FROM' in text_upper:
            context['in_from_clause'] = True
        elif 'SELECT' in text_upper:
            context['in_select_clause'] = True
        
        return context
    
    def _get_keyword_completions(self, current_word: str, keywords: List[str] = None) -> List[str]:
        """获取关键字补全建议"""
        if keywords is None:
            keywords = self.keywords
        
        completions = []
        for keyword in keywords:
            if keyword.upper().startswith(current_word):
                completions.append(keyword)
        
        return completions
    
    def _get_table_completions(self, current_word: str) -> List[str]:
        """获取表名补全建议"""
        completions = []
        
        if self.database:
            try:
                # 获取数据库中的表名
                tables = self.database.get_tables()
                for table in tables:
                    if table.upper().startswith(current_word):
                        completions.append(table)
            except:
                pass
        
        return completions
    
    def _get_column_completions(self, current_word: str) -> List[str]:
        """获取列名补全建议"""
        completions = []
        
        if self.database:
            try:
                # 获取所有表的列名
                tables = self.database.get_tables()
                for table in tables:
                    try:
                        # 这里需要根据实际的数据库接口来获取列名
                        # 暂时使用简单的实现
                        pass
                    except:
                        pass
            except:
                pass
        
        return completions
    
    def _get_function_completions(self, current_word: str) -> List[str]:
        """获取函数补全建议"""
        completions = []
        
        for func in self.functions:
            if func.upper().startswith(current_word):
                completions.append(f"{func}()")
        
        return completions
    
    def _get_operator_completions(self, current_word: str) -> List[str]:
        """获取操作符补全建议"""
        operators = ['=', '!=', '<>', '<', '>', '<=', '>=', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS']
        completions = []
        
        for op in operators:
            if op.upper().startswith(current_word):
                completions.append(op)
        
        return completions


def create_sql_completer(database=None):
    """创建SQL补全器实例"""
    return SQLCompleter(database)


if __name__ == "__main__":
    # 测试代码
    completer = SQLCompleter()
    
    # 测试不同的输入
    test_cases = [
        "SEL",
        "SELECT * FROM ",
        "SELECT * FROM users WHERE ",
        "CREATE TABLE test (",
        "UPDATE users SET ",
        "INSERT INTO users VALUES ("
    ]
    
    for test in test_cases:
        print(f"\n输入: '{test}'")
        completions = completer.get_completions(test)
        if completions:
            print(f"补全建议: {', '.join(completions[:5])}")  # 只显示前5个
        else:
            print("没有找到补全建议")

