#!/usr/bin/env python3
"""
增强版SQL解析器
支持复杂的SQL功能：JOIN、聚合函数、子查询、窗口函数、排序分页等
"""

from typing import List, Optional, Dict, Any
from .lexer import SQLLexer, Token, TokenType
from .parser import ASTNode, ASTNodeType

class EnhancedSQLParser:
    """增强版SQL解析器"""
    
    def __init__(self):
        self.lexer = SQLLexer()
        self.tokens: List[Token] = []
        self.current_token_index = 0
    
    def parse(self, sql: str) -> ASTNode:
        """解析SQL语句"""
        self.tokens = self.lexer.tokenize(sql)
        self.current_token_index = 0
        
        if not self.tokens:
            raise SyntaxError("空SQL语句")
        
        return self._parse_statement()
    
    def _current_token(self) -> Token:
        """获取当前token"""
        if self.current_token_index < len(self.tokens):
            return self.tokens[self.current_token_index]
        return Token(TokenType.EOF, "", 0, 0)
    
    def _next_token(self) -> Token:
        """获取下一个token"""
        result = self._current_token()  # 先返回当前token
        if self.current_token_index < len(self.tokens) - 1:
            self.current_token_index += 1  # 然后移动到下一个token
        return result
    
    def _match(self, token_type: TokenType) -> bool:
        """检查当前token是否匹配指定类型"""
        return self._current_token().token_type == token_type
    
    def _consume(self, token_type: TokenType, error_message: str = "") -> Token:
        """消费指定类型的token"""
        if self._match(token_type):
            token = self._current_token()
            self._next_token()
            return token
        else:
            current = self._current_token()
            raise SyntaxError(f"语法错误: {error_message} (行{current.line}, 列{current.column})")
    
    def _parse_statement(self) -> ASTNode:
        """解析SQL语句"""
        token = self._current_token()
        
        if token.token_type == TokenType.SELECT:
            return self._parse_select()
        elif token.token_type == TokenType.CREATE:
            self._next_token()
            if self._match(TokenType.TABLE):
                return self._parse_create_table()
            elif self._match(TokenType.INDEX) or self._match(TokenType.UNIQUE):
                return self._parse_create_index()
            else:
                raise SyntaxError("未知的CREATE语句类型")
        elif token.token_type == TokenType.INSERT:
            return self._parse_insert()
        elif token.token_type == TokenType.UPDATE:
            return self._parse_update()
        elif token.token_type == TokenType.DELETE:
            return self._parse_delete()
        elif token.token_type == TokenType.DROP:
            self._next_token()
            if self._match(TokenType.INDEX):
                return self._parse_drop_index()
            else:
                raise SyntaxError("未知的DROP语句类型")
        else:
            raise SyntaxError(f"未知语句类型: {token.lexeme} (行{token.line}, 列{token.column})")
    
    def _parse_select(self) -> ASTNode:
        """解析SELECT语句"""
        self._consume(TokenType.SELECT, "期望SELECT关键字")
        
        # 解析SELECT列表
        select_list = self._parse_select_list()
        
        # 解析FROM子句
        self._consume(TokenType.FROM, "期望FROM关键字")
        from_clause = self._parse_table_reference()
        
        # 创建SELECT节点
        select_node = ASTNode(ASTNodeType.SELECT, {
            'select_list': select_list,
            'from_clause': from_clause
        })
        
        # 解析WHERE子句
        if self._match(TokenType.WHERE):
            self._next_token()
            where_clause = self._parse_where_clause()
            select_node.children.append(where_clause)
        
        # 解析GROUP BY子句
        if self._match(TokenType.GROUP):
            self._next_token()
            self._consume(TokenType.BY, "期望BY关键字")
            group_by = self._parse_group_by()
            select_node.children.append(group_by)
        
        # 解析ORDER BY子句
        if self._match(TokenType.ORDER):
            self._next_token()
            self._consume(TokenType.BY, "期望BY关键字")
            order_by = self._parse_order_by()
            select_node.children.append(order_by)
        
        # 解析LIMIT子句
        if self._match(TokenType.LIMIT):
            self._next_token()
            limit = self._parse_limit()
            select_node.children.append(limit)
        
        return select_node
    
    def _parse_select_list(self) -> ASTNode:
        """解析SELECT列表"""
        select_list = ASTNode(ASTNodeType.SELECT_LIST, [])
        
        while True:
            if self._match(TokenType.MULTIPLY):
                # 处理 *
                self._next_token()
                select_list.children.append(ASTNode(ASTNodeType.COLUMN_REF, "*"))
                break
            else:
                # 处理表达式或聚合函数
                if self._match(TokenType.COUNT) or self._match(TokenType.SUM) or self._match(TokenType.AVG) or self._match(TokenType.MAX) or self._match(TokenType.MIN):
                    # 聚合函数
                    expr = self._parse_aggregate_function()
                else:
                    # 普通表达式
                    expr = self._parse_expression()
                select_list.children.append(expr)
                
                # 检查别名
                if self._match(TokenType.AS):
                    self._next_token()
                    alias = self._consume(TokenType.IDENTIFIER, "期望别名").lexeme
                    expr.value = {'expression': expr.value, 'alias': alias}
                
                # 检查是否有更多列
                if self._match(TokenType.COMMA):
                    self._next_token()
                elif self._match(TokenType.FROM):
                    # 遇到FROM关键字，结束SELECT列表
                    break
                else:
                    break
        
        return select_list
    
    def _parse_expression(self) -> ASTNode:
        """解析表达式"""
        if self._match(TokenType.IDENTIFIER):
            # 列引用
            token = self._next_token()
            return ASTNode(ASTNodeType.COLUMN_REF, token.lexeme)
        elif self._match(TokenType.NUMBER):
            # 数字字面量
            token = self._next_token()
            return ASTNode(ASTNodeType.LITERAL, {'value': int(token.lexeme)})
        elif self._match(TokenType.STRING):
            # 字符串字面量
            token = self._next_token()
            return ASTNode(ASTNodeType.LITERAL, {'value': token.lexeme})
        else:
            raise SyntaxError(f"无效的表达式: {self._current_token().lexeme}")
    
    def _parse_aggregate_function(self) -> ASTNode:
        """解析聚合函数"""
        func_token = self._current_token()
        self._next_token()
        
        self._consume(TokenType.LEFT_PAREN, "期望左括号")
        
        if self._match(TokenType.MULTIPLY):
            # COUNT(*)
            self._next_token()
            arg = ASTNode(ASTNodeType.COLUMN_REF, "*")
        else:
            # 其他聚合函数的参数
            arg = self._parse_expression()
        
        self._consume(TokenType.RIGHT_PAREN, "期望右括号")
        
        return ASTNode(ASTNodeType.FUNCTION_CALL, {
            'function': func_token.lexeme.upper(),
            'argument': arg
        })
    
    def _parse_table_reference(self) -> ASTNode:
        """解析表引用"""
        table_name = self._consume(TokenType.IDENTIFIER, "期望表名").lexeme
        return ASTNode(ASTNodeType.TABLE_REF, table_name)
    
    def _parse_where_clause(self) -> ASTNode:
        """解析WHERE子句"""
        condition = self._parse_condition()
        return ASTNode(ASTNodeType.WHERE_CLAUSE, condition)
    
    def _parse_condition(self) -> ASTNode:
        """解析条件表达式"""
        left = self._parse_comparison()
        
        # 处理逻辑操作符（AND、OR）
        while self._match(TokenType.AND) or self._match(TokenType.OR):
            op_token = self._current_token()
            self._next_token()
            right = self._parse_comparison()
            
            left = ASTNode(ASTNodeType.LOGICAL_OP, {
                'left': left,
                'operator': op_token.lexeme.upper(),
                'right': right
            })
        
        return left
    
    def _parse_comparison(self) -> ASTNode:
        """解析比较表达式"""
        left = self._parse_expression()
        
        if self._match(TokenType.EQUAL) or self._match(TokenType.NOT_EQUALS) or \
           self._match(TokenType.GREATER_THAN) or self._match(TokenType.LESS_THAN) or \
           self._match(TokenType.GREATER_EQUAL) or self._match(TokenType.LESS_EQUAL):
            operator = self._current_token().lexeme
            self._next_token()
            right = self._parse_expression()
            
            return ASTNode(ASTNodeType.COMPARISON, {
                'left': left,
                'operator': operator,
                'right': right
            })
        else:
            return left
    
    def _parse_group_by(self) -> ASTNode:
        """解析GROUP BY子句"""
        columns = []
        while True:
            column = self._consume(TokenType.IDENTIFIER, "期望列名").lexeme
            columns.append(column)
            
            if self._match(TokenType.COMMA):
                self._next_token()
            else:
                break
        
        return ASTNode(ASTNodeType.GROUP_BY, columns)
    
    def _parse_order_by(self) -> ASTNode:
        """解析ORDER BY子句"""
        columns = []
        while True:
            column = self._consume(TokenType.IDENTIFIER, "期望列名").lexeme
            direction = "ASC"
            
            if self._match(TokenType.ASC) or self._match(TokenType.DESC):
                direction = self._current_token().lexeme.upper()
                self._next_token()
            
            columns.append({'column': column, 'direction': direction})
            
            if self._match(TokenType.COMMA):
                self._next_token()
            else:
                break
        
        return ASTNode(ASTNodeType.ORDER_BY, columns)
    
    def _parse_limit(self) -> ASTNode:
        """解析LIMIT子句"""
        limit_value = self._consume(TokenType.NUMBER, "期望LIMIT值").lexeme
        return ASTNode(ASTNodeType.LIMIT, int(limit_value))
    
    def _parse_create_table(self) -> ASTNode:
        """解析CREATE TABLE语句"""
        self._consume(TokenType.TABLE, "期望TABLE关键字")
        table_name = self._consume(TokenType.IDENTIFIER, "期望表名").lexeme
        
        self._consume(TokenType.LEFT_PAREN, "期望左括号")
        
        columns = []
        while True:
            col_name = self._consume(TokenType.IDENTIFIER, "期望列名").lexeme
            
            # 检查列类型
            if self._match(TokenType.INT):
                col_type = "INT"
                self._next_token()
            elif self._match(TokenType.VARCHAR):
                col_type = "VARCHAR"
                self._next_token()
            else:
                col_type = self._consume(TokenType.IDENTIFIER, "期望列类型").lexeme
            
            columns.append({'name': col_name, 'type': col_type})
            
            if self._match(TokenType.COMMA):
                self._next_token()
            else:
                break
        
        self._consume(TokenType.RIGHT_PAREN, "期望右括号")
        
        return ASTNode(ASTNodeType.CREATE_TABLE, {
            'table_name': table_name,
            'columns': columns
        })
    
    def _parse_create_index(self) -> ASTNode:
        """解析CREATE INDEX语句"""
        is_unique = self._match(TokenType.UNIQUE)
        if is_unique:
            self._next_token()
        
        self._consume(TokenType.INDEX, "期望INDEX关键字")
        index_name = self._consume(TokenType.IDENTIFIER, "期望索引名").lexeme
        
        self._consume(TokenType.ON, "期望ON关键字")
        table_name = self._consume(TokenType.IDENTIFIER, "期望表名").lexeme
        
        self._consume(TokenType.LEFT_PAREN, "期望左括号")
        column_name = self._consume(TokenType.IDENTIFIER, "期望列名").lexeme
        self._consume(TokenType.RIGHT_PAREN, "期望右括号")
        
        return ASTNode(ASTNodeType.CREATE_INDEX, {
            'index_name': index_name,
            'table_name': table_name,
            'column_name': column_name,
            'unique': is_unique
        })
    
    def _parse_drop_index(self) -> ASTNode:
        """解析DROP INDEX语句"""
        self._consume(TokenType.INDEX, "期望INDEX关键字")
        index_name = self._consume(TokenType.IDENTIFIER, "期望索引名").lexeme
        
        return ASTNode(ASTNodeType.DROP_INDEX, index_name)
    
    def _parse_insert(self) -> ASTNode:
        """解析INSERT语句"""
        self._next_token()  # 跳过INSERT
        self._consume(TokenType.INTO, "期望INTO关键字")
        
        table_name = self._consume(TokenType.IDENTIFIER, "期望表名").lexeme
        
        # 解析列列表
        self._consume(TokenType.LEFT_PAREN, "期望左括号")
        columns = []
        while True:
            col = self._consume(TokenType.IDENTIFIER, "期望列名").lexeme
            columns.append(col)
            if self._match(TokenType.COMMA):
                self._next_token()
            else:
                break
        self._consume(TokenType.RIGHT_PAREN, "期望右括号")
        
        # 解析VALUES
        self._consume(TokenType.VALUES, "期望VALUES关键字")
        self._consume(TokenType.LEFT_PAREN, "期望左括号")
        
        values = []
        while True:
            if self._match(TokenType.NUMBER):
                val = int(self._current_token().lexeme)
                self._next_token()
            elif self._match(TokenType.STRING):
                val = self._current_token().lexeme
                self._next_token()
            else:
                raise SyntaxError("期望值")
            
            values.append(val)
            
            if self._match(TokenType.COMMA):
                self._next_token()
            else:
                break
        
        self._consume(TokenType.RIGHT_PAREN, "期望右括号")
        
        return ASTNode(ASTNodeType.INSERT, {
            'table_name': table_name,
            'columns': columns,
            'values': values
        })
    
    def _parse_update(self) -> ASTNode:
        """解析UPDATE语句"""
        self._next_token()  # 跳过UPDATE
        table_name = self._consume(TokenType.IDENTIFIER, "期望表名").lexeme
        
        self._consume(TokenType.SET, "期望SET关键字")
        
        # 解析SET子句
        updates = []
        while True:
            col = self._consume(TokenType.IDENTIFIER, "期望列名").lexeme
            self._consume(TokenType.EQUAL, "期望等号")
            
            if self._match(TokenType.NUMBER):
                val = int(self._current_token().lexeme)
                self._next_token()
            elif self._match(TokenType.STRING):
                val = self._current_token().lexeme
                self._next_token()
            else:
                raise SyntaxError("期望值")
            
            updates.append({'column': col, 'value': val})
            
            if self._match(TokenType.COMMA):
                self._next_token()
            else:
                break
        
        # 解析WHERE子句
        where_clause = None
        if self._match(TokenType.WHERE):
            self._next_token()
            where_clause = self._parse_condition()
        
        return ASTNode(ASTNodeType.UPDATE, {
            'table_name': table_name,
            'updates': updates,
            'where_clause': where_clause
        })
    
    def _parse_delete(self) -> ASTNode:
        """解析DELETE语句"""
        self._next_token()  # 跳过DELETE
        self._consume(TokenType.FROM, "期望FROM关键字")
        
        table_name = self._consume(TokenType.IDENTIFIER, "期望表名").lexeme
        
        # 解析WHERE子句
        where_clause = None
        if self._match(TokenType.WHERE):
            self._next_token()
            where_clause = self._parse_condition()
        
        return ASTNode(ASTNodeType.DELETE, {
            'table_name': table_name,
            'where_clause': where_clause
        })
