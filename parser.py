"""
SQL语法分析器
基于递归下降方法构建抽象语法树(AST)
支持CREATE TABLE、INSERT、SELECT、DELETE四类语句
"""
from typing import List, Optional, Union, Dict, Any
from dataclasses import dataclass
from enum import Enum
from .lexer import Token, TokenType, SQLLexer


class ASTNodeType(Enum):
    """AST节点类型"""
    # 语句类型
    CREATE_TABLE = "CREATE_TABLE"
    CREATE_INDEX = "CREATE_INDEX"
    DROP_INDEX = "DROP_INDEX"
    INSERT = "INSERT"
    SELECT = "SELECT"
    DELETE = "DELETE"
    UPDATE = "UPDATE"
    
    # 表达式类型
    COLUMN_REF = "COLUMN_REF"
    LITERAL = "LITERAL"
    BINARY_OP = "BINARY_OP"
    COMPARISON = "COMPARISON"
    LOGICAL_OP = "LOGICAL_OP"
    FUNCTION_CALL = "FUNCTION_CALL"
    
    # SELECT相关
    SELECT_LIST = "SELECT_LIST"
    TABLE_REF = "TABLE_REF"
    GROUP_BY = "GROUP_BY"
    ORDER_BY = "ORDER_BY"
    LIMIT = "LIMIT"
    
    # 其他
    COLUMN_DEF = "COLUMN_DEF"
    VALUE_LIST = "VALUE_LIST"
    COLUMN_LIST = "COLUMN_LIST"
    WHERE_CLAUSE = "WHERE_CLAUSE"
    SET_CLAUSE = "SET_CLAUSE"
    ASSIGNMENT = "ASSIGNMENT"


@dataclass
class ASTNode:
    """抽象语法树节点"""
    node_type: ASTNodeType
    value: Optional[Any] = None
    children: List['ASTNode'] = None
    
    def __post_init__(self):
        if self.children is None:
            self.children = []
    
    def add_child(self, child: 'ASTNode'):
        """添加子节点"""
        self.children.append(child)
    
    def __repr__(self):
        return f"ASTNode({self.node_type.value}, {self.value}, {len(self.children)} children)"


class ParserError(Exception):
    """语法分析错误"""
    def __init__(self, message: str, line: int, column: int):
        self.message = message
        self.line = line
        self.column = column
        super().__init__(f"语法错误: {message} (行{line}, 列{column})")


class SQLParser:
    """SQL语法分析器"""
    
    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.current_token_index = 0
        self.current_token = tokens[0] if tokens else None
    
    def parse(self) -> List[ASTNode]:
        """解析Token流，返回AST节点列表"""
        statements = []
        
        while not self._is_at_end():
            if self._match(TokenType.SEMICOLON):
                continue  # 跳过分号
            
            statement = self._parse_statement()
            if statement:
                statements.append(statement)
            
            # 跳过分号（如果存在）
            if not self._is_at_end() and self.current_token.token_type == TokenType.SEMICOLON:
                self._advance()
        
        return statements
    
    def _parse_statement(self) -> Optional[ASTNode]:
        """解析语句"""
        if self._match(TokenType.CREATE):
            return self._parse_create_table()
        elif self._match(TokenType.INSERT):
            return self._parse_insert()
        elif self._match(TokenType.SELECT):
            return self._parse_select()
        elif self._match(TokenType.DELETE):
            return self._parse_delete()
        elif self._match(TokenType.UPDATE):
            return self._parse_update()
        else:
            self._error(f"未知语句类型: {self.current_token.lexeme}")
            return None
    
    def _parse_create_table(self) -> ASTNode:
        """解析CREATE TABLE语句"""
        # CREATE TABLE table_name (column_definitions)
        self._consume(TokenType.TABLE, "期望TABLE关键字")
        
        table_name = self._consume(TokenType.IDENTIFIER, "期望表名").lexeme
        
        self._consume(TokenType.LEFT_PAREN, "期望左括号")
        
        column_defs = []
        while not self._check(TokenType.RIGHT_PAREN):
            column_def = self._parse_column_definition()
            column_defs.append(column_def)
            
            if self._check(TokenType.COMMA):
                self._advance()
        
        self._consume(TokenType.RIGHT_PAREN, "期望右括号")
        
        create_table_node = ASTNode(ASTNodeType.CREATE_TABLE, table_name)
        for column_def in column_defs:
            create_table_node.add_child(column_def)
        
        return create_table_node
    
    def _parse_column_definition(self) -> ASTNode:
        """解析列定义"""
        column_name = self._consume(TokenType.IDENTIFIER, "期望列名").lexeme
        column_type = self._consume([TokenType.INT, TokenType.VARCHAR], "期望数据类型").lexeme
        
        return ASTNode(ASTNodeType.COLUMN_DEF, {
            'name': column_name,
            'type': column_type
        })
    
    def _parse_insert(self) -> ASTNode:
        """解析INSERT语句"""
        # INSERT INTO table_name (column_list) VALUES (value_list)
        self._consume(TokenType.INTO, "期望INTO关键字")
        
        table_name = self._consume(TokenType.IDENTIFIER, "期望表名").lexeme
        
        self._consume(TokenType.LEFT_PAREN, "期望左括号")
        
        column_list = self._parse_column_list()
        
        self._consume(TokenType.RIGHT_PAREN, "期望右括号")
        self._consume(TokenType.VALUES, "期望VALUES关键字")
        self._consume(TokenType.LEFT_PAREN, "期望左括号")
        
        value_list = self._parse_value_list()
        
        self._consume(TokenType.RIGHT_PAREN, "期望右括号")
        
        insert_node = ASTNode(ASTNodeType.INSERT, table_name)
        insert_node.add_child(column_list)
        insert_node.add_child(value_list)
        
        return insert_node
    
    def _parse_column_list(self) -> ASTNode:
        """解析列列表"""
        column_list_node = ASTNode(ASTNodeType.COLUMN_LIST)
        
        while not self._check(TokenType.RIGHT_PAREN):
            if self._check(TokenType.MULTIPLY):
                # 处理 * 通配符
                self._advance()
                column_list_node.add_child(ASTNode(ASTNodeType.COLUMN_REF, "*"))
            else:
                column_name = self._consume(TokenType.IDENTIFIER, "期望列名").lexeme
                column_list_node.add_child(ASTNode(ASTNodeType.COLUMN_REF, column_name))
            
            if self._check(TokenType.COMMA):
                self._advance()
        
        return column_list_node
    
    def _parse_value_list(self) -> ASTNode:
        """解析值列表"""
        value_list_node = ASTNode(ASTNodeType.VALUE_LIST)
        
        while not self._check(TokenType.RIGHT_PAREN):
            value = self._parse_expression()
            value_list_node.add_child(value)
            
            if self._check(TokenType.COMMA):
                self._advance()
        
        return value_list_node
    
    def _parse_select_column_list(self) -> ASTNode:
        """解析SELECT语句的列列表"""
        column_list_node = ASTNode(ASTNodeType.COLUMN_LIST)
        
        while not self._check(TokenType.FROM):
            if self._check(TokenType.MULTIPLY):
                # 处理 * 通配符
                self._advance()
                column_list_node.add_child(ASTNode(ASTNodeType.COLUMN_REF, "*"))
            else:
                column_name = self._consume(TokenType.IDENTIFIER, "期望列名").lexeme
                column_list_node.add_child(ASTNode(ASTNodeType.COLUMN_REF, column_name))
            
            if self._check(TokenType.COMMA):
                self._advance()
        
        return column_list_node
    
    def _parse_select(self) -> ASTNode:
        """解析SELECT语句"""
        # SELECT column_list FROM table_name [WHERE condition]
        column_list = self._parse_select_column_list()
        
        self._consume(TokenType.FROM, "期望FROM关键字")
        table_name = self._consume(TokenType.IDENTIFIER, "期望表名").lexeme
        
        select_node = ASTNode(ASTNodeType.SELECT, table_name)
        select_node.add_child(column_list)
        
        # 解析WHERE子句
        if self._match(TokenType.WHERE):
            where_clause = self._parse_where_clause()
            select_node.add_child(where_clause)
        
        return select_node
    
    def _parse_delete(self) -> ASTNode:
        """解析DELETE语句"""
        # DELETE FROM table_name [WHERE condition]
        self._consume(TokenType.FROM, "期望FROM关键字")
        table_name = self._consume(TokenType.IDENTIFIER, "期望表名").lexeme
        
        delete_node = ASTNode(ASTNodeType.DELETE, table_name)
        
        # 解析WHERE子句
        if self._match(TokenType.WHERE):
            where_clause = self._parse_where_clause()
            delete_node.add_child(where_clause)
        
        return delete_node
    
    def _parse_update(self) -> ASTNode:
        """解析UPDATE语句"""
        # UPDATE table_name SET column=value [, column=value ...] [WHERE condition]
        table_name = self._consume(TokenType.IDENTIFIER, "期望表名").lexeme
        
        update_node = ASTNode(ASTNodeType.UPDATE, table_name)
        
        # 解析SET子句
        self._consume(TokenType.SET, "期望SET关键字")
        set_clause = self._parse_set_clause()
        update_node.add_child(set_clause)
        
        # 解析WHERE子句
        if self._match(TokenType.WHERE):
            where_clause = self._parse_where_clause()
            update_node.add_child(where_clause)
        
        return update_node
    
    def _parse_set_clause(self) -> ASTNode:
        """解析SET子句"""
        set_node = ASTNode(ASTNodeType.SET_CLAUSE)
        
        while True:
            # 解析赋值 column = value
            column_name = self._consume(TokenType.IDENTIFIER, "期望列名").lexeme
            self._consume(TokenType.EQUAL, "期望等号")
            value = self._parse_expression()
            
            assignment = ASTNode(ASTNodeType.ASSIGNMENT, column_name)
            assignment.add_child(value)
            set_node.add_child(assignment)
            
            if not self._match(TokenType.COMMA):
                break
        
        return set_node
    
    def _parse_where_clause(self) -> ASTNode:
        """解析WHERE子句"""
        condition = self._parse_expression()
        where_node = ASTNode(ASTNodeType.WHERE_CLAUSE)
        where_node.add_child(condition)
        return where_node
    
    def _parse_expression(self) -> ASTNode:
        """解析表达式"""
        return self._parse_comparison()
    
    def _parse_comparison(self) -> ASTNode:
        """解析比较表达式"""
        left = self._parse_primary()
        
        if self._match([TokenType.EQUAL, TokenType.GREATER_THAN, TokenType.LESS_THAN, 
                       TokenType.GREATER_EQUAL, TokenType.LESS_EQUAL, TokenType.NOT_EQUALS]):
            operator = self._previous().lexeme
            right = self._parse_primary()
            
            comparison_node = ASTNode(ASTNodeType.COMPARISON, operator)
            comparison_node.add_child(left)
            comparison_node.add_child(right)
            return comparison_node
        
        return left
    
    def _parse_primary(self) -> ASTNode:
        """解析基本表达式"""
        if self._match(TokenType.NUMBER):
            return ASTNode(ASTNodeType.LITERAL, {
                'type': 'number',
                'value': self._previous().lexeme
            })
        
        if self._match(TokenType.STRING):
            return ASTNode(ASTNodeType.LITERAL, {
                'type': 'string',
                'value': self._previous().lexeme
            })
        
        if self._match(TokenType.IDENTIFIER):
            return ASTNode(ASTNodeType.COLUMN_REF, self._previous().lexeme)
        
        if self._match(TokenType.LEFT_PAREN):
            expr = self._parse_expression()
            self._consume(TokenType.RIGHT_PAREN, "期望右括号")
            return expr
        
        self._error(f"意外的token: {self.current_token.lexeme}")
        return None
    
    def _match(self, token_types: Union[TokenType, List[TokenType]]) -> bool:
        """检查当前token是否匹配指定类型"""
        if isinstance(token_types, TokenType):
            token_types = [token_types]
        
        for token_type in token_types:
            if self._check(token_type):
                self._advance()
                return True
        return False
    
    def _check(self, token_type: TokenType) -> bool:
        """检查当前token是否为指定类型"""
        if self._is_at_end():
            return False
        return self.current_token.token_type == token_type
    
    def _advance(self) -> Token:
        """前进到下一个token"""
        if not self._is_at_end():
            self.current_token_index += 1
            self.current_token = self.tokens[self.current_token_index]
        return self._previous()
    
    def _previous(self) -> Token:
        """获取前一个token"""
        return self.tokens[self.current_token_index - 1]
    
    def _consume(self, token_type: Union[TokenType, List[TokenType]], message: str) -> Token:
        """消费指定类型的token"""
        if isinstance(token_type, TokenType):
            token_type = [token_type]
        
        if self._check(token_type[0]) if len(token_type) == 1 else any(self._check(t) for t in token_type):
            return self._advance()
        
        self._error(message)
        # 这里不应该到达，因为_error会抛出异常
        return None
    
    def _is_at_end(self) -> bool:
        """检查是否到达token流末尾"""
        return self.current_token.token_type == TokenType.EOF
    
    def _error(self, message: str):
        """抛出语法错误"""
        raise ParserError(message, self.current_token.line, self.current_token.column)


def print_ast(node: ASTNode, indent: int = 0):
    """打印AST结构"""
    print("  " * indent + f"{node.node_type.value}: {node.value}")
    for child in node.children:
        print_ast(child, indent + 1)


def main():
    """测试语法分析器"""
    test_sql = """
    CREATE TABLE student(id INT, name VARCHAR, age INT);
    INSERT INTO student(id,name,age) VALUES (1,'Alice',20);
    SELECT id,name FROM student WHERE age > 18;
    DELETE FROM student WHERE id = 1;
    """
    
    lexer = SQLLexer()
    tokens = lexer.tokenize(test_sql)
    
    parser = SQLParser(tokens)
    try:
        ast_nodes = parser.parse()
        print("语法分析结果 (AST):")
        for i, node in enumerate(ast_nodes):
            print(f"\n语句 {i + 1}:")
            print_ast(node)
    except ParserError as e:
        print(f"语法错误: {e}")


if __name__ == "__main__":
    main()
