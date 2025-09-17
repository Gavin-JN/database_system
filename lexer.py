"""
SQL词法分析器
识别SQL关键字、标识符、常量、运算符、分隔符等
"""
import re
from typing import List, Optional, Tuple
from enum import Enum


class TokenType(Enum):
    """Token类型枚举"""
    # 关键字
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    CREATE = "CREATE"
    TABLE = "TABLE"
    INSERT = "INSERT"
    INTO = "INTO"
    VALUES = "VALUES"
    DELETE = "DELETE"
    UPDATE = "UPDATE"
    SET = "SET"
    INT = "INT"
    VARCHAR = "VARCHAR"
    
    # 聚合函数
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MAX = "MAX"
    MIN = "MIN"
    
    # 分组和排序
    GROUP = "GROUP"
    BY = "BY"
    ORDER = "ORDER"
    ASC = "ASC"
    DESC = "DESC"
    LIMIT = "LIMIT"
    
    # 索引
    INDEX = "INDEX"
    UNIQUE = "UNIQUE"
    DROP = "DROP"
    ON = "ON"
    
    # 逻辑操作符
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    
    # 别名
    AS = "AS"
    
    # 标识符和常量
    IDENTIFIER = "IDENTIFIER"
    NUMBER = "NUMBER"
    STRING = "STRING"
    WILDCARD = "WILDCARD"  # *
    
    # 运算符
    EQUAL = "EQUAL"
    EQUALS = "EQUALS"
    GREATER_THAN = "GREATER_THAN"
    LESS_THAN = "LESS_THAN"
    GREATER_EQUAL = "GREATER_EQUAL"
    LESS_EQUAL = "LESS_EQUAL"
    NOT_EQUALS = "NOT_EQUALS"
    PLUS = "PLUS"
    MINUS = "MINUS"
    MULTIPLY = "MULTIPLY"
    DIVIDE = "DIVIDE"
    
    # 分隔符
    SEMICOLON = "SEMICOLON"
    COMMA = "COMMA"
    LEFT_PAREN = "LEFT_PAREN"
    RIGHT_PAREN = "RIGHT_PAREN"
    LEFT_BRACKET = "LEFT_BRACKET"
    RIGHT_BRACKET = "RIGHT_BRACKET"
    DOT = "DOT"
    BACKTICK = "BACKTICK"
    
    # 其他
    EOF = "EOF"
    ERROR = "ERROR"


class Token:
    """Token类，包含种别码、词素值、位置信息"""
    
    def __init__(self, token_type: TokenType, lexeme: str, line: int, column: int):
        self.token_type = token_type
        self.lexeme = lexeme
        self.line = line
        self.column = column
    
    def __repr__(self):
        return f"[{self.token_type.value}, {self.lexeme}, {self.line}, {self.column}]"
    
    def __str__(self):
        return self.__repr__()


class LexerError(Exception):
    """词法分析错误"""
    pass


class SQLLexer:
    """SQL词法分析器"""
    
    def __init__(self):
        # SQL关键字映射
        self.keywords = {
            'SELECT': TokenType.SELECT,
            'FROM': TokenType.FROM,
            'WHERE': TokenType.WHERE,
            'CREATE': TokenType.CREATE,
            'TABLE': TokenType.TABLE,
            'INSERT': TokenType.INSERT,
            'INTO': TokenType.INTO,
            'VALUES': TokenType.VALUES,
            'DELETE': TokenType.DELETE,
            'UPDATE': TokenType.UPDATE,
            'SET': TokenType.SET,
            'INT': TokenType.INT,
            'VARCHAR': TokenType.VARCHAR,
            'COUNT': TokenType.COUNT,
            'SUM': TokenType.SUM,
            'AVG': TokenType.AVG,
            'MAX': TokenType.MAX,
            'MIN': TokenType.MIN,
            'GROUP': TokenType.GROUP,
            'BY': TokenType.BY,
            'ORDER': TokenType.ORDER,
            'ASC': TokenType.ASC,
            'DESC': TokenType.DESC,
            'LIMIT': TokenType.LIMIT,
            'INDEX': TokenType.INDEX,
            'UNIQUE': TokenType.UNIQUE,
            'DROP': TokenType.DROP,
            'ON': TokenType.ON,
            'AND': TokenType.AND,
            'OR': TokenType.OR,
            'NOT': TokenType.NOT,
            'AS': TokenType.AS,
        }
        
        # 运算符映射
        self.operators = {
            '=': TokenType.EQUAL,
            '>': TokenType.GREATER_THAN,
            '<': TokenType.LESS_THAN,
            '>=': TokenType.GREATER_EQUAL,
            '<=': TokenType.LESS_EQUAL,
            '!=': TokenType.NOT_EQUALS,
            '!': TokenType.NOT,  # 添加单字符 !
            '+': TokenType.PLUS,
            '-': TokenType.MINUS,
            '*': TokenType.MULTIPLY,
            '/': TokenType.DIVIDE,
        }
        
        # 分隔符映射
        self.delimiters = {
            ';': TokenType.SEMICOLON,
            ',': TokenType.COMMA,
            '(': TokenType.LEFT_PAREN,
            ')': TokenType.RIGHT_PAREN,
            '[': TokenType.LEFT_BRACKET,
            ']': TokenType.RIGHT_BRACKET,
            '.': TokenType.DOT,
            '`': TokenType.BACKTICK,
        }
    
    def tokenize(self, input_text: str) -> List[Token]:
        """对输入文本进行词法分析，返回Token列表"""
        tokens = []
        lines = input_text.split('\n')
        
        for line_num, line in enumerate(lines, 1):
            tokens.extend(self._tokenize_line(line, line_num))
        
        # 添加EOF token
        tokens.append(Token(TokenType.EOF, "", len(lines), 0))
        return tokens
    
    def _tokenize_line(self, line: str, line_num: int) -> List[Token]:
        """对单行进行词法分析"""
        tokens = []
        column = 1
        i = 0
        
        while i < len(line):
            char = line[i]
            
            # 跳过空白字符
            if char.isspace():
                i += 1
                column += 1
                continue
            
            # 处理注释
            if char == '-' and i + 1 < len(line) and line[i + 1] == '-':
                break  # 行注释，跳过该行剩余部分
            
            # 处理字符串常量
            if char == "'" or char == '"':
                token, new_i = self._parse_string(line, i, line_num, column)
                tokens.append(token)
                i = new_i
                column += (new_i - i)
                continue
            
            # 处理数字常量
            if char.isdigit():
                token, new_i = self._parse_number(line, i, line_num, column)
                tokens.append(token)
                i = new_i
                column += (new_i - i)
                continue
            
            # 处理标识符和关键字
            if char.isalpha() or char == '_':
                token, new_i = self._parse_identifier(line, i, line_num, column)
                tokens.append(token)
                i = new_i
                column += (new_i - i)
                continue
            
            # 处理运算符
            if char in self.operators:
                token, new_i = self._parse_operator(line, i, line_num, column)
                tokens.append(token)
                i = new_i
                column += (new_i - i)
                continue
            
            # 处理分隔符
            if char in self.delimiters:
                token_type = self.delimiters[char]
                tokens.append(Token(token_type, char, line_num, column))
                i += 1
                column += 1
                continue
            
            # 非法字符
            tokens.append(Token(TokenType.ERROR, char, line_num, column))
            i += 1
            column += 1
        
        return tokens
    
    def _parse_string(self, line: str, start: int, line_num: int, column: int) -> Tuple[Token, int]:
        """解析字符串常量"""
        quote_char = line[start]  # 获取引号字符（单引号或双引号）
        i = start + 1  # 跳过开始的引号
        value = ""
        
        while i < len(line):
            if line[i] == quote_char:
                # 找到结束的引号
                return Token(TokenType.STRING, value, line_num, column), i + 1
            value += line[i]
            i += 1
        
        # 字符串未闭合
        return Token(TokenType.ERROR, f"{quote_char}{value}", line_num, column), i
    
    def _parse_number(self, line: str, start: int, line_num: int, column: int) -> Tuple[Token, int]:
        """解析数字常量"""
        i = start
        value = ""
        
        while i < len(line) and (line[i].isdigit() or line[i] == '.'):
            value += line[i]
            i += 1
        
        return Token(TokenType.NUMBER, value, line_num, column), i
    
    def _parse_identifier(self, line: str, start: int, line_num: int, column: int) -> Tuple[Token, int]:
        """解析标识符或关键字"""
        i = start
        value = ""
        
        while i < len(line) and (line[i].isalnum() or line[i] == '_'):
            value += line[i]
            i += 1
        
        # 检查是否为关键字
        upper_value = value.upper()
        if upper_value in self.keywords:
            token_type = self.keywords[upper_value]
        else:
            token_type = TokenType.IDENTIFIER
        
        return Token(token_type, value, line_num, column), i
    
    def _parse_operator(self, line: str, start: int, line_num: int, column: int) -> Tuple[Token, int]:
        """解析运算符"""
        char = line[start]
        
        # 检查双字符运算符
        if start + 1 < len(line):
            two_char = line[start:start + 2]
            if two_char in self.operators:
                return Token(self.operators[two_char], two_char, line_num, column), start + 2
        
        # 单字符运算符
        if char in self.operators:
            return Token(self.operators[char], char, line_num, column), start + 1
        
        # 未知运算符
        return Token(TokenType.ERROR, char, line_num, column), start + 1


def main():
    """测试词法分析器"""
    test_sql = """
    CREATE TABLE student(id INT, name VARCHAR, age INT);
    INSERT INTO student(id,name,age) VALUES (1,'Alice',20);
    SELECT id,name FROM student WHERE age > 18;
    DELETE FROM student WHERE id = 1;
    """
    
    lexer = SQLLexer()
    tokens = lexer.tokenize(test_sql)
    
    print("词法分析结果:")
    for token in tokens:
        print(token)


if __name__ == "__main__":
    main()
