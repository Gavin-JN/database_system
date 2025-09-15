"""
SQL语义分析器
进行表/列存在性检查、类型一致性检查、列数/列序检查
维护系统目录(Catalog)
"""
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass
from enum import Enum
from .parser import ASTNode, ASTNodeType


class DataType(Enum):
    """数据类型枚举"""
    INT = "INT"
    VARCHAR = "VARCHAR"


@dataclass
class ColumnInfo:
    """列信息"""
    name: str
    data_type: DataType
    nullable: bool = True


@dataclass
class TableInfo:
    """表信息"""
    name: str
    columns: Dict[str, ColumnInfo]
    column_order: List[str]  # 列的顺序


class SemanticError(Exception):
    """语义分析错误"""
    def __init__(self, error_type: str, line: int, column: int, reason: str):
        self.error_type = error_type
        self.line = line
        self.column = column
        self.reason = reason
        super().__init__(f"[{error_type}, 行{line}列{column}, {reason}]")


class Catalog:
    """系统目录，维护数据库元数据"""
    
    def __init__(self):
        self.tables: Dict[str, TableInfo] = {}
    
    def create_table(self, table_name: str, columns: List[ColumnInfo]) -> None:
        """创建表"""
        if table_name in self.tables:
            raise SemanticError("TABLE_EXISTS", 0, 0, f"表 '{table_name}' 已存在")
        
        column_dict = {}
        column_order = []
        for col in columns:
            column_dict[col.name] = col
            column_order.append(col.name)
        
        self.tables[table_name] = TableInfo(table_name, column_dict, column_order)
    
    def get_table(self, table_name: str) -> Optional[TableInfo]:
        """获取表信息"""
        return self.tables.get(table_name)
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.tables
    
    def get_column(self, table_name: str, column_name: str) -> Optional[ColumnInfo]:
        """获取列信息"""
        table = self.get_table(table_name)
        if table:
            return table.columns.get(column_name)
        return None
    
    def column_exists(self, table_name: str, column_name: str) -> bool:
        """检查列是否存在"""
        return self.get_column(table_name, column_name) is not None


class SemanticAnalyzer:
    """SQL语义分析器"""
    
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
    
    def analyze(self, ast_nodes: List[ASTNode]) -> List[str]:
        """分析AST节点列表，返回分析结果"""
        results = []
        
        for node in ast_nodes:
            try:
                result = self._analyze_node(node)
                if result:
                    results.append(result)
            except SemanticError as e:
                results.append(str(e))
        
        return results
    
    def _analyze_node(self, node: ASTNode) -> Optional[str]:
        """分析单个AST节点"""
        if node.node_type == ASTNodeType.CREATE_TABLE:
            return self._analyze_create_table(node)
        elif node.node_type == ASTNodeType.INSERT:
            return self._analyze_insert(node)
        elif node.node_type == ASTNodeType.SELECT:
            return self._analyze_select(node)
        elif node.node_type == ASTNodeType.DELETE:
            return self._analyze_delete(node)
        
        return None
    
    def _analyze_create_table(self, node: ASTNode) -> str:
        """分析CREATE TABLE语句"""
        table_name = node.value
        
        # 检查表是否已存在
        if self.catalog.table_exists(table_name):
            raise SemanticError("TABLE_EXISTS", 0, 0, f"表 '{table_name}' 已存在")
        
        # 解析列定义
        columns = []
        for child in node.children:
            if child.node_type == ASTNodeType.COLUMN_DEF:
                col_info = ColumnInfo(
                    name=child.value['name'],
                    data_type=DataType(child.value['type'])
                )
                columns.append(col_info)
        
        # 检查列名重复
        column_names = [col.name for col in columns]
        if len(column_names) != len(set(column_names)):
            raise SemanticError("DUPLICATE_COLUMN", 0, 0, "列名重复")
        
        # 创建表
        self.catalog.create_table(table_name, columns)
        return f"语义检查通过: 成功创建表 '{table_name}'"
    
    def _analyze_insert(self, node: ASTNode) -> str:
        """分析INSERT语句"""
        table_name = node.value
        
        # 检查表是否存在
        if not self.catalog.table_exists(table_name):
            raise SemanticError("TABLE_NOT_EXISTS", 0, 0, f"表 '{table_name}' 不存在")
        
        table_info = self.catalog.get_table(table_name)
        
        # 获取列列表和值列表
        column_list_node = node.children[0]
        value_list_node = node.children[1]
        
        # 检查列数是否一致
        if len(column_list_node.children) != len(value_list_node.children):
            raise SemanticError("COLUMN_COUNT_MISMATCH", 0, 0, 
                              f"列数({len(column_list_node.children)})与值数({len(value_list_node.children)})不匹配")
        
        # 检查列是否存在
        for col_node in column_list_node.children:
            if col_node.node_type == ASTNodeType.COLUMN_REF:
                col_name = col_node.value
                if not self.catalog.column_exists(table_name, col_name):
                    raise SemanticError("COLUMN_NOT_EXISTS", 0, 0, f"列 '{col_name}' 不存在")
        
        # 检查类型一致性
        for i, (col_node, value_node) in enumerate(zip(column_list_node.children, value_list_node.children)):
            if col_node.node_type == ASTNodeType.COLUMN_REF:
                col_name = col_node.value
                col_info = self.catalog.get_column(table_name, col_name)
                
                if col_info and value_node.node_type == ASTNodeType.LITERAL:
                    value_type = value_node.value['type']
                    expected_type = col_info.data_type.value
                    
                    if not self._is_type_compatible(value_type, expected_type):
                        raise SemanticError("TYPE_MISMATCH", 0, 0, 
                                          f"列 '{col_name}' 期望类型 {expected_type}，实际类型 {value_type}")
        
        return f"语义检查通过: 成功插入到表 '{table_name}'"
    
    def _analyze_select(self, node: ASTNode) -> str:
        """分析SELECT语句"""
        table_name = node.value
        
        # 检查表是否存在
        if not self.catalog.table_exists(table_name):
            raise SemanticError("TABLE_NOT_EXISTS", 0, 0, f"表 '{table_name}' 不存在")
        
        # 检查列是否存在
        column_list_node = node.children[0]
        for col_node in column_list_node.children:
            if col_node.node_type == ASTNodeType.COLUMN_REF:
                col_name = col_node.value
                # 跳过 * 通配符
                if col_name != "*" and not self.catalog.column_exists(table_name, col_name):
                    raise SemanticError("COLUMN_NOT_EXISTS", 0, 0, f"列 '{col_name}' 不存在")
        
        # 检查WHERE子句
        if len(node.children) > 1:  # 有WHERE子句
            where_node = node.children[1]
            if where_node.node_type == ASTNodeType.WHERE_CLAUSE:
                self._analyze_where_clause(where_node, table_name)
        
        return f"语义检查通过: 成功查询表 '{table_name}'"
    
    def _analyze_delete(self, node: ASTNode) -> str:
        """分析DELETE语句"""
        table_name = node.value
        
        # 检查表是否存在
        if not self.catalog.table_exists(table_name):
            raise SemanticError("TABLE_NOT_EXISTS", 0, 0, f"表 '{table_name}' 不存在")
        
        # 检查WHERE子句
        if len(node.children) > 0:  # 有WHERE子句
            where_node = node.children[0]
            if where_node.node_type == ASTNodeType.WHERE_CLAUSE:
                self._analyze_where_clause(where_node, table_name)
        
        return f"语义检查通过: 成功删除表 '{table_name}' 中的记录"
    
    def _analyze_where_clause(self, where_node: ASTNode, table_name: str) -> None:
        """分析WHERE子句"""
        condition_node = where_node.children[0]
        self._analyze_expression(condition_node, table_name)
    
    def _analyze_expression(self, node: ASTNode, table_name: str) -> None:
        """分析表达式"""
        if node.node_type == ASTNodeType.COLUMN_REF:
            col_name = node.value
            if not self.catalog.column_exists(table_name, col_name):
                raise SemanticError("COLUMN_NOT_EXISTS", 0, 0, f"列 '{col_name}' 不存在")
        
        elif node.node_type == ASTNodeType.COMPARISON:
            # 检查比较表达式的左右操作数
            left = node.children[0]
            right = node.children[1]
            
            self._analyze_expression(left, table_name)
            self._analyze_expression(right, table_name)
            
            # 检查类型兼容性
            if (left.node_type == ASTNodeType.COLUMN_REF and 
                right.node_type == ASTNodeType.LITERAL):
                col_name = left.value
                col_info = self.catalog.get_column(table_name, col_name)
                value_type = right.value['type']
                
                if col_info and not self._is_type_compatible(value_type, col_info.data_type.value):
                    raise SemanticError("TYPE_MISMATCH", 0, 0, 
                                      f"列 '{col_name}' 与值类型不匹配")
        
        # 递归分析子节点
        for child in node.children:
            self._analyze_expression(child, table_name)
    
    def _is_type_compatible(self, value_type: str, expected_type: str) -> bool:
        """检查类型是否兼容"""
        if expected_type == "INT":
            return value_type == "number"
        elif expected_type == "VARCHAR":
            return value_type == "string"
        return False


def main():
    """测试语义分析器"""
    from .lexer import SQLLexer
    from .parser import SQLParser
    
    test_sql = """
    CREATE TABLE student(id INT, name VARCHAR, age INT);
    INSERT INTO student(id,name,age) VALUES (1,'Alice',20);
    SELECT id,name FROM student WHERE age > 18;
    DELETE FROM student WHERE id = 1;
    """
    
    # 词法分析
    lexer = SQLLexer()
    tokens = lexer.tokenize(test_sql)
    
    # 语法分析
    parser = SQLParser(tokens)
    ast_nodes = parser.parse()
    
    # 语义分析
    catalog = Catalog()
    analyzer = SemanticAnalyzer(catalog)
    results = analyzer.analyze(ast_nodes)
    
    print("语义分析结果:")
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
