"""
SQL执行计划生成器
将AST转换为逻辑执行计划
支持CreateTable、Insert、SeqScan、Filter、Project等算子
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
from .parser import ASTNode, ASTNodeType


class OperatorType(Enum):
    """执行算子类型"""
    CREATE_TABLE = "CreateTable"
    INSERT = "Insert"
    SEQ_SCAN = "SeqScan"
    FILTER = "Filter"
    PROJECT = "Project"
    DELETE = "Delete"
    UPDATE = "Update"


@dataclass
class ExecutionPlan:
    """执行计划节点"""
    operator_type: OperatorType
    table_name: Optional[str] = None
    columns: Optional[List[str]] = None
    values: Optional[List[Any]] = None
    condition: Optional[Dict[str, Any]] = None
    children: List['ExecutionPlan'] = None
    
    def __post_init__(self):
        if self.children is None:
            self.children = []
    
    def add_child(self, child: 'ExecutionPlan'):
        """添加子计划"""
        self.children.append(child)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {
            'operator': self.operator_type.value,
            'table_name': self.table_name,
            'columns': self.columns,
            'values': self.values,
            'condition': self.condition,
            'children': [child.to_dict() for child in self.children]
        }
        return result
    
    def to_s_expression(self) -> str:
        """转换为S表达式格式"""
        if not self.children:
            # 叶子节点
            parts = [f"({self.operator_type.value}"]
            if self.table_name:
                parts.append(f"table={self.table_name}")
            if self.columns:
                parts.append(f"columns={self.columns}")
            if self.values:
                parts.append(f"values={self.values}")
            if self.condition:
                parts.append(f"condition={self.condition}")
            parts.append(")")
            return " ".join(parts)
        else:
            # 内部节点
            child_exprs = [child.to_s_expression() for child in self.children]
            return f"({self.operator_type.value} {' '.join(child_exprs)})"


class PlanGenerator:
    """执行计划生成器"""
    
    def __init__(self):
        pass
    
    def generate_plan(self, ast_nodes: List[ASTNode]) -> List[ExecutionPlan]:
        """为AST节点列表生成执行计划"""
        plans = []
        
        for node in ast_nodes:
            plan = self._generate_plan_for_node(node)
            if plan:
                plans.append(plan)
        
        return plans
    
    def _generate_plan_for_node(self, node: ASTNode) -> Optional[ExecutionPlan]:
        """为单个AST节点生成执行计划"""
        if node.node_type == ASTNodeType.CREATE_TABLE:
            return self._generate_create_table_plan(node)
        elif node.node_type == ASTNodeType.INSERT:
            return self._generate_insert_plan(node)
        elif node.node_type == ASTNodeType.SELECT:
            return self._generate_select_plan(node)
        elif node.node_type == ASTNodeType.DELETE:
            return self._generate_delete_plan(node)
        elif node.node_type == ASTNodeType.UPDATE:
            return self._generate_update_plan(node)
        
        return None
    
    def _generate_create_table_plan(self, node: ASTNode) -> ExecutionPlan:
        """生成CREATE TABLE执行计划"""
        table_name = node.value
        columns = []
        
        for child in node.children:
            if child.node_type == ASTNodeType.COLUMN_DEF:
                columns.append({
                    'name': child.value['name'],
                    'type': child.value['type']
                })
        
        return ExecutionPlan(
            operator_type=OperatorType.CREATE_TABLE,
            table_name=table_name,
            columns=columns
        )
    
    def _generate_insert_plan(self, node: ASTNode) -> ExecutionPlan:
        """生成INSERT执行计划"""
        table_name = node.value
        column_list_node = node.children[0]
        value_list_node = node.children[1]
        
        # 提取列名
        columns = []
        for col_node in column_list_node.children:
            if col_node.node_type == ASTNodeType.COLUMN_REF:
                columns.append(col_node.value)
        
        # 提取值
        values = []
        for value_node in value_list_node.children:
            if value_node.node_type == ASTNodeType.LITERAL:
                values.append(value_node.value['value'])
        
        return ExecutionPlan(
            operator_type=OperatorType.INSERT,
            table_name=table_name,
            columns=columns,
            values=values
        )
    
    def _generate_select_plan(self, node: ASTNode) -> ExecutionPlan:
        """生成SELECT执行计划"""
        table_name = node.value
        column_list_node = node.children[0]
        
        # 提取列名
        columns = []
        for col_node in column_list_node.children:
            if col_node.node_type == ASTNodeType.COLUMN_REF:
                columns.append(col_node.value)
        
        # 创建Project算子
        project_plan = ExecutionPlan(
            operator_type=OperatorType.PROJECT,
            columns=columns
        )
        
        # 检查是否有WHERE子句
        if len(node.children) > 1:
            where_node = node.children[1]
            if where_node.node_type == ASTNodeType.WHERE_CLAUSE:
                # 创建SeqScan算子
                seq_scan_plan = ExecutionPlan(
                    operator_type=OperatorType.SEQ_SCAN,
                    table_name=table_name
                )
                
                # 将SeqScan添加到Project
                project_plan.add_child(seq_scan_plan)
                
                # 创建Filter算子
                condition = self._extract_condition(where_node.children[0])
                filter_plan = ExecutionPlan(
                    operator_type=OperatorType.FILTER,
                    condition=condition
                )
                filter_plan.add_child(project_plan)
                
                return filter_plan
        
        # 没有WHERE子句，直接SeqScan + Project
        seq_scan_plan = ExecutionPlan(
            operator_type=OperatorType.SEQ_SCAN,
            table_name=table_name
        )
        project_plan.add_child(seq_scan_plan)
        
        return project_plan
    
    def _generate_delete_plan(self, node: ASTNode) -> ExecutionPlan:
        """生成DELETE执行计划"""
        table_name = node.value
        
        # 检查是否有WHERE子句
        if len(node.children) > 0:
            where_node = node.children[0]
            if where_node.node_type == ASTNodeType.WHERE_CLAUSE:
                # 有WHERE子句，需要Filter + Delete
                condition = self._extract_condition(where_node.children[0])
                
                delete_plan = ExecutionPlan(
                    operator_type=OperatorType.DELETE,
                    table_name=table_name,
                    condition=condition
                )
                
                return delete_plan
        
        # 没有WHERE子句，删除所有记录
        return ExecutionPlan(
            operator_type=OperatorType.DELETE,
            table_name=table_name
        )
    
    def _extract_condition(self, condition_node: ASTNode) -> Dict[str, Any]:
        """从条件节点提取条件信息"""
        if condition_node.node_type == ASTNodeType.COMPARISON:
            operator = condition_node.value
            left = condition_node.children[0]
            right = condition_node.children[1]
            
            # 提取列名和值
            column_name = None
            value = None
            
            if left.node_type == ASTNodeType.COLUMN_REF:
                column_name = left.value
            elif right.node_type == ASTNodeType.COLUMN_REF:
                column_name = right.value
            
            if left.node_type == ASTNodeType.LITERAL:
                value = left.value['value']
            elif right.node_type == ASTNodeType.LITERAL:
                value = right.value['value']
            
            return {
                'column': column_name,
                'operator': operator,
                'value': value
            }
        
        return {}
    
    def _generate_update_plan(self, node: ASTNode) -> ExecutionPlan:
        """生成UPDATE执行计划"""
        table_name = node.value
        set_clause = node.children[0]
        
        # 提取SET子句中的赋值
        assignments = {}
        for assignment in set_clause.children:
            if assignment.node_type == ASTNodeType.ASSIGNMENT:
                column_name = assignment.value
                value_node = assignment.children[0]
                if value_node.node_type == ASTNodeType.LITERAL:
                    assignments[column_name] = value_node.value['value']
        
        # 检查是否有WHERE子句
        if len(node.children) > 1:
            where_node = node.children[1]
            if where_node.node_type == ASTNodeType.WHERE_CLAUSE:
                condition = self._extract_condition(where_node.children[0])
                
                return ExecutionPlan(
                    operator_type=OperatorType.UPDATE,
                    table_name=table_name,
                    columns=list(assignments.keys()),
                    values=list(assignments.values()),
                    condition=condition
                )
        
        # 没有WHERE子句，更新所有记录
        return ExecutionPlan(
            operator_type=OperatorType.UPDATE,
            table_name=table_name,
            columns=list(assignments.keys()),
            values=list(assignments.values())
        )


def print_plan_tree(plan: ExecutionPlan, indent: int = 0):
    """打印执行计划树"""
    print("  " * indent + f"{plan.operator_type.value}")
    if plan.table_name:
        print("  " * (indent + 1) + f"table: {plan.table_name}")
    if plan.columns:
        print("  " * (indent + 1) + f"columns: {plan.columns}")
    if plan.values:
        print("  " * (indent + 1) + f"values: {plan.values}")
    if plan.condition:
        print("  " * (indent + 1) + f"condition: {plan.condition}")
    
    for child in plan.children:
        print_plan_tree(child, indent + 1)


def main():
    """测试执行计划生成器"""
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
    
    # 生成执行计划
    planner = PlanGenerator()
    plans = planner.generate_plan(ast_nodes)
    
    print("执行计划生成结果:")
    for i, plan in enumerate(plans):
        print(f"\n语句 {i + 1} 的执行计划:")
        print_plan_tree(plan)
        print(f"\nS表达式: {plan.to_s_expression()}")


if __name__ == "__main__":
    main()
