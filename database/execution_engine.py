"""
执行引擎
实现各种执行算子：CreateTable、Insert、SeqScan、Filter、Project等
"""
from typing import List, Dict, Any, Optional, Iterator
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod
from sql_compiler.planner import ExecutionPlan, OperatorType
from storage.storage_engine import StorageEngine, ColumnInfo, DataType, Record


class ExecutionResult:
    """执行结果"""
    
    def __init__(self, success: bool = True, message: str = "", data: List[Dict] = None):
        self.success = success
        self.message = message
        self.data = data or []
        self.rows_affected = 0
    
    def add_row(self, row: Dict[str, Any]):
        """添加一行数据"""
        self.data.append(row)
    
    def set_rows_affected(self, count: int):
        """设置影响的行数"""
        self.rows_affected = count


class ExecutionOperator(ABC):
    """执行算子基类"""
    
    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> ExecutionResult:
        """执行算子"""
        pass


class CreateTableOperator(ExecutionOperator):
    """创建表算子"""
    
    def __init__(self, table_name: str, columns: List[Dict[str, str]]):
        self.table_name = table_name
        self.columns = columns
    
    def execute(self, context: Dict[str, Any]) -> ExecutionResult:
        """执行创建表操作"""
        storage_engine = context.get('storage_engine')
        if not storage_engine:
            return ExecutionResult(False, "存储引擎未初始化")
        
        # 检查表是否已存在
        if storage_engine.table_exists(self.table_name):
            return ExecutionResult(False, f"表 '{self.table_name}' 已存在")
        
        # 转换列定义
        column_infos = []
        for col_def in self.columns:
            column_infos.append(ColumnInfo(
                name=col_def['name'],
                data_type=DataType(col_def['type'])
            ))
        
        # 创建表
        success = storage_engine.create_table(self.table_name, column_infos)
        if success:
            return ExecutionResult(True, f"表 '{self.table_name}' 创建成功")
        else:
            return ExecutionResult(False, f"创建表 '{self.table_name}' 失败")


class InsertOperator(ExecutionOperator):
    """插入算子"""
    
    def __init__(self, table_name: str, columns: List[str], values: List[Any]):
        self.table_name = table_name
        self.columns = columns
        self.values = values
    
    def execute(self, context: Dict[str, Any]) -> ExecutionResult:
        """执行插入操作"""
        storage_engine = context.get('storage_engine')
        if not storage_engine:
            return ExecutionResult(False, "存储引擎未初始化")
        
        # 检查表是否存在
        if not storage_engine.table_exists(self.table_name):
            return ExecutionResult(False, f"表 '{self.table_name}' 不存在")
        
        # 构建记录数据
        record_data = {}
        for col_name, value in zip(self.columns, self.values):
            record_data[col_name] = value
        
        # 插入记录
        success = storage_engine.insert_record(self.table_name, record_data)
        if success:
            return ExecutionResult(True, f"成功插入1条记录到表 '{self.table_name}'")
        else:
            return ExecutionResult(False, f"插入记录到表 '{self.table_name}' 失败")


class SeqScanOperator(ExecutionOperator):
    """顺序扫描算子"""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
    
    def execute(self, context: Dict[str, Any]) -> ExecutionResult:
        """执行顺序扫描操作"""
        storage_engine = context.get('storage_engine')
        if not storage_engine:
            return ExecutionResult(False, "存储引擎未初始化")
        
        # 检查表是否存在
        if not storage_engine.table_exists(self.table_name):
            return ExecutionResult(False, f"表 '{self.table_name}' 不存在")
        
        # 获取所有记录
        records = storage_engine.select_records(self.table_name)
        
        result = ExecutionResult(True, f"扫描表 '{self.table_name}' 完成")
        for record in records:
            if not record.is_deleted:
                result.add_row(record.data)
        
        return result


class FilterOperator(ExecutionOperator):
    """过滤算子"""
    
    def __init__(self, condition: Dict[str, Any]):
        self.condition = condition
    
    def execute(self, context: Dict[str, Any]) -> ExecutionResult:
        """执行过滤操作"""
        # 获取输入数据
        input_data = context.get('input_data', [])
        
        result = ExecutionResult(True, "过滤操作完成")
        
        for row in input_data:
            if self._matches_condition(row, self.condition):
                result.add_row(row)
        
        return result
    
    def _matches_condition(self, row: Dict[str, Any], condition: Dict[str, Any]) -> bool:
        """检查行是否满足条件"""
        if not condition:
            return True
        
        column = condition.get('column')
        operator = condition.get('operator')
        value = condition.get('value')
        
        if not all([column, operator, value is not None]):
            return False
        
        row_value = row.get(column)
        if row_value is None:
            return False
        
        # 确保类型匹配
        try:
            if isinstance(value, str) and isinstance(row_value, (int, float)):
                # 如果比较值是字符串，但行值是数字，尝试转换
                if value.isdigit():
                    value = int(value)
                else:
                    return False
            elif isinstance(value, (int, float)) and isinstance(row_value, str):
                # 如果比较值是数字，但行值是字符串，尝试转换
                if row_value.isdigit():
                    row_value = int(row_value)
                else:
                    return False
        except (ValueError, AttributeError):
            return False
        
        if operator == '=':
            return row_value == value
        elif operator == '>':
            return row_value > value
        elif operator == '<':
            return row_value < value
        elif operator == '>=':
            return row_value >= value
        elif operator == '<=':
            return row_value <= value
        elif operator == '!=':
            return row_value != value
        
        return False


class ProjectOperator(ExecutionOperator):
    """投影算子"""
    
    def __init__(self, columns: List[str]):
        self.columns = columns
    
    def execute(self, context: Dict[str, Any]) -> ExecutionResult:
        """执行投影操作"""
        # 获取输入数据
        input_data = context.get('input_data', [])
        
        result = ExecutionResult(True, "投影操作完成")
        
        for row in input_data:
            projected_row = {}
            # 处理 * 通配符
            if "*" in self.columns:
                # 如果包含 *，返回所有列
                projected_row = row.copy()
            else:
                # 否则只返回指定的列
                for col in self.columns:
                    if col in row:
                        projected_row[col] = row[col]
                    else:
                        projected_row[col] = None
            result.add_row(projected_row)
        
        return result


class DeleteOperator(ExecutionOperator):
    """删除算子"""
    
    def __init__(self, table_name: str, condition: Optional[Dict[str, Any]] = None):
        self.table_name = table_name
        self.condition = condition
    
    def execute(self, context: Dict[str, Any]) -> ExecutionResult:
        """执行删除操作"""
        storage_engine = context.get('storage_engine')
        if not storage_engine:
            return ExecutionResult(False, "存储引擎未初始化")
        
        # 检查表是否存在
        if not storage_engine.table_exists(self.table_name):
            return ExecutionResult(False, f"表 '{self.table_name}' 不存在")
        
        # 删除记录
        deleted_count = storage_engine.delete_records(self.table_name, self.condition)
        
        if self.condition:
            return ExecutionResult(True, f"从表 '{self.table_name}' 删除了 {deleted_count} 条记录")
        else:
            return ExecutionResult(True, f"清空了表 '{self.table_name}' 的所有记录")


class UpdateOperator(ExecutionOperator):
    """更新算子"""
    
    def __init__(self, table_name: str, columns: List[str], values: List[Any], 
                 condition: Optional[Dict[str, Any]] = None):
        self.table_name = table_name
        self.columns = columns
        self.values = values
        self.condition = condition
    
    def execute(self, context: Dict[str, Any]) -> ExecutionResult:
        """执行更新操作"""
        storage_engine = context.get('storage_engine')
        if not storage_engine:
            return ExecutionResult(False, "存储引擎未初始化")
        
        # 检查表是否存在
        if not storage_engine.table_exists(self.table_name):
            return ExecutionResult(False, f"表 '{self.table_name}' 不存在")
        
        # 构建更新数据
        update_data = dict(zip(self.columns, self.values))
        
        # 更新记录
        updated_count = storage_engine.update_records(self.table_name, update_data, self.condition)
        
        if self.condition:
            return ExecutionResult(True, f"更新了表 '{self.table_name}' 中的 {updated_count} 条记录")
        else:
            return ExecutionResult(True, f"更新了表 '{self.table_name}' 中的所有 {updated_count} 条记录")


class ExecutionEngine:
    """执行引擎"""
    
    def __init__(self, storage_engine: StorageEngine):
        self.storage_engine = storage_engine
        self.operator_factory = {
            OperatorType.CREATE_TABLE: self._create_create_table_operator,
            OperatorType.INSERT: self._create_insert_operator,
            OperatorType.SEQ_SCAN: self._create_seq_scan_operator,
            OperatorType.FILTER: self._create_filter_operator,
            OperatorType.PROJECT: self._create_project_operator,
            OperatorType.DELETE: self._create_delete_operator,
            OperatorType.UPDATE: self._create_update_operator,
        }
    
    def execute_plan(self, plan: ExecutionPlan) -> ExecutionResult:
        """执行执行计划"""
        try:
            return self._execute_plan_recursive(plan, {})
        except Exception as e:
            return ExecutionResult(False, f"执行失败: {str(e)}")
    
    def _execute_plan_recursive(self, plan: ExecutionPlan, context: Dict[str, Any]) -> ExecutionResult:
        """递归执行执行计划"""
        # 添加存储引擎到上下文
        context['storage_engine'] = self.storage_engine
        
        # 创建算子
        operator = self._create_operator(plan)
        if not operator:
            return ExecutionResult(False, f"未知的算子类型: {plan.operator_type}")
        
        # 如果有子计划，先执行子计划
        if plan.children and len(plan.children) > 0:
            child_results = []
            for child_plan in plan.children:
                child_result = self._execute_plan_recursive(child_plan, context)
                if not child_result.success:
                    return child_result
                child_results.append(child_result)
            
            # 将子计划的结果合并到上下文中
            if len(child_results) == 1:
                context['input_data'] = child_results[0].data
            else:
                # 多个子计划的情况，这里简化处理
                context['input_data'] = child_results[0].data
        
        # 执行当前算子
        result = operator.execute(context)
        return result
    
    def _create_operator(self, plan: ExecutionPlan) -> Optional[ExecutionOperator]:
        """创建算子"""
        factory = self.operator_factory.get(plan.operator_type)
        if factory:
            return factory(plan)
        return None
    
    def _create_create_table_operator(self, plan: ExecutionPlan) -> CreateTableOperator:
        """创建CreateTable算子"""
        return CreateTableOperator(plan.table_name, plan.columns)
    
    def _create_insert_operator(self, plan: ExecutionPlan) -> InsertOperator:
        """创建Insert算子"""
        return InsertOperator(plan.table_name, plan.columns, plan.values)
    
    def _create_seq_scan_operator(self, plan: ExecutionPlan) -> SeqScanOperator:
        """创建SeqScan算子"""
        return SeqScanOperator(plan.table_name)
    
    def _create_filter_operator(self, plan: ExecutionPlan) -> FilterOperator:
        """创建Filter算子"""
        return FilterOperator(plan.condition)
    
    def _create_project_operator(self, plan: ExecutionPlan) -> ProjectOperator:
        """创建Project算子"""
        return ProjectOperator(plan.columns)
    
    def _create_delete_operator(self, plan: ExecutionPlan) -> DeleteOperator:
        """创建Delete算子"""
        return DeleteOperator(plan.table_name, plan.condition)
    
    def _create_update_operator(self, plan: ExecutionPlan) -> UpdateOperator:
        """创建Update算子"""
        return UpdateOperator(plan.table_name, plan.columns, plan.values, plan.condition)


def main():
    """测试执行引擎"""
    from ..storage.storage_engine import StorageEngine
    
    # 创建存储引擎和执行引擎
    storage_engine = StorageEngine("test_execution.dat")
    execution_engine = ExecutionEngine(storage_engine)
    
    print("执行引擎测试:")
    
    # 测试创建表
    from ..sql_compiler.planner import ExecutionPlan, OperatorType
    
    create_plan = ExecutionPlan(
        operator_type=OperatorType.CREATE_TABLE,
        table_name="student",
        columns=[
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "age", "type": "INT"}
        ]
    )
    
    result = execution_engine.execute_plan(create_plan)
    print(f"创建表: {result.message}")
    
    # 测试插入记录
    insert_plan = ExecutionPlan(
        operator_type=OperatorType.INSERT,
        table_name="student",
        columns=["id", "name", "age"],
        values=[1, "Alice", 20]
    )
    
    result = execution_engine.execute_plan(insert_plan)
    print(f"插入记录: {result.message}")
    
    # 测试查询
    seq_scan_plan = ExecutionPlan(
        operator_type=OperatorType.SEQ_SCAN,
        table_name="student"
    )
    
    project_plan = ExecutionPlan(
        operator_type=OperatorType.PROJECT,
        columns=["id", "name"]
    )
    project_plan.add_child(seq_scan_plan)
    
    result = execution_engine.execute_plan(project_plan)
    print(f"查询结果: {result.message}")
    for row in result.data:
        print(f"  {row}")
    
    # 刷新数据
    storage_engine.flush_all()


if __name__ == "__main__":
    main()
