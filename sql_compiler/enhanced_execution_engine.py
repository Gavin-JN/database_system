#!/usr/bin/env python3
"""
增强版SQL执行引擎
支持复杂的SQL功能执行
"""

import time
from typing import List, Dict, Any, Optional
from .parser import ASTNode, ASTNodeType
from storage.storage_engine import StorageEngine
from database.catalog import SystemCatalog

class ExecutionResult:
    """执行结果"""
    def __init__(self, success: bool, message: str = "", data: List[Dict] = None, rows_affected: int = 0):
        self.success = success
        self.message = message
        self.data = data or []
        self.rows_affected = rows_affected

class EnhancedExecutionEngine:
    """增强版SQL执行引擎"""
    
    def __init__(self, storage_engine: StorageEngine, catalog: SystemCatalog):
        self.storage_engine = storage_engine
        self.catalog = catalog
    
    def execute(self, plan: ASTNode) -> ExecutionResult:
        """执行查询计划"""
        try:
            if plan.node_type == ASTNodeType.SELECT:
                return self._execute_select(plan)
            elif plan.node_type == ASTNodeType.INSERT:
                return self._execute_insert(plan)
            elif plan.node_type == ASTNodeType.UPDATE:
                return self._execute_update(plan)
            elif plan.node_type == ASTNodeType.DELETE:
                return self._execute_delete(plan)
            elif plan.node_type == ASTNodeType.CREATE_TABLE:
                return self._execute_create_table(plan)
            elif plan.node_type == ASTNodeType.CREATE_INDEX:
                return self._execute_create_index(plan)
            elif plan.node_type == ASTNodeType.DROP_INDEX:
                return self._execute_drop_index(plan)
            else:
                return ExecutionResult(False, f"不支持的语句类型: {plan.node_type}")
        except Exception as e:
            return ExecutionResult(False, f"执行错误: {str(e)}")
    
    def _execute_select(self, plan: ASTNode) -> ExecutionResult:
        """执行SELECT语句"""
        try:
            # 获取FROM子句
            from_clause = plan.value['from_clause']
            table_name = from_clause.value
            
            # 检查表是否存在
            if not self.storage_engine.table_exists(table_name):
                return ExecutionResult(False, f"表 '{table_name}' 不存在")
            
            # 获取所有数据
            data = self.storage_engine.select_records(table_name, {})
            
            # 应用WHERE子句
            if len(plan.children) > 0:
                for child in plan.children:
                    if child.node_type == ASTNodeType.WHERE_CLAUSE:
                        data = self._apply_where_clause(data, child)
                    elif child.node_type == ASTNodeType.GROUP_BY:
                        data = self._apply_group_by(data, child, plan.value['select_list'])
                    elif child.node_type == ASTNodeType.ORDER_BY:
                        data = self._apply_order_by(data, child)
                    elif child.node_type == ASTNodeType.LIMIT:
                        data = self._apply_limit(data, child)
            
            # 应用SELECT列表
            result_data = self._apply_select_list(data, plan.value['select_list'])
            
            return ExecutionResult(True, "查询完成", result_data)
        except Exception as e:
            return ExecutionResult(False, f"SELECT执行错误: {str(e)}")
    
    def _apply_where_clause(self, data: List[Dict], where_clause: ASTNode) -> List[Dict]:
        """应用WHERE子句"""
        filtered_data = []
        # WHERE子句的条件节点存储在value中
        condition_node = where_clause.value
        if condition_node is None:
            return data  # 没有条件，返回所有数据
        
        for row in data:
            if self._evaluate_condition(row, condition_node):
                filtered_data.append(row)
        return filtered_data
    
    def _apply_group_by(self, data: List[Dict], group_by: ASTNode, select_list: ASTNode) -> List[Dict]:
        """应用GROUP BY子句"""
        groups = {}
        
        # 按分组列分组
        for row in data:
            group_key = tuple(self._get_row_value(row, col, None) for col in group_by.value)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)
        
        # 对每个组应用聚合函数
        result = []
        for group_key, group_data in groups.items():
            group_result = {}
            
            # 添加分组列
            for i, col in enumerate(group_by.value):
                group_result[col] = group_key[i]
            
            # 应用聚合函数
            for select_item in select_list.children:
                if select_item.node_type == ASTNodeType.FUNCTION_CALL:
                    func_name = select_item.value['function']
                    if func_name == 'COUNT':
                        if select_item.value['argument'].value == '*':
                            group_result[f"{func_name}(*)"] = len(group_data)
                        else:
                            col_name = select_item.value['argument'].value
                            non_null_count = sum(1 for row in group_data if self._get_row_value(row, col_name) is not None)
                            group_result[f"{func_name}({col_name})"] = non_null_count
                    elif func_name == 'SUM':
                        col_name = select_item.value['argument'].value
                        total = sum(self._convert_to_number(self._get_row_value(row, col_name, 0)) for row in group_data)
                        group_result[f"{func_name}({col_name})"] = total
                    elif func_name == 'AVG':
                        col_name = select_item.value['argument'].value
                        values = [self._convert_to_number(self._get_row_value(row, col_name, 0)) for row in group_data if self._get_row_value(row, col_name) is not None]
                        if values:
                            group_result[f"{func_name}({col_name})"] = sum(values) / len(values)
                        else:
                            group_result[f"{func_name}({col_name})"] = 0
                    elif func_name == 'MAX':
                        col_name = select_item.value['argument'].value
                        values = [self._convert_to_number(self._get_row_value(row, col_name, 0)) for row in group_data if self._get_row_value(row, col_name) is not None]
                        group_result[f"{func_name}({col_name})"] = max(values) if values else 0
                    elif func_name == 'MIN':
                        col_name = select_item.value['argument'].value
                        values = [self._convert_to_number(self._get_row_value(row, col_name, 0)) for row in group_data if self._get_row_value(row, col_name) is not None]
                        group_result[f"{func_name}({col_name})"] = min(values) if values else 0
            
            result.append(group_result)
        
        return result
    
    def _apply_order_by(self, data: List[Dict], order_by: ASTNode) -> List[Dict]:
        """应用ORDER BY子句"""
        def sort_key(row):
            key_values = []
            for order_item in order_by.value:
                col = order_item['column']
                direction = order_item['direction']
                value = self._convert_to_number(self._get_row_value(row, col, 0))
                if direction == 'DESC':
                    value = -value
                key_values.append(value)
            return key_values
        
        return sorted(data, key=sort_key)
    
    def _apply_limit(self, data: List[Dict], limit: ASTNode) -> List[Dict]:
        """应用LIMIT子句"""
        limit_value = limit.value
        return data[:limit_value]
    
    def _apply_select_list(self, data: List[Dict], select_list: ASTNode) -> List[Dict]:
        """应用SELECT列表"""
        if not data:
            return []
        
        # 检查是否有聚合函数
        has_aggregate = any(
            item.node_type == ASTNodeType.FUNCTION_CALL 
            for item in select_list.children
        )
        
        if has_aggregate:
            # 全局聚合函数
            result = {}
            for select_item in select_list.children:
                if select_item.node_type == ASTNodeType.COLUMN_REF and select_item.value == '*':
                    # 处理 *
                    if data:
                        # 将Record对象转换为字典
                        first_row = data[0]
                        if hasattr(first_row, 'data'):
                            result.update(first_row.data)
                        else:
                            result.update(first_row)
                elif select_item.node_type == ASTNodeType.FUNCTION_CALL:
                    # 处理别名情况
                    if isinstance(select_item.value, dict) and 'expression' in select_item.value:
                        # 有别名的情况
                        func_data = select_item.value['expression']
                        alias = select_item.value['alias']
                    else:
                        # 没有别名的情况
                        func_data = select_item.value
                        alias = None
                    
                    func_name = func_data['function']
                    if func_name == 'COUNT':
                        if func_data['argument'] and func_data['argument'].value == '*':
                            key = alias if alias else f"{func_name}(*)"
                            result[key] = len(data)
                        else:
                            col_name = func_data['argument'].value if func_data['argument'] else '*'
                            non_null_count = sum(1 for row in data if self._get_row_value(row, col_name) is not None)
                            key = alias if alias else f"{func_name}({col_name})"
                            result[key] = non_null_count
                    elif func_name == 'SUM':
                        col_name = func_data['argument'].value if func_data['argument'] else '*'
                        total = sum(self._convert_to_number(self._get_row_value(row, col_name, 0)) for row in data)
                        key = alias if alias else f"{func_name}({col_name})"
                        result[key] = total
                    elif func_name == 'AVG':
                        col_name = func_data['argument'].value if func_data['argument'] else '*'
                        values = [self._convert_to_number(self._get_row_value(row, col_name, 0)) for row in data if self._get_row_value(row, col_name) is not None]
                        if values:
                            avg_value = sum(values) / len(values)
                        else:
                            avg_value = 0
                        key = alias if alias else f"{func_name}({col_name})"
                        result[key] = avg_value
                    elif func_name == 'MAX':
                        col_name = func_data['argument'].value if func_data['argument'] else '*'
                        values = [self._convert_to_number(self._get_row_value(row, col_name, 0)) for row in data if self._get_row_value(row, col_name) is not None]
                        key = alias if alias else f"{func_name}({col_name})"
                        result[key] = max(values) if values else 0
                    elif func_name == 'MIN':
                        col_name = func_data['argument'].value if func_data['argument'] else '*'
                        values = [self._convert_to_number(self._get_row_value(row, col_name, 0)) for row in data if self._get_row_value(row, col_name) is not None]
                        key = alias if alias else f"{func_name}({col_name})"
                        result[key] = min(values) if values else 0
            return [result]
        else:
            # 普通SELECT
            result = []
            for row in data:
                new_row = {}
                for select_item in select_list.children:
                    if select_item.node_type == ASTNodeType.COLUMN_REF:
                        if select_item.value == '*':
                            # 将Record对象转换为字典
                            if hasattr(row, 'data'):
                                new_row.update(row.data)
                            else:
                                new_row.update(row)
                        else:
                            col_name = select_item.value
                            new_row[col_name] = self._get_row_value(row, col_name)
                    elif select_item.node_type == ASTNodeType.FUNCTION_CALL:
                        # 这里应该处理非聚合函数，暂时跳过
                        pass
                result.append(new_row)
            return result
    
    def _get_row_value(self, row, key, default=None):
        """获取行中的值，处理Record对象和字典"""
        if hasattr(row, 'data'):
            return row.data.get(key, default)
        elif hasattr(row, 'get_value'):
            return row.get_value(key)
        elif hasattr(row, 'get'):
            return row.get(key, default)
        else:
            return getattr(row, key, default)
    
    def _evaluate_condition(self, row, condition: ASTNode) -> bool:
        """评估条件表达式"""
        if condition.node_type == ASTNodeType.COMPARISON:
            left_value = self._evaluate_expression(row, condition.value['left'])
            right_value = self._evaluate_expression(row, condition.value['right'])
            operator = condition.value['operator']
            
            # 类型转换
            left_value = self._convert_to_number(left_value)
            right_value = self._convert_to_number(right_value)
            
            if operator == '=':
                return left_value == right_value
            elif operator == '!=':
                return left_value != right_value
            elif operator == '>':
                return left_value > right_value
            elif operator == '<':
                return left_value < right_value
            elif operator == '>=':
                return left_value >= right_value
            elif operator == '<=':
                return left_value <= right_value
        
        elif condition.node_type == ASTNodeType.LOGICAL_OP:
            left_result = self._evaluate_condition(row, condition.value['left'])
            right_result = self._evaluate_condition(row, condition.value['right'])
            operator = condition.value['operator']
            
            if operator == 'AND':
                return left_result and right_result
            elif operator == 'OR':
                return left_result or right_result
        
        return False
    
    def _evaluate_expression(self, row: Dict, expr: ASTNode) -> Any:
        """评估表达式"""
        if expr.node_type == ASTNodeType.LITERAL:
            return expr.value['value']
        elif expr.node_type == ASTNodeType.COLUMN_REF:
            return self._get_row_value(row, expr.value)
        else:
            return None
    
    def _extract_condition_from_ast(self, condition: ASTNode) -> Dict[str, Any]:
        """从AST条件节点提取条件字典"""
        if condition.node_type == ASTNodeType.COMPARISON:
            left = condition.value['left']
            right = condition.value['right']
            operator = condition.value['operator']
            
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
    
    def _convert_to_number(self, value: Any) -> Any:
        """尝试将值转换为数字"""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return value
        
        if isinstance(value, str):
            try:
                if '.' not in value:
                    return int(value)
                else:
                    return float(value)
            except ValueError:
                return value
        
        return value
    
    def _execute_insert(self, plan: ASTNode) -> ExecutionResult:
        """执行INSERT语句"""
        table_name = plan.value['table_name']
        columns = plan.value['columns']
        values = plan.value['values']
        
        # 检查表是否存在
        if not self.storage_engine.table_exists(table_name):
            return ExecutionResult(False, f"表 '{table_name}' 不存在")
        
        # 构建记录
        record = {}
        for i, col in enumerate(columns):
            if i < len(values):
                record[col] = values[i]
        
        # 插入记录
        success = self.storage_engine.insert_record(table_name, record)
        if success:
            return ExecutionResult(True, "插入成功")
        else:
            return ExecutionResult(False, "插入失败")
    
    def _execute_update(self, plan: ASTNode) -> ExecutionResult:
        """执行UPDATE语句"""
        table_name = plan.value['table_name']
        updates = plan.value['updates']
        where_clause = plan.value['where_clause']
        
        # 检查表是否存在
        if not self.storage_engine.table_exists(table_name):
            return ExecutionResult(False, f"表 '{table_name}' 不存在")
        
        # 构建更新数据字典
        update_data = {}
        for update in updates:
            update_data[update['column']] = update['value']
        
        # 构建WHERE条件
        condition = None
        if where_clause:
            condition = self._extract_condition_from_ast(where_clause)
        
        # 调用存储引擎的更新方法
        updated_count = self.storage_engine.update_records(table_name, update_data, condition)
        
        # 刷新到磁盘
        self.storage_engine.flush_all()
        
        return ExecutionResult(True, f"更新了 {updated_count} 条记录")
    
    def _execute_delete(self, plan: ASTNode) -> ExecutionResult:
        """执行DELETE语句"""
        table_name = plan.value['table_name']
        where_clause = plan.value['where_clause']
        
        # 检查表是否存在
        if not self.storage_engine.table_exists(table_name):
            return ExecutionResult(False, f"表 '{table_name}' 不存在")
        
        # 构建WHERE条件
        condition = None
        if where_clause:
            condition = self._extract_condition_from_ast(where_clause)
        
        # 调用存储引擎的删除方法
        deleted_count = self.storage_engine.delete_records(table_name, condition)
        
        # 刷新到磁盘
        self.storage_engine.flush_all()
        
        return ExecutionResult(True, f"删除了 {deleted_count} 条记录")
    
    def _execute_create_table(self, plan: ASTNode) -> ExecutionResult:
        """执行CREATE TABLE语句"""
        table_name = plan.value['table_name']
        columns = plan.value['columns']
        
        # 检查表是否已存在
        if self.storage_engine.table_exists(table_name):
            return ExecutionResult(False, f"表 '{table_name}' 已存在")
        
        # 创建表
        from storage.storage_engine import ColumnInfo, DataType
        
        column_infos = []
        for col in columns:
            col_type = DataType.VARCHAR if col['type'].upper() == 'VARCHAR' else DataType.INT
            column_infos.append(ColumnInfo(col['name'], col_type))
        
        success = self.storage_engine.create_table(table_name, column_infos)
        if success:
            # 更新系统目录
            self.catalog.add_table(table_name, column_infos)
            return ExecutionResult(True, f"表 '{table_name}' 创建成功")
        else:
            return ExecutionResult(False, f"表 '{table_name}' 创建失败")
    
    def _execute_create_index(self, plan: ASTNode) -> ExecutionResult:
        """执行CREATE INDEX"""
        index_info = plan.value
        index_name = index_info['index_name']
        table_name = index_info['table_name']
        column_name = index_info['column_name']
        is_unique = index_info['unique']
        
        # 检查表是否存在
        if not self.storage_engine.table_exists(table_name):
            return ExecutionResult(False, f"表 '{table_name}' 不存在")
        
        # 创建实际索引
        from storage.index import IndexType
        success = self.storage_engine.create_index(table_name, column_name, IndexType.BPLUS_TREE)
        
        if success:
            # 将索引信息存储到系统目录
            index_data = {
                'index_name': index_name,
                'table_name': table_name,
                'column_name': column_name,
                'unique': is_unique,
                'created_at': time.time()
            }
            
            # 存储索引信息
            self.storage_engine.insert_record('pg_indexes', index_data)
            return ExecutionResult(True, f"索引 '{index_name}' 创建成功")
        else:
            return ExecutionResult(False, f"索引 '{index_name}' 创建失败")
    
    def _execute_drop_index(self, plan: ASTNode) -> ExecutionResult:
        """执行DROP INDEX"""
        index_name = plan.value
        
        # 首先从系统目录获取索引信息
        condition = {'column': 'index_name', 'operator': '=', 'value': index_name}
        index_records = self.storage_engine.select_records('pg_indexes', condition)
        
        if not index_records:
            return ExecutionResult(False, f"索引 '{index_name}' 不存在")
        
        # 获取索引信息
        index_record = index_records[0]
        table_name = index_record.get_value('table_name')
        column_name = index_record.get_value('column_name')
        
        # 删除实际索引
        success = self.storage_engine.drop_index(table_name, column_name)
        
        if success:
            # 从系统目录删除索引信息
            deleted_count = self.storage_engine.delete_records('pg_indexes', condition)
            if deleted_count > 0:
                return ExecutionResult(True, f"索引 '{index_name}' 删除成功")
            else:
                return ExecutionResult(False, f"索引 '{index_name}' 删除失败")
        else:
            return ExecutionResult(False, f"索引 '{index_name}' 删除失败")
