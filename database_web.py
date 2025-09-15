#!/usr/bin/env python3
"""
数据库Web IDE - 基于Web的数据库编程接口
使用Flask提供Web界面
"""
from flask import Flask, render_template, request, jsonify
import json
import os
from database.database import Database
import threading
import time


app = Flask(__name__)
app.secret_key = 'database_web_ide_secret_key'

# 全局数据库实例
db_instance = None
db_lock = threading.Lock()


def get_database():
    """获取数据库实例"""
    global db_instance
    with db_lock:
        if db_instance is None:
            db_instance = Database("web_database.db")
        return db_instance


@app.route('/')
def index():
    """主页"""
    return render_template('database_ide.html')


@app.route('/api/execute', methods=['POST'])
def execute_sql():
    """执行SQL API"""
    try:
        data = request.get_json()
        sql = data.get('sql', '').strip()
        
        if not sql:
            return jsonify({
                'success': False,
                'message': 'SQL语句不能为空',
                'data': [],
                'errors': []
            })
        
        # 获取数据库实例
        db = get_database()
        
        # 执行SQL
        result = db.execute_sql(sql)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'执行错误: {str(e)}',
            'data': [],
            'errors': []
        })


@app.route('/api/tables', methods=['GET'])
def get_tables():
    """获取表列表API"""
    try:
        db = get_database()
        tables = db.get_tables()
        return jsonify({
            'success': True,
            'tables': tables
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'tables': []
        })


@app.route('/api/table_info/<table_name>', methods=['GET'])
def get_table_info(table_name):
    """获取表信息API"""
    try:
        db = get_database()
        table_info = db.get_table_info(table_name)
        
        if table_info:
            return jsonify({
                'success': True,
                'table_info': table_info
            })
        else:
            return jsonify({
                'success': False,
                'message': f'表 "{table_name}" 不存在'
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })




@app.route('/api/database_info', methods=['GET'])
def get_database_info():
    """获取数据库信息API"""
    try:
        db = get_database()
        info = db.get_database_info()
        return jsonify({
            'success': True,
            'info': info
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })


@app.route('/api/save_file', methods=['POST'])
def save_file():
    """保存文件API"""
    try:
        data = request.get_json()
        filename = data.get('filename', '')
        content = data.get('content', '')
        
        if not filename:
            return jsonify({
                'success': False,
                'message': '文件名不能为空'
            })
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return jsonify({
            'success': True,
            'message': f'文件已保存: {filename}'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'保存文件失败: {str(e)}'
        })


@app.route('/api/load_file', methods=['POST'])
def load_file():
    """加载文件API"""
    try:
        data = request.get_json()
        filename = data.get('filename', '')
        
        if not filename:
            return jsonify({
                'success': False,
                'message': '文件名不能为空'
            })
        
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return jsonify({
            'success': True,
            'content': content
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'加载文件失败: {str(e)}'
        })


if __name__ == '__main__':
    # 创建模板目录
    os.makedirs('templates', exist_ok=True)
    
    # 创建HTML模板
    html_template = '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>数据库Web IDE</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Consolas', 'Monaco', monospace;
            background-color: #1e1e1e;
            color: #d4d4d4;
            height: 100vh;
            overflow: hidden;
        }
        
        .container {
            display: flex;
            height: 100vh;
        }
        
        .sidebar {
            width: 300px;
            background-color: #252526;
            border-right: 1px solid #3e3e42;
            padding: 20px;
            overflow-y: auto;
        }
        
        .main-content {
            flex: 1;
            display: flex;
            flex-direction: column;
        }
        
        .toolbar {
            background-color: #2d2d30;
            padding: 10px 20px;
            border-bottom: 1px solid #3e3e42;
            display: flex;
            gap: 10px;
            align-items: center;
        }
        
        .editor-container {
            flex: 1;
            display: flex;
            flex-direction: column;
        }
        
        .editor {
            flex: 1;
            background-color: #1e1e1e;
            color: #d4d4d4;
            border: none;
            padding: 20px;
            font-family: 'Consolas', 'Monaco', monospace;
            font-size: 14px;
            line-height: 1.5;
            resize: none;
            outline: none;
        }
        
        .output-container {
            height: 300px;
            background-color: #1e1e1e;
            border-top: 1px solid #3e3e42;
            display: flex;
            flex-direction: column;
        }
        
        .output-tabs {
            display: flex;
            background-color: #2d2d30;
            border-bottom: 1px solid #3e3e42;
        }
        
        .output-tab {
            padding: 10px 20px;
            cursor: pointer;
            border-right: 1px solid #3e3e42;
            background-color: #2d2d30;
        }
        
        .output-tab.active {
            background-color: #1e1e1e;
        }
        
        .output-content {
            flex: 1;
            padding: 20px;
            overflow-y: auto;
            font-family: 'Consolas', 'Monaco', monospace;
            font-size: 12px;
        }
        
        .result-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }
        
        .result-table th,
        .result-table td {
            border: 1px solid #3e3e42;
            padding: 8px;
            text-align: left;
        }
        
        .result-table th {
            background-color: #2d2d30;
        }
        
        .error {
            color: #f48771;
        }
        
        .success {
            color: #4ec9b0;
        }
        
        .button {
            background-color: #0e639c;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 3px;
            cursor: pointer;
            font-size: 12px;
        }
        
        .button:hover {
            background-color: #1177bb;
        }
        
        .button:disabled {
            background-color: #3e3e42;
            cursor: not-allowed;
        }
        
        .button.secondary {
            background-color: #3e3e42;
        }
        
        .button.secondary:hover {
            background-color: #4e4e52;
        }
        
        .status-bar {
            background-color: #007acc;
            color: white;
            padding: 5px 20px;
            font-size: 12px;
            display: flex;
            justify-content: space-between;
        }
        
        .table-list {
            list-style: none;
            margin-top: 10px;
        }
        
        .table-item {
            padding: 5px 10px;
            cursor: pointer;
            border-radius: 3px;
            margin-bottom: 2px;
        }
        
        .table-item:hover {
            background-color: #3e3e42;
        }
        
        .hidden {
            display: none;
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- 侧边栏 -->
        <div class="sidebar">
            <h3>数据库管理</h3>
            <div style="margin-top: 20px;">
                <button class="button" onclick="refreshTables()">刷新表列表</button>
                <button class="button secondary" onclick="showDatabaseInfo()">数据库信息</button>
            </div>
            
            <h4 style="margin-top: 30px;">表列表</h4>
            <ul id="tableList" class="table-list">
                <li>加载中...</li>
            </ul>
            
            <h4 style="margin-top: 30px;">文件操作</h4>
            <div>
                <button class="button secondary" onclick="saveFile()">保存文件</button>
                <button class="button secondary" onclick="loadFile()">加载文件</button>
            </div>
        </div>
        
        <!-- 主内容区 -->
        <div class="main-content">
            <!-- 工具栏 -->
            <div class="toolbar">
                <button class="button" onclick="executeSQL()" id="executeBtn">执行 (F5)</button>
                <button class="button secondary" onclick="clearEditor()">清空</button>
                <button class="button secondary" onclick="clearOutput()">清空输出</button>
                <select id="execMode" style="margin-left: 20px;">
                    <option value="all">执行全部</option>
                    <option value="selected">执行选中</option>
                </select>
            </div>
            
            <!-- 编辑器 -->
            <div class="editor-container">
                <textarea id="sqlEditor" class="editor" placeholder="在这里输入SQL语句...&#10;&#10;示例:&#10;CREATE TABLE students(id INT, name VARCHAR, age INT);&#10;INSERT INTO students VALUES (1, 'Alice', 20);&#10;SELECT * FROM students;"></textarea>
            </div>
            
            <!-- 输出区域 -->
            <div class="output-container">
                <div class="output-tabs">
                    <div class="output-tab active" onclick="switchTab('result')">查询结果</div>
                    <div class="output-tab" onclick="switchTab('log')">执行日志</div>
                    <div class="output-tab" onclick="switchTab('error')">错误信息</div>
                </div>
                <div id="resultContent" class="output-content">
                    <div>等待执行SQL语句...</div>
                </div>
                <div id="logContent" class="output-content hidden">
                    <div>执行日志将显示在这里...</div>
                </div>
                <div id="errorContent" class="output-content hidden">
                    <div>错误信息将显示在这里...</div>
                </div>
            </div>
        </div>
    </div>
    
    <!-- 状态栏 -->
    <div class="status-bar">
        <span id="statusText">就绪</span>
        <span id="dbStatus">数据库: 已连接</span>
    </div>
    
    <script>
        let currentTab = 'result';
        
        // 初始化
        document.addEventListener('DOMContentLoaded', function() {
            refreshTables();
            setupKeyboardShortcuts();
        });
        
        // 键盘快捷键
        function setupKeyboardShortcuts() {
            document.addEventListener('keydown', function(e) {
                if (e.key === 'F5' || (e.ctrlKey && e.key === 'Enter')) {
                    e.preventDefault();
                    executeSQL();
                }
            });
        }
        
        // 切换标签页
        function switchTab(tabName) {
            // 隐藏所有内容
            document.querySelectorAll('.output-content').forEach(content => {
                content.classList.add('hidden');
            });
            
            // 移除所有标签的active类
            document.querySelectorAll('.output-tab').forEach(tab => {
                tab.classList.remove('active');
            });
            
            // 显示选中的内容
            document.getElementById(tabName + 'Content').classList.remove('hidden');
            document.querySelector(`[onclick="switchTab('${tabName}')"]`).classList.add('active');
            
            currentTab = tabName;
        }
        
        // 执行SQL
        async function executeSQL() {
            const editor = document.getElementById('sqlEditor');
            const execMode = document.getElementById('execMode').value;
            const executeBtn = document.getElementById('executeBtn');
            
            let sql;
            if (execMode === 'selected') {
                const start = editor.selectionStart;
                const end = editor.selectionEnd;
                sql = editor.value.substring(start, end);
                
                if (!sql.trim()) {
                    alert('请先选中要执行的代码');
                    return;
                }
            } else {
                sql = editor.value;
            }
            
            if (!sql.trim()) {
                alert('请输入SQL语句');
                return;
            }
            
            executeBtn.disabled = true;
            executeBtn.textContent = '执行中...';
            updateStatus('正在执行...');
            
            try {
                const response = await fetch('/api/execute', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ sql: sql })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showResult(result.data);
                    logMessage(`执行成功: ${result.message}`);
                    
                    if (result.rows_affected > 0) {
                        logMessage(`影响行数: ${result.rows_affected}`);
                    }
                } else {
                    showError(result.message);
                    if (result.errors) {
                        result.errors.forEach(error => showError(error));
                    }
                }
                
            } catch (error) {
                showError(`执行错误: ${error.message}`);
            } finally {
                executeBtn.disabled = false;
                executeBtn.textContent = '执行 (F5)';
                updateStatus('执行完成');
            }
        }
        
        // 显示查询结果
        function showResult(data) {
            const content = document.getElementById('resultContent');
            
            if (!data || data.length === 0) {
                content.innerHTML = '<div>查询结果为空</div>';
            } else {
                let html = `<div>查询结果 (${data.length} 条记录):</div>`;
                html += '<table class="result-table">';
                
                // 表头
                if (data.length > 0) {
                    html += '<tr>';
                    Object.keys(data[0]).forEach(key => {
                        html += `<th>${key}</th>`;
                    });
                    html += '</tr>';
                }
                
                // 数据行
                data.forEach((row, index) => {
                    html += '<tr>';
                    Object.values(row).forEach(value => {
                        html += `<td>${value || ''}</td>`;
                    });
                    html += '</tr>';
                });
                
                html += '</table>';
                content.innerHTML = html;
            }
            
            switchTab('result');
        }
        
        // 显示错误信息
        function showError(message) {
            const content = document.getElementById('errorContent');
            const timestamp = new Date().toLocaleTimeString();
            content.innerHTML += `<div class="error">[${timestamp}] ${message}</div>`;
            switchTab('error');
        }
        
        // 记录日志
        function logMessage(message) {
            const content = document.getElementById('logContent');
            const timestamp = new Date().toLocaleTimeString();
            content.innerHTML += `<div class="success">[${timestamp}] ${message}</div>`;
        }
        
        // 清空编辑器
        function clearEditor() {
            document.getElementById('sqlEditor').value = '';
        }
        
        // 清空输出
        function clearOutput() {
            document.getElementById('resultContent').innerHTML = '<div>等待执行SQL语句...</div>';
            document.getElementById('logContent').innerHTML = '<div>执行日志将显示在这里...</div>';
            document.getElementById('errorContent').innerHTML = '<div>错误信息将显示在这里...</div>';
        }
        
        // 刷新表列表
        async function refreshTables() {
            try {
                const response = await fetch('/api/tables');
                const result = await response.json();
                
                const tableList = document.getElementById('tableList');
                
                if (result.success && result.tables.length > 0) {
                    tableList.innerHTML = result.tables.map(table => 
                        `<li class="table-item" onclick="showTableInfo('${table}')">${table}</li>`
                    ).join('');
                } else {
                    tableList.innerHTML = '<li>没有表</li>';
                }
            } catch (error) {
                console.error('刷新表列表失败:', error);
            }
        }
        
        // 显示表信息
        async function showTableInfo(tableName) {
            try {
                const response = await fetch(`/api/table_info/${tableName}`);
                const result = await response.json();
                
                if (result.success) {
                    const info = result.table_info;
                    let message = `表: ${info.name}\\n`;
                    message += `创建时间: ${info.created_at}\\n`;
                    message += `页数: ${info.page_count}\\n`;
                    message += '列信息:\\n';
                    info.columns.forEach(col => {
                        message += `  ${col.name}: ${col.type}\\n`;
                    });
                    alert(message);
                } else {
                    alert(result.message);
                }
            } catch (error) {
                alert(`获取表信息失败: ${error.message}`);
            }
        }
        
        // 显示数据库信息
        async function showDatabaseInfo() {
            try {
                const response = await fetch('/api/database_info');
                const result = await response.json();
                
                if (result.success) {
                    const info = result.info;
                    let message = `数据库文件: ${info.database_file}\\n`;
                    message += `表数量: ${info.tables}\\n`;
                    message += `缓存命中率: ${(info.storage.cache.hit_rate * 100).toFixed(2)}%\\n`;
                    message += `总页数: ${info.storage.pages.total_pages}\\n`;
                    message += `空闲页数: ${info.storage.pages.free_pages}`;
                    alert(message);
                } else {
                    alert(result.message);
                }
            } catch (error) {
                alert(`获取数据库信息失败: ${error.message}`);
            }
        }
        
        // 保存文件
        function saveFile() {
            const content = document.getElementById('sqlEditor').value;
            const filename = prompt('请输入文件名:', 'query.sql');
            
            if (filename) {
                fetch('/api/save_file', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ filename: filename, content: content })
                })
                .then(response => response.json())
                .then(result => {
                    if (result.success) {
                        alert(result.message);
                    } else {
                        alert(result.message);
                    }
                })
                .catch(error => {
                    alert(`保存文件失败: ${error.message}`);
                });
            }
        }
        
        // 加载文件
        function loadFile() {
            const filename = prompt('请输入文件名:', 'query.sql');
            
            if (filename) {
                fetch('/api/load_file', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ filename: filename })
                })
                .then(response => response.json())
                .then(result => {
                    if (result.success) {
                        document.getElementById('sqlEditor').value = result.content;
                        alert('文件加载成功');
                    } else {
                        alert(result.message);
                    }
                })
                .catch(error => {
                    alert(`加载文件失败: ${error.message}`);
                });
            }
        }
        
        // 更新状态
        function updateStatus(message) {
            document.getElementById('statusText').textContent = message;
        }
    </script>
</body>
</html>'''
    
    # 写入HTML模板
    with open('templates/database_ide.html', 'w', encoding='utf-8') as f:
        f.write(html_template)
    
    print("数据库Web IDE启动中...")
    print("访问地址: http://localhost:5000")
    print("按 Ctrl+C 停止服务器")
    
    app.run(debug=True, host='0.0.0.0', port=5000)



