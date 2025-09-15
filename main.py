#!/usr/bin/env python3
"""
数据库系统主程序入口
提供命令行界面和交互式数据库操作
支持用户ID与数据库文件绑定
"""
import sys
import os
from database.database import Database, DatabaseCLI
from database.user_manager import UserManager


def main():
    """主函数"""
    # 检查命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help" or sys.argv[1] == "-h":
            print("数据库系统")
            print("用法: python main.py <用户ID> [数据库文件]")
            print("  用户ID: 必需，指定用户标识符")
            print("  数据库文件: 可选，指定要打开的数据库文件")
            print("  如果不指定数据库文件，将列出该用户的所有数据库")
            print("  示例: python main.py user1 mydb.db")
            print("  示例: python main.py user1  # 列出用户1的所有数据库")
            return
        elif len(sys.argv) == 2:
            # 只有用户ID，列出该用户的所有数据库
            user_id = sys.argv[1]
            list_user_databases(user_id)
            return
        elif len(sys.argv) == 3:
            # 用户ID和数据库文件
            user_id = sys.argv[1]
            db_file = sys.argv[2]
            open_database(user_id, db_file)
            return
        else:
            print("参数错误。使用 --help 查看帮助信息")
            return
    else:
        print("缺少用户ID参数。使用 --help 查看帮助信息")
        return


def list_user_databases(user_id: str):
    """列出用户的所有数据库"""
    user_manager = UserManager()
    
    print(f"用户 {user_id} 的数据库列表:")
    print("=" * 50)
    
    databases = user_manager.get_user_databases(user_id)
    
    if not databases:
        print("该用户没有绑定的数据库")
        print("\n要绑定数据库，请使用:")
        print(f"  python main.py {user_id} <数据库文件>")
        return
    
    for i, db_file in enumerate(databases, 1):
        binding_info = user_manager.get_binding_info(db_file)
        if binding_info:
            print(f"{i}. {db_file}")
            print(f"   创建时间: {binding_info.created_at}")
            print(f"   最后访问: {binding_info.last_accessed}")
            print(f"   文件存在: {'是' if os.path.exists(db_file) else '否'}")
            print()
    
    print(f"总共 {len(databases)} 个数据库")
    print("\n要打开特定数据库，请使用:")
    print(f"  python main.py {user_id} <数据库文件>")


def open_database(user_id: str, db_file: str):
    """打开指定的数据库"""
    user_manager = UserManager()
    
    # 检查数据库文件是否已被其他用户绑定
    owner = user_manager.get_database_owner(db_file)
    if owner and owner != user_id:
        print(f"错误: 数据库文件 {db_file} 已被用户 {owner} 绑定")
        print("一个数据库文件只能被一个用户绑定")
        return
    
    # 如果数据库文件不存在，创建它
    if not os.path.exists(db_file):
        print(f"数据库文件 {db_file} 不存在，将创建新文件")
    
    # 绑定数据库到用户（如果尚未绑定）
    if not user_manager.is_database_bound(db_file):
        if not user_manager.bind_database(user_id, db_file):
            print("绑定数据库失败")
            return
    
    # 更新最后访问时间
    user_manager.update_last_accessed(db_file)
    
    # 创建数据库连接
    try:
        db = Database(db_file)
        print(f"数据库已连接到: {db_file}")
        print(f"用户: {user_id}")
        
        # 启动CLI
        cli = DatabaseCLI(db, user_id)
        cli.start()
        
    except Exception as e:
        print(f"启动数据库失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
