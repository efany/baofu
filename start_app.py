#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
启动基金管理应用

使用方法：
python3 start_app.py
"""

import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    """启动应用"""
    try:
        print("正在启动基金管理应用...")
        print("=" * 50)
        
        # 导入应用
        from task_dash.app import app
        
        print("✅ 应用导入成功")
        print("🚀 正在启动服务器...")
        print("📱 访问地址: http://localhost:8050")
        print("🔧 数据源管理: http://localhost:8050/data_sources_manage")
        print("📊 产品管理: http://localhost:8050/products_manage")
        print("=" * 50)
        print("按 Ctrl+C 停止服务")
        
        # 启动服务器
        app.run_server(
            debug=True,
            host='0.0.0.0',
            port=8050,
            dev_tools_hot_reload=True
        )
        
    except KeyboardInterrupt:
        print("\n👋 服务已停止")
    except Exception as e:
        print(f"❌ 启动失败: {e}")
        print("\n请检查:")
        print("1. 数据库连接是否正常")
        print("2. 依赖包是否已安装")
        print("3. 端口8050是否被占用")
        sys.exit(1)

if __name__ == "__main__":
    main()