"""
PDF工具模块
提供Markdown到PDF转换等功能
使用pypandoc作为核心转换引擎
"""

import os
import tempfile
import platform
from datetime import datetime
from typing import Optional, Dict, Any

# 条件导入pypandoc，避免在未安装时导入错误
try:
    import pypandoc
    PYPANDOC_AVAILABLE = True
except ImportError:
    PYPANDOC_AVAILABLE = False


def get_system_chinese_fonts():
    """
    根据操作系统获取合适的中文字体
    
    Returns:
        Dict[str, str]: 字体配置字典
    """
    system = platform.system().lower()
    
    # 检查是否在WSL环境中（可以访问Windows字体）
    wsl_fonts_available = os.path.exists('/mnt/c/Windows/Fonts/simsun.ttc')
    
    if system == 'windows':
        return {
            'CJKmainfont': 'SimSun',         # 宋体
            'CJKsansfont': 'SimHei',         # 黑体
            'CJKmonofont': 'SimSun',         # 等宽字体用宋体
            'mainfont': 'Times New Roman',   # 英文主字体
            'sansfont': 'Arial',             # 英文无衬线
            'monofont': 'Courier New'        # 英文等宽
        }
    elif system == 'darwin':  # macOS
        return {
            'CJKmainfont': 'Songti SC',      # 宋体-简
            'CJKsansfont': 'Heiti SC',       # 黑体-简
            'CJKmonofont': 'Menlo',          # 等宽字体
            'mainfont': 'Times New Roman',   # 英文主字体
            'sansfont': 'Helvetica',         # 英文无衬线
            'monofont': 'Menlo'              # 英文等宽
        }
    elif system == 'linux':
        # 如果在WSL环境中，优先使用Windows字体
        if wsl_fonts_available:
            return {
                'CJKmainfont': 'SimSun',         # 宋体
                'CJKsansfont': 'SimHei',         # 黑体
                'CJKmonofont': 'SimSun',         # 等宽字体用宋体
                'mainfont': 'Times New Roman',   # 英文主字体
                'sansfont': 'Arial',             # 英文无衬线
                'monofont': 'Courier New'        # 英文等宽
            }
        else:
            return {
                'CJKmainfont': 'Noto Sans CJK SC',  # 思源黑体
                'CJKsansfont': 'Noto Sans CJK SC',  # 思源黑体
                'CJKmonofont': 'Noto Sans Mono CJK SC',  # 思源等宽
                'mainfont': 'Liberation Serif',     # 英文主字体
                'sansfont': 'Liberation Sans',      # 英文无衬线
                'monofont': 'Liberation Mono'       # 英文等宽
            }
    else:
        # 默认配置，尝试常见字体
        return {
            'CJKmainfont': 'SimSun',
            'CJKsansfont': 'SimHei', 
            'CJKmonofont': 'SimSun',
            'mainfont': 'Times New Roman',
            'sansfont': 'Arial',
            'monofont': 'Courier New'
        }


def create_font_variables(custom_fonts=None):
    """
    创建字体变量列表
    
    Args:
        custom_fonts: 自定义字体配置
        
    Returns:
        List[str]: 字体变量参数列表
    """
    fonts = custom_fonts or get_system_chinese_fonts()
    
    font_vars = []
    for font_type, font_name in fonts.items():
        font_vars.append(f'--variable={font_type}:{font_name}')
    
    return font_vars


def markdown_to_pdf(
    markdown_content: str,
    output_path: str,
    title: str = "Document",
    custom_css: Optional[str] = None,
    pdf_options: Optional[Dict[str, Any]] = None
) -> bool:
    """
    将Markdown内容转换为PDF文件
    
    Args:
        markdown_content: Markdown文本内容
        output_path: 输出PDF文件路径
        title: 文档标题
        custom_css: 自定义CSS样式文件路径或CSS内容
        pdf_options: PDF生成选项
        
    Returns:
        bool: 转换是否成功
        
    Raises:
        ImportError: 缺少必要的依赖库
        Exception: PDF生成失败
    """
    
    if not PYPANDOC_AVAILABLE:
        raise ImportError("缺少pypandoc库，请安装: pip install pypandoc")
    
    # 确保输出目录存在
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # 默认PDF选项
    default_options = [
        '--pdf-engine=xelatex',  # 使用xelatex引擎，支持中文
        '--variable=geometry:margin=2cm',  # 页面边距
        '--variable=documentclass:article',  # 文档类型
        '--variable=fontsize:12pt',  # 字体大小
        '--variable=linestretch:1.5',  # 行间距
        '--table-of-contents',  # 生成目录
        '--number-sections',  # 章节编号
    ]
    
    # 添加系统适配的字体配置
    default_options.extend(create_font_variables())
    
    # 使用自定义选项或默认选项
    extra_args = pdf_options if pdf_options else default_options
    
    # 添加标题
    if title and title != "Document":
        extra_args.extend([
            f'--variable=title:{title}',
            f'--variable=author:baofu系统自动生成',
            f'--variable=date:{datetime.now().strftime("%Y年%m月%d日")}'
        ])
    
    # 处理CSS样式
    css_file = None
    if custom_css:
        if os.path.isfile(custom_css):
            # 如果是文件路径
            css_file = custom_css
        else:
            # 如果是CSS内容，创建临时文件
            css_file = _create_temp_css_file(custom_css)
        
        extra_args.append(f'--css={css_file}')
    
    try:
        # 使用pypandoc转换
        output = pypandoc.convert_text(
            markdown_content,
            'pdf',
            format='md',
            outputfile=output_path,
            extra_args=extra_args
        )
        
        return True
        
    except Exception as e:
        raise Exception(f"PDF生成失败: {str(e)}。请确保已安装pandoc和xelatex。")
    
    finally:
        # 清理临时CSS文件
        if css_file and custom_css and not os.path.isfile(custom_css):
            try:
                os.unlink(css_file)
            except:
                pass


def _create_temp_css_file(css_content: str) -> str:
    """
    创建临时CSS文件
    
    Args:
        css_content: CSS内容
        
    Returns:
        str: 临时文件路径
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.css', delete=False, encoding='utf-8') as f:
        f.write(css_content)
        return f.name


def markdown_to_pdf_simple(
    markdown_content: str,
    output_path: str,
    title: str = "Document"
) -> bool:
    """
    简化版Markdown转PDF，使用基本设置
    
    Args:
        markdown_content: Markdown文本内容
        output_path: 输出PDF文件路径
        title: 文档标题
        
    Returns:
        bool: 转换是否成功
    """
    if not PYPANDOC_AVAILABLE:
        raise ImportError("缺少pypandoc库，请安装: pip install pypandoc")
    
    # 确保输出目录存在
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # 基本选项
    extra_args = [
        '--pdf-engine=xelatex',
        '--variable=geometry:margin=2.5cm',
        '--variable=fontsize:11pt',
        '--variable=linestretch:1.4',
        f'--variable=title:{title}',
        f'--variable=date:{datetime.now().strftime("%Y-%m-%d")}'
    ]
    
    # 添加系统适配的字体配置
    extra_args.extend(create_font_variables())
    
    try:
        pypandoc.convert_text(
            markdown_content,
            'pdf',
            format='md',
            outputfile=output_path,
            extra_args=extra_args
        )
        return True
    except Exception as e:
        raise Exception(f"PDF生成失败: {str(e)}")


def markdown_to_html(markdown_content: str, title: str = "Document") -> str:
    """
    将Markdown转换为HTML
    
    Args:
        markdown_content: Markdown文本内容
        title: 文档标题
        
    Returns:
        str: HTML内容
    """
    if not PYPANDOC_AVAILABLE:
        raise ImportError("缺少pypandoc库，请安装: pip install pypandoc")
    
    try:
        html_content = pypandoc.convert_text(
            markdown_content,
            'html',
            format='md',
            extra_args=['--standalone', f'--variable=title:{title}']
        )
        return html_content
    except Exception as e:
        raise Exception(f"HTML生成失败: {str(e)}")


def generate_pdf_filename(base_name: str, output_dir: str = None) -> str:
    """
    生成PDF文件名和路径
    
    Args:
        base_name: 基础文件名
        output_dir: 输出目录，默认为task_dash/cache/pdf目录
        
    Returns:
        str: 完整的文件路径
    """
    # 清理文件名，只保留安全字符
    safe_name = "".join(c for c in base_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
    
    # 添加时间戳
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{safe_name}_{timestamp}.pdf" if safe_name else f"document_{timestamp}.pdf"
    
    # 确定输出目录
    if output_dir is None:
        # 默认为task_dash/cache/pdf目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        output_dir = os.path.join(project_root, 'task_dash', 'cache', 'pdf')
    
    # 确保目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    return os.path.join(output_dir, filename)


def template_to_pdf(
    template_data: Dict,
    render_block_func,
    output_path: str = None,
    custom_css: str = None
) -> str:
    """
    将模板数据转换为PDF
    
    Args:
        template_data: 模板数据字典
        render_block_func: 渲染块的函数
        output_path: 输出路径，如果为None则自动生成
        custom_css: 自定义CSS样式
        
    Returns:
        str: 生成的PDF文件路径
        
    Raises:
        Exception: 转换失败
    """
    # 生成完整的Markdown内容
    full_markdown = ""
    template_name = template_data.get('template_name', '未命名模板')
    template_description = template_data.get('template_description', '')
    
    # 添加模板标题和描述
    full_markdown += f"# {template_name}\n\n"
    if template_description:
        full_markdown += f"*{template_description}*\n\n---\n\n"
    else:
        full_markdown += "---\n\n"
    
    # 遍历所有块
    blocks = template_data.get('template_content', [])
    for i, block_data in enumerate(blocks):
        try:
            block_markdown = render_block_func(block_data)
            full_markdown += block_markdown
            
            if i < len(blocks) - 1:
                full_markdown += "\n---\n\n"
                
        except Exception as e:
            block_title = block_data.get('block_title', f'块 #{i+1}')
            full_markdown += f"**[错误: {block_title}]**\n\n"
            full_markdown += f"```\n渲染失败: {str(e)}\n```\n\n"
    
    # 如果没有指定输出路径，自动生成
    if output_path is None:
        output_path = generate_pdf_filename(template_name)
    
    # 转换为PDF
    success = markdown_to_pdf(
        markdown_content=full_markdown,
        output_path=output_path,
        title=template_name,
        custom_css=custom_css
    )
    
    if success:
        return output_path
    else:
        raise Exception("PDF生成失败")


def check_pdf_dependencies() -> Dict[str, bool]:
    """
    检查PDF生成所需的依赖
    
    Returns:
        Dict[str, bool]: 依赖检查结果
    """
    dependencies = {
        'pypandoc': False,
        'pandoc': False,
        'xelatex': False
    }
    
    # 检查pypandoc库
    if PYPANDOC_AVAILABLE:
        dependencies['pypandoc'] = True
        
        # 检查pandoc
        try:
            pypandoc.get_pandoc_version()
            dependencies['pandoc'] = True
        except:
            pass
            
        # 检查xelatex (TeX Live)
        try:
            import subprocess
            result = subprocess.run(['xelatex', '--version', ], 
                                  capture_output=True, text=True, timeout=10, encoding='utf-8')
            if result.returncode == 0:
                dependencies['xelatex'] = True
        except:
            pass
    
    return dependencies


def get_dependency_install_instructions() -> Dict[str, Any]:
    """
    获取依赖安装说明
    
    Returns:
        Dict[str, Any]: 安装说明字典
    """
    return {
        'pypandoc': 'pip install pypandoc',
        'pandoc': {
            'Ubuntu/Debian': 'sudo apt-get install pandoc',
            'macOS': 'brew install pandoc',
            'Windows': '下载并安装 https://pandoc.org/installing.html',
            'Conda': 'conda install -c conda-forge pandoc'
        },
        'xelatex': {
            'Ubuntu/Debian': 'sudo apt-get install texlive-xetex texlive-fonts-recommended',
            'macOS': 'brew install --cask mactex',
            'Windows': '下载并安装 https://miktex.org/',
            'Conda': 'conda install -c conda-forge texlive-core'
        }
    }


def install_pandoc_if_missing():
    """
    自动安装pandoc（如果pypandoc可用但pandoc缺失）
    """
    if not PYPANDOC_AVAILABLE:
        raise ImportError("需要先安装pypandoc: pip install pypandoc")
    
    try:
        pypandoc.get_pandoc_version()
        return True  # pandoc已安装
    except:
        # 尝试自动下载pandoc
        try:
            pypandoc.download_pandoc()
            return True
        except Exception as e:
            raise Exception(f"自动安装pandoc失败: {str(e)}")


# 财务报告专用的CSS样式（用于HTML输出）
FINANCIAL_REPORT_CSS = """
body {
    font-family: "Source Han Sans CN", "Noto Sans CJK SC", sans-serif;
    line-height: 1.6;
    color: #333;
    max-width: 900px;
    margin: 0 auto;
    padding: 40px 20px;
}

h1 {
    color: #1a5490;
    border-bottom: 3px solid #1a5490;
    padding-bottom: 10px;
    margin-bottom: 30px;
    text-align: center;
}

h2 {
    color: #2c3e50;
    border-left: 4px solid #3498db;
    padding-left: 15px;
    margin-top: 2em;
}

h3, h4, h5, h6 {
    color: #34495e;
    margin-top: 1.5em;
}

table {
    border-collapse: collapse;
    width: 100%;
    margin: 1.5em 0;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

th {
    background-color: #1a5490;
    color: white;
    font-weight: bold;
    padding: 12px;
    text-align: center;
}

td {
    border: 1px solid #ddd;
    padding: 8px 12px;
    text-align: right;
}

td:first-child {
    text-align: left;
    font-weight: 500;
}

tr:nth-child(even) {
    background-color: #f8f9fa;
}

.financial-summary {
    background-color: #e8f4f8;
    border-left: 5px solid #1a5490;
    padding: 15px;
    margin: 20px 0;
}

.positive {
    color: #27ae60;
    font-weight: bold;
}

.negative {
    color: #e74c3c;
    font-weight: bold;
}

.chart-placeholder {
    background-color: #f8f9fa;
    border: 2px dashed #bdc3c7;
    padding: 40px;
    text-align: center;
    color: #7f8c8d;
    margin: 20px 0;
}
"""

# PDF生成的默认选项配置
def get_pdf_engine_options():
    """
    获取PDF引擎选项，动态配置字体
    
    Returns:
        Dict[str, List[str]]: PDF引擎选项字典
    """
    base_xelatex_options = [
        '--pdf-engine=xelatex',
        '--variable=geometry:margin=2cm',
        '--variable=documentclass:article',
        '--variable=fontsize:11pt',
        '--variable=linestretch:1.4',
        '--table-of-contents',
        '--number-sections',
    ]
    
    # 添加系统适配的字体配置
    xelatex_options = base_xelatex_options + create_font_variables()
    
    return {
        'xelatex': xelatex_options,
    'pdflatex': [
        '--pdf-engine=pdflatex',
        '--variable=geometry:margin=2cm',
        '--variable=documentclass:article',
        '--variable=fontsize:11pt',
        '--variable=linestretch:1.4',
    ],
    'weasyprint': [
        '--pdf-engine=weasyprint',
    ]
}

# 简化版PDF选项（不需要LaTeX）
SIMPLE_PDF_OPTIONS = [
    '--variable=geometry:margin=2.5cm',
    '--variable=fontsize:11pt',
    '--variable=linestretch:1.4',
]


def test_basic_functions():
    """
    测试不依赖pypandoc的基础功能
    """
    print("🧪 基础功能测试（无需pypandoc）")
    print("=" * 40)
    
    # 测试文件名生成
    print("1️⃣ 测试文件名生成功能...")
    try:
        filename1 = generate_pdf_filename("测试报告")
        print(f"   ✅ 标准文件名: {os.path.basename(filename1)}")
        
        filename2 = generate_pdf_filename("Test Report with Space & Special@#$%")
        print(f"   ✅ 特殊字符清理: {os.path.basename(filename2)}")
        
        filename3 = generate_pdf_filename("")
        print(f"   ✅ 空名称处理: {os.path.basename(filename3)}")
        
        # 测试自定义目录
        import tempfile
        temp_dir = tempfile.mkdtemp()
        filename4 = generate_pdf_filename("自定义目录测试", temp_dir)
        print(f"   ✅ 自定义目录: {filename4}")
        
        print("   ✅ 文件名生成功能测试通过")
        
    except Exception as e:
        print(f"   ❌ 文件名生成测试失败: {e}")
    
    # 测试依赖检查
    print("\n2️⃣ 测试依赖检查功能...")
    try:
        deps = check_pdf_dependencies()
        print("   依赖状态:")
        for dep, available in deps.items():
            status = "✅ 已安装" if available else "❌ 未安装"
            print(f"     {dep}: {status}")
        print("   ✅ 依赖检查功能测试通过")
        
    except Exception as e:
        print(f"   ❌ 依赖检查测试失败: {e}")
    
    # 测试安装说明
    print("\n3️⃣ 测试安装说明功能...")
    try:
        instructions = get_dependency_install_instructions()
        print("   安装说明获取成功:")
        for dep, inst in instructions.items():
            print(f"     {dep}: {type(inst).__name__}")
        print("   ✅ 安装说明功能测试通过")
        
    except Exception as e:
        print(f"   ❌ 安装说明测试失败: {e}")
    
    print("\n📊 基础功能测试完成")
    return True


def main():
    """
    主函数 - 用于测试PDF工具的各项功能
    """
    print("🔧 PDF工具测试程序")
    print("=" * 50)
    
    # 首先测试基础功能
    test_basic_functions()
    print("\n" + "=" * 50)
    
    # 1. 检查依赖
    print("1️⃣ 检查PDF生成依赖...")
    dependencies = check_pdf_dependencies()
    for dep, available in dependencies.items():
        status = "✅ 已安装" if available else "❌ 未安装"
        print(f"   {dep}: {status}")
    
    if not dependencies['pypandoc']:
        print("❌ 缺少pypandoc库，无法继续测试")
        print("💡 安装方法: pip install pypandoc")
        return
    
    if not dependencies['pandoc']:
        print("⚠️ 缺少pandoc，尝试自动安装...")
        try:
            install_pandoc_if_missing()
            print("✅ pandoc自动安装成功")
        except Exception as e:
            print(f"❌ pandoc自动安装失败: {e}")
            print("💡 请手动安装pandoc: https://pandoc.org/installing.html")
            return
    
    print("\n2️⃣ 测试基础功能...")
    
    # 测试用的Markdown内容
    test_markdown = """
# PDF工具测试报告

*测试时间: {date}*

---
title: 测试文档
author: 张三
date: 2025-07-26
fontsize: 12pt
mainfont: Microsoft YaHei
---

---

## 功能测试

### 1. 基本文档结构

这是一个**测试文档**，用于验证PDF生成功能。

### 2. 列表功能

- 项目一：基本文本
- 项目二：*斜体文本*
- 项目三：**粗体文本**

### 3. 表格功能

| 功能 | 状态 | 说明 |
|------|------|------|
| Markdown转换 | ✅ | 正常工作 |
| PDF生成 | ✅ | 正常工作 |
| 中文支持 | ✅ | 正常工作 |

### 4. 代码块

```python
def test_function():
    print("Hello, PDF World!")
    return True
```

## 总结

PDF工具功能测试完成。

---

**免责声明**: 这是一个测试文档，仅用于验证PDF生成功能。
""".format(date=datetime.now().strftime("%Y年%m月%d日"))
    
    try:
        # 2.1 测试文件名生成
        print("   📝 测试文件名生成...")
        test_filename = generate_pdf_filename("PDF工具测试")
        print(f"   生成文件路径: {test_filename}")
        
        # 2.2 测试HTML转换
        print("   🌐 测试Markdown转HTML...")
        html_content = markdown_to_html(test_markdown, "PDF工具测试报告")
        print(f"   HTML长度: {len(html_content)} 字符")
        
        # 2.3 测试简化版PDF生成
        print("   📄 测试简化版PDF生成...")
        simple_pdf_path = test_filename.replace('.pdf', '_simple.pdf')
        success = markdown_to_pdf_simple(
            markdown_content=test_markdown,
            output_path=simple_pdf_path,
            title="PDF工具测试报告（简化版）"
        )
        
        if success:
            print(f"   ✅ 简化版PDF生成成功: {simple_pdf_path}")
            print(f"   📁 文件大小: {os.path.getsize(simple_pdf_path)} 字节")
        else:
            print("   ❌ 简化版PDF生成失败")
        
        # 2.4 测试完整版PDF生成（如果有XeLaTeX）
        if dependencies['xelatex']:
            print("   📄 测试完整版PDF生成...")
            full_pdf_path = test_filename.replace('.pdf', '_full.pdf')
            success = markdown_to_pdf(
                markdown_content=test_markdown,
                output_path=full_pdf_path,
                title="PDF工具测试报告（完整版）",
                custom_css=FINANCIAL_REPORT_CSS,
                pdf_options=PDF_ENGINE_OPTIONS['xelatex']
            )
            
            if success:
                print(f"   ✅ 完整版PDF生成成功: {full_pdf_path}")
                print(f"   📁 文件大小: {os.path.getsize(full_pdf_path)} 字节")
            else:
                print("   ❌ 完整版PDF生成失败")
        else:
            print("   ⚠️ 跳过完整版PDF测试（缺少XeLaTeX）")
        
        # 3. 测试模板转换功能
        print("\n3️⃣ 测试模板转换功能...")
        
        # 模拟模板数据
        test_template = {
            "template_name": "测试模板报告",
            "template_description": "这是一个用于测试的模板",
            "template_content": [
                {
                    "block_id": "summary",
                    "block_type": "text",
                    "block_title": "摘要",
                    "config": {
                        "content": "这是测试模板的摘要部分，用于验证模板转换功能。"
                    }
                },
                {
                    "block_id": "data_table",
                    "block_type": "table",
                    "block_title": "数据表格",
                    "config": {
                        "headers": ["项目", "数值", "状态"],
                        "rows": [
                            ["测试项目1", "100", "正常"],
                            ["测试项目2", "200", "正常"]
                        ]
                    }
                }
            ]
        }
        
        # 模拟渲染函数
        def mock_render_block(block_data):
            block_type = block_data.get('block_type', 'text')
            title = block_data.get('block_title', '未命名块')
            config = block_data.get('config', {})
            
            if block_type == 'text':
                content = config.get('content', '空白内容')
                return f"## {title}\n\n{content}\n\n"
            elif block_type == 'table':
                headers = config.get('headers', [])
                rows = config.get('rows', [])
                
                markdown = f"## {title}\n\n"
                if headers:
                    markdown += "| " + " | ".join(headers) + " |\n"
                    markdown += "| " + " | ".join(['---'] * len(headers)) + " |\n"
                    
                    for row in rows:
                        if len(row) == len(headers):
                            markdown += "| " + " | ".join(row) + " |\n"
                
                return markdown + "\n"
            else:
                return f"## {title}\n\n*未知块类型: {block_type}*\n\n"
        
        # 测试模板转PDF
        template_pdf_path = template_to_pdf(
            template_data=test_template,
            render_block_func=mock_render_block
        )
        
        print(f"   ✅ 模板PDF生成成功: {template_pdf_path}")
        print(f"   📁 文件大小: {os.path.getsize(template_pdf_path)} 字节")
        
        # 4. 显示生成的文件列表
        print("\n4️⃣ 生成的文件列表:")
        cache_dir = os.path.dirname(test_filename)
        if os.path.exists(cache_dir):
            pdf_files = [f for f in os.listdir(cache_dir) if f.endswith('.pdf')]
            for pdf_file in sorted(pdf_files):
                file_path = os.path.join(cache_dir, pdf_file)
                file_size = os.path.getsize(file_path)
                print(f"   📄 {pdf_file} ({file_size} 字节)")
        
        print("\n✅ 所有测试完成！")
        
        # 5. 提供测试总结
        print("\n📊 测试总结:")
        print(f"   - 依赖检查: {'✅ 通过' if all(dependencies.values()) else '⚠️ 部分缺失'}")
        print(f"   - 基础功能: ✅ 通过")
        print(f"   - 模板转换: ✅ 通过")
        print(f"   - 文件生成: ✅ 通过")
        
        if not dependencies['xelatex']:
            print("\n💡 建议:")
            print("   - 安装XeLaTeX以获得更好的中文支持和排版效果")
            instructions = get_dependency_install_instructions()
            if 'xelatex' in instructions:
                print("   - XeLaTeX安装方法:")
                for system, cmd in instructions['xelatex'].items():
                    print(f"     {system}: {cmd}")
    
    except Exception as e:
        print(f"\n❌ 测试过程中出现错误: {str(e)}")
        print("\n🔍 错误排查建议:")
        print("   1. 确保已安装pypandoc: pip install pypandoc")
        print("   2. 确保已安装pandoc: https://pandoc.org/installing.html")
        print("   3. 如需中文支持，请安装XeLaTeX")
        print("   4. 检查文件权限和磁盘空间")


if __name__ == "__main__":
    import sys
    
    # 如果提供了 --basic 参数，只运行基础测试
    if len(sys.argv) > 1 and sys.argv[1] == "--basic":
        test_basic_functions()
    else:
        main()