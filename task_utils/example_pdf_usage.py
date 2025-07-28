"""
PDF工具使用示例
演示如何使用基于pypandoc的pdf_utils模块
"""

from pdf_utils import (
    markdown_to_pdf, 
    markdown_to_pdf_simple,
    markdown_to_html,
    template_to_pdf, 
    check_pdf_dependencies, 
    get_dependency_install_instructions,
    install_pandoc_if_missing,
    generate_pdf_filename,
    FINANCIAL_REPORT_CSS,
    PDF_ENGINE_OPTIONS
)

def example_markdown_to_pdf():
    """示例：将Markdown转换为PDF（完整版）"""
    
    # 示例Markdown内容
    markdown_content = """
# 投资组合分析报告

*生成时间: 2024-01-15*

---

## 概要

本报告分析了当前投资组合的表现和风险特征。基于过去12个月的数据，我们对投资组合的风险收益特征进行了全面评估。

## 持仓明细

| 资产名称 | 代码 | 权重 | 收益率 | 风险评级 |
|---------|------|------|--------|----------|
| 沪深300ETF | 510300 | 35% | +12.5% | 中等 |
| 创业板ETF | 159915 | 25% | +8.3% | 较高 |
| 债券基金 | 000001 | 40% | +4.2% | 较低 |

## 风险分析

> **重要提示**: 市场存在波动风险，过往表现不代表未来收益，请谨慎投资。

### 关键指标

- **总收益率**: +8.7%
- **最大回撤**: -5.2%
- **夏普比率**: 1.34
- **波动率**: 12.8%

### 风险建议

1. **分散投资**: 当前组合已实现良好的分散化
2. **定期调整**: 建议每季度进行一次再平衡
3. **风险控制**: 密切关注最大回撤指标

```python
# 计算收益率示例代码
def calculate_return(initial_value, final_value):
    return (final_value - initial_value) / initial_value * 100

def calculate_sharpe_ratio(returns, risk_free_rate=0.03):
    excess_return = returns.mean() - risk_free_rate
    return excess_return / returns.std()
```

---

**免责声明**: 本报告仅供参考，不构成投资建议。投资有风险，入市需谨慎。
"""
    
    # 生成PDF文件路径（将使用task_dash/cache/pdf目录）
    output_path = generate_pdf_filename("投资组合分析报告")
    
    try:
        # 方法1：使用完整版（需要LaTeX）
        print("尝试使用XeLaTeX引擎生成PDF...")
        success = markdown_to_pdf(
            markdown_content=markdown_content,
            output_path=output_path,
            title="投资组合分析报告",
            pdf_options=PDF_ENGINE_OPTIONS['xelatex']
        )
        
        if success:
            print(f"✅ 完整版PDF生成成功: {output_path}")
        else:
            print("❌ 完整版PDF生成失败")
            
    except Exception as e:
        print(f"❌ 完整版生成错误: {e}")
        
        # 方法2：降级到简化版
        try:
            print("尝试使用简化版生成PDF...")
            simple_output = output_path.replace('.pdf', '_simple.pdf')
            success = markdown_to_pdf_simple(
                markdown_content=markdown_content,
                output_path=simple_output,
                title="投资组合分析报告"
            )
            
            if success:
                print(f"✅ 简化版PDF生成成功: {simple_output}")
            else:
                print("❌ 简化版PDF生成失败")
                
        except Exception as e2:
            print(f"❌ 简化版生成错误: {e2}")


def example_html_output():
    """示例：生成HTML预览"""
    
    markdown_content = """
# 基金投资简报

## 本月表现

| 基金名称 | 净值 | 涨跌幅 |
|---------|------|--------|
| 华夏成长 | 1.234 | +2.3% |
| 易方达价值 | 2.456 | +1.8% |

## 投资建议

**建议持有**，市场前景良好。
"""
    
    try:
        html_content = markdown_to_html(markdown_content, "基金投资简报")
        
        # 保存HTML文件
        html_path = generate_pdf_filename("基金简报").replace('.pdf', '.html')
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        print(f"✅ HTML文件生成成功: {html_path}")
        
    except Exception as e:
        print(f"❌ HTML生成错误: {e}")


def example_template_to_pdf():
    """示例：将模板数据转换为PDF"""
    
    # 模拟模板数据
    template_data = {
        "template_name": "基金分析报告",
        "template_description": "基于历史数据的基金表现分析",
        "template_content": [
            {
                "block_id": "summary_text",
                "block_type": "text",
                "block_title": "执行摘要",
                "config": {
                    "content": "本报告分析了过去一年中基金的表现情况。",
                    "style": "paragraph"
                }
            },
            {
                "block_id": "fund_table",
                "block_type": "product_table",
                "block_title": "基金持仓",
                "config": {
                    "product_type": "funds",
                    "display_fields": ["ts_code", "name", "latest_nav"],
                    "max_rows": 5
                }
            }
        ]
    }
    
    # 模拟渲染函数
    def mock_render_block(block_data):
        block_type = block_data.get('block_type', 'text')
        config = block_data.get('config', {})
        title = block_data.get('block_title', '未命名块')
        
        if block_type == 'text':
            content = config.get('content', '空白文本')
            return f"## {title}\n\n{content}\n\n"
        elif block_type == 'product_table':
            return f"## {title}\n\n| 代码 | 名称 | 最新净值 |\n|------|------|----------|\n| 000001 | 华夏成长 | 1.234 |\n| 000002 | 易方达价值 | 2.345 |\n\n"
        else:
            return f"## {title}\n\n*{block_type}类型的内容*\n\n"
    
    try:
        # 转换为PDF
        output_path = template_to_pdf(
            template_data=template_data,
            render_block_func=mock_render_block,
            custom_css=FINANCIAL_REPORT_CSS
        )
        
        print(f"✅ 模板PDF生成成功: {output_path}")
        
    except Exception as e:
        print(f"❌ 错误: {e}")


def check_environment():
    """检查PDF生成环境"""
    
    print("🔍 检查PDF生成依赖...")
    
    deps = check_pdf_dependencies()
    instructions = get_dependency_install_instructions()
    
    all_available = True
    
    for dep, available in deps.items():
        status = "✅" if available else "❌"
        print(f"{status} {dep}: {'已安装' if available else '未安装'}")
        
        if not available:
            all_available = False
            if dep in instructions:
                if isinstance(instructions[dep], dict):
                    print(f"   安装说明:")
                    for system, cmd in instructions[dep].items():
                        print(f"   - {system}: {cmd}")
                else:
                    print(f"   安装命令: {instructions[dep]}")
    
    # 尝试自动安装pandoc
    if deps['pypandoc'] and not deps['pandoc']:
        print("\n🔧 尝试自动安装pandoc...")
        try:
            install_pandoc_if_missing()
            print("✅ pandoc自动安装成功")
            deps['pandoc'] = True
        except Exception as e:
            print(f"❌ pandoc自动安装失败: {e}")
    
    if all_available or (deps['pypandoc'] and deps['pandoc']):
        if deps['xelatex']:
            print("\n🎉 所有依赖都已安装，可以生成高质量PDF！")
        else:
            print("\n⚠️  可以生成基本PDF，但缺少XeLaTeX可能影响中文显示")
        return True
    else:
        print("\n❌ 请先安装缺少的依赖")
        return False


if __name__ == "__main__":
    print("基于pypandoc的PDF工具使用示例\n" + "="*60)
    
    # 检查环境
    if check_environment():
        print("\n📄 生成Markdown PDF示例...")
        example_markdown_to_pdf()
        
        print("\n🌐 生成HTML预览示例...")
        example_html_output()
        
        print("\n📊 生成模板PDF示例...")
        example_template_to_pdf()
    
    print("\n✨ 示例完成！")
    print("\n💡 提示:")
    print("   - 如果遇到中文显示问题，请确保安装了XeLaTeX")
    print("   - 简化版PDF不需要LaTeX，但功能有限")
    print("   - HTML输出可以作为PDF的预览替代方案")