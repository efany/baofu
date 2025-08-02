"""
基金数据块实现
接收产品编号和时间窗口，调用fund_data_generator生成数据并展示
"""

from typing import List, Optional
from datetime import datetime, date
from .base_block import BaseBlock, BlockParameter
from task_dash.utils import generate_chart_image
import sys
import os
import pandas as pd
import re

# 添加项目路径以便导入数据生成器
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))


class FundBlock(BaseBlock):
    """基金数据块"""
    
    def __init__(self, block_data: dict, mysql_db=None):
        """初始化基金数据块
        
        Args:
            block_data: 块数据
            mysql_db: MySQL数据库实例，如果为None则在使用时会报错
        """
        super().__init__(block_data)
        self.mysql_db = mysql_db
    
    @property
    def block_name(self) -> str:
        return "基金数据"
    
    @property
    def block_icon(self) -> str:
        return "💰"
    
    @property
    def block_description(self) -> str:
        return "显示指定基金的净值走势、收益统计和详细数据"
    
    def _get_fund_options(self) -> List[dict]:
        """获取基金选项列表"""
        if not self.mysql_db:
            # 如果没有数据库连接，返回默认选项
            return [
                {"label": "000001.OF - 华夏成长混合", "value": "000001.OF"},
                {"label": "110022.OF - 易方达消费行业股票", "value": "110022.OF"}
            ]
        
        try:
            from database.db_funds import DBFunds
            
            # 使用注入的数据库连接
            db_funds = DBFunds(self.mysql_db)
            funds_df = db_funds.get_all_funds()
            
            # 转换为选项格式
            options = []
            if not funds_df.empty:
                for _, row in funds_df.iterrows():
                    ts_code = row.get('ts_code', '')
                    name = row.get('name', '')
                    management = row.get('management', '')
                    
                    # 构建显示标签
                    label = f"{ts_code}"
                    if name:
                        label += f" - {name}"
                    if management:
                        label += f" ({management})"
                    
                    options.append({
                        "label": label,
                        "value": ts_code
                    })
            
            # 按基金代码排序
            options.sort(key=lambda x: x["value"])
            
            return options
            
        except Exception as e:
            # 如果获取失败，返回默认选项
            return [
                {"label": "000001.OF - 华夏成长混合", "value": "000001.OF"},
                {"label": "110022.OF - 易方达消费行业股票", "value": "110022.OF"}
            ]
    
    @property
    def parameters(self) -> List[BlockParameter]:
        return [
            BlockParameter(
                name="fund_code",
                label="基金代码",
                param_type="select",
                default_value="000001.OF",
                required=True,
                options=self._get_fund_options(),
                description="选择要分析的基金",
                placeholder="请选择基金"
            ),
            BlockParameter(
                name="display_type",
                label="显示内容",
                param_type="select",
                default_value="summary",
                required=True,
                options=[
                    {"label": "📊 摘要信息", "value": "summary"},
                    {"label": "📈 净值走势图", "value": "chart"},
                    {"label": "📋 详细数据表", "value": "table"},
                    {"label": "📑 完整报告", "value": "full"}
                ],
                description="选择要显示的内容类型"
            ),
            BlockParameter(
                name="show_dividends",
                label="显示分红信息",
                param_type="boolean",
                default_value=True,
                description="在图表中标记分红发放点"
            ),
            BlockParameter(
                name="normalize_data",
                label="归一化净值",
                param_type="boolean",
                default_value=False,
                description="将净值数据归一化到起始点为1.0，便于比较不同基金"
            ),
            BlockParameter(
                name="period_filter",
                label="时间周期",
                param_type="select",
                default_value="all",
                options=[
                    {"label": "近1天", "value": "1d"},
                    {"label": "近1周", "value": "1w"},
                    {"label": "近1个月", "value": "1m"},
                    {"label": "近1个季度", "value": "3m"},
                    {"label": "近半年", "value": "6m"},
                    {"label": "近1年", "value": "1y"},
                    {"label": "近3年", "value": "3y"},
                    {"label": "近5年", "value": "5y"},
                    {"label": "今年至今", "value": "ytd"},
                    {"label": "全部数据", "value": "all"}
                ],
                description="选择时间范围"
            ),
            BlockParameter(
                name="show_benchmark",
                label="显示基准对比",
                param_type="boolean",
                default_value=False,
                description="显示与业绩基准的对比（如果数据可用）"
            )
        ]
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """解析日期字符串"""
        if not date_str or not date_str.strip():
            return None
        
        try:
            return datetime.strptime(date_str.strip(), '%Y-%m-%d').date()
        except ValueError:
            return None
    
    def _format_table_value(self, value, column_name: str = "") -> str:
        """格式化表格数值，提供更好的显示效果"""
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return "—"  # 使用em-dash替代N/A
            
            # 百分比格式化
            if ('率' in column_name or 'pct' in column_name.lower() or 
                'rate' in column_name.lower() or '收益' in column_name or '回撤' in column_name):
                if abs(value) < 0.01:
                    return f"{value:+.3f}%"
                else:
                    return f"{value:+.2f}%"
            
            # 货币和净值格式化
            elif ('净值' in column_name or '价格' in column_name or 'nav' in column_name.lower()):
                return f"{value:.4f}"
            
            # 年份格式化
            elif '年' in column_name and isinstance(value, (int, float)) and 1900 <= value <= 2100:
                return str(int(value))
            
            # 整数格式化
            elif isinstance(value, int) or (isinstance(value, float) and value.is_integer()):
                if abs(value) >= 1000:
                    return f"{int(value):,}"
                else:
                    return str(int(value))
            
            # 普通小数格式化
            else:
                if abs(value) < 0.001:
                    return f"{value:.6f}"
                elif abs(value) < 1:
                    return f"{value:.4f}"
                else:
                    return f"{value:.2f}"
        else:
            # 字符串值处理
            str_value = str(value).strip()
            
            # 处理已经包含百分号的字符串
            if '%' in str_value and ('(' in str_value or '->' in str_value):
                # 如: "+15.25% (1.0000 -> 1.1525)" 或 "投资最大回撤: -8.45%"
                return str_value
            elif '%' in str_value:
                # 简单百分比字符串
                return str_value
            elif re.match(r'^\d{4}$', str_value):
                # 年份格式
                return str_value
            elif re.match(r'^\d{4}[年]?\d{1,2}[月]?', str_value):
                # 日期格式
                return str_value
            else:
                # 普通字符串
                return str_value


    def _get_last_valid_date(self, fund_code: str) -> date:
        """获取指定基金的最后有效日期"""
        try:
            from database.db_funds_nav import DBFundsNav
            
            db_funds_nav = DBFundsNav(self.mysql_db)
            latest_date_str = db_funds_nav.get_latest_nav_date(fund_code)
            
            if latest_date_str:
                return datetime.strptime(latest_date_str, '%Y-%m-%d').date()
        except Exception:
            pass
        
        return date.today()
    
    def _calculate_period_dates(self, fund_code: str = None) -> tuple:
        """根据时间周期计算开始和结束日期"""
        from datetime import timedelta
        
        period_filter = self.get_parameter_value("period_filter", "all")
        
        # 获取最后有效日期作为end_date
        if fund_code:
            end_date = self._get_last_valid_date(fund_code)
        else:
            end_date = date.today()
        
        if period_filter == "1d":
            start_date = end_date - timedelta(days=1)
            return start_date, end_date
        elif period_filter == "1w":
            start_date = end_date - timedelta(days=7)
            return start_date, end_date
        elif period_filter == "1m":
            start_date = end_date - timedelta(days=30)
            return start_date, end_date
        elif period_filter == "3m":
            start_date = end_date - timedelta(days=90)
            return start_date, end_date
        elif period_filter == "6m":
            start_date = end_date - timedelta(days=180)
            return start_date, end_date
        elif period_filter == "1y":
            start_date = end_date - timedelta(days=365)
            return start_date, end_date
        elif period_filter == "3y":
            start_date = end_date - timedelta(days=365 * 3)
            return start_date, end_date
        elif period_filter == "5y":
            start_date = end_date - timedelta(days=365 * 5)
            return start_date, end_date
        elif period_filter == "ytd":
            start_date = date(end_date.year, 1, 1)
            return start_date, end_date
        else:  # "all"
            return None, None

    def _generate_fund_data(self) -> dict:
        """生成基金数据"""
        fund_code = self.get_parameter_value("fund_code", "").strip()
        
        if not fund_code:
            return {"error": "基金代码不能为空"}
        
        if not self.mysql_db:
            return {"error": "数据库连接未初始化"}
        
        # 计算日期范围
        start_date, end_date = self._calculate_period_dates(fund_code)
        
        try:
            # 导入所需模块（延迟导入以避免循环依赖）
            from task_dash.datas.fund_data_generator import FundDataGenerator
            
            # 使用注入的数据库连接
            generator = FundDataGenerator(
                fund_code=fund_code,
                mysql_db=self.mysql_db,
                start_date=start_date,
                end_date=end_date
            )
            
            # 加载数据
            success = generator.load()
            if not success:
                return {"error": f"无法加载基金 {fund_code} 的数据"}
            
            # 获取各种数据
            summary_data = generator.get_summary_data()
            chart_data = generator.get_chart_data(
                normalize=self.get_parameter_value("normalize_data", False)
            )
            extra_data = generator.get_extra_datas()
            
            return {
                "success": True,
                "fund_code": fund_code,
                "summary": summary_data,
                "chart": chart_data,
                "extra_data": extra_data,
                "start_date": start_date,
                "end_date": end_date
            }
            
        except ImportError as e:
            return {"error": f"导入模块失败: {str(e)}"}
        except Exception as e:
            return {"error": f"数据生成失败: {str(e)}"}
    
    def render_to_html(self, for_pdf: bool = False) -> str:
        """渲染为HTML
        
        Args:
            for_pdf: 是否为PDF导出，True时使用绝对路径生成图片
        """
        display_type = self.get_parameter_value("display_type", "summary")

        # 生成基金数据
        data_result = self._generate_fund_data()

        html = f"<h2>{self.block_title}</h2>\n\n"

        # 如果有错误，显示错误信息
        if "error" in data_result:
            html += f'<div class="alert alert-danger"><strong>❌ 错误</strong>: {data_result["error"]}</div>\n\n'
            return html
        
        # 基金基础信息
        fund_code = data_result["fund_code"]
        summary = data_result["summary"]
        
        if display_type in ["summary", "full"]:
            html += '<h3>📊 基金概览</h3>\n\n'
            
            if summary:
                html += '<div class="fund-summary">\n'
                html += '<ul class="list-unstyled">\n'
                for label, value in summary:
                    html += f'  <li><strong>{label}</strong>: {value}</li>\n'
                html += '</ul>\n'
                html += '</div>\n\n'
            else:
                html += '<p><em>暂无摘要数据</em></p>\n\n'
        
        if display_type in ["chart", "full"]:
            html += '<h3>📈 净值走势</h3>\n\n'
            chart_data = data_result["chart"]
            
            if chart_data:
                # 尝试生成图表图片
                try:
                    # 根据是否为PDF导出选择返回路径类型
                    image_path = generate_chart_image(chart_data, "fund", fund_code, return_absolute_path=for_pdf)
                    
                    if image_path:
                        # 插入图片到HTML
                        html += f'<div class="chart-container" style="text-align: center; margin: 20px 0;">\n'
                        html += f'  <img src="{image_path}" alt="净值走势图" style="max-width: 100%; height: auto; border: 1px solid #ddd; border-radius: 4px;" />\n'
                        html += f'</div>\n\n'
                    else:
                        # 如果图片生成失败，显示占位符
                        html += '<div class="chart-placeholder" style="background-color: #f8f9fa; border: 2px dashed #bdc3c7; padding: 40px; text-align: center; color: #7f8c8d; margin: 20px 0;">\n'
                        html += '<p>📈 [净值走势图]</p>\n'
                        html += f'<p>基金代码: {fund_code}</p>\n'
                        html += '</div>\n\n'
                except Exception:
                    # 如果有任何错误，显示占位符
                    html += '<div class="chart-placeholder" style="background-color: #f8f9fa; border: 2px dashed #bdc3c7; padding: 40px; text-align: center; color: #7f8c8d; margin: 20px 0;">\n'
                    html += '<p>📈 [净值走势图]</p>\n'
                    html += f'<p>基金代码: {fund_code}</p>\n'
                    html += '</div>\n\n'
                
                # 显示数据概览
                if chart_data:
                    first_trace = chart_data[0] if chart_data else {}
                    if 'x' in first_trace and 'y' in first_trace:
                        dates = first_trace['x']
                        values = first_trace['y']
                        if dates and values:
                            start_date = dates[0] if isinstance(dates[0], str) else dates[0].strftime('%Y-%m-%d')
                            end_date = dates[-1] if isinstance(dates[-1], str) else dates[-1].strftime('%Y-%m-%d')
                            start_value = values[0]
                            end_value = values[-1]
                            
                            html += '<div class="data-overview" style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin: 15px 0;">\n'
                            html += f'<p><strong>时间范围</strong>: {start_date} 至 {end_date}</p>\n'
                            html += f'<p><strong>起始净值</strong>: {start_value:.4f}</p>\n'
                            html += f'<p><strong>最新净值</strong>: {end_value:.4f}</p>\n'
                            
                            if start_value and end_value:
                                return_rate = (end_value - start_value) / start_value * 100
                                color = "green" if return_rate >= 0 else "red"
                                html += f'<p><strong>区间收益</strong>: <span style="color: {color}; font-weight: bold;">{return_rate:+.2f}%</span></p>\n'
                            html += '</div>\n\n'
                
                # 显示图表配置信息
                show_dividends = self.get_parameter_value("show_dividends", True)
                normalize_data = self.get_parameter_value("normalize_data", False)

                config_info = []
                config_info.append("图表类型: 折线图")
                if show_dividends:
                    config_info.append("包含分红标记")
                if normalize_data:
                    config_info.append("数据已归一化")
                
                if config_info:
                    html += f'<p><strong>图表配置</strong>: {", ".join(config_info)}</p>\n\n'
            else:
                html += '<p><em>暂无图表数据</em></p>\n\n'
        
        if display_type in ["table", "full"]:
            html += '<h3>📋 统计数据</h3>\n\n'
            extra_data = data_result["extra_data"]
            
            if extra_data:
                # 添加数据概览框
                html += '<div class="alert alert-info">\n'
                html += '<p><strong>📊 数据概览</strong>: 以下表格显示了基金的详细统计信息，包括基础指标、年度统计和季度统计等。</p>\n'
                html += '</div>\n\n'
                
                for i, table_data in enumerate(extra_data):
                    table_title = table_data.get('name', '数据表')
                    
                    # 检查是否有pd_data字段（新格式）
                    if 'pd_data' in table_data and not table_data['pd_data'].empty:
                        df = table_data['pd_data']
                        
                        # 使用HTML表格渲染
                        table_html = self._create_html_table(df, table_title)
                        html += table_html
                        
                        # 为重要表格添加解释说明
                        if '基础指标' in table_title:
                            html += '<div class="alert alert-light"><small>💡 <strong>说明</strong>: 基础指标显示了投资的核心收益和风险指标，帮助评估基金表现。</small></div>\n\n'
                        elif '年度统计' in table_title:
                            html += '<div class="alert alert-light"><small>📅 <strong>说明</strong>: 年度统计按年份展示收益表现，便于进行历史业绩比较。</small></div>\n\n'
                        elif '季度统计' in table_title:
                            html += '<div class="alert alert-light"><small>📈 <strong>说明</strong>: 季度统计提供更细粒度的业绩分析，有助于识别季节性表现模式。</small></div>\n\n'
                    
                    # 兼容旧格式（columns和data字段）
                    elif 'columns' in table_data and 'data' in table_data:
                        table_columns = table_data.get('columns', [])
                        table_rows = table_data.get('data', [])
                        
                        if table_columns and table_rows:
                            html += f'<h4>📊 {table_title}</h4>\n\n'
                            
                            # 创建HTML表格
                            html += '<table class="table table-striped table-bordered" style="margin: 20px 0;">\n'
                            
                            # 表头
                            html += '  <thead class="table-dark">\n'
                            html += '    <tr>\n'
                            headers = [col.get('name', '') for col in table_columns]
                            for header in headers:
                                html += f'      <th style="text-align: center; padding: 12px;">{header}</th>\n'
                            html += '    </tr>\n'
                            html += '  </thead>\n'
                            
                            # 表体
                            html += '  <tbody>\n'
                            display_rows = table_rows[:10]
                            for row in display_rows:
                                html += '    <tr>\n'
                                for col in table_columns:
                                    col_id = col.get('id', '')
                                    value = row.get(col_id, '')
                                    formatted_value = self._format_table_value(value, col_id)
                                    html += f'      <td style="text-align: center; padding: 8px;">{formatted_value}</td>\n'
                                html += '    </tr>\n'
                            
                            if len(table_rows) > 10:
                                html += '    <tr>\n'
                                summary_cols = ["..."] * (len(headers) - 1) + [f"<em>共 {len(table_rows)} 行</em>"]
                                for col in summary_cols:
                                    html += f'      <td style="text-align: center; padding: 8px; font-style: italic;">{col}</td>\n'
                                html += '    </tr>\n'
                            
                            html += '  </tbody>\n'
                            html += '</table>\n\n'
                        else:
                            html += f'<h4>📊 {table_title}</h4>\n<p><em>暂无表格数据</em></p>\n\n'
                    else:
                        html += f'<h4>📊 {table_title}</h4>\n<p><em>暂无表格数据</em></p>\n\n'
                
                # 添加数据汇总信息
                html += '<hr style="margin: 30px 0;">\n'
                html += f'<p><strong>📋 统计汇总</strong>: 共展示了 {len(extra_data)} 个数据表格</p>\n\n'
            else:
                html += '<div class="alert alert-info"><strong>ℹ️ 提示</strong>: 没有可用的统计数据。</div>\n\n'
        
        # 添加数据源信息
        html += '<hr style="margin: 30px 0;">\n'
        html += '<p><small><em>数据来源: baofu基金数据库</em></small></p>\n'
        
        return html
    
    def _create_html_table(self, df: pd.DataFrame, title: str) -> str:
        """创建HTML表格"""
        if df.empty:
            return f'<h4>📊 {title}</h4>\n<p><em>暂无数据</em></p>\n\n'
        
        html = f'<h4>📊 {title}</h4>\n\n'
        
        # 创建HTML表格
        html += '<table class="table table-striped table-bordered" style="margin: 20px 0; font-size: 14px;">\n'
        
        # 表头
        html += '  <thead style="background-color: #1a5490; color: white;">\n'
        html += '    <tr>\n'
        headers = df.columns.tolist()
        
        # 表头图标映射
        header_icons = {
            '指标': '📋', '数值': '💰', '年份': '📅', '季度': '📈',
            '收益率': '📊', '年化收益率': '📈', '最大回撤': '📉',
            '波动率': '📊', '年化波动率': '🌊'
        }
        
        for header in headers:
            icon = header_icons.get(header, '📋')
            html += f'      <th style="text-align: center; padding: 12px; font-weight: bold;">{icon} {header}</th>\n'
        html += '    </tr>\n'
        html += '  </thead>\n'
        
        # 表体
        html += '  <tbody>\n'
        display_rows = df.head(10)
        for row_idx, (_, row) in enumerate(display_rows.iterrows()):
            # 交替行颜色
            bg_color = "#f8f9fa" if row_idx % 2 == 0 else "#ffffff"
            html += f'    <tr style="background-color: {bg_color};">\n'
            
            for col_name, value in zip(headers, row):
                formatted_value = self._format_table_value(value, col_name)
                
                # 为重要数值添加颜色和样式
                cell_style = "text-align: center; padding: 8px;"
                
                if isinstance(value, (int, float)) and not pd.isna(value):
                    # 百分比数值的颜色标识
                    if '率' in col_name or '收益' in col_name or '回撤' in col_name:
                        if value > 0:
                            cell_style += " color: green; font-weight: bold;"
                            formatted_value = f"🟢 {formatted_value}"
                        elif value < 0:
                            cell_style += " color: red; font-weight: bold;"
                            formatted_value = f"🔴 {formatted_value}"
                        else:
                            formatted_value = f"⚫ {formatted_value}"
                    # 净值等重要数值加粗
                    elif '净值' in col_name or '价格' in col_name:
                        cell_style += " font-weight: bold;"
                
                # 处理字符串中已包含百分比和符号的情况
                elif isinstance(value, str) and '%' in str(value):
                    if '+' in str(value):
                        cell_style += " color: green; font-weight: bold;"
                        formatted_value = f"🟢 {formatted_value}"
                    elif '-' in str(value) and '回撤' not in str(value):
                        cell_style += " color: red; font-weight: bold;"
                        formatted_value = f"🔴 {formatted_value}"
                
                html += f'      <td style="{cell_style}">{formatted_value}</td>\n'
            html += '    </tr>\n'
        
        # 如果有更多数据，添加汇总行
        if len(df) > 10:
            html += '    <tr style="background-color: #e9ecef; font-style: italic;">\n'
            summary_data = ["⋯"] * (len(headers) - 1) + [f"📊 <em>共 {len(df)} 行数据</em>"]
            for cell in summary_data:
                html += f'      <td style="text-align: center; padding: 8px;">{cell}</td>\n'
            html += '    </tr>\n'
        
        html += '  </tbody>\n'
        html += '</table>\n\n'
        
        return html