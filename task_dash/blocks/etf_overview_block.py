"""
ETF概览块实现
以紧凑的表格形式展示多个ETF的关键数据，每行显示4个ETF
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from .base_block import BaseBlock, BlockParameter
import sys
import os
import logging
from database.db_stocks import DBStocks
from task_dash.utils import get_data_briefs

# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 添加项目路径以便导入数据生成器
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))


class EtfOverviewBlock(BaseBlock):
    """ETF概览块"""
    
    def __init__(self, block_data: dict, mysql_db=None):
        """初始化ETF概览块
        
        Args:
            block_data: 块数据
            mysql_db: MySQL数据库实例，如果为None则在使用时会报错
        """
        logger.info(f"初始化ETF概览块 - block_data keys: {list(block_data.keys()) if block_data else 'None'}")
        logger.info(f"数据库连接状态: {'已连接' if mysql_db else '未连接'}")
        super().__init__(block_data)
        self.mysql_db = mysql_db
    
    @property
    def block_name(self) -> str:
        return "ETF概览"
    
    @property
    def block_icon(self) -> str:
        return "💼"
    
    @property
    def block_description(self) -> str:
        return "以紧凑表格形式展示多个ETF的关键数据概览，每行显示4个ETF"
    
    def _get_etf_options(self) -> List[dict]:
        """获取ETF选项列表"""
        logger.debug("开始获取ETF选项列表")
        
        if not self.mysql_db:
            logger.warning("数据库连接未初始化，返回空选项列表")
            return []
        
        try:
            # 使用注入的数据库连接
            db_stocks = DBStocks(self.mysql_db)
            stocks_df = db_stocks.get_all_stocks()
            logger.debug(f"从数据库获取到 {len(stocks_df)} 条股票数据")
            
            if stocks_df.empty:
                logger.error("数据库中没有股票数据")
                raise ValueError("数据库中没有股票数据")
            
            # 筛选出ETF（包含'ETF'的条目）
            etf_df = stocks_df[stocks_df['name'].str.contains('ETF', na=False, case=False)]
            logger.debug(f"筛选出 {len(etf_df)} 个ETF")
            
            if etf_df.empty:
                logger.error("数据库中没有ETF数据")
                raise ValueError("数据库中没有ETF数据")
            
            # 使用get_data_briefs生成选项
            etf_options = get_data_briefs("stock", etf_df)
            logger.debug(f"生成 {len(etf_options)} 个ETF选项")
            
            # 按代码排序
            etf_options.sort(key=lambda x: x["value"])
            logger.debug("ETF选项排序完成")
            return etf_options
            
        except Exception as e:
            logger.error(f"获取ETF选项时发生异常: {str(e)}", exc_info=True)
            raise
    
    @property
    def parameters(self) -> List[BlockParameter]:
        logger.debug("开始获取块参数")
        try:
            etf_options = self._get_etf_options()
            logger.debug(f"成功获取 {len(etf_options)} 个ETF选项")
        except Exception as e:
            logger.error(f"获取ETF选项失败: {str(e)}")
            etf_options = []
        
        return [
            BlockParameter(
                name="selected_etfs",
                label="选择ETF",
                param_type="multiselect",
                default_value=["159949.SZ", "512550.SS", "159633.SZ", "159628.SZ"],
                required=True,
                options=etf_options,
                description="选择要显示的ETF列表，支持多选",
                placeholder="请选择ETF"
            ),
            BlockParameter(
                name="time_period",
                label="统计周期",
                param_type="select",
                default_value="1m",
                required=True,
                options=[
                    {"label": "近1天", "value": "1d"},
                    {"label": "近1周", "value": "1w"},
                    {"label": "近1个月", "value": "1m"},
                    {"label": "近3个月", "value": "3m"},
                    {"label": "近6个月", "value": "6m"},
                    {"label": "近1年", "value": "1y"},
                    {"label": "今年至今", "value": "ytd"},
                    {"label": "全部数据", "value": "all"}
                ],
                description="选择数据统计的时间范围"
            ),
        ]
    
    def _get_last_valid_date(self, etf_code: str) -> date:
        """获取指定ETF的最后有效日期"""
        if not self.mysql_db:
            raise ValueError("数据库连接未初始化，无法获取ETF最后有效日期")
        
        from database.db_stocks_day_hist import DBStocksDayHist
        
        db_stocks_day_hist = DBStocksDayHist(self.mysql_db)
        latest_date_str = db_stocks_day_hist.get_latest_hist_date(etf_code)
        
        if not latest_date_str:
            raise ValueError(f"ETF {etf_code} 没有历史数据")
        
        return datetime.strptime(latest_date_str, '%Y-%m-%d').date()
    
    def _calculate_period_dates(self, etf_code: str = None) -> tuple:
        """根据时间周期计算开始和结束日期"""
        time_period = self.get_parameter_value("time_period", "1m")
        
        # 获取最后有效日期作为end_date
        if etf_code:
            end_date = self._get_last_valid_date(etf_code)
        else:
            end_date = date.today()
        
        if time_period == "1d":
            start_date = end_date - timedelta(days=1)
        elif time_period == "1w":
            start_date = end_date - timedelta(days=7)
        elif time_period == "1m":
            start_date = end_date - timedelta(days=30)
        elif time_period == "3m":
            start_date = end_date - timedelta(days=90)
        elif time_period == "6m":
            start_date = end_date - timedelta(days=180)
        elif time_period == "1y":
            start_date = end_date - timedelta(days=365)
        elif time_period == "ytd":
            start_date = date(end_date.year, 1, 1)
        else:  # "all"
            start_date = None
        
        return start_date, end_date
    
    def _get_trend_type(self, return_rate_str: str) -> str:
        """根据收益率字符串判断涨跌趋势"""
        if not return_rate_str or return_rate_str == "无数据":
            return "neutral"
        
        try:
            if "%" in return_rate_str:
                rate_num = float(return_rate_str.replace("%", "").replace("+", ""))
                if rate_num > 0:
                    return "up"
                elif rate_num < 0:
                    return "down"
                else:
                    return "neutral"
        except:
            pass
        
        return "neutral"
    
    def _get_etf_data(self, etf_code: str) -> Dict[str, Any]:
        """获取单个ETF的数据"""
        logger.debug(f"开始获取ETF数据: {etf_code}")
        
        if not self.mysql_db:
            logger.error(f"ETF {etf_code}: 数据库连接未初始化")
            return {"error": "数据库连接未初始化"}
        
        try:
            start_date, end_date = self._calculate_period_dates(etf_code)
            logger.debug(f"ETF {etf_code}: 计算日期范围 {start_date} 到 {end_date}")
            
            from task_dash.datas.stock_data_generator import StockDataGenerator
            from task_dash.utils import get_stock_name
            
            logger.debug(f"ETF {etf_code}: 创建数据生成器")
            generator = StockDataGenerator(
                stock_code=etf_code,
                mysql_db=self.mysql_db,
                start_date=start_date,
                end_date=end_date
            )
            
            logger.debug(f"ETF {etf_code}: 开始加载数据")
            success = generator.load()
            if not success:
                logger.warning(f"ETF {etf_code}: 数据加载失败")
                return {"error": f"无法加载ETF {etf_code} 的数据"}
            
            logger.debug(f"ETF {etf_code}: 数据加载成功，获取摘要数据")
            # 获取摘要数据
            summary_data = generator.get_summary_data()
            
            # 将摘要数据转换为字典格式
            summary_dict = {item[0]: item[1] for item in summary_data}
            logger.debug(f"ETF {etf_code}: 摘要数据转换完成，字段: {list(summary_dict.keys())}")
            
            # 获取ETF名称
            etf_name = get_stock_name(etf_code)
            logger.debug(f"ETF {etf_code}: 获取到名称: {etf_name}")
            
            # 判断涨跌趋势
            return_rate = summary_dict.get("区间收益率", "无数据")
            trend_type = self._get_trend_type(return_rate)
            logger.debug(f"ETF {etf_code}: 收益率 {return_rate}, 趋势 {trend_type}")
            
            result = {
                "success": True,
                "code": etf_code,
                "name": etf_name,
                "summary": summary_dict,
                "trend": trend_type
            }
            logger.debug(f"ETF {etf_code}: 数据获取完成")
            return result
            
        except Exception as e:
            logger.error(f"ETF {etf_code}: 数据获取异常: {str(e)}", exc_info=True)
            return {"error": f"数据获取失败: {str(e)}"}
    
    def render_to_html(self, for_pdf: bool = False) -> str:
        """渲染为HTML"""
        logger.info(f"开始渲染ETF概览块 - for_pdf: {for_pdf}")
        
        # 在渲染阶段进行严格验证
        if not self.mysql_db:
            logger.error("数据库连接未初始化，无法渲染ETF概览")
            raise ValueError("数据库连接未初始化，无法渲染ETF概览")
        
        html = ""
        
        # 获取选中的ETF列表
        selected_etfs = self.get_parameter_value("selected_etfs", [])
        logger.info(f"选中的ETF列表: {selected_etfs}")
        
        if not selected_etfs:
            logger.warning("没有选择ETF，返回警告消息")
            html += '<div style="color: #666; text-align: center; padding: 20px;">请选择至少一个ETF</div>'
            return html
        
        time_period = self.get_parameter_value("time_period", "1m")
        logger.info(f"时间周期: {time_period}")
        
        # 时间周期标签映射
        time_labels = {
            "1d": "近1天", "1w": "近1周", "1m": "近1个月", 
            "3m": "近3个月", "6m": "近6个月", "1y": "近1年", 
            "ytd": "今年至今", "all": "全部数据"
        }
        period_label = time_labels.get(time_period, time_period)
        
        # 4列紧凑表格布局
        logger.debug("开始构建HTML表格")
        html += f'''<div style="margin: 20px auto; max-width: 1200px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
    <div style="background: #f8f9fa; padding: 16px; border-radius: 6px; margin-bottom: 16px; text-align: center;">
        <h3 style="margin: 0; color: #495057; font-size: 18px; font-weight: 500;">ETF 概览</h3>
        <p style="margin: 8px 0 0 0; color: #6c757d; font-size: 14px;">统计周期: {period_label}</p>
    </div>
    <table style="width: 100%; border-collapse: collapse; border: 1px solid #dee2e6; border-radius: 6px; overflow: hidden;">'''
        
        # 每行显示4个ETF
        etfs_per_row = 4
        logger.info(f"将渲染 {len(selected_etfs)} 个ETF，每行 {etfs_per_row} 个")
        
        for i in range(0, len(selected_etfs), etfs_per_row):
            row_etfs = selected_etfs[i:i + etfs_per_row]
            logger.debug(f"处理第 {i//etfs_per_row + 1} 行ETF: {row_etfs}")
            
            # 获取所有ETF数据
            etf_data_list = []
            for etf_code in row_etfs:
                etf_data = self._get_etf_data(etf_code)
                etf_data_list.append(etf_data)
            
            # 渲染一行ETF
            html += '        <tr>\n'
            
            for j, etf_code in enumerate(row_etfs):
                etf_data = etf_data_list[j]
                
                if "error" in etf_data:
                    try:
                        from task_dash.utils import get_stock_name
                        etf_name = get_stock_name(etf_code)
                    except:
                        etf_name = etf_code
                    
                    html += f'''            <td style="width: 25%; padding: 16px; border: 1px solid #dee2e6; background: #f8f9fa; vertical-align: top;">
                <div style="text-align: center;">
                    <div style="font-weight: 600; color: #495057; margin-bottom: 4px; font-size: 14px;">{etf_name}</div>
                    <div style="color: #6c757d; font-size: 11px; margin-bottom: 8px;">{etf_code}</div>
                    <div style="color: #dc3545; font-weight: 500; font-size: 12px;">数据获取失败</div>
                </div>
            </td>'''
                else:
                    summary = etf_data.get("summary", {})
                    trend = etf_data.get("trend", "neutral")
                    return_rate = summary.get("区间收益率", "无数据")
                    price_change = summary.get("价格变化", "—")
                    
                    # 简洁的颜色方案
                    if trend == "up":
                        return_color = "#28a745"
                        bg_color = "#f8fff8"
                    elif trend == "down":
                        return_color = "#dc3545"
                        bg_color = "#fff8f8"
                    else:
                        return_color = "#6c757d"
                        bg_color = "#ffffff"
                    
                    try:
                        from task_dash.utils import get_stock_name
                        etf_name = get_stock_name(etf_code)
                    except:
                        etf_name = etf_code
                    
                    html += f'''            <td style="width: 25%; padding: 16px; border: 1px solid #dee2e6; background: {bg_color}; vertical-align: top;">
                <div style="text-align: center;">
                    <div style="font-weight: 600; color: #495057; margin-bottom: 4px; font-size: 14px;">{etf_name}</div>
                    <div style="color: #6c757d; font-size: 11px; margin-bottom: 8px;">{etf_code}</div>
                    <div style="color: {return_color}; font-weight: 700; font-size: 16px; margin-bottom: 4px;">{return_rate}</div>
                    <div style="color: #6c757d; font-size: 11px; margin-bottom: 8px;">{price_change}</div>'''
                    
                    # 如果不是近一天，显示高低点信息
                    if time_period != "1d":
                        period_high = summary.get("期间最高", "—")
                        period_low = summary.get("期间最低", "—")
                        html += f'''                    <div style="border-top: 1px solid #e9ecef; padding-top: 8px; font-size: 10px; color: #6c757d;">
                        <div style="margin-bottom: 2px;">最高: {period_high}</div>
                        <div>最低: {period_low}</div>
                    </div>'''
                    
                    html += '''                </div>
            </td>'''
            
            # 填充空列
            for _ in range(etfs_per_row - len(row_etfs)):
                html += '            <td style="width: 25%; border: 1px solid #dee2e6; background: #fafafa;"></td>'
            
            html += '\n        </tr>\n'
        
        html += f'''    </table>
    <div style="text-align: center; margin-top: 16px; color: #6c757d; font-size: 12px;">
        共展示 {len(selected_etfs)} 个ETF
    </div>
</div>'''
        
        logger.info("ETF概览块HTML渲染完成")
        logger.debug(f"生成的HTML长度: {len(html)} 字符")
        return html