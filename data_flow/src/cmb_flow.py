import sys
import os
import time
import json
from typing import List, Dict
from datetime import datetime
from openpyxl import Workbook

from loguru import logger

# Add src directories to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'web_crawler', 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'data_process', 'src'))

from cmb_finance_crawler import CMBFinanceCrawler
from fund_nav_processor import FundNavProcessor
from process_utils import export_analysis_to_excel

class CMBDataFlow:
    def __init__(self, 
                 output_dir: str = "output/cmb",
                 cache_dir: str = "cache/cmb",
                 blacklist_config: str = "config/cmb_blacklist.json"):
        """
        初始化数据流处理器
        
        Args:
            output_dir: 输出目录
            cache_dir: 缓存目录
            blacklist_config: 黑名单配置文件路径
        """
        self.output_dir = output_dir
        self.cache_dir = cache_dir
        self.blacklist_config = self._load_blacklist_config(blacklist_config)
        self.crawler = CMBFinanceCrawler(delay=1.0, cache_dir=cache_dir)
        
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
    def _load_blacklist_config(self, config_path: str) -> Dict:
        """
        加载黑名单配置
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            Dict: 黑名单配置
        """
        try:
            if not os.path.exists(config_path):
                logger.warning(f"黑名单配置文件不存在: {config_path}")
                return {"product_blacklist": {"enabled": False, "rules": []}}
                
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            logger.info(f"已加载黑名单配置: {len(config.get('product_blacklist', {}).get('rules', []))} 条规则")
            return config
        except Exception as e:
            logger.error(f"加载黑名单配置失败: {str(e)}")
            return {"product_blacklist": {"enabled": False, "rules": []}}
            
    def _is_special_focus(self, product: Dict) -> bool:
        """
        检查产品是否在特别关注列表中
        
        Args:
            product: 产品信息字典
            
        Returns:
            bool: 是否在特别关注列表中
        """
        if not self.blacklist_config.get('special_focus', {}).get('enabled', False):
            return False
            
        rules = self.blacklist_config.get('special_focus', {}).get('rules', [])
        
        for rule in rules:
            field = rule.get('field')
            values = rule.get('values', [])
            
            if not field or not values:
                continue
                
            if field in product and product[field] in values:
                logger.debug(f"产品 {product.get('PrdCode')} 命中特别关注规则: {field}={product[field]}")
                return True
                
        return False
        
    def _is_product_blacklisted(self, product: Dict) -> bool:
        """
        检查产品是否在黑名单中
        
        Args:
            product: 产品信息字典
            
        Returns:
            bool: 是否在黑名单中
        """
        # 如果产品在特别关注列表中，直接返回False（不过滤）
        if self._is_special_focus(product):
            return False
            
        if not self.blacklist_config.get('product_blacklist', {}).get('enabled', False):
            return False
            
        rules = self.blacklist_config.get('product_blacklist', {}).get('rules', [])
        
        for rule in rules:
            field = rule.get('field')
            if not field:
                continue
            if field == "LastNavDays":
                if int(product["last_nav_days"]) > int(rule.get('value', 30)):
                    logger.debug(f"产品 {product.get('PrdCode')} 命中黑名单规则: {field}={product.get('last_nav_days')}")
                    return True
                continue
            elif field == "PrdCode" or field == "Style" or field == "Risk":
                values = rule.get('values', [])
                if field in product and product[field] in values:
                    logger.debug(f"产品 {product.get('PrdCode')} 命中黑名单规则: {field}={product[field]}")
                    return True
                
        return False
        
    def process_products(self) -> List[Dict]:
        """
        处理所有产品数据
        
        Returns:
            List[Dict]: 过滤后的产品列表
        """
        # 获取所有产品系列
        series = self.crawler.get_product_series()
        logger.info(f"Found {len(series)} series")
        time.sleep(1.0)
        
        # 获取并过滤产品
        all_products = []
        filtered_products = []
        special_focus_products = []
        
        for series_info in series:
            products = self.crawler.get_serie_products(series_info['series_code'])
            logger.info(f"Found {len(products)} products in series {series_info['series_code']}")
            
            # 应用过滤规则
            for product in products:
                if self._is_special_focus(product):
                    special_focus_products.append(product)
                    filtered_products.append(product)
                    logger.info(f"产品 {product.get('PrdCode')} - {product.get('PrdName')} 被特别关注")
                elif not self._is_product_blacklisted(product):
                    filtered_products.append(product)
                else:
                    logger.info(f"产品 {product.get('PrdCode')} - {product.get('PrdName')} 被过滤（黑名单）")
                    
            all_products.extend(products)
            time.sleep(1.0)

        # 输出过滤统计
        logger.info(f"总产品数: {len(all_products)}")
        logger.info(f"特别关注产品数: {len(special_focus_products)}")
        logger.info(f"过滤后产品数: {len(filtered_products)}")
        logger.info(f"过滤掉的产品数: {len(all_products) - len(filtered_products)}")
        
        # 导出过滤后的产品列表
        self.crawler.export_product_list(filtered_products, self.output_dir, "filtered_products")
        
        # 导出特别关注的产品列表
        if special_focus_products:
            self.crawler.export_product_list(special_focus_products, self.output_dir, "special_focus_products")

        for product in filtered_products:
            product_info = self.crawler.crawl_product_nav(product)
            product["nav_data"] = product_info['nav_data']
            product["info_data"] = product_info['info_data']

            # 处理净值数据
            nav_processor = FundNavProcessor(product_info['nav_data'])
            nav_analysis = nav_processor.process_nav_data()
            product["yearly_returns"] = nav_analysis['yearly_returns']
            product["quarterly_returns"] = nav_analysis['quarterly_returns']
            product["period_returns"] = nav_analysis['period_returns']

            # 导出分析结果
            export_analysis_to_excel(product["PrdCode"], product, self.output_dir)
        
        return filtered_products
    
    def get_period_return(self, fund_data, period_name):
        for period in fund_data['period_returns']:
            if period['period'] == period_name:
                return float(period['reinvest_annualized'])
        logger.warning(f"基金 {fund_data['TypeCode']} 没有{period_name}年再投资收益率")
        return -100.0
    
    
    def get_period_max_drawdown(self, fund_data, period_name):
        for period in fund_data['period_returns']:
            if period['period'] == period_name:
                return float(period['max_drawdown'])
        logger.warning(f"基金 {fund_data['TypeCode']} 没有{period_name}年最大回撤")
        return -100.0

    def export_filtered_results(self, products:List[Dict]) -> None:
        """将产品列表导出到Excel"""
        # try:
        # 确保缓存目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 生成文件名，使用原始日期配置
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
        filename = f"cmb_products_{timestamp}.xlsx"
        filepath = os.path.join(self.output_dir, filename)
                
        # 将数据导出到Excel
        # 创建工作簿
        wb = Workbook()

        data_sheet = wb.active
        data_sheet.title = "products"

        # 写入表头
        headers = ["TypeCode", "TypeName", "PrdCode", "PrdName", "LastNavDate", "LastNavDays", "Risk", "Style", "Currency", "Term", 
                    "1y再投资收益率", "3y再投资收益率", "5y再投资收益率", "1y最大回撤", "3y最大回撤", "5y最大回撤"]
        data_sheet.append(headers)

        # 写入数据
        for product in products:

            data_sheet.append([
                product['TypeCode'],
                product['TypeName'],
                product['PrdCode'],
                product['PrdName'],
                product['last_nav_date'],
                product['last_nav_days'],
                product['Risk'],
                product['Style'],
                product['Currency'],
                product['Term'],
                self.get_period_return(product, '1y'),
                self.get_period_return(product, '3y'),
                self.get_period_return(product, '5y'),
                self.get_period_max_drawdown(product, '1y'),
                self.get_period_max_drawdown(product, '3y'),
                self.get_period_max_drawdown(product, '5y'),
            ])

        # 保存工作簿
        wb.save(filepath)
        logger.info(f"Data exported to: {filepath}")
        return filepath
        # except Exception as e:
        #     logger.error(f"导出数据失败: {str(e)}")
        #     return ""

def __init_logger(log_dir):

    logger.add(os.path.join(log_dir, "app.log"), rotation="100 KB")
    logger.info(f"日志目录设置为: {log_dir}")

def main():
    # 处理命令行参数
    output_dir = "output/cmb"  # 默认输出目录
    log_dir = "logs"  # 默认日志目录
    blacklist_config = "config/cmb_blacklist.json"  # 默认黑名单配置文件
    
    if len(sys.argv) > 1:
        try:
            dir_index = sys.argv.index("--output_dir")
            output_dir = sys.argv[dir_index + 1]

            log_dir_index = sys.argv.index("--log_dir")
            log_dir = sys.argv[log_dir_index + 1]
            
            blacklist_index = sys.argv.index("--blacklist")
            blacklist_config = sys.argv[blacklist_index + 1]
        except (ValueError, IndexError):
            pass
            
    # 初始化日志
    __init_logger(log_dir)
    logger.info(f"输出目录: {output_dir}")
    logger.info(f"黑名单配置: {blacklist_config}")
    
    # 创建数据流处理器
    flow = CMBDataFlow(
        output_dir=output_dir,
        cache_dir="cache/cmb",
        blacklist_config=blacklist_config
    )
    
    # 处理产品数据
    filtered_products = flow.process_products()

    # 导出过滤后的结果
    flow.export_filtered_results(filtered_products)

    logger.info(f"处理完成，过滤后共 {len(filtered_products)} 个产品")

if __name__ == "__main__":
    main() 