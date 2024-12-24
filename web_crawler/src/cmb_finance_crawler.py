import requests
from bs4 import BeautifulSoup
from typing import Set, List, Dict
import time
from utils import is_valid_url, get_robots_parser
from openpyxl import Workbook
from datetime import datetime
import os

class CMBFinanceCrawler:
    """
    A crawler for fetching financial product data from CMB (招商银行理财).
    
    This crawler can:
    1. Fetch financial product NAV history data
    2. Support multi-page crawling
    3. Export data to Excel file
    4. Respect robots.txt and implement polite crawling
    
    Data fields include:
    - product_code: 产品代码
    - product_name: 产品名称
    - nav: 单位净值
    - acc_nav: 累计净值
    - nav_date: 净值日期
    
    Usage:
        crawler = CMBFinanceCrawler(
            product_code="JY020237",    # 产品代码
            start_page=1,               # 起始页码
            total_pages=2,              # 要爬取的总页数
            delay=1.0                   # 请求间隔(秒)
        )
        crawler.crawl_all_pages()       # 爬取所有页面
        data = crawler.get_finance_data()  # 获取爬取的数据
        crawler.export_to_excel()       # 导出到Excel
    """

    def __init__(self, product_code: str, start_page: int = 1, total_pages: int = 1, delay: float = 1.0):
        """
        Initialize the CMB finance product crawler
        
        Args:
            product_code: The product code to crawl (e.g., "JY020237")
            start_page: The page number to start with (default: 1)
            total_pages: Total number of pages to crawl (default: 1)
            delay: Delay between requests in seconds (default: 1.0)
        """
        self.product_code = product_code
        self.current_page = start_page
        self.total_pages = total_pages
        self.delay = delay
        self.base_url = self._build_url()
        self.visited_urls: Set[str] = set()
        self.robots_parser = get_robots_parser(self.base_url)
        self.finance_data: List[Dict[str, str]] = []

    def _build_url(self) -> str:
        """Build the URL with the current parameters."""
        base = f"https://www.cmbchina.com/cfweb/Personal/saproductdetail.aspx?saaCod=D07&funCod={self.product_code}&type=prodvalue"
        if self.current_page > 1:
            base += f"&PageNo={self.current_page}"
        return base

    def crawl_all_pages(self) -> None:
        """
        Crawl all specified pages of finance data.
        When total_pages is -1, it will automatically crawl until no more data is found.
        """
        page = self.current_page
        initial_data_count = len(self.finance_data)
        
        while True:
            print(f"\nCrawling page {page} / {self.total_pages}...")
            self.current_page = page
            url = self._build_url()
            self._crawl_url(url)
            
            # 检查是否需要继续爬取
            if self.total_pages == -1:
                # 如果数据量没有增加，说明已经没有新数据了
                if len(self.finance_data) == initial_data_count:
                    print(f"\nNo more data found after page {page-1}")
                    break
                initial_data_count = len(self.finance_data)
            else:
                # 如果指定了页数，到达后就停止
                if self.current_page >= self.total_pages:
                    print(f"\nReached the specified total pages: {self.total_pages}")   
                    break
                
            page += 1
            time.sleep(self.delay)

    def get_finance_data(self) -> List[Dict[str, str]]:
        """Return the collected finance data."""
        return self.finance_data

    def _crawl_url(self, url: str) -> None:
        """Crawl a specific URL and extract information."""
        if not is_valid_url(url) or url in self.visited_urls:
            return

        if self.robots_parser and not self.robots_parser.can_fetch("*", url):
            print(f"Robots.txt disallows accessing {url}")
            return

        try:
            print(f"Crawling: {url}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Referer': 'https://www.cmbchina.com/'
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = response.apparent_encoding
            response.raise_for_status()
            
            self.visited_urls.add(url)
            self._process_page(response.text)
            time.sleep(self.delay)

        except requests.RequestException as e:
            print(f"Error crawling {url}: {str(e)}")
        except UnicodeEncodeError as e:
            print(f"Encoding error for {url}: {str(e)}")

    def _process_page(self, html: str) -> None:
        """Extract and process information from the page."""
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        if not table:
            print("No data table found")
            return

        # Process data rows
        rows = table.find_all('tr')[1:]  # Skip header row
        data_found = False
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 4:
                data = {
                    'product_code': cells[0].get_text(strip=True),
                    'product_name': cells[1].get_text(strip=True),
                    'nav': cells[2].get_text(strip=True),
                    'acc_nav': cells[3].get_text(strip=True),
                    'nav_date': cells[4].get_text(strip=True) if len(cells) > 4 else ''
                }
                # 检查数据是否有效（不为空）
                if any(data.values()):
                    self.finance_data.append(data)
                    data_found = True
        
        if data_found:
            print(f"Extracted {len(rows)} records from current page")
        else:
            print("No valid data found in current page")

    def export_to_excel(self, output_dir: str = "output") -> str:
        """
        Export finance data to Excel file
        :param output_dir: Directory to save the Excel file
        :return: Path to the created Excel file
        """
        if not self.finance_data:
            print("No data to export")
            return ""

        os.makedirs(output_dir, exist_ok=True)
        wb = Workbook()
        ws = wb.active
        ws.title = f"Finance_{self.product_code}"

        # Write headers
        headers = ["产品代码", "产品名称", "单位净值", "累计净值", "净值日期"]
        for col, header in enumerate(headers, 1):
            ws.cell(row=1, column=col, value=header)

        # Write data
        for row, data in enumerate(self.finance_data, 2):
            ws.cell(row=row, column=1, value=data['product_code'])
            ws.cell(row=row, column=2, value=data['product_name'])
            ws.cell(row=row, column=3, value=float(data['nav']))
            ws.cell(row=row, column=4, value=float(data['acc_nav']))
            ws.cell(row=row, column=5, value=data['nav_date'])

        # Auto-adjust column width
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 2)
            ws.column_dimensions[column_letter].width = adjusted_width

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"finance_{self.product_code}_{timestamp}.xlsx"
        filepath = os.path.join(output_dir, filename)

        wb.save(filepath)
        print(f"\nData exported to: {filepath}")
        return filepath 