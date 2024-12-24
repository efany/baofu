from urllib.parse import urlparse
import requests
from urllib.robotparser import RobotFileParser
from typing import Optional

def is_valid_url(url: str) -> bool:
    """Check if the URL is valid and uses HTTP/HTTPS protocol."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
    except ValueError:
        return False

def get_robots_parser(base_url: str) -> Optional[RobotFileParser]:
    """Get and parse robots.txt for the given domain."""
    try:
        parsed_url = urlparse(base_url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        
        parser = RobotFileParser(robots_url)
        parser.read()
        return parser
    except Exception as e:
        print(f"Error reading robots.txt: {str(e)}")
        return None 