"""
Async Web Scraper for Fiyat Takip v8.0
Config-driven selectors, structured error logging, non-blocking
"""
import re
import json
import asyncio
import random
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse
from playwright.async_api import async_playwright, Page, BrowserContext, Route, Request

logger = logging.getLogger("FiyatTakip.Scraper")

# Load configuration from external file
CONFIG_PATH = Path(__file__).parent / "selectors.json"

def load_selectors() -> Dict:
    """Load selectors from JSON configuration file."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
            logger.info(f"✅ Selector config loaded from {CONFIG_PATH}")
            return config
    except FileNotFoundError:
        logger.error(f"❌ Config file not found: {CONFIG_PATH}", exc_info=True)
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON in config file: {e}", exc_info=True)
        return {}

# Load config at module level
_CONFIG = load_selectors()
SELECTOR_MAP = {k: v for k, v in _CONFIG.items() if k not in ("product_url_patterns", "pagination_params")}
PRODUCT_URL_PATTERNS = {k: re.compile(v) for k, v in _CONFIG.get("product_url_patterns", {}).items()}
PAGINATION_PARAMS = _CONFIG.get("pagination_params", {})

# Constants
MAX_CONCURRENCY = 20
PAGE_TIMEOUT = 15000
DEFAULT_IMAGE_URL = "https://via.placeholder.com/200x200?text=Resim+Yok"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]


@dataclass
class ScrapedData:
    url: str
    name: str = ""
    price: float = 0.0
    image_url: str = ""
    source: str = "unknown"
    is_valid: bool = False
    error_message: str = ""


class ValidationService:
    """Validates scraped data quality."""
    INVALID_NAME_PATTERNS = [r'^https?://', r'^www\.', r'^Bilinmeyen', r'^Unknown', r'^Hata$', r'^N/A$', r'^\.com']
    
    @classmethod
    def validate(cls, data: ScrapedData) -> ScrapedData:
        errors = []
        if data.price <= 0:
            errors.append("Fiyat bulunamadı")
        if not data.name or len(data.name.strip()) < 3:
            errors.append("Ürün ismi yok")
        else:
            for pattern in cls.INVALID_NAME_PATTERNS:
                if re.match(pattern, data.name, re.IGNORECASE):
                    errors.append(f"Geçersiz isim: {data.name[:30]}")
                    break
        if not data.image_url:
            data.image_url = DEFAULT_IMAGE_URL
        if errors:
            data.is_valid = False
            data.error_message = "; ".join(errors)
        else:
            data.is_valid = True
            data.error_message = ""
        return data


class URLAnalyzer:
    """Analyzes URLs to determine type and domain."""
    
    @classmethod
    def get_domain(cls, url: str) -> Optional[str]:
        for domain in SELECTOR_MAP.keys():
            if domain in url:
                return domain
        return None
    
    @classmethod
    def analyze(cls, url: str) -> Tuple[str, str]:
        domain = cls.get_domain(url)
        if not domain:
            return ("unknown", "")
        
        is_product = False
        if "trendyol.com" in domain and "-p-" in url:
            is_product = True
        elif "hepsiburada.com" in domain and ("-p-" in url or "-pm-" in url):
            is_product = True
        elif "amazon" in domain and "/dp/" in url:
            is_product = True
        elif "n11.com" in domain and ("/urun/" in url or "/p/" in url):
            is_product = True
        elif "akakce.com" in domain and ".html" in url and "-fiyati," in url:
            is_product = True
        
        return ("product", domain) if is_product else ("category", domain)


def clean_price(price_text: str) -> float:
    """Parse price from various text formats."""
    if not price_text:
        return 0.0
    text = price_text.strip()
    try:
        text = re.sub(r'[^0-9.,]', '', text)
        if not text:
            return 0.0
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text:
            if len(text.split(",")[-1]) == 2:
                text = text.replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "." in text:
            if text.count(".") > 1 or len(text.split(".")[-1]) == 3:
                text = text.replace(".", "")
        return float(text)
    except ValueError as e:
        logger.warning(f"Price parse error for '{price_text[:20]}': {e}")
        return 0.0


def clean_product_name(name: str) -> str:
    """Clean product name from site suffixes."""
    if not name:
        return ""
    patterns = [
        r'\s*[|–—-]\s*(Akakçe|Trendyol|Hepsiburada|Amazon|N11|Cimri|Epey).*$',
        r'\s*-\s*(En Ucuz|Fiyatları).*$'
    ]
    result = name.strip()
    for pattern in patterns:
        result = re.sub(pattern, '', result, flags=re.IGNORECASE)
    return result.strip()[:200]


async def extract_product_info(page: Page, url: str) -> ScrapedData:
    """Extract product information from page using JSON-LD and CSS selectors."""
    domain = URLAnalyzer.get_domain(url)
    data = ScrapedData(url=url)
    
    # Try JSON-LD first
    try:
        scripts = await page.query_selector_all('script[type="application/ld+json"]')
        for script in scripts:
            try:
                content = await script.inner_text()
                if not content:
                    continue
                jd = json.loads(content)
                if isinstance(jd, dict) and "@graph" in jd:
                    jd = jd["@graph"]
                if isinstance(jd, list):
                    jd = next((i for i in jd if i.get("@type") == "Product"), {})
                
                if jd.get("name"):
                    data.name = clean_product_name(str(jd["name"]))
                
                offers = jd.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                p = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
                if p:
                    data.price = float(p)
                
                # Handle image (could be string, list, or dict)
                img = jd.get("image")
                if isinstance(img, list) and img:
                    img = img[0]
                if isinstance(img, dict):
                    data.image_url = img.get("contentUrl") or img.get("url") or ""
                elif isinstance(img, str):
                    data.image_url = img
                
                # Ensure image_url is string
                if not isinstance(data.image_url, str):
                    data.image_url = ""
                
                if data.name and data.price > 0:
                    data.source = "jsonld"
                    return ValidationService.validate(data)
            except json.JSONDecodeError:
                continue
            except Exception as e:
                logger.debug(f"JSON-LD parse warning: {e}")
                continue
    except Exception as e:
        logger.debug(f"JSON-LD extraction failed: {e}")
    
    # Fallback to CSS selectors
    if domain and domain in SELECTOR_MAP:
        sels = SELECTOR_MAP[domain]
        
        # Extract name
        for s in sels.get("name", []):
            try:
                el = await page.query_selector(s)
                if el:
                    text = await el.inner_text()
                    if text and len(text.strip()) > 3:
                        data.name = clean_product_name(text)
                        break
            except Exception as e:
                logger.debug(f"Name selector '{s}' failed: {e}")
        
        # Extract price
        for s in sels.get("price", []):
            try:
                el = await page.query_selector(s)
                if el:
                    text = await el.inner_text()
                    price = clean_price(text)
                    if price > 0:
                        data.price = price
                        break
            except Exception as e:
                logger.debug(f"Price selector '{s}' failed: {e}")
        
        # Extract image
        if not data.image_url:
            for s in sels.get("image", []):
                try:
                    el = await page.query_selector(s)
                    if el:
                        src = await el.get_attribute("src")
                        if src:
                            data.image_url = src
                            break
                except Exception as e:
                    logger.debug(f"Image selector '{s}' failed: {e}")
    
    # Final fallback for name
    if not data.name:
        try:
            title = await page.title()
            data.name = clean_product_name(title) if title else "Bilinmeyen Ürün"
        except Exception as e:
            logger.warning(f"Title extraction failed: {e}")
            data.name = "Bilinmeyen Ürün"
    
    data.source = "css/fallback"
    return ValidationService.validate(data)


class BrowserManager:
    """Manages Playwright browser lifecycle with resource optimization."""
    
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.request_count = 0
        self._lock = asyncio.Lock()
    
    async def start(self):
        """Start browser if not already running."""
        if not self.playwright:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-accelerated-2d-canvas",
                    "--no-first-run",
                    "--no-zygote",
                    "--disable-gpu",
                    "--window-size=1920,1080"
                ]
            )
            await self._refresh_context()
            logger.info("✅ Browser başlatıldı")
    
    async def stop(self):
        """Stop browser and cleanup."""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            logger.info("🛑 Browser kapatıldı")
        except Exception as e:
            logger.error(f"Browser stop error: {e}", exc_info=True)
        finally:
            self.playwright = None
            self.browser = None
            self.context = None
    
    async def _refresh_context(self):
        """Create fresh browser context with resource blocking and stealth."""
        if self.context:
            await self.context.close()
        
        self.context = await self.browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1920, "height": 1080},
            locale="tr-TR",
            timezone_id="Europe/Istanbul",
            java_script_enabled=True
        )
        self.request_count = 0
        
        # Inject stealth scripts to hide automation markers
        await self.context.add_init_script("""
            // Override webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Override plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Override languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['tr-TR', 'tr', 'en-US', 'en']
            });
            
            // Remove automation markers from window
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        """)
        
        async def route_handler(route: Route, request: Request):
            # Block unnecessary resources
            if request.resource_type in ["image", "media", "font"]:
                await route.abort()
            else:
                await route.continue_()
        
        await self.context.route("**/*", route_handler)
    
    async def get_context(self) -> BrowserContext:
        """Get browser context, refreshing if needed."""
        # Auto-start browser if not running
        if not self.browser:
            await self.start()
        
        async with self._lock:
            self.request_count += 1
            if self.request_count >= 50 or not self.context:
                await self._refresh_context()
            return self.context


# Global browser manager instance
_MANAGER: Optional[BrowserManager] = None


def get_manager() -> BrowserManager:
    """Get or create browser manager singleton."""
    global _MANAGER
    if _MANAGER is None:
        _MANAGER = BrowserManager()
    return _MANAGER


async def start_browser():
    """Start the browser manager."""
    await get_manager().start()


async def stop_browser():
    """Stop the browser manager."""
    global _MANAGER
    if _MANAGER:
        await _MANAGER.stop()
        _MANAGER = None


async def force_restart_browser():
    """Force restart the browser."""
    await stop_browser()
    await start_browser()


async def scrape_single_url(url: str, context: Optional[BrowserContext] = None) -> ScrapedData:
    """Scrape a single URL."""
    manager = get_manager()
    if not context:
        context = await manager.get_context()
    
    page = await context.new_page()
    try:
        await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        
        dom = URLAnalyzer.get_domain(url)
        if dom and dom in SELECTOR_MAP:
            try:
                price_selectors = SELECTOR_MAP[dom].get("price", [])
                if price_selectors:
                    await page.wait_for_selector(price_selectors[0], timeout=3000)
            except Exception:
                pass  # Timeout is acceptable here
        
        data = await extract_product_info(page, url)
        return data
    except Exception as e:
        logger.warning(f"Scrape failed for {url[:50]}: {e}")
        return ScrapedData(url=url, error_message=str(e))
    finally:
        await page.close()


async def get_products_batch(urls: List[str], batch_size: int = 20) -> List[Dict[str, Any]]:
    """Scrape multiple URLs with concurrency control."""
    manager = get_manager()
    context = await manager.get_context()
    
    # Use semaphore to limit concurrency
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    
    async def bound_scrape(url: str) -> Dict[str, Any]:
        async with sem:
            d = await scrape_single_url(url, context)
            return {
                "url": d.url,
                "name": d.name,
                "price": d.price,
                "image_url": d.image_url,
                "is_valid": d.is_valid,
                "error_message": d.error_message
            }
    
    results = await asyncio.gather(*[bound_scrape(u) for u in urls], return_exceptions=True)
    
    # Handle any exceptions in results
    processed = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            logger.error(f"Batch item failed: {urls[i][:50]} - {r}")
            processed.append({
                "url": urls[i],
                "name": "",
                "price": 0.0,
                "image_url": "",
                "is_valid": False,
                "error_message": str(r)
            })
        else:
            processed.append(r)
    
    return processed


async def get_product_data(url: str) -> Dict[str, Any]:
    """Scrape a single product URL."""
    d = await scrape_single_url(url)
    return {
        "url": d.url,
        "name": d.name,
        "price": d.price,
        "image_url": d.image_url,
        "is_valid": d.is_valid
    }


async def discover_links(category_url: str, max_pages: int = 15) -> List[str]:
    """Discover product links from a category page."""
    url_type, domain = URLAnalyzer.analyze(category_url)
    if url_type == "product":
        return [category_url]
    if not domain:
        return []
    
    # Extract category path for filtering (e.g., "ekran-karti" from the URL)
    parsed_url = urlparse(category_url)
    # Use removesuffix (Python 3.9+) instead of rstrip to properly remove .html suffix
    category_path = parsed_url.path
    if category_path.endswith('.html'):
        category_path = category_path[:-5]  # Remove ".html" properly
    category_path = category_path.rstrip('/')
    
    # Get the main category segment (e.g., "ekran-karti" from "/ekran-karti")
    category_segment = category_path.split('/')[-1] if category_path else None
    
    logger.info(f"🔍 Kategori filtresi: {category_segment}")
    
    found = set()
    manager = get_manager()
    context = await manager.get_context()
    page = await context.new_page()
    
    try:
        for page_num in range(1, max_pages + 1):
            target_url = category_url
            
            if page_num > 1:
                separator = "&" if "?" in category_url else "?"
                param = PAGINATION_PARAMS.get(domain, "page")
                target_url = f"{category_url}{separator}{param}={page_num}"
            
            logger.info(f"🕷️ Sayfa {page_num}: {target_url}")
            
            try:
                await page.goto(target_url, timeout=15000, wait_until="domcontentloaded")
                # Scroll to load lazy content
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                await asyncio.sleep(0.5)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1.0)  # Increased wait time for lazy loading
                
                # Get all links but exclude footer links
                hrefs = await page.evaluate("""() => {
                    // Exclude footer links
                    const footer = document.querySelector('footer, .footer, #footer, .f-links_v8');
                    const footerLinks = footer ? new Set(Array.from(footer.querySelectorAll('a')).map(a => a.href)) : new Set();
                    
                    return Array.from(document.querySelectorAll('a'))
                        .map(a => a.href)
                        .filter(href => href && !footerLinks.has(href));
                }""")
                regex = PRODUCT_URL_PATTERNS.get(domain)
                count = 0
                matched_before_filter = 0
                
                for href in hrefs:
                    if not href or (domain and domain not in href):
                        continue
                    
                    is_product = False
                    if regex and regex.search(href):
                        is_product = True
                    elif not regex and len(href) > 20 and any(x in href for x in ["-p-", "urun", "dp/", ".html"]):
                        is_product = True
                    
                    if is_product:
                        matched_before_filter += 1
                    
                    # Category filter: Only apply if we have a category segment
                    # Check if URL path starts with the category segment (less restrictive)
                    if is_product and category_segment:
                        href_path = urlparse(href).path
                        # Accept URLs that contain the category anywhere in the path
                        if category_segment not in href_path:
                            logger.debug(f"⏭️ Kategori dışı atlandı: {href[:60]}")
                            continue
                    
                    if is_product and href not in found:
                        found.add(href)
                        count += 1
                
                logger.info(f"📦 Sayfa {page_num}: {count} yeni ürün (regex eşlesen: {matched_before_filter})")
                
                if count == 0 and page_num > 1:
                    break
                    
            except Exception as e:
                logger.warning(f"Page {page_num} failed: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Link discovery failed: {e}", exc_info=True)
    finally:
        await page.close()
    
    return list(found)
