import re
import json
import asyncio
import random
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse, unquote
from playwright.async_api import async_playwright, Page, BrowserContext, Browser, Playwright, Route, Request

logger = logging.getLogger("FiyatTakip.Scraper")
from config import USER_AGENTS, PAGE_TIMEOUT, BATCH_SIZE

MAX_CONCURRENCY = 20  # Ryzen 7600 Gücü

SELECTOR_MAP = {
    "trendyol.com": {
        "price": [".prc-dsc", ".product-price-container span", "[data-testid='price']", ".pr-bx-pr-dsc"],
        "name": [".pr-new-br", "h1.pr-nm", "h1", "[data-testid='product-title']"],
        "image": [".base-product-image img", "img.ph-gl-img", "[data-testid='product-image'] img"]
    },
    "hepsiburada.com": {
        "price": ["#originalPrice", "[data-test-id='price-current-price']", "span[data-bind='markupText']", ".product-price"],
        "name": ["h1#product-name", "h1[data-test-id='title']", "h1", ".product-name"],
        "image": ["img.product-image", "img[data-test-id='product-image']", ".product-image img"]
    },
    "amazon.com.tr": {
        "price": [".a-price-whole", "#priceblock_ourprice", "#priceblock_dealprice", "#corePrice_feature_div .a-offscreen"],
        "name": ["#productTitle", "h1 span#productTitle", "#title span"],
        "image": ["#landingImage", "#imgBlkFront", "#main-image"]
    },
    "n11.com": {
        "price": [".newPrice ins", ".price ins", ".proMainPrice ins", ".unf-p-new-price"],
        "name": [".proName", "h1.pro-name", "h1", ".unf-p-name"],
        "image": [".imgObj img", ".product-image img", ".unf-p-img img"]
    },
    "akakce.com": {
        "price": [".pb_v8 .pt_v8", ".pt_v8", "span.pt_v8", ".fiyat_v8", "[data-price]"],
        "name": ["h1", ".pr_v8 h1", "h1.pn_v8", ".product-title"],
        "image": [".img_w img", ".img_v8 img", "[data-img] img", ".product-image img"]
    },
    "cimri.com": {
        "price": ["[data-testid='product-price']", ".product-price", "span[class*='ProductCard_price']", ".price-value"],
        "name": ["h1", "[data-testid='product-title']", "h1[class*='ProductInfo']", ".product-title"],
        "image": ["[data-testid='product-image'] img", ".product-image img", ".main-image img"]
    },
    "epey.com": {
        "price": [".fiyat.fiyat_sat", ".min-price", ".urun-fiyati", ".price-min"],
        "name": ["h1.baslik", "h1.urun-baslik", "h1", ".product-name"],
        "image": [".buyuk_resim img", ".product-image img", ".main-img img"]
    }
}

PRODUCT_URL_PATTERNS = {
    "akakce.com": re.compile(r"-fiyati,\d+\.html$"),
    "trendyol.com": re.compile(r"-p-\d+"),
    "hepsiburada.com": re.compile(r"-pm?-\d+$|/[a-z0-9-]+-p-[A-Z0-9]+$"),
    "amazon.com.tr": re.compile(r"/dp/[A-Z0-9]+"),
    "n11.com": re.compile(r"/urun/|/p/\d+"),
    "cimri.com": re.compile(r"/[a-z0-9-]+-\d+$"),
    "epey.com": re.compile(r"/[a-z0-9-]+/[a-z0-9-]+$"),
}

DEFAULT_IMAGE_URL = "https://via.placeholder.com/200x200?text=Resim+Yok"

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
    INVALID_NAME_PATTERNS = [r'^https?://', r'^www\.', r'^Bilinmeyen', r'^Unknown', r'^Hata$', r'^N/A$', r'^\.com']
    @classmethod
    def validate(cls, data: ScrapedData) -> ScrapedData:
        errors = []
        if data.price <= 0: errors.append("Fiyat bulunamadı")
        if not data.name or len(data.name.strip()) < 3: errors.append("Ürün ismi yok")
        else:
            for pattern in cls.INVALID_NAME_PATTERNS:
                if re.match(pattern, data.name, re.IGNORECASE):
                    errors.append(f"Geçersiz isim: {data.name[:30]}")
                    break
        if not data.image_url: data.image_url = DEFAULT_IMAGE_URL
        if errors:
            data.is_valid = False, 
            data.error_message = "; ".join(errors)
        else:
            data.is_valid = True
            data.error_message = ""
        return data

class URLAnalyzer:
    @classmethod
    def get_domain(cls, url: str) -> Optional[str]:
        for domain in SELECTOR_MAP.keys():
            if domain in url: return domain
        return None
    @classmethod
    def analyze(cls, url: str) -> Tuple[str, str]:
        domain = cls.get_domain(url)
        if not domain: return ("unknown", "")
        if any(ind in url.lower() for ind in ["-p-", "/dp/", "/urun/", "-fiyati,", "/product/"]): return ("product", domain)
        if any(ind in url.lower() for ind in ["/sr?", "/ara?", "/kategori/", "/category/", "?q=", "?src="]): return ("category", domain)
        return ("product", domain)

def clean_price(price_text: str) -> float:
    if not price_text: return 0.0
    text = price_text.strip()
    try:
        text = re.sub(r'[^0-9.,]', '', text)
        if not text: return 0.0
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."): text = text.replace(".", "").replace(",", ".")
            else: text = text.replace(",", "")
        elif "," in text:
            if len(text.split(",")[-1]) == 2: text = text.replace(",", ".")
            else: text = text.replace(",", "")
        elif "." in text:
            if text.count(".") > 1 or len(text.split(".")[-1]) == 3: text = text.replace(".", "")
        return float(text)
    except: return 0.0

def clean_product_name(name: str) -> str:
    if not name: return ""
    patterns = [r'\s*[|–—-]\s*(Akakçe|Trendyol|Hepsiburada|Amazon|N11|Cimri|Epey).*$', r'\s*-\s*(En Ucuz|Fiyatları).*$']
    result = name.strip()
    for pattern in patterns: result = re.sub(pattern, '', result, flags=re.IGNORECASE)
    return result.strip()[:200]

async def extract_product_info(page: Page, url: str) -> ScrapedData:
    domain = URLAnalyzer.get_domain(url)
    data = ScrapedData(url=url)
    try:
        scripts = await page.query_selector_all('script[type="application/ld+json"]')
        for script in scripts:
            try:
                content = await script.inner_text()
                if not content: continue
                jd = json.loads(content)
                if isinstance(jd, dict) and "@graph" in jd: jd = jd["@graph"]
                if isinstance(jd, list): jd = next((i for i in jd if i.get("@type") == "Product"), {})
                if jd.get("name"): data.name = clean_product_name(str(jd["name"]))
                offers = jd.get("offers", {})
                if isinstance(offers, list): offers = offers[0] if offers else {}
                p = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
                if p: data.price = float(p)
                img = jd.get("image")
                if isinstance(img, list): data.image_url = img[0]
                elif isinstance(img, dict): data.image_url = img.get("url", "")
                elif isinstance(img, str): data.image_url = img
                if data.name and data.price > 0:
                    data.source = "jsonld"
                    return ValidationService.validate(data)
            except: continue
    except: pass
    
    if domain:
        sels = SELECTOR_MAP.get(domain, {})
        for s in sels.get("name", []):
            try:
                el = await page.query_selector(s)
                if el: data.name = clean_product_name(await el.inner_text())
                if data.name: break
            except: pass
        for s in sels.get("price", []):
            try:
                el = await page.query_selector(s)
                if el: data.price = clean_price(await el.inner_text())
                if data.price > 0: break
            except: pass
        if not data.image_url:
            for s in sels.get("image", []):
                try:
                    el = await page.query_selector(s)
                    if el: data.image_url = await el.get_attribute("src") or ""
                    if data.image_url: break
                except: pass
    
    if not data.name:
        try: data.name = clean_product_name(await page.title())
        except: data.name = "Bilinmeyen Ürün"
        
    data.source = "css/fallback"
    return ValidationService.validate(data)

class BrowserManager:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.request_count = 0
        self._lock = asyncio.Lock()
    
    async def start(self):
        if not self.playwright:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-gpu"]
            )
            await self._refresh_context()

    async def stop(self):
        if self.context: await self.context.close()
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()
        self.playwright = None

    async def _refresh_context(self):
        if self.context: await self.context.close()
        self.context = await self.browser.new_context(user_agent=random.choice(USER_AGENTS))
        self.request_count = 0
        async def route_handler(route: Route, request: Request):
            if request.resource_type in ["image", "media", "font", "stylesheet"]: await route.abort()
            else: await route.continue_()
        await self.context.route("**/*", route_handler)

    async def get_context(self) -> BrowserContext:
        async with self._lock:
            self.request_count += 1
            if self.request_count >= 50 or not self.context: await self._refresh_context()
            return self.context

_MANAGER = None
def get_manager(): global _MANAGER; _MANAGER = _MANAGER or BrowserManager(); return _MANAGER
async def start_browser(): await get_manager().start()
async def stop_browser(): global _MANAGER; await _MANAGER.stop() if _MANAGER else None; _MANAGER = None
async def force_restart_browser(): await stop_browser(); await start_browser()

async def scrape_single_url(url: str, context: Optional[BrowserContext] = None) -> ScrapedData:
    manager = get_manager()
    if not context: context = await manager.get_context()
    page = await context.new_page()
    try:
        await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        dom = URLAnalyzer.get_domain(url)
        if dom and dom in SELECTOR_MAP:
             try: await page.wait_for_selector(SELECTOR_MAP[dom]["price"][0], timeout=3000)
             except: pass
        data = await extract_product_info(page, url)
    except Exception as e:
        data = ScrapedData(url=url, error_message=str(e))
    finally:
        await page.close()
    return data

async def get_products_batch(urls: List[str], batch_size: int = BATCH_SIZE) -> List[Dict[str, Any]]:
    manager = get_manager()
    context = await manager.get_context()
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    async def bound_scrape(url):
        async with sem:
            d = await scrape_single_url(url, context)
            return {"url": d.url, "name": d.name, "price": d.price, "image_url": d.image_url, "is_valid": d.is_valid, "error_message": d.error_message}
    return await asyncio.gather(*[bound_scrape(u) for u in urls])

async def get_product_data(url: str) -> Dict[str, Any]:
    d = await scrape_single_url(url)
    return {"url": d.url, "name": d.name, "price": d.price, "image_url": d.image_url, "is_valid": d.is_valid}

async def discover_links(category_url: str, max_pages: int = 5) -> List[str]:
    """AKILLI SAYFALAMA: URL Manipülasyonu ile çok daha hızlı"""
    url_type, domain = URLAnalyzer.analyze(category_url)
    if url_type == "product": return [category_url]
    if not domain: return []
    
    found = set()
    manager = get_manager()
    context = await manager.get_context()
    page = await context.new_page()
    base_url = category_url.split("?")[0]
    
    try:
        for page_num in range(1, max_pages + 1):
            target_url = category_url
            if page_num > 1:
                if "trendyol.com" in domain: target_url = f"{base_url}?pi={page_num}"
                elif "hepsiburada.com" in domain: target_url = f"{base_url}?sayfa={page_num}"
                elif "n11.com" in domain: target_url = f"{base_url}?pg={page_num}"
                elif "akakce.com" in domain: target_url = f"{base_url}?p={page_num}"
                elif "amazon.com" in domain: target_url = f"{base_url}&page={page_num}"
            
            logger.info(f"🕷️ Sayfa {page_num}: {target_url}")
            try:
                await page.goto(target_url, timeout=15000, wait_until="domcontentloaded")
                # Scroll yerine sadece bir kere dibe vur
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(0.5)
                
                hrefs = await page.evaluate("() => Array.from(document.querySelectorAll('a')).map(a => a.href)")
                regex = PRODUCT_URL_PATTERNS.get(domain)
                count = 0
                for href in hrefs:
                    if not href or (domain and domain not in href): continue
                    is_p = False
                    if regex and regex.search(href): is_p = True
                    elif not regex and len(href)>25 and any(x in href for x in ["-p-", "urun", "dp/", ".html"]): is_p = True
                    
                    if is_p and href not in found:
                        found.add(href)
                        count += 1
                if count == 0 and page_num > 1: break
            except: continue
    finally:
        await page.close()
    return list(found)
