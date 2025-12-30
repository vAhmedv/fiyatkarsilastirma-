"""
Fiyat Takip Sistemi - Scraper Modülü (Async Edition)

Bu modül web sitelerinden fiyat ve ürün bilgisi toplar.
Özellikler:
- Async Playwright (Native Async/Await)
- Akıllı Kaynak Engelleme (Resim/Font/CSS yok) -> Roket Hızı
- Akıllı URL Tespiti (Ürün vs Kategori)
- JSON-LD + Open Graph + CSS 3-katmanlı veri çıkarma
- Validasyon Katmanı
- Anti-bot önlemleri (User-Agent rotation, context refresh)
"""

import re
import json
import asyncio
import random
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse, unquote

from playwright.async_api import async_playwright, Page, BrowserContext, Browser, Playwright, Route, Request

# --- LOGGING SETUP ---
logger = logging.getLogger("FiyatTakip.Scraper")

# --- CONFIG ---
from config import USER_AGENTS, PAGE_TIMEOUT, BATCH_SIZE

# Sabitler
MAX_CONCURRENCY = 10  # Maksimum eşzamanlı sekme sayısı

# CSS Selector fallbacks
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

# URL Patterns
PRODUCT_URL_PATTERNS = {
    "akakce.com": re.compile(r"-fiyati,\d+\.html$"),
    "trendyol.com": re.compile(r"-p-\d+"),
    "hepsiburada.com": re.compile(r"-pm?-\d+$|/[a-z0-9-]+-p-[A-Z0-9]+$"),
    "amazon.com.tr": re.compile(r"/dp/[A-Z0-9]+"),
    "n11.com": re.compile(r"/urun/|/p/\d+"),
    "cimri.com": re.compile(r"/[a-z0-9-]+-\d+$"),
    "epey.com": re.compile(r"/[a-z0-9-]+/[a-z0-9-]+$"),
}

CATEGORY_URL_PATTERNS = {
    "akakce.com": re.compile(r"akakce\.com/[a-z0-9-]+(?:/[a-z0-9-]+)?(?:\?|$)"),
    "trendyol.com": re.compile(r"/sr\?|/[a-z-]+(?:\?|$)"),
}

DEFAULT_IMAGE_URL = "https://via.placeholder.com/200x200?text=Resim+Yok"


# ==============================================================================
# VERİ SINIFI VE VALIDASYON (Aynı kaldı)
# ==============================================================================

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
    INVALID_NAME_PATTERNS = [
        r'^https?://', r'^www\.', r'^Bilinmeyen', r'^Unknown', r'^Hata$', r'^N/A$', r'^\.com'
    ]
    
    @classmethod
    def validate(cls, data: ScrapedData) -> ScrapedData:
        errors = []
        if data.price <= 0:
            errors.append("Fiyat bulunamadı veya geçersiz")
        if not data.name or len(data.name.strip()) < 3:
            errors.append("Ürün ismi bulunamadı")
        else:
            for pattern in cls.INVALID_NAME_PATTERNS:
                if re.match(pattern, data.name, re.IGNORECASE):
                    errors.append(f"Geçersiz ürün ismi: {data.name[:30]}...")
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
    @classmethod
    def get_domain(cls, url: str) -> Optional[str]:
        for domain in SELECTOR_MAP.keys():
            if domain in url:
                return domain
        return None
    
    @classmethod
    def is_product_page(cls, url: str) -> bool:
        domain = cls.get_domain(url)
        if not domain: return False
        pattern = PRODUCT_URL_PATTERNS.get(domain)
        if pattern and pattern.search(url): return True
        return any(ind in url.lower() for ind in ["-p-", "/dp/", "/urun/", "-fiyati,", "/product/"])
    
    @classmethod
    def analyze(cls, url: str) -> Tuple[str, str]:
        domain = cls.get_domain(url)
        if not domain: return ("unknown", "")
        if cls.is_product_page(url): return ("product", domain)
        category_indicators = ["/sr?", "/ara?", "/kategori/", "/category/", "?q=", "?src="]
        if any(ind in url.lower() for ind in category_indicators): return ("category", domain)
        return ("product", domain)


# ==============================================================================
# YARDIMCI FONSİYONLAR
# ==============================================================================

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
    except (ValueError, AttributeError):
        return 0.0

def clean_product_name(name: str) -> str:
    if not name: return ""
    patterns = [
        r'\s*[|–—-]\s*(Akakçe|Trendyol|Hepsiburada|HepsiBurada|Amazon|N11|Cimri|Epey).*$',
        r'\s*-\s*(En Ucuz|Fiyatları|Fiyatı|Satın Al).*$',
        r'^\s*(En Ucuz)\s*',
    ]
    result = name.strip()
    for pattern in patterns:
        result = re.sub(pattern, '', result, flags=re.IGNORECASE)
    return result.strip()[:200]

def extract_name_from_url(url: str) -> str:
    try:
        if "akakce.com" in url:
            match = re.search(r'/en-ucuz-([^/]+)-fiyati', url)
            if match: return match.group(1).replace('-', ' ').title()[:200]
        parsed = urlparse(url)
        path = unquote(parsed.path)
        segment = path.rstrip('/').split('/')[-1]
        segment = re.sub(r'\.[a-z]+$', '', segment)
        name = segment.replace('-', ' ').replace('_', ' ').title()
        if len(name) > 5: return name[:200]
    except Exception: pass
    return ""


# ==============================================================================
# ASYNC VERİ ÇIKARMA (3-KATMANLI)
# ==============================================================================

async def extract_from_jsonld(page: Page) -> Dict[str, Any]:
    result = {"name": "", "price": 0.0, "image_url": "", "source": "jsonld"}
    try:
        # Script etiketlerini bul
        scripts = await page.query_selector_all('script[type="application/ld+json"]')
        for script in scripts:
            try:
                content = await script.inner_text()
                if not content or not content.strip(): continue
                
                data = json.loads(content)
                if isinstance(data, dict) and "@graph" in data: data = data["@graph"]
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") == "Product":
                            data = item
                            break
                    else: continue
                
                if not isinstance(data, dict): continue
                if data.get("@type", "") not in ["Product", "IndividualProduct", "ProductModel"]: continue
                
                if data.get("name"): result["name"] = clean_product_name(str(data["name"]))
                
                offers = data.get("offers", {})
                if isinstance(offers, list): offers = offers[0] if offers else {}
                if isinstance(offers, dict):
                    price = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
                    if price: result["price"] = float(price)
                
                image = data.get("image")
                if isinstance(image, list): image = image[0] if image else ""
                if isinstance(image, dict): image = image.get("url", "")
                if image: result["image_url"] = str(image)
                
                if result["name"] or result["price"] > 0:
                    return result
            except: continue
    except Exception as e:
        logger.debug(f"JSON-LD error: {e}")
    return result

async def extract_from_opengraph(page: Page) -> Dict[str, Any]:
    result = {"name": "", "price": 0.0, "image_url": "", "source": "opengraph"}
    try:
        og_title = await page.query_selector('meta[property="og:title"]')
        if og_title:
            content = await og_title.get_attribute("content")
            if content: result["name"] = clean_product_name(content)
            
        og_price = await page.query_selector('meta[property="product:price:amount"]')
        if og_price:
            content = await og_price.get_attribute("content")
            if content: result["price"] = clean_price(content)
            
        if result["price"] == 0:
            og_price_alt = await page.query_selector('meta[property="og:price:amount"]')
            if og_price_alt:
                content = await og_price_alt.get_attribute("content")
                if content: result["price"] = clean_price(content)
        
        og_image = await page.query_selector('meta[property="og:image"]')
        if og_image:
            content = await og_image.get_attribute("content")
            if content: result["image_url"] = content
    except Exception as e:
        logger.debug(f"OpenGraph error: {e}")
    return result

async def extract_from_css(page: Page, domain: str) -> Dict[str, Any]:
    result = {"name": "", "price": 0.0, "image_url": "", "source": "css"}
    selectors = SELECTOR_MAP.get(domain, {"price": [".price"], "name": ["h1"], "image": ["img"]})
    try:
        for sel in selectors.get("name", []):
            try:
                el = await page.query_selector(sel)
                if el:
                    val = await el.inner_text()
                    if val and len(val.strip()) > 3:
                        result["name"] = clean_product_name(val)
                        break
            except: continue
            
        for sel in selectors.get("price", []):
            try:
                el = await page.query_selector(sel)
                if el:
                    val = await el.inner_text()
                    price = clean_price(val)
                    if price > 0:
                        result["price"] = price
                        break
            except: continue
            
        for sel in selectors.get("image", []):
            try:
                el = await page.query_selector(sel)
                if el:
                    val = await el.get_attribute("src") or await el.get_attribute("data-src")
                    if val:
                        result["image_url"] = val
                        break
            except: continue
    except Exception as e:
        logger.debug(f"CSS error: {e}")
    return result

async def extract_product_info(page: Page, url: str) -> ScrapedData:
    domain = URLAnalyzer.get_domain(url)
    data = ScrapedData(url=url)
    
    # 1. JSON-LD
    result = await extract_from_jsonld(page)
    if result["name"] and result["price"] > 0:
        data.name = result["name"]
        data.price = result["price"]
        data.image_url = result["image_url"]
        data.source = "jsonld"
        return ValidationService.validate(data)
    
    # 2. Open Graph
    og_result = await extract_from_opengraph(page)
    if not result["name"] and og_result["name"]: 
        result["name"] = og_result["name"]
        result["source"] = "opengraph"
    if result["price"] == 0 and og_result["price"] > 0: result["price"] = og_result["price"]
    if not result["image_url"] and og_result["image_url"]: result["image_url"] = og_result["image_url"]
    
    if result["name"] and result["price"] > 0:
        data.name = result["name"]
        data.price = result["price"]
        data.image_url = result["image_url"]
        data.source = result.get("source", "opengraph")
        return ValidationService.validate(data)
    
    # 3. CSS
    if domain:
        css_result = await extract_from_css(page, domain)
        if not result["name"] and css_result["name"]:
            result["name"] = css_result["name"]
            result["source"] = "css"
        if result["price"] == 0 and css_result["price"] > 0: result["price"] = css_result["price"]
        if not result["image_url"] and css_result["image_url"]: result["image_url"] = css_result["image_url"]
    
    # 4. Fallbacks (Title / URL)
    if not result["name"]:
        try:
            title = await page.title()
            if title: result["name"] = clean_product_name(title)
        except: pass
        if not result.get("name"):
            result["name"] = extract_name_from_url(url)
    
    if not result["name"]: result["name"] = "Bilinmeyen Ürün"
    
    data.name = result["name"]
    data.price = result["price"]
    data.image_url = result["image_url"]
    data.source = result.get("source", "fallback")
    return ValidationService.validate(data)


# ==============================================================================
# ASYNC BROWSER MANAGER
# ==============================================================================

class BrowserManager:
    """Async Browser Manager - Resources Blocking ile Hızlandırılmış"""
    
    def __init__(self):
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.request_count = 0
        self.max_requests_per_context = 50
        self._lock = asyncio.Lock()
    
    async def start(self):
        if not self.playwright:
            logger.info("🚀 Playwright başlatılıyor...")
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                # args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
                args=[
                   "--disable-blink-features=AutomationControlled",
                   "--no-sandbox",
                   "--disable-setuid-sandbox",
                   "--disable-dev-shm-usage",
                   "--disable-accelerated-2d-canvas",
                   "--no-first-run",
                   "--no-zygote",
                   "--disable-gpu",
                ]
            )
            await self._refresh_context()
            logger.info("✅ Browser hazır (Async).")
    
    async def stop(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.playwright = None
        logger.info("🛑 Browser durduruldu.")
    
    async def _refresh_context(self):
        """Context yenile ve kaynak engelleme kurallarını uygula"""
        if self.context:
            try:
                await self.context.close()
            except: pass
            
        self.context = await self.browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1280, 'height': 800}
        )
        self.request_count = 0
        
        # KAYNAK ENGELLEME: Resim, Font, CSS, Medya YÜKLENMEZ!
        # Sadece HTML ve XHR/Fetch/Script izin ver.
        async def route_handler(route: Route, request: Request):
            # Kaynak tipine göre engelleme
            if request.resource_type in ["image", "media", "font", "stylesheet"]:
                await route.abort()
            else:
                await route.continue_()
                
        await self.context.route("**/*", route_handler)
        logger.debug("🔄 Context yenilendi ve Resource Blocking aktif.")

    async def get_context(self) -> BrowserContext:
        async with self._lock:
            self.request_count += 1
            if self.request_count >= self.max_requests_per_context:
                await self._refresh_context()
            if not self.context:
                await self._refresh_context()
            return self.context
    
    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

# Global Singleton
_MANAGER: Optional[BrowserManager] = None

def get_manager() -> BrowserManager:
    global _MANAGER
    if _MANAGER is None:
        _MANAGER = BrowserManager()
    return _MANAGER

async def start_browser():
    await get_manager().start()

async def stop_browser():
    global _MANAGER
    if _MANAGER:
        await _MANAGER.stop()
    _MANAGER = None

async def force_restart_browser():
    await stop_browser()
    await start_browser()


# ==============================================================================
# ANA TARAMA FONKSİYONLARI (ASYNC)
# ==============================================================================

async def scrape_single_url(url: str, context: Optional[BrowserContext] = None) -> ScrapedData:
    """Tek bir URL'yi asenkron olarak tara"""
    manager = get_manager()
    if not context:
        context = await manager.get_context()
    
    page = await context.new_page()
    data = ScrapedData(url=url)
    
    try:
        # Gereksiz servisleri engelle (Google Analytics, reklamlar vb.)
        # await page.route("**/*analytics*", lambda route: route.abort())
        
        # Sayfaya git
        await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        
        # Akıllı Bekleme: Selector veya Load State
        try:
            domain = URLAnalyzer.get_domain(url)
            if domain and domain in SELECTOR_MAP:
                # O sitenin fiyat elementini bekle (en fazla 3sn)
                price_selector = SELECTOR_MAP[domain]["price"][0]
                await page.wait_for_selector(price_selector, timeout=3000)
            else:
                # Genel bekleme
                await page.wait_for_load_state("networkidle", timeout=3000)
        except:
            pass # Timeout yerse devam et, belki veri gelmiştir
        
        data = await extract_product_info(page, url)
        
        if data.is_valid:
            logger.info(f"✅ [{data.source}] {data.name[:30]}... - {data.price} TL")
        else:
            logger.warning(f"⚠️ Validation Failed [{url[-40:]}]: {data.error_message}")
            
    except Exception as e:
        data.error_message = str(e)
        logger.warning(f"⚠️ Scrape Error [{url[-40:]}]: {e}")
    finally:
        await page.close()
        
    return data


async def get_products_batch(urls: List[str], batch_size: int = BATCH_SIZE) -> List[Dict[str, Any]]:
    """
    Concurrent (Eşzamanlı) Tarama
    asyncio.gather kullanarak aynı anda birden fazla sekme açar.
    """
    if not urls: return []
    
    manager = get_manager()
    context = await manager.get_context() # Tek context paylaşıyoruz (daha hızlı)
    
    # Concurrency Limiti (Semaphore)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    
    async def bound_scrape(url):
        async with sem:
            data = await scrape_single_url(url, context)
            return {
                "url": data.url,
                "name": data.name,
                "price": data.price,
                "image_url": data.image_url,
                "is_valid": data.is_valid,
                "error_message": data.error_message,
                "source": data.source
            }
    
    tasks = [bound_scrape(url) for url in urls]
    results = await asyncio.gather(*tasks)
    return results


async def get_product_data(url: str) -> Dict[str, Any]:
    """Tek ürün verisi wrapper"""
    data = await scrape_single_url(url)
    return {
        "url": data.url, "name": data.name, "price": data.price, 
        "image_url": data.image_url, "is_valid": data.is_valid, 
        "error_message": data.error_message, "source": data.source
    }


async def discover_links(category_url: str, max_pages: int = 5) -> List[str]:
    """Kategori tarama (Async)"""
    url_type, domain = URLAnalyzer.analyze(category_url)
    if url_type == "product": return [category_url]
    if not domain: return []
    
    found = set()
    manager = get_manager()
    context = await manager.get_context()
    page = await context.new_page()
    current_url = category_url
    
    try:
        for page_num in range(1, max_pages + 1):
            logger.info(f"🕷️ Sayfa {page_num}: {current_url[:50]}...")
            try:
                await page.goto(current_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                # Lazy load scroll
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1) 
                
                hrefs = await page.evaluate("() => Array.from(document.querySelectorAll('a')).map(a => a.href)")
                
                regex = PRODUCT_URL_PATTERNS.get(domain)
                new_count = 0
                for href in hrefs:
                    if not href or (domain and domain not in href): continue
                    if regex and regex.search(href):
                        if href not in found:
                            found.add(href)
                            new_count += 1
                    elif not regex and len(href) > 30 and any(x in href for x in ["product", "urun", "-p-", ".html"]):
                        if href not in found:
                            found.add(href)
                            new_count += 1
                
                logger.info(f"   ➕ {new_count} yeni link")
                if new_count == 0: break
                
                # Pagination Logic Simplified
                if "akakce.com" in category_url and "?p=" not in current_url:
                     current_url = f"{category_url}?p=2"
                elif "akakce.com" in category_url and "?p=" in current_url:
                     try:
                         base, p_val = current_url.split("?p=")
                         current_url = f"{base}?p={int(p_val) + 1}"
                     except: break
                else:
                    break # Diğer siteler için basit text match pagination eklenebilir
                    
            except Exception as e:
                logger.warning(f"Sayfa hatası: {e}")
                break
    finally:
        await page.close()
        
    return list(found)
