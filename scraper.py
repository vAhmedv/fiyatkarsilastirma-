"""
Fiyat Takip Sistemi - Scraper Modülü v7.0 (Robust Edition)

Bu modül web sitelerinden fiyat ve ürün bilgisi toplar.
Özellikler:
- Akıllı URL Tespiti (Ürün vs Kategori)
- JSON-LD + Open Graph + CSS 3-katmanlı veri çıkarma
- Validasyon Katmanı (Veri bütünlüğü kontrolü)
- Batch paralel tarama (ThreadPoolExecutor)
- Anti-bot önlemleri (User-Agent rotation, context refresh)
"""

import re
import json
import time
import random
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, unquote
from playwright.sync_api import sync_playwright, Page, BrowserContext

# --- LOGGING SETUP ---
logger = logging.getLogger("FiyatTakip.Scraper")

# --- CONFIG ---
from config import USER_AGENTS, PAGE_TIMEOUT, BATCH_SIZE

# CSS Selector fallbacks (son çare olarak kullanılır)
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

# URL Patterns for Product vs Category detection
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

# Default placeholder image
DEFAULT_IMAGE_URL = "https://via.placeholder.com/200x200?text=Resim+Yok"


# ==============================================================================
# VERİ SINIFI VE VALIDASYON
# ==============================================================================

@dataclass
class ScrapedData:
    """Taranmış veri için type-safe konteyner."""
    url: str
    name: str = ""
    price: float = 0.0
    image_url: str = ""
    source: str = "unknown"
    is_valid: bool = False
    error_message: str = ""


class ValidationService:
    """
    Veri bütünlüğü kontrolleri.
    Veritabanına yazılmadan önce çalışır.
    """
    
    INVALID_NAME_PATTERNS = [
        r'^https?://',           # URL ile başlıyor
        r'^www\.',               # www. ile başlıyor
        r'^Bilinmeyen',          # Varsayılan isim
        r'^Unknown',
        r'^Hata$',
        r'^N/A$',
        r'^\.com',               # Domain uzantısı
    ]
    
    @classmethod
    def validate(cls, data: ScrapedData) -> ScrapedData:
        """
        Veriyi kontrol et ve is_valid bayrağını ayarla.
        Geçersiz verilerin error_message'ına sebep yaz.
        """
        errors = []
        
        # Fiyat kontrolü
        if data.price <= 0:
            errors.append("Fiyat bulunamadı veya geçersiz")
        
        # İsim kontrolü
        if not data.name or len(data.name.strip()) < 3:
            errors.append("Ürün ismi bulunamadı")
        else:
            for pattern in cls.INVALID_NAME_PATTERNS:
                if re.match(pattern, data.name, re.IGNORECASE):
                    errors.append(f"Geçersiz ürün ismi: {data.name[:30]}...")
                    break
        
        # Görsel kontrolü (sadece uyarı, reject etme)
        if not data.image_url:
            data.image_url = DEFAULT_IMAGE_URL
        
        # Sonuç
        if errors:
            data.is_valid = False
            data.error_message = "; ".join(errors)
        else:
            data.is_valid = True
            data.error_message = ""
        
        return data


class URLAnalyzer:
    """
    Akıllı URL Tespiti.
    Ürün sayfası mı, kategori sayfası mı anla.
    """
    
    @classmethod
    def get_domain(cls, url: str) -> Optional[str]:
        """URL'den domain'i çıkar."""
        for domain in SELECTOR_MAP.keys():
            if domain in url:
                return domain
        return None
    
    @classmethod
    def is_product_page(cls, url: str) -> bool:
        """URL bir ürün sayfası mı?"""
        domain = cls.get_domain(url)
        if not domain:
            return False
        
        pattern = PRODUCT_URL_PATTERNS.get(domain)
        if pattern and pattern.search(url):
            return True
        
        # Fallback: Genel ürün sayfası göstergeleri
        product_indicators = ["-p-", "/dp/", "/urun/", "-fiyati,", "/product/"]
        return any(ind in url.lower() for ind in product_indicators)
    
    @classmethod
    def is_category_page(cls, url: str) -> bool:
        """URL bir kategori sayfası mı?"""
        domain = cls.get_domain(url)
        if not domain:
            return False
        
        # Ürün sayfası değilse ve desteklenen domain ise -> kategori
        if cls.is_product_page(url):
            return False
        
        pattern = CATEGORY_URL_PATTERNS.get(domain)
        if pattern and pattern.search(url):
            return True
        
        # Fallback: Kategori sayfası göstergeleri
        category_indicators = ["/sr?", "/ara?", "/kategori/", "/category/", "?q=", "?src="]
        return any(ind in url.lower() for ind in category_indicators)
    
    @classmethod
    def analyze(cls, url: str) -> Tuple[str, str]:
        """
        URL'i analiz et.
        Returns: (url_type, domain)
        url_type: 'product', 'category', veya 'unknown'
        """
        domain = cls.get_domain(url)
        
        if not domain:
            return ("unknown", "")
        
        if cls.is_product_page(url):
            return ("product", domain)
        elif cls.is_category_page(url):
            return ("category", domain)
        else:
            # Varsayılan: ürün olarak dene
            return ("product", domain)


# ==============================================================================
# YARDIMCI FONKSİYONLAR
# ==============================================================================

def clean_price(price_text: str) -> float:
    """Fiyat metnini float'a çevir (TR formatı destekli)."""
    if not price_text:
        return 0.0
    
    text = price_text.strip()
    try:
        # Para birimi ve gereksiz karakterleri temizle
        text = re.sub(r'[^0-9.,]', '', text)
        
        if not text:
            return 0.0
        
        # Türk formatı: 1.234,56 -> 1234.56
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text:
            # Virgülden sonra 2 hane varsa ondalık, değilse binlik
            parts = text.split(",")
            if len(parts[-1]) == 2:
                text = text.replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "." in text:
            # Birden fazla nokta varsa binlik ayırıcı
            if text.count(".") > 1:
                text = text.replace(".", "")
            # Noktadan sonra 3 hane varsa binlik
            elif len(text.split(".")[-1]) == 3:
                text = text.replace(".", "")
        
        return float(text)
    except (ValueError, AttributeError):
        return 0.0


def clean_product_name(name: str) -> str:
    """Ürün ismini temizle, gereksiz ekleri kaldır."""
    if not name:
        return ""
    
    # Site isimlerini kaldır
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
    """URL'den ürün ismini çıkar (son fallback)."""
    try:
        if "akakce.com" in url:
            match = re.search(r'/en-ucuz-([^/]+)-fiyati', url)
            if match:
                slug = match.group(1)
                return slug.replace('-', ' ').title()[:200]
        
        parsed = urlparse(url)
        path = unquote(parsed.path)
        segment = path.rstrip('/').split('/')[-1]
        segment = re.sub(r'\.[a-z]+$', '', segment)
        name = segment.replace('-', ' ').replace('_', ' ').title()
        
        if len(name) > 5:
            return name[:200]
    except Exception:
        pass
    
    return ""


# ==============================================================================
# 3-KATMANLI VERİ ÇIKARMA (JSON-LD > Open Graph > CSS)
# ==============================================================================

def extract_from_jsonld(page: Page) -> Dict[str, Any]:
    """
    KATMAN 1: JSON-LD yapısal veri çıkarma.
    E-ticaret sitelerinin %95+'ı bu formatı destekler.
    """
    result = {"name": "", "price": 0.0, "image_url": "", "source": "jsonld"}
    
    try:
        scripts = page.query_selector_all('script[type="application/ld+json"]')
        
        for script in scripts:
            try:
                content = script.inner_text()
                if not content or not content.strip():
                    continue
                
                data = json.loads(content)
                
                # @graph array formatı kontrolü
                if isinstance(data, dict) and "@graph" in data:
                    data = data["@graph"]
                
                # Array ise Product tipini bul
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") == "Product":
                            data = item
                            break
                    else:
                        continue
                
                # Product tipini kontrol et
                if not isinstance(data, dict):
                    continue
                
                item_type = data.get("@type", "")
                if item_type not in ["Product", "IndividualProduct", "ProductModel"]:
                    continue
                
                # İsim
                if data.get("name"):
                    result["name"] = clean_product_name(str(data["name"]))
                
                # Fiyat (offers içinde)
                offers = data.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                
                if isinstance(offers, dict):
                    price = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
                    if price:
                        result["price"] = float(price)
                
                # Resim
                image = data.get("image")
                if isinstance(image, list):
                    image = image[0] if image else ""
                if isinstance(image, dict):
                    image = image.get("url", "")
                if image:
                    result["image_url"] = str(image)
                
                # Başarılı veri bulundu
                if result["name"] or result["price"] > 0:
                    logger.debug(f"✅ JSON-LD: {result['name'][:30]}... - {result['price']} TL")
                    return result
                    
            except json.JSONDecodeError:
                continue
            except Exception as e:
                logger.debug(f"JSON-LD parse error: {e}")
                continue
                
    except Exception as e:
        logger.debug(f"JSON-LD extraction error: {e}")
    
    return result


def extract_from_opengraph(page: Page) -> Dict[str, Any]:
    """
    KATMAN 2: Open Graph meta tag'lerinden veri çıkarma.
    Sosyal medya paylaşımları için kullanılan standart format.
    """
    result = {"name": "", "price": 0.0, "image_url": "", "source": "opengraph"}
    
    try:
        # og:title (ürün ismi)
        og_title = page.query_selector('meta[property="og:title"]')
        if og_title:
            content = og_title.get_attribute("content")
            if content:
                result["name"] = clean_product_name(content)
        
        # product:price:amount (fiyat)
        og_price = page.query_selector('meta[property="product:price:amount"]')
        if og_price:
            content = og_price.get_attribute("content")
            if content:
                result["price"] = clean_price(content)
        
        # og:price:amount alternatif
        if result["price"] == 0:
            og_price_alt = page.query_selector('meta[property="og:price:amount"]')
            if og_price_alt:
                content = og_price_alt.get_attribute("content")
                if content:
                    result["price"] = clean_price(content)
        
        # og:image (resim)
        og_image = page.query_selector('meta[property="og:image"]')
        if og_image:
            content = og_image.get_attribute("content")
            if content:
                result["image_url"] = content
        
        if result["name"] or result["price"] > 0:
            logger.debug(f"✅ OpenGraph: {result['name'][:30]}... - {result['price']} TL")
            
    except Exception as e:
        logger.debug(f"OpenGraph extraction error: {e}")
    
    return result


def extract_from_css(page: Page, domain: str) -> Dict[str, Any]:
    """
    KATMAN 3: CSS selector'larla veri çıkarma (fallback).
    Diğer yöntemler başarısız olursa kullanılır.
    """
    result = {"name": "", "price": 0.0, "image_url": "", "source": "css"}
    
    selectors = SELECTOR_MAP.get(domain, {"price": [".price"], "name": ["h1"], "image": ["img"]})
    
    try:
        # İsim
        for sel in selectors.get("name", []):
            try:
                el = page.query_selector(sel)
                if el:
                    val = el.inner_text()
                    if val and len(val.strip()) > 3:
                        result["name"] = clean_product_name(val)
                        break
            except Exception:
                continue
        
        # Fiyat
        for sel in selectors.get("price", []):
            try:
                el = page.query_selector(sel)
                if el:
                    val = el.inner_text()
                    price = clean_price(val)
                    if price > 0:
                        result["price"] = price
                        break
            except Exception:
                continue
        
        # Resim
        for sel in selectors.get("image", []):
            try:
                el = page.query_selector(sel)
                if el:
                    val = el.get_attribute("src") or el.get_attribute("data-src")
                    if val:
                        result["image_url"] = val
                        break
            except Exception:
                continue
        
        if result["name"] or result["price"] > 0:
            logger.debug(f"✅ CSS: {result['name'][:30]}... - {result['price']} TL")
            
    except Exception as e:
        logger.debug(f"CSS extraction error: {e}")
    
    return result


def extract_from_title(page: Page) -> str:
    """Sayfa başlığından ürün ismi çıkar (son fallback)."""
    try:
        title = page.title()
        if title and len(title) > 5:
            return clean_product_name(title)
    except Exception:
        pass
    return ""


def extract_product_info(page: Page, url: str) -> ScrapedData:
    """
    Ana veri çıkarma fonksiyonu.
    3-katmanlı fallback sistemi: JSON-LD > OpenGraph > CSS > URL
    """
    domain = URLAnalyzer.get_domain(url)
    data = ScrapedData(url=url)
    
    # KATMAN 1: JSON-LD
    result = extract_from_jsonld(page)
    if result["name"] and result["price"] > 0:
        data.name = result["name"]
        data.price = result["price"]
        data.image_url = result["image_url"]
        data.source = "jsonld"
        return ValidationService.validate(data)
    
    # KATMAN 2: Open Graph
    og_result = extract_from_opengraph(page)
    
    # Eksik verileri tamamla
    if not result["name"] and og_result["name"]:
        result["name"] = og_result["name"]
        result["source"] = "opengraph"
    if result["price"] == 0 and og_result["price"] > 0:
        result["price"] = og_result["price"]
    if not result["image_url"] and og_result["image_url"]:
        result["image_url"] = og_result["image_url"]
    
    if result["name"] and result["price"] > 0:
        data.name = result["name"]
        data.price = result["price"]
        data.image_url = result["image_url"]
        data.source = result.get("source", "opengraph")
        return ValidationService.validate(data)
    
    # KATMAN 3: CSS Selectors
    if domain:
        css_result = extract_from_css(page, domain)
        
        if not result["name"] and css_result["name"]:
            result["name"] = css_result["name"]
            result["source"] = "css"
        if result["price"] == 0 and css_result["price"] > 0:
            result["price"] = css_result["price"]
        if not result["image_url"] and css_result["image_url"]:
            result["image_url"] = css_result["image_url"]
    
    # KATMAN 4: Page Title + URL (son çare)
    if not result["name"]:
        title_name = extract_from_title(page)
        if title_name:
            result["name"] = title_name
            result["source"] = "title"
    
    if not result["name"]:
        url_name = extract_name_from_url(url)
        if url_name:
            result["name"] = url_name
            result["source"] = "url"
    
    # Hala isim yoksa varsayılan
    if not result["name"]:
        result["name"] = "Bilinmeyen Ürün"
    
    data.name = result["name"]
    data.price = result["price"]
    data.image_url = result["image_url"]
    data.source = result.get("source", "fallback")
    
    return ValidationService.validate(data)


# ==============================================================================
# TARAYICI YÖNETİMİ (Threaded Singleton Pattern)
# ==============================================================================

import threading
import queue as queue_module

class BrowserManager(threading.Thread):
    """
    Tarayıcı yaşam döngüsü yöneticisi - ayrı thread'de çalışır.
    Bu sayede sync_playwright asyncio loop dışında çalışabilir.
    """
    
    def __init__(self):
        super().__init__(daemon=True)
        self.playwright = None
        self.browser = None
        self.context = None
        self.running = True
        self.ready_event = threading.Event()
        self.request_count = 0
        self.context_refresh_interval = 50
        self.task_queue = queue_module.Queue()
        self._lock = threading.Lock()
    
    def run(self):
        """Thread ana döngüsü - tarayıcıyı başlatır ve istekleri işler."""
        logger.info("🚀 BrowserManager thread başlatılıyor...")
        
        with sync_playwright() as p:
            self.playwright = p
            self.browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
            )
            self._refresh_context()
            logger.info("✅ Tarayıcı hazır.")
            self.ready_event.set()
            
            # İstek işleme döngüsü (BrowserActor benzeri)
            while self.running:
                try:
                    task = self.task_queue.get(timeout=1)
                except queue_module.Empty:
                    continue
                
                if task is None:  # Sonlandırma sinyali
                    break
                
                func, args, result_queue = task
                try:
                    result = func(*args)
                    result_queue.put(("success", result))
                except Exception as e:
                    result_queue.put(("error", e))
            
            # Temizlik
            logger.info("🛑 BrowserManager kapatılıyor...")
            try:
                if self.context:
                    self.context.close()
            except Exception as e:
                logger.warning(f"Context close error: {e}")
            try:
                if self.browser:
                    self.browser.close()
            except Exception as e:
                logger.warning(f"Browser close error: {e}")
        
        logger.info("🛑 BrowserManager kapatıldı.")
    
    def _refresh_context(self):
        """Context'i yenile (anti-bot)."""
        if self.context:
            try:
                self.context.close()
            except Exception:
                pass
        
        self.context = self.browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1280, 'height': 800}
        )
        self.request_count = 0
        logger.debug("🔄 Context yenilendi")
    
    def submit_task(self, func, *args):
        """Thread-safe görev gönderimi."""
        result_queue = queue_module.Queue()
        self.task_queue.put((func, args, result_queue))
        
        # Sonucu bekle
        status, result = result_queue.get(timeout=60)
        if status == "error":
            raise result
        return result
    
    def get_context(self):
        """Context'i döndür - context refresh kontrolü ile."""
        with self._lock:
            self.request_count += 1
            if self.request_count >= self.context_refresh_interval:
                self._refresh_context()
            return self.context
    
    def create_fresh_context(self):
        """Yeni context oluştur."""
        return self.browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1280, 'height': 800}
        )
    
    def shutdown(self):
        """Thread'i güvenli şekilde kapat."""
        self.running = False
        self.task_queue.put(None)
        self.join(timeout=10)
        if self.is_alive():
            logger.warning("⚠️ BrowserManager thread did not stop cleanly")


# Global instance yönetimi
_MANAGER_LOCK = threading.Lock()
_MANAGER: Optional[BrowserManager] = None

def get_manager() -> BrowserManager:
    """Singleton BrowserManager instance'ı döndür."""
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None or not _MANAGER.is_alive():
            _MANAGER = BrowserManager()
            _MANAGER.start()
            _MANAGER.ready_event.wait(timeout=15)
    return _MANAGER


# Global fonksiyonlar (geriye dönük uyumluluk)
def start_browser():
    get_manager()
    logger.info("🚀 Browser started.")

def stop_browser():
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER and _MANAGER.is_alive():
            logger.info("🛑 Stopping browser...")
            _MANAGER.shutdown()
        _MANAGER = None

def force_restart_browser():
    """Tarayıcıyı zorla yeniden başlat."""
    global _MANAGER
    logger.info("🔄 Force restart initiated...")
    
    with _MANAGER_LOCK:
        if _MANAGER:
            _MANAGER.shutdown()
        _MANAGER = None
    
    time.sleep(1)
    get_manager()
    logger.info("✅ Force restart complete.")


# ==============================================================================
# TARAMA FONKSİYONLARI
# ==============================================================================

def scrape_single_url(url: str, context: BrowserContext = None) -> ScrapedData:
    """
    Tek bir URL'yi tara ve ürün bilgisi döndür.
    """
    data = ScrapedData(url=url)
    
    own_context = False
    if context is None:
        context = get_manager().get_context()
        own_context = False  # Shared context, kapatma
    
    page = None
    try:
        page = context.new_page()
        page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        
        # JavaScript içeriğinin yüklenmesi için kısa bekleme
        time.sleep(0.8)
        
        # 3-katmanlı veri çıkarma
        data = extract_product_info(page, url)
        
        if data.is_valid:
            logger.info(f"✅ [{data.source}] {data.name[:30]}... - {data.price} TL")
        else:
            logger.warning(f"⚠️ Validation Failed [{url[-40:]}]: {data.error_message}")
        
    except Exception as e:
        data.error_message = str(e)
        logger.warning(f"⚠️ Scrape Error [{url[-40:]}]: {e}")
    finally:
        if page:
            try:
                page.close()
            except Exception:
                pass
    
    return data


def get_product_data(url: str) -> Dict[str, Any]:
    """Tek ürün verisi al (geriye dönük uyumluluk)."""
    data = scrape_single_url(url)
    return {
        "url": data.url,
        "name": data.name,
        "price": data.price,
        "image_url": data.image_url,
        "is_valid": data.is_valid,
        "error_message": data.error_message,
        "source": data.source
    }


def get_products_batch(urls: List[str], max_workers: int = BATCH_SIZE) -> List[Dict[str, Any]]:
    """
    Birden fazla URL'yi paralel olarak tara.
    BrowserManager thread'i içinde güvenli şekilde çalışır.
    """
    if not urls:
        return []
    
    manager = get_manager()
    
    # Tüm işlemi BrowserManager thread'ine devret
    def _batch_scrape_in_manager_thread(target_urls: List[str]):
        # Bu fonksiyon BrowserManager thread'inde çalışır
        # Bu yüzden self.browser'a erişim güvenlidir
        context = manager.browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1280, 'height': 800}
        )
        results = []
        try:
            # Seri veya paralel?
            # BrowserContext içinde birden fazla sayfa açmak thread-safe değildir (async hariç).
            # Sync API kullanıyoruz ve BrowserManager tek thread.
            # Bu yüzden burada threading kullanamayız, seri yapmalıyız. 
            # VEYA: Her sayfa için ayrı işlem gönderip bekleyebiliriz ama bu yavaş olur.
            # BrowserManager içinde ThreadPoolExecutor kullanmak RİSKLİDİR çünkü sync Playwright
            # nesnelerine (context, page) farklı threadlerden erişilemez.
            
            # ÇÖZÜM: Tek thread içinde seri olarak (veya async ile) yapmak en güvenlisi.
            # Ancak performans için 'max_workers' istendi.
            # Sync Playwright ile multi-thread scraping için HER thread'in kendi PLAYWRIGHT instance'ı olması gerekir.
            # Mevcut mimaride tek bir global Playwright var.
            # Bu yüzden: Ya seri yapacağız, ya da çok riskli.
            # GÜVENLİ OLAN: Seri (Sequential) ama hızlı (Context reuse).
            
            for url in target_urls:
                try:
                    # scrape_single_url context'i kullanabilir
                    data = scrape_single_url(url, context)
                    results.append({
                        "url": data.url,
                        "name": data.name,
                        "price": data.price,
                        "image_url": data.image_url,
                        "is_valid": data.is_valid,
                        "error_message": data.error_message,
                        "source": data.source
                    })
                except Exception as e:
                    logger.warning(f"⚠️ Item Error [{url[-20:]}]: {e}")
                    results.append({
                        "url": url,
                        "is_valid": False, 
                        "error_message": str(e),
                        "price": 0.0,
                        "name": "Hata",
                        "image_url": DEFAULT_IMAGE_URL
                    })
                    
        finally:
            try:
                context.close()
            except Exception:
                pass
        return results

    # Görevi gönder ve sonucu bekle
    return manager.submit_task(_batch_scrape_in_manager_thread, urls)


def discover_links(category_url: str, max_pages: int = 5) -> List[str]:
    """
    Kategori sayfasından ürün linklerini keşfet.
    Sayfalama desteği ile birden fazla sayfa taranabilir.
    
    ÖNEMLİ: Bu fonksiyon kategori URL'sini kendisi ürün olarak KAYDETMEZ.
    Sadece içindeki ürün linklerini döndürür.
    """
    # Önce URL analizi yap
    url_type, domain = URLAnalyzer.analyze(category_url)
    
    # Eğer bu bir ürün sayfası ise, sadece kendisini döndür
    if url_type == "product":
        logger.info(f"✅ URL bir ürün sayfası: {category_url[:50]}...")
        return [category_url]
    
    # Desteklenmeyen domain
    if not domain:
        logger.warning(f"⚠️ Desteklenmeyen domain: {category_url[:50]}...")
        return []
    
    found = set()
    context = get_manager().get_context()
    page = context.new_page()
    current_url = category_url
    
    try:
        for page_num in range(1, max_pages + 1):
            logger.info(f"🕷️ Sayfa {page_num}: {current_url[:50]}...")
            
            try:
                page.goto(current_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            except Exception as e:
                logger.warning(f"Sayfa yüklenemedi: {e}")
                break
            
            time.sleep(1)
            
            # Lazy load için scroll
            for _ in range(3):
                page.mouse.wheel(0, 800)
                time.sleep(0.3)
            
            # Tüm linkleri topla
            hrefs = page.evaluate("() => Array.from(document.querySelectorAll('a')).map(a => a.href)")
            
            regex = PRODUCT_URL_PATTERNS.get(domain)
            
            before_count = len(found)
            for href in hrefs:
                if not href:
                    continue
                if domain and domain not in href:
                    continue
                if regex and regex.search(href):
                    found.add(href)
                elif not regex and len(href) > 30:
                    if any(x in href for x in ["product", "urun", "-p-", ".html"]):
                        found.add(href)
            
            new_found = len(found) - before_count
            logger.info(f"   ➕ {new_found} yeni link")
            
            if new_found == 0:
                break
            
            # Sonraki sayfa (Akakçe için)
            if "akakce.com" in category_url:
                if "?p=" in current_url:
                    try:
                        base, p_val = current_url.split("?p=")
                        current_url = f"{base}?p={int(p_val) + 1}"
                    except Exception:
                        break
                else:
                    current_url = f"{category_url}?p=2"
            else:
                break
                
    except Exception as e:
        logger.error(f"🕷️ Discover Error: {e}")
    finally:
        page.close()
    
    logger.info(f"🕷️ TOPLAM: {len(found)} link")
    return list(found)


# ==============================================================================
# YARDIMCI FONKSİYONLAR (Geriye Uyumluluk)
# ==============================================================================

def get_products_parallel(urls: List[str], max_workers: int = 3) -> List[Dict]:
    """Eski fonksiyon - get_products_batch'e yönlendir."""
    return get_products_batch(urls, max_workers)


def calculate_db_size_estimate(count: int) -> str:
    """Veritabanı boyut tahmini."""
    return f"~{(count * 500) / (1024 * 1024):.2f} MB"
