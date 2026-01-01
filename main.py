"""
Fiyat Takip v8.0 - Production-Grade FastAPI Application
Fully async, non-blocking, with security hardening
"""
import asyncio
import logging
import json
import ipaddress
import socket
from datetime import datetime, timedelta
from typing import List, Optional
from urllib.parse import urlparse

import colorlog
from fastapi import FastAPI, Request, Form, Depends, BackgroundTasks, Query, HTTPException, Header
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from sqlmodel import select, func
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from database import init_db, get_session, get_db_context, close_db
from models import Product, PriceHistory, ProductStatus, Category
from scraper import (
    get_product_data, get_products_batch, discover_links,
    start_browser, stop_browser, force_restart_browser, URLAnalyzer
)
from config import ITEMS_PER_PAGE, ALLOWED_DOMAINS, SCAN_INTERVAL_HOURS

# ============================================================================
# LOGGING SETUP
# ============================================================================
log_queue: asyncio.Queue = asyncio.Queue()


class QueueHandler(logging.Handler):
    """Handler that pushes logs to an async queue for streaming."""
    def emit(self, record):
        try:
            msg = self.format(record)
            if log_queue.qsize() > 100:
                try:
                    log_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            log_queue.put_nowait(msg)
        except Exception:
            pass  # Logging should never break the app


# Console handler with colors
console_handler = colorlog.StreamHandler()
console_handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S',
    reset=True,
    log_colors={'DEBUG': 'cyan', 'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red', 'CRITICAL': 'red,bg_white'}
))

logging.basicConfig(level=logging.INFO, handlers=[console_handler])
logger = logging.getLogger("FiyatTakip")

# Queue handler for web terminal
queue_handler = QueueHandler()
queue_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(queue_handler)
logging.getLogger("FiyatTakip.Scraper").addHandler(queue_handler)
logging.getLogger("FiyatTakip.Database").addHandler(queue_handler)

# Uvicorn logs to web terminal
uvicorn_handler = QueueHandler()
uvicorn_handler.setFormatter(logging.Formatter('%(message)s'))
logging.getLogger("uvicorn.access").addHandler(uvicorn_handler)
logging.getLogger("uvicorn.error").addHandler(uvicorn_handler)


# ============================================================================
# SCAN MANAGER (Singleton Pattern - Replaces threading globals)
# ============================================================================
class ScanManager:
    """Thread-safe async scan state manager."""
    _instance: Optional["ScanManager"] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._is_scanning = False
        self._current_task: Optional[asyncio.Task] = None
        self._initialized = True
    
    @property
    def is_scanning(self) -> bool:
        return self._is_scanning
    
    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()
    
    async def start_scan(self) -> bool:
        """Try to start a scan. Returns False if already scanning."""
        async with self._lock:
            if self._is_scanning:
                return False
            self._is_scanning = True
            self._stop_event.clear()
            return True
    
    async def stop_scan(self):
        """Signal scan to stop."""
        self._stop_event.set()
    
    async def finish_scan(self):
        """Mark scan as complete."""
        async with self._lock:
            self._is_scanning = False
            self._stop_event.clear()
    
    def reset(self):
        """Reset manager state."""
        self._is_scanning = False
        self._stop_event.clear()


scan_manager = ScanManager()


# ============================================================================
# SECURITY: ADMIN KEY VERIFICATION (DISABLED FOR LOCAL DEV)
# ============================================================================
# Admin key is disabled for local development
# Re-enable in production by uncommenting the verification logic


async def verify_admin_header():
    """Admin verification disabled for local development."""
    return True


# ============================================================================
# URL VALIDATION WITH SSRF PROTECTION
# ============================================================================
ALLOWED_SCHEMES = {'http', 'https'}
BLOCKED_IP_RANGES = [
    ipaddress.ip_network('127.0.0.0/8'),      # Localhost
    ipaddress.ip_network('10.0.0.0/8'),       # Private
    ipaddress.ip_network('172.16.0.0/12'),    # Private
    ipaddress.ip_network('192.168.0.0/16'),   # Private
    ipaddress.ip_network('169.254.0.0/16'),   # Link-local
    ipaddress.ip_network('0.0.0.0/8'),        # Invalid
]


def is_ip_blocked(ip_str: str) -> bool:
    """Check if IP is in blocked ranges (SSRF protection)."""
    try:
        ip = ipaddress.ip_address(ip_str)
        return any(ip in network for network in BLOCKED_IP_RANGES)
    except ValueError:
        return False


def validate_url(url: str) -> tuple[bool, str]:
    """Validate URL with SSRF protection."""
    if not url or not url.strip():
        return False, "URL boş olamaz"
    
    try:
        parsed = urlparse(url.strip())
        
        if parsed.scheme not in ALLOWED_SCHEMES:
            return False, "Geçersiz şema (sadece http/https)"
        
        if not parsed.netloc:
            return False, "Geçersiz URL formatı"
        
        domain = parsed.netloc.lower()
        
        # Remove port if present
        if ':' in domain:
            domain = domain.split(':')[0]
        
        # Check against allowed domains
        if not any(allowed in domain for allowed in ALLOWED_DOMAINS):
            return False, f"Desteklenmeyen site. İzin verilenler: {', '.join(ALLOWED_DOMAINS)}"
        
        # SSRF Protection: Resolve and check IP
        try:
            ip = socket.gethostbyname(domain)
            if is_ip_blocked(ip):
                return False, "Bu URL'e erişim engellendi (güvenlik)"
        except socket.gaierror:
            return False, "Domain çözümlenemedi"
        
        return True, ""
    except Exception as e:
        logger.error(f"URL validation error: {e}", exc_info=True)
        return False, str(e)


# ============================================================================
# BACKGROUND TASKS
# ============================================================================
async def cleanup_old_data():
    """Delete price history older than 30 days."""
    logger.info("🧹 Veritabanı temizliği yapılıyor...")
    try:
        async with get_db_context() as session:
            cutoff = datetime.now() - timedelta(days=30)
            stmt = delete(PriceHistory).where(PriceHistory.timestamp < cutoff)
            await session.execute(stmt)
            await session.commit()
            logger.info("✅ Eski veriler temizlendi")
    except Exception as e:
        logger.error(f"Cleanup error: {e}", exc_info=True)


async def check_pending_items():
    """Process any pending items in the queue."""
    logger.info("🔄 Bekleyen ürünler kontrol ediliyor...")
    try:
        async with get_db_context() as session:
            result = await session.execute(
                select(Product.url).where(Product.status == ProductStatus.PENDING)
            )
            pending_urls = [row[0] for row in result.fetchall()]
        
        if pending_urls:
            logger.info(f"⚡ {len(pending_urls)} bekleyen ürün bulundu")
            await process_bulk_list(pending_urls, is_drain_mode=True)
        else:
            logger.info("✅ Kuyruk temiz")
    except Exception as e:
        logger.error(f"Pending check error: {e}", exc_info=True)


async def process_bulk_list(urls: List[str], is_drain_mode: bool = False):
    """Process a list of URLs for scraping."""
    if not is_drain_mode:
        if not await scan_manager.start_scan():
            logger.info("⚠️ Sistem meşgul, ürünler PENDING modunda sıraya alındı")
            return
    
    BATCH_SIZE = 20
    
    try:
        logger.info(f"📦 Toplu Tarama: {len(urls)} ürün")
        
        for batch_start in range(0, len(urls), BATCH_SIZE):
            if scan_manager.should_stop:
                logger.info("⏹️ Tarama durduruldu")
                break
            
            batch_urls = urls[batch_start:batch_start + BATCH_SIZE]
            
            try:
                results = await asyncio.wait_for(
                    get_products_batch(batch_urls, batch_size=BATCH_SIZE),
                    timeout=60.0
                )
                
                async with get_db_context() as session:
                    for data in results:
                        url = data.get("url", "")
                        if not url:
                            continue
                        
                        # Find or create product
                        result = await session.execute(
                            select(Product).where(Product.url == url)
                        )
                        existing = result.scalar_one_or_none()
                        
                        if existing:
                            prod = existing
                        else:
                            prod = Product(url=url, name=data.get("name") or "Yeni Ürün")
                            session.add(prod)
                        
                        price = data.get("price", 0.0)
                        
                        if data.get("is_valid") and price > 0:
                            prod.current_price = price
                            if prod.lowest_price == 0 or price < prod.lowest_price:
                                prod.lowest_price = price
                            prod.status = ProductStatus.ACTIVE
                            prod.error_message = None
                            
                            # Update name if still placeholder
                            if data.get("name") and ("Yeni" in prod.name or not prod.name):
                                prod.name = data["name"]
                        else:
                            prod.status = ProductStatus.ERROR
                            prod.error_message = data.get("error_message", "Veri çekilemedi")
                        
                        if data.get("image_url"):
                            prod.image_url = data["image_url"]
                        
                        prod.last_checked = datetime.now()
                        
                        # Add price history
                        if price > 0:
                            await session.flush()  # Get prod.id
                            session.add(PriceHistory(product_id=prod.id, price=price))
                    
                    await session.commit()
                
                logger.info(f"✅ Batch {batch_start // BATCH_SIZE + 1} tamamlandı")
                
            except asyncio.TimeoutError:
                logger.warning("⚠️ Batch timeout - devam ediliyor")
            except Exception as e:
                logger.error(f"❌ Batch error: {e}", exc_info=True)
            
            if not scan_manager.should_stop:
                await asyncio.sleep(0.5)
    
    finally:
        await scan_manager.finish_scan()
        if not is_drain_mode:
            asyncio.create_task(check_pending_items())


async def update_all_products():
    """Update all products in the database."""
    if scan_manager.is_scanning:
        logger.info("⚠️ Tarama zaten devam ediyor")
        return
    
    if not await scan_manager.start_scan():
        return
    
    BATCH_SIZE = 20
    
    try:
        async with get_db_context() as session:
            result = await session.execute(
                select(Product.id, Product.url).where(Product.status != "disabled")
            )
            products = result.fetchall()
        
        product_list = [(row[0], row[1]) for row in products]
        logger.info(f"🔄 Toplam güncelleme: {len(product_list)} ürün")
        
        for batch_start in range(0, len(product_list), BATCH_SIZE):
            if scan_manager.should_stop:
                logger.info("⏹️ Güncelleme durduruldu")
                break
            
            batch = product_list[batch_start:batch_start + BATCH_SIZE]
            batch_urls = [url for _, url in batch]
            
            try:
                results = await asyncio.wait_for(
                    get_products_batch(batch_urls, batch_size=BATCH_SIZE),
                    timeout=60.0
                )
                
                url_map = {r.get("url"): r for r in results}
                
                async with get_db_context() as session:
                    for pid, url in batch:
                        data = url_map.get(url, {})
                        
                        result = await session.execute(
                            select(Product).where(Product.id == pid)
                        )
                        prod = result.scalar_one_or_none()
                        
                        if not prod:
                            continue
                        
                        price = data.get("price", 0.0)
                        
                        if data.get("is_valid") and price > 0:
                            prod.current_price = price
                            if prod.lowest_price == 0 or price < prod.lowest_price:
                                prod.lowest_price = price
                            if data.get("name") and ("Yeni" in prod.name or not prod.name):
                                prod.name = data["name"]
                            prod.status = ProductStatus.ACTIVE
                            prod.last_checked = datetime.now()
                            session.add(PriceHistory(product_id=prod.id, price=price))
                        else:
                            prod.status = ProductStatus.ERROR
                            prod.last_checked = datetime.now()
                        
                        if data.get("image_url"):
                            prod.image_url = data["image_url"]
                    
                    await session.commit()
                
            except asyncio.TimeoutError:
                logger.warning("⚠️ Güncelleme batch timeout")
            except Exception as e:
                logger.error(f"❌ Güncelleme batch error: {e}", exc_info=True)
            
            if not scan_manager.should_stop:
                await asyncio.sleep(0.5)
    
    finally:
        await scan_manager.finish_scan()
        asyncio.create_task(check_pending_items())


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    try:
        await init_db()
        await start_browser()
        logger.info("✅ Server başlatıldı")
        
        scheduler = AsyncIOScheduler()
        scheduler.add_job(update_all_products, 'interval', hours=SCAN_INTERVAL_HOURS)
        scheduler.add_job(cleanup_old_data, 'interval', days=1)
        scheduler.start()
        
        yield
        
    except Exception as e:
        logger.critical(f"Startup failed: {e}", exc_info=True)
        raise
    finally:
        try:
            if scan_manager.is_scanning:
                await scan_manager.stop_scan()
                await asyncio.sleep(2)
            await stop_browser()
            await close_db()
            logger.info("🛑 Server kapatıldı")
        except Exception as e:
            logger.error(f"Shutdown error: {e}", exc_info=True)


app = FastAPI(lifespan=lifespan, title="Fiyat Takip v8.0")

try:
    templates = Jinja2Templates(directory="templates")
except Exception as e:
    logger.warning(f"Templates directory issue: {e}")


# ============================================================================
# PUBLIC ENDPOINTS
# ============================================================================
@app.get("/", response_class=HTMLResponse)
async def home(
    request: Request,
    page: int = Query(1, ge=1),
    session: AsyncSession = Depends(get_session)
):
    """Main dashboard."""
    offset = (page - 1) * ITEMS_PER_PAGE
    
    # Get products
    query = select(Product).where(
        Product.status == ProductStatus.ACTIVE
    ).order_by(Product.is_favorite.desc(), Product.last_checked.desc())
    
    result = await session.execute(query.offset(offset).limit(ITEMS_PER_PAGE))
    products = result.scalars().all()
    
    # Get counts
    total_result = await session.execute(
        select(func.count()).select_from(Product).where(Product.status == ProductStatus.ACTIVE)
    )
    total = total_result.scalar() or 0
    
    discounts_result = await session.execute(
        select(func.count()).select_from(Product).where(
            Product.status == ProductStatus.ACTIVE,
            Product.current_price > 0,
            Product.current_price < Product.lowest_price
        )
    )
    discounts = discounts_result.scalar() or 0
    
    total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    product_names = json.dumps({str(p.id): p.name for p in products})
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "products": products,
        "total": total,
        "discounts": discounts,
        "is_scanning": scan_manager.is_scanning,
        "current_page": page,
        "total_pages": total_pages,
        "items_per_page": ITEMS_PER_PAGE,
        "product_names_json": product_names
    })


@app.get("/terminal", response_class=HTMLResponse)
async def terminal_page(request: Request):
    """Live terminal page."""
    return templates.TemplateResponse("terminal.html", {"request": request})


@app.get("/stream-logs")
async def stream_logs(request: Request):
    """Server-Sent Events for log streaming."""
    async def event_generator():
        while True:
            if await request.is_disconnected():
                break
            try:
                data = await asyncio.wait_for(log_queue.get(), timeout=1.0)
                yield f"data: {data}\n\n"
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/add")
async def add_product(
    request: Request,
    bg: BackgroundTasks,
    url: str = Form(...),
    session: AsyncSession = Depends(get_session)
):
    """Add a new product or category URL."""
    is_valid, msg = validate_url(url)
    if not is_valid:
        raise HTTPException(status_code=400, detail=msg)
    
    try:
        url_type, domain = URLAnalyzer.analyze(url)
        
        if url_type == "category":
            # Check if category already exists
            result = await session.execute(
                select(Category).where(Category.url == url)
            )
            if not result.scalar_one_or_none():
                session.add(Category(url=url, domain=domain, name=f"{domain} Kategori"))
                await session.commit()
            
            links = await discover_links(url)
            
            if links:
                count = 0
                for link in links:
                    result = await session.execute(
                        select(Product).where(Product.url == link)
                    )
                    if not result.scalar_one_or_none():
                        session.add(Product(url=link, name="Yeni Ürün", status=ProductStatus.PENDING))
                        count += 1
                await session.commit()
                
                if not scan_manager.is_scanning:
                    bg.add_task(process_bulk_list, links)
                
                return JSONResponse({
                    "success": True,
                    "message": f"{count} yeni ürün eklendi",
                    "count": len(links)
                })
            else:
                raise HTTPException(status_code=400, detail="Ürün bulunamadı")
        else:
            # Single product
            result = await session.execute(
                select(Product).where(Product.url == url)
            )
            if not result.scalar_one_or_none():
                session.add(Product(url=url, name="Yeni Ürün", status=ProductStatus.PENDING))
                await session.commit()
            
            if not scan_manager.is_scanning:
                bg.add_task(process_bulk_list, [url])
            
            return JSONResponse({
                "success": True,
                "message": "Ürün eklendi, işleniyor",
                "count": 1
            })
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Add product error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/history/{pid}")
async def get_history(pid: int, session: AsyncSession = Depends(get_session)):
    """Get price history for a product."""
    result = await session.execute(
        select(PriceHistory).where(
            PriceHistory.product_id == pid
        ).order_by(PriceHistory.timestamp)
    )
    history = result.scalars().all()
    
    return JSONResponse({
        "labels": [h.timestamp.strftime("%d.%m %H:%M") for h in history],
        "prices": [h.price for h in history]
    })


@app.get("/api/status")
async def api_status(session: AsyncSession = Depends(get_session)):
    """Get current system status."""
    total_result = await session.execute(
        select(func.count()).select_from(Product)
    )
    total = total_result.scalar() or 0
    
    active_result = await session.execute(
        select(func.count()).select_from(Product).where(Product.status == ProductStatus.ACTIVE)
    )
    active = active_result.scalar() or 0
    
    return JSONResponse({
        "is_scanning": scan_manager.is_scanning,
        "total": total,
        "active": active
    })


# ============================================================================
# PROTECTED ENDPOINTS (Now open for local development)
# ============================================================================
@app.api_route("/scan-now", methods=["GET", "POST"])
async def scan_now(
    bg: BackgroundTasks,
    _: bool = Depends(verify_admin_header)
):
    """Trigger a full product update scan."""
    if scan_manager.is_scanning:
        return RedirectResponse(url="/", status_code=303)
    
    bg.add_task(update_all_products)
    return RedirectResponse(url="/", status_code=303)


@app.api_route("/stop-scan", methods=["GET", "POST"])
async def stop_scan(_: bool = Depends(verify_admin_header)):
    """Stop the current scan."""
    await scan_manager.stop_scan()
    return RedirectResponse(url="/", status_code=303)


@app.api_route("/delete/{pid}", methods=["GET", "DELETE"])
async def delete_product(
    pid: int,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_header)
):
    """Delete a product and its price history."""
    result = await session.execute(
        select(Product).where(Product.id == pid)
    )
    product = result.scalar_one_or_none()
    
    if product:
        await session.delete(product)
        await session.commit()
        return RedirectResponse(url="/", status_code=303)
    
    raise HTTPException(status_code=404, detail="Ürün bulunamadı")


@app.api_route("/favorite/{pid}", methods=["GET", "POST"])
async def toggle_favorite(
    pid: int,
    session: AsyncSession = Depends(get_session)
):
    """Toggle favorite status for a product."""
    result = await session.execute(
        select(Product).where(Product.id == pid)
    )
    product = result.scalar_one_or_none()
    
    if product:
        product.is_favorite = not product.is_favorite
        await session.commit()
        return RedirectResponse(url="/", status_code=303)
    
    raise HTTPException(status_code=404, detail="Ürün bulunamadı")


@app.api_route("/reset", methods=["GET", "POST"])
async def reset_system(_: bool = Depends(verify_admin_header)):
    """Reset the scanning system."""
    scan_manager.reset()
    await force_restart_browser()
    return RedirectResponse(url="/", status_code=303)
