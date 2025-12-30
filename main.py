import asyncio
import sys
import threading
import logging
import re
import json
from queue import Queue, Empty
from urllib.parse import urlparse
from typing import Optional

# Logging Setup
# Custom Handler for Streaming Logs
log_queue = asyncio.Queue()

class QueueHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            if log_queue.qsize() > 100: # Prune if too full
                try:
                    log_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            log_queue.put_nowait(msg)
        except Exception:
            self.handleError(record)

# ColorLog Setup
import colorlog

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S',
    reset=True,
    log_colors={
        'DEBUG':    'cyan',
        'INFO':     'green',
        'WARNING':  'yellow',
        'ERROR':    'red',
        'CRITICAL': 'red,bg_white',
    },
    secondary_log_colors={},
    style='%'
))

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        handler,         # Console (Renkli)
    ]
)
logger = logging.getLogger("FiyatTakip")

# Add our custom handler for web stream (still useful if needed internally)
queue_handler = QueueHandler()
queue_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(queue_handler)

# Capture scraper logs
logging.getLogger("FiyatTakip.Scraper").addHandler(queue_handler)

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI, Request, Form, Depends, BackgroundTasks, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from sqlmodel import Session, select, func
from contextlib import asynccontextmanager
from typing import List
from datetime import datetime

from database import create_db_and_tables, get_session, engine
from models import Product, PriceHistory, ProductStatus
from scraper import get_product_data, get_products_batch, discover_links, start_browser, stop_browser, URLAnalyzer
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# GLOBAL STATES (with thread-safe locking)
GLOBAL_STOP_EVENT = threading.Event()
IS_SCANNING = False
IS_SCANNING_LOCK = threading.Lock()

from config import ITEMS_PER_PAGE, ALLOWED_DOMAINS, SCAN_INTERVAL_HOURS

# Allowed URL schemes for security
ALLOWED_SCHEMES = {'http', 'https'}

def validate_url(url: str) -> tuple[bool, str]:
    """Validate URL for security. Returns (is_valid, error_message)."""
    if not url or not url.strip():
        return False, "URL boş olamaz"
    
    try:
        parsed = urlparse(url.strip())
        
        # Check scheme
        if parsed.scheme not in ALLOWED_SCHEMES:
            return False, f"Geçersiz URL şeması: {parsed.scheme}. Sadece http/https desteklenir."
        
        # Check domain exists
        if not parsed.netloc:
            return False, "Geçersiz URL formatı"
        
        # Check against allowed domains (SSRF protection)
        domain = parsed.netloc.lower()
        if not any(allowed in domain for allowed in ALLOWED_DOMAINS):
            return False, f"Bu site desteklenmiyor: {domain}. Desteklenen siteler: Trendyol, Hepsiburada, Amazon.com.tr, N11, Akakce, Cimri, Epey"
        
        return True, ""
    except Exception as e:
        return False, f"URL ayrıştırma hatası: {e}"

def get_scanning_status() -> bool:
    """Thread-safe getter for IS_SCANNING."""
    with IS_SCANNING_LOCK:
        return IS_SCANNING

def set_scanning_status(value: bool):
    """Thread-safe setter for IS_SCANNING."""
    global IS_SCANNING
    with IS_SCANNING_LOCK:
        IS_SCANNING = value

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    start_browser()
    logger.info("✅ Server başlatıldı.")
    
    scheduler = AsyncIOScheduler()
    scheduler.add_job(update_all_products, 'interval', hours=SCAN_INTERVAL_HOURS) 
    scheduler.start()
    yield
    
    # Graceful shutdown: wait for scanning to complete or force stop
    if get_scanning_status():
        logger.info("⏳ Tarama devam ediyor, durduruluyor...")
        GLOBAL_STOP_EVENT.set()
        # Give some time for tasks to notice the stop event
        await asyncio.sleep(2)
    
    stop_browser()
    logger.info("🛑 Server kapatıldı.")

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# === TASKS ===

async def process_bulk_list(urls: List[str]):
    """Toplu URL işleme - batch paralel tarama ile."""
    if get_scanning_status():
        logger.warning("⚠️ Zaten tarama yapılıyor, atlanıyor.")
        return
    
    set_scanning_status(True)
    GLOBAL_STOP_EVENT.clear()
    
    BATCH_SIZE = 5  # Aynı anda 5 URL tara
    
    try:
        logger.info(f"📦 Bulk Task: {len(urls)} URL işlenecek (Batch: {BATCH_SIZE})")
        
        # URL'leri batch'lere böl
        for batch_start in range(0, len(urls), BATCH_SIZE):
            if GLOBAL_STOP_EVENT.is_set():
                logger.info("🛑 Bulk Task durduruldu.")
                break
            
            batch_urls = urls[batch_start:batch_start + BATCH_SIZE]
            batch_num = (batch_start // BATCH_SIZE) + 1
            total_batches = (len(urls) + BATCH_SIZE - 1) // BATCH_SIZE
            
            logger.info(f"📦 Batch {batch_num}/{total_batches}: {len(batch_urls)} URL taranıyor...")
            
            try:
                # Paralel tarama
                results = await asyncio.to_thread(get_products_batch, batch_urls)
                
                # Tek transaction'da veritabanına yaz (Thread-safe wrapper)
                def _save_batch_to_db(batch_results):
                    with Session(engine) as session:
                        for data in batch_results:
                            url = data.get("url", "")
                            if not url:
                                continue
                            
                            # Validation: Skip invalid data
                            is_valid = data.get("is_valid", False)
                            
                            existing = session.exec(select(Product).where(Product.url == url)).first()
                            product = existing if existing else Product(url=url, name=data.get("name", "Yeni Ürün"))
                            
                            new_price = data.get("price", 0.0)
                            
                            if is_valid and new_price > 0:
                                product.current_price = new_price
                                if product.lowest_price == 0 or new_price < product.lowest_price:
                                    product.lowest_price = new_price
                                product.status = ProductStatus.ACTIVE
                                product.error_message = None
                            else:
                                product.status = ProductStatus.ERROR
                                product.error_message = data.get("error_message", "Veri çekilemedi")
                            
                            if data.get("image_url"):
                                product.image_url = data["image_url"]
                            if data.get("name") and data["name"] not in ["Hata", "Bilinmeyen Ürün"]:
                                if "Yeni Ürün" in product.name or "Bilinmeyen" in product.name:
                                    product.name = data["name"]
                            
                            product.last_checked = datetime.now()
                            session.add(product)
                            session.flush() # ID alabilmek için
                            
                            if new_price > 0:
                                session.add(PriceHistory(product_id=product.id, price=new_price))
                        
                        session.commit()

                await asyncio.to_thread(_save_batch_to_db, results)
                
                logger.info(f"✅ Batch {batch_num} tamamlandı.")
                
            except Exception as e:
                logger.error(f"❌ Batch Error: {e}")
            
            # Batch'ler arası kısa bekleme (anti-bot)
            if not GLOBAL_STOP_EVENT.is_set() and batch_start + BATCH_SIZE < len(urls):
                await asyncio.sleep(0.5)
        
        logger.info("✅ Bulk Task tamamlandı.")

    except Exception as e:
        logger.error(f"❌ Bulk Task Critical Error: {e}")
    finally:
        set_scanning_status(False)

async def update_all_products():
    """Tüm ürünleri güncelle - batch paralel tarama ile."""
    if get_scanning_status():
        logger.warning("⚠️ Zaten tarama yapılıyor, atlanıyor.")
        return
    
    set_scanning_status(True)
    GLOBAL_STOP_EVENT.clear()
    
    BATCH_SIZE = 5  # Aynı anda 5 ürün tara
    
    try:
        logger.info("🚀 Otomatik/Manuel Tarama başladı (Batch Mode)...")
        
        # 1. Ürünleri getir (sadece ACTIVE ve ERROR durumundakileri)
        # 1. Ürünleri getir (sadece ACTIVE ve ERROR durumundakileri)
        def _fetch_products_to_scan():
            with Session(engine) as session:
                products = session.exec(
                    select(Product).where(Product.status.in_([ProductStatus.ACTIVE, ProductStatus.ERROR, ProductStatus.PENDING]))
                ).all()
                # Detach objects or copy data, because session closes
                p_list = [(p.id, p.url, p.name) for p in products]
                
                # Tüm ürünleri PROCESSING olarak işaretle
                for p in products:
                    p.status = ProductStatus.PROCESSING
                session.commit()
                return p_list

        product_list = await asyncio.to_thread(_fetch_products_to_scan)
        
        total_products = len(product_list)
        total_batches = (total_products + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(f"📋 Toplam {total_products} ürün, {total_batches} batch'te taranacak.")
        
        updated_count = 0
        error_count = 0
        
        # 2. Batch'ler halinde tara
        for batch_start in range(0, total_products, BATCH_SIZE):
            if GLOBAL_STOP_EVENT.is_set():
                logger.info("🛑 Tarama durduruldu.")
                break
            
            batch = product_list[batch_start:batch_start + BATCH_SIZE]
            batch_urls = [url for _, url, _ in batch]
            batch_num = (batch_start // BATCH_SIZE) + 1
            
            logger.info(f"🔄 Batch {batch_num}/{total_batches}: {len(batch)} ürün taranıyor...")
            
            try:
                # Paralel tarama
                results = await asyncio.to_thread(get_products_batch, batch_urls)
                
                # URL -> result mapping
                url_to_result = {r.get("url", ""): r for r in results}
                
                # Tek transaction'da güncelle
                # Tek transaction'da güncelle (Thread-safe wrapper)
                def _update_batch_in_db(batch_items, url_map):
                    with Session(engine) as session:
                        for pid, url, old_name in batch_items:
                            data = url_map.get(url, {})
                            new_price = data.get("price", 0.0)
                            is_valid = data.get("is_valid", False)
                            
                            p = session.get(Product, pid)
                            if p:
                                if is_valid and new_price > 0:
                                    p.current_price = new_price
                                    if p.lowest_price == 0 or new_price < p.lowest_price:
                                        p.lowest_price = new_price
                                    
                                    # İsmi güncelle (Bilinmeyen veya eksikse)
                                    new_name = data.get("name", "")
                                    if new_name and new_name not in ["Hata", "Bilinmeyen Ürün"]:
                                        if "Bilinmeyen" in p.name or "Yeni Ürün" in p.name or len(p.name) < 5:
                                            p.name = new_name
                                    
                                    p.status = ProductStatus.ACTIVE
                                    p.error_message = None
                                    p.last_checked = datetime.now()
                                    session.add(p)
                                    session.add(PriceHistory(product_id=p.id, price=new_price))
                                    # updated_count artışı context dışında yapılacak veya dönüş değeri olacak
                                else:
                                    p.status = ProductStatus.ERROR
                                    p.error_message = data.get("error_message", "Veri çekilemedi")
                                    p.last_checked = datetime.now()
                                    session.add(p)
                                    # error_count artışı context dışında
                        
                        session.commit()

                await asyncio.to_thread(_update_batch_in_db, batch, url_to_result)
                
                # İstatistikleri güncelle (basitçe her batch'teki başarılı/başarısız sayısını tahmin et)
                # Tam kesin sayı için DB'den dönen değer gerekir ama şimdilik log için yeterli
                current_errors = sum(1 for r in results if not r.get("is_valid", False))
                error_count += current_errors
                updated_count += (len(batch) - current_errors)
                
                logger.info(f"✅ Batch {batch_num} tamamlandı.")
                
            except Exception as e:
                logger.error(f"❌ Batch {batch_num} Error: {e}")
                error_count += len(batch)
            
            # Batch'ler arası kısa bekleme
            if not GLOBAL_STOP_EVENT.is_set() and batch_start + BATCH_SIZE < total_products:
                await asyncio.sleep(0.5)
        
        logger.info(f"🏁 Tarama tamamlandı. ✅ {updated_count} güncellendi, ⚠️ {error_count} hata.")
        
    except Exception as e:
        logger.error(f"❌ Critical Update Loop Error: {e}")
    finally:
        set_scanning_status(False)

# === ENDPOINTS ===

@app.get("/", response_class=HTMLResponse)
async def home(
    request: Request, 
    page: int = Query(1, ge=1),
    session: Session = Depends(get_session)
):
    """Ana sayfa - Sayfalama destekli."""
    try:
        offset = (page - 1) * ITEMS_PER_PAGE
        
        # Sadece ACTIVE ürünleri göster (ERROR ve PENDING gizle)
        query = select(Product).where(
            Product.status == ProductStatus.ACTIVE
        ).order_by(Product.is_favorite.desc(), Product.last_checked.desc())
        
        # Toplam sayı
        total_query = select(func.count()).select_from(Product).where(Product.status == ProductStatus.ACTIVE)
        total = session.exec(total_query).one()
        
        # Sayfalanmış sonuçlar
        products = session.exec(query.offset(offset).limit(ITEMS_PER_PAGE)).all()
        
        # İndirimler (sadece ACTIVE olanlar arasında)
        discounts = session.exec(
            select(func.count()).select_from(Product).where(
                Product.status == ProductStatus.ACTIVE,
                Product.current_price > 0,
                Product.current_price < Product.lowest_price
            )
        ).one()
        
        # Sayfa bilgileri
        total_pages = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        
        # JSON Injection Fix: Pre-calculate safe JSON
        product_names_json = json.dumps({str(p.id): p.name for p in products})
        
        return templates.TemplateResponse("index.html", {
            "request": request, 
            "products": products, 
            "total": total, 
            "discounts": discounts, 
            "is_scanning": get_scanning_status(),
            "current_page": page,
            "total_pages": total_pages,
            "items_per_page": ITEMS_PER_PAGE,
            "product_names_json": product_names_json
        })
    except Exception as e:
        import traceback
        print("=" * 50)
        print(f"HOME ERROR: {e}")
        traceback.print_exc()
        print("=" * 50)
        logger.exception(f"❌ Home Error: {e}")
        raise

@app.get("/stream-logs")
async def stream_logs(request: Request):
    async def event_generator():
        while True:
            if await request.is_disconnected():
                break
            try:
                # Wait for log with timeout to allow periodic connection check
                data = await asyncio.wait_for(log_queue.get(), timeout=1.0)
                yield f"data: {data}\n\n"
            except asyncio.TimeoutError:
                # Send keepalive comment
                yield ": keepalive\n\n"
            except Exception as e:
                yield f"data: Log Error: {e}\n\n"
                break
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/add")
async def add(request: Request, bg: BackgroundTasks, url: str = Form(...), session: Session = Depends(get_session)):
    """
    Ürün ekleme - JSON yanıt döndürür (AJAX uyumlu).
    Geçersiz URL'lerde 400 hatası döndürür -> Frontend'de Toast gösterilir.
    """
    # Validate URL for security
    is_valid, error_msg = validate_url(url)
    if not is_valid:
        logger.warning(f"⚠️ Invalid URL rejected: {url[:50]}... - {error_msg}")
        raise HTTPException(status_code=400, detail=error_msg)
    
    # Simple Referer check
    referer = request.headers.get("referer", "")
    host = request.headers.get("host", "")
    if referer and host and host not in referer:
        logger.warning(f"⚠️ Possible CSRF attempt blocked. Referer: {referer}")
        raise HTTPException(status_code=403, detail="Güvenlik hatası")
    
    try:
        # URL Analizi: Kategori mi Ürün mü?
        url_type, domain = URLAnalyzer.analyze(url)
        
        if url_type == "category":
            # Kategori
            # Kategorileri DB'ye eklemiyoruz, direkt tarıyoruz (şimdilik)
            # Ama veri kaybı olmaması için burayı da iyileştirebiliriz.
            # Şimdilik sadece ürünler için "Pending" mantığı uyguluyoruz.
            links = await asyncio.to_thread(discover_links, url)
            if links:
                logger.info(f"🕷️ Kategori tespit edildi. {len(links)} ürün bulundu.")
                
                # Bulunan linkleri DB'ye ekle
                count_new = 0
                for link in links:
                    existing = session.exec(select(Product).where(Product.url == link)).first()
                    if not existing:
                        session.add(Product(url=link, name="Yeni Ürün", status=ProductStatus.PENDING))
                        count_new += 1
                session.commit()
                
                if not get_scanning_status():
                    bg.add_task(process_bulk_list, links)
                    return JSONResponse({
                        "success": True,
                        "message": f"Kategori taranıyor. {count_new} yeni ürün eklendi.",
                        "count": len(links)
                    })
                else:
                    return JSONResponse({
                        "success": True,
                        "message": f"{count_new} ürün sıraya eklendi. Tarama bitince işlenecek.",
                        "count": len(links)
                    })
            else:
                raise HTTPException(status_code=400, detail="Kategoride ürün bulunamadı.")
        else:
            # Tek ürün
            # ÖNCE DB'ye kaydet
            existing = session.exec(select(Product).where(Product.url == url)).first()
            if not existing:
                session.add(Product(url=url, name="Yeni Ürün", status=ProductStatus.PENDING))
                session.commit()
            
            # Sonra tarama durumuna göre işlem
            if not get_scanning_status():
                bg.add_task(process_bulk_list, [url])
                return JSONResponse({
                    "success": True,
                    "message": "Ürün arka planda işleniyor.",
                    "count": 1
                })
            else:
                return JSONResponse({
                    "success": True,
                    "message": "Ürün sıraya eklendi. Tarama bitince işlenecek.",
                    "count": 1
                })
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Add Error: {e}")
        raise HTTPException(status_code=500, detail=f"Beklenmeyen hata: {str(e)}")

@app.get("/scan-now")
async def scan(bg: BackgroundTasks):
    logger.info("👉 'Tümünü Tara' isteği alındı.")
    bg.add_task(update_all_products)
    return RedirectResponse("/", status_code=303)

@app.get("/stop-scan")
async def stop_scan():
    GLOBAL_STOP_EVENT.set()
    logger.info("🛑 Stop sinyali gönderildi.")
    return RedirectResponse("/", status_code=303)

@app.get("/delete/{pid}")
async def delete(pid: int, session: Session = Depends(get_session)):
    try:
        histories = session.exec(select(PriceHistory).where(PriceHistory.product_id == pid)).all()
        for h in histories:
            session.delete(h)
        
        p = session.get(Product, pid)
        if p:
            session.delete(p)
        session.commit()
        logger.info(f"🗑️ Ürün silindi: ID={pid}")
    except Exception as e:
        logger.error(f"❌ Delete Error: {e}")
    return RedirectResponse("/", status_code=303)

@app.get("/history/{pid}")
async def hist(pid: int):
    
    def _fetch_history(product_id):
        with Session(engine) as session:
            h = session.exec(select(PriceHistory).where(PriceHistory.product_id==product_id).order_by(PriceHistory.timestamp)).all()
            # Dict'e çevir
            return [{"timestamp": x.timestamp, "price": x.price} for x in h]

    history = await asyncio.to_thread(_fetch_history, pid)
    return JSONResponse({"labels": [x["timestamp"].strftime("%d.%m %H:%M") for x in history], "prices": [x["price"] for x in history]})

@app.get("/favorite/{pid}")
async def fav(pid: int, session: Session = Depends(get_session)):
    try:
        p = session.get(Product, pid)
        if p:
            p.is_favorite = not p.is_favorite
            session.add(p)
            session.commit()
    except Exception as e:
        logger.error(f"❌ Favorite Error: {e}")
    return RedirectResponse("/", status_code=303)

@app.get("/reset")
async def reset_system():
    """Sistemi sifirla: IS_SCANNING flag'ini temizle ve BrowserActor'u yeniden baslat."""
    logger.info("🔄 Sistem sifirlanıyor...")
    
    GLOBAL_STOP_EVENT.clear()
    set_scanning_status(False)
    
    try:
        from scraper import force_restart_browser
        await asyncio.to_thread(force_restart_browser)
        logger.info("✅ Sistem sifirlandı.")
    except Exception as e:
        logger.error(f"❌ Reset Error: {e}")
    
    return RedirectResponse("/", status_code=303)

@app.get("/api/status")
async def api_status():
    """API endpoint: Tarama durumu ve istatistikler (Async Read)."""
    
    def _fetch_stats():
        with Session(engine) as session:
            total = session.exec(select(func.count()).select_from(Product)).one()
            active = session.exec(select(func.count()).select_from(Product).where(Product.status == ProductStatus.ACTIVE)).one()
            errors = session.exec(select(func.count()).select_from(Product).where(Product.status == ProductStatus.ERROR)).one()
            processing = session.exec(select(func.count()).select_from(Product).where(Product.status == ProductStatus.PROCESSING)).one()
            return total, active, errors, processing

    total, active, errors, processing = await asyncio.to_thread(_fetch_stats)
    
    return JSONResponse({
        "is_scanning": get_scanning_status(),
        "total_products": total,
        "active": active,
        "errors": errors,
        "processing": processing
    })
