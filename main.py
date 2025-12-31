import asyncio
import sys
import threading
import logging
import json
from queue import Queue, Empty
from urllib.parse import urlparse
from typing import List, Optional
from datetime import datetime
import colorlog
from fastapi import FastAPI, Request, Form, Depends, BackgroundTasks, Query, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from sqlmodel import Session, select, func
from contextlib import asynccontextmanager

from database import create_db_and_tables, get_session, engine
from models import Product, PriceHistory, ProductStatus, Category
from scraper import get_product_data, get_products_batch, discover_links, start_browser, stop_browser, force_restart_browser, URLAnalyzer
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from config import ITEMS_PER_PAGE, ALLOWED_DOMAINS, SCAN_INTERVAL_HOURS

# LOGGING
log_queue = asyncio.Queue()
class QueueHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            if log_queue.qsize() > 100:
                try: log_queue.get_nowait()
                except: pass
            log_queue.put_nowait(msg)
        except: self.handleError(record)

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S',
    reset=True,
    log_colors={'DEBUG':'cyan', 'INFO':'green', 'WARNING':'yellow', 'ERROR':'red', 'CRITICAL':'red,bg_white'}
))
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger("FiyatTakip")
queue_handler = QueueHandler()
queue_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(queue_handler)
logging.getLogger("FiyatTakip.Scraper").addHandler(queue_handler)

GLOBAL_STOP_EVENT = threading.Event()
IS_SCANNING = False
IS_SCANNING_LOCK = threading.Lock()
ALLOWED_SCHEMES = {'http', 'https'}

def validate_url(url: str) -> tuple[bool, str]:
    if not url or not url.strip(): return False, "URL boş olamaz"
    try:
        parsed = urlparse(url.strip())
        if parsed.scheme not in ALLOWED_SCHEMES: return False, "Geçersiz şema"
        if not parsed.netloc: return False, "Geçersiz format"
        domain = parsed.netloc.lower()
        if not any(a in domain for a in ALLOWED_DOMAINS): return False, "Desteklenmeyen site"
        return True, ""
    except Exception as e: return False, str(e)

def get_scanning_status() -> bool:
    with IS_SCANNING_LOCK: return IS_SCANNING
def set_scanning_status(value: bool):
    global IS_SCANNING
    with IS_SCANNING_LOCK: IS_SCANNING = value

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        create_db_and_tables()
        await start_browser()
        logger.info("✅ Server başlatıldı.")
        scheduler = AsyncIOScheduler()
        scheduler.add_job(update_all_products, 'interval', hours=SCAN_INTERVAL_HOURS) 
        scheduler.start()
        yield
    except Exception as e:
        logger.critical(f"Startup Failed: {e}")
        raise
    finally:
        try:
            if get_scanning_status():
                GLOBAL_STOP_EVENT.set()
                await asyncio.sleep(2)
            await stop_browser()
            logger.info("🛑 Server kapatıldı.")
        except Exception as e: logger.error(f"Shutdown error: {e}")

app = FastAPI(lifespan=lifespan)
try: templates = Jinja2Templates(directory="templates")
except: logger.warning("Templates directory not found!")

async def process_bulk_list(urls: List[str]):
    if get_scanning_status(): return
    set_scanning_status(True)
    GLOBAL_STOP_EVENT.clear()
    BATCH_SIZE = 10 
    try:
        logger.info(f"📦 Bulk Task: {len(urls)} URL (Batch: {BATCH_SIZE})")
        for batch_start in range(0, len(urls), BATCH_SIZE):
            if GLOBAL_STOP_EVENT.is_set(): break
            batch_urls = urls[batch_start:batch_start + BATCH_SIZE]
            try:
                results = await asyncio.wait_for(get_products_batch(batch_urls, batch_size=BATCH_SIZE), timeout=45.0)
                def _save(res):
                    with Session(engine) as session:
                        for data in res:
                            url = data.get("url", "")
                            if not url: continue
                            ex = session.exec(select(Product).where(Product.url == url)).first()
                            prod = ex if ex else Product(url=url, name=data.get("name", "Yeni Ürün"))
                            price = data.get("price", 0.0)
                            if data.get("is_valid") and price > 0:
                                prod.current_price = price
                                if prod.lowest_price == 0 or price < prod.lowest_price: prod.lowest_price = price
                                prod.status = ProductStatus.ACTIVE
                                prod.error_message = None
                            else:
                                prod.status = ProductStatus.ERROR
                                prod.error_message = data.get("error_message", "Veri çekilemedi")
                            if data.get("image_url"): prod.image_url = data["image_url"]
                            if data.get("name") and "Yeni" in prod.name: prod.name = data["name"]
                            prod.last_checked = datetime.now()
                            session.add(prod)
                            session.flush()
                            if price > 0: session.add(PriceHistory(product_id=prod.id, price=price))
                        session.commit()
                await asyncio.to_thread(_save, results)
            except asyncio.TimeoutError: logger.warning("⚠️ Batch timeout")
            except Exception as e: logger.error(f"❌ Batch Error: {e}")
            if not GLOBAL_STOP_EVENT.is_set(): await asyncio.sleep(0.5)
    finally: set_scanning_status(False)

async def update_all_products():
    if get_scanning_status(): return
    set_scanning_status(True)
    GLOBAL_STOP_EVENT.clear()
    BATCH_SIZE = 10
    try:
        def _fetch():
            with Session(engine) as session:
                prods = session.exec(select(Product).where(Product.status != "disabled")).all()
                pl = [(p.id, p.url) for p in prods]
                for p in prods: p.status = ProductStatus.PROCESSING
                session.commit()
                return pl
        plist = await asyncio.to_thread(_fetch)
        for batch_start in range(0, len(plist), BATCH_SIZE):
            if GLOBAL_STOP_EVENT.is_set(): break
            batch = plist[batch_start:batch_start + BATCH_SIZE]
            b_urls = [u for _, u in batch]
            try:
                results = await asyncio.wait_for(get_products_batch(b_urls, batch_size=BATCH_SIZE), timeout=45.0)
                url_map = {r.get("url"): r for r in results}
                def _update(items, map_data):
                    with Session(engine) as session:
                        for pid, url in items:
                            d = map_data.get(url, {})
                            p = session.get(Product, pid)
                            if p:
                                price = d.get("price", 0.0)
                                if d.get("is_valid") and price > 0:
                                    p.current_price = price
                                    if p.lowest_price == 0 or price < p.lowest_price: p.lowest_price = price
                                    if d.get("name") and "Yeni" in p.name: p.name = d["name"]
                                    p.status = ProductStatus.ACTIVE
                                    p.last_checked = datetime.now()
                                    session.add(p)
                                    session.add(PriceHistory(product_id=p.id, price=price))
                                else:
                                    p.status = ProductStatus.ERROR
                                    p.last_checked = datetime.now()
                                    session.add(p)
                        session.commit()
                await asyncio.to_thread(_update, batch, url_map)
            except: pass
            if not GLOBAL_STOP_EVENT.is_set(): await asyncio.sleep(0.5)
    finally: set_scanning_status(False)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, page: int = Query(1, ge=1), session: Session = Depends(get_session)):
    offset = (page - 1) * ITEMS_PER_PAGE
    query = select(Product).where(Product.status == ProductStatus.ACTIVE).order_by(Product.is_favorite.desc(), Product.last_checked.desc())
    total = session.exec(select(func.count()).select_from(Product).where(Product.status == ProductStatus.ACTIVE)).one()
    products = session.exec(query.offset(offset).limit(ITEMS_PER_PAGE)).all()
    discounts = session.exec(select(func.count()).select_from(Product).where(Product.status==ProductStatus.ACTIVE, Product.current_price>0, Product.current_price<Product.lowest_price)).one()
    total_pages = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    p_names = json.dumps({str(p.id): p.name for p in products})
    return templates.TemplateResponse("index.html", {
        "request": request, "products": products, "total": total, "discounts": discounts, 
        "is_scanning": get_scanning_status(), "current_page": page, "total_pages": total_pages, 
        "items_per_page": ITEMS_PER_PAGE, "product_names_json": p_names
    })

@app.get("/stream-logs")
async def stream_logs(request: Request):
    async def event_generator():
        while True:
            if await request.is_disconnected(): break
            try:
                data = await asyncio.wait_for(log_queue.get(), timeout=1.0)
                yield f"data: {data}\n\n"
            except: yield ": keepalive\n\n"
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/add")
async def add(request: Request, bg: BackgroundTasks, url: str = Form(...), session: Session = Depends(get_session)):
    is_valid, msg = validate_url(url)
    if not is_valid: raise HTTPException(status_code=400, detail=msg)
    try:
        utype, domain = URLAnalyzer.analyze(url)
        if utype == "category":
            # KATEGORİ KAYDI
            ex_cat = session.exec(select(Category).where(Category.url == url)).first()
            if not ex_cat:
                session.add(Category(url=url, domain=domain, name=f"{domain} Kategori"))
                session.commit()
            
            links = await discover_links(url)
            if links:
                cnt = 0
                for l in links:
                    if not session.exec(select(Product).where(Product.url == l)).first():
                        session.add(Product(url=l, name="Yeni Ürün", status=ProductStatus.PENDING))
                        cnt += 1
                session.commit()
                if not get_scanning_status(): bg.add_task(process_bulk_list, links)
                return JSONResponse({"success": True, "message": f"{cnt} yeni ürün eklendi", "count": len(links)})
            else: raise HTTPException(status_code=400, detail="Ürün bulunamadı")
        else:
            if not session.exec(select(Product).where(Product.url == url)).first():
                session.add(Product(url=url, name="Yeni Ürün", status=ProductStatus.PENDING))
                session.commit()
            if not get_scanning_status(): bg.add_task(process_bulk_list, [url])
            return JSONResponse({"success": True, "message": "İşleniyor", "count": 1})
    except Exception as e: raise HTTPException(500, detail=str(e))

@app.get("/scan-now")
async def scan(bg: BackgroundTasks):
    bg.add_task(update_all_products)
    return RedirectResponse("/", status_code=303)

@app.get("/stop-scan")
async def stop_scan():
    GLOBAL_STOP_EVENT.set()
    return RedirectResponse("/", status_code=303)

@app.get("/delete/{pid}")
async def delete(pid: int, session: Session = Depends(get_session)):
    try:
        hist = session.exec(select(PriceHistory).where(PriceHistory.product_id == pid)).all()
        for h in hist: session.delete(h)
        p = session.get(Product, pid)
        if p: session.delete(p)
        session.commit()
    except: pass
    return RedirectResponse("/", status_code=303)

@app.get("/history/{pid}")
async def hist(pid: int):
    def _fh(pid):
        with Session(engine) as s:
            h = s.exec(select(PriceHistory).where(PriceHistory.product_id==pid).order_by(PriceHistory.timestamp)).all()
            return [{"timestamp": x.timestamp, "price": x.price} for x in h]
    history = await asyncio.to_thread(_fh, pid)
    return JSONResponse({"labels": [x["timestamp"].strftime("%d.%m %H:%M") for x in history], "prices": [x["price"] for x in history]})

@app.get("/favorite/{pid}")
async def fav(pid: int, session: Session = Depends(get_session)):
    p = session.get(Product, pid)
    if p:
        p.is_favorite = not p.is_favorite
        session.add(p)
        session.commit()
    return RedirectResponse("/", status_code=303)

@app.get("/reset")
async def reset_system():
    GLOBAL_STOP_EVENT.clear()
    set_scanning_status(False)
    await force_restart_browser()
    return RedirectResponse("/", status_code=303)

@app.get("/api/status")
async def api_status():
    def _st():
        with Session(engine) as s:
            t = s.exec(select(func.count()).select_from(Product)).one()
            a = s.exec(select(func.count()).select_from(Product).where(Product.status == ProductStatus.ACTIVE)).one()
            return t, a
    t, a = await asyncio.to_thread(_st)
    return JSONResponse({"is_scanning": get_scanning_status(), "total": t, "active": a})
