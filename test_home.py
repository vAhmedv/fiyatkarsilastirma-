#!/usr/bin/env python
"""Test the home endpoint logic directly to capture any exceptions."""
import traceback
from sqlmodel import Session, select, func
from database import engine
from models import Product, ProductStatus, PriceHistory
from config import ITEMS_PER_PAGE
from jinja2 import Environment, FileSystemLoader
import json

def test_home():
    print("Testing home endpoint logic...")
    
    try:
        page = 1
        offset = (page - 1) * ITEMS_PER_PAGE
        
        with Session(engine) as session:
            print(f"  1. ITEMS_PER_PAGE: {ITEMS_PER_PAGE}")
            
            # Query
            query = select(Product).where(
                Product.status == ProductStatus.ACTIVE
            ).order_by(Product.is_favorite.desc(), Product.last_checked.desc())
            
            # Total count
            total_query = select(func.count()).select_from(Product).where(Product.status == ProductStatus.ACTIVE)
            total = session.exec(total_query).one()
            print(f"  2. Total count: {total}")
            
            # Products
            products = session.exec(query.offset(offset).limit(ITEMS_PER_PAGE)).all()
            print(f"  3. Products fetched: {len(products)}")
            
            # Discounts
            discounts = session.exec(
                select(func.count()).select_from(Product).where(
                    Product.status == ProductStatus.ACTIVE,
                    Product.current_price > 0,
                    Product.current_price < Product.lowest_price
                )
            ).one()
            print(f"  4. Discounts: {discounts}")
            
            # Page info
            total_pages = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            print(f"  5. Total pages: {total_pages}")
            
            # JSON
            product_names_json = json.dumps({str(p.id): p.name for p in products})
            print(f"  6. JSON created, length: {len(product_names_json)}")
            
            # Template
            print("  7. Loading template...")
            env = Environment(loader=FileSystemLoader("templates"))
            template = env.get_template("index.html")
            
            # Render
            print("  8. Rendering template...")
            html = template.render(
                products=products,
                total=total,
                discounts=discounts,
                is_scanning=False,
                current_page=page,
                total_pages=total_pages,
                items_per_page=ITEMS_PER_PAGE,
                product_names_json=product_names_json,
                request=None  # This might be the issue
            )
            print(f"  9. Rendered! HTML length: {len(html)}")
            print("\n✅ SUCCESS! Home endpoint logic works correctly.")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("\nFull traceback:")
        traceback.print_exc()

if __name__ == "__main__":
    test_home()
