from typing import Optional, List
from sqlmodel import SQLModel, Field, Relationship
from datetime import datetime
from enum import Enum

class ProductStatus(str, Enum):
    """Ürün veri kalitesi durumu."""
    ACTIVE = "active"       # Fiyat ve isim başarıyla çekildi
    PROCESSING = "processing"  # Şu anda taranıyor
    ERROR = "error"         # Fiyat veya isim çekilemedi
    PENDING = "pending"     # Henüz taranmadı

# Ürün Modeli
class Product(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str  # Ürün adı
    url: str   # Takip edilen ürün linki
    current_price: float = 0.0  # Şuan ki fiyat
    lowest_price: float = 0.0   # Şimdiye kadar görülmüş en düşük fiyat
    last_checked: datetime = Field(default_factory=datetime.now) # Son kontrol zamanı
    image_url: Optional[str] = None # Ürün resmi (otomatik çekilecek)
    is_favorite: bool = Field(default=False) # Favori durumu
    target_price: float = Field(default=0.0) # Hedef fiyat
    status: str = Field(default=ProductStatus.PENDING) # Veri kalitesi durumu
    error_message: Optional[str] = None # Hata detayı (opsiyonel)


    # İlişkiler (Opsiyonel, geçmiş verisine erişmek için)
    price_history: List["PriceHistory"] = Relationship(back_populates="product")

# Fiyat Geçmişi Modeli (Grafik veya analiz için)
class PriceHistory(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(foreign_key="product.id")
    price: float
    timestamp: datetime = Field(default_factory=datetime.now)

    product: Optional[Product] = Relationship(back_populates="price_history")
