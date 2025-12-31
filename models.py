from typing import Optional, List
from sqlmodel import SQLModel, Field, Relationship
from datetime import datetime
from enum import Enum

class ProductStatus(str, Enum):
    ACTIVE = "active"
    PROCESSING = "processing"
    ERROR = "error"
    PENDING = "pending"

class Category(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str 
    url: str = Field(index=True)
    domain: str
    product_count: int = Field(default=0)
    last_scanned: datetime = Field(default_factory=datetime.now)

class Product(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    url: str
    current_price: float = 0.0
    lowest_price: float = 0.0
    last_checked: datetime = Field(default_factory=datetime.now)
    image_url: Optional[str] = None
    is_favorite: bool = Field(default=False)
    target_price: float = Field(default=0.0)
    status: str = Field(default=ProductStatus.PENDING)
    error_message: Optional[str] = None
    price_history: List["PriceHistory"] = Relationship(back_populates="product")

class PriceHistory(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(foreign_key="product.id")
    price: float
    timestamp: datetime = Field(default_factory=datetime.now)
    product: Optional[Product] = Relationship(back_populates="price_history")
