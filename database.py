from sqlmodel import SQLModel, create_engine, Session

# SQLite veritabanı dosyası adı
sqlite_file_name = "prices.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

# check_same_thread=False, API isteklerinde thread sorunları yaşamamak için gereklidir (SQLite için)
engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})

from sqlalchemy import event

# WAL modunu aktifleştir
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA busy_timeout=5000;")  # 5 saniye bekle
    cursor.close()

def create_db_and_tables():
    """Veritabanını ve tabloları oluşturur.
    
    NOT: Şu anda bu fonksiyon sadece yeni tabloları oluşturur.
    Mevcut tabloların şema değişiklikleri için Alembic kullanılmalıdır.
    
    Alembic entegrasyonu için:
        1. pip install alembic
        2. alembic init alembic
        3. alembic.ini ve env.py dosyalarını yapılandır
        4. alembic revision --autogenerate -m "init"
        5. alembic upgrade head
    
    Daha fazla bilgi: https://alembic.sqlalchemy.org/
    """
    SQLModel.metadata.create_all(engine)

def get_session():
    """Dependency Injection için session sağlar."""
    with Session(engine) as session:
        yield session
