
import sqlalchemy
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime

DATABASE_URL = "sqlite:///./functions.db"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# --- Database Model ---

class WasmModule(Base):
    __tablename__ = "wasm_modules"

    id = Column(String, primary_key=True, index=True)
    description = Column(String)
    filepath = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_db_and_tables():
    Base.metadata.create_all(bind=engine) 