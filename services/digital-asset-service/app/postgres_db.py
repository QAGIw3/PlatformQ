import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

# In a real application, these would be managed more securely (e.g., Vault)
# For now, we'll use environment variables as is common in containerized apps.
SQLALCHEMY_DATABASE_URL = os.getenv("POSTGRES_DATABASE_URL", "postgresql://user:password@localhost/platformq_assets")

engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Dependency for FastAPI
def get_db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 