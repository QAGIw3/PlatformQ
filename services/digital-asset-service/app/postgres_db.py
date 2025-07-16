import os
import hvac
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# In a real application, these would be managed more securely (e.g., Vault)
# For now, we'll use environment variables as is common in containerized apps.
VAULT_ADDR = os.getenv("VAULT_ADDR")
VAULT_ROLE_ID = os.getenv("VAULT_ROLE_ID")
VAULT_SECRET_ID = os.getenv("VAULT_SECRET_ID")

def get_db_credentials_from_vault():
    client = hvac.Client(url=VAULT_ADDR)
    client.auth.approle.login(
        role_id=VAULT_ROLE_ID,
        secret_id=VAULT_SECRET_ID,
    )
    creds = client.secrets.database.generate_credentials(
        name="digital-asset-service",
    )
    return creds["data"]["username"], creds["data"]["password"]

username, password = get_db_credentials_from_vault()
SQLALCHEMY_DATABASE_URL = f"postgresql://{username}:{password}@postgres:5432/platformq?sslmode=disable"

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