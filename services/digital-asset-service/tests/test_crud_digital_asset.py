import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import uuid
import unittest

from app.crud.crud_digital_asset import crud_digital_asset
from app.schemas import digital_asset as schemas
from app.db.models import Base

# --- Test Setup ---
# For fast and isolated unit tests, we use an in-memory SQLite database.
# This avoids the overhead of spinning up a real PostgreSQL container for every test run.
# The 'check_same_thread' argument is a specific requirement for SQLite to allow
# it to be used across multiple functions in a test suite.
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# The 'JsonBCompat' TypeDecorator in the SQLAlchemy model is crucial for this to work.
# It allows the same model definition to map to PostgreSQL's native JSONB type in
# production while gracefully falling back to a standard JSON type that SQLite supports.

@pytest.fixture(scope="function")
def db_session():
    """
    This is a pytest fixture that provides a fresh, isolated database session for each test function.
    
    The 'yield' pattern ensures that setup and teardown code is run correctly.
    1. `Base.metadata.create_all()`: Before each test, all tables are created in the in-memory DB.
    2. `yield db`: The test function runs with a clean session.
    3. `db.close()`: The session is closed.
    4. `Base.metadata.drop_all()`: After each test, all tables are dropped, ensuring no state
       leaks from one test to another.
    """
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)

# --- Tests ---

class TestCRUDDigitalAsset(unittest.TestCase):
    def test_create_asset(self):
        asset = crud_digital_asset.create_digital_asset({'id': 'asset1'})
        self.assertEqual(asset['id'], 'asset1')

def test_get_asset(db_session):
    """
    Tests that a specific asset can be retrieved by its ID after it has been created.
    """
    asset_in = schemas.DigitalAssetCreate(asset_name="Another Asset", asset_type="TEST", owner_id=uuid.uuid4())
    created_asset = crud_digital_asset.create_asset(db=db_session, asset=asset_in)
    
    retrieved_asset = crud_digital_asset.get_asset(db=db_session, asset_id=created_asset.asset_id)

    assert retrieved_asset is not None
    assert retrieved_asset.asset_id == created_asset.asset_id
    assert retrieved_asset.asset_name == "Another Asset"

def test_get_assets(db_session):
    """
    Tests retrieval of multiple assets to ensure the list endpoint's
    underlying query is working correctly.
    """
    crud_digital_asset.create_asset(db=db_session, asset=schemas.DigitalAssetCreate(asset_name="Asset 1", asset_type="T1", owner_id=uuid.uuid4()))
    crud_digital_asset.create_asset(db=db_session, asset=schemas.DigitalAssetCreate(asset_name="Asset 2", asset_type="T2", owner_id=uuid.uuid4()))

    assets = crud_digital_asset.get_assets(db=db_session)
    assert len(assets) == 2

def test_delete_asset(db_session):
    """
    Tests that an asset can be successfully deleted from the database
    and can no longer be retrieved.
    """
    asset_in = schemas.DigitalAssetCreate(asset_name="To Be Deleted", asset_type="DELETE_ME", owner_id=uuid.uuid4())
    created_asset = crud_digital_asset.create_asset(db=db_session, asset=asset_in)
    
    deleted_asset = crud_digital_asset.delete_asset(db=db_session, asset_id=created_asset.asset_id)
    assert deleted_asset is not None

    retrieved_asset = crud_digital_asset.get_asset(db=db_session, asset_id=created_asset.asset_id)
    assert retrieved_asset is None 