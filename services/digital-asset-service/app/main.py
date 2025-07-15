from fastapi import FastAPI
from .api import endpoints

# Set up the database tables
from platformq_shared.postgres_db import engine, Base
from .db import models
Base.metadata.create_all(bind=engine)


app = FastAPI(
    title="Digital Asset Service",
    description="Manages metadata for all Digital Assets on the platformQ.",
    version="0.1.0",
)

# Include the API router
app.include_router(endpoints.router, prefix="/api/v1/assets", tags=["Digital Assets"])

@app.get("/")
def read_root():
    return {"message": "digital-asset-service is running"}

# TODO: Replace placeholder dependencies with real ones.
# TODO: Integrate with the shared_lib.base_service factory function.
