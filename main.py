from fastapi import FastAPI
from router.routers import router
from utilities.generic_utils import get_models
from database.database import  engines
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Inventory Summary")\
# Add CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For dev, allow all. Later tighten it.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")


# Create tables for each database
for business, engine in engines.items():
    models = get_models(business)
    models.Base.metadata.create_all(bind=engine)