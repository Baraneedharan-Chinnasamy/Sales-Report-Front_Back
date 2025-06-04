from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from router.reports import router as reports_router
from router.detiles import router as detiles_router
from router.filters import router as filters_router
from router.targets import router as targets_router
from router.export import router as export_router
from router.columns import router as columns_router
from router.groupby import router as groupby_router


from utilities.generic_utils import get_models
from database.database import engines

app = FastAPI(title="Inventory Summary")

# === CORS Middleware ===
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Register routers ===
app.include_router(reports_router, prefix="/api")
app.include_router(detiles_router, prefix="/api")
app.include_router(filters_router, prefix="/api")
app.include_router(targets_router, prefix="/api")
app.include_router(export_router, prefix="/api")
app.include_router(columns_router, prefix="/api")
app.include_router(groupby_router, prefix="/api")


# === Auto-create tables ===
for business, engine in engines.items():
    models = get_models(business)
    models.Base.metadata.create_all(bind=engine)
