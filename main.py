from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from router.Sale_Report import router as reports_router
from router.Detiled import router as detiles_router
from router.Filters_Values import router as filters_router
from router.Targets import router as targets_router
from router.Export_To_Sheets import router as export_router
from router.Columns_GroupBy import router as columns_router
from router.GroupBy_Summary import router as groupby_router
from Authentication.auth import router as Authentication
from router.Launch_Summary import router as inventory_summary_router
from router.Pre_load_items import router as pre_load_items_router


from utilities.generic_utils import get_models
from database.database import engines

app = FastAPI(title="Inventory Summary")

# === CORS Middleware ===
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # In production, specify allowed origins
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
app.include_router(Authentication, prefix="/api")
app.include_router(inventory_summary_router, prefix="/api")
app.include_router(pre_load_items_router, prefix="/api")


# === Auto-create tables ===
for business, engine in engines.items():
    models = get_models(business)
    models.Base.metadata.create_all(bind=engine)
