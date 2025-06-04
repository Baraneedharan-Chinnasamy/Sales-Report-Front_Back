from fastapi import APIRouter, Query, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from database.database import get_db
from utilities.columns import get_field_values, get_item_columns
from utilities.generic_utils import get_models

router = APIRouter()

@router.get("/filter/available-fields")
def available_fields(business: str, db: Session = Depends(get_db)):
    try:
        models = get_models(business)
        fields = list(get_item_columns(models).keys())
        return JSONResponse(content={"fields": fields})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@router.get("/filter/field-values")
def field_values(
    field_name: str,
    business: str,
    search: str = "",
    offset: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    try:
        models = get_models(business)
        result = get_field_values(field_name, search, db, models, offset, limit)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
