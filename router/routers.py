import traceback
import io
import json
import asyncio
import pandas as pd
from typing import Optional, List
from fastapi.responses import JSONResponse
from fastapi import APIRouter, Depends, status, Query
from sqlalchemy.orm import Session
import os
from datetime import datetime
from pydantic import BaseModel
from database.database import get_db
from utilities.generic_utils import get_models
from utilities.utils import daily_sale_report
from utilities.grouping import detiled
from utilities.clean import clean_json
from utilities.columns import get_field_values, get_item_columns

router = APIRouter()

async def run_in_thread(fn, *args):
    """Run blocking function in separate thread to avoid blocking FastAPI"""
    return await asyncio.to_thread(fn, *args)

@router.get("/daily-report")
async def daily_report(
    Start_Date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    End_Date: Optional[str] = Query(None, description="Optional end date in YYYY-MM-DD format"),
    aggregation: Optional[str] = Query("daily", description="Choose aggregation type"),
    business: str = Query(..., description="Business name identifier"),
    item_filter: Optional[str] = Query(None, description="Optional item filter in JSON"),
    db: Session = Depends(get_db)
):
    try:
        models = get_models(business)
        
        item_filter_dict = None
        if item_filter:
            item_filter_dict = json.loads(item_filter)

        summary_df: pd.DataFrame = await run_in_thread(
            daily_sale_report,
            db,
            models,
            Start_Date,
            End_Date,
            business,
            aggregation,
            item_filter_dict
        )

        for colname in summary_df.columns:
            if pd.api.types.is_datetime64_any_dtype(summary_df[colname]):
                summary_df[colname] = summary_df[colname].dt.strftime('%Y-%m-%d')

        summary_data = summary_df.to_dict(orient="records")

        return JSONResponse(
            content={"report": summary_data},
            status_code=200
        )

    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            content={"message": "Something went wrong", "error": str(e)},
            status_code=500
        )

@router.get("/detiles")
async def detiles_report(
    Start_Date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    End_Date: Optional[str] = Query(None, description="Optional end date in YYYY-MM-DD format"),
    business: str = Query(..., description="Business name identifier"),
    aggregation: Optional[str] = Query("daily", description="Choose aggregation type"),
    col: str = Query(..., description="Column name (e.g., Size or Age)"),
    group_by: Optional[str] = Query(None, description="Comma-separated fields to group by"),
    item_filter: Optional[str] = Query(None, description="Optional item filter in JSON"),
    db: Session = Depends(get_db)
):
    try:
        models = get_models(business)

        group_by_fields = group_by.split(",") if group_by else None

        item_filter_dict = None
        if item_filter:
            item_filter_dict = json.loads(item_filter)

        summary_df: pd.DataFrame = await run_in_thread(
            detiled,
            db,
            models,
            col,
            Start_Date,
            End_Date,
            business,
            aggregation,
            group_by_fields,
            item_filter_dict
        )

        for colname in summary_df.columns:
            if pd.api.types.is_datetime64_any_dtype(summary_df[colname]):
                summary_df[colname] = summary_df[colname].dt.strftime('%Y-%m-%d')

        data = clean_json(summary_df.to_dict(orient="records"))

        return JSONResponse(
            content={"data": data},
            status_code=200
        )

    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            content={"message": "Something went wrong", "error": str(e)},
            status_code=500
        )

@router.get("/filter/available-fields")
def available_fields(
    business: str = Query(..., description="Business name to identify database"),
    db: Session = Depends(get_db)
):
    try:
        models = get_models(business)
        field_map = get_item_columns(models)
        available_fields = list(field_map.keys())
        return JSONResponse(content={"fields": available_fields}, status_code=200)
    except Exception as e:
        return JSONResponse(
            content={"message": "Error fetching fields", "error": str(e)},
            status_code=500
        )

@router.get("/filter/field-values")
def field_values(
    field_name: str = Query(...),
    business: str = Query(...),
    search: str = Query(""),
    offset: int = Query(0),
    limit: int = Query(100),
    db: Session = Depends(get_db)
):
    try:
        models = get_models(business)
        result = get_field_values(field_name, search, db, models, offset, limit)
        return JSONResponse(content=result, status_code=200)
    except Exception as e:
        return JSONResponse(
            content={"message": "Error fetching field values", "error": str(e)},
            status_code=500
        )


TARGET_FILE_PATH = os.path.join("target_data", "targets.json")

class TargetEntry(BaseModel):
    Business_Name: str
    Start_Date: str
    Product_Type: str
    Target_Value: float

@router.post("/set-daily-targets")
def set_targets(data: List[TargetEntry]):
    if os.path.exists(TARGET_FILE_PATH):
        with open(TARGET_FILE_PATH, "r") as f:
            existing_data = json.load(f)
    else:
        existing_data = []

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for entry in data:
        product_type_column = "Product_Type" if entry.Business_Name == "BEE7W5ND34XQZRM" else "Category"
        new_entry = {
            "Business_Name": entry.Business_Name.strip().lower(),
            product_type_column: entry.Product_Type.strip().lower(),
            "Start_Date": entry.Start_Date,
            "Target_Value": entry.Target_Value,
            "Uploaded_At": now
        }
        existing_data.append(new_entry)

    os.makedirs("target_data", exist_ok=True)
    with open(TARGET_FILE_PATH, "w") as f:
        json.dump(existing_data, f, indent=4)

    return {"message": "Targets appended successfully", "entries_added": len(data)}
