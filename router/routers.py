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


@router.get("/daily-report", summary="Generate daily sales report")
async def daily_report(
    Start_Date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    End_Date: Optional[str] = Query(None, description="Optional end date in YYYY-MM-DD format"),
    aggregation: Optional[str] = Query("daily", description="Choose aggregation type: daily, weekly, monthly, or custom"),
    business: str = Query(..., description="Business name identifier"),
    item_filter: Optional[str] = Query(None, description="Optional item filter in JSON format"),
    compare_with: Optional[str] = Query(None, description="Optional comparison period in JSON format with start_date and end_date"),
    db: Session = Depends(get_db)
):
    try:
        # Load business-specific models
        models = get_models(business)
        
        # Parse item filter if provided
        item_filter_dict = None
        if item_filter:
            try:
                item_filter_dict = json.loads(item_filter)
            except json.JSONDecodeError:
                return JSONResponse(
                    content={"message": "Invalid JSON format in item_filter parameter", "status": "error"},
                    status_code=400
                )
        
        # Parse comparison period if provided
        compare_with_dict = None
        if compare_with:
            try:
                compare_with_dict = json.loads(compare_with)
                # Validate that required fields are present
                if 'start_date' not in compare_with_dict or 'end_date' not in compare_with_dict:
                    return JSONResponse(
                        content={"message": "Comparison period must include start_date and end_date", "status": "error"},
                        status_code=400
                    )
            except json.JSONDecodeError:
                return JSONResponse(
                    content={"message": "Invalid JSON format in compare_with parameter", "status": "error"},
                    status_code=400
                )
        
        # Validate aggregation parameter
        valid_aggregations = ["daily", "weekly", "monthly", "custom", "compare"]
        if aggregation not in valid_aggregations:
            return JSONResponse(
                content={"message": f"Invalid aggregation. Must be one of: {', '.join(valid_aggregations)}", "status": "error"},
                status_code=400
            )
        
        # Run report generation in a separate thread to avoid blocking
        report_data = await run_in_thread(
            daily_sale_report,
            db,
            models,
            Start_Date,
            End_Date,
            business,
            aggregation,
            item_filter_dict,
            compare_with_dict
        )
        
        # If report data is empty, return appropriate response
        if not report_data:
            return JSONResponse(
                content={"message": "No data found for the specified criteria", "status": "success", "data": {}},
                status_code=200
            )
        
        return JSONResponse(
            content={"message": "Report generated successfully", "status": "success", "data": report_data},
            status_code=200
        )
    
    except ValueError as e:
        # Handle specific validation errors
        return JSONResponse(
            content={"message": str(e), "status": "error"},
            status_code=400
        )
    
    except Exception as e:  
        # Log the full traceback for debugging
        traceback.print_exc()
        
        return JSONResponse(
            content={
                "message": "An error occurred while generating the report", 
                "error": str(e),
                "status": "error"
            },
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


# File path where targets will be stored
TARGET_FILE_PATH = os.path.join("target_data", "targets.json")



class TargetEntry(BaseModel):
    Business_Name: str 
    Start_Date: str 
    Target_Column: str
    Target_Key: str 
    Target_Value: int

@router.post("/set-daily-targets")
def set_targets(data: List[TargetEntry]):
    # Load existing target data if file exists
    if os.path.exists(TARGET_FILE_PATH):
        with open(TARGET_FILE_PATH, "r") as f:
            existing_data = json.load(f)
    else:
        existing_data = []

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Process new entries
    for entry in data:
        new_entry = {
            "Business_Name": entry.Business_Name.strip().lower(),
            "Target_Column": entry.Target_Column.strip(),
            "Target_Key": entry.Target_Key.strip(),
            "Start_Date": entry.Start_Date,
            "Target_Value": entry.Target_Value,
            "Uploaded_At": now,
            "Status_History": [
                {
                    "status": True,
                    "timestamp": entry.Start_Date
                }
            ]
        }
        existing_data.append(new_entry)

    # Ensure the folder exists
    os.makedirs("target_data", exist_ok=True)

    # Save updated targets
    with open(TARGET_FILE_PATH, "w") as f:
        json.dump(existing_data, f, indent=4)

    return {
        "message": "Targets appended successfully.",
        "entries_added": len(data)
    }



class UpdateTargetRequest(BaseModel):
    Business_Name: str
    Target_Column: str
    Target_Key: str
    Start_Date: str
    status: bool = None  # Optional
    Target_Value: int = None  # Optional

@router.post("/update-target-entry")
def update_target_entry(data: UpdateTargetRequest):
    if not os.path.exists(TARGET_FILE_PATH):
        return {"error": "Target data not found."}

    with open(TARGET_FILE_PATH, "r") as f:
        targets = json.load(f)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated = False

    for target in targets:
        if (
            target["Business_Name"] == data.Business_Name.strip().lower()
            and target["Target_Column"] == data.Target_Column.strip()
            and target["Target_Key"] == data.Target_Key.strip()
            and target["Start_Date"] == data.Start_Date
        ):
            # Update Target Value if given
            if data.Target_Value is not None:
                target["Target_Value"] = data.Target_Value

            # Add new status to history if provided
            if data.status is not None:
                if "Status_History" not in target:
                    target["Status_History"] = []
                target["Status_History"].append({
                    "status": data.status,
                    "timestamp": now
                })

            updated = True
            break

    if not updated:
        return {"error": "Target not found."}

    with open(TARGET_FILE_PATH, "w") as f:
        json.dump(targets, f, indent=4)

    return {"message": "Target updated successfully."}



@router.get("/list-targets-with-status")
def list_targets_with_status(business_name: str = Query(..., description="Business Name (case-insensitive)")):
    if not os.path.exists(TARGET_FILE_PATH):
        return []

    with open(TARGET_FILE_PATH, "r") as f:
        targets = json.load(f)

    now = pd.to_datetime(datetime.now())
    bname = business_name.strip().lower()

    all_targets = []

    for t in targets:
        if t.get("Business_Name", "").strip().lower() != bname:
            continue

        # Parse start date
        start_date = pd.to_datetime(t.get("Start_Date"), errors="coerce")
        if pd.isna(start_date):
            continue

        # Determine current status
        history = t.get("Status_History", [])
        last_status = True  # default is active if no history
        for h in sorted(history, key=lambda x: x["timestamp"]):
            if pd.to_datetime(h["timestamp"]) <= now:
                last_status = h["status"]
            else:
                break

        all_targets.append({
            "Business_Name": t["Business_Name"],
            "Target_Column": t["Target_Column"],
            "Target_Key": t["Target_Key"],
            "Start_Date": t["Start_Date"],
            "Target_Value": t["Target_Value"],
            "Status": last_status
        })

    return all_targets
