import pandas  as pd
from fastapi import APIRouter, Depends, Body
from fastapi.responses import JSONResponse
from pandas import Timestamp
from pydantic import BaseModel, validator
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from Authentication.functions import verify_access_token_cookie
from database.database import get_db
from Launch.Launch import generate_inventory_summary
from utilities.generic_utils import get_models
import traceback, asyncio
from decimal import Decimal
import math

router = APIRouter()

# Pydantic model for request body
class LaunchSummaryRequest(BaseModel):
    days: int
    group_by: str
    business: str
    item_filter: Optional[Dict[str, Any]] = None
    variation_columns: Optional[List[str]] = []
    launch_date_filter: Optional[str] = None
    calculate_first_period: Optional[bool] = True
    calculate_second_period: Optional[bool] = True
    
    # No validation needed - both periods can be disabled if user wants

# Utility to run blocking code in thread
async def run_in_thread(fn, *args):
    return await asyncio.to_thread(fn, *args)

# Sanitize data to ensure it's JSON serializable
def sanitize_for_json(data):
    if isinstance(data, dict):
        return {k: sanitize_for_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_for_json(i) for i in data]
    elif isinstance(data, float):
        if math.isinf(data) or math.isnan(data):
            return None  # Replace with None or custom string if needed
        return data
    elif isinstance(data, Decimal):
        return float(data)
    elif isinstance(data, (pd.Timestamp, pd._libs.tslibs.timestamps.Timestamp)):
        if pd.isna(data):  # Check for NaT (Not a Time) explicitly
            return None  # Or return empty string '' or any other placeholder
        return data.strftime('%Y-%m-%d')  # Format the date appropriately
    elif pd.isna(data):  # Catch any other pandas NA/NaT types
        return None
    elif hasattr(data, 'isoformat'):  # Handle datetime objects
        return data.isoformat()
    elif hasattr(data, '__dict__') and not isinstance(data, (str, int, float, bool)):
        # Handle complex objects by converting to dict
        return sanitize_for_json(data.__dict__)
    return data
# Main route
@router.post("/launch-summary")
async def inventory_summary(
    payload: LaunchSummaryRequest,
    db: Session = Depends(get_db),
    token: str = Depends(verify_access_token_cookie)
):
    try:
        models = get_models(payload.business)

        summary_data = await run_in_thread(
            generate_inventory_summary,
            db,
            models,
            payload.days,
            payload.group_by,
            payload.business,
            payload.item_filter,
            payload.variation_columns,
            payload.launch_date_filter,
            payload.calculate_first_period,
            payload.calculate_second_period
        )
        
        print(f"Variation columns: {payload.variation_columns}")
        print(f"Period calculation - First: {payload.calculate_first_period}, Second: {payload.calculate_second_period}")
        
        if hasattr(summary_data, 'to_dict'):
            summary_data = summary_data.to_dict('records')

        sanitized_data = sanitize_for_json(summary_data)
        return JSONResponse(content=sanitized_data)

    except ValueError as ve:
        # Handle any ValueError from the function
        return JSONResponse(
            status_code=400,
            content={"message": "Validation Error", "error": str(ve), "status": "error"}
        )
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"message": "Error", "error": str(e), "status": "error"}
        )