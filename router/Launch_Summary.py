from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Optional
from sqlalchemy.orm import Session
from Authentication.functions import verify_access_token_cookie
from database.database import get_db
from Launch.Launch import generate_inventory_summary
from utilities.generic_utils import get_models
import json, traceback, asyncio
import pandas as pd
from io import StringIO
from decimal import Decimal

router = APIRouter()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

# This function helps run tasks in a background thread, ensuring FastAPI doesn't block
async def run_in_thread(fn, *args):
    return await asyncio.to_thread(fn, *args)

@router.get("/launch-summary")
async def inventory_summary(
    days: int = Query(...),
    group_by: str = Query(...),
    business: str = Query(...),
    item_filter: Optional[str] = Query(None),
    variation_columns: list[str] = Query([]),  # List of columns for variations
    launch_date_filter: Optional[str] = Query(None),  # Optional launch date filter
    db: Session = Depends(get_db),
    token=Depends(verify_access_token_cookie)
):
    try:
        # Get the models based on the business type
        models = get_models(business)
        
        # Convert the item filter string (if provided) into a dictionary
        item_filter_dict = json.loads(item_filter) if item_filter else None
        
        # Run the inventory summary generation in a background thread
        summary_data = await run_in_thread(
            generate_inventory_summary,
            db, models, days, group_by, business, item_filter_dict, variation_columns, launch_date_filter
        )
        
        # Convert the summary data (assumed to be a DataFrame) to CSV format
        if hasattr(summary_data, 'to_dict'):
            summary_data = summary_data.to_dict('records')  # Convert DataFrame to list of records
        
        # Create a StringIO object to save the CSV file in memory
        csv_buffer = StringIO()
        df = pd.DataFrame(summary_data)
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)  # Rewind the in-memory file
        
        # Return the CSV file as a downloadable response
        return StreamingResponse(
            csv_buffer,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=inventory_summary.csv"}
        )
    
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            status_code=500, 
            content={
                "message": "Error", 
                "error": str(e), 
                "status": "error"
            }
        )
