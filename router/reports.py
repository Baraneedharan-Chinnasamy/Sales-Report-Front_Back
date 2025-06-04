from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from typing import Optional
from sqlalchemy.orm import Session
from database.database import get_db
from utilities.utils import daily_sale_report
from utilities.generic_utils import get_models
from decimal import Decimal
import json, traceback, asyncio

router = APIRouter()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

async def run_in_thread(fn, *args):
    return await asyncio.to_thread(fn, *args)

@router.get("/daily-report")
async def daily_report(
    Start_Date: str,
    End_Date: Optional[str] = None,
    aggregation: Optional[str] = "daily",
    business: str = Query(...),
    item_filter: Optional[str] = None,
    compare_with: Optional[str] = None,
    db: Session = Depends(get_db)
):
    try:
        models = get_models(business)

        item_filter_dict = json.loads(item_filter) if item_filter else None
        compare_with_dict = json.loads(compare_with) if compare_with else None

        report_data = await run_in_thread(
            daily_sale_report,
            db, models, Start_Date, End_Date,
            business, aggregation, item_filter_dict, compare_with_dict
        )

        return JSONResponse(
            content=json.loads(json.dumps({
                "message": "Report generated successfully",
                "status": "success",
                "data": report_data
            }, cls=DecimalEncoder)),
            status_code=200
        )
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"message": "Error", "error": str(e), "status": "error"})
