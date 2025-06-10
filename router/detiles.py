from fastapi import APIRouter, Query, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import Optional
from Authentication.functions import get_current_user, verify_access_token_cookie
from database.database import get_db
from models.task import User
from utilities.generic_utils import get_models
from utilities.detiled import detiled
from utilities.clean import clean_json
import pandas as pd, traceback, json, asyncio

router = APIRouter()

async def run_in_thread(fn, *args):
    return await asyncio.to_thread(fn, *args)

@router.get("/detiles")
async def detiles_report(
    Start_Date: str,
    End_Date: Optional[str] = None,
    business: str = Query(...),
    aggregation: Optional[str] = "daily",
    col: str = Query(...),
    group_by: Optional[str] = None,
    item_filter: Optional[str] = None,
    db: Session = Depends(get_db),
    token=Depends(verify_access_token_cookie)
):
    try:
        models = get_models(business)
        group_by_fields = group_by.split(",") if group_by else None
        item_filter_dict = json.loads(item_filter) if item_filter else None

        summary_df: pd.DataFrame = await run_in_thread(
            detiled, db, models, col, Start_Date, End_Date,
            business, aggregation, group_by_fields, item_filter_dict
        )

        for colname in summary_df.columns:
            if pd.api.types.is_datetime64_any_dtype(summary_df[colname]):
                summary_df[colname] = summary_df[colname].dt.strftime('%Y-%m-%d')

        data = clean_json(summary_df.to_dict(orient="records"))
        return JSONResponse(content={"data": data}, status_code=200)
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(content={"message": "Error", "error": str(e)}, status_code=500)
