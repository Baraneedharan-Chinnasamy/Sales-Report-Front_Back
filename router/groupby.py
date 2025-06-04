# router/groupby.py

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from fastapi.responses import JSONResponse
import json
import pandas as pd
import numpy as np
from typing import Optional

from database.database import get_db
from utilities.generic_utils import get_models
from utilities.MainGroupBy import agg_grp
import asyncio
import traceback

router = APIRouter()

async def run_in_thread(fn, *args):
    return await asyncio.to_thread(fn, *args)

@router.get("/groupby/aggregation", summary="Perform group-by aggregation for items")
async def groupby_aggregation(
    business: str = Query(..., description="Business name (e.g., beelittle)"),
    Start_Date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    End_Date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    data_fields: str = Query(..., description="JSON list of all required fields (dimensions + aggregations)"),
    groupby: str = Query(..., description="JSON list of columns to group by"),
    item_filter: Optional[str] = Query(None, description="Optional JSON filter for items"),
    db: Session = Depends(get_db)
):
    try:
        # Parse incoming parameters
        parsed_data_fields = json.loads(data_fields)
        parsed_groupby = json.loads(groupby)

        if not isinstance(parsed_data_fields, list) or not isinstance(parsed_groupby, list):
            return JSONResponse(
                content={"message": "data_fields and groupby must be JSON arrays", "status": "error"},
                status_code=400
            )

        item_filter_dict = {}
        if item_filter:
            item_filter_dict["item_filter"] = json.loads(item_filter)

        models = get_models(business)
        groupby_dict = {"groupby": parsed_groupby}

        result_df = await run_in_thread(
            agg_grp,
            db,
            models,
            business,
            item_filter_dict,
            parsed_data_fields,
            groupby_dict,
            Start_Date,
            End_Date
        )

        for col in result_df.columns:
            if pd.api.types.is_datetime64_any_dtype(result_df[col]):
                result_df[col] = result_df[col].dt.strftime('%Y-%m-%d')

        result_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        result_df = result_df.where(pd.notnull(result_df), None)

        return JSONResponse(
            content={
                "message": "Aggregation successful",
                "status": "success",
                "data": result_df.to_dict(orient="records")
            },
            status_code=200
        )

    except json.JSONDecodeError as e:
        return JSONResponse(
            content={"message": "Invalid JSON in input", "error": str(e), "status": "error"},
            status_code=400
        )
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            content={"message": "Error performing aggregation", "error": str(e), "status": "error"},
            status_code=500
        )
