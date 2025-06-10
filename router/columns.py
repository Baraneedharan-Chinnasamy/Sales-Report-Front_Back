# router/columns.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from fastapi.responses import JSONResponse
import traceback

from Authentication.functions import  verify_access_token_cookie
from database.database import get_db
from models.task import User
from utilities.functions import get_column_names
from utilities.generic_utils import get_models
import asyncio

router = APIRouter()

async def run_in_thread(fn, *args):
    return await asyncio.to_thread(fn, *args)

@router.post("/get_column_names")
async def get_table(business: str, db: Session = Depends(get_db),token=Depends(verify_access_token_cookie)):
    try:
        print(f"Fetching filter data for business: {business}")

        models = get_models(business)
        print(f"Using models: {models}")

        column_name_df = await run_in_thread(get_column_names, db, models, business)

        if column_name_df.empty:
            print("No data found!")
            return {"columns": []}

        print("Data fetched successfully!")

        column_list = column_name_df["Column"].dropna().astype(str).tolist()

        return {"columns": column_list}

    except Exception as e:
        print(f"Error occurred: {e}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"message": "Something went wrong"})
