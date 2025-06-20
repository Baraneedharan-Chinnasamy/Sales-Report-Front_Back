from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from fastapi.responses import JSONResponse
import traceback
import asyncio
from Authentication.functions import verify_access_token_cookie
from database.database import get_db
from models.task import User
from utilities.functions import get_column_names
from utilities.columns import get_field_values, get_item_columns
from utilities.generic_utils import get_models

router = APIRouter()

async def run_in_thread(fn, *args):
    return await asyncio.to_thread(fn, *args)

@router.get("/get_columns_and_fields")
async def get_columns_and_fields(business: str, db: Session = Depends(get_db), token=Depends(verify_access_token_cookie)):
    try:
        print(f"Fetching columns and fields data for business: {business}")
        models = get_models(business)
        print(f"Using models: {models}")

        # Aggregation columns (common to all)
        agg_columns = [
            "launch_date", "Days_Since_Launch", "Total_Quantity", "Total_Value",
            "Total_Item_Viewed", "Total_Item_Atc",
            "Per_Day_Value", "Per_Day_Quantity", "Per_Day_View", "Per_Day_atc",
            "Conversion_Percentage"
        ]

        # Business-specific columns
        business_columns = {
            "BEE7W5ND34XQZRM": [
                "Item_Id", "Item_Name", "Item_Type", "Item_Code", "Sale_Price", "Sale_Discount",
                "Current_Stock", "Is_Public", "Age", "Discount", "Bottom", "Bundles", "Fabric", "Filling",
                "Gender", "Pack_Size", "Pattern", "Product_Type", "Sale", "Size", "Sleeve", "Style", "Top",
                "Weave_Type", "Weight", "Width", "batch", "bottom_fabric", "brand_name", "discounts",
                "inventory_type", "offer_date", "quadrant", "relist_date", "restock_status", "season",
                "season_style", "seasons_style", "print_size", "Print_Style", "Colour", "Print_Theme",
                "Print_Colour", "Print_Key_Motif"
            ],
            "PRT9X2C6YBMLV0F": [
                "Item_Id", "Item_Name", "Item_Type", "Item_Code", "Sale_Price", "Sale_Discount", "Current_Stock",
                "Is_Public", "Category", "Colour", "Fabric", "Fit", "Lining", "Neck", "Occasion", "Print", "Size",
                "Sleeve", "batch", "bottom_length", "bottom_print", "bottom_type", "collections", "details",
                "pocket", "top_length", "waistband", "Pack"
            ],
            "ZNG45F8J27LKMNQ": [
                "Item_Id", "Item_Name", "Item_Type", "Item_Code", "Sale_Price", "Sale_Discount", "Current_Stock",
                "Is_Public", "Category", "Colour", "Fabric", "Fit", "Neck", "Occasion", "Print", "Size", "Sleeve",
                "batch", "details", "office_wear_collection", "print_type", "quadrant", "style_type", "feeding_friendly"
            ],
            "ADBXOUERJVK038L": [
                "Item_Id", "Item_Name", "Item_Type", "Item_Code", "Sale_Price", "Sale_Discount", "Current_Stock",
                "Is_Public", "Category", "Age", "Bottom", "Colour", "Fabric", "Gender", "Neck_Closure", "Neck_Type",
                "Occassion", "Pack_Size", "Print_Collections", "Print_Pattern", "Print_Size", "Printed_Pattern",
                "Sleeve", "Top", "Weave_Type", "age_category", "batch", "bottom_fabric", "print_size",
                "product_category", "product_type"
            ]
        }

        # Choose appropriate columns
        if business not in business_columns:
            return JSONResponse(status_code=400, content={"message": "Invalid business ID"})

        # Combine dynamic columns and agg columns
        groupby_payload = {
            "columns": business_columns[business],
            "agg": agg_columns
        }

        # Field names (as is)
        fields = list(get_item_columns(models).keys())

        # Final response
        return {
            "groupby": groupby_payload,
            "field_names": fields
        }

    except Exception as e:
        print(f"Error occurred: {e}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"message": "Something went wrong"})
