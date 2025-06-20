from fastapi import APIRouter, Query, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from Authentication.functions import get_current_user, verify_access_token_cookie
from database.database import get_db
from models.task import User
from utilities.columns import get_field_values, get_item_columns
from utilities.generic_utils import get_models
from sqlalchemy import inspect

# Define excluded columns globally or in your function
excluded_columns = ['id', 'created_at', 'updated_at']  # Example of excluded columns

# Function to get columns dynamically from a model
def get_item_columns(models):
    # Inspect the model
    mapper = inspect(models.Item)
    field_map = {}

    # Iterate over the columns of the model
    for column in mapper.columns:
        if column.key not in excluded_columns:  # Exclude unwanted columns
            field_map[column.key] = column
    return field_map


# FastAPI router for the endpoint
router = APIRouter()

@router.get("/filter/field-values")
def field_values(
    field_name: str,
    business: str,
    search: str = "",
    item_filter: dict = {},  # Added parameter for item_filter (previous filters)
    offset: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    token=Depends(verify_access_token_cookie)
):
    try:
        # Get models dynamically based on the business
        models = get_models(business)
        result = get_field_values(field_name, search, item_filter, db, models, offset, limit)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


def get_field_values(field_name, search, item_filter, db, models, offset=0, limit=100):
    # Get the field map dynamically
    field_map = get_item_columns(models)

    # Check if the provided field_name exists in the model columns
    if field_name not in field_map:
        return {"error": "Invalid field name"}

    # Get the actual column object from field_map
    column = field_map[field_name]

    # Start the query to fetch distinct field values
    query = db.query(column.distinct())

    # Apply filters if provided (item_filter)
    if item_filter:
        for field_name, conditions in item_filter.items():
            # Since you're passing actual field names, we can directly use them here
            if not hasattr(models.Item, field_name):  # Check if the field exists in the model
                print(f"Warning: filter field '{field_name}' not found in model — skipping")
                continue

            column_attr = getattr(models.Item, field_name)

            for condition in conditions:
                op = condition.get("operator")
                value = condition.get("value")

                if isinstance(value, list) and len(value) == 1 and isinstance(value[0], list):
                    value = value[0]

                # Apply different operators dynamically
                if op == "In":
                    query = query.filter(column_attr.in_([value] if isinstance(value, str) else value))
                elif op == "Not_In":
                    query = query.filter(~column_attr.in_([value] if isinstance(value, str) else value))
                elif op == "Equal":
                    query = query.filter(column_attr == value)
                elif op == "Not_Equal":
                    query = query.filter(column_attr != value)
                elif op == "Less_Than":
                    query = query.filter(column_attr < value)
                elif op == "Greater_Than":
                    query = query.filter(column_attr > value)
                elif op == "Less_Than_Or_Equal":
                    query = query.filter(column_attr <= value)
                elif op == "Greater_Than_Or_Equal":
                    query = query.filter(column_attr >= value)
                elif op == "Between":
                    if isinstance(value, list) and len(value) == 2:
                        query = query.filter(column_attr.between(value[0], value[1]))
                    else:
                        print(f"Warning: 'Between' operator for field '{field_name}' requires exactly two values — skipping")

    # Apply search filter if provided
    if search:
        query = query.filter(column.ilike(f"%{search}%"))

    # Get the total count for pagination
    total = query.count()

    # Fetch the results with offset and limit
    results = query.offset(offset).limit(limit).all()

    # Extract the unique values from the results
    unique_values = [str(row[0]) for row in results if row[0] is not None]

    return {
        "field": field_name,
        "values": unique_values,
        "total": total,
        "has_more": offset + limit < total
    }
