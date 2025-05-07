from sqlalchemy.inspection import inspect
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
import numpy as np
from functools import lru_cache

# Columns we should exclude
excluded_columns = {"Item_Id", "Updated_At", "Created_At"}

# Dynamic Column Loader
def get_item_columns(models):
    mapper = inspect(models.Item)
    field_map = {}
    for column in mapper.columns:
        if column.key not in excluded_columns:
            field_map[column.key] = column
    return field_map


def get_field_values(field_name, search, db, models, offset=0, limit=100):
    field_map = get_item_columns(models)

    if field_name not in field_map:
        return {"error": "Invalid field name"}

    column = field_map[field_name]

    query = db.query(column.distinct())
    if search:
        query = query.filter(column.ilike(f"%{search}%"))

    total = query.count()  # total matching rows
    results = query.order_by(column).offset(offset).limit(limit).all()

    unique_values = [row[0] for row in results if row[0] is not None]

    return {
        "field": field_name,
        "values": unique_values,
        "total": total,
        "has_more": offset + limit < total
    }
