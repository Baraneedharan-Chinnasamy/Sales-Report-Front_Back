import pandas as pd
import os
import json
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
import numpy as np
from functools import lru_cache

def calculate_historical_stock(db, models, items_df, report_date):
    
    
    # Step 1: Start with a copy of the current items dataframe
    historical_items = items_df.copy()
    
    # Step 2: Get all sales that happened AFTER the report date
    after_sales = db.query(
        models.Sale.Item_Id, 
        func.sum(models.Sale.Quantity).label('sold_after')
    ).filter(
        models.Sale.Date >= report_date
    ).group_by(
        models.Sale.Item_Id
    ).all()
    
    after_sales_df = pd.DataFrame(after_sales, columns=["Item_Id", "sold_after"])
    
    # Step 3: Merge with items dataframe
    if not after_sales_df.empty and not historical_items.empty:
        historical_items = pd.merge(historical_items, after_sales_df, on="Item_Id", how="left")
        historical_items["sold_after"] = historical_items["sold_after"].fillna(0)
    else:
        historical_items["sold_after"] = 0
    
    # Step 4: Calculate historical stock: current_stock + sold_after
    # Note: We can't account for inventory additions since we don't have that data
    historical_items["Historical_Stock"] = historical_items["Current_Stock"] + historical_items["sold_after"]
    
    return historical_items


TARGET_FILE = os.path.join("target_data", "targets.json")

# Internal cached version
@lru_cache(maxsize=1)
def _load_targets_cached():
    if not os.path.exists(TARGET_FILE):
        return pd.DataFrame(columns=[
            "Business_Name", "Target_Column", "Target_Key",
            "Start_Date", "Target_Value", "Uploaded_At", "Status_History", "Target_Key_Lower"
        ])

    with open(TARGET_FILE, "r") as f:
        data = json.load(f)

    # Ensure Status_History is always present
    for entry in data:
        if "Status_History" not in entry:
            entry["Status_History"] = []

    df = pd.DataFrame(data)

    # Format date fields
    df["Start_Date"] = pd.to_datetime(df["Start_Date"], errors="coerce")
    df["Uploaded_At"] = pd.to_datetime(df["Uploaded_At"], errors="coerce")

    # Normalize text fields
    df["Business_Name"] = df["Business_Name"].str.strip().str.lower()
    df["Target_Column"] = df["Target_Column"].str.strip()
    df["Target_Key"] = df["Target_Key"].fillna("").str.strip()
    df["Target_Key_Lower"] = df["Target_Key"].str.lower()

    return df

# Public wrapper with optional reload
def load_targets(force_reload=False):
    """
    Load targets with optional force reload (bypassing cache).
    """
    if force_reload:
        _load_targets_cached.cache_clear()
    return _load_targets_cached()



# Get unique periods (days) for reporting
def get_unique_periods(start_date, end_date, aggregation):
    if aggregation == "daily":
        return pd.date_range(start=start_date, end=end_date, freq='D')
    elif aggregation == "weekly":
        return pd.date_range(start=start_date, end=end_date, freq='W-MON')  # weeks starting Mondays
    elif aggregation == "monthly":
        return pd.date_range(start=start_date, end=end_date, freq='MS')
    elif aggregation == "custom":
        return [start_date]

        

@lru_cache(maxsize=None)
def cached_is_target_active(status_json_str, period_end_str):
    status_history = json.loads(status_json_str)
    period_end_date = pd.to_datetime(period_end_str).date()
    
    if not status_history:
        return True

    last_status = None
    # Sort entries by timestamp date
    for entry in sorted(status_history, key=lambda x: pd.to_datetime(x["timestamp"]).date()):
        entry_date = pd.to_datetime(entry["timestamp"]).date()
        if entry_date <= period_end_date:
            last_status = entry["status"]
        else:
            break

    return last_status is True  # Must be explicitly True to be active # Must be explicitly True to be active

def filter_active_targets(df, period_start, period_end):
    active = []
    for _, row in df.iterrows():
        target_start = pd.to_datetime(row["Start_Date"])
        # Consider targets that started before or during the period
        if target_start <= period_end:
            status_json_str = json.dumps(row.get("Status_History", []))
            # Check active status at period_end (or you can check at period_start as well)
            if cached_is_target_active(status_json_str, period_end.strftime("%Y-%m-%d %H:%M:%S")):
                active.append(row)
    return pd.DataFrame(active)


def get_previous_period(dt, aggregation, start_date, end_date):
    if aggregation == "daily":
        return dt - timedelta(days=1)

    if aggregation == "weekly":
        return dt - timedelta(weeks=1)

    if aggregation == "monthly":
        prev_month_end = dt.replace(day=1) - timedelta(days=1)  # last day of prev month
        return prev_month_end.replace(day=1)

    if aggregation == "custom":
        span = (end_date - start_date).days + 1
        return start_date - timedelta(days=span)

    raise ValueError(f"Unknown aggregation: {aggregation}")