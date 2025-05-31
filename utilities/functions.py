import pandas as pd
import os
import json
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
import numpy as np
from utilities.Grouping import (
    group_by_bee, group_by_dic_prathisham, group_by_dic_zing, group_by_dic_adb)
from functools import lru_cache

def calculate_historical_stock(db, models, items_df, report_date):
    historical_items = items_df.copy()

    after_sales = db.query(
        models.Sale.Item_Id, 
        func.sum(models.Sale.Quantity).label('sold_after')
    ).filter(
        models.Sale.Date >= report_date
    ).group_by(
        models.Sale.Item_Id
    ).all()

    after_sales_df = pd.DataFrame(after_sales, columns=["Item_Id", "sold_after"])

    if not after_sales_df.empty and not historical_items.empty:
        historical_items = pd.merge(historical_items, after_sales_df, on="Item_Id", how="left")
        historical_items["sold_after"] = historical_items["sold_after"].fillna(0)
    else:
        historical_items["sold_after"] = 0

    historical_items["Historical_Stock"] = historical_items["Current_Stock"] + historical_items["sold_after"]
    
    return historical_items


TARGET_FILE = os.path.join("target_data", "targets.json")

@lru_cache(maxsize=1)
def _load_targets_cached():
    if not os.path.exists(TARGET_FILE):
        return pd.DataFrame(columns=[
            "Business_Name", "Target_Column", "Target_Key",
            "Start_Date", "Target_Value", "Uploaded_At", "Status_History", "Target_Key_Lower"
        ])

    with open(TARGET_FILE, "r") as f:
        data = json.load(f)

    for entry in data:
        if "Status_History" not in entry:
            entry["Status_History"] = []

    df = pd.DataFrame(data)

    df["Start_Date"] = pd.to_datetime(df["Start_Date"], errors="coerce")
    df["Uploaded_At"] = pd.to_datetime(df["Uploaded_At"], errors="coerce")

    df["Business_Name"] = df["Business_Name"].str.strip().str.lower()
    df["Target_Column"] = df["Target_Column"].str.strip()
    df["Target_Key"] = df["Target_Key"].fillna("").str.strip()
    df["Target_Key_Lower"] = df["Target_Key"].str.lower()

    return df

def load_targets(force_reload=False):
    if force_reload:
        _load_targets_cached.cache_clear()
    return _load_targets_cached()


def get_unique_periods(start_date, end_date, aggregation):
    if aggregation == "daily":
        return pd.date_range(start=start_date, end=end_date, freq='D')
    elif aggregation == "weekly":
        return pd.date_range(start=start_date, end=end_date, freq='W-MON')
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
    for entry in sorted(status_history, key=lambda x: pd.to_datetime(x["timestamp"]).date()):
        entry_date = pd.to_datetime(entry["timestamp"]).date()
        if entry_date <= period_end_date:
            last_status = entry["status"]
        else:
            break

    return True if last_status is None else last_status is True


def was_target_active_in_period(status_json_str, period_start, period_end):
    status_history = json.loads(status_json_str)
    if not status_history:
        return True

    last_status = None
    active_during = False

    for entry in sorted(status_history, key=lambda x: pd.to_datetime(x["timestamp"])):
        entry_date = pd.to_datetime(entry["timestamp"])
        if entry_date <= period_end:
            last_status = entry["status"]
            if last_status is True and entry_date <= period_end:
                if entry_date <= period_end and entry_date >= period_start:
                    active_during = True
                elif entry_date <= period_start:
                    active_during = True
        else:
            break

    return active_during or (last_status is True)


def filter_active_targets(df, period_start, period_end, aggregation="daily"):
    active = []
    for _, row in df.iterrows():
        target_start = pd.to_datetime(row["Start_Date"])
        if target_start <= period_end:
            status_json_str = json.dumps(row.get("Status_History", []))
            if aggregation == "monthly":
                if was_target_active_in_period(status_json_str, period_start, period_end):
                    active.append(row)
            else:
                if cached_is_target_active(status_json_str, period_end.strftime("%Y-%m-%d %H:%M:%S")):
                    active.append(row)
    return pd.DataFrame(active)


def get_previous_period(dt, aggregation, start_date, end_date):
    if aggregation == "daily":
        return dt - timedelta(days=1)

    if aggregation == "weekly":
        return dt - timedelta(weeks=1)

    if aggregation == "monthly":
        prev_month_end = dt.replace(day=1) - timedelta(days=1)
        return prev_month_end.replace(day=1)

    if aggregation == "custom":
        span = (end_date - start_date).days + 1
        return start_date - timedelta(days=span)

    raise ValueError(f"Unknown aggregation: {aggregation}")



def process_beelittle(t1):
    import pandas as pd
    restock_status_map = {"DISCONTINUE": "DISCONTINUED", "DIS CONTINUED": "DISCONTINUED", "CONTINUE": "CONTINUED"}
    age_map = {"NB": "Newborn", "0-3m,Newborn": "Newborn,0-3m", "1-2 yr": "1-2yr", "3-6M": "3-6m", "6-12M": "6-12m", "1.5-2yr": "18-24m", "1-1.5yr": "12-18m"}
    fabric_map = {"cotton": "Cotton", "Mulmul": "MulMul", "Crinkle fabric": "Crinkle", "Organic Cotton,Muslin": "Muslin,Organic Cotton", "Velvet,Cotton": "Cotton,Velvet", "Tissue,Crepe": "Crepe,Tissue"}
    pack_size_map = {"1-pack": "1-Pack"}
    product_type_map = {"Hoodie,Sweater": "Hoodie, Sweater", "Sweater,Hoodie": "Hoodie, Sweater", "Cap Mittens Booties": "Cap, Mittens, Booties", "Jabla,Top": "Jabla, Top", "Top,Jabla": "Jabla, Top", "Sweater , Pant": "Sweater and Pant"}

    def process_style_theme_motif(item, label):
        if not item or not isinstance(item, str):
            return [f"Multi-{label} Design"]
        items = item.split(", ")
        return items[:2] if len(items) <= 2 else [f"Multi-{label} Design"]

    def split_and_expand(df, col, new_col_1, new_col_2, label):
        if col in df.columns:
            df[col] = df[col].apply(lambda x: process_style_theme_motif(x, label))
            df[[new_col_1, new_col_2]] = pd.DataFrame(df[col].tolist(), index=df.index)
            df[new_col_2] = df[new_col_2].fillna("None")
            df.drop(columns=[col], inplace=True)
        return df

    for col, col1, col2, label in [
        ("print_style", "Print_Style_1", "Print_Style_2", "Style"),
        ("print_theme", "Print_Theme_1", "Print_Theme_2", "Theme"),
        ("print_key_motif", "Print_Key_Motif_1", "Print_Key_Motif_2", "Key Motif"),
        ("print_colour", "Print_Colour_1", "Print_Colour_2", "print_colour"),
    ]:
        t1 = split_and_expand(t1, col, col1, col2, label)

    if "Colour" in t1.columns:
        t1["Colour"] = t1["Colour"].apply(lambda x: x if isinstance(x, list) else [x])
        t1["Colour"] = t1["Colour"].apply(lambda x: x[:2] if len(x) >= 2 else x + ["None"])
        t1[["Colour_1", "Colour_2"]] = pd.DataFrame(t1["Colour"].tolist(), index=t1.index)
        t1.drop(columns=["Colour"], inplace=True)

    if "restock_status" in t1.columns:
        t1["restock_status"] = t1["restock_status"].replace(restock_status_map)
    if "Age" in t1.columns:
        t1["Age"] = t1["Age"].replace(age_map)
    if "Fabric" in t1.columns:
        t1["Fabric"] = t1["Fabric"].replace(fabric_map)
    if "Pack_Size" in t1.columns:
        t1["Pack_Size"] = t1["Pack_Size"].replace(pack_size_map)
    if "Product_Type" in t1.columns:
        t1["Product_Type"] = t1["Product_Type"].replace(product_type_map)

    return t1


def process_prathiksham(t1):
    maps = {
        "Category": {'Dress,Kurta':'Kurta,Dress'},
        "Colour": {'Teal,White':'Teal,White'},
        "Fabric": {'Cotton,Kota Doria':'Kota Doria,Cotton'},
        "Neck": {'Collar,V-Neck':'V-Neck,Collar', 'Collar,V Neck':'V-Neck,Collar', 'V Neck,Collar':'V-Neck,Collar', 'Collar V Neck':'V-Neck,Collar'},
        "Occasion": {'Brunch Wear,Casual Wear':'Casual Wear,Brunch Wear','Office Wear,Regular Wear':'Regular Wear,Office Wear','Casual Wear,Regular Wear':'Regular Wear,Casual Wear'},
        "Print": {'Handblock,Stripes':'Stripes,Handblock','Floral,Stripes':'Stripes,Floral','Floral,Solid':'Solid,Floral','Embroidered,Solid':'Solid,Embroidered'},
        "Sleeve": {'Sleeveless,Elbow Fit':'Sleeveless,Elbow Fit'},
        "Product_Availability": {'Mad e to order':'Made To Order'},
    
    }

    for col, replacements in maps.items():
        if col in t1.columns:
            t1[col] = t1[col].replace(replacements)

    return t1


def process_zing(t1):
    def safe_split(x):
        if pd.isna(x):
            return ''
        return ', '.join(sorted(str(item).strip() for item in str(x).split(',') if item.strip()))

    for col in ['Colour', 'Fit', 'Neck', 'Occasion', 'Print', 'print_type']:
        if col in t1.columns:
            t1[col] = t1[col].apply(safe_split)

    sleeve_dict = {
        'Three-Quarter Sleeves': 'Three-Quarter Sleeves',
        'Three Quarter Sleeves': 'Three-Quarter Sleeves',
        'Three Quarters Sleeves': 'Three-Quarter Sleeves',
        'Three Quarter Sleeve': 'Three-Quarter Sleeves',
        'Sleeveless': 'Sleeveless',
        'Elbow Sleeves': 'Elbow Sleeves',
        'Elbow Sleeve': 'Elbow Sleeves',
        'Half-Sleeve': 'Half Sleeve',
        'Half Sleeve': 'Half Sleeve',
        'Full Sleeves': 'Full Sleeves',
        'Full Sleeve': 'Full Sleeves',
        'Short Sleeves': 'Short Sleeves',
        'Short Sleeve': 'Short Sleeves',
        'Short': 'Short Sleeves'
    }

    if "Sleeve" in t1.columns:
        t1["Sleeve"] = t1["Sleeve"].replace(sleeve_dict)
    if "Category" in t1.columns:
        t1["Category"] = t1["Category"].replace({"Co-ord,Kurta Set":"Kurta Set,Co-ord"})

    return t1



def get_column_names(db: Session, models, business: str):
    if business == "BEE7W5ND34XQZRM":
        groupby = group_by_bee
    elif business == "PRT9X2C6YBMLV0F":
        groupby = group_by_dic_prathisham
    elif business == "ZNG45F8J27LKMNQ":
        groupby = group_by_dic_zing
    elif business == "ADBXOUERJVK038L":
        groupby = group_by_dic_adb
    else:
        print("Business name is wrong")
        return pd.DataFrame()

    column_names = list(groupby.keys())
    df = pd.DataFrame({"Column": column_names})
    return df