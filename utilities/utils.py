import pandas as pd
import os
import json
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
import numpy as np
from functools import lru_cache


def daily_sale_report(db: Session, models, Start_Date, End_Date=None, business_name: str = None, aggregation="daily", item_filter: dict = None):
    Start_Date = pd.to_datetime(Start_Date)
    End_Date = pd.to_datetime(End_Date) if End_Date else Start_Date
    product_type_column = "Product_Type" if business_name == "BEE7W5ND34XQZRM" else "Category"
    TARGET_FILE = os.path.join("target_data", "targets.json")

    @lru_cache(maxsize=1)
    def load_targets():
        if not os.path.exists(TARGET_FILE):
            return pd.DataFrame(columns=["Business_Name", product_type_column, "Start_Date", "Target_Value", "Uploaded_At"])
        with open(TARGET_FILE, "r") as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        df["Start_Date"] = pd.to_datetime(df["Start_Date"])
        df["Uploaded_At"] = pd.to_datetime(df["Uploaded_At"])
        df["Business_Name"] = df["Business_Name"].str.strip().str.lower()
        # Make sure both product type columns exist in the DataFrame
        for col in ["Category", "Product_Type"]:
            if col in df.columns:
                df[col] = df[col].fillna("").str.strip().str.lower()
        return df

    targets_df = load_targets()
    targets_df = targets_df[targets_df["Business_Name"] == business_name.strip().lower()]

    def get_unique_periods(start, end, agg):
        if agg == "daily":
            return pd.date_range(start=start, end=end, freq="D")
        elif agg == "monthly":
            return pd.date_range(start=start, end=end, freq="MS")
        elif agg == "custom":
            return [start]

    periods = get_unique_periods(Start_Date, End_Date, aggregation)

    item_cols = [
        models.Item.Item_Id, models.Item.Item_Name, models.Item.Item_Type,
        models.Item.Item_Code, models.Item.Sale_Price, models.Item.Current_Stock,
        getattr(models.Item, product_type_column)
    ]
    age_or_size_col = models.Item.Age if business_name == "BEE7W5ND34XQZRM" else models.Item.Size
    item_query = db.query(*item_cols, age_or_size_col)

    if item_filter:
        for field_name, conditions in item_filter.items():
            for condition in conditions:
                op = condition.get("operator")
                value = condition.get("value")
                if isinstance(value, list) and len(value) == 1 and isinstance(value[0], list):
                    value = value[0]

                if op == "In":
                    item_query = item_query.filter(getattr(models.Item, field_name).in_([value] if isinstance(value, str) else value))
                elif op == "Not_In":
                    item_query = item_query.filter(~getattr(models.Item, field_name).in_([value] if isinstance(value, str) else value))

                target_columns = [col.lower() for col in targets_df.columns]
                if field_name.lower() in target_columns:
                    if op == "In":
                        targets_df = targets_df[targets_df[field_name].str.lower().isin([v.lower() for v in value])]
                    elif op == "Not_In":
                        targets_df = targets_df[~targets_df[field_name].str.lower().isin([v.lower() for v in value])]

    col = "Age" if business_name == "BEE7W5ND34XQZRM" else "Size"

    items_data = [row._asdict() for row in item_query.all()]
    t1 = pd.DataFrame(items_data)
    t1["Current_Stock"] = t1["Current_Stock"].astype(int)
    t1["Sale_Price"] = t1["Sale_Price"].astype(int)

    last_sold = db.query(models.Sale.Item_Id, func.max(models.Sale.Date)).group_by(models.Sale.Item_Id).all()
    last_sold = pd.DataFrame(last_sold, columns=["Item_Id", "Date"])
    t1_merged = pd.merge(t1, last_sold, how="left", on="Item_Id")
    t1_merged = t1_merged.rename(columns={"Date": "Last_Sold_Date"})
    t1_merged["Last_Sold_Date"] = pd.to_datetime(t1_merged["Last_Sold_Date"])

    summary = []

    # Pre-process targets to handle date ranges
    # Sort targets by Start_Date and Uploaded_At (for same date conflicts)
    targets_df = targets_df.sort_values(by=["Start_Date", "Uploaded_At"], ascending=[True, False])

    for dt in periods:
        if aggregation == "daily":
            period_start = period_end = dt
        elif aggregation == "monthly":
            period_start = dt
            period_end = dt + pd.offsets.MonthEnd(0)
        elif aggregation == "custom":
            period_start = Start_Date
            period_end = End_Date

        date_label = dt.strftime("%Y-%m-%d") if aggregation == "daily" else f"{period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}"
        days_in_period = (pd.to_datetime(period_end) - pd.to_datetime(period_start)).days + 1

        t1_merged["Available"] = (t1_merged["Current_Stock"] > 0) | (t1_merged["Last_Sold_Date"] >= dt)
        available = t1_merged[t1_merged["Available"]].copy()

        # Find applicable targets for the current date
        # Get targets effective on the current date (target start date <= current date)
        applicable_targets = targets_df[targets_df["Start_Date"] <= dt].copy()
        
        # For each product type, find the most recent target before the current date
        def get_latest_target(group):
            # Sort by Start_Date (descending) and Uploaded_At (descending)
            sorted_group = group.sort_values(by=["Start_Date", "Uploaded_At"], ascending=[False, False])
            # Return the first row (most recent target)
            return sorted_group.iloc[0:1]
        
        # Group by product type and get the latest target for each
        if not applicable_targets.empty:
            # Use the appropriate product_type_column for grouping
            group_col = product_type_column if product_type_column in applicable_targets.columns else "Category"
            applicable_targets = applicable_targets.groupby(group_col, as_index=False).apply(get_latest_target)
            # Reset index to flatten the DataFrame
            applicable_targets = applicable_targets.reset_index(drop=True)

        stock = t1["Current_Stock"].sum()
        stock_val = (t1["Current_Stock"] * t1["Sale_Price"]).sum()
        total_target_value = applicable_targets["Target_Value"].sum() * days_in_period

        if aggregation == "daily":
            sale_data = db.query(models.Sale.Item_Id, func.sum(models.Sale.Quantity), func.sum(models.Sale.Total_Value)).filter(models.Sale.Date == dt).group_by(models.Sale.Item_Id).all()
            views_atc = db.query(models.ViewsAtc.Item_Id, func.sum(models.ViewsAtc.Items_Viewed), func.sum(models.ViewsAtc.Items_Addedtocart)).filter(models.ViewsAtc.Date == dt).group_by(models.ViewsAtc.Item_Id).all()
        else:
            sale_data = db.query(models.Sale.Item_Id, func.sum(models.Sale.Quantity), func.sum(models.Sale.Total_Value)).filter(models.Sale.Date.between(period_start, period_end)).group_by(models.Sale.Item_Id).all()
            views_atc = db.query(models.ViewsAtc.Item_Id, func.sum(models.ViewsAtc.Items_Viewed), func.sum(models.ViewsAtc.Items_Addedtocart)).filter(models.ViewsAtc.Date.between(period_start, period_end)).group_by(models.ViewsAtc.Item_Id).all()

        t2 = pd.DataFrame(sale_data, columns=["Item_Id", "Quantity", "Total_Value"])
        t2 = t2[t2['Item_Id'].isin(t1['Item_Id'].unique())]
        t3 = pd.DataFrame(views_atc, columns=["Item_Id", "Items_Viewed", "Items_Addedtocart"])
        t3["Item_Id"] = t3["Item_Id"].astype("int")
        t3 = t3[t3['Item_Id'].isin(t1['Item_Id'].unique())]

        if not t2.empty:
            sold_item_ids = t2.Item_Id.unique()
            sold_items = t1[t1["Item_Id"].isin(sold_item_ids)]
            sold_col_group = {k: int(v) for k, v in t2.merge(t1, on="Item_Id").groupby(col)["Quantity"].sum().to_dict().items()}
        else:
            sold_items = pd.DataFrame()
            sold_col_group = {}

        t2["Total_Value"] = t2["Total_Value"].astype("float")
        total_quantity_sold = int(t2["Quantity"].sum()) if not t2.empty else 0
        deviation = ((t2.Total_Value.sum() - total_target_value) / total_target_value * 100) if total_target_value else 0
        stock_col_group = t1.groupby(col)["Current_Stock"].sum().to_dict()

        summary.append({
            "Date": date_label,
            f"Total_Number_Of_{product_type_column}_Available": int(available[product_type_column].nunique()),
            f"Number_Of_{product_type_column}_Sold": int(sold_items[product_type_column].nunique()) if not sold_items.empty else 0,
            "Total_Number_Of_Item_Type_Available": int(available["Item_Type"].nunique()),
            "Number_Of_Item_Type_Sold": int(sold_items["Item_Type"].nunique()) if not sold_items.empty else 0,
            "Total_Number_Of_Item_Name_Available": int(available["Item_Name"].nunique()),
            "Number_Of_Item_Name_Sold": int(sold_items["Item_Name"].nunique()) if not sold_items.empty else 0,
            "Total_Number_Of_Item_Code_Available": int(available["Item_Code"].nunique()),
            "Number_Of_Item_Code_Sold": int(sold_items["Item_Code"].nunique()) if not sold_items.empty else 0,
            "Total_Current_Stock": int(stock),
            "Current_Stock_Value": float(stock_val),
            "Total_Quantity_Sold": total_quantity_sold,
            "Total_Sale_Value": int(t2["Total_Value"].sum()) if not t2.empty else 0,
            "Total_Target_Value": int(total_target_value),
            "%_Deviation": round(float(deviation), 2),
            "Total_Items_Viewed": int(t3["Items_Viewed"].sum()) if not t3.empty else 0,
            "Total_Items_Added_To_Cart": int(t3["Items_Addedtocart"].sum()) if not t3.empty else 0,
            f"{col}_Sold_Quantities": sold_col_group,
            f"Current_Stock_By_{col}": stock_col_group
        })

    return pd.DataFrame(summary)