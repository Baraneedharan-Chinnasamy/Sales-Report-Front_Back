import pandas as pd
import os
import json
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
import numpy as np
from functools import lru_cache
from utilities.functions import load_targets, calculate_historical_stock, get_unique_periods, filter_active_targets

def daily_sale_report(db: Session, models, Start_Date, End_Date=None, business_name: str = None, aggregation="daily", item_filter: dict = None, compare_with=None):

    Start_Date = pd.to_datetime(Start_Date)
    End_Date = pd.to_datetime(End_Date) if End_Date else Start_Date
    product_type_column = "Product_Type" if business_name == "BEE7W5ND34XQZRM" else "Category"

    targets_df = load_targets(force_reload=True)
    targets_df = targets_df[targets_df["Business_Name"] == business_name.strip().lower()]
    effective_aggregation = "custom" if aggregation == "compare" else aggregation

    periods = get_unique_periods(Start_Date, End_Date, effective_aggregation)

    comparison_results = None
    if aggregation == "compare" and compare_with and isinstance(compare_with, dict) and 'start_date' in compare_with and 'end_date' in compare_with:
        compare_start = pd.to_datetime(compare_with['start_date'])
        compare_end = pd.to_datetime(compare_with['end_date'])
        comparison_results = daily_sale_report(
            db=db,
            models=models,
            Start_Date=compare_start,
            End_Date=compare_end,
            business_name=business_name,
            aggregation="custom",
            item_filter=item_filter
        )

    previous_periods = []
    for dt in periods:
        if effective_aggregation == "daily":
            previous_periods.append(dt - timedelta(days=1))
        elif effective_aggregation == "weekly":
            previous_periods.append(dt - timedelta(days=7))
        elif effective_aggregation == "monthly":
            previous_month = dt.replace(day=1) - timedelta(days=1)
            previous_periods.append(previous_month.replace(day=1))
        elif effective_aggregation == "custom":
            days_diff = (End_Date - Start_Date).days + 1
            previous_start = Start_Date - timedelta(days=days_diff)
            previous_periods.append(previous_start)

    age_or_size_col_name = "Age" if business_name in ["BEE7W5ND34XQZRM", "ADBXOUERJVK038L"] else "Size"

    item_cols = [models.Item.Item_Id, models.Item.Item_Name, models.Item.Item_Type,
                 models.Item.Item_Code, models.Item.Sale_Price, models.Item.Current_Stock,
                 getattr(models.Item, product_type_column)]

    target_columns = targets_df["Target_Column"].unique()
    for col_name in target_columns:
        if hasattr(models.Item, col_name):
            col = getattr(models.Item, col_name)
            if col not in item_cols:
                item_cols.append(col)

    age_or_size_col = getattr(models.Item, age_or_size_col_name)
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

    items_data = [row._asdict() for row in item_query.all()]
    items_df = pd.DataFrame(items_data)

    if items_df.empty:
        column_names = [col.key for col in item_cols] + [age_or_size_col_name]
        items_df = pd.DataFrame(columns=column_names)
        items_df["Current_Stock"] = pd.Series(dtype=int)
        items_df["Sale_Price"] = pd.Series(dtype=int)
    else:
        items_df["Current_Stock"] = items_df["Current_Stock"].astype(int)
        items_df["Sale_Price"] = items_df["Sale_Price"].astype(int)

    last_sold = db.query(models.Sale.Item_Id, func.max(models.Sale.Date)).group_by(models.Sale.Item_Id).all()
    last_sold_df = pd.DataFrame(last_sold, columns=["Item_Id", "Last_Sold_Date"])

    if not items_df.empty and not last_sold_df.empty:
        items_df = pd.merge(items_df, last_sold_df, how="left", on="Item_Id")
        items_df["Last_Sold_Date"] = pd.to_datetime(items_df["Last_Sold_Date"])
    else:
        items_df["Last_Sold_Date"] = pd.NaT

    for col in target_columns:
        if col in items_df.columns and items_df[col].dtype == object:
            items_df[f"{col}_lower"] = items_df[col].astype(str).str.strip().str.lower()
    previous_sales_data = {}

    for i, prev_dt in enumerate(previous_periods):
        current_dt = periods[i]
        if effective_aggregation == "daily":
            prev_period_start = prev_period_end = prev_dt
            current_period_label = current_dt.strftime("%Y-%m-%d")
        elif effective_aggregation == "weekly":
            prev_period_start = prev_dt - timedelta(days=prev_dt.weekday())
            prev_period_end = prev_period_start + timedelta(days=6)
            current_period_label = f"{current_dt.strftime('%Y-%m-%d')} to {(current_dt + timedelta(days=6 - current_dt.weekday())).strftime('%Y-%m-%d')}"
        elif effective_aggregation == "monthly":
            prev_period_start = prev_dt
            prev_period_end = prev_dt + pd.offsets.MonthEnd(0)
            current_period_label = f"{current_dt.strftime('%Y-%m-%d')} to {(current_dt + pd.offsets.MonthEnd(0)).strftime('%Y-%m-%d')}"
        elif effective_aggregation == "custom":
            days_diff = (End_Date - Start_Date).days + 1
            prev_period_start = prev_dt
            prev_period_end = prev_dt + timedelta(days=days_diff - 1)
            current_period_label = f"{Start_Date.strftime('%Y-%m-%d')} to {End_Date.strftime('%Y-%m-%d')}"

        prev_sale_query = db.query(models.Sale.Item_Id, func.sum(models.Sale.Total_Value)).filter(
            models.Sale.Date.between(prev_period_start, prev_period_end))

        if item_filter:
            for field_name, conditions in item_filter.items():
                for condition in conditions:
                    op = condition.get("operator")
                    value = condition.get("value")
                    if isinstance(value, list) and len(value) == 1 and isinstance(value[0], list):
                        value = value[0]
                    if op == "In":
                        prev_sale_query = prev_sale_query.filter(
                            getattr(models.Item, field_name).in_([value] if isinstance(value, str) else value))
                    elif op == "Not_In":
                        prev_sale_query = prev_sale_query.filter(
                            ~getattr(models.Item, field_name).in_([value] if isinstance(value, str) else value))

        prev_sale_query = prev_sale_query.join(models.Item, models.Sale.Item_Id == models.Item.Item_Id)
        prev_sale_data = prev_sale_query.group_by(models.Sale.Item_Id).all()
        prev_sales_df = pd.DataFrame(prev_sale_data, columns=["Item_Id", "Total_Value"])
        prev_total_sale = prev_sales_df["Total_Value"].sum() if not prev_sales_df.empty else 0
        previous_sales_data[current_period_label] = {
            "overall": float(prev_total_sale),
            "items": {row["Item_Id"]: float(row["Total_Value"]) for _, row in prev_sales_df.iterrows()}
            if not prev_sales_df.empty else {}
        }

    final_results = []
    stock_key = f"Stock_By_{age_or_size_col_name}"
    sales_key = f"Sales_By_{age_or_size_col_name}"

    # Continue to period loop (next part)...
    for i, dt in enumerate(periods):
        if effective_aggregation == "daily":
            period_start = period_end = dt
            days_in_period = 1
        elif effective_aggregation == "weekly":
            period_start = dt - timedelta(days=dt.weekday())
            period_end = period_start + timedelta(days=6)
            days_in_period = 7
        elif effective_aggregation == "monthly":
            period_start = dt
            period_end = dt + pd.offsets.MonthEnd(0)
            days_in_period = (period_end - period_start).days + 1
        elif effective_aggregation == "custom":
            period_start = Start_Date
            period_end = End_Date
            days_in_period = (period_end - period_start).days + 1

        date_label = dt.strftime("%Y-%m-%d") if effective_aggregation == "daily" else f"{period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}"

        if effective_aggregation == "daily":
            sale_data = db.query(models.Sale.Item_Id, func.sum(models.Sale.Quantity), func.sum(models.Sale.Total_Value)).filter(
                models.Sale.Date == dt).group_by(models.Sale.Item_Id).all()
            views_atc = db.query(models.ViewsAtc.Item_Id, func.sum(models.ViewsAtc.Items_Viewed), func.sum(models.ViewsAtc.Items_Addedtocart)).filter(
                models.ViewsAtc.Date == dt).group_by(models.ViewsAtc.Item_Id).all()
        else:
            sale_data = db.query(models.Sale.Item_Id, func.sum(models.Sale.Quantity), func.sum(models.Sale.Total_Value)).filter(
                models.Sale.Date.between(period_start, period_end)).group_by(models.Sale.Item_Id).all()
            views_atc = db.query(models.ViewsAtc.Item_Id, func.sum(models.ViewsAtc.Items_Viewed), func.sum(models.ViewsAtc.Items_Addedtocart)).filter(
                models.ViewsAtc.Date.between(period_start, period_end)).group_by(models.ViewsAtc.Item_Id).all()

        sales_df = pd.DataFrame(sale_data, columns=["Item_Id", "Quantity", "Total_Value"])
        if not sales_df.empty and not items_df.empty:
            sales_df = sales_df[sales_df["Item_Id"].isin(items_df["Item_Id"].unique())]
            sales_df["Quantity"] = sales_df["Quantity"].astype(int)
            sales_df["Total_Value"] = sales_df["Total_Value"].astype(float)

        views_df = pd.DataFrame(views_atc, columns=["Item_Id", "Items_Viewed", "Items_Addedtocart"])
        if not views_df.empty:
            views_df["Item_Id"] = views_df["Item_Id"].astype("int")
            if not items_df.empty:
                views_df = views_df[views_df["Item_Id"].isin(items_df["Item_Id"].unique())]

        historical_items_df = calculate_historical_stock(db, models, items_df, period_start)

        if not historical_items_df.empty:
            historical_items_df["Available"] = (historical_items_df["Historical_Stock"] > 0) | (historical_items_df["Last_Sold_Date"] > dt)

        applicable_targets = filter_active_targets(targets_df, period_start, period_end, effective_aggregation)

        period_result = {
            "Date": date_label,
            "target_wise": []
        }

        if historical_items_df.empty and applicable_targets.empty:
            continue

        available_items = historical_items_df[historical_items_df["Available"]].copy() if "Available" in historical_items_df.columns else pd.DataFrame()
        overall_sales = sales_df if not sales_df.empty else pd.DataFrame(columns=["Item_Id", "Quantity", "Total_Value"])
        overall_views = views_df if not views_df.empty else pd.DataFrame(columns=["Item_Id", "Items_Viewed", "Items_Addedtocart"])

        stock_by_group = available_items.groupby(age_or_size_col_name)
        stock_by_age_size = {
            str(k): {
                "Quantity": int(group["Historical_Stock"].sum()),
                "Item_Name_Count": int(group["Item_Name"].nunique())
            } for k, group in stock_by_group
        }

        sales_by_age_size = {}
        if not overall_sales.empty:
            sold_items_overall = pd.merge(overall_sales, historical_items_df, on="Item_Id")
            sales_group = sold_items_overall.groupby(age_or_size_col_name)
            sales_by_age_size = {
                str(k): {
                    "Quantity": int(group["Quantity"].sum()),
                    "Item_Name_Count": int(group["Item_Name"].nunique())
                } for k, group in sales_group
            }

        total_sale_value_overall = overall_sales["Total_Value"].sum() if not overall_sales.empty else 0
        prev_total_sale = previous_sales_data.get(date_label, {}).get("overall", 0)
        growth_percentage = ((total_sale_value_overall - prev_total_sale) / prev_total_sale * 100) if prev_total_sale > 0 else (float('inf') if total_sale_value_overall > 0 else 0)

        # Update with next part...
        period_result.update({
            "Total_Sale_Value": float(round(total_sale_value_overall, 0)),
            "Sale_Growth_Percentage": float(round(growth_percentage, 2)) if growth_percentage != float('inf') else "N/A (no previous sales)",
            "Total_Current_Stock": int(historical_items_df["Historical_Stock"].sum()),
            "Current_Stock_Value": float((historical_items_df["Historical_Stock"] * historical_items_df["Sale_Price"]).sum()),
            "Sell_Through_Rate": float(round((overall_sales["Quantity"].sum() / historical_items_df["Historical_Stock"].sum()) * 100, 2)) if historical_items_df["Historical_Stock"].sum() > 0 else 0,
            f"Total_Number_Of_{product_type_column}_Available": int(available_items[product_type_column].nunique()) if not available_items.empty else 0,
            "Total_Number_Of_Item_Type_Available": int(available_items["Item_Type"].nunique()) if not available_items.empty else 0,
            "Total_Number_Of_Item_Name_Available": int(available_items["Item_Name"].nunique()) if not available_items.empty else 0,
            "Total_Number_Of_Item_Code_Available": int(available_items["Item_Code"].nunique()) if not available_items.empty else 0,
            stock_key: stock_by_age_size,
            sales_key: sales_by_age_size,
            "Total_Quantity_Sold": int(overall_sales["Quantity"].sum()) if not overall_sales.empty else 0,
            "Conversion_Rate": float(round((overall_views["Items_Addedtocart"].sum() / overall_views["Items_Viewed"].sum()) * 100, 2)) if overall_views["Items_Viewed"].sum() > 0 else 0,
            "Total_Items_Viewed": int(overall_views["Items_Viewed"].sum()) if not overall_views.empty else 0,
            "Total_Items_Added_To_Cart": int(overall_views["Items_Addedtocart"].sum()) if not overall_views.empty else 0
        })

        if not overall_sales.empty:
            period_result.update({
                f"Number_Of_{product_type_column}_Sold": int(sold_items_overall[product_type_column].nunique()),
                "Number_Of_Item_Type_Sold": int(sold_items_overall["Item_Type"].nunique()),
                "Number_Of_Item_Name_Sold": int(sold_items_overall["Item_Name"].nunique()),
                "Number_Of_Item_Code_Sold": int(sold_items_overall["Item_Code"].nunique())
            })
        else:
            period_result.update({
                f"Number_Of_{product_type_column}_Sold": 0,
                "Number_Of_Item_Type_Sold": 0,
                "Number_Of_Item_Name_Sold": 0,
                "Number_Of_Item_Code_Sold": 0
            })

        # Target-wise metrics
        for _, target in applicable_targets.iterrows():
            target_col = target["Target_Column"]
            target_key = target["Target_Key_Lower"]
            daily_target_value = target["Target_Value"]
            adjusted_target_value = daily_target_value * days_in_period

            if target_col not in historical_items_df.columns:
                continue

            target_col_lower = f"{target_col}_lower"
            if target_col_lower in historical_items_df.columns:
                target_items = historical_items_df[historical_items_df[target_col_lower] == target_key].copy()
            else:
                target_items = historical_items_df[historical_items_df[target_col].astype(str).str.strip().str.lower() == target_key].copy()

            if target_items.empty:
                continue

            target_available = target_items[target_items["Available"]].copy() if "Available" in target_items.columns else pd.DataFrame()

            target_sales = pd.DataFrame(columns=["Item_Id", "Quantity", "Total_Value"])
            if not sales_df.empty:
                target_sales = pd.merge(sales_df, target_items[["Item_Id"]], on="Item_Id")

            target_views = pd.DataFrame(columns=["Item_Id", "Items_Viewed", "Items_Addedtocart"])
            if not views_df.empty:
                target_views = pd.merge(views_df, target_items[["Item_Id"]], on="Item_Id")

            target_sold_items = pd.merge(target_sales, target_items, on="Item_Id") if not target_sales.empty else pd.DataFrame()
            stock_group = target_available.groupby(age_or_size_col_name)
            stock_by_age_size = {
                str(k): {
                    "Quantity": int(group["Historical_Stock"].sum()),
                    "Item_Name_Count": int(group["Item_Name"].nunique())
                } for k, group in stock_group
            }

            sales_by_age_size = {}
            if not target_sold_items.empty:
                sales_group = target_sold_items.groupby(age_or_size_col_name)
                sales_by_age_size = {
                    str(k): {
                        "Quantity": int(group["Quantity"].sum()),
                        "Item_Name_Count": int(group["Item_Name"].nunique())
                    } for k, group in sales_group
                }

            # continue in Part 5...
            total_sale_value = target_sales["Total_Value"].sum() if not target_sales.empty else 0

            deviation = ((total_sale_value - adjusted_target_value) / adjusted_target_value * 100) if adjusted_target_value > 0 else 0

            prev_target_sales = 0
            if date_label in previous_sales_data:
                prev_item_sales = previous_sales_data[date_label]["items"]
                for item_id in target_items["Item_Id"]:
                    prev_target_sales += prev_item_sales.get(item_id, 0)

            if prev_target_sales > 0:
                target_growth_percentage = ((total_sale_value - prev_target_sales) / prev_target_sales) * 100
            else:
                target_growth_percentage = float('inf') if total_sale_value > 0 else 0

            target_report = {
                "Target_Column": target_col,
                "Target_Key": target["Target_Key"],
                "Target_Value": float(adjusted_target_value),
                "Total_Sale_Value": float(round(total_sale_value, 0)),
                "Sale_Growth_Percentage": float(round(target_growth_percentage, 2)) if target_growth_percentage != float('inf') else "N/A (no previous sales)",
                "Percentage_Deviation": round(float(deviation), 2),
                "Total_Current_Stock": int(target_items["Historical_Stock"].sum()),
                "Current_Stock_Value": float((target_items["Historical_Stock"] * target_items["Sale_Price"]).sum()),
                "Sell_Through_Rate": float(round((target_sales["Quantity"].sum() / target_items["Historical_Stock"].sum()) * 100, 2)) if target_items["Historical_Stock"].sum() > 0 else 0,
                f"Total_Number_Of_{product_type_column}_Available": int(target_available[product_type_column].nunique()) if not target_available.empty else 0,
                "Total_Number_Of_Item_Type_Available": int(target_available["Item_Type"].nunique()) if not target_available.empty else 0,
                "Total_Number_Of_Item_Name_Available": int(target_available["Item_Name"].nunique()) if not target_available.empty else 0,
                "Total_Number_Of_Item_Code_Available": int(target_available["Item_Code"].nunique()) if not target_available.empty else 0,
                sales_key: sales_by_age_size,
                "Total_Quantity_Sold": int(target_sales["Quantity"].sum()) if not target_sales.empty else 0,
                "Conversion_Rate": float(round((target_views["Items_Addedtocart"].sum() / target_views["Items_Viewed"].sum()) * 100, 2)) if target_views["Items_Viewed"].sum() > 0 else 0,
                "Total_Items_Viewed": int(target_views["Items_Viewed"].sum()) if not target_views.empty else 0,
                "Total_Items_Added_To_Cart": int(target_views["Items_Addedtocart"].sum()) if not target_views.empty else 0,
                stock_key: stock_by_age_size
            }

            if not target_sold_items.empty:
                target_report.update({
                    f"Number_Of_{product_type_column}_Sold": int(target_sold_items[product_type_column].nunique()),
                    "Number_Of_Item_Type_Sold": int(target_sold_items["Item_Type"].nunique()),
                    "Number_Of_Item_Name_Sold": int(target_sold_items["Item_Name"].nunique()),
                    "Number_Of_Item_Code_Sold": int(target_sold_items["Item_Code"].nunique())
                })
            else:
                target_report.update({
                    f"Number_Of_{product_type_column}_Sold": 0,
                    "Number_Of_Item_Type_Sold": 0,
                    "Number_Of_Item_Name_Sold": 0,
                    "Number_Of_Item_Code_Sold": 0
                })

            period_result["target_wise"].append(target_report)

        final_results.append(period_result)
    if not final_results:
        return []

    # Define preferred field order dynamically
    stock_key = f"Stock_By_{age_or_size_col_name}"
    sales_key = f"Sales_By_{age_or_size_col_name}"

    preferred_field_order = [
        "Date",
        "Total_Sale_Value",
        "Sale_Growth_Percentage",
        "Sell_Through_Rate",
        "Conversion_Rate",
        "Total_Quantity_Sold",
        "Total_Current_Stock",
        "Current_Stock_Value",
        "Total_Items_Viewed",
        "Total_Items_Added_To_Cart",
        f"Total_Number_Of_{product_type_column}_Available",
        f"Number_Of_{product_type_column}_Sold",
        "Total_Number_Of_Item_Type_Available",
        "Number_Of_Item_Type_Sold",
        "Total_Number_Of_Item_Name_Available",
        "Number_Of_Item_Name_Sold",
        "Total_Number_Of_Item_Code_Available",
        "Number_Of_Item_Code_Sold",
        stock_key,
        sales_key,
        "target_wise"
    ]

    preferred_target_order = [
        "Target_Column",
        "Target_Key",
        "Target_Value",
        "Total_Sale_Value",
        "Sale_Growth_Percentage",
        "Percentage_Deviation",
        "Sell_Through_Rate",
        "Conversion_Rate",
        "Total_Quantity_Sold",
        "Total_Current_Stock",
        "Current_Stock_Value",
        "Total_Items_Viewed",
        "Total_Items_Added_To_Cart",
        f"Total_Number_Of_{product_type_column}_Available",
        f"Number_Of_{product_type_column}_Sold",
        "Total_Number_Of_Item_Type_Available",
        "Number_Of_Item_Type_Sold",
        "Total_Number_Of_Item_Name_Available",
        "Number_Of_Item_Name_Sold",
        "Total_Number_Of_Item_Code_Available",
        "Number_Of_Item_Code_Sold",
        stock_key,
        sales_key
    ]

    # Reorder results
    ordered_results = []
    for period_result in final_results:
        ordered = {key: period_result.get(key) for key in preferred_field_order if key in period_result}
        if "target_wise" in period_result and isinstance(period_result["target_wise"], list):
            ordered["target_wise"] = [
                {key: t.get(key) for key in preferred_target_order if key in t}
                for t in period_result["target_wise"]
            ]
        ordered_results.append(ordered)

    # Summary metrics
    total_sale_value_all = sum(r.get("Total_Sale_Value", 0) for r in ordered_results)
    total_quantity_all = sum(r.get("Total_Quantity_Sold", 0) for r in ordered_results)
    total_Views_all = sum(r.get("Total_Items_Viewed",0) for r in ordered_results)
    total_Atc_all = sum(r.get("Total_Items_Added_To_Cart",0) for r in ordered_results)

    growth_graph_data = [{
        "period": r["Date"],
        "sale_value": r.get("Total_Sale_Value", 0),
        "growth_percentage": r.get("Sale_Growth_Percentage", 0)
    } for r in ordered_results]

    # Historical stock at start
    historical_stock_df = calculate_historical_stock(db, models, items_df, Start_Date)
    starting_stock = historical_stock_df["Historical_Stock"].sum()

    sell_through_summary = (total_quantity_all / starting_stock * 100) if starting_stock > 0 else 0

    result_summary = {
        "summary": {
            "Start_Date": Start_Date.strftime("%Y-%m-%d"),
            "End_Date": End_Date.strftime("%Y-%m-%d"),
            "Total_Sale_Value": float(total_sale_value_all),
            "Total_Quantity_Sold": int(total_quantity_all),
            "Total_Items_Viewed":int(total_Views_all),
            "Total_Items_Added_To_Cart":int(total_Atc_all),
            "Sell_Through_Rate": round(sell_through_summary, 2),
            "aggregation": aggregation,
            "growth_graph_data": growth_graph_data
        }
    }

    result = {
        "summary": result_summary["summary"],
        "details": ordered_results
    }

    if comparison_results:
        comp_summary = comparison_results.get("summary", {})
        if comp_summary:
            result["summary"]["comparison"] = comp_summary
            result["comparison_details"] = comparison_results.get("details", [])

    return result
