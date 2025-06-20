from sqlalchemy import and_, func, inspect
import pandas as pd
import numpy as np
from utilities.functions import (
    process_beelittle, process_prathiksham, process_zing)
from GroupBy.Grouping import (
    group_by_bee, group_by_dic_prathisham, group_by_dic_zing, group_by_dic_adb)


def agg_grp(db, models, business, filter_dict, data_fields, groupby_dict, launch_agg=None, launch_date=None, start_date=None, end_date=None):
    start_date = pd.to_datetime(start_date) if start_date else None
    end_date = pd.to_datetime(end_date) if end_date else None

    all_requested_fields = set(data_fields)

    derived_column_dependencies = {
        "Print_Style_1": "print_style",
        "Print_Style_2": "print_style",
        "Print_Theme_1": "print_theme",
        "Print_Theme_2": "print_theme",
        "Print_Key_Motif_1": "print_key_motif",
        "Print_Key_Motif_2": "print_key_motif",
        "Print_Colour_1": "print_colour",
        "Print_Colour_2": "print_colour",
        "Colour_1": "Colour",
        "Colour_2": "Colour",
        "__Batch": "batch"
    }

    # Define metric dependencies
    metric_dependencies = {
        "Per_Day_Value": {"Total_Value", "Days_Since_Launch"},
        "Per_Day_Quantity": {"Total_Quantity", "Days_Since_Launch"},
        "Per_Day_View": {"Total_Item_Viewed", "Days_Since_Launch"},
        "Per_Day_atc": {"Total_Item_Atc", "Days_Since_Launch"},
        "Conversion_Percentage": {"Total_Quantity", "Total_Item_Atc"},
        "Days_Since_Launch": {"launch_date"}  # Days_Since_Launch depends on launch_date
    }

    # Define derived metrics that are calculated after grouping
    derived_metrics = {
        "Per_Day_Value",
        "Per_Day_Quantity", 
        "Per_Day_View",
        "Per_Day_atc",
        "Conversion_Percentage"
    }

    # Add dependent fields automatically
    fields_to_add = set()
    for field in all_requested_fields:
        if field in metric_dependencies:
            fields_to_add.update(metric_dependencies[field])
    
    all_requested_fields.update(fields_to_add)

    raw_columns_to_add = {
        raw_col
        for derived_col in all_requested_fields
        if (raw_col := derived_column_dependencies.get(derived_col)) is not None
    }

    required_item_columns = {"Item_Id"}
    
    # Only add launch_date if Days_Since_Launch is needed
    if "Days_Since_Launch" in all_requested_fields:
        required_item_columns.add("launch_date")
    
    # Only add Current_Stock if it's specifically requested
    if "Current_Stock" in all_requested_fields:
        required_item_columns.add("Current_Stock")

    required_columns = all_requested_fields.union(raw_columns_to_add, required_item_columns)

    group_by_dic_map = {
        "BEE7W5ND34XQZRM": (group_by_bee, process_beelittle),
        "PRT9X2C6YBMLV0F": (group_by_dic_prathisham, process_prathiksham),
        "ZNG45F8J27LKMNQ": (group_by_dic_zing, process_zing),
        "ADBXOUERJVK038L": (group_by_dic_adb, None),
    }

    if business not in group_by_dic_map:
        raise ValueError("Invalid business name")

    group_by_dic, cleaner_func = group_by_dic_map[business]

    item_query = db.query(*[
        getattr(models.Item, col) for col in required_columns if hasattr(models.Item, col)
    ])

    item_filter = filter_dict.get("item_filter", {})
    field_mapping = models.get_db_to_attr_map() if hasattr(models, "get_db_to_attr_map") else {
        column.name: column.key
        for column in inspect(models.Item).columns
        if column.key not in {"Item_Id", "Updated_At", "Created_At"}
    }

    for field_name, conditions in item_filter.items():
        model_attr = field_mapping.get(field_name, field_name)
        if not hasattr(models.Item, model_attr):
            print(f"Warning: filter field '{field_name}' not found in model — skipping")
            continue
        column_attr = getattr(models.Item, model_attr)

        for condition in conditions:
            op = condition.get("operator")
            value = condition.get("value")
            if isinstance(value, list) and len(value) == 1 and isinstance(value[0], list):
                value = value[0]
            if op == "In":
                item_query = item_query.filter(column_attr.in_([value] if isinstance(value, str) else value))
            elif op == "Not_In":
                item_query = item_query.filter(~column_attr.in_([value] if isinstance(value, str) else value))
            elif op == "Less_Than_Or_Equal":
                item_query = item_query.filter(column_attr <= value)
            elif op == "Greater_Than_Or_Equal":
                item_query = item_query.filter(column_attr >= value)
            elif op == "Between" and isinstance(value, list) and len(value) == 2:
                item_query = item_query.filter(column_attr.between(value[0], value[1]))

    t1 = pd.DataFrame([row._asdict() for row in item_query.all()])
    if cleaner_func:
        t1 = cleaner_func(t1)

    # Only process launch_date if it exists and is needed
    if "launch_date" in t1.columns:
        t1["launch_date"] = pd.to_datetime(t1["launch_date"], errors="coerce")
    
    if "Sale_Price" in t1.columns:
        t1["Sale_Price"] = pd.to_numeric(t1["Sale_Price"], errors="coerce").fillna(0)
    if "Sale_Discount" in t1.columns:
        t1["Sale_Discount"] = pd.to_numeric(t1["Sale_Discount"], errors="coerce").fillna(0)
    if "Current_Stock" in t1.columns:
        t1["Current_Stock"] = pd.to_numeric(t1["Current_Stock"], errors="coerce").fillna(0)

    # Only get First_Sold_Date if launch_date processing is needed
    if "launch_date" in t1.columns:
        t2 = pd.DataFrame(db.query(
            func.min(models.Sale.Date).label("First_Sold_Date"),
            models.Sale.Item_Id
        ).group_by(models.Sale.Item_Id).all(), columns=["First_Sold_Date", "Item_Id"])

        t1 = pd.merge(t1, t2, on="Item_Id", how="left")
        t1["launch_date"] = t1["launch_date"].fillna(t1["First_Sold_Date"])
        t1.drop(columns=["First_Sold_Date"], inplace=True)

    # Only calculate Days_Since_Launch if needed
    if "Days_Since_Launch" in all_requested_fields and "launch_date" in t1.columns:
        current_date = pd.Timestamp.now().normalize()
        t1["Days_Since_Launch"] = (current_date - t1["launch_date"]).dt.days
        t1["Days_Since_Launch"] = t1["Days_Since_Launch"].fillna(0).clip(lower=0)

    item_ids = t1["Item_Id"].dropna().unique().tolist()
    group_columns = set(groupby_dict.get("groupby", []))
    agg_fields = [col for col in all_requested_fields if col not in group_columns]

    # Sales data - only if needed
    sales_metrics = {"Total_Quantity", "Total_Value", "Conversion_Percentage"}
    if any(f in agg_fields for f in sales_metrics):
        query3 = db.query(
            models.Sale.Item_Id,
            func.sum(models.Sale.Quantity).label("Total_Quantity"),
            func.sum(models.Sale.Total_Value).label("Total_Value")
        ).filter(models.Sale.Item_Id.in_(item_ids))

        if start_date and end_date:
            query3 = query3.filter(and_(models.Sale.Date >= start_date, models.Sale.Date <= end_date))

        t3 = pd.DataFrame(query3.group_by(models.Sale.Item_Id).all(), columns=["Item_Id", "Total_Quantity", "Total_Value"])
        t1 = pd.merge(t1, t3, on="Item_Id", how="left")
        for col in ["Total_Value", "Total_Quantity"]:
            if col in t1.columns:
                t1[col] = pd.to_numeric(t1[col], errors="coerce").fillna(0)

    # Views/ATC data - only if needed
    views_metrics = {"Total_Item_Viewed", "Total_Item_Atc", "Conversion_Percentage"}
    if any(f in agg_fields for f in views_metrics):
        query4 = db.query(
            models.ViewsAtc.Item_Id,
            func.sum(models.ViewsAtc.Items_Viewed).label("Total_Item_Viewed"),
            func.sum(models.ViewsAtc.Items_Addedtocart).label("Total_Item_Atc")
        ).filter(models.ViewsAtc.Item_Id.in_(item_ids))

        if start_date and end_date:
            query4 = query4.filter(and_(models.ViewsAtc.Date >= start_date, models.ViewsAtc.Date <= end_date))

        t4 = pd.DataFrame(query4.group_by(models.ViewsAtc.Item_Id).all(), columns=["Item_Id", "Total_Item_Viewed", "Total_Item_Atc"])
        if not t4.empty:
            t4["Item_Id"] = t4["Item_Id"].astype(int)
        t1 = pd.merge(t1, t4, on="Item_Id", how="left")
        for col in ["Total_Item_Viewed", "Total_Item_Atc"]:
            if col in t1.columns:
                t1[col] = pd.to_numeric(t1[col], errors="coerce").fillna(0)

    t1.replace([np.inf, -np.inf], 999999, inplace=True)
    for col in t1.columns:
        if pd.api.types.is_numeric_dtype(t1[col]):
            t1[col] = t1[col].fillna(0)
        elif pd.api.types.is_string_dtype(t1[col]):
            t1[col] = t1[col].fillna("None")

    # Check for missing fields - exclude derived metrics that are calculated after grouping
    base_fields = [field for field in data_fields if field not in derived_metrics]
    missing_fields = [field for field in base_fields if field not in t1.columns]
    if missing_fields:
        raise ValueError(f"The following fields are not available in the data: {missing_fields}")

    # Get all available fields from the original request
    available_fields = [field for field in all_requested_fields if field in t1.columns]
    df = t1[available_fields]
    
    for col in group_columns:
        if col not in df.columns:
            raise ValueError(f"Groupby column '{col}' not found in data")

    agg_dict = {
        col: group_by_dic[col]
        for col in df.columns
        if col not in group_columns and col in group_by_dic
    }

    if not agg_dict:
        grouped_df = df.drop_duplicates().reset_index(drop=True)
    else:
        grouped_df = df.groupby(list(group_columns)).agg(agg_dict).reset_index()

    # Calculate derived metrics after grouping - only if the columns exist
    # Check if any per-day metrics are requested
    per_day_metrics_requested = any(metric in data_fields for metric in ["Per_Day_Value", "Per_Day_Quantity", "Per_Day_View", "Per_Day_atc"])
    
    if "Days_Since_Launch" in grouped_df.columns and (per_day_metrics_requested or "Days_Since_Launch" in data_fields):
        grouped_df["Days_Since_Launch"] = grouped_df["Days_Since_Launch"].round(2)
        # Don't replace 0 with NaN for division - handle it in each calculation
        days_since_launch_safe = grouped_df["Days_Since_Launch"].replace(0, np.nan)

        if "Per_Day_Value" in data_fields and "Total_Value" in grouped_df.columns:
            grouped_df["Per_Day_Value"] = grouped_df["Total_Value"] / days_since_launch_safe

        if "Per_Day_Quantity" in data_fields and "Total_Quantity" in grouped_df.columns:
            grouped_df["Per_Day_Quantity"] = grouped_df["Total_Quantity"] / days_since_launch_safe

        if "Per_Day_View" in data_fields and "Total_Item_Viewed" in grouped_df.columns:
            grouped_df["Per_Day_View"] = grouped_df["Total_Item_Viewed"] / days_since_launch_safe

        if "Per_Day_atc" in data_fields and "Total_Item_Atc" in grouped_df.columns:
            grouped_df["Per_Day_atc"] = grouped_df["Total_Item_Atc"] / days_since_launch_safe

    if "Conversion_Percentage" in data_fields and {"Total_Quantity", "Total_Item_Atc"}.issubset(grouped_df.columns):
        grouped_df["Conversion_Percentage"] = np.where(
            grouped_df["Total_Item_Atc"] > 0,
            (grouped_df["Total_Quantity"] / grouped_df["Total_Item_Atc"]) * 100,
            0
        )

    # Round metrics that were calculated
    for col in ["Per_Day_View", "Per_Day_atc", "Per_Day_Value", "Per_Day_Quantity", "Conversion_Percentage"]:
        if col in grouped_df.columns:
            grouped_df[col] = grouped_df[col].round(2)

    # Clean up final data
    grouped_df.replace([np.inf, -np.inf], 999999, inplace=True)
    for col in grouped_df.columns:
        if pd.api.types.is_numeric_dtype(grouped_df[col]):
            grouped_df[col] = grouped_df[col].fillna(0)
        elif pd.api.types.is_string_dtype(grouped_df[col]):
            grouped_df[col] = grouped_df[col].fillna("None")

    if "launch_date" in grouped_df.columns:
        grouped_df["launch_date"] = pd.to_datetime(grouped_df["launch_date"], errors="coerce").dt.strftime('%Y-%m-%d')

    # Return only the originally requested fields
    final_fields = [field for field in data_fields if field in grouped_df.columns]
    return grouped_df[final_fields]