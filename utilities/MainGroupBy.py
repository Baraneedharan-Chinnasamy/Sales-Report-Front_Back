from sqlalchemy import and_, func
import pandas as pd
import numpy as np
from utilities.functions import (
    process_beelittle, process_prathiksham, process_zing)
from utilities.Grouping import (
    group_by_bee, group_by_dic_prathisham, group_by_dic_zing, group_by_dic_adb)


def agg_grp(db, models, business, filter_dict, data_fields, groupby_dict, start_date=None, end_date=None):
    # Parse date range
    start_date = pd.to_datetime(start_date) if start_date else None
    end_date = pd.to_datetime(end_date) if end_date else None

    all_requested_fields = set(data_fields)

# Map derived columns back to raw columns required by processor functions
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
        "__Batch":"batch"
    }

    # Automatically include required raw columns for derived fields
    raw_columns_to_add = {
        raw_col
        for derived_col in all_requested_fields
        if (raw_col := derived_column_dependencies.get(derived_col)) is not None
    }

    required_item_columns = {"Item_Id", "launch_date", "Current_Stock"}
    required_columns = all_requested_fields.union(raw_columns_to_add, required_item_columns)


    # Business-specific groupby and processor
    group_by_dic_map = {
        "BEE7W5ND34XQZRM": (group_by_bee, process_beelittle),
        "PRT9X2C6YBMLV0F": (group_by_dic_prathisham, process_prathiksham),
        "ZNG45F8J27LKMNQ": (group_by_dic_zing, process_zing),
        "ADBXOUERJVK038L": (group_by_dic_adb, None),
    }

    if business not in group_by_dic_map:
        raise ValueError("Invalid business name")

    group_by_dic, cleaner_func = group_by_dic_map[business]

    # Query only required columns
    item_query = db.query(*[
        getattr(models.Item, col) for col in required_columns if hasattr(models.Item, col)
    ])

    # Apply item filters
    # Apply item filters
    item_filter = filter_dict.get("item_filter", {})

    # Resolve field mapping
    field_mapping = {}
    if hasattr(models, "get_db_to_attr_map"):
        field_mapping = models.get_db_to_attr_map()
    else:
        from sqlalchemy.inspection import inspect
        mapper = inspect(models.Item)
        field_mapping = {
            column.name: column.key
            for column in mapper.columns
            if column.key not in {"Item_Id", "Updated_At", "Created_At"}
        }

    for field_name, conditions in item_filter.items():
        # Use mapped attribute if available; else fallback to field_name if it exists
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


    item_data = [row._asdict() for row in item_query.all()]
    t1 = pd.DataFrame(item_data)

    if cleaner_func:
        t1 = cleaner_func(t1)

    # Type conversion with null handling
    if "launch_date" in t1.columns:
        t1["launch_date"] = pd.to_datetime(t1["launch_date"], errors="coerce")
    if "Sale_Price" in t1.columns:
        t1["Sale_Price"] = pd.to_numeric(t1["Sale_Price"], errors="coerce").fillna(0)
    if "Sale_Discount" in t1.columns:
        t1["Sale_Discount"] = pd.to_numeric(t1["Sale_Discount"], errors="coerce").fillna(0)
    if "Current_Stock" in t1.columns:
        t1["Current_Stock"] = pd.to_numeric(t1["Current_Stock"], errors="coerce").fillna(0)

    # Merge First Sold Date
    sales_min_query = db.query(
        func.min(models.Sale.Date).label("First_Sold_Date"),
        models.Sale.Item_Id
    ).group_by(models.Sale.Item_Id)
    t2 = pd.DataFrame(sales_min_query.all(), columns=["First_Sold_Date", "Item_Id"])

    t1 = pd.merge(t1, t2, on="Item_Id", how="left")
    if "launch_date" in t1:
        t1["launch_date"] = t1["launch_date"].fillna(t1["First_Sold_Date"])
    t1.drop(columns=["First_Sold_Date"], inplace=True)

    # Safe calculation of Days_Since_Launch
    current_date = pd.Timestamp.now().normalize()
    t1["Days_Since_Launch"] = round((current_date - t1["launch_date"]).dt.days,0)
    t1["Days_Since_Launch"] = t1["Days_Since_Launch"].fillna(0).clip(lower=0)  # Ensure non-negative

    item_ids = t1["Item_Id"].dropna().unique().tolist()

    # Derive aggregation fields
    group_columns = set(groupby_dict.get("groupby", []))
    agg_fields = [col for col in all_requested_fields if col not in group_columns]

    # Sales-based fields
    if any(f in agg_fields for f in [
        "Total_Quantity", "Total_Value", "Per_Day_Value", "Per_Day_Quantity", "Days_Until_Stockout", "Conversion_Percentage"
    ]):
        query3 = db.query(
            models.Sale.Item_Id,
            func.sum(models.Sale.Quantity).label("Total_Quantity"),
            func.sum(models.Sale.Total_Value).label("Total_Value")
        ).filter(models.Sale.Item_Id.in_(item_ids))

        if start_date and end_date:
            query3 = query3.filter(and_(models.Sale.Date >= start_date, models.Sale.Date <= end_date))

        t3 = pd.DataFrame(query3.group_by(models.Sale.Item_Id).all(), columns=["Item_Id", "Total_Quantity", "Total_Value"])
        t1 = pd.merge(t1, t3, on="Item_Id", how="left")

        # Safe type conversion with null handling
        for col in ["Total_Value", "Total_Quantity"]:
            if col in t1.columns:
                t1[col] = pd.to_numeric(t1[col], errors="coerce").fillna(0)

        # Safe per-day calculations
        t1["Per_Day_Value"] = np.where(
            t1["Days_Since_Launch"] > 0, 
            t1["Total_Value"] / t1["Days_Since_Launch"], 
            0
        )
        t1["Per_Day_Value"] = round((t1["Per_Day_Value"]),2)
        t1["Per_Day_Quantity"] = np.where(
            t1["Days_Since_Launch"] > 0, 
            t1["Total_Quantity"] / t1["Days_Since_Launch"], 
            0
        )
        t1["Per_Day_Quantity"] = round((t1["Per_Day_Quantity"]),2)
        t1["Days_Until_Stockout"] = np.where(
            t1["Per_Day_Quantity"] > 0, 
            t1["Current_Stock"] / t1["Per_Day_Quantity"], 
            np.inf  # Will be replaced with a large number later
        )
        t1["Days_Until_Stockout"] = round((t1["Days_Until_Stockout"]),2)

    # Views/ATC-based fields
    if any(f in agg_fields for f in [
        "Total_Item_Viewed", "Total_Item_Atc", "Per_Day_View", "Per_Day_atc", "Conversion_Percentage"
    ]):
        query4 = db.query(
            models.ViewsAtc.Item_Id,
            func.sum(models.ViewsAtc.Items_Viewed).label("Total_Item_Viewed"),
            func.sum(models.ViewsAtc.Items_Addedtocart).label("Total_Item_Atc")
        ).filter(models.ViewsAtc.Item_Id.in_(item_ids))

        if start_date and end_date:
            query4 = query4.filter(and_(models.ViewsAtc.Date >= start_date, models.ViewsAtc.Date <= end_date))

        t4 = pd.DataFrame(query4.group_by(models.ViewsAtc.Item_Id).all(), columns=["Item_Id", "Total_Item_Viewed", "Total_Item_Atc"])
        if not t4.empty:
            t4["Item_Id"] = t4["Item_Id"].astype('int')
        t1 = pd.merge(t1, t4, on="Item_Id", how="left")
        
        for col in ["Total_Item_Viewed", "Total_Item_Atc"]:
            if col in t1.columns:
                t1[col] = pd.to_numeric(t1[col], errors="coerce").fillna(0)

        # Safe per-day calculations
        t1["Per_Day_View"] = np.where(
            t1["Days_Since_Launch"] > 0, 
            t1["Total_Item_Viewed"] / t1["Days_Since_Launch"], 
            0
        )
        t1["Per_Day_View"] = round((t1["Per_Day_View"]),2)
        t1["Per_Day_atc"] = np.where(
            t1["Days_Since_Launch"] > 0, 
            t1["Total_Item_Atc"] / t1["Days_Since_Launch"], 
            0
        )
        t1["Per_Day_atc"] = round((t1["Per_Day_atc"]),2)

    # Safe conversion calculation
    if "Conversion_Percentage" in agg_fields:
        t1["Conversion_Percentage"] = np.where(
            t1["Total_Item_Atc"] > 0, 
            (t1["Total_Quantity"] / t1["Total_Item_Atc"]) * 100, 0)
        t1["Conversion_Percentage"] = round((t1["Conversion_Percentage"]),2)

    # Clean up any remaining NaN/inf values before aggregation
    # Replace inf with a reasonable large number (e.g., 999999)
    t1.replace([np.inf, -np.inf], 999999, inplace=True)
    
    # Fill NaN values appropriately by column type
    for col in t1.columns:
        if pd.api.types.is_numeric_dtype(t1[col]):
            t1[col] = t1[col].fillna(0)
        elif pd.api.types.is_string_dtype(t1[col]):
            t1[col] = t1[col].fillna("None")
        elif pd.api.types.is_datetime64_any_dtype(t1[col]):
            # Keep datetime NaT as is, will be handled later
            pass
    # Validate requested fields
    missing_fields = [field for field in all_requested_fields if field not in t1.columns]
    if missing_fields:
        raise ValueError(f"The following fields are not available in the data: {missing_fields}")

    # Group and aggregate
    df = t1[list(all_requested_fields)]
    for col in group_columns:
        if col not in df.columns:
            raise ValueError(f"Groupby column '{col}' not found in data")

    agg_dict = {
        col: group_by_dic[col]
        for col in df.columns
        if col not in group_columns and col in group_by_dic
    }

    if not agg_dict:
        # If no aggregation needed, just return unique combinations
        grouped_df = df.drop_duplicates().reset_index(drop=True)
    else:
        grouped_df = df.groupby(list(group_columns)).agg(agg_dict).reset_index()

    if "Per_Day_View" in grouped_df.columns:
        grouped_df["Per_Day_View"] = grouped_df["Per_Day_View"].round(2)
    if "Per_Day_atc" in grouped_df.columns:
        grouped_df["Per_Day_atc"] = grouped_df["Per_Day_atc"].round(2)
    if "Days_Since_Launch" in grouped_df.columns:
        grouped_df["Days_Since_Launch"] = grouped_df["Days_Since_Launch"].round(2)

    # Final cleanup of any remaining NaN/inf values after grouping
    grouped_df.replace([np.inf, -np.inf], 999999, inplace=True)
    
    # Fill any remaining NaN values
    for col in grouped_df.columns:
        if pd.api.types.is_numeric_dtype(grouped_df[col]):
            grouped_df[col] = grouped_df[col].fillna(0)
        elif pd.api.types.is_string_dtype(grouped_df[col]):
            grouped_df[col] = grouped_df[col].fillna("None")

    # Convert date format after grouping if needed
    if "launch_date" in grouped_df.columns:
        grouped_df["launch_date"] = pd.to_datetime(grouped_df["launch_date"], errors="coerce").dt.strftime('%Y-%m-%d')

    return grouped_df