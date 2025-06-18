import pandas as pd
import warnings
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.inspection import inspect
from utilities.functions import load_targets, calculate_historical_stock, get_unique_periods, filter_active_targets

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

def detiled(db: Session, models, colu, Start_Date, End_Date=None, business_name: str = None, aggregation="daily", group_by_fields=None, item_filter: dict = None):

    if aggregation == "compare":
        aggregation = "custom"
    Start_Date = pd.to_datetime(Start_Date)
    End_Date = pd.to_datetime(End_Date) if End_Date else Start_Date

    product_type_column = "Product_Type" if business_name == "BEE7W5ND34XQZRM" else "Category"
    col = "Age" if business_name == "BEE7W5ND34XQZRM" or business_name == "ADBXOUERJVK038L" else "Size"


    # Load targets and filter for this business
    targets_df = load_targets(force_reload=True)
    targets_df = targets_df[targets_df["Business_Name"] == business_name.strip().lower()].copy()
    targets_df["Target_Key_Lower"] = targets_df["Target_Key"].astype(str).str.lower().str.strip().str.replace(r"\s+", " ", regex=True)

    periods = get_unique_periods(Start_Date, End_Date, aggregation)

    # --- FIX: Create mapping of active targets for each full period range ---
    active_target_map = {}
    for period in periods:
        if aggregation == "daily":
            period_start = period
            period_end = period
        elif aggregation == "weekly":
            period_start = period - timedelta(days=period.weekday())  # Monday
            period_end = period_start + timedelta(days=6)  # Sunday
        elif aggregation == "monthly":
            period_start = period
            period_end = period + pd.offsets.MonthEnd(0)
        elif aggregation == "custom":
            period_start = Start_Date
            period_end = End_Date
        else:
            period_start = period
            period_end = period

        active_targets = filter_active_targets(targets_df, period_start, period_end)
        active_target_map[period] = active_targets

    # Calculate previous periods for growth comparison
    previous_periods = []
    for dt in periods:
        if aggregation == "daily":
            # Previous day
            previous_periods.append(dt - timedelta(days=1))
        elif aggregation == "weekly":
            aligned_dt = dt - timedelta(days=dt.weekday())  # Align to Monday
            previous_periods.append(aligned_dt - timedelta(days=7))  # Previous week's Monday

        elif aggregation == "monthly":
            # Previous month (same day of previous month)
            previous_month = dt.replace(day=1) - timedelta(days=1)
            previous_periods.append(previous_month.replace(day=1))
        elif aggregation == "custom":
            # Previous period of same length
            days_diff = (End_Date - Start_Date).days + 1
            previous_start = Start_Date - timedelta(days=days_diff)
            previous_periods.append(previous_start)



    age_or_size_col = getattr(models.Item, col)
    item_cols = [
        models.Item.Item_Id, models.Item.Item_Name, models.Item.Item_Type,
        models.Item.Item_Code, models.Item.Sale_Price, models.Item.Current_Stock,
        getattr(models.Item, product_type_column)
    ]

    # Add all target columns to the query
    for col_name in targets_df["Target_Column"].unique():
        if hasattr(models.Item, col_name):
            attr = getattr(models.Item, col_name)
            if attr not in item_cols:
                item_cols.append(attr)

    item_query = db.query(*item_cols, age_or_size_col)
    # Field mapping resolution
    if hasattr(models, "get_db_to_attr_map"):
        field_mapping = models.get_db_to_attr_map()
    else:
        mapper = inspect(models.Item)
        field_mapping = {
            column.name: column.key
            for column in mapper.columns
            if column.key not in {"Item_Id", "Updated_At", "Created_At"}
    }

    if item_filter:
        for field_name, conditions in item_filter.items():
            # Try mapped attribute first, fallback to field_name if it's a valid column on the model
            actual_attr = field_mapping.get(field_name, field_name)

            if not hasattr(models.Item, actual_attr):
                print(f"Warning: filter field '{field_name}' not found in model — skipping")
                continue

            column_attr = getattr(models.Item, actual_attr)

            for condition in conditions:
                op = condition.get("operator")
                value = condition.get("value")

                if isinstance(value, list) and len(value) == 1 and isinstance(value[0], list):
                    value = value[0]

                if op == "In":
                    item_query = item_query.filter(column_attr.in_([value] if isinstance(value, str) else value))
                elif op == "Not_In":
                    item_query = item_query.filter(~column_attr.in_([value] if isinstance(value, str) else value))
                elif op == "Equal":
                    item_query = item_query.filter(column_attr == value)
                elif op == "Not_Equal":
                    item_query = item_query.filter(column_attr != value)
                elif op == "Less_Than":
                    item_query = item_query.filter(column_attr < value)
                elif op == "Greater_Than":
                    item_query = item_query.filter(column_attr > value)
                elif op == "Less_Than_Or_Equal":
                    item_query = item_query.filter(column_attr <= value)
                elif op == "Greater_Than_Or_Equal":
                    item_query = item_query.filter(column_attr >= value)
                elif op == "Between":
                    if isinstance(value, list) and len(value) == 2:
                        item_query = item_query.filter(column_attr.between(value[0], value[1]))
                    else:
                        print(f"Warning: 'Between' operator for field '{field_name}' requires exactly two values — skipping")


    item_df = pd.DataFrame([row._asdict() for row in item_query.all()])
    if item_df.empty:
        return pd.DataFrame()

    item_df["Sale_Price"] = item_df["Sale_Price"].apply(lambda x: float(x) if x is not None else 0.0)
    item_df["Current_Stock"] = item_df["Current_Stock"].fillna(0).astype(int)

    last_sold = db.query(models.Sale.Item_Id, func.max(models.Sale.Date)).group_by(models.Sale.Item_Id).all()
    last_sold_df = pd.DataFrame(last_sold, columns=["Item_Id", "Last_Sold_Date"])
    item_df = item_df.merge(last_sold_df, how="left", on="Item_Id")
    item_df["Last_Sold_Date"] = pd.to_datetime(item_df["Last_Sold_Date"])

    # Create lowercase columns for case-insensitive matching with targets
    for col_name in targets_df["Target_Column"].unique():
        if col_name in item_df.columns:
            item_df[f"{col_name}_lower"] = item_df[col_name].astype(str).str.lower().str.strip().str.replace(r"\s+", " ", regex=True)

    if group_by_fields is None:
        group_by_fields = [product_type_column, "Item_Type", "Item_Name"]
        
    # Dictionary to store previous period's aggregated sales by group for growth calculation
    previous_period_grouped_sales = {}
    
    summary_list = []
    
    # First, gather previous period data for all items
    for i, prev_dt in enumerate(previous_periods):
        current_dt = periods[i]
        
        if aggregation == "daily":
            prev_period_start = prev_period_end = prev_dt

        elif aggregation == "weekly":
            prev_period_start = prev_dt
            prev_period_end = prev_dt + timedelta(days=6)

        elif aggregation == "monthly":
            prev_period_start = prev_dt
            prev_period_end = prev_dt + pd.offsets.MonthEnd(0)
        elif aggregation == "custom":
            days_diff = (End_Date - Start_Date).days + 1
            prev_period_start = prev_dt
            prev_period_end = prev_dt + timedelta(days=days_diff-1)
        

        date_label_pre = f"{prev_period_start.strftime('%Y-%m-%d')} to {prev_period_end.strftime('%Y-%m-%d')}" if aggregation == "weekly" else dt.strftime("%Y-%m-%d")

    
        # Get ALL previous period sales data (not just filtered items)
        prev_sale_items = db.query(
            models.Sale.Item_Id, 
            func.sum(models.Sale.Quantity).label('Quantity'), 
            func.sum(models.Sale.Total_Value).label('Total_Value')
        ).filter(
            models.Sale.Date.between(prev_period_start, prev_period_end)
        ).group_by(models.Sale.Item_Id).all()
        
        
        if not prev_sale_items:
            continue
            
        # Create dataframe of previous period sales
        prev_sales_df = pd.DataFrame([{
            'Item_Id': s.Item_Id,
            'Quantity': float(s.Quantity) if s.Quantity else 0,
            'Total_Value': float(s.Total_Value) if s.Total_Value else 0
        } for s in prev_sale_items])
        
        # Join with item data to get grouping fields
        prev_period_items = prev_sales_df.merge(item_df, on="Item_Id", how="inner")
       
        # Store by current date as key
        period_key = f"{prev_period_start.strftime('%Y-%m-%d')} to {prev_period_end.strftime('%Y-%m-%d')}" if aggregation == "weekly" else current_dt.strftime("%Y-%m-%d")
        previous_period_grouped_sales[period_key] = {}
        
        # For target-aware grouping
        use_target_column = "Target_Column" in group_by_fields
        if use_target_column:
            for _, target in targets_df.iterrows():
                target_col = target["Target_Column"]
                target_key_lower = target["Target_Key_Lower"]
                filter_col = f"{target_col}_lower"
                
                if filter_col not in prev_period_items.columns:
                    continue
                    
                target_items = prev_period_items[prev_period_items[filter_col] == target_key_lower].copy()
               
                if target_items.empty:
                    continue
                    
                # Group by the same fields as we'll use for current period
                group_fields = [g for g in group_by_fields if g != "Target_Column" and g in target_items.columns]
                if group_fields:
                    for keys, group in target_items.groupby(group_fields):
                        # Create a unique key for this group
                       
                        if not isinstance(keys, tuple):
                            keys = (keys,)
                        group_key = (target_col, target_key_lower) + tuple(str(k) for k in keys)
                        previous_period_grouped_sales[period_key][group_key] = group["Total_Value"].sum()
                        
                else:
                    # If no additional grouping fields
                    group_key = (target_col, target_key_lower)
                    previous_period_grouped_sales[period_key][group_key] = target_items["Total_Value"].sum()
        else:
            # Regular grouping without target
            valid_group_fields = [f for f in group_by_fields if f in prev_period_items.columns]
            if valid_group_fields:
                for keys, group in prev_period_items.groupby(valid_group_fields):
                    if not isinstance(keys, tuple):
                        keys = (keys,)
                    group_key = tuple(str(k) for k in keys)
                    previous_period_grouped_sales[period_key][group_key] = group["Total_Value"].sum()
            else:
                # If no valid grouping fields, store total
                previous_period_grouped_sales[period_key][('total',)] = prev_period_items["Total_Value"].sum()

    # Now process current periods with updated growth calculation
    for i, dt in enumerate(periods):
        period_start, period_end = dt, dt
        if aggregation == "weekly":
            period_start = dt - timedelta(days=dt.weekday())  # Monday
            period_end = period_start + timedelta(days=6)    
        elif aggregation == "monthly":
            period_start = dt
            period_end = dt + pd.offsets.MonthEnd(0)
        elif aggregation == "custom":
            period_start = Start_Date
            period_end = End_Date

        days_in_period = (period_end - period_start).days + 1
        date_label = f"{period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}" if not aggregation == "daily"  else dt.strftime("%Y-%m-%d")
       

        # Get applicable targets for this period
        applicable_targets = filter_active_targets(targets_df, period_start, period_end)



        sales = db.query(models.Sale.Item_Id, func.sum(models.Sale.Quantity), func.sum(models.Sale.Total_Value))\
            .filter(models.Sale.Date.between(period_start, period_end)).group_by(models.Sale.Item_Id).all()
        views = db.query(models.ViewsAtc.Item_Id, func.sum(models.ViewsAtc.Items_Viewed), func.sum(models.ViewsAtc.Items_Addedtocart))\
            .filter(models.ViewsAtc.Date.between(period_start, period_end)).group_by(models.ViewsAtc.Item_Id).all()

        sales_df = pd.DataFrame(sales, columns=["Item_Id", "Quantity", "Total_Value"])
        views_df = pd.DataFrame(views, columns=["Item_Id", "Items_Viewed", "Items_Addedtocart"])

        views_df["Item_Id"] = views_df["Item_Id"].astype(int)
        sales_df["Quantity"] = sales_df["Quantity"].fillna(0).astype(float)
        sales_df["Total_Value"] = sales_df["Total_Value"].fillna(0).astype(float)
        views_df["Items_Viewed"] = views_df["Items_Viewed"].fillna(0).astype(int)
        views_df["Items_Addedtocart"] = views_df["Items_Addedtocart"].fillna(0).astype(int)

        sales_df = sales_df[sales_df["Item_Id"].isin(item_df["Item_Id"])]
        views_df = views_df[views_df["Item_Id"].isin(item_df["Item_Id"])]

        # Calculate historical stock for each item at period_end date
        historical_df = calculate_historical_stock(db, models, item_df, period_end)
        historical_df["Available"] = (historical_df["Historical_Stock"] > 0) | (historical_df["Last_Sold_Date"] > period_end)

        # Make sure we have Historical_Stock as numeric and handle missing values consistently 
        historical_df["Historical_Stock"] = historical_df["Historical_Stock"].fillna(0).astype(float)

        # Merge sales and views data with historical stock info first
        merged = historical_df.merge(sales_df, on="Item_Id", how="left")
        merged = merged.merge(views_df, on="Item_Id", how="left")
        
        # Fill NA values with 0 for calculation
        merged["Quantity"] = merged["Quantity"].fillna(0)
        merged["Total_Value"] = merged["Total_Value"].fillna(0)
        merged["Items_Viewed"] = merged["Items_Viewed"].fillna(0)
        merged["Items_Addedtocart"] = merged["Items_Addedtocart"].fillna(0)
        merged["Sale_Price"] = merged["Sale_Price"].apply(lambda x: float(x) if x is not None else 0.0)

        # --- Check if Target_Column is in group_by_fields ---
        use_target_column = "Target_Column" in group_by_fields

        # --- Enhanced Target-Aware Grouping ---
        if use_target_column:
            for _, target in applicable_targets.iterrows():
                target_col = target["Target_Column"]
                target_key = target["Target_Key"]
                target_key_lower = target["Target_Key_Lower"]
                filter_col = f"{target_col}_lower"
                
                if filter_col not in merged.columns:
                    continue

                # Filter data for this specific target
                subgroup = merged[merged[filter_col] == target_key_lower].copy()
                if subgroup.empty:
                    continue
                
                # Adjust target value based on period length
                adjusted_target_value = target["Target_Value"] * days_in_period
                
                # Group by the requested fields (excluding Target_Column)
                group_fields = [g for g in group_by_fields if g != "Target_Column" and g in subgroup.columns]
                
                if group_fields:
                    grouped_items = subgroup.groupby(group_fields)
                else:
                    # If no additional grouping fields, create a single group
                    grouped_items = [((), subgroup)]
                
                for keys, group_df in (grouped_items if isinstance(grouped_items, list) else grouped_items):
                    summary = {"Date": date_label}

                    # Insert group-by fields in correct order
                    for field in group_by_fields:
                        if field == "Target_Column":
                            summary["Target_Column"] = target_col
                        elif field in group_df.columns:
                            if isinstance(keys, tuple):
                                summary[field] = keys[group_fields.index(field)]
                            else:
                                summary[field] = keys

                    summary["Target_Key"] = target_key
                    
                    # Get total sale value for this group
                    total_sale_value = group_df["Total_Value"].sum()
                    
                    # Get previous period sale value for growth calculation
                    prev_total_sale = 0
                    if date_label_pre in previous_period_grouped_sales:
                        # Create a matching key for this group
                        if not isinstance(keys, tuple):
                            keys = (keys,) if keys != () else ()
                            
                        if group_fields:
                            group_key = (target_col, target_key_lower) + tuple(str(summary.get(field, '')) for field in group_fields)
                        else:
                            group_key = (target_col, target_key_lower)
                            
                        # Look up the previous period value using the group key
                        prev_total_sale = previous_period_grouped_sales[date_label_pre].get(group_key, 0)
                        print("prev_total_sale",prev_total_sale)
                    
                    # Calculate growth percentage
                    if prev_total_sale == 0 and total_sale_value == 0:
                        summary["Sale_Growth_Percentage"] = 0
                    elif prev_total_sale == 0 and total_sale_value > 0:
                        summary["Sale_Growth_Percentage"] = 100.0
                    elif prev_total_sale > 0 and total_sale_value == 0:
                        summary["Sale_Growth_Percentage"] = -100.0
                    else:
                        growth_percentage = ((total_sale_value - prev_total_sale) / prev_total_sale) * 100
                        summary["Sale_Growth_Percentage"] = float(round(growth_percentage, 2))
                    
                    # Calculate stock values - using the historical stock at period_end
                    total_historical_stock = group_df["Historical_Stock"].sum()
                    total_stock_value = (group_df["Historical_Stock"] * group_df["Sale_Price"]).sum()
                    
                    # Calculate sell-through rate
                    total_quantity_sold = group_df["Quantity"].sum()
                    sell_through_rate = (total_quantity_sold / total_historical_stock) * 100 if total_historical_stock > 0 else 0

                    # Include target-related columns with adjusted target value
                    target_info = {
                        "Total_Target_Value": adjusted_target_value,
                        "%_Deviation": round(((total_sale_value - adjusted_target_value) / adjusted_target_value * 100) if adjusted_target_value else 0, 2),
                    }

                    # Group data by Size/Age for detailed breakdowns with Item_Name_Count
                    size_quantities = {}
                    stock_by_size = {}
                    
                    for size_key, size_group in group_df.groupby(col):
                        size_quantities[str(size_key)] = {
                            "Quantity": float(size_group["Quantity"].sum()),
                            "Item_Name_Count": int(size_group["Item_Name"].nunique())
                        }
                        stock_by_size[str(size_key)] = {
                            "Quantity": int(size_group["Historical_Stock"].sum()),
                            "Item_Name_Count": int(size_group["Item_Name"].nunique())
                        }
                    
                    summary.update({
                        "Total_Quantity_Sold": total_quantity_sold,
                        "Total_Sale_Value": total_sale_value,
                        "Total_Current_Stock": total_historical_stock,
                        "Current_Stock_Value": total_stock_value,
                        "Sell_Through_Rate": float(round(sell_through_rate, 2)),
                        "Conversion_Rate": float(round((group_df["Items_Addedtocart"].sum() / group_df["Items_Viewed"].sum()) * 100, 2)) if group_df["Items_Viewed"].sum() > 0 else 0,
                        "Total_Items_Viewed": group_df["Items_Viewed"].sum(),
                        "Total_Items_Added_To_Cart": group_df["Items_Addedtocart"].sum(),
                        f"Sales_By_{col}": size_quantities,
                        f"Stock_By_{col}": stock_by_size
                    })
                    
                    # Add target information
                    summary.update(target_info)
                    summary_list.append(summary)

        else:
            # Regular grouping (without Target_Column)
            valid_group_fields = [f for f in group_by_fields if f in merged.columns]
            if not valid_group_fields:
                valid_group_fields = [product_type_column]
            
            # First group the data to get item counts and basic aggregations
            grouped_data = merged.groupby(valid_group_fields)
            
            for keys, group_data in grouped_data:
                if not isinstance(keys, tuple):
                    keys = (keys,)
                
                # Create summary dict with group identifying fields
                summary = {"Date": date_label}
                for i, field in enumerate(valid_group_fields):
                    summary[field] = keys[i]
                
                # Get total sale value for this group
                total_sale_value = group_data["Total_Value"].sum()
                
                # Get previous period sale value for this group
                prev_total_sale = 0
                if date_label_pre in previous_period_grouped_sales:
                    # Create a matching key for the previous period lookup
                    group_key = tuple(str(k) for k in keys)
                    prev_total_sale = previous_period_grouped_sales[date_label_pre].get(group_key, 0)
                
                # Calculate growth percentage
                if prev_total_sale == 0 and total_sale_value == 0:
                    summary["Sale_Growth_Percentage"] = 0
                elif prev_total_sale == 0 and total_sale_value > 0:
                    summary["Sale_Growth_Percentage"] = 100.0
                elif prev_total_sale > 0 and total_sale_value == 0:
                    summary["Sale_Growth_Percentage"] = -100.0
                else:
                    growth_percentage = ((total_sale_value - prev_total_sale) / prev_total_sale) * 100
                    summary["Sale_Growth_Percentage"] = float(round(growth_percentage, 2))
                
                # Calculate stock values - using historical stock
                total_historical_stock = group_data["Historical_Stock"].sum()
                total_stock_value = (group_data["Historical_Stock"] * group_data["Sale_Price"]).sum()
                
                # Calculate sell-through rate
                total_quantity_sold = group_data["Quantity"].sum()
                sell_through_rate = (total_quantity_sold / total_historical_stock) * 100 if total_historical_stock > 0 else 0
                
                # Group data by Size/Age for detailed breakdowns with Item_Name_Count
                size_quantities = {}
                stock_by_size = {}
                
                for size_key, size_group in group_data.groupby(col):
                    size_quantities[str(size_key)] = {
                        "Quantity": float(size_group["Quantity"].sum()),
                        "Item_Name_Count": int(size_group["Item_Name"].nunique())
                    }
                    stock_by_size[str(size_key)] = {
                        "Quantity": int(size_group["Historical_Stock"].sum()),
                        "Item_Name_Count": int(size_group["Item_Name"].nunique())
                    }
                
                summary.update({
                    "Total_Quantity_Sold": total_quantity_sold,
                    "Total_Sale_Value": total_sale_value,
                    "Total_Current_Stock": total_historical_stock,
                    "Current_Stock_Value": total_stock_value,
                    "Sell_Through_Rate": float(round(sell_through_rate, 2)),
                    "Conversion_Rate": float(round((group_data["Items_Addedtocart"].sum() / group_data["Items_Viewed"].sum()) * 100, 2)) if group_data["Items_Viewed"].sum() > 0 else 0,
                    "Total_Items_Viewed": group_data["Items_Viewed"].sum(),
                    "Total_Items_Added_To_Cart": group_data["Items_Addedtocart"].sum(),
                    f"Sales_By_{col}": size_quantities,
                    f"Stock_By_{col}": stock_by_size
                })
                
                summary_list.append(summary)

    result_df = pd.DataFrame(summary_list)
    if not result_df.empty:
        drop_cols = ["Date"] + [col for col in group_by_fields if col in result_df.columns]
        if "Target_Key" in result_df.columns and "Target_Column" in group_by_fields:
            drop_cols.append("Target_Key")
        result_df = result_df.drop_duplicates(subset=drop_cols)
        result_df = result_df.sort_values(by=["Date", "Total_Quantity_Sold"], ascending=False)

        # --- Column Reordering ---
        preferred_order = [
            "Date", "Target_Column", "Target_Key","Product_Type","Category","Item_Type","Item_Name","Total_Target_Value", "Total_Sale_Value", "%_Deviation", "Sale_Growth_Percentage",
            "Sell_Through_Rate", "Conversion_Rate", "Total_Quantity_Sold",
            "Total_Current_Stock", "Current_Stock_Value",
            "Total_Items_Viewed", "Total_Items_Added_To_Cart"
        ]

        import re
        remaining_cols = [col for col in result_df.columns if col not in preferred_order]
        size_related = sorted([col for col in remaining_cols if re.search(r"Sales_By_(Size|Age)", col)])
        stock_related = sorted([col for col in remaining_cols if col.startswith("Stock_By_")])

        final_column_order = preferred_order + size_related + stock_related + [
            col for col in remaining_cols if col not in size_related + stock_related
        ]

        result_df = result_df[[col for col in final_column_order if col in result_df.columns]]

    return result_df