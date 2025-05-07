import pandas as pd
import os
import json
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func
from functools import lru_cache
import warnings

# Suppress pandas warnings
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

def detiled(db: Session, models, colu, Start_Date, End_Date=None, business_name: str = None, aggregation="daily", group_by_fields=None, item_filter: dict = None):
    """
    Generate detailed reporting based on sales, items, and targets data.
    """
    Start_Date = pd.to_datetime(Start_Date)
    End_Date = pd.to_datetime(End_Date) if End_Date else Start_Date

    # Determine which product type column to use based on business name
    product_type_column = "Product_Type" if business_name == "BEE7W5ND34XQZRM" else "Category"
    TARGET_FILE = os.path.join("target_data", "targets.json")

    @lru_cache(maxsize=1)
    def load_targets():
        """Load and prepare targets data from JSON file."""
        if not os.path.exists(TARGET_FILE):
            return pd.DataFrame(columns=["Business_Name", "Product_Type", "Category", "Start_Date", "Target_Value", "Uploaded_At"])
        with open(TARGET_FILE, "r") as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        if df.empty:
            return df
        df["Start_Date"] = pd.to_datetime(df["Start_Date"])
        df["Uploaded_At"] = pd.to_datetime(df["Uploaded_At"])
        df["Business_Name"] = df["Business_Name"].str.strip().str.lower()
        # Ensure both product type columns exist
        for col in ["Product_Type", "Category"]:
            if col in df.columns:
                df[col] = df[col].str.strip().str.lower()
        return df

    targets_df = load_targets()
    if not targets_df.empty:
        targets_df = targets_df[targets_df["Business_Name"] == business_name.strip().lower()]

    def get_unique_periods(start, end, agg):
        """Generate date ranges based on aggregation type."""
        if agg == "daily":
            return pd.date_range(start=start, end=end, freq="D")
        elif agg == "monthly":
            return pd.date_range(start=start, end=end, freq="MS")
        elif agg == "custom":
            # For custom, we just return a single date (the start date)
            # to represent the entire period
            return [start]

    periods = get_unique_periods(Start_Date, End_Date, aggregation)

    # Load Items Table
    col = "Age" if business_name == "BEE7W5ND34XQZRM" else "Size"
    age_or_size_col = getattr(models.Item, col)
    
    # Create list of columns to query including the product type column
    item_cols = [
        models.Item.Item_Id, models.Item.Item_Name, models.Item.Item_Type,
        models.Item.Item_Code, models.Item.Sale_Price, models.Item.Current_Stock,
        getattr(models.Item, product_type_column)
    ]

    item_query = db.query(*item_cols, age_or_size_col)
    
    # Apply item_filter if provided
    if item_filter:
        for field_name, conditions in item_filter.items():
            for condition in conditions:
                op = condition.get("operator")
                value = condition.get("value")

                # Flatten any list-of-lists (in case it's accidentally wrapped)
                if isinstance(value, list) and len(value) == 1 and isinstance(value[0], list):
                    value = value[0]

                if op == "In":
                    item_query = item_query.filter(getattr(models.Item, field_name).in_(
                        [value] if isinstance(value, str) else value
                    ))
                elif op == "Not_In":
                    item_query = item_query.filter(~getattr(models.Item, field_name).in_(
                        [value] if isinstance(value, str) else value
                    ))

    # Get items data and convert to DataFrame
    items_data = [row._asdict() for row in item_query.all()]
    if not items_data:
        return pd.DataFrame()  # Return empty DataFrame if no items found
    
    t1 = pd.DataFrame(items_data)
    t1["Current_Stock"] = t1["Current_Stock"].fillna(0).astype(int)
    t1["Sale_Price"] = t1["Sale_Price"].fillna(0).astype(float)
    
    # Ensure product_type_column exists and has proper format
    if product_type_column in t1.columns:
        t1[product_type_column] = t1[product_type_column].astype(str).str.strip().str.lower()

    # Find Last Sold Date
    last_sold = db.query(models.Sale.Item_Id, func.max(models.Sale.Date)).group_by(models.Sale.Item_Id).all()
    last_sold_df = pd.DataFrame(last_sold, columns=["Item_Id", "Date"]) if last_sold else pd.DataFrame(columns=["Item_Id", "Date"])
    
    if not last_sold_df.empty:
        t1 = pd.merge(t1, last_sold_df, how="left", on="Item_Id").rename(columns={"Date": "Last_Sold_Date"})
        t1["Last_Sold_Date"] = pd.to_datetime(t1["Last_Sold_Date"])
    else:
        t1["Last_Sold_Date"] = pd.NaT

    summary_list = []

    # Set default group_by_fields if not provided
    if group_by_fields is None:
        group_by_fields = [product_type_column, "Item_Type", "Item_Name"]
    
    # Make sure all group_by_fields exist in t1
    valid_group_by_fields = [field for field in group_by_fields if field in t1.columns]
    
    # If no valid fields, use the product_type_column as fallback
    if not valid_group_by_fields and product_type_column in t1.columns:
        valid_group_by_fields = [product_type_column]
    
    # If still empty, prevent errors by using a safe fallback
    if not valid_group_by_fields:
        valid_group_by_fields = ["Item_Id"]
    
    # Ensure product_type_column is always included for internal processing if it exists
    processing_fields = list(set(valid_group_by_fields + [product_type_column] if product_type_column in t1.columns else valid_group_by_fields))

    for dt in periods:
        if aggregation == "daily":
            period_start = period_end = dt
        elif aggregation == "monthly":
            period_start = dt
            period_end = dt + pd.offsets.MonthEnd(0)
        elif aggregation == "custom":
            # For custom aggregation, use the entire date range
            period_start = Start_Date
            period_end = End_Date

        days_in_period = (period_end - period_start).days + 1

        # For availability, use the end date for custom aggregation
        availability_date = period_end if aggregation == "custom" else dt
        t1["Available"] = t1["Current_Stock"].gt(0) | t1["Last_Sold_Date"].ge(availability_date)
        available = t1[t1["Available"]].copy()

        # For custom aggregation, use the start date for target calculation
        target_date = period_start
        
        # Process targets if they exist and product_type_column is in t1
        pt_deviation_map = {}
        if not targets_df.empty and product_type_column in t1.columns:
            applicable_targets = (
                targets_df[targets_df["Start_Date"] <= target_date]
                .sort_values(by=[product_type_column, "Start_Date", "Uploaded_At"], ascending=[True, False, False])
            )
            
            if not applicable_targets.empty:
                applicable_targets = applicable_targets.drop_duplicates(subset=product_type_column, keep="first")

        # Load sales and views data based on the period
        if aggregation == "daily":
            sale_data = db.query(models.Sale.Item_Id, func.sum(models.Sale.Quantity), func.sum(models.Sale.Total_Value))\
                          .filter(models.Sale.Date == dt).group_by(models.Sale.Item_Id).all()
            views_atc = db.query(models.ViewsAtc.Item_Id, func.sum(models.ViewsAtc.Items_Viewed), func.sum(models.ViewsAtc.Items_Addedtocart))\
                          .filter(models.ViewsAtc.Date == dt).group_by(models.ViewsAtc.Item_Id).all()
        else:
            sale_data = db.query(models.Sale.Item_Id, func.sum(models.Sale.Quantity), func.sum(models.Sale.Total_Value))\
                          .filter(models.Sale.Date.between(period_start, period_end)).group_by(models.Sale.Item_Id).all()
            views_atc = db.query(models.ViewsAtc.Item_Id, func.sum(models.ViewsAtc.Items_Viewed), func.sum(models.ViewsAtc.Items_Addedtocart))\
                          .filter(models.ViewsAtc.Date.between(period_start, period_end)).group_by(models.ViewsAtc.Item_Id).all()

        t2 = pd.DataFrame(sale_data, columns=["Item_Id", "Quantity", "Total_Value"]) if sale_data else pd.DataFrame(columns=["Item_Id", "Quantity", "Total_Value"])
        t3 = pd.DataFrame(views_atc, columns=["Item_Id", "Items_Viewed", "Items_Addedtocart"]) if views_atc else pd.DataFrame(columns=["Item_Id", "Items_Viewed", "Items_Addedtocart"])

        # Ensure t2 and t3 have correct data types
        if not t2.empty:
            t2["Quantity"] = t2["Quantity"].fillna(0).astype(float)
            t2["Total_Value"] = t2["Total_Value"].fillna(0).astype(float)
            
        if not t3.empty:
            t3["Item_Id"] = t3["Item_Id"].astype(int)
            t3["Items_Viewed"] = t3["Items_Viewed"].fillna(0).astype(int)
            t3["Items_Addedtocart"] = t3["Items_Addedtocart"].fillna(0).astype(int)
            
        # Ensure t2 and t3 are restricted to filtered items only
        if not t1.empty:
            item_ids_set = set(t1["Item_Id"])
            if not t2.empty:
                t2 = t2[t2["Item_Id"].isin(item_ids_set)]
            if not t3.empty:
                t3 = t3[t3["Item_Id"].isin(item_ids_set)]

        # Create view data
        t1_views = t1.copy()
        if not t3.empty:
            t1_views = pd.merge(t1, t3, how="left", on="Item_Id")
        t1_views["Items_Viewed"] = t1_views.get("Items_Viewed", 0).fillna(0).astype(int)
        t1_views["Items_Addedtocart"] = t1_views.get("Items_Addedtocart", 0).fillna(0).astype(int)

        # Create intermediate dataframe with all needed fields for sales
        item_fields = [col for col in processing_fields if col in t1.columns] + ["Item_Id", col, "Item_Code"]
        item_fields = list(set(item_fields))  # Remove duplicates
        
        grouped_sale = t1[item_fields].copy()
        if not t2.empty:
            grouped_sale = pd.merge(grouped_sale, t2, how="left", on="Item_Id")
            
        grouped_sale["Quantity"] = grouped_sale.get("Quantity", 0).fillna(0).astype(float)
        grouped_sale["Total_Value"] = grouped_sale.get("Total_Value", 0).fillna(0).astype(float)

        # Calculate product type level metrics for targets if product_type_column exists
        if product_type_column in t1.columns and not targets_df.empty:
            pt_sales = grouped_sale.groupby(product_type_column).agg(
                Total_Sale_Value=('Total_Value', 'sum')
            ).reset_index()

            if not pt_sales.empty and not applicable_targets.empty:
                pt_sales[product_type_column] = pt_sales[product_type_column].astype(str).str.strip().str.lower()
                pt_sales = pd.merge(pt_sales, applicable_targets[[product_type_column, "Target_Value"]], 
                                    how="left", on=product_type_column)

                pt_sales["Target_Period"] = pd.to_numeric(pt_sales["Target_Value"], errors='coerce').fillna(0) * days_in_period
                pt_sales["Total_Sale_Value"] = pt_sales["Total_Sale_Value"].fillna(0).astype(float)
                pt_sales["Target_Period"] = pt_sales["Target_Period"].astype(float)
                
                # Safely calculate deviation percent
                pt_sales["Deviation_Percent"] = 0.0  # Default to 0
                mask = pt_sales["Target_Period"] > 0  # Only calculate where target > 0
                if mask.any():
                    pt_sales.loc[mask, "Deviation_Percent"] = ((pt_sales.loc[mask, "Total_Sale_Value"] - 
                                                              pt_sales.loc[mask, "Target_Period"]) / 
                                                              pt_sales.loc[mask, "Target_Period"]) * 100
                
                pt_sales_unique = pt_sales.drop_duplicates(subset=product_type_column)
                pt_deviation_map = pt_sales_unique.set_index(product_type_column)[["Target_Period", "Deviation_Percent"]].to_dict("index")

        # Prepare for grouping - ensure all group_by_fields exist
        valid_group_fields = [f for f in valid_group_by_fields if f in grouped_sale.columns]
        if not valid_group_fields:
            # If no valid fields for grouping, use Item_Id as fallback
            valid_group_fields = ["Item_Id"]
            
        # Group by the valid group_by_fields
        agg_dict = {
            "Quantity": "sum",
            "Total_Value": "sum"
        }
        
        # Only add product_type_column if it exists AND not in group_by_fields
        # This prevents the "cannot insert X, already exists" error
        if product_type_column in grouped_sale.columns and product_type_column not in valid_group_fields:
            agg_dict[product_type_column] = "first"
        
        # Check if any of the group fields are also in the aggregation dict to prevent duplicates
        for field in valid_group_fields:
            if field in agg_dict:
                del agg_dict[field]
                
        # Perform groupby with proper error handling
        try:
            grouped = grouped_sale.groupby(valid_group_fields).agg(agg_dict)
            # Use drop=False to avoid trying to reinsert existing columns
            grouped = grouped.reset_index(drop=False)
        except ValueError as e:
            # If reset_index fails, try an alternative approach
            grouped_data = grouped_sale.groupby(valid_group_fields).agg(agg_dict)
            # Manually construct the dataframe to avoid column conflicts
            index_values = grouped_data.index.to_frame()
            data_values = grouped_data.reset_index(drop=True)
            grouped = pd.concat([index_values, data_values], axis=1)

        # Create final summary with properly grouped data
        for _, row in grouped.iterrows():
            # For custom date range, use a formatted string that includes both dates
            if aggregation == "custom":
                date_str = f"{period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}"
                summary = {"Date": date_str}
            else:
                summary = {"Date": dt}
            
            # Add ONLY the requested group_by_fields that exist in the data
            for field in valid_group_fields:
                if field in row and pd.notna(row[field]):
                    summary[field] = row[field]
                else:
                    summary[field] = None
            
            # Get product type for target calculation if available
            target_val = 0
            deviation = 0
            
            if product_type_column in row and pt_deviation_map:
                pt = row.get(product_type_column)
                if pd.notna(pt):
                    pt_normalized = str(pt).strip().lower()
                    # Get target info based on product type
                    target_info = pt_deviation_map.get(pt_normalized, {"Target_Period": 0, "Deviation_Percent": 0})
                    target_val = target_info["Target_Period"]
                    deviation = target_info["Deviation_Percent"]
                
            # Create condition to filter items matching this group
            condition = pd.Series(True, index=t1.index)
            for field in valid_group_fields:
                if field in row and field in t1.columns:
                    if pd.isna(row[field]):
                        condition &= t1[field].isna()
                    else:
                        condition &= (t1[field] == row[field])
                
            item_ids = t1.loc[condition, "Item_Id"].unique() if not condition.empty else []
            avail = available[available["Item_Id"].isin(item_ids)] if item_ids.size > 0 else pd.DataFrame()
            cur = t1[condition] if not condition.empty else pd.DataFrame()
            
            # Calculate metrics
            views_grp = t1_views[condition] if not condition.empty else pd.DataFrame()
            total_items_viewed = views_grp["Items_Viewed"].sum() if not views_grp.empty else 0
            total_items_added_to_cart = views_grp["Items_Addedtocart"].sum() if not views_grp.empty else 0
            
            stock = cur["Current_Stock"].sum() if not cur.empty else 0
            stock_val = (cur["Current_Stock"] * cur["Sale_Price"]).sum() if not cur.empty else 0
            
            # Handle col grouping safely
            sold_col_group = {}
            stock_col_group = {}
            
            if not cur.empty and col in cur.columns:
                # For sold quantities by col
                if not t2.empty:
                    sold_df = cur.merge(t2, on="Item_Id", how="left")
                    sold_df["Quantity"] = sold_df["Quantity"].fillna(0)
                    sold_col_group = sold_df.groupby(col)["Quantity"].sum().fillna(0).to_dict()
                
                # For stock by col
                stock_col_group = cur.groupby(col)["Current_Stock"].sum().fillna(0).to_dict()
            
            # Convert any numpy/pandas numeric types to Python native types
            sold_col_group = {str(k): float(v) for k, v in sold_col_group.items()}
            stock_col_group = {str(k): int(v) for k, v in stock_col_group.items()}
            
            # Add the metrics in consistent order
            summary.update({
                "Total_Quantity_Sold": float(row.get("Quantity", 0)),
                "Total_Sale_Value": float(row.get("Total_Value", 0)),
                "Total_Current_Stock": int(stock),
                "Current_Stock_Value": float(stock_val),
                "Total_Target_Value": round(float(target_val), 0),
                "%_Deviation": round(float(deviation), 2),
                "Total_Items_Viewed": int(total_items_viewed),
                "Total_Items_Added_To_Cart": int(total_items_added_to_cart),
                f"{col}_Sold_Quantities": sold_col_group,
                f"Current_Stock_By_{col}": stock_col_group
            })
            
            # Add to summary_list
            summary_list.append(summary)
    
    result_df = pd.DataFrame(summary_list) if summary_list else pd.DataFrame()
    
    # Final check for duplicates based on Date and group_by_fields
    if not result_df.empty:
        duplicate_cols = ["Date"] + [f for f in valid_group_fields if f in result_df.columns]
        result_df = result_df.drop_duplicates(subset=duplicate_cols)
    
    return result_df