import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from Launch.period import process_period_data, calculate_alltime_metrics
from utilities.functions import process_beelittle, process_prathiksham, process_zing
from sqlalchemy.inspection import inspect

def generate_inventory_summary(db: Session, models, days: int, group_by: str, business: str, 
                              item_filter: dict = None, variation_columns: list = None, 
                              launch_date_filter: str = None, calculate_first_period: bool = True,
                              calculate_second_period: bool = True):

    
    print("variation_columns", variation_columns)
    print("business", business)
    print(f"Period calculation settings - First: {calculate_first_period}, Second: {calculate_second_period}")
    
    # Resolve field mapping for variation columns
    field_mapping = {}
    if hasattr(models, "get_db_to_attr_map"):
        field_mapping = models.get_db_to_attr_map()
    else:
        mapper = inspect(models.Item)
        field_mapping = {
            column.name: column.key
            for column in mapper.columns
            if column.key not in {"Item_Id", "Updated_At", "Created_At"}
        }
    
    # Map variation columns to their actual database column names
    mapped_variation_columns = []
    if variation_columns:
        for col in variation_columns:
            # Check if column needs mapping
            mapped_col = field_mapping.get(col, col)
            
            # Verify the mapped column exists in the model
            if hasattr(models.Item, mapped_col):
                mapped_variation_columns.append(mapped_col)
                print(f"Mapped variation column: {col} -> {mapped_col}")
            else:
                print(f"Warning: Variation column '{col}' (mapped to '{mapped_col}') does not exist in Item model - skipping")
        
        # Update variation_columns to use mapped names
        variation_columns = mapped_variation_columns
    
    # Validate and set grouping columns
    if group_by.lower() == "item_id":
        grp = ["Item_Id"]
    elif group_by.lower() == "item_name":
        if business.upper() in ["PRT9X2C6YBMLV0F", "ZNG45F8J27LKMNQ", "ADBXOUERJVK038L"]:
            grp = ["Item_Name", "Item_Type", "Category"]
        elif business.upper() == "BEE7W5ND34XQZRM":
            grp = ["Item_Name", "Item_Type", "Product_Type"]
    else:
        raise ValueError("group_by must be either 'item_id' or 'item_name'")

    # Set category column based on business
    colu = "Product_Type" if business.upper() == "BEE7W5ND34XQZRM" else "Category"
    
    # Build base query attributes - always include base columns
    base_query_attrs = ["Item_Id", "Item_Name", "Item_Type", "Sale_Price", "Sale_Discount",
                       "Current_Stock", "launch_date"]
    
    # Add business-specific columns
    if business == "BEE7W5ND34XQZRM":
        base_query_attrs.extend(["Product_Type"])
    else:
        base_query_attrs.extend(["Category" if business in ["PRT9X2C6YBMLV0F", "ZNG45F8J27LKMNQ","ADBXOUERJVK038L"] else ""])
    
    # Add mapped variation columns to query
    query_attrs = base_query_attrs.copy()
    if variation_columns:
        for col in variation_columns:
            if col not in query_attrs:
                query_attrs.append(col)
    
    # Remove empty strings from query_attrs
    query_attrs = [attr for attr in query_attrs if attr]

    # Step 2: Build base queries with filtering support
    def apply_item_filter(query):
        """Apply item_filter to a query"""
        if item_filter:
            for field_name, conditions in item_filter.items():
                # Apply field mapping for filter fields as well
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
                        query = query.filter(column_attr.in_([value] if isinstance(value, str) else value))
                    elif op == "Not_In":
                        query = query.filter(~column_attr.in_([value] if isinstance(value, str) else value))
                    elif op == "Less_Than_Or_Equal":
                        query = query.filter(column_attr <= value)
                    elif op == "Greater_Than_Or_Equal":
                        query = query.filter(column_attr >= value)
                    elif op == "Between":
                        if isinstance(value, list) and len(value) == 2:
                            query = query.filter(column_attr.between(value[0], value[1]))
                        else:
                            print(f"Warning: 'Between' operator requires a list of exactly two values — skipping")
        return query

    # Build unified item query for all businesses
    item_query = db.query(*[getattr(models.Item, attr) for attr in query_attrs])
    
    # Apply item filter
    item_query = apply_item_filter(item_query)
    
    # Apply launch date filter if provided
    if launch_date_filter:
        filter_date = pd.to_datetime(launch_date_filter)
        item_query = item_query.filter(models.Item.launch_date >= filter_date)
    
    t1_raw = item_query.all()
    
    if not t1_raw:
        print(f"No items found matching the filters")
        return pd.DataFrame()
    
    # Convert to DataFrame
    rows = [row._asdict() for row in t1_raw]
    t1 = pd.DataFrame(rows)
    
    # Apply business-specific processing
    if business.upper() == "BEE7W5ND34XQZRM":
        t1 = process_beelittle(t1)
    elif business.upper() == "PRT9X2C6YBMLV0F":
        t1 = process_prathiksham(t1)
    elif business.upper() == "ZNG45F8J27LKMNQ":
        t1 = process_zing(t1)

    print(t1.columns)
    
    # Get list of filtered Item_Ids for sales and views filtering
    filtered_item_ids = t1["Item_Id"].unique().tolist()
    
    # Create dt dataframe for variations (now includes all queried columns)
    dt = t1.copy()  # dt now contains all the columns we queried
    
    # Batch all database queries together with item filtering
    sales_query = db.query(models.Sale.Item_Id, models.Sale.Date, models.Sale.Quantity, models.Sale.Total_Value)
    sales_query = sales_query.filter(models.Sale.Item_Id.in_(filtered_item_ids))
    sales = sales_query.all()
    
    viewsatc_query = db.query(models.ViewsAtc.Item_Id, models.ViewsAtc.Date, models.ViewsAtc.Items_Viewed, models.ViewsAtc.Items_Addedtocart)
    viewsatc_query = viewsatc_query.filter(models.ViewsAtc.Item_Id.in_(filtered_item_ids))
    viewsatc = viewsatc_query.all()
    
    first_sold_query = db.query(models.Sale.Item_Id, func.min(models.Sale.Date).label("First_Sold_Date")).group_by(models.Sale.Item_Id)
    first_sold_query = first_sold_query.filter(models.Sale.Item_Id.in_(filtered_item_ids))
    first_sold_dates = first_sold_query.all()
    
    last_sold_query = db.query(models.Sale.Item_Id, func.max(models.Sale.Date).label("Last_Sold_Date")).group_by(models.Sale.Item_Id)
    last_sold_query = last_sold_query.filter(models.Sale.Item_Id.in_(filtered_item_ids))
    last_sold_dates = last_sold_query.all()
    
    # Convert to dataframes
    t2 = pd.DataFrame(sales, columns=["Item_Id", "Date", "Quantity", "Total_Value"])
    t3 = pd.DataFrame(viewsatc, columns=["Item_Id", "Date", "Items_Viewed", "Items_Addedtocart"])
    t4 = pd.DataFrame(first_sold_dates, columns=["Item_Id", "First_Sold_Date"])
    t5 = pd.DataFrame(last_sold_dates, columns=["Item_Id", "Last_Sold_Date"])
    
    # Preprocess data types in one batch to avoid redundant conversions
    t1["launch_date"] = pd.to_datetime(t1["launch_date"])
    t1["Item_Id"] = t1["Item_Id"].astype(int)
    t1["Sale_Price"] = t1["Sale_Price"].astype(int)
    t1["Current_Stock"] = t1["Current_Stock"].astype(int)
    t1["Sale_Discount"] = t1["Sale_Discount"].astype(float)
    
    if not t2.empty:
        t2["Date"] = pd.to_datetime(t2["Date"])
    if not t3.empty:
        t3["Date"] = pd.to_datetime(t3["Date"])
        t3["Item_Id"] = t3["Item_Id"].astype(int)
    if not t5.empty:
        t5["Item_Id"] = t5["Item_Id"].astype(int)
        t5["Last_Sold_Date"] = pd.to_datetime(t5["Last_Sold_Date"])
    
    # Merge first sold date
    t1 = pd.merge(t1, t4, how="left", on="Item_Id")
    t1["launch_date"] = t1["launch_date"].fillna(t1["First_Sold_Date"])
    
    # Pre-calculate all-time aggregations to avoid redundant calculations
    if group_by.lower() == "item_id":
        temp_t2 = t2.groupby("Item_Id").agg({"Quantity": "sum"}).rename(columns={"Quantity": "Alltime_Total_Quantity"}).reset_index() if not t2.empty else pd.DataFrame(columns=["Item_Id", "Alltime_Total_Quantity"])
        t3_total = t3.groupby("Item_Id").agg({
            "Items_Viewed": "sum",
            "Items_Addedtocart": "sum"
        }).rename(columns={
            "Items_Addedtocart": "Alltime_Items_Addedtocart",
            "Items_Viewed": "Alltime_Items_Viewed"
        }).reset_index() if not t3.empty else pd.DataFrame(columns=["Item_Id", "Alltime_Items_Addedtocart", "Alltime_Items_Viewed"])
    else:
        # For item_name grouping, add the grouping columns first
        if not t2.empty:
            t2_with_groups = pd.merge(t2, t1[['Item_Id'] + grp].drop_duplicates(), on='Item_Id', how='left')
            temp_t2 = t2_with_groups.groupby(grp).agg({"Quantity": "sum"}).rename(columns={"Quantity": "Alltime_Total_Quantity"}).reset_index()
        else:
            temp_t2 = pd.DataFrame(columns=grp + ["Alltime_Total_Quantity"])
            
        if not t3.empty:
            t3_with_groups = pd.merge(t3, t1[['Item_Id'] + grp].drop_duplicates(), on='Item_Id', how='left')
            t3_total = t3_with_groups.groupby(grp).agg({
                "Items_Viewed": "sum",
                "Items_Addedtocart": "sum"
            }).rename(columns={
                "Items_Addedtocart": "Alltime_Items_Addedtocart",
                "Items_Viewed": "Alltime_Items_Viewed"
            }).reset_index()
        else:
            t3_total = pd.DataFrame(columns=grp + ["Alltime_Items_Addedtocart", "Alltime_Items_Viewed"])
    
    # Check if both periods are disabled - if so, calculate only all-time metrics
    if not calculate_first_period and not calculate_second_period:
        print("Warning: Both periods disabled. Calculating only all-time metrics.")
        
        # Create a basic dataframe to calculate all-time metrics
        join_cols = 'Item_Id' if group_by.lower() == "item_id" else grp
        
        # Prepare basic data for all-time calculation
        if group_by.lower() == "item_id":
            basic_df = t1[['Item_Id', 'Item_Name', 'Item_Type', colu, 'Sale_Price', 'Sale_Discount', 
                          'Current_Stock', 'launch_date']].copy()
        else:
            basic_df = t1.groupby(grp, as_index=False).agg({
                'Item_Id': 'first',
                'Sale_Price': 'mean',
                'Sale_Discount': 'mean',
                'Current_Stock': 'sum',
                'launch_date': 'min'
            })
        
        # Add variation columns if they exist
        if variation_columns:
            for col in variation_columns:
                if col in t1.columns:
                    if group_by.lower() == "item_id":
                        basic_df[col] = t1[col]
                    else:
                        # For item_name grouping, aggregate variation columns
                        t1_var = t1.groupby(grp)[col].apply(lambda x: ', '.join(sorted(set(x.dropna().astype(str))))).reset_index()
                        basic_df = pd.merge(basic_df, t1_var, on=grp, how='left')
        
        # Calculate all-time metrics using the existing function
        basic_results = calculate_alltime_metrics(basic_df, temp_t2, t3_total, t5, t1, join_cols, group_by, grp)
        
        # Apply column ordering
        basic_results = order_columns(basic_results, colu, variation_columns, [], [])
        
        # Apply basic formatting
        basic_results = format_final_results(basic_results)
        
        print(f"Basic results with all-time metrics shape: {basic_results.shape}")
        return basic_results
    
    # Optimized get_inventory_summary function
    def get_inventory_summary(t1, t2, t3, periods_config):
        """
        Optimized function to calculate inventory summary for multiple periods at once
        
        Args:
            t1: Items dataframe
            t2: Sales dataframe
            t3: Views/ATC dataframe
            periods_config: Dictionary with period configurations
                           e.g., {'first_period': {'start': 0, 'end': days}, 'second_period': {'start': days, 'end': 2*days}}
        
        Returns:
            Dictionary with period results
        """
        results = {}
        
        # Pre-process group mapping for item_name grouping once
        if group_by.lower() == "item_name":
            grp_mapping = t1[['Item_Id'] + grp].drop_duplicates()
            if not t2.empty and 'Item_Name' not in t2.columns:
                t2 = pd.merge(t2, grp_mapping, on='Item_Id', how='left')
            if not t3.empty and 'Item_Name' not in t3.columns:
                t3 = pd.merge(t3, grp_mapping, on='Item_Id', how='left')
        
        join_cols = 'Item_Id' if group_by.lower() == "item_id" else grp
        
        for period_name, period_config in periods_config.items():
            start_offset = period_config['start']
            end_offset = period_config['end']
            
            # Calculate date range for each item
            t1_period = t1.copy()
            t1_period['Start_Date'] = t1_period['launch_date'] + pd.to_timedelta(start_offset, unit='D')
            t1_period['End_Date'] = t1_period['launch_date'] + pd.to_timedelta(end_offset, unit='D')
            t1_period['Period_Days'] = end_offset - start_offset
            
            get_lst = grp + ['Start_Date', 'End_Date']
            
            # Filter data based on date range using vectorized operations
            if not t2.empty:
                t2_merge = pd.merge(t2, t1_period[get_lst], on=join_cols, how='inner')
                t2_filtered = t2_merge[(t2_merge['Date'] >= t2_merge['Start_Date']) & (t2_merge['Date'] <= t2_merge['End_Date'])]
            else:
                t2_filtered = pd.DataFrame(columns=['Item_Id', 'Date', 'Quantity', 'Total_Value'] + (grp if group_by.lower() == "item_name" else []))
                
            if not t3.empty:
                t3_merge = pd.merge(t3, t1_period[get_lst], on=join_cols, how='inner')
                t3_filtered = t3_merge[(t3_merge['Date'] >= t3_merge['Start_Date']) & (t3_merge['Date'] <= t3_merge['End_Date'])]
            else:
                t3_filtered = pd.DataFrame(columns=['Item_Id', 'Date', 'Items_Viewed', 'Items_Addedtocart'] + (grp if group_by.lower() == "item_name" else []))
            
            # Aggregate data by grouping columns
            agg_dict = {
                'Item_Id': 'first' if group_by.lower() == "item_name" else 'first',
                'Current_Stock': 'sum',
                'launch_date': 'min',
                'Period_Days': 'first',
                'Sale_Price': 'mean',
                'Sale_Discount': 'mean',
            }
            
            # Add variation columns to aggregation if they exist
            if variation_columns:
                for col in variation_columns:
                    if col in t1_period.columns:
                        agg_dict[col] = lambda x: ', '.join(sorted(set(x.dropna().astype(str))))
            
            t1_agg = t1_period.groupby(grp, as_index=False).agg(agg_dict)
            t2_agg = t2_filtered.groupby(grp, as_index=False)[['Quantity', 'Total_Value']].sum() if not t2_filtered.empty else pd.DataFrame(columns=grp + ['Quantity', 'Total_Value'])
            t3_agg = t3_filtered.groupby(grp, as_index=False)[['Items_Viewed', 'Items_Addedtocart']].sum() if not t3_filtered.empty else pd.DataFrame(columns=grp + ['Items_Viewed', 'Items_Addedtocart'])
            
            # Merge using one operation when possible
            period_df = pd.merge(t1_agg, t2_agg, on=join_cols, how='left')
            period_df = pd.merge(period_df, t3_agg, on=join_cols, how='left')
            
            # Fill NA values all at once
            period_df = period_df.fillna(0)
            
            results[period_name] = period_df
        
        return results
    
    # Configure periods based on boolean flags
    periods_config = {}
    if calculate_first_period:
        periods_config['first_period'] = {'start': 0, 'end': days}
    if calculate_second_period:
        periods_config['second_period'] = {'start': days, 'end': 2 * days}
    
    # Get all period summaries in one optimized call
    period_summaries = get_inventory_summary(t1, t2, t3, periods_config)
    
    # Process each period using the existing process_period_data function
    period_results = {}
    for period_name, period_df in period_summaries.items():
        print(f"Processing {period_name}...")
        period_results[period_name] = process_period_data(
            t1, t2, t3, t4, t5, temp_t2, t3_total, dt, colu, days, 
            period_df, period_name, group_by, grp, variation_columns
        )
        print(f"Available columns in {period_name}_results:", period_results[period_name].columns.tolist())
    
    # Handle variation column conflicts
    if variation_columns:
        for period_name, period_result in period_results.items():
            for col in variation_columns:
                x_col = f"{col}_x"
                y_col = f"{col}_y"
                if x_col in period_result.columns:
                    period_result[col] = period_result[x_col]
                elif y_col in period_result.columns:
                    period_result[col] = period_result[y_col]
    
    # Create combined results based on available periods
    if len(period_results) == 2:  # Both periods calculated
        first_period_results = period_results['first_period']
        second_period_results = period_results['second_period']
        
        # Get common columns - these are columns that should be shared between periods
        common_cols = ["Item_Id", "Item_Name", "Item_Type", colu, "Sale_Discount", "launch_date", 
                       "days_since_launch", "Projected_Days_to_Sellout", "Days_Sold_Out_Past", 
                       "Current_Stock", "Total_Stock", "Current_Stock_Value", "Total_Stock_Value", 
                       "Sale_Price", "Sale_Price_After_Discount", "Alltime_Total_Quantity",
                       "Alltime_Total_Quantity_Value", "Alltime_Perday_Quantity", "Alltime_Items_Viewed",
                       "Alltime_Perday_View", "Alltime_Items_Addedtocart", "Alltime_Perday_ATC",
                       "Total_Stock_Sold_Percentage"]

        # Add variation columns to common columns if they exist in the results
        if variation_columns:
            print("Checking for variation columns in results")
            for col in variation_columns:
                for period_result in period_results.values():
                    if col in period_result.columns:
                        common_cols.append(col)
                        break
        
        # Both periods calculated - merge them
        combined_results = first_period_results[common_cols].copy()
        
        # Get period-specific columns
        first_period_specific_cols = [col for col in first_period_results.columns 
                                     if col.startswith("first_period") or 
                                     (col.startswith("Predicted_Quantity_Next") and "first_period" in col)]
        
        second_period_specific_cols = [col for col in second_period_results.columns 
                                      if col.startswith("second_period") or 
                                      (col.startswith("Predicted_Quantity_Next") and "second_period" in col)]
        
        # Add first period specific columns
        for col in first_period_specific_cols:
            combined_results[col] = first_period_results[col]
        
        # Add second period specific columns with a single merge
        join_cols = ['Item_Id'] if group_by.lower() == "item_id" else grp
        second_period_cols = join_cols + second_period_specific_cols
        
        # Drop overlapping variation columns from second_period_results to avoid suffixes
        if variation_columns:
            drop_cols = [col for col in variation_columns if col in second_period_results.columns and col not in join_cols]
            second_period_results_clean = second_period_results.drop(columns=drop_cols)
        else:
            second_period_results_clean = second_period_results
        
        combined_results = pd.merge(combined_results, second_period_results_clean[second_period_cols], on=join_cols, how='left')
        
    elif len(period_results) == 1:  # Only one period calculated
        combined_results = list(period_results.values())[0].copy()
    else:
        # No periods calculated (shouldn't reach here due to earlier check)
        print("Error: No periods were calculated")
        return pd.DataFrame()
    
    # Remove duplicate columns
    combined_results = combined_results.loc[:, ~combined_results.columns.duplicated()]
    
    # Apply column ordering
    period_names = list(period_results.keys())
    combined_results = order_columns(combined_results, colu, variation_columns, 
                                   ['first_period'] if 'first_period' in period_names else [],
                                   ['second_period'] if 'second_period' in period_names else [])
    
    # Final formatting
    combined_results = format_final_results(combined_results)

    print(f"Final results shape: {combined_results.shape}")
    print(f"Periods calculated: {list(period_results.keys())}")
    
    return combined_results


def order_columns(df, colu, variation_columns=None, first_period_included=None, second_period_included=None):
    """
    Order columns according to the specified requirements:
    1. Common columns first (Item_Id, Item_Name, etc.)
    2. Variation columns after category/product_type
    3. First period columns
    4. Second period columns
    5. Any remaining columns
    """
    if df.empty:
        return df
    
    # Define the standard column order
    standard_order = [
        "Item_Id",
        "Item_Name", 
        "Item_Type",
        colu,  # Category or Product_Type
    ]
    
    # Add variation columns right after category/product_type
    if variation_columns:
        standard_order.extend([col for col in variation_columns if col in df.columns])
    
    # Continue with remaining common columns
    standard_order.extend([
        "Sale_Price",
        "Sale_Discount", 
        "Sale_Price_After_Discount",
        "launch_date",
        "days_since_launch",
        "Current_Stock",
        "Current_Stock_Value",
        "Total_Stock", 
        "Total_Stock_Value",
        "Alltime_Total_Quantity",
        "Projected_Days_to_Sellout",
        "Days_Sold_Out_Past",
        "Last_Sold_Date"
    ])
    
    # Add remaining all-time columns
    alltime_cols = [
        "Alltime_Total_Quantity_Value",
        "Alltime_Perday_Quantity", 
        "Alltime_Items_Viewed",
        "Alltime_Perday_View",
        "Alltime_Items_Addedtocart", 
        "Alltime_Perday_ATC",
        "Total_Stock_Sold_Percentage"
    ]
    standard_order.extend(alltime_cols)
    
    # Collect period-specific columns
    period_specific_cols = []
    
    # Add first period columns if included
    if first_period_included and 'first_period' in first_period_included:
        first_period_cols = [col for col in df.columns if col.startswith("first_period")]
        period_specific_cols.extend(sorted(first_period_cols))
    
    # Add second period columns if included  
    if second_period_included and 'second_period' in second_period_included:
        second_period_cols = [col for col in df.columns if col.startswith("second_period")]
        period_specific_cols.extend(sorted(second_period_cols))
    
    standard_order.extend(period_specific_cols)
    
    # Find any remaining columns not in our standard order
    remaining_cols = [col for col in df.columns if col not in standard_order]
    standard_order.extend(sorted(remaining_cols))
    
    # Filter to only include columns that actually exist in the dataframe
    final_order = [col for col in standard_order if col in df.columns]
    
    # Reorder the dataframe
    ordered_df = df[final_order]
    
    print(f"Column order applied. First 10 columns: {final_order[:10]}")
    return ordered_df


def format_final_results(df):
    """
    Apply final formatting to the results dataframe
    """
    if df.empty:
        return df
    
    # Round numeric columns
    numeric_cols = df.select_dtypes(include=['number']).columns
    df[numeric_cols] = df[numeric_cols].round(2)
    
    # Format date columns
    if "launch_date" in df.columns and not df["launch_date"].empty:
        if pd.api.types.is_datetime64_any_dtype(df["launch_date"]):
            df["launch_date"] = df["launch_date"].dt.strftime('%Y-%m-%d')
    
    # Sort by Item_Id
    df = df.sort_values(by="Item_Id").reset_index(drop=True)
    
    return df