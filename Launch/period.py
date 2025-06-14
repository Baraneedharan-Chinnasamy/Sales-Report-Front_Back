# Updated process_period_data function with proper parameter sync
import pandas as pd
import numpy as np


def process_period_data(t1, t2, t3, t4, t5, temp_t2, t3_total, dt, colu, days, period_df, 
                       period_name, group_by, grp, variation_columns=None):  # Added missing parameter
    
    # Merge with alltime quantity data
    join_cols = 'Item_Id' if group_by.lower() == "item_id" else grp
    df_final = pd.merge(period_df, temp_t2, how="left", on=join_cols)
    df_final["Alltime_Total_Quantity"] = df_final["Alltime_Total_Quantity"].fillna(0)
    
    # Calculate derived columns using vectorized operations
    df_final["Total_Stock"] = df_final["Alltime_Total_Quantity"] + df_final["Current_Stock"]
    df_final["Stock_Sold_Percentage"] = ((df_final["Quantity"] / df_final["Total_Stock"]) * 100).round(2).fillna(0)
    
    # Calculate days since launch
    df_final['launch_date'] = pd.to_datetime(df_final['launch_date'], errors='coerce')
    df_final['days_since_launch'] = (pd.Timestamp.today() - df_final['launch_date']).dt.days
    
    # Merge all-time data
    df_final = pd.merge(df_final, t3_total, how="left", on=join_cols)
    df_final["Alltime_Items_Addedtocart"] = df_final["Alltime_Items_Addedtocart"].fillna(0)
    df_final["Alltime_Items_Viewed"] = df_final["Alltime_Items_Viewed"].fillna(0)
    
    # Merge last sold date efficiently
    if group_by.lower() == "item_id":
        df_final = pd.merge(df_final, t5, how="left", on="Item_Id")
    else:
        t5_with_groups = pd.merge(t5, t1[['Item_Id'] + grp].drop_duplicates(), on='Item_Id', how='left')
        t5_grouped = t5_with_groups.groupby(grp).agg({"Last_Sold_Date": "max"}).reset_index()
        df_final = pd.merge(df_final, t5_grouped, how="left", on=grp)
    
    # Vectorized calculation for days sold out
    df_final['Days_Sold_Out_Past'] = np.where(
        df_final['Current_Stock'] == 0,
        (df_final['Last_Sold_Date'] - df_final['launch_date']).dt.days, 0)
    df_final['Days_Sold_Out_Past'] = df_final['Days_Sold_Out_Past'].fillna(0)
    
    if period_name == "first_period":
        df_final["days_left"] = np.where(df_final["days_since_launch"] > days, days, df_final["days_since_launch"])
    else:
        df_final["days_left"] = np.where(df_final["days_since_launch"] > 2*days, days, df_final["days_since_launch"] - days)

    # Ensure days_left is not zero to avoid division by zero
    df_final["days_left"] = np.maximum(df_final["days_left"], 1)

    # Calculate period-specific metrics using vectorized operations
    df_final["Period_Perday_Quantity"] = df_final["Quantity"] / df_final["days_left"]
    df_final["Period_Perday_View"] = df_final["Items_Viewed"] / df_final["days_left"]
    df_final["Period_Perday_ATC"] = df_final["Items_Addedtocart"] / df_final["days_left"]
    
    # All-time per-day metrics using numpy for vectorized operations
    df_final["Alltime_Perday_Quantity"] = np.where(
        df_final["Current_Stock"] == 0,
        df_final["Alltime_Total_Quantity"] / np.maximum(df_final["Days_Sold_Out_Past"], 1),
        df_final["Alltime_Total_Quantity"] / np.maximum(df_final["days_since_launch"], 1)
    ).round(2)
    df_final["Alltime_Perday_Quantity"] = df_final["Alltime_Perday_Quantity"].fillna(0)
    
    # Calculate sale price after discount
    sale_price_after_discount = (df_final["Sale_Price"] * (100 - df_final["Sale_Discount"]) / 100)
    
    # Calculate stock values (vectorized)
    df_final["Alltime_Total_Quantity_Value"] = df_final["Alltime_Total_Quantity"] * sale_price_after_discount
    df_final["Current_Stock_Value"] = df_final["Current_Stock"] * sale_price_after_discount
    df_final["Total_Stock_Value"] = df_final["Total_Stock"] * sale_price_after_discount
    df_final['Sale_Price_After_Discount'] = sale_price_after_discount
    
    # Rename for clarity
    df_final = df_final.rename(columns={"Quantity": "Quantity_Sold", "Total_Value": "Sold_Quantity_Value"})
    
    # Calculate remaining metrics using vectorized operations
    df_final["Alltime_Perday_View"] = (df_final["Alltime_Items_Viewed"] / np.maximum(df_final["days_since_launch"], 1)).round(2).fillna(0)
    df_final["Alltime_Perday_ATC"] = (df_final["Alltime_Items_Addedtocart"] / np.maximum(df_final["days_since_launch"], 1)).round(2).fillna(0)
    df_final["Total_Stock_Sold_Percentage"] = (df_final["Alltime_Total_Quantity"] / df_final["Total_Stock"] * 100).round(2).fillna(0)
    
    # Avoid division by zero for projected days calculation
    with np.errstate(divide='ignore', invalid='ignore'):
        df_final["Projected_Days_to_Sellout"] = np.where(
            df_final["Alltime_Perday_Quantity"] > 0,
            df_final["Current_Stock"] / df_final["Alltime_Perday_Quantity"],
            np.inf
        )
    
    # Period-specific prediction
    column_name = f"Predicted_Quantity_Next_{days}Days_Based_on_{period_name}"
    df_final[column_name] = np.where(
        df_final["Current_Stock"] != 0,
        df_final["Period_Perday_Quantity"] * days,
        0
    )
    
    # Handle selected columns instead of variations
    if group_by.lower() == "item_id" and dt is not None and not dt.empty:
        if variation_columns:
            print("Hi")
            # Use selected columns instead of variations
            df_selected = create_selected_columns_dataframe(dt, colu, variation_columns)
            df_final = pd.merge(df_final, df_selected, how="left", on="Item_Id")
        else:
            # Just merge basic info without additional columns
            basic_cols = ['Item_Id', 'Item_Name', 'Item_Type', colu]
            df_basic = dt[basic_cols].drop_duplicates()
            df_final = pd.merge(df_final, df_basic, how="left", on="Item_Id", suffixes=('', '_dt'))
    elif group_by.lower() == "item_name" and variation_columns:
        # For item_name grouping, add the selected columns as aggregated values
        print(f"Item_name grouping with selected columns: {variation_columns}")
    else:
        print("No additional columns added")
    
    # Add period identifier
    df_final["Period"] = period_name
    
    # Select columns with period-specific prefix for later renaming
    period_specific_columns = {
        "Quantity_Sold": f"{period_name}_Quantity_Sold",
        "Sold_Quantity_Value": f"{period_name}_Sold_Quantity_Value",
        "Items_Viewed": f"{period_name}_Items_Viewed",
        "Items_Addedtocart": f"{period_name}_Items_Addedtocart",
        "Period_Perday_Quantity": f"{period_name}_Perday_Quantity",
        "Period_Perday_View": f"{period_name}_Perday_View",
        "Period_Perday_ATC": f"{period_name}_Perday_ATC",
        "Stock_Sold_Percentage": f"{period_name}_Stock_Sold_Percentage",
        column_name: column_name
    }
    
    # Rename period-specific columns
    df_final = df_final.rename(columns=period_specific_columns)
    
    return df_final


# Updated function to add selected columns with comma-separated values
def create_selected_columns_dataframe(df, colu, selected_columns=None):
    """
    Create dataframe with selected columns, comma-separating multiple values within groups
    """
    if df is None or df.empty:
        base_cols = ['Item_Id', 'Item_Name', 'Item_Type', colu]
        return pd.DataFrame(columns=base_cols + (selected_columns or []))
    
    # Ensure required columns exist
    required_cols = ['Item_Id', 'Item_Name', 'Item_Type', colu]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Warning: Missing columns in data: {missing_cols}")
        base_cols = ['Item_Id', 'Item_Name', 'Item_Type', colu]
        return pd.DataFrame(columns=base_cols + (selected_columns or []))
    
    df[colu] = df[colu].fillna("None")  # Fill NaN values with "None"
    
    # If no specific columns selected, return basic grouping
    if not selected_columns:
        return df[['Item_Id', 'Item_Name', 'Item_Type', colu]].drop_duplicates()
    
    # Filter selected_columns to only include columns that exist in df
    print(df.columns)
    valid_selected_columns = [col for col in selected_columns if col in df.columns]
    if not valid_selected_columns:
        print(f"Warning: None of the selected columns exist in data: {selected_columns}")
        return df[['Item_Id', 'Item_Name', 'Item_Type', colu]].drop_duplicates()
    
    # Group by the base columns
    grouped = df.groupby(['Item_Name', 'Item_Type', colu])
    result_rows = []

    for (item_name, item_type, colu_value), group in grouped:
        # For each group, we need to handle Item_Id and selected columns
        item_ids = group['Item_Id'].unique()
        
        # Create aggregated values for selected columns
        agg_data = {
            'Item_Name': item_name,
            'Item_Type': item_type,
            colu: colu_value
        }
        
        # For each selected column, comma-separate unique values
        for col in valid_selected_columns:
            unique_values = group[col].dropna().astype(str).unique()
            # Remove empty strings and 'None' values, then join with comma
            clean_values = [val for val in unique_values if val and val != 'None' and val != 'nan']
            agg_data[col] = ', '.join(clean_values) if clean_values else ''
        
        # If multiple Item_Ids in group, create one row per Item_Id with the aggregated column values
        for item_id in item_ids:
            row_data = {'Item_Id': item_id}
            row_data.update(agg_data)
            result_rows.append(row_data)
    
    result_df = pd.DataFrame(result_rows)
    if not result_df.empty:
        print(f"Selected columns dataframe created for {len(result_df)} items with columns: {valid_selected_columns}")
    
    return result_df


