import pandas as pd
import os
import json
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func
from functools import lru_cache

def detiled(db: Session, models, colu, Start_Date, End_Date=None, business_name: str = None, aggregation="daily", group_by_fields=None, item_filter: dict = None):
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
        df[product_type_column] = df[product_type_column].str.strip().str.lower()
        return df

    targets_df = load_targets()
    targets_df = targets_df[targets_df["Business_Name"] == business_name.strip().lower()]

    def get_unique_periods(start, end, agg):
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
    item_cols = [
        models.Item.Item_Id, models.Item.Item_Name, models.Item.Item_Type,
        models.Item.Item_Code, models.Item.Sale_Price, models.Item.Current_Stock,
        getattr(models.Item, product_type_column)
    ]
    age_or_size_col = models.Item.Age if business_name == "BEE7W5ND34XQZRM" else models.Item.Size
    col = "Age" if business_name == "BEE7W5ND34XQZRM" else "Size"

    item_query = db.query(*item_cols, age_or_size_col)
    # 🔥 Apply item_filter if provided
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

    items_data = [row._asdict() for row in item_query.all()]
    t1 = pd.DataFrame(items_data)
    t1["Current_Stock"] = t1["Current_Stock"].fillna(0).astype(int)
    t1["Sale_Price"] = t1["Sale_Price"].fillna(0).astype(int)

    # Find Last Sold Date
    last_sold = db.query(models.Sale.Item_Id, func.max(models.Sale.Date)).group_by(models.Sale.Item_Id).all()
    last_sold = pd.DataFrame(last_sold, columns=["Item_Id", "Date"])
    t1 = pd.merge(t1, last_sold, how="left", on="Item_Id").rename(columns={"Date": "Last_Sold_Date"})
    t1["Last_Sold_Date"] = pd.to_datetime(t1["Last_Sold_Date"])

    summary_list = []

    # Set default group_by_fields if not provided
    if group_by_fields is None:
        group_by_fields = [product_type_column, "Item_Type", "Item_Name"]
    
    # Ensure product_type_column is always included in PROCESSING but not necessarily in output
    processing_fields = list(group_by_fields)  # Create a copy of group_by_fields
    if product_type_column not in processing_fields:
        processing_fields.append(product_type_column)

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
        t1["Available"] = (t1["Current_Stock"] > 0) | (t1["Last_Sold_Date"] >= availability_date)
        available = t1[t1["Available"]].copy()

        # For custom aggregation, use the start date for target calculation
        target_date = period_start
        applicable_targets = (
            targets_df[targets_df["Start_Date"] <= target_date]
            .sort_values(by=[product_type_column, "Start_Date", "Uploaded_At"], ascending=[True, False, False])
            .drop_duplicates(subset=product_type_column, keep="first")
        )

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

        t2 = pd.DataFrame(sale_data, columns=["Item_Id", "Quantity", "Total_Value"])
        t3 = pd.DataFrame(views_atc, columns=["Item_Id", "Items_Viewed", "Items_Addedtocart"])


        t3["Item_Id"] = t3["Item_Id"].astype(int)
        # Ensure t2 and t3 are restricted to filtered items only
        if not t1.empty:
            item_ids_set = set(t1["Item_Id"])
            t2 = t2[t2["Item_Id"].isin(item_ids_set)]
            t3 = t3[t3["Item_Id"].isin(item_ids_set)]

        t1_views = pd.merge(t1, t3, how="left", on="Item_Id").fillna({"Items_Viewed": 0, "Items_Addedtocart": 0})

        # Always use processing_fields (which includes product_type_column) for calculations
        all_combos = t1[processing_fields].drop_duplicates()

        grouped_sale = pd.merge(
            t1[processing_fields + ["Item_Id", col, "Item_Code"]],
            t2, how="left", on="Item_Id"
        ).fillna({"Quantity": 0, "Total_Value": 0})

        # Group by processing_fields for internal calculations
        grouped = grouped_sale.groupby(processing_fields).agg({
            "Quantity": "sum",
            "Total_Value": "sum"
        }).reset_index()

        grouped = pd.merge(all_combos, grouped, on=processing_fields, how="left").fillna({"Quantity": 0, "Total_Value": 0})

        # Product type level aggregation for target calculation
        pt_sales = grouped.groupby(product_type_column).agg(
            Total_Sale_Value=('Total_Value', 'sum')
        ).reset_index()

        pt_sales[product_type_column] = pt_sales[product_type_column].str.strip().str.lower()
        applicable_targets[product_type_column] = applicable_targets[product_type_column].str.strip().str.lower()
        pt_sales = pd.merge(pt_sales, applicable_targets, how="left", on=product_type_column)

        pt_sales["Target_Period"] = pt_sales["Target_Value"] * days_in_period
        pt_sales["Total_Sale_Value"] = pt_sales["Total_Sale_Value"].fillna(0)
        pt_sales["Target_Period"] = pt_sales["Target_Period"].astype(float)
        pt_sales["Total_Sale_Value"] = pt_sales["Total_Sale_Value"].astype(float)
        pt_sales["Deviation_Percent"] = ((pt_sales["Total_Sale_Value"] - pt_sales["Target_Period"]) / pt_sales["Target_Period"]) * 100
        pt_sales["Deviation_Percent"] = pt_sales["Deviation_Percent"].fillna(0)
        pt_sales_unique = pt_sales.drop_duplicates(subset=product_type_column)
        pt_deviation_map = pt_sales_unique.set_index(product_type_column)[["Target_Period", "Deviation_Percent"]].to_dict("index")

        

        # Create final summary based on the actual group_by_fields
        for _, row in grouped.iterrows():
            # For custom date range, use a formatted string that includes both dates
            if aggregation == "custom":
                date_str = f"{period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}"
                summary = {"Date": date_str}
            else:
                summary = {"Date": dt}
            
            # Add ONLY the requested group_by_fields
            for field in group_by_fields:
                summary[field] = row.get(field)
            
            # Get product type for internal calculations but don't add to summary if not requested
            pt = row[product_type_column]
            pt_normalized = str(pt).strip().lower() if pd.notnull(pt) else ""
            
            # Get target info based on product type
            target_info = pt_deviation_map.get(pt_normalized, {"Target_Period": 0, "Deviation_Percent": 0})
            target_val = target_info["Target_Period"] 
            deviation = target_info["Deviation_Percent"]
                
            # Create condition to filter items matching this group
            condition = pd.Series(True, index=t1.index)
            for field in processing_fields:
                condition &= (t1[field] == row[field])
                
            item_ids = t1.loc[condition, "Item_Id"].unique()
            avail = available[available["Item_Id"].isin(item_ids)]
            cur = t1[condition]
            
            views_grp = t1_views[condition]
            total_items_viewed = views_grp["Items_Viewed"].sum()
            total_items_added_to_cart = views_grp["Items_Addedtocart"].sum()
            
            stock = cur["Current_Stock"].sum()
            stock_val = (cur["Current_Stock"] * cur["Sale_Price"]).sum()
            
            sold_col_group = cur.merge(t2, on="Item_Id", how="left").groupby(col)["Quantity"].sum().fillna(0).to_dict()
            sold_col_group = {k: int(v) for k, v in sold_col_group.items()}
            stock_col_group = cur.groupby(col)["Current_Stock"].sum().fillna(0).to_dict()
            
            # Add the metrics in consistent order
            summary.update({
                "Total_Quantity_Sold": float(row["Quantity"]),
                "Total_Sale_Value": float(row["Total_Value"]),
                "Total_Current_Stock": int(stock),
                "Current_Stock_Value": float(stock_val),
                "Total_Target_Value": round(float(target_val), 0),
                "%_Deviation": round(float(deviation), 2),
                "Total_Items_Viewed": int(total_items_viewed),
                "Total_Items_Added_To_Cart": int(total_items_added_to_cart),
                f"{col}_Sold_Quantities": sold_col_group,
                f"Current_Stock_By_{col}": stock_col_group
            })
            
            summary_list.append(summary)
    
    result_df = pd.DataFrame(summary_list)
    
    return result_df



