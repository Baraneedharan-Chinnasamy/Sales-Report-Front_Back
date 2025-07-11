group_by_dic_prathisham = {
    "Item_Id": "min",
    "Item_Name": "nunique",
    "Item_Type": "nunique",
    "Item_Code": "count",
    "Sale_Price": "mean",
    "Sale_Discount": lambda x: (x != 0).sum(),
    "Current_Stock": "sum",
    "Is_Public": "count",
    "Category": "nunique",
    "Colour": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Fabric": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Fit": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Lining": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Neck": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Occasion": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Print": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Size": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Sleeve": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "batch": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "bottom_length": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "bottom_print": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "bottom_type": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "collections": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "details": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "launch_date": "min",
    "pocket": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "top_length": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "waistband": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Pack": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Days_Since_Launch": "mean",
    "Total_Quantity": "sum",
    "Total_Value": "sum",
    "Total_Item_Viewed": "sum",
    "Total_Item_Atc": "sum",
    # Derived metrics - do not include aggregation functions
    # These will be calculated after grouping
}

group_by_bee = {
    "Item_Id": "min",
    "Item_Name": "nunique",
    "Item_Type": "nunique",
    "Item_Code": "count",
    "Sale_Price": "mean",
    "Sale_Discount": lambda x: (x != 0).sum(),
    "Current_Stock": "sum",
    "Is_Public": "count",
    "Age": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Discount": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Bottom": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Bundles": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Fabric": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Filling": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Gender": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Pack_Size": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Pattern": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Product_Type": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Sale": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Size": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Sleeve": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Style": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Top": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Weave_Type": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Weight": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Width": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "batch": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "bottom_fabric": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "brand_name": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "discounts": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "inventory_type": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "launch_date": "min",
    "offer_date": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "quadrant": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "relist_date": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "restock_status": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "season": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "season_style": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "seasons_style": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "print_size": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Print_Style": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Colour": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Print_Theme": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Print_Colour": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Print_Key_Motif": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Days_Since_Launch": "mean",
    "Total_Quantity": "sum",
    "Total_Value": "sum",
    "Total_Item_Viewed": "sum",
    "Total_Item_Atc": "sum",
    # Derived metrics - do not include aggregation functions
    # These will be calculated after grouping
}

group_by_dic_zing = {
    "Item_Id": "min",
    "Item_Name": "nunique",
    "Item_Type": "nunique",
    "Item_Code": "count",
    "Sale_Price": "mean",
    "Sale_Discount": lambda x: (x != 0).sum(),
    "Current_Stock": "sum",
    "Is_Public": "count",
    "Category": "nunique",
    "Colour": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Fabric": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Fit": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Neck": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Occasion": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Print": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Size": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Sleeve": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "batch": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "details": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "office_wear_collection": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "print_type": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "quadrant": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "launch_date": "min",
    "style_type": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "feeding_friendly": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Days_Since_Launch": "mean",
    "Total_Quantity": "sum",
    "Total_Value": "sum",
    "Total_Item_Viewed": "sum",
    "Total_Item_Atc": "sum",
    # Derived metrics - do not include aggregation functions
    # These will be calculated after grouping
}

group_by_dic_adb = {
    "Item_Id": "min",
    "Item_Name": "nunique",
    "Item_Type": "nunique",
    "Item_Code": "count",
    "Sale_Price": "mean",
    "Sale_Discount": lambda x: (x != 0).sum(),
    "Current_Stock": "sum",
    "Is_Public": "count",
    "Category": "nunique",
    "Age": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Bottom": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Colour": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Fabric": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Gender": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Neck_Closure": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Neck_Type": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Occassion": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Pack_Size": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Print_Collections": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Print_Pattern": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Print_Size": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Printed_Pattern": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Sleeve": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Top": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "Weave_Type": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "age_category": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "batch": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "bottom_fabric": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "launch_date": "min",
    "print_size": lambda x: ', '.join(sorted(set(str(item) for item in x if item is not None))),
    "product_category": "nunique",
    "product_type": "nunique",
    "Days_Since_Launch": "mean",
    "Total_Quantity": "sum",
    "Total_Value": "sum",
    "Total_Item_Viewed": "sum",
    "Total_Item_Atc": "sum",
   
}

# List of derived metrics that should not be in the groupby dictionaries
# These are calculated after grouping in the main function
DERIVED_METRICS = {
    "Per_Day_Value",
    "Per_Day_Quantity", 
    "Per_Day_View",
    "Per_Day_atc",
    "Conversion_Percentage"
}