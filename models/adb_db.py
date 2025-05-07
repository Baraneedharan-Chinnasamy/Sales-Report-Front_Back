from sqlalchemy import Column, Integer, String, Date, Boolean, TIMESTAMP, func, DECIMAL
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Item(Base):
    __tablename__ = "items"

    Item_Id = Column(Integer, primary_key=True, autoincrement=True)
    Item_Name = Column(String(255), nullable=True)
    Item_Type = Column(String(255), nullable=True)
    Item_Code = Column(String(255), nullable=True)
    Sale_Price = Column(String(255), nullable=True)
    Sale_Discount = Column(String(255), nullable=True)
    Uom = Column(String(255), nullable=True)
    Current_Stock = Column(String(255), nullable=True)
    Is_Public = Column(String(255), nullable=True)
    Age = Column(String(255), nullable=True)
    Bottom = Column(String(255), nullable=True)
    Category = Column(String(255), nullable=True)
    Colour = Column(String(255), nullable=True)
    Fabric = Column(String(255), nullable=True)
    Gender = Column(String(255), nullable=True)
    Neck_Closure = Column(String(255), nullable=True)
    Neck_Type = Column(String(255), nullable=True)
    Occassion = Column(String(255), nullable=True)
    Pack_Size = Column(String(255), nullable=True)
    Print_Collections = Column(String(255), nullable=True)
    Print_Pattern = Column(String(255), nullable=True)
    Print_Size = Column(String(255), nullable=True)
    Printed_Pattern = Column(String(255), nullable=True)
    Sleeve = Column(String(255), nullable=True)
    Top = Column(String(255), nullable=True)
    Weave_Type = Column(String(255), nullable=True)
    age_category = Column(String(255), nullable=True, name = "__Age_Category")
    batch = Column(String(255), nullable=True, name = "__Batch")
    bottom_fabric = Column(String(255), nullable=True, name = "__Bottom_Fabric")
    launch_date = Column(Date, nullable=True, name = "__Launch_Date")
    neck_closure = Column(String(255), nullable=True, name = "__Neck_Closure")
    print_size = Column(String(255), nullable=True, name = "__Print_Size")
    product_category = Column(String(255), nullable=True, name = "__Product_Category")
    product_type = Column(String(255), nullable=True, name = "__Product_Type")
    restock_status = Column(String(255), nullable=True, name = "__Restock_Status")
    Updated_At = Column(TIMESTAMP, nullable=True, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

class Sale(Base):
    __tablename__ = "sale"

    Date = Column(Date, primary_key=True, nullable=False)
    Item_Id = Column(Integer, primary_key=True, nullable=False)
    Item_Name = Column(String(255), nullable=True)
    Item_Code = Column(String(255), nullable=True)
    Quantity = Column(Integer, nullable=True)
    Total_Value = Column(DECIMAL(10, 2), nullable=True)
    Average_Quantity = Column(DECIMAL(10, 2), nullable=True)
    Updated_At = Column(TIMESTAMP, nullable=True, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

class ViewsAtc(Base):
    __tablename__ = "viewsatc"

    Date = Column(Date, primary_key=True, nullable=False)
    Item_Id = Column(String(255), primary_key=True, nullable=False)
    Ga4id = Column(String(255), nullable=True)
    Items_Viewed = Column(Integer, nullable=True)
    Items_Addedtocart = Column(Integer, nullable=True)
    Average_Views = Column(DECIMAL(10, 2), nullable=True)
    Average_Addtocarts = Column(DECIMAL(10, 2), nullable=True)
    Updated_At = Column(TIMESTAMP, nullable=True, server_default=func.current_timestamp(), onupdate=func.current_timestamp())