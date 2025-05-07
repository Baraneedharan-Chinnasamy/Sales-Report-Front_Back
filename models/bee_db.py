from sqlalchemy import Column, Integer, String, Date, TIMESTAMP, ForeignKey, DECIMAL, func
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

# Items Table
class Item(Base):
    __tablename__ = "items"

    Item_Id = Column(Integer, primary_key=True, index=True)
    Item_Name = Column(String(255), nullable=True)
    Item_Type = Column(String(255), nullable=True)
    Item_Code = Column(String(255), nullable=True)
    Sale_Price = Column(String(255), nullable=True)
    Sale_Discount = Column(String(255), nullable=True)
    Uom = Column(String(255), nullable=True)
    Current_Stock = Column(Integer, nullable=True)
    Is_Public = Column(String(255), nullable=True)
    Age = Column(String(255), nullable=True)
    Amount = Column(String(255), nullable=True)
    Bottom = Column(String(255), nullable=True)
    Bundles = Column(String(255), nullable=True)
    Colour = Column(String(255), nullable=True)
    Discount = Column(String(255), nullable=True)
    Fabric = Column(String(255), nullable=True)
    Filling = Column(String(255), nullable=True)
    Gender = Column(String(255), nullable=True)
    Pack_Size = Column(String(255), nullable=True)
    Pattern = Column(String(255), nullable=True)
    Product_Type = Column(String(255), nullable=True)
    Quantity = Column(String(255), nullable=True)
    Sale = Column(String(255), nullable=True)
    Size = Column(String(255), nullable=True)
    Sleeve = Column(String(255), nullable=True)
    Style = Column(String(255), nullable=True)
    Top = Column(String(255), nullable=True)
    Weave_Type = Column(String(255), nullable=True)
    Weight = Column(String(255), nullable=True)
    Width = Column(String(255), nullable=True)
    

    # Handling column names with __ and spaces
    batch = Column(String(255), nullable=True, name="__Batch")
    bottom_fabric = Column(String(255), nullable=True, name="__Bottom_Fabric")
    brand_name = Column(String(255), nullable=True, name="__Brand_Name")
    discounts = Column(String(255), nullable=True, name="__Discount")
    inventory_type = Column(String(255), nullable=True, name="__Inventory_Type")
   
    launch_date = Column(Date, nullable=True, name="__Launch_Date")
    offer_date = Column(String(255), nullable=True, name="__Offer_Date")
    quadrant = Column(String(255), nullable=True, name="__Quadrant")
    relist_date = Column(String(255), nullable=True, name="__Relist_Date")
    restock_status = Column(String(255), nullable=True, name="__Restock_Status")
    season = Column(String(255), nullable=True, name="__Season")
    season_style = Column(String(255), nullable=True, name="__Season_Style")
    supplier_name = Column(String(255), nullable=True, name="__Supplier_Name")
    Updated_At = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

    print_colour = Column(String(255), nullable=True, name="Print Colour")
    print_size = Column(String(255), nullable=True, name="Print Size")
    print_theme = Column(String(255), nullable=True, name="Print Theme")
    print_key_motif = Column(String(255), nullable=True, name="Print Key Motif")
    print_style = Column(String(255), nullable=True, name="Print Style")

    
    # Relationship with Sale and ViewsAtc
    sales = relationship("Sale", back_populates="item_sales")
    viewsatc = relationship("ViewsAtc", back_populates="item_viewsatc")


# Sale Table
class Sale(Base):
    __tablename__ = "sale"

    Date = Column(Date, primary_key=True)
    Item_Name = Column(String(255), nullable=True)
    Item_Code = Column(String(255), nullable=True)  
    Item_Id = Column(Integer, ForeignKey("items.Item_Id"), primary_key=True)
    Quantity = Column(Integer, nullable=True)
    Total_Value = Column(DECIMAL(10,2), nullable=True)
    Average_Quantity = Column(DECIMAL(10,2), nullable=True)
    Updated_At = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

    # Relationship with Items Table
    item_sales = relationship("Item", back_populates="sales")


# ViewsAtc Table
class ViewsAtc(Base):
    __tablename__ = "viewsatc"

    Date = Column(Date, primary_key=True)
    Ga4id = Column(String(255), nullable=True)
    Items_Viewed = Column(Integer, nullable=True)
    Items_Addedtocart = Column(Integer, nullable=True)
    Item_Id = Column(String(255), ForeignKey("items.Item_Id"), primary_key=True)
    
    
    Average_Views = Column(DECIMAL(10,2), nullable=True)
    Average_Addtocarts = Column(DECIMAL(10,2), nullable=True)
    Updated_At = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    year = Column(Integer, nullable=True)
    month = Column(Integer, nullable=True)

    # Relationship with Items Table
    item_viewsatc = relationship("Item", back_populates="viewsatc")
