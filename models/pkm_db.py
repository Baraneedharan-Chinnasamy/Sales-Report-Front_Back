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
    Category = Column(String(255), nullable=True)
    Colour = Column(String(255), nullable=True)
    Fabric = Column(String(255), nullable=True)
    Fit = Column(String(255), nullable=True)
    Lining = Column(String(255), nullable=True)
    Neck = Column(String(255), nullable=True)
    Occasion = Column(String(255), nullable=True)
    Print = Column(String(255), nullable=True)
    Product_Availability = Column(String(255), nullable=True)
    Size = Column(String(255), nullable=True)
    Sleeve = Column(String(255), nullable=True)
    Pack = Column(String(255), nullable=True)

    # Handling column names with __ and spaces
    batch = Column(String(255), nullable=True, name="__Batch")
    bottom_length = Column(String(255), nullable=True, name="__Bottom_Length")
    bottom_print = Column(String(255), nullable=True, name="__Bottom_Print")
    bottom_type = Column(String(255), nullable=True, name="__Bottom_Type")
    collections = Column(String(255), nullable=True, name="__Collections")
    details = Column(String(255), nullable=True, name="__Details")
    dispatch_time = Column(String(255), nullable=True, name="__Dispatch_Time")
    launch_date = Column(Date, nullable=True, name="__Launch_Date")
    new_arrivals = Column(String(255), nullable=True, name="__New_Arrivals")
    pack_details = Column(String(255), nullable=True, name="__Pack_Details")
    pocket = Column(String(255), nullable=True, name="__Pocket")
    top_length = Column(String(255), nullable=True, name="__Top_Length")
    waistband = Column(String(255), nullable=True, name="__Waistband")

    Updated_At = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

    # Relationship with Sale and ViewsAtc
    sales = relationship("Sale", back_populates="item_sales")
    viewsatc = relationship("ViewsAtc", back_populates="item_viewsatc")


# Sale Table
class Sale(Base):
    __tablename__ = "sale"

    Date = Column(Date, primary_key=True)
    Item_Id = Column(Integer, ForeignKey("items.Item_Id"), primary_key=True)
    Item_Name = Column(String(255), nullable=True)
    Item_Code = Column(String(255), nullable=True)
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
    Item_Id = Column(String(255), ForeignKey("items.Item_Id"), primary_key=True)
    Ga4id = Column(String(255), nullable=True)
    Items_Viewed = Column(Integer, nullable=True)
    Items_Addedtocart = Column(Integer, nullable=True)
    Average_Views = Column(DECIMAL(10,2), nullable=True)
    Average_Addtocarts = Column(DECIMAL(10,2), nullable=True)
    Updated_At = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

    # Relationship with Items Table
    item_viewsatc = relationship("Item", back_populates="viewsatc")
