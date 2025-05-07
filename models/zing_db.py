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
    Sale_Price = Column(DECIMAL(10,2), nullable=True)
    Sale_Discount = Column(DECIMAL(10,2), nullable=True)
    Uom = Column(String(255), nullable=True)
    Current_Stock = Column(Integer, nullable=True)
    Is_Public = Column(String(255), nullable=True)
    Category = Column(String(255), nullable=True)
    Colour = Column(String(255), nullable=True)
    Fabric = Column(String(255), nullable=True)
    Fit = Column(String(255), nullable=True)
    Neck = Column(String(255), nullable=True)
    Occasion = Column(String(255), nullable=True)
    Offer = Column(String(255), nullable=True)
    Print = Column(String(255), nullable=True)
    Size = Column(String(255), nullable=True)
    Sleeve = Column(String(255), nullable=True)

    # Handling column names with __ and spaces
    batch = Column(String(255), nullable=True, name="__Batch")
    collection_you_will_love = Column(String(255), nullable=True, name="__Collection_You_Will_Love")
    details = Column(String(255), nullable=True, name="__Details")
    eorder = Column(String(255), nullable=True, name="__Eorder__")
    launch_date = Column(Date, nullable=True, name="__Launch_Date")
    mood = Column(String(255), nullable=True, name="__Mood")
    new_item_type = Column(String(255), nullable=True, name="__New_Item_Type")
    new_launch = Column(String(255), nullable=True, name="__New_Launch")
    offer_date = Column(Date, nullable=True, name="__Offer_Date")
    office_wear_collection = Column(String(255), nullable=True, name="__Office_Wear_Collection")
    price = Column(String(255), nullable=True, name="__Price")
    print_type = Column(String(255), nullable=True, name="__Print_Type")
    quadrant = Column(String(255), nullable=True, name="__Quadrant")
    restock_date = Column(Date, nullable=True, name="__Restock_Date")
    style_type = Column(String(255), nullable=True, name="__Style_Type")

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
    Item_Id = Column(Integer, ForeignKey("items.Item_Id"), primary_key=True)
    Ga4id = Column(String(255), nullable=True)
    Items_Viewed = Column(Integer, nullable=True)
    Items_Addedtocart = Column(Integer, nullable=True)
    Average_Views = Column(DECIMAL(10,2), nullable=True)
    Average_Addtocarts = Column(DECIMAL(10,2), nullable=True)
    Updated_At = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

    # Relationship with Items Table
    item_viewsatc = relationship("Item", back_populates="viewsatc")
