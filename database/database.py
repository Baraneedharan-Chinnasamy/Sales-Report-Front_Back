import os
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi import HTTPException


# Load environment variables
load_dotenv()

DB_USER = urllib.parse.quote_plus(os.getenv("DB_USER"))
DB_PASSWORD = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
DB_HOST = os.getenv("DB_HOST")

DB_PORT = os.getenv("DB_PORT", "3306") 

DATABASES = {
    "ZNG45F8J27LKMNQ": f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/zing",
    "PRT9X2C6YBMLV0F": f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/prathiksham",
    "BEE7W5ND34XQZRM": f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/beelittle",
    "ADBXOUERJVK038L": f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/adoreaboo",
}
# Maintain separate session makers for each DB
engines = {
    name: create_engine(
        url,
        pool_size=20,             
        max_overflow=5,           
        pool_timeout=30,          
        pool_recycle=3600,        
        echo=False                
    )
    for name, url in DATABASES.items()
}
session_makers = {name: sessionmaker(bind=eng, autocommit=False, autoflush=False) for name, eng in engines.items()}


# Function to get the database session dynamically
def get_db(business: str):
    if business not in session_makers:
        raise HTTPException(status_code=400, detail="Invalid business name")
    
    db = session_makers[business]()
    try:
        yield db
    finally:
        db.close()

# Mapping of business codes to actual business names
BUSINESS_CODE_MAP = {
    "ZNG45F8J27LKMNQ": "zing",
    "PRT9X2C6YBMLV0F": "prathiksham",
    "BEE7W5ND34XQZRM": "beelittle",
    "ADBXOUERJVK038L" : "adoreaboo"
}

def get_business_name(business_code: str) -> str:
    """Convert business code to business name."""
    return BUSINESS_CODE_MAP.get(business_code, None)
