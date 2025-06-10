from datetime import datetime, timedelta
from fastapi import Depends, HTTPException, Request
from passlib.context import CryptContext
from jose import jwt, JWTError
from requests import Session

from database.database import get_db
from models.task import User

SECRET_KEY = "your-secret-key"  # Replace with env variable in production
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 days in minutes = 10080

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain: str, hashed: str):
    return pwd_context.verify(plain, hashed)

def create_access_token(data: dict, expires_delta=None):
    to_encode = data.copy()
    if expires_delta is None:
        expires_delta = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    expire = datetime.now() + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str):
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None


def get_current_user(request: Request,
    db: Session = Depends(get_db)):
    token = request.cookies.get("access_token")  # Make sure this matches the actual cookie name!
    if token is None:
        raise HTTPException(status_code=401, detail="Not authenticated (token missing)")

    payload = decode_token(token)
    if payload is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    employee_id = payload.get("employee_id")
    if employee_id is None:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    user = db.query(User).filter(User.employee_id == employee_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return user


def verify_access_token_cookie(request: Request):
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return token