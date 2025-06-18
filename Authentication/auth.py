# router/columns.py
from datetime import timedelta
import random
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session
from fastapi.responses import JSONResponse
from database.database import get_db
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from models.task import User,DropdownOption
from Authentication.functions import decode_token, send_email, verify_password,create_access_token,get_current_user
router = APIRouter()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")

@router.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):

    user = db.query(User).filter(User.email == form_data.username).first()
    if not user or not verify_password(form_data.password, user.password_hash):
       
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token({"sub": user.username, "employee_id": user.employee_id})
    

    response = JSONResponse(content={"message": "Login successful"})
    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        max_age=60 * 60 * 24 * 7,
        expires=60 * 60 * 24 * 7,
        secure=False
    )
    return response

@router.get("/me")
def get_me(token: str = Depends(oauth2_scheme)):
    return {"message": "You're authenticated", "token": token}


@router.post("/logout")
def logout(response: Response):
    response.delete_cookie(
        key="access_token",
        path="/",         
        httponly=True,    
        samesite="lax"    
    )
    return {"message": "Logged out"}


@router.get("/users")
def read_current_user(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    # ✅ Fetch all users
    all_users = db.query(User).all()
    people = [
        {
            "employee_id": user.employee_id,
            "username": user.username
        }
        for user in all_users
    ]

    # ✅ Fetch all active dropdown options
    active_options = db.query(DropdownOption.type, DropdownOption.value).filter(
        DropdownOption.is_active == True
    ).all()

    dropdown_map_full = {}
    for type_, value in active_options:
        type_lower = type_.lower()
        dropdown_map_full.setdefault(type_lower, []).append(value)

    # ✅ Filter dropdowns based on current user's permissions
    filtered_dropdown_map = {}

    user_permissions = current_user.permissions or {}
    allowed_brands = user_permissions.get("brands", {})

    # ✅ Filter brands
    filtered_dropdown_map["brand_name"] = [
        brand for brand in dropdown_map_full.get("brand_name", [])
        if brand in allowed_brands
    ] if allowed_brands else []

    # ✅ Filter roles and format_types
    allowed_roles = set()
    allowed_formats = set()

    for brand_perms in allowed_brands.values():
        for format_type, roles in brand_perms.items():
            allowed_formats.add(format_type)
            allowed_roles.update(roles)

    # ✅ Filter roles
    filtered_dropdown_map["role"] = [
        role for role in dropdown_map_full.get("role", [])
        if role in allowed_roles
    ]

    # ✅ Filter format_type
    filtered_dropdown_map["format_type"] = [
        fmt for fmt in dropdown_map_full.get("format_type", [])
        if fmt in allowed_formats
    ]

    # ✅ Copy other dropdowns without filtering
    for key in dropdown_map_full:
        if key not in {"brand_name", "role", "format_type"}:
            filtered_dropdown_map[key] = dropdown_map_full[key]

    

    return {
        "employee_id": current_user.employee_id,
        "username": current_user.username,
        "email": current_user.email,
        "designation": current_user.designation,
        "permissions": current_user.permissions,
        "people": people,
        "dropdowns": filtered_dropdown_map
    }

class ForgotPasswordRequest(BaseModel):
    email: EmailStr


@router.post("/forgot-password")
def forgot_password(request: ForgotPasswordRequest, db: Session = Depends(get_db)):

    user = db.query(User).filter(User.email == request.email).first()
    if not user:
        raise HTTPException(status_code=404, detail="Email not found")

    otp = str(random.randint(100000, 999999))
    payload = {"sub": user.email, "otp": otp}
    token = create_access_token(payload, expires_delta=timedelta(minutes=10))

    email_body = (
        f"Hi {user.username},\n\n"
        f"Your OTP for password reset is: {otp}\n\n"
        f"This OTP is valid for 10 minutes."
    )

    subject = "Your OTP for Password Reset"
    success = send_email(user.email, subject, email_body)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to send OTP email. Please try again later.")

    return {"message": "OTP has been sent to your email", "token": token}

class ResetPasswordRequest(BaseModel):
    email: str
    otp: str
    new_password: str
    token: str
from passlib.context import CryptContext
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
def hash_password(password: str):
    return pwd_context.hash(password)

@router.post("/reset-password")
def reset_password(data: ResetPasswordRequest, db: Session = Depends(get_db)):

    payload = decode_token(data.token)
    if not payload:
        raise HTTPException(status_code=400, detail="Invalid or expired token")

    email = payload.get("sub")
    token_otp = payload.get("otp")

    if email != data.email or token_otp != data.otp:
        raise HTTPException(status_code=400, detail="Invalid email or OTP")

    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.password_hash = hash_password(data.new_password)
    db.commit()
    return {"message": "Password reset successful"}
