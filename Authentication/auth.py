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
    # Filter the permissions dictionary to include only 'admin' and 'reportrix'
    filtered_permissions = {key: current_user.permissions[key] for key in ["admin", "reportrix"] if key in current_user.permissions}

    return {
        "employee_id": current_user.employee_id,
        "username": current_user.username,
        "email": current_user.email,
        "permissions": filtered_permissions
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
