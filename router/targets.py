from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from typing import List
from datetime import datetime
import os, json, pandas as pd

from Authentication.functions import get_current_user, verify_access_token_cookie
from models.task import User

router = APIRouter()

TARGET_FILE_PATH = os.path.join("target_data", "targets.json")

class TargetEntry(BaseModel):
    Business_Name: str 
    Start_Date: str 
    Target_Column: str
    Target_Key: str 
    Target_Value: int

@router.post("/target/set-daily-targets")
def set_targets(data: List[TargetEntry],token=Depends(verify_access_token_cookie)):
    existing = []
    if os.path.exists(TARGET_FILE_PATH):
        with open(TARGET_FILE_PATH) as f:
            existing = json.load(f)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for entry in data:
        existing.append({
            **entry.dict(),
            "Uploaded_At": now,
            "Status_History": [{"status": True, "timestamp": entry.Start_Date}]
        })

    os.makedirs("target_data", exist_ok=True)
    with open(TARGET_FILE_PATH, "w") as f:
        json.dump(existing, f, indent=4)

    return {"message": "Targets added", "count": len(data)}

@router.get("/target/list-targets-with-status")
def list_targets_with_status(business_name: str,token=Depends(verify_access_token_cookie)):
    if not os.path.exists(TARGET_FILE_PATH):
        return []

    with open(TARGET_FILE_PATH) as f:
        targets = json.load(f)

    now = pd.to_datetime(datetime.now())
    bname = business_name.strip().lower()

    result = []
    for t in targets:
        if t.get("Business_Name", "").lower() != bname:
            continue
        history = t.get("Status_History", [])
        last_status = True
        for h in sorted(history, key=lambda x: x["timestamp"]):
            if pd.to_datetime(h["timestamp"]) <= now:
                last_status = h["status"]
        t["Status"] = last_status
        result.append(t)
    return result

class UpdateTargetRequest(BaseModel):
    Business_Name: str
    Target_Column: str
    Target_Key: str
    Start_Date: str
    status: bool = None  # Optional
    Target_Value: int = None  

@router.post("/target/update-target-entry")
def update_target_entry(data: UpdateTargetRequest):
    if not os.path.exists(TARGET_FILE_PATH):
        return {"error": "Target data not found."}

    with open(TARGET_FILE_PATH, "r") as f:
        targets = json.load(f)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated = False

    for target in targets:
        if (
            target["Business_Name"].strip().lower() == data.Business_Name.strip().lower()
            and target["Target_Column"].strip().lower() == data.Target_Column.strip().lower()
            and target["Target_Key"].strip().lower() == data.Target_Key.strip().lower()
            and target["Start_Date"] == data.Start_Date
        ):
            # Update Target Value if given
            if data.Target_Value is not None:
                target["Target_Value"] = data.Target_Value

            # Add new status to history if provided
            if data.status is not None:
                if "Status_History" not in target:
                    target["Status_History"] = []
                target["Status_History"].append({
                    "status": data.status,
                    "timestamp": now
                })

            updated = True
            break

    if not updated:
        return {"error": "Target not found."}

    with open(TARGET_FILE_PATH, "w") as f:
        json.dump(targets, f, indent=4)

    return {"message": "Target updated successfully."}
