from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Any
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json, re, traceback
from Authentication.functions import get_current_user, verify_access_token_cookie
from models.task import User

router = APIRouter()

SERVICE_ACCOUNT_FILE = 'credentials/google-sheets-service.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

# Replace with your actual links
BRAND_SHEET_MAP = {
    "PRT9X2C6YBMLV0F": "https://docs.google.com/spreadsheets/d/1q5CAMOxVZnFAowxq9w0bbuX9bEPtwJOa9ERA3wCOReQ/edit?usp=sharing",
    "BEE7W5ND34XQZRM": "https://docs.google.com/spreadsheets/d/1fyzL0TPVWSvQ71-N14AIav9e0qCAqGRu47dhUjA2R44/edit?usp=sharing",
    "ADBXOUERJVK038L": "https://docs.google.com/spreadsheets/d/1AmFyKI_XMIrSsxyVk11fEgwa8RJMcBwYSKWuQvHh-eU/edit?usp=sharing",
    "ZNG45F8J27LKMNQ": "https://docs.google.com/spreadsheets/d/15Y79kB1STCwCTNJT6dcK-weqazbqQeptXzXcDgJykT8/edit?usp=sharing"
}


class ExportRequest(BaseModel):
    brand: str
    sheet: str
    data: List[Any]

@router.post("/export-to-sheet")
async def export_to_sheet(payload: ExportRequest,token=Depends(verify_access_token_cookie)):
    try:
        url = BRAND_SHEET_MAP.get(payload.brand.upper())
        if not url:
            return JSONResponse(status_code=400, content={"status": "error", "message": "No spreadsheet for brand"})

        match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
        if not match:
            return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid URL"})

        spreadsheet_id = match.group(1)
        sheets_api = build('sheets', 'v4', credentials=credentials).spreadsheets()
        meta = sheets_api.get(spreadsheetId=spreadsheet_id).execute()
        titles = [s['properties']['title'] for s in meta['sheets']]
        titles_lower = [t.lower() for t in titles]

        date_str = datetime.now().strftime('%Y%m%d')
        brand_abbr = payload.brand.upper()[:3]
        sheet_base = f"{payload.sheet.strip()}-{brand_abbr}-{date_str}"
        main_sheet = sheet_base
        target_sheet = f"{sheet_base}_TARGET"

        i = 1
        while main_sheet.lower() in titles_lower or target_sheet.lower() in titles_lower:
            main_sheet = f"{sheet_base}_{i}"
            target_sheet = f"{sheet_base}_TARGET_{i}"
            i += 1

        main_data, target_data = [], []
        for row in payload.data:
            if isinstance(row, dict):
                t_wise = row.pop("target_wise", None)
                main_data.append(row)
                if t_wise:
                    for t in t_wise:
                        row_data = {"Date": row.get("Date")}
                        row_data.update(t)
                        target_data.append(row_data)

        for sheet_name, data in [(main_sheet, main_data), (target_sheet, target_data)]:
            if not data: continue
            headers = list(data[0].keys())
            values = [headers]
            for r in data:
                values.append([
                    json.dumps(r.get(h, "")) if isinstance(r.get(h), (dict, list)) else r.get(h, "") for h in headers
                ])
            sheets_api.batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
            ).execute()
            sheets_api.values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                body={"values": values}
            ).execute()

        return {"status": "success", "message": f"Data written to '{main_sheet}' and '{target_sheet}'" if target_data else f"Data written to '{main_sheet}'"}

    except Exception as e:
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"status": "error", "message": "Failed to export", "details": str(e)})
