import logging
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import Dict
from fastapi import FastAPI, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import requests
import json

from utils.db_managment import SQLHandler
from signature.digital_signature import DigitalSignature
from utils.utility import main
from hspm_version_mapper import HSPM_VERSION_MAP

app = FastAPI()

templates = Jinja2Templates(directory="templates")

BACKEND_URL = "http://localhost:8000/request_frames"

signer = DigitalSignature()

@app.post("/request_frames")
async def request_frames(data: Dict):
    uid = data.get("uid")
    if not uid:
        raise HTTPException(status_code=400, detail="UID is missing in the request.")

    hspm_version_from_user = HSPM_VERSION_MAP.get(data.get("hspm_version"))
    hspm_version = hspm_version_from_user if hspm_version_from_user else "default_db"

    try:
        with SQLHandler(hspm_version) as db:
            past_result = db.load_xml_from_sqlite(uid)

            if past_result is not None:
                past_result, status = past_result
                if past_result == "":
                    return JSONResponse(
                        status_code=503,
                        content={"statusCode": 503, "xml": "",
                                 "message": "Frame Generator was not able to build frame. Increase bit rate or reduce required update rates."}
                    )
                elif str(past_result).startswith("FAILED | "):
                    error_message = past_result.split(" | ")[1]
                    clear_db_on_failure(hspm_version, uid)
                    return JSONResponse(
                        status_code=406,
                        content={"statusCode": 406, "xml": "", "message": error_message}
                    )
                else:
                    return JSONResponse(
                        status_code=int(status),
                        content={"statusCode": int(status),
                                 "xml": signer.sign_doc(past_result),
                                 "message": "Frames generated successfully!"}
                    )

            db.insert_uid_with_none(uid)

        xml_string, status = main(data)

        if xml_string:
            with SQLHandler(hspm_version) as db:
                db.update_xml_in_sqlite(uid, xml_string, status)

            return JSONResponse(
                status_code=int(status),
                content={"statusCode": int(status), "xml": signer.sign_doc(xml_string),
                         "message": "Frame generated successfully!"}
            )
        else:
            error_message = "Unable to build frames for your request. Please check compatibility!"
            with SQLHandler(hspm_version) as db:
                db.update_xml_in_sqlite(uid, f"FAILED | {error_message}", 406)
            return JSONResponse(
                status_code=406,
                content={"statusCode": 406, "xml": "", "message": error_message}
            )

    except Exception as e:
        logging.error(f"Error: {e}")
        error_message = str(e)
        if "the JSON object must be str, bytes or bytearray, not NoneType" in error_message:
            error_message = "RTOF.json file didn't load correctly. Please check your input."
        with SQLHandler(hspm_version) as db:
            db.update_xml_in_sqlite(uid, f"FAILED | {error_message}", 406)
        return JSONResponse(
            status_code=406,
            content={"statusCode": 406, "xml": "", "message": error_message}
        )

def clear_db_on_failure(db_name, uid):
    with SQLHandler(db_name) as db:
        db.remove_uid_from_db(uid)


@app.get("/", response_class=HTMLResponse)
async def form_get(request: Request):
    return templates.TemplateResponse("form.html", {"request": request, "response": None})


@app.post("/", response_class=HTMLResponse)
async def form_post(
    request: Request,
    ROP: float = Form(None),
    Bitrate: float = Form(None),
    ToolCount: int = Form(None),
    UID: str = Form(None),
    json_file: UploadFile = File(None)
):
    try:
        if json_file:
            contents = await json_file.read()
            data = json.loads(contents)

            # Override UID with JOB NUMBER from JSON
            if "JOB NUMBER" in data:
                UID = data["JOB NUMBER"]
            data["uid"] = UID

            # Override ToolCount with length of TOOLS array
            if "TOOLS" in data and isinstance(data["TOOLS"], list):
                ToolCount = len(data["TOOLS"])
            data["ToolCount"] = ToolCount

            # Set ROP and Bitrate from form if provided, else keep from JSON
            if ROP is not None:
                data["ROP"] = ROP
            if Bitrate is not None:
                data["Bitrate"] = Bitrate

            # Optionally, you can include hspm_version
            if "hspm_version" not in data:
                data["hspm_version"] = "v1"

        else:
            # If no JSON uploaded, use form values
            data = {
                "ROP": ROP,
                "Bitrate": Bitrate,
                "ToolCount": ToolCount,
                "uid": UID,
                "hspm_version": "v1"
            }

        # Send all data to backend
        r = requests.post(BACKEND_URL, json=data)
        backend_response = r.json()

    except Exception as e:
        backend_response = {"error": str(e)}

    return templates.TemplateResponse(
        "form.html",
        {"request": request, "response": backend_response}
    )
