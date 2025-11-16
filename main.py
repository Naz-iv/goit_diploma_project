import json, os
import uuid

from fastapi import FastAPI, Request, BackgroundTasks, File, UploadFile, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from utils.utility import request_frames
from ml_recommendations.recommendation_module import get_recommender

app = FastAPI()

templates = Jinja2Templates(directory="templates")

# 🔥 Mount static directory
app.mount("/static", StaticFiles(directory="static"), name="static")

BACKEND_URL = "http://localhost:8000/request_frames"

TASKS = {}

def process_frame(task_id, data):

    xml_string, status = request_frames(data)
    global TASKS

    TASKS[task_id]["status"] = status
    TASKS[task_id]["result"] = xml_string

def recommend_parameters(task_id, data):

    recommender = get_recommender()

    rop, bitrate, predicted_duration, status = recommender.recommend(data)
    global TASKS

    formatted = {
        "recommendation": [
            rop,
            bitrate
        ],
        "expected_time": predicted_duration
    }

    TASKS[task_id] = {
        "status": status,
        "result": formatted
    }


@app.get("/", response_class=HTMLResponse)
async def form_get(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "response": None})


@app.get("/new-request", response_class=HTMLResponse)
async def form_get(request: Request):
    return templates.TemplateResponse("form.html", {"request": request, "response": None})


@app.post("/submit")
async def submit_frame(
    background_tasks: BackgroundTasks,
    rop: float = Form(None),
    bitrate: float = Form(None),
    num_fsls: int = Form(None),
    job_number: str = Form(None),
    json_file: UploadFile = File(None)
):
    data = {"ROP": rop, "Bitrate": bitrate, "num_fsls": num_fsls, "job_number": job_number}

    if json_file:
        contents = await json_file.read()
        data = json.loads(contents)

    #Saving user input to data file
    for key, value in data.items():
        if "fsl" in key and isinstance(data[key], dict):
            value["ROP"] = rop
            value["bitrate"] = bitrate

    # Create background task
    task_id = str(uuid.uuid4())
    TASKS[task_id] = {"status": "processing", "result": None}

    background_tasks.add_task(process_frame, task_id, data)

    # Redirect user to task status page
    return RedirectResponse(url=f"/task/{task_id}", status_code=303)


@app.get("/task/{task_id}")
async def task_status(task_id: str, request: Request):
    return templates.TemplateResponse(
        "processing.html",
        {"request": request, "task_id": task_id}
    )

@app.get("/status/{task_id}")
def get_status(task_id: str):
    return TASKS.get(task_id, {"status": "not_found"})

@app.get("/result/{task_id}")
async def result_page(task_id: str, request: Request):
    task = TASKS.get(task_id)

    return templates.TemplateResponse(
        "result_new.html",
        {
            "request": request,
            "fsl_body": task.get("result", "<h1>No result available</h1>")
        }
    )


@app.post("/recommend")
async def get_recommended_parameters(
    background_tasks: BackgroundTasks,
    rop: float = Form(None),
    bitrate: float = Form(None),
    num_fsls: int = Form(None),
    job_number: str = Form(None),
    json_file: UploadFile = File(None)
):
    data = {"ROP": rop, "Bitrate": bitrate, "Num_FSLs": num_fsls, "job_number": job_number}

    if json_file:
        contents = await json_file.read()
        json_data = json.loads(contents)

    # Example: count tools
    for key, value in json_data.items():
        if "fsl" in key and isinstance(json_data[key], dict):
            data["ToolCount"] = len(json.loads(value["TOOLS"]))
            break

    # Create background task
    task_id = str(uuid.uuid4())
    TASKS[task_id] = {"status": "processing", "result": None}

    background_tasks.add_task(recommend_parameters, task_id, data)

    # Return task ID (no redirect)
    return JSONResponse({"task_id": task_id})


@app.get("/analytics", response_class=HTMLResponse)
async def analytics_get(request: Request):
    analytics_metrics = r"C:\Users\nivankiv\git\goit_diploma_project\static\analytics\metrics.json"

    # Load your metrics JSON
    with open(analytics_metrics, "r") as f:
        metrics = json.load(f)

    # Folder for all chart images
    chart_root = "/static/analytics/"

    return templates.TemplateResponse(
        "analytics.html",
        {
            "request": request,
            "metrics": metrics,
            "chart_root": chart_root
        }
    )