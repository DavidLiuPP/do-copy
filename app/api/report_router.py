from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import logging

from app.modules.reports.scheduler_report import verify_scheduled_plan
from app.modules.reports.optimizer_report import get_driver_plan_stats_controller, compare_optimiser_plan

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/verify_scheduled_plan")
async def verify_schedule_data(request: Request, plan_date: str):
    """
    Compare the scheduled plan with the original events and return the delta
    
    Parameters:
        plan_date (str): The date to verify data for
    """
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')
        
        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )

        result = await verify_scheduled_plan(carrier, plan_date)

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"},
            status_code=500
        )

@router.get("/get_driver_plan_stats")
async def get_driver_plan_stats(request: Request, plan_id: str):
    """
    Get the stats for the driver plan
    """
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')
        
        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )
        
        if not plan_id:
            return JSONResponse(
                content={"message": "Plan ID is required"},
                status_code=400
            )

        user_payload = { "carrier": carrier }
        result = await get_driver_plan_stats_controller(user_payload, plan_id)

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"},
            status_code=500
        )

@router.post("/compare_optimiser_plan")
async def compare_plan(request: Request):
    """
    Compare the optimiser plan with the original events and return the delta
    Parameters:
        request (Request): The incoming request containing the file
    """
    try:
        form = await request.form()
        plan_branch = [v for k, v in form.multi_items() if k == "plan_branch"]
        user_payload = {
            "carrier": form.get("carrier"),
            "userId": form.get("carrier"),
            "plan_branch": plan_branch,
            "shift": form.get("shift")
        }
        plan_date = form.get('plan_date')

        if not form.get("carrier"):
            return JSONResponse(
                content={"message": "Carrier ID is required"},
                status_code=400
            )

        if not plan_date:
            return JSONResponse(
                content={"message": "Plan date is required"},
                status_code=400
            )

        result = await compare_optimiser_plan(user_payload, plan_date)

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"}, status_code=500
        )
