from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import logging
from datetime import datetime

from app.modules.reports.scheduler_report import verify_scheduled_plan
from app.modules.reports.optimizer_report import compare_optimiser_plan, generate_driver_assign_report, get_plan_recommendations_and_actual_plan
from app.modules.upload_model.upload_file import get_file_from_s3

from vrp_optimizer.optimizer import Optimizer

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


@router.post("/get_driver_plan_input_from_s3")
async def get_optimizer_input(request: Request):
    """
    Get the optimizer input for the plan
    """
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')

        body = await request.json()
        plan_id = body.get('plan_id')

        if not plan_id:
            return JSONResponse(
                content={"message": "Plan ID is required"},
                status_code=400
            )

        key = f"{carrier}_{plan_id}.json"
        if not (key.endswith(".json") or key.endswith(".pkl")):
            key += ".json"
        if "/" not in key:
            key = f"optimizer-plans/{key}"

        content = await get_file_from_s3(key)

        return JSONResponse(
            content={"result": content, "status": "success"},
            status_code=200
        )

    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"},
            status_code=500
        )


@router.post("/run_optimizer_from_input")
async def get_driver_plan_from_s3(request: Request):
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')
        
        content = await request.json()

        try:
            optimizer = Optimizer(
                moves=content['moves'] if content.get('moves') else [],
                drivers=content['drivers'] if content.get('drivers') else [], 
                depot_locations=content['depot_locations'] if content.get('depot_locations') else [],
                yard_locations=content['yard_locations'] if content.get('yard_locations') else [],
                timezone=content['timezone'] if content.get('timezone') else '',
                distance_unit=content['distance_unit'] if content.get('distance_unit') else 'mi',
                time_limit=content['time_limit'] if content.get('time_limit') else 5 * 60,
                equipment_validations=content['equipment_validations'] if content.get('equipment_validations') else [],
                location_distance_matrix=content['location_distance_matrix'] if content.get('location_distance_matrix') else [],
                plan_start_minute=content['plan_start_minute'] if content.get('plan_start_minute') else 0,
                plan_end_minute=content['plan_end_minute'] if content.get('plan_end_minute') else 1440,
                plan_date=datetime.fromisoformat(content['plan_date']) if content.get('plan_date') else None,
                carrier_id=carrier,
                max_time_dimension_minutes=content['max_time_dimension_minutes'] if content.get('max_time_dimension_minutes') else 1440,
                chassis_limits=content['chassis_limits'] if content.get('chassis_limits') else {},
                driver_history=content['driver_history'] if content.get('driver_history') else {},
                is_in_day_plan=content['is_in_day_plan'] if content.get('is_in_day_plan') else False,
                dispatch_plan_params=content['dispatch_plan_params'] if content.get('dispatch_plan_params') else {}
            )
            _optimal_plan, _d_schedule = optimizer.optimize()

            return JSONResponse(
                content={"result": _optimal_plan, "status": "success"},
                status_code=200
            )
        except Exception as e:
            logger.error(e)
            return JSONResponse(
                content={"message": str(e), "status": "error"},
                status_code=500
            )

    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"},
            status_code=500
        )

# route to get plan recommendations and actually what happened
@router.post("/get_plan_recommendations_and_actual_plan_sheets")
async def get_plan_recommendations_and_actual_plan_sheets(request: Request):
    """
    Get the plan recommendations and actual plan
    """
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')
        
        result = await get_plan_recommendations_and_actual_plan(carrier)

        return JSONResponse(
            content={"result": result, "status": "success"},
            status_code=200
        )
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"},
            status_code=500
        )

@router.post("/generate_driver_assign_report")
async def generate_driver_assign_report_endpoint(request: Request):
    """
    Generate driver assignment report for the given date
    """
    try:
        user_payload = await request.json()
        carrier = user_payload.get('carrier')
        
        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )

        plan_date = user_payload.get('plan_date')
        
        if not plan_date:
            return JSONResponse(
                content={"message": "Plan date is required"},
                status_code=400
            )

        result = await generate_driver_assign_report(user_payload, plan_date)

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"},
            status_code=500
        )
