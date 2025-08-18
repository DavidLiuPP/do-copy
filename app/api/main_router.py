import logging
import pytz
from datetime import datetime
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from app.modules.scheduler.move_scheduler import predict_next_move
from app.modules.optimizer.driver_plan import get_optmized_driver_plan, get_optimizer_review_recommendation, get_in_day_actions
from app.modules.optimizer.driver_plan_edit import add_move_to_optimizer_plan, remove_move_from_optimizer_plan, reassign_move_to_optimizer_plan
from app.modules.optimizer.eta_service import store_driver_eta_details, get_eta_details
from fastapi import BackgroundTasks


router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/generate_schedule")
async def get_load_next_move(request: Request):
    """
    Get the next move for the driver

    Parameters:
        plan_date (str): The date of the plan : required
        store_plan (bool): Whether to store the plan or not : required
    """
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')

        body = await request.json()
        store_plan = body.get('store_plan', False)
        plan_date = body.get('plan_date')
        
        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )
        
        if plan_date:
            try:
                datetime.strptime(plan_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError("Invalid plan_date format. Expected YYYY-MM-DD")
        
        if not plan_date:
            plan_date = datetime.now(tz=pytz.UTC).isoformat()
        else:
            plan_date = (
                datetime.strptime(plan_date, '%Y-%m-%d')
                .replace(hour=12, minute=0, second=0, microsecond=0)
                .isoformat()
            )

        result = await predict_next_move(
            user_payload=user_payload,
            plan_date=plan_date,
            options={ "store_plan": store_plan, "use_vrp": False }
        )

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"}, status_code=500
        )

@router.post("/generate_driver_plan")
async def generate_driver_plan(request: Request, background_tasks: BackgroundTasks):
    """
    Get the driver plan for the given date

    Parameters:
        plan_date (str): The date of the plan : required
    """
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')
        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )
        
        body = await request.json()
        plan_date = body.get('plan_date')
        reference_numbers = body.get('reference_numbers', [])
        save_plan = body.get('save_plan', False)
        add_to_existing_plan = body.get('add_to_existing_plan', False)
        plan_branch = body.get('plan_branch', [])
        shift = body.get('shift', None)

        is_approved_move_ids_provided = 'approved_modified_move_ids' in body
        approved_modified_move_ids = body.get('approved_modified_move_ids', [])
        
        result = await get_optmized_driver_plan(
            user_payload=user_payload, 
            plan_date=plan_date, 
            reference_numbers=reference_numbers, 
            save_plan=save_plan, 
            add_to_existing_plan=add_to_existing_plan,
            is_approved_move_ids_provided=is_approved_move_ids_provided,
            approved_modified_move_ids=approved_modified_move_ids,
            plan_branch=plan_branch,
            shift=shift,
            background_tasks=background_tasks,
        )

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"},
            status_code=500
        )

@router.post("/optimizer_review_recommendation")
async def optimizer_review_recommendation(request: Request):
    """
    Get the optimizer plan recommendation for the given date

    Parameters:
        plan_date (str): The date of the plan : required
    """
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')
        
        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )
        
        body = await request.json()
        plan_date = body.get('plan_date')
        reference_numbers = body.get('reference_numbers', [])
        plan_branch = body.get('plan_branch', [])
        shift = body.get('shift', None)
    
        result = await get_optimizer_review_recommendation(
            user_payload=user_payload,
            plan_date=plan_date,
            reference_numbers=reference_numbers,
            plan_branch=plan_branch,
            shift=shift
        )

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(content={"message": str(e), "status": "error"}, status_code=500)


@router.get("/add_move_to_optimizer_plan")
async def add_move_to_plan(request: Request, reference_number: str, driver: str, load_assigned_date: str, plan_id: str, move_id: str | None = None, is_free_flow_move: bool = False):
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')

        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )

        payload = {
            "reference_number": reference_number,
            "driver": driver,
            "load_assigned_date": load_assigned_date,
            "plan_id": plan_id
        }

        if (move_id):
            payload['move_id'] = move_id
        if (is_free_flow_move):
            payload['is_free_flow_move'] = is_free_flow_move

        result = await add_move_to_optimizer_plan(carrier, payload)

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(content={"message": str(e), "status": "error"}, status_code=500)

@router.get('/remove_move_from_optimizer_plan')
async def remove_move_from_plan(request: Request, reference_number: str, plan_id: str, optimizer_load_id: str, move_id: str | None = None, is_free_flow_move: bool = False):
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')

        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )
    
        payload = {
            "reference_number": reference_number,
            "plan_id": plan_id,
            "optimizer_load_id": optimizer_load_id
        }

        if (move_id):
            payload['move_id'] = move_id
        if (is_free_flow_move):
            payload['is_free_flow_move'] = is_free_flow_move

        result = await remove_move_from_optimizer_plan(carrier, payload)

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(content={"message": str(e), "status": "error"}, status_code=500)

@router.post("/reassign_move_to_optimizer_plan")
async def reassign_move_to_plan(request: Request):
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')

        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )
        
        body = await request.json()

        result = await reassign_move_to_optimizer_plan(carrier, body)

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(content={"message": str(e), "status": "error"}, status_code=500)


@router.post("/get_in_day_actions")
async def in_day_actions(request: Request):
    """
    Get the optimizer plan recommendation for the given date

    Parameters:
        plan_date (str): The date of the plan : required
    """
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')
        
        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )
        
        body = await request.json()
        plan_date = body.get('plan_date')
        reference_numbers = body.get('reference_numbers', [])
        plan_branch = body.get('plan_branch', [])
        shift = body.get('shift', None)
    
        result = await get_in_day_actions(
            user_payload=user_payload,
            plan_date=plan_date,
            reference_numbers=reference_numbers,
            plan_branch=plan_branch,
            shift=shift,
        )

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(content={"message": str(e), "status": "error"}, status_code=500)

@router.post("/store_driver_eta_details")
async def store_eta(request: Request):
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')

        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )
        
        payload = await request.json()
        eta_payloads = payload.get('eta_payloads', [])

        if payload.get('active_move'):
            eta_payloads.append(payload)

        result = await store_driver_eta_details(
            user_payload=user_payload,
            eta_payloads=eta_payloads
        )             

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(content={"message": str(e), "status": "error"}, status_code=500)

@router.post("/get_eta_details")
async def get_eta(request: Request):
    try:
        user_payload = request.state.user
        carrier = user_payload.get('carrier')

        if not carrier:
            return JSONResponse(
                content={"message": "Carrier ID not found in token"},
                status_code=400
            )
        
        payload = await request.json()
        eta_payload = payload.get('eta_payload')
        
        result = await get_eta_details(
            user_payload=user_payload,
            eta_payload=eta_payload
        )

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)
    except Exception as e:
        logger.error(e)
        return JSONResponse(content={"message": str(e), "status": "error"}, status_code=500)
