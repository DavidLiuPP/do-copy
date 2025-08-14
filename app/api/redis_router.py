from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import logging
from app.services.redis_service import set_carrier_settings, set_default_yard_location

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/set_carrier_settings")
async def set_carrier_in_redis(request: Request):
    """
    Set carrier settings in Redis

    Parameters:
        request (Request): The incoming request containing the file
    """
    try:
        form = await request.form()
        carrier_id = form.get("carrier_id")

        if not carrier_id:
            return JSONResponse(
                content={"message": "Carrier ID is required"},
                status_code=400
            )

        result = await set_carrier_settings(carrier_id)

        response = { "result": str(result), "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"}, status_code=500
        )

@router.post("/set_default_yard_location")
async def set_yard_location(request: Request):
    """
    Set the default yard location in Redis

    Parameters:
        request (Request): The incoming request containing the carrier ID and location
    """
    try:
        body = await request.json()
        carrier_id = body.get("carrier_id")
        location = body.get("location")
        if not carrier_id:
            return JSONResponse(
                content={"message": "Carrier ID is required"},
                status_code=400)
        
        result = await set_default_yard_location(carrier_id, location)

        response = { "result": str(result), "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"}, status_code=500
        )
