import logging
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.modules.agents.street_turn import generate_street_turns_recommendation

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/recommend_street_turns")
async def get_recommend_street_turns(request: Request):
    """
    Recommend street turns for a driver
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
        container_types = body.get('container_types', [])
        container_sizes = body.get('container_sizes', [])
        container_owners = body.get('container_owners', [])
        arrival_threshold = body.get('arrival_threshold', 0)
        delivery_radius = body.get('delivery_radius', 10)
        
        result = await generate_street_turns_recommendation(
            user_payload=user_payload,
            container_types=container_types,
            container_sizes=container_sizes,
            container_owners=container_owners,
            arrival_threshold=arrival_threshold,
            delivery_radius=delivery_radius
        )

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)
    
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"},
            status_code=500
        )
        
