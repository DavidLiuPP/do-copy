from typing import Any
import requests
from settings import settings
from app.mongo_services.mongo_service import get_token

async def get_recommendation_bulk(carrier: str, payload: Any) -> Any:
    try:
        trackos_token = await get_token(carrier)
        
        headers = {
            "Authorization": f"Bearer {trackos_token}",
            "Content-Type": "application/json",
            "xapikey": settings.TRACKOS_API_KEY
        }

        response = requests.post(f"{settings.TRACKOS_API_URL}/api/EMPTY/get-return-recommendations", headers=headers, json=payload)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx, 5xx)
        return response.json()
        
    except requests.exceptions.RequestException as e:
        # Handle network errors, timeouts, etc
        raise Exception(f"Failed to get recommendation from Trackos API: {str(e)}")
    except ValueError as e:
        # Handle JSON decode errors
        raise Exception(f"Failed to parse Trackos API response: {str(e)}")