from typing import Dict, Any
import logging
from app.services.trackos_service import get_recommendation_bulk
from settings import settings
import requests
from app.mongo_services.mongo_service import get_token

logger = logging.getLogger(__name__)

async def drayos_api_handler(carrier: str, request_type: str, endpoint: str, payload: Any = None) -> Any:
    token = await get_token(carrier)
    headers = {'Authorization': f"Bearer {token}"}
    
    url = f'{settings.DRAYOS_API_URL}/{endpoint}'
    
    try:
        if request_type.upper() == 'GET':
            response = requests.get(url, headers=headers)
        elif request_type.upper() == 'POST':
            response = requests.post(url, headers=headers, json=payload)
        else:
            raise ValueError(f"Unsupported request type: {request_type}")
            
        response.raise_for_status()
        response_data = response.json()
        
        if response_data.get('statusCode') == 200:
            return response_data.get('data')
        else:
            logger.error(f"Drayos API returned non-200 status code: {response_data.get('statusCode')}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to call Drayos API endpoint {endpoint}: {str(e)}")
        return None

async def get_recommended_returns(carrier: str, payload_data: Any) -> Dict[str, Any]:
    try:
        response = await get_recommendation_bulk(carrier, payload_data)
        return response.get('returnLocations', {})
    except Exception as e:
        raise Exception(f"Error getting recommendation: {str(e)}")