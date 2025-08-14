import json
import logging
from bson import ObjectId
from typing import Dict, Any, Optional

from app.redis_connection import redis_client
from app.mongo_services.mongo_service import get_mongo_db
from app.postgres_connection import PostgresConnection
from app.mongo_services.mongo_service import get_setting_collection
from app.utils.common_utils import flatten_bson

logger = logging.getLogger(__name__)

CARRIER_SETTINGS_PREFIX = "DO_carrier_settings"
DEFAULT_YARD_LOCATION_PREFIX = "default_yard_location"
DEFAULT_YARD_LOCATION_PREFIX_MULTIPLE = "default_yard_location_multiple"

SETTINGS_TTL = 1 * 60 * 60  # 1 hour in seconds

async def set_carrier_settings(carrier_id: str) -> bool:
    """
    Store carrier settings in Redis with TTL.

    Args:
        carrier_id: Unique identifier for the carrier
        settings: Dictionary containing carrier settings

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        redis_key = f"{CARRIER_SETTINGS_PREFIX}:{carrier_id}"

        setting_collection = await get_setting_collection()

        settings = await setting_collection.find_one({"carrier": ObjectId(carrier_id)}, {"carrier": 1, "isDispatchAutomationEnabled": 1, "shiftTimes": 1})

        # Convert cursor to list
        settings_json = flatten_bson(settings)

        settings_json = json.dumps(settings_json)
        
        # Store in Redis with TTL
        if redis_client:
            await redis_client.set(redis_key, settings_json, ex=SETTINGS_TTL)

        return settings

    except Exception as e:
        logger.error(f"Error setting carrier settings in Redis: {str(e)}")
        return False

async def get_carrier_settings(carrier_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve carrier settings from Redis.

    Args:
        carrier_id: Unique identifier for the carrier

    Returns:
        Optional[Dict[str, Any]]: Carrier settings if found, None otherwise
    """
    try:

        if redis_client:
            redis_key = f"{CARRIER_SETTINGS_PREFIX}:{carrier_id}"
            settings_json = await redis_client.get(redis_key)
            
            if settings_json:
                settings_json = json.loads(settings_json)
                return settings_json
        
        settings_json = await set_carrier_settings(carrier_id)
            
        return settings_json
    
    except Exception as e:
        logger.error(f"Error getting carrier settings from Redis: {str(e)}")
        return None

async def set_default_yard_location(carrier_id: str, location: Dict[str, Any]) -> bool:
    """
    Set the default yard location in Redis
    """
    try:
        redis_key = f"{DEFAULT_YARD_LOCATION_PREFIX_MULTIPLE}:{carrier_id}"
        location_json = json.dumps(location)
        SQL = """
            INSERT INTO default_yard_locations (carrier_id, location)
            VALUES ($1, $2)
            RETURNING carrier_id;
        """
        
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        
        async with pool.acquire() as conn:
            row = await conn.fetchrow(SQL, carrier_id, location_json)
            if row:
                if redis_client:
                    await get_default_yard_location(carrier_id, True)
                return True
            else:
                return False
    
    except Exception as e:
        logger.error(f"Error setting default yard location in Redis: {str(e)}")
        return False
    
async def get_default_yard_location(carrier_id: str, get_from_db: bool = False) -> Optional[Dict[str, Any]]:
    """
    Get the default yard location from Redis
    """
    try:
        default_yard_locations = None
        if not get_from_db and redis_client:
            redis_key = f"{DEFAULT_YARD_LOCATION_PREFIX_MULTIPLE}:{carrier_id}"
            default_yard_locations = await redis_client.get(redis_key)

        if not default_yard_locations:
            postgres = PostgresConnection()
            pool = await postgres.get_pool()
            SQL = """
                SELECT * FROM default_yard_locations WHERE carrier_id = $1
            """
            async with pool.acquire() as conn:
                row = await conn.fetch(SQL, carrier_id)
                if row:
                    locations = [dict(r) for r in row]
                    if len(locations) > 0:
                        # get all customer_id from locations
                        customer_ids = [ObjectId(location['customer_id']) for location in locations]
                        db = await get_mongo_db()
                        customer_details = await db.customers.find({ "_id": {"$in" : customer_ids} }, { "_id": 1,"company_name": 1, "address": 1, "city": 1, "state": 1, "country": 1, "zip_code": 1 }).to_list(None)
                        default_yard_locations = []
                        for customer in customer_details:
                            # Find the corresponding location record to get chassis permissions
                            location_record = next((l for l in locations if l['customer_id'] == str(customer.get("_id", ""))), None)
                            default_yard_locations.append({
                                "customerId": str(customer.get("_id", "")),
                                "company_name": customer.get("company_name", ""),
                                "address": customer.get("address", {}),
                                "city": customer.get("city", ""),
                                "state": customer.get("state", ""),
                                "country": customer.get("country", ""),
                                "zip_code": customer.get("zip_code", ""),
                                "id": str(location_record.get("id", "")) if location_record else "",
                                "is_chassis_pick_allowed": location_record.get("is_chassis_pick_allowed", True) if location_record else True,
                                "is_chassis_termination_allowed": location_record.get("is_chassis_termination_allowed", True) if location_record else True
                            })

                        if redis_client:    
                            redis_key = f"{DEFAULT_YARD_LOCATION_PREFIX_MULTIPLE}:{carrier_id}"
                            await redis_client.set(redis_key, json.dumps(default_yard_locations), ex=SETTINGS_TTL)
                        
                        return default_yard_locations
        
        if default_yard_locations:
            return json.loads(default_yard_locations)
        
        return None
    except Exception as e:
        logger.error(f"Error getting default yard location from Redis: {str(e)}")
        raise ConnectionError(f"Error getting default yard location from Redis: {str(e)}")
    


async def get_shift_times(carrier: str) -> Dict[str, Any]:
    """
    Get the shift times from Redis
    """
    try:
        carrier_settings = await get_carrier_settings(carrier)
        shift_times = carrier_settings.get('shiftTimes', [])
        shift_times = [dict(s) for s in shift_times if s.get('isEnabled')]
        
        return shift_times
    except Exception as e:
        logger.error(f"Error getting shift times from Redis: {str(e)}")
        raise ConnectionError(f"Error getting shift times from Redis: {str(e)}")