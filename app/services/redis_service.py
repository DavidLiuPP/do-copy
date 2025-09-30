import json
import logging
from bson import ObjectId
from typing import Dict, Any, Optional

from app.modules.optimizer.constants import CHASSIS_YARD_CONFIGS, YARD_ALLOWED_OPERATIONS
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

async def set_carrier_settings(carrier_id: str) -> Optional[Dict[str, Any]]:
    """
    Store carrier settings in Redis with TTL.

    Args:
        carrier_id: Unique identifier for the carrier

    Returns:
        Optional[Dict[str, Any]]: Carrier settings if successful, None if not found, False if error
    """
    try:
        redis_key = f"{CARRIER_SETTINGS_PREFIX}:{carrier_id}"

        setting_collection = await get_setting_collection()

        settings = await setting_collection.find_one({"carrier": ObjectId(carrier_id)})

        if not settings:
            logger.warning(f"No carrier settings found in database for carrier_id: {carrier_id}")
            return None

        # Convert cursor to list
        settings_json = flatten_bson(settings)

        settings_json_str = json.dumps(settings_json)
        
        # Store in Redis with TTL
        if redis_client:
            await redis_client.set(redis_key, settings_json_str, ex=SETTINGS_TTL)

        return settings_json

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
        if not redis_client:
            settings_json = await set_carrier_settings(carrier_id)
            
            # Handle case where set_carrier_settings returns False (error case) or None (not found)
            if settings_json is False:
                logger.error(f"Failed to set carrier settings for carrier_id: {carrier_id}")
                return {}
            elif settings_json is None:
                logger.warning(f"No carrier settings found for carrier_id: {carrier_id}")
                return {}
                
            return settings_json

        redis_key = f"{CARRIER_SETTINGS_PREFIX}:{carrier_id}"
        settings_json = await redis_client.get(redis_key)
        
        if settings_json:
            settings_json = json.loads(settings_json)
            return settings_json
        
        # If not found in Redis, fetch from database
        logger.info(f"Carrier settings not found in Redis for carrier_id: {carrier_id}, fetching from database")
        settings_json = await set_carrier_settings(carrier_id)
        
        # Handle case where set_carrier_settings returns False (error case) or None (not found)
        if settings_json is False:
            logger.error(f"Failed to set carrier settings for carrier_id: {carrier_id}")
            return {}
        elif settings_json is None:
            logger.warning(f"No carrier settings found for carrier_id: {carrier_id}")
            return {}
            
        return settings_json
    
    except Exception as e:
        logger.error(f"Error getting carrier settings from Redis: {str(e)}")
        return {}

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
    
async def get_default_yard_location(carrier_id: str, get_from_db: bool = False, plan_branch: list = [], allowed_operations: list = []) -> Optional[Dict[str, Any]]:
    """
    Get the default yard location from Redis
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        SQL = f"SELECT * FROM default_yard_locations WHERE carrier_id = '{carrier_id}'"
        
        
        if len(plan_branch) > 0:
            SQL += f" AND plan_branch ?| array{plan_branch}"
        if len(allowed_operations) > 0:
            SQL += f" AND allowed_operations ?| array{allowed_operations}"

        async with pool.acquire() as conn:
            row = await conn.fetch(SQL)
            if row:
                locations = [dict(r) for r in row]
                if len(locations) > 0:
                    # get all customer_id from locations
                    customer_ids = [ObjectId(location['customer_id']) for location in locations]
                    
                    chassis_yard_ids = []
                    container_yard_ids = []
                    for loc in locations:
                        allowed_ops = loc.get('allowed_operations', [])
                        if YARD_ALLOWED_OPERATIONS['CHASSIS_DROP_HOOK'] in allowed_ops:
                            chassis_yard_ids.append(loc['customer_id'])
                        if YARD_ALLOWED_OPERATIONS['CONTAINER_DROP_HOOK'] in allowed_ops:
                            container_yard_ids.append(loc['customer_id'])
                    
                    db = await get_mongo_db()
                    customer_details = await db.customers.find({ "_id": {"$in" : customer_ids} }, { "_id": 1,"company_name": 1, "address": 1, "city": 1, "state": 1, "country": 1, "zip_code": 1 }).to_list(None)
                    default_yard_locations = []
                    for customer in customer_details:
                        # Find the corresponding location record to get chassis permissions
                        allowed_operations = []
                        customer_id_str = str(customer.get("_id", ""))
                        if customer_id_str in chassis_yard_ids:
                            allowed_operations.append(YARD_ALLOWED_OPERATIONS['CHASSIS_DROP_HOOK'])
                            
                        if customer_id_str in container_yard_ids:
                            allowed_operations.append(YARD_ALLOWED_OPERATIONS['CONTAINER_DROP_HOOK'])
                        
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
                            "is_chassis_termination_allowed": location_record.get("is_chassis_termination_allowed", True) if location_record else True,
                            "allowed_operations": allowed_operations
                        })
                        
                    return default_yard_locations
            return []
    except Exception as e:
        logger.error(f"Error getting default yard location: {str(e)}")
        raise Exception(f"Failed to get default yard location: {str(e)}")


async def get_shift_times(carrier: str) -> Dict[str, Any]:
    """
    Get the shift times from Redis
    """
    try:
        carrier_settings = await get_carrier_settings(carrier)
        
        # Handle case where carrier_settings is None or False
        if not carrier_settings:
            logger.warning(f"No carrier settings found for carrier: {carrier}")
            return []
        
        shift_times = carrier_settings.get('shiftTimes', [])
        isShiftEnabled = carrier_settings.get('isShiftEnabled', False)

        if not isShiftEnabled:
            shift_times = []

        shift_times = [dict(s) for s in shift_times if s.get('isEnabled')]
        
        return shift_times
    except Exception as e:
        logger.error(f"Error getting shift times from Redis: {str(e)}")
        return []