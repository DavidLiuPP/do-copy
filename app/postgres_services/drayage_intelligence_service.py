import json
import logging
from app.postgres_connection import PostgresConnection
from app.mongo_services.mongo_service import get_mongo_db
from bson.objectid import ObjectId
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

EMPTY_RETURN_OPTIONS = {
    'ASSIGN_PLACEHOLDER_LOCATION': 'ASSIGN_PLACEHOLDER_LOCATION',
    'EAST_WEST_SPLIT': 'EAST_WEST_SPLIT',
    'NEAREST_TO_DELIVERY': 'NEAREST_TO_DELIVERY',
    'SINGLE_YARD': 'SINGLE_YARD'
}

async def get_drayage_intelligence(carrier: str, projection: List[str] = None) -> Dict[str, Any]:
    """
    Get drayage intelligence for a carrier with optional projection.
    Now includes empty_drop_yard_config for DROPCONTAINER yard selection logic.
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        
        # Build SQL with projection
        if projection:
            columns = ', '.join(projection)
            if 'empty_drop_yard_config' not in projection:
                columns += ', empty_drop_yard_config'
            if 'empty_drop_logic_version' not in projection:
                columns += ', empty_drop_logic_version'
            SQL = f"SELECT {columns} FROM drayage_intelligence WHERE carrier = $1"
        else:
            SQL = "SELECT * FROM drayage_intelligence WHERE carrier = $1"
        
        async with pool.acquire() as conn:
            row = await conn.fetchrow(SQL, carrier)
            if row:
                row_data = dict(row)
                
                if row_data.get('empty_logic_type') == EMPTY_RETURN_OPTIONS['ASSIGN_PLACEHOLDER_LOCATION'] and 'empty_return_location' in row_data:
                    db = await get_mongo_db()
                    customer = await db.customers.find_one(
                        {"_id": ObjectId(row['empty_return_location'])},
                        {"company_name": 1, "address": 1, "city": 1, "state": 1, "country": 1, "zip_code": 1}
                    )
                    row_data['empty_return_location'] = customer if customer else None
                
                if row_data.get('empty_drop_yard_config'):
                    yard_config = row_data['empty_drop_yard_config']
                    if isinstance(yard_config, str):
                        yard_config = json.loads(yard_config)
                        row_data['empty_drop_yard_config'] = yard_config

                    # Fetch yard details from MongoDB for all configured yards
                    if yard_config.get('yards'):
                        db = await get_mongo_db()
                        yard_details = {}
                        
                        for yard_key, yard_info in yard_config['yards'].items():
                            if yard_info.get('yard_id'):
                                try:
                                    customer = await db.customers.find_one(
                                        {"_id": ObjectId(yard_info['yard_id'])},
                                        {"company_name": 1, "address": 1, "city": 1, "state": 1, "country": 1, "zip_code": 1}
                                    )
                                    if customer:
                                        yard_details[yard_key] = {
                                            **yard_info,
                                            'customer_data': customer
                                        }
                                except Exception as e:
                                    logger.warning(f"Could not fetch yard {yard_info['yard_id']}: {str(e)}")
                        
                        row_data['empty_drop_yard_details'] = yard_details
                
                if row_data.get('is_exclude_from_scheduler') and row_data.get('exclude_locations_for_scheduler'):
                    if isinstance(row_data['exclude_locations_for_scheduler'], str):
                        row_data['exclude_locations_for_scheduler'] = json.loads(row_data['exclude_locations_for_scheduler'])
                else:
                    row_data['exclude_locations_for_scheduler'] = []
                
                return row_data
            return {}
    
    except Exception as e:
        logger.error(f"Error getting drayage intelligence: {str(e)}")
        return {}