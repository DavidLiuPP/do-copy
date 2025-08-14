import json
import logging
from app.postgres_connection import PostgresConnection
from app.mongo_services.mongo_service import get_mongo_db
from bson.objectid import ObjectId
from typing import Dict, Any

logger = logging.getLogger(__name__)

EMPTY_RETURN_OPTIONS = {
    'ASSIGN_PLACEHOLDER_LOCATION': 'ASSIGN_PLACEHOLDER_LOCATION'
}

async def get_drayage_intelligence(carrier: str, projection: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Get drayage intelligence for a carrier with optional projection.
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        SQL = """
            SELECT {} FROM drayage_intelligence WHERE carrier = $1
        """
        if projection:
            columns = ', '.join(projection)
            SQL = SQL.format(columns)
        else:
            SQL = SQL.format('*')
        async with pool.acquire() as conn:
            row = await conn.fetchrow(SQL, carrier)
            if row:
                row_data = dict(row)
                if row_data.get('empty_logic_type') == EMPTY_RETURN_OPTIONS['ASSIGN_PLACEHOLDER_LOCATION'] and 'empty_return_location' in row_data:
                    db = await get_mongo_db()
                    customer = await db.customers.find_one({ "_id": ObjectId(row['empty_return_location']) }, { "company_name": 1, "address": 1, "city": 1, "state": 1, "country": 1, "zip_code": 1 })
                    row_data['empty_return_location'] = customer if customer else None

                if row_data.get('is_exclude_from_scheduler') and len(row_data.get('exclude_locations_for_scheduler', '')) > 0:
                    row_data['exclude_locations_for_scheduler'] = json.loads(row_data['exclude_locations_for_scheduler']) if row_data['exclude_locations_for_scheduler'] else []
                else:
                    row_data['exclude_locations_for_scheduler'] = []
                return row_data
            return {}
    
    except Exception as e:
        print(f"Error getting drayage intelligence: {str(e)}")
        return {}