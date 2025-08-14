import json
import logging
from typing import Dict, Any, List
from app.synced_db_connection import get_synced_db_client

logger = logging.getLogger(__name__)

async def get_equipment_validations(carrier: str) -> Dict[str, Any]:
    """
    Retrieve user settings from PostgreSQL for a given carrier.
    """
    try:
        synced_db = get_synced_db_client()
        pool = await synced_db.get_pool()

        settings = {}
        
        async with pool.acquire() as conn:
            query = """
                SELECT *
                FROM equipment_validations 
                WHERE carrier = $1
            """
            
            rows = await conn.fetch(query, carrier)
            
            if rows:
                settings = dict(rows[0])

        if settings.get('equipment_validations'):
            settings['equipment_validations'] = json.loads(settings['equipment_validations'])

        return settings
    
    except Exception as e:
        logger.error(f"Error retrieving user settings from database: {str(e)}")
        raise


async def get_container_sizes(carrier: str, container_size_ids: List[str]) -> Dict[str, Any]:
    """
    Retrieve container sizes from PostgreSQL for a given carrier.
    """
    try:
        synced_db = get_synced_db_client()
        pool = await synced_db.get_pool()
        
        async with pool.acquire() as conn:
            query = """
                SELECT _id, name, label
                FROM container_sizes
                WHERE carrier = $1
                AND _id = ANY($2)
            """
            
            rows = await conn.fetch(query, carrier, container_size_ids)
            
            if rows:
                return [dict(row) for row in rows]

        return []
    
    except Exception as e:
        logger.error(f"Error retrieving container sizes from database: {str(e)}")
        raise

async def get_container_types(carrier: str, container_type_ids: List[str]) -> Dict[str, Any]:
    """
    Retrieve container types from PostgreSQL for a given carrier.
    """
    try:
        synced_db = get_synced_db_client()
        pool = await synced_db.get_pool()
        
        async with pool.acquire() as conn:
            query = """
                SELECT _id, name, label
                FROM container_types
                WHERE carrier = $1
                AND _id = ANY($2)
            """
            
            rows = await conn.fetch(query, carrier, container_type_ids)
            
            if rows:
                return [dict(row) for row in rows]

        return []
    
    except Exception as e:
        logger.error(f"Error retrieving container types from database: {str(e)}")
        raise