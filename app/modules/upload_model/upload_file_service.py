"""
PostgreSQL service module for managing event location database operations.

This module provides an abstraction layer for interacting with the event_location table
in PostgreSQL, implementing proper error handling, connection pooling, and data validation.

The module follows repository pattern for database operations and implements comprehensive
error handling and logging for production use.
"""
import logging
import os.path
from typing import Dict, Any, List
from app.postgres_connection import PostgresConnection
from asyncpg.exceptions import UniqueViolationError, ForeignKeyViolationError

logger = logging.getLogger(__name__)

async def get_model_files(carrier: str, file_type: str, load_type: str, file_name: str = None, ignore_carrier=False) -> List[Dict[str, Any]]:
    try:

        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        
        async with pool.acquire() as conn:
            
            base_query = f"""
                SELECT DISTINCT ON (file_name)
                    file_name,
                    load_type,
                    carrier,
                    file_type,
                    file_url,
                    version
                FROM upload_models 
                WHERE 1=1
            """
            if carrier and not ignore_carrier:
                base_query += f" AND carrier = '{carrier}'"

            if file_type is not None:
                base_query += f" AND file_type = '{file_type}'"

            if file_name is not None:
                base_query += f" AND file_name = '{file_name}'"

            if load_type is not None:
                base_query += f" AND load_type = '{load_type}'"
            
            base_query += f"  ORDER BY file_name, version DESC"

            records = await conn.fetch(base_query)

            # if len(records) == 0 and not ignore_carrier:
            #     records = await conn.fetch(base_query, "60747b8448cb687a0f377e82")
                
            return sorted(records, key=lambda x: x['version'], reverse=True)
        
    except ConnectionError as e:
        logger.error(f"Database connection error: {str(e)}", exc_info=True)
        raise ConnectionError(f"Failed to connect to database: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error retrieving event location data: {str(e)}", exc_info=True)
        raise
    
async def store_file_in_db(object: Dict[str, Any]) -> None:
    try:
        if not object or not isinstance(object, dict):
            raise ValueError("Object must be a non-empty dictionary")
        
        # if not object["carrier"] or not isinstance(object["carrier"], str):
        #     raise ValueError("Carrier must be a non-empty string")

        if not object["file_name"] or not isinstance(object["file_name"], str):
            raise ValueError("File name must be a non-empty string")

        if not object["load_type"] or not isinstance(object["load_type"], str):
            raise ValueError("Load type must be a non-empty string")
        
        if not object["file_type"] or not isinstance(object["file_type"], str):
            raise ValueError("File type must be a non-empty string")
        
        if not object["file_url"] or not isinstance(object["file_url"], str):
            raise ValueError("File URL must be a non-empty string")
        
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO upload_models (carrier, file_name, load_type, file_type, file_url, version, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, object["carrier"], object["file_name"], object["load_type"], object["file_type"], object["file_url"], object["version"])

    except ConnectionError as e:
        logger.error(f"Database connection error: {str(e)}", exc_info=True)
        raise ConnectionError(f"Failed to connect to database: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error storing file in database: {str(e)}", exc_info=True)
        raise