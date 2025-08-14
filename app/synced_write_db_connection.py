import asyncpg
import logging
from settings import settings

logger = logging.getLogger(__name__)

async def get_direct_db_connection():
    """
    Get a direct PostgreSQL connection without using a connection pool.
    
    Returns:
        asyncpg.Connection: Direct database connection
        
    Raises:
        ConnectionError: If unable to establish connection
    """
    try:
        POSTGRES_URL = settings.SYNCED_WRITE_DB_URL
        
        if not POSTGRES_URL:
            raise ValueError("Synced PostgreSQL configuration is incomplete")
            
        connection = await asyncpg.connect(POSTGRES_URL)
        logger.info("Direct PostgreSQL connection established successfully")
        return connection
        
    except Exception as e:
        logger.error(f"Failed to establish direct PostgreSQL connection: {str(e)}")
        raise ConnectionError(f"Direct PostgreSQL connection failed: {str(e)}")
