import asyncpg
import logging
from typing import Optional
from settings import settings

logger = logging.getLogger(__name__)

class PostgresConnection:
    """
    A singleton class to manage PostgreSQL connection.
    Implements connection pooling and proper error handling.
    """
    _instance: Optional['PostgresConnection'] = None
    _pool = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PostgresConnection, cls).__new__(cls)
        return cls._instance

    async def initialize(self):
        """
        Initialize PostgreSQL connection pool during application startup.
        
        Raises:
            ConnectionError: If unable to establish PostgreSQL connection
            ConfigurationError: If database configuration is invalid
        """
        try:
            POSTGRES_URL = settings.POSTGRES_URL

            # Validate PostgreSQL connection settings
            if not POSTGRES_URL:
                raise ValueError("PostgreSQL configuration is incomplete")

            # Configure connection pool with optimal settings
            self._pool = await asyncpg.create_pool(
                POSTGRES_URL,
                min_size=5,
                max_size=20,
                max_inactive_connection_lifetime=30.0,  # Close inactive connections after 30 seconds
                timeout=30.0  # Connection timeout of 30 seconds
            )

            # Verify connection is successful
            async with self._pool.acquire() as conn:
                await conn.execute('SELECT 1')
            logger.info("PostgreSQL connection established successfully")

        except Exception as e:
            logger.error(f"Failed to establish PostgreSQL connection: {str(e)}")
            raise ConnectionError(f"PostgreSQL connection failed: {str(e)}")
        

    async def reconnect(self):
        """
        Reconnect to a new PostgreSQL connection.
        """
        try:
            # Disconnect current connection
            await self.close()
            await self.initialize()

            logger.info("PostgreSQL connection reestablished successfully")
        except Exception as e:
            logger.error(f"Failed to reconnect to PostgreSQL: {str(e)}")
            raise ConnectionError(f"PostgreSQL reconnection failed: {str(e)}")

    async def get_pool(self):
        """
        Returns a PostgreSQL connection pool.
        
        Returns:
            asyncpg.Pool: Connection pool instance
            
        Raises:
            ConnectionError: If pool is not initialized
        """
        if self._pool is None:
            raise ConnectionError("PostgreSQL connection not initialized. Call initialize() first.")
        return self._pool

    async def close(self):
        """Closes the PostgreSQL connection pool and cleans up resources during application shutdown"""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            logger.info("PostgreSQL connection closed")

def get_postgres_client():
    """
    Factory function to get PostgreSQL connection manager instance.
    
    Returns:
        PostgresConnection: Singleton instance of PostgreSQL connection manager
    """
    return PostgresConnection()

async def get_postgres_pool():
    """
    Get the PostgreSQL connection pool. Connection must be initialized first.
    
    Returns:
        asyncpg.Pool: Connection pool instance
    """
    client = get_postgres_client()
    return await client.get_pool()
