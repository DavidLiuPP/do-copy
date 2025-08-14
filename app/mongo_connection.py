import motor.motor_asyncio
import logging
from typing import Optional
from settings import settings

logger = logging.getLogger(__name__)

class MongoDBConnection:
    """
    A singleton class to manage MongoDB connection with read-only access.
    Implements connection pooling and proper error handling.
    """
    _instance: Optional['MongoDBConnection'] = None
    _db = None
    _trackos_db = None
    _client = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBConnection, cls).__new__(cls)
        return cls._instance
    async def initialize(self):
        """
        Initialize MongoDB connection during application startup.
        
        Raises:
            ConnectionError: If unable to establish MongoDB connection
            ConfigurationError: If database configuration is invalid
        """
        try:
            # Validate MongoDB connection settings
            if not settings.MONGO_URL or not settings.MONGO_DB:
                raise ValueError("MongoDB configuration is incomplete")
            # Configure client with optimal settings for read-only access
            self._client = motor.motor_asyncio.AsyncIOMotorClient(
                settings.MONGO_URL,
                readPreference='secondary',  # Read from secondary nodes for load balancing
                w=0,  # Don't wait for write acknowledgement
                maxIdleTimeMS=30000,  # Close idle connections after 30 seconds
                serverSelectionTimeoutMS=5000,  # Fail fast if server selection takes too long
                connectTimeoutMS=5000,
                retryWrites=False  # Disable retry writes since this is read-only
            )
            # Verify connection is successful
            await self._client.admin.command('ping')
            logger.info("MongoDB connection established successfully")

            self._db = self._client[settings.MONGO_DB]
            if settings.TRACKOS_DB:
                self._trackos_db = self._client[settings.TRACKOS_DB]
        except Exception as e:
            logger.error(f"Failed to establish MongoDB connection: {str(e)}")
            raise ConnectionError(f"MongoDB connection failed: {str(e)}")

    async def get_database(self):
        """
        Returns a MongoDB database instance with read-only access.
        
        Returns:
            motor.motor_asyncio.AsyncIOMotorDatabase: Database instance
            
        Raises:
            ConnectionError: If database is not initialized
        """
        if self._db is None:
            raise ConnectionError("MongoDB connection not initialized. Call initialize() first.")
        return self._db
    
    async def get_trackos_db(self):
        if self._trackos_db is None:
            raise ConnectionError("MongoDB connection not initialized. Call initialize() first.")
        return self._trackos_db
    async def close(self):
        """Closes the MongoDB connection and cleans up resources during application shutdown"""
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None
            self._trackos_db = None
            logger.info("MongoDB connection closed")
def get_mongo_client():
    """
    Factory function to get MongoDB connection manager instance.
    
    Returns:
        MongoDBConnection: Singleton instance of MongoDB connection manager
    """
    return MongoDBConnection()
async def get_mongo_db():
    """
    Get the MongoDB database instance. Connection must be initialized first.
    
    Returns:
        motor.motor_asyncio.AsyncIOMotorDatabase: Database instance
    """
    client = get_mongo_client()
    return await client.get_database()
async def get_trackos_db():
    client = get_mongo_client()
    return await client.get_trackos_db()