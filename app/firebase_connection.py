import logging
import requests  # or import aiohttp for async
from typing import Any, Dict, Optional
from settings import settings

logger = logging.getLogger(__name__)

class FirebaseConnection:
    """
    A singleton class to manage Firebase Realtime Database connection.
    Handles initialization and data push operations with proper error handling.
    """
    _instance: Optional['FirebaseConnection'] = None
    _initialized = False
    _client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirebaseConnection, cls).__new__(cls)
        return cls._instance

    async def push_to_channel(self, channel: str, data: Dict[str, Any]) -> None:
        """
        Store data at specified Firebase channel/path using REST API.
        Overwrites any existing data at that location.
        
        Args:
            channel: Firebase path to store data at
            data: Dictionary containing data to store
            
        Raises:
            ConnectionError: If Firebase is not initialized
            ValueError: If channel or data is invalid
        """
        try:
            # Format the URL for Firebase REST API
            if settings.FIREBASE_DATABASEURL and settings.FIREBASE_APIKEY:
                url = f"{settings.FIREBASE_DATABASEURL}/{channel}.json"
                params = {'auth': settings.FIREBASE_APIKEY}
                
                # Use PUT to overwrite existing data instead of POST to push
                response = requests.put(url, json=data, params=params)
                response.raise_for_status()  # Raises an HTTPError for bad responses

                logger.debug(f"Successfully stored data at Firebase channel: {channel}")
        except Exception as e:
            logger.error(f"Failed to store data at Firebase channel {channel}: {str(e)}")
            raise

def get_firebase_client():
    """
    Factory function to get Firebase connection manager instance.
    
    Returns:
        FirebaseConnection: Singleton instance of Firebase connection manager
    """
    return FirebaseConnection()
