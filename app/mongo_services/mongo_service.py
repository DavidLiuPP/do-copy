"""
MongoDB service module for managing load-related database operations.

This module provides an abstraction layer for interacting with the loads collection
in MongoDB, implementing proper error handling and connection management.
"""
import logging
from motor.motor_asyncio import AsyncIOMotorCollection
from app.mongo_connection import get_mongo_db
from bson.objectid import ObjectId
logger = logging.getLogger(__name__)


async def get_loads_collection() -> AsyncIOMotorCollection:
    """
    Get MongoDB loads collection with proper connection handling.

    Returns:
        AsyncIOMotorCollection: MongoDB collection instance for loads

    Raises:
        ConnectionError: If database connection cannot be established
        Exception: For other database-related errors

    Note:
        This function ensures the database connection is properly initialized
        before accessing the collection. It implements proper error handling
        and logging for production use.
    """
    try:
        db = await get_mongo_db()
        if db is None:
            raise ConnectionError("Failed to get database connection")
            
        collection = db.loads
        return collection

    except ConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error accessing loads collection: {str(e)}")
        raise Exception(f"Failed to access loads collection: {str(e)}")
    


async def get_charge_group_collection() -> AsyncIOMotorCollection:
    """
    Get MongoDB loads collection with proper connection handling.

    Returns:
        AsyncIOMotorCollection: MongoDB collection instance for charge groups

    Raises:
        ConnectionError: If database connection cannot be established
        Exception: For other database-related errors

    Note:
        This function ensures the database connection is properly initialized
        before accessing the collection. It implements proper error handling
        and logging for production use.
    """
    try:
        db = await get_mongo_db()
        if db is None:
            raise ConnectionError("Failed to get database connection")
            
        collection = db.chargegroups
        return collection

    except ConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error accessing charge groups collection: {str(e)}")
        raise Exception(f"Failed to access charge groups collection: {str(e)}")


async def get_trips_collection() -> AsyncIOMotorCollection:
    """
    Get MongoDB trips collection with proper connection handling.

    Returns:
        AsyncIOMotorCollection: MongoDB collection instance for trips

    Raises:
        ConnectionError: If database connection cannot be established
        Exception: For other database-related errors

    Note:
        This function ensures the database connection is properly initialized
        before accessing the collection. It implements proper error handling
        and logging for production use.
    """
    try:
        db = await get_mongo_db()
        if db is None:
            raise ConnectionError("Failed to get database connection")
            
        collection = db.triporders
        return collection

    except ConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error accessing trips collection: {str(e)}")
        raise Exception(f"Failed to access trips collection: {str(e)}")
    
async def get_orders_collection() -> AsyncIOMotorCollection:
    """
    Get MongoDB orders collection with proper connection handling.

    Returns:
        AsyncIOMotorCollection: MongoDB collection instance for orders

    Raises:
        ConnectionError: If database connection cannot be established
        Exception: For other database-related errors

    Note:
        This function ensures the database connection is properly initialized
        before accessing the collection. It implements proper error handling
        and logging for production use.
    """
    try:
        db = await get_mongo_db()
        if db is None:
            raise ConnectionError("Failed to get database connection")
            
        collection = db.orders
        return collection

    except ConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error accessing orders collection: {str(e)}")
        raise Exception(f"Failed to access orders collection: {str(e)}")


async def get_token(carrier: str) -> str:
    db = await get_mongo_db()
    res = await db.sessions.find_one({"user": ObjectId(carrier), "token": {"$ne": None}})
    if res: 
        return res.get('token', '')
    else:
        return ''
    
async def get_setting_collection() -> AsyncIOMotorCollection:
    try:
        db = await get_mongo_db()
        if db is None:
            raise ConnectionError("Failed to get database connection")
            
        collection = db.settings
        return collection

    except ConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error accessing settings collection: {str(e)}")
        raise Exception(f"Failed to access settings collection: {str(e)}")