import logging
from typing import List, Dict, Any
from bson import ObjectId

from app.mongo_services.mongo_service import get_trips_collection, get_orders_collection
from app.utils.common_utils import flatten_bson

logger = logging.getLogger(__name__)

async def get_trips(criteria: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
    """
    Generic function to retrieve trips from MongoDB based on provided criteria.

    Args:
        criteria (Dict[str, Any]): Query criteria/filter to apply
        limit (int, optional): Maximum number of trips to return. Defaults to 10.

    Returns:
        List[Dict[str, Any]]: List of trip documents matching the criteria

    Raises:
        Exception: For database operation errors
    """
    try:
        # Get MongoDB collection
        trips_collection = await get_trips_collection()

        # Execute query with provided criteria and projection
        cursor = trips_collection.find(
            criteria,
            {}
        ).limit(limit)

        # Convert cursor to list
        trips = await cursor.to_list(length=limit)
        return flatten_bson(trips)

    except Exception as e:
        logger.error(f"Error retrieving trips from database: {str(e)}")
        raise

async def get_orders(criteria: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
    """
    Generic function to retrieve orders from MongoDB based on provided criteria.

    Args:
        criteria (Dict[str, Any]): Query criteria/filter to apply
        limit (int, optional): Maximum number of orders to return. Defaults to 10.

    Returns:
        List[Dict[str, Any]]: List of trip documents matching the criteria

    Raises:
        Exception: For database operation errors
    """
    try:
        # Get MongoDB collection
        orders_collection = await get_orders_collection()

        # Execute query with provided criteria and projection
        cursor = orders_collection.find(
            criteria,
            {}
        ).limit(limit)

        # Convert cursor to list
        orders = await cursor.to_list(length=limit)
        return flatten_bson(orders)

    except Exception as e:
        logger.error(f"Error retrieving orders from database: {str(e)}")
        raise

async def get_trips_by_trip_ids(carrier: str, trip_ids: List[str], limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get trips by trip ids.
    """
    try:
        criteria = {
            "carrier": ObjectId(carrier),
            "_id": {"$in": [ObjectId(trip_id) for trip_id in trip_ids]},
            "isDeleted": False
        }
        return await get_trips(criteria, limit)
    
    except Exception as e:
        logger.error(f"Error retrieving trips from database: {str(e)}")
        raise

async def get_free_flow_trips_by_order_ids(carrier: str, order_ids: List[str], limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Get available free flow trips.
    """
    try:
        criteria = {
            "carrier": ObjectId(carrier),
            "tripType": "FREEFLOW",
            "status": "AVAILABLE",
            "orderId": {"$in": [ObjectId(order_id) for order_id in order_ids]},
            "driverId": {"$exists": False},
            "isDeleted": False
        }
        return await get_trips(criteria, limit)
    
    except Exception as e:
        logger.error(f"Error retrieving trips from database: {str(e)}")
        raise

async def get_order_details(carrier: str, order_ids: List[str], limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Get trips by trip ids.
    """
    try:
        criteria = {
            "carrier": ObjectId(carrier),
            "_id": {"$in": [ObjectId(order_id) for order_id in order_ids]},
            "isDeleted": False
        }
        return await get_orders(criteria, limit)
    
    except Exception as e:
        logger.error(f"Error retrieving trips from database: {str(e)}")
        raise
