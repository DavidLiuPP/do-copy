"""Street Turn Recommendation Module

This module provides functionality for generating street turn recommendations by matching
empty import containers with upcoming export loads based on container attributes,
location proximity, and timing constraints.

The module implements an optimized matching algorithm that considers:
- Container compatibility (size, type, owner)
- Geographic proximity using distance calculations
- Temporal constraints with free return dates and cutoff times

Logic for street turn
1. Find Import containers, where
    a. container is already delivered
    b. status is Dropped
    c. is ready for return
    d. container type, size, ssl should be there
    e. if specific type, size, ssl are provided in configuratio, only fetch loads having one of those type/size/owner
2. Find export containers, where
    a. container status is Pending, Available
    b. container type, size, ssl should be there
    c. if specific type, size, ssl are provided in configuratio, only fetch loads having one of those type/size/owner
3. Find combinations
    a. where container type/size/owner is matching for both containers
    b. the distance between import dropped location and empty pickup location should be < specified miles (default 10 miles)
    c. the FRD of import should fall between ERD & CutOff of Export (if FRD exists, if not this check will be skipped)
4. Prioritization
    a. if multiple match found for an Export, select the import with least proximity distance.
"""

import logging
from datetime import datetime, timedelta
from bson.objectid import ObjectId
from typing import Dict, List, Optional, Tuple

from app.mongo_services.load_service import get_loads
from app.utils.distance_calc import calculate_distance_between_locations
from app.services.common_service import get_carrier_preferences
logger = logging.getLogger(__name__)

# Define load projection fields for MongoDB queries
LOAD_PROJECTION = {
    "_id": 1,
    "reference_number": 1,
    "containerNo": 1,
    "status": 1,
    "loadStatus": 1,
    "carrier": 1,
    "type_of_load": 1,
    "containerType": 1,
    "containerSize": 1,
    "containerOwner": 1,
    "driverOrderId": 1,
    "freeReturnDate": 1,
    "containerAvailableDay": 1,
    "cutOff": 1,
    "consigneeInfo": 1,
    "allStatusDates": 1,
}

# Load status constants
IMPORT_STATUS = "DROPCONTAINER_DEPARTED"
EXPORT_STATUSES = ["PENDING", "AVAILABLE"]


async def get_available_empty_import_containers(
    carrier: str,
    container_types: List[str],
    container_sizes: List[str], 
    container_owners: List[str]
) -> List[Dict]:
    """
    Retrieve available empty import containers matching specified criteria.
    """
    try:
        query_filter = {
            "carrier": ObjectId(carrier),
            "status": IMPORT_STATUS,
            "isDeleted": False,
            "type_of_load": "IMPORT",
            "createdAt": { "$gte": datetime.now() - timedelta(days=45) },
            "containerType": {"$exists": True},
            "containerSize": {"$exists": True},
            "containerOwner": {"$exists": True},
            "driverOrderId.prevType": "DELIVERLOAD",
            "isReadyForPickup": True,
        }

        # Add container attribute filters if provided
        for attr, values in [
            ("containerType", container_types),
            ("containerSize", container_sizes),
            ("containerOwner", container_owners)
        ]:
            if values:
                query_filter[attr] = {
                    "$in": [
                        ObjectId(val) if isinstance(val, str) else val 
                        for val in values
                    ]
                }

        return await get_loads(query_filter, LOAD_PROJECTION, 1000)

    except Exception as err:
        logger.error("Failed to get available empty import containers: %s", str(err))
        raise Exception(f"Failed to get available empty import containers: {str(err)}")


async def get_upcoming_export_loads(
    carrier: str,
    container_types: List[str],
    container_sizes: List[str],
    container_owners: List[str]
) -> List[Dict]:
    """
    Retrieve upcoming export loads matching specified criteria.
    """
    try:
        query_filter = {
            "carrier": ObjectId(carrier),
            "status": {"$in": EXPORT_STATUSES},
            "isDeleted": False,
            "type_of_load": "EXPORT",
            "createdAt": { "$gte": datetime.now() - timedelta(days=45) },
            "containerType": {"$exists": True},
            "containerSize": {"$exists": True},
            "containerOwner": {"$exists": True},
            "$or": [
                { "containerAvailableDay": { "$exists": True } },
                { "cutOff": { "$exists": True } }
            ]
        }

        # Add container attribute filters if provided
        for attr, values in [
            ("containerType", container_types),
            ("containerSize", container_sizes),
            ("containerOwner", container_owners)
        ]:
            if values:
                query_filter[attr] = {
                    "$in": [
                        ObjectId(val) if isinstance(val, str) else val 
                        for val in values
                    ]
                }

        return await get_loads(query_filter, LOAD_PROJECTION, 1000)

    except Exception as err:
        logger.error("Failed to get upcoming export loads: %s", str(err))
        raise Exception(f"Failed to get upcoming export loads: {str(err)}")


def find_best_import_match(
    user_payload: Dict,
    imports: List[Dict],
    import_dates: Dict,
    earliest_return_date: datetime,
    warehouse_location: Dict,
    delivery_radius: int
) -> Optional[Dict]:
    """
    Find the closest valid import container meeting all matching criteria.

    Args:
        imports: List of potential import containers
        import_dates: Pre-calculated import free return dates
        earliest_return_date: Export load earliest return date
        warehouse_location: Export load pickup location
        delivery_radius: Maximum allowed distance

    Returns:
        Best matching import container or None if no valid match found
    """
    min_distance = float('inf')
    best_import = None

    for empty_import in imports:
        # Check date constraint first (cheap operation)
        if earliest_return_date and import_dates.get(empty_import["_id"]) and import_dates[empty_import["_id"]] < earliest_return_date:
            continue

        # Calculate distance only if date constraint passes
        drop_location = empty_import["driverOrderId"]["address"]
        distance = calculate_distance_between_locations(
            drop_location,
            warehouse_location,
            user_payload['distanceUnit']
        )

        if distance <= delivery_radius and distance < min_distance:
            min_distance = distance
            best_import = empty_import

    return best_import


async def generate_street_turns_recommendation(
    user_payload: Dict,
    container_types: List[str],
    container_sizes: List[str],
    container_owners: List[str],
    arrival_threshold: int,
    delivery_radius: int
) -> List[Dict]:
    """
    Generate optimized street turn recommendations by matching empty imports with export loads.

    The function implements a multi-step matching process:
    1. Fetch available empty imports and upcoming exports
    2. Pre-filter and index containers by attributes for efficient lookup
    3. Match containers based on attributes, location proximity and timing constraints
    4. Prioritize matches by minimizing travel distance

    Args:
        user_payload: User context information including carrier
        container_types: Acceptable container types to consider
        container_sizes: Acceptable container sizes to consider
        container_owners: Acceptable container owners to consider
        arrival_threshold: Time threshold for container arrival (minutes)
        delivery_radius: Maximum distance radius for matches (miles)

    Returns:
        List of recommended street turn matches containing export and import load details

    Raises:
        Exception: If recommendation generation fails with detailed error message
    """
    try:
        carrier = user_payload.get("carrier")
        if not carrier:
            raise ValueError("Missing required carrier information in user payload")
        
        carrier_preferences = await get_carrier_preferences(carrier)
        user_payload['distanceUnit'] = carrier_preferences.get('distanceUnit', 'mi')

        # Fetch export loads and extract unique container attributes
        export_loads = await get_upcoming_export_loads(
            carrier, container_types, container_sizes, container_owners
        )
        
        # Extract unique container attributes from exports
        container_attrs = {
            "types": {load["containerType"] for load in export_loads},
            "sizes": {load["containerSize"] for load in export_loads},
            "owners": {load["containerOwner"] for load in export_loads}
        }

        # Fetch matching import containers
        empty_imports = await get_available_empty_import_containers(
            carrier,
            list(container_attrs["types"]),
            list(container_attrs["sizes"]), 
            list(container_attrs["owners"])
        )

        # Index imports by container attributes for efficient lookup
        filtered_imports: Dict[Tuple[str, str, str], List[Dict]] = {}
        for empty_import in empty_imports:
            key = (
                empty_import["containerSize"],
                empty_import["containerType"],
                empty_import["containerOwner"]
            )
            filtered_imports.setdefault(key, []).append(empty_import)

        # Pre-calculate dates to avoid repeated parsing
        import_dates = {}
        for empty_import in empty_imports:
            free_return_date = datetime.fromisoformat(empty_import["freeReturnDate"]) if empty_import.get("freeReturnDate") else None
            delivered_date = datetime.fromisoformat(empty_import["allStatusDates"]["deliverContainerDeparted"]) if empty_import.get("allStatusDates", {}).get("deliverContainerDeparted") else None
            import_dates[empty_import["_id"]] = free_return_date if free_return_date and delivered_date and free_return_date > delivered_date else None

        street_turn_recommendations = []

        # Match exports with closest valid import
        for export_load in export_loads:
            try:
                key = (
                    export_load["containerSize"],
                    export_load["containerType"],
                    export_load["containerOwner"]
                )

                if key not in filtered_imports:
                    continue

                earliest_return_date = datetime.fromisoformat(export_load["containerAvailableDay"]) if export_load.get("containerAvailableDay") else None
                warehouse_location = export_load["consigneeInfo"]["address"]

                # Find closest valid import meeting all constraints
                best_match = find_best_import_match(
                    user_payload,
                    filtered_imports[key],
                    import_dates,
                    earliest_return_date,
                    warehouse_location,
                    delivery_radius
                )

                if best_match:
                    street_turn_recommendations.append({
                        "export_load": export_load.get('reference_number'),
                        "empty_import": best_match.get('reference_number')
                    })
                    filtered_imports[key].remove(best_match)
            
            except Exception as err:
                logger.error("Failed to generate street turns recommendation: %s", str(err))
                continue

        return street_turn_recommendations

    except Exception as err:
        logger.error("Failed to generate street turns recommendation: %s", str(err))
        raise Exception(f"Failed to generate street turns recommendation: {str(err)}")
