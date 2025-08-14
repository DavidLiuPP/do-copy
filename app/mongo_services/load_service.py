"""
Load service module for retrieving available loads from MongoDB.

This module provides functionality to fetch available loads for a specific carrier,
implementing proper error handling, input validation and performance optimization.
"""
import logging
import copy
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from bson.objectid import ObjectId
from app.utils.common_utils import flatten_bson
from app.modules.optimizer.constants import DROPPED_LOADED, DROPPED_EMPTY
from app.mongo_services.mongo_service import get_loads_collection, get_charge_group_collection
from app.utils.distance_calc import calculate_distance_between_locations
from app.mongo_services.constant import LOAD_PROJECTION, DISPATCHED_STATUSES, LOAD_PROJECTION_FOR_OPTIMIZER

logger = logging.getLogger(__name__)

async def get_loads(criteria: Dict[str, Any], projection: Dict[str, Any] = LOAD_PROJECTION, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Generic function to retrieve loads from MongoDB based on provided criteria.

    Args:
        criteria (Dict[str, Any]): Query criteria/filter to apply
        limit (int, optional): Maximum number of loads to return. Defaults to 10.

    Returns:
        List[Dict[str, Any]]: List of load documents matching the criteria

    Raises:
        Exception: For database operation errors
    """
    try:
        # Get MongoDB collection
        loads_collection = await get_loads_collection()

        # Execute query with provided criteria and projection
        cursor = loads_collection.find(
            criteria,
            projection
        ).limit(limit)

        # Convert cursor to list
        loads = await cursor.to_list(length=limit)
        return flatten_bson(loads)

    except Exception as e:
        logger.error(f"Error retrieving loads from database: {str(e)}")
        raise


async def get_loadcharges(criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generic function to retrieve load charges from MongoDB based on provided criteria.

    Args:
        criteria (Dict[str, Any]): Query criteria/filter to apply

    Returns:
        List[Dict[str, Any]]: List of load charge documents matching the criteria

    Raises:
        Exception: For database operation errors
    """
    try:
        # Get MongoDB collection
        loadcharges_collection = await get_charge_group_collection()

        # Execute query with provided criteria and projection
        cursor = loadcharges_collection.find(
            criteria,
            {
                "loadId": 1,
                "totalAmount": 1,
                "totalAmountWithTax": 1
            }
        )

        # Convert cursor to list
        loadcharges = await cursor.to_list()
        return flatten_bson(loadcharges)

    except Exception as e:
        logger.error(f"Error retrieving load charges from database: {str(e)}")
        raise


async def get_active_loads(
    carrier: str,
    limit: int = 10,
    plan_branch: list = [],
    options: dict = {},
) -> List[Dict[str, Any]]:
    """
    Retrieve active loads for a specific carrier from MongoDB.
    """
    try:

        load_statuses = DISPATCHED_STATUSES
        if options.get('ignored_pending_loads', False):
            load_statuses = [status for status in DISPATCHED_STATUSES if status != 'PENDING']   

        # Build query filter
        query_filter = {
            "carrier": ObjectId(carrier),
            "status": { "$in": load_statuses },
            "type_of_load": { "$in": ["IMPORT", "EXPORT"] },
            "createdAt": { "$gte": datetime.now() - timedelta(days=90) },
            "isDeleted": False
        }

        if plan_branch:
            query_filter["terminal"] = {
                "$in": [ObjectId(branch) for branch in plan_branch]
            }

        if options.get('scheduled_moves_only', False):
            query_filter['$or'] = [
                { "pickupTimes.pickupFromTime": { "$exists": True, "$ne": None } },
                { "deliveryTimes.deliveryFromTime": { "$exists": True, "$ne": None } },
                { "returnFromTime": { "$exists": True, "$ne": None } },
            ]

        # Execute query and get loads
        loads = await get_loads(query_filter, LOAD_PROJECTION, limit)

        # Get all loadIds from the loads
        load_ids = [ObjectId(load["_id"]) for load in loads]

        # Find charge_groups with matching loadIds and populate the load data
        pipeline = {
            "carrier": ObjectId(carrier),
            "loadId": {"$in": load_ids},
            "isDefault": True
        }
        charge_groups = await get_loadcharges(pipeline)

        for load in loads:
            load_id = load["_id"]
            for charge_group in charge_groups:
                if charge_group["loadId"] == load_id:
                    load["revenue"] = charge_group["totalAmountWithTax"]
        
        return loads

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error retrieving loads for carrier {carrier}: {str(e)}")
        raise Exception(f"Failed to retrieve available loads: {str(e)}")


async def get_loads_with_reference_numbers(
    carrier: str,
    reference_numbers: List[str],
    plan_branch: list = [],
    projection = LOAD_PROJECTION,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Get loads with reference numbers.
    """
    try:
        # if load_numbers is present get the load from that list
        query_filter = {
            "reference_number": { "$in": reference_numbers },
            "carrier": ObjectId(carrier),
            "isDeleted": False,
        }

        if len(plan_branch) > 0:
            query_filter["terminal"] = {
                "$in": [ObjectId(branch) for branch in plan_branch]
            }

        loads = await get_loads(query_filter, projection, limit)
        
        # Get all loadIds from the loads
        load_ids = [ObjectId(load["_id"]) for load in loads]

        # Find charge_groups with matching loadIds and populate the load data
        pipeline = {
            "carrier": ObjectId(carrier),
            "loadId": {"$in": load_ids},
            "isDefault": True
        }
        charge_groups = await get_loadcharges(pipeline)

        for load in loads:
            load_id = load["_id"]
            for charge_group in charge_groups:
                if charge_group["loadId"] == load_id:
                    load["revenue"] = charge_group["totalAmountWithTax"]

        return loads
    
    except Exception as e:
        logger.error(f"Error getting loads with reference numbers: {str(e)}")
        raise Exception(f"Failed to get loads with reference numbers: {str(e)}")


async def get_available_loads(
    carrier: str,
    reference_numbers: List[str],
    plan_branch: list = [],
    limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Get available loads for a given carrier.
    """
    try:
        loads = await get_loads_with_reference_numbers(
            carrier=carrier,
            reference_numbers=reference_numbers,
            plan_branch=plan_branch,
            projection=LOAD_PROJECTION_FOR_OPTIMIZER,
            limit=limit
        )

        # filter loads that are available or ready to be returned
        filtered_loads = []
        for load in loads:
            if load['status'] == 'PENDING':
                continue
            if load['status'] == 'DROPCONTAINER_DEPARTED' and load.get('driverOrderId', {}).get('prevType') == 'DELIVERLOAD':
                if load['type_of_load'] == 'IMPORT' and load['loadStatus'] == DROPPED_LOADED:
                    continue
                if load['type_of_load'] == 'EXPORT' and load['loadStatus'] == DROPPED_EMPTY:
                    continue
            filtered_loads.append(load)

        for load in filtered_loads:
            moves = get_moves_from_driver_order(load.get('driverOrder', []))
            matched_moves = []

            for move in moves:
                assigned_move = False
                move_id = next((event.get('moveId') for event in move if not event.get('isVoidOut')), None)
                
                for event in move:
                    if event.get('driver') and not event.get('isVoidOut'):
                        assigned_move = True
                        break

                if assigned_move:
                    matched_moves.append(move_id)

            driver_order = load.get('driverOrder', [])
            for event in driver_order:
                if event.get('moveId') in matched_moves:
                    event['is_manually_planned'] = True

            load['driverOrder'] = driver_order

        return filtered_loads
    
    except Exception as e:
        logger.error(f"Error getting available loads: {str(e)}")
        raise Exception(f"Failed to get available loads: {str(e)}")


def is_move_ending_event(routing_event: Optional[Dict], next_event: Optional[Dict]) -> bool:
    """
    Check if the current routing event marks the end of a move sequence.
    
    Args:
        routing_event: Current routing event dictionary
        next_event: Next routing event dictionary in sequence
        
    Returns:
        bool: True if current event ends a move sequence, False otherwise
    """
    if not routing_event or not next_event:
        return False
        
    tms_end_move_events = [
        'DROPCONTAINER',
        'LIFTOFF', 
        'CHASSISTERMINATION'
    ]
    
    return (not routing_event.get('isVoidOut', False) and
            routing_event.get('type') in tms_end_move_events and
            next_event.get('type') not in tms_end_move_events)


def get_moves_from_driver_order(driver_order, options=None):
    """
    Extract routing moves from driver order sequence.
    
    Args:
        driver_order: List of driver order events
        options: Dict with filtering options:
            - exclude_void_out: Skip void out events
            - exclude_combined_move: Skip combined trip moves  
            - exclude_assigned_move: Skip moves assigned to drivers
            - exclude_started_move: Skip moves that have already started
            - exclude_completed_move: Skip moves that have already been completed
            - started_move_only: Only include moves that have already started
    Returns:
        Dict containing list of routing moves
    """
    if options is None:
        options = {}
        
    exclude_void_out = options.get('exclude_void_out', False)
    exclude_combined_move = options.get('exclude_combined_move', False) 
    exclude_assigned_move = options.get('exclude_assigned_move', False)
    exclude_started_move = options.get('exclude_started_move', False)
    exclude_completed_move = options.get('exclude_completed_move', False)
    started_move_only = options.get('started_move_only', False)
    combined_move_only = options.get('combined_move_only', False)
    exclude_unplanned_completed_move = options.get('exclude_unplanned_completed_move', False)

    routing_moves = []
    current_pos = 0
    last_pos = 0

    if not driver_order:
        return {'routing_moves': []}

    # Create deep copy and add index
    _driver_order = copy.deepcopy(driver_order)
    for i, el in enumerate(_driver_order):
        el['d_index'] = i

    while current_pos < len(_driver_order):
        is_last_event = current_pos == len(_driver_order) - 1
        has_next = current_pos + 1 < len(_driver_order)
        
        if (is_last_event or 
            (has_next and is_move_ending_event(_driver_order[current_pos], 
                                             _driver_order[current_pos + 1]))):
            
            _move = _driver_order[last_pos:current_pos + 1]
            
            if exclude_void_out:
                _move = [event for event in _move if not event.get('isVoidOut')]
                
            is_combined_trip = any(event.get('combineTripId') and not event.get('isDualTransaction') for event in _move)
            is_assigned_move = any((event.get('driver') or event.get('drayos_carrier')) and not event.get('isVoidOut') for event in _move)
            is_started_move = any(event.get('arrived', None) for event in _move)
            is_completed_move = all(event.get('departed', None) for event in _move)
            is_manually_planned = all(event.get('is_manually_planned', False) for event in _move)
            should_include = True
            
            if exclude_combined_move:
                should_include = should_include and not is_combined_trip
            
            if exclude_assigned_move:
                should_include = should_include and not is_assigned_move
            
            if exclude_started_move:
                should_include = should_include and not is_started_move
            
            if exclude_completed_move:
                should_include = should_include and not is_completed_move

            if exclude_unplanned_completed_move:
                should_include = should_include and (is_manually_planned or not is_completed_move)

            if started_move_only:
                should_include = should_include and is_started_move
            
            if combined_move_only:
                should_include = should_include and is_combined_trip
                                
            if should_include:
                routing_moves.append(_move)
                
            last_pos = current_pos + 1
            
        current_pos += 1
        
    return routing_moves


async def get_assigned_moves(
    user_payload: Dict[str, Any],
    from_time: datetime,
    to_time: datetime,
    add_to_existing_plan: bool = False,
    plan_branch: list = [],
    plan_drivers = None,
    reference_numbers = []
) -> List[Dict[str, Any]]:
    """
    Get assigned moves for a given carrier and plan date.
    """
    try:
        carrier = user_payload.get('carrier')

        # if load_numbers is present get the load from that list
        query_filter = {
            "carrier": ObjectId(carrier),
            "status": { "$in": [*DISPATCHED_STATUSES, 'COMPLETED'] },
            "type_of_load": { "$in": ["IMPORT", "EXPORT"] },
            "createdAt": { "$gte": datetime.now() - timedelta(days=90) },
            "isDeleted": False,
            "driverOrder": {
                "$elemMatch": {
                    "$or": [
                        {
                            "loadAssignedDate": {
                                "$gte": from_time,
                                "$lt": to_time
                            },
                        },
                        {
                            "departed": {
                                "$gte": from_time,
                                "$lt": to_time
                            }
                        }
                    ],
                    "driver": (
                        { "$in": [ObjectId(driver) for driver in plan_drivers] }
                        if plan_drivers
                        else { "$ne": None }
                    ),
                    "isVoidOut": False
                }
            },
        }

        if plan_branch:
            query_filter["terminal"] = {
                "$in": [ObjectId(branch) for branch in plan_branch]
            }

        if len(reference_numbers) > 0:
            query_filter["reference_number"] = { "$in": reference_numbers }

        loads = await get_loads(query_filter, LOAD_PROJECTION, 1000)

        # Get all loadIds from the loads
        load_ids = [ObjectId(load["_id"]) for load in loads]

        # Find charge_groups with matching loadIds and populate the load data
        pipeline = {
            "carrier": ObjectId(carrier),
            "loadId": {"$in": load_ids},
            "isDefault": True
        }
        charge_groups = await get_loadcharges(pipeline)

        # Create lookup dict for charge groups to avoid nested loop
        charge_group_map = {str(cg["loadId"]): cg["totalAmountWithTax"] for cg in charge_groups}
        
        all_loads = []
        
        for load in loads:
            load_id = str(load["_id"])
            
            # Use dict lookup instead of loop
            if load_id in charge_group_map:
                load["revenue"] = charge_group_map[load_id]

            moves = get_moves_from_driver_order(load.get('driverOrder', []), { 'started_move_only': not add_to_existing_plan })
            
            # Pre-calculate date comparisons once
            matched_moves = []
            for move in moves:
                valid_move = False
                move_id = next((event.get('moveId') for event in move if not event.get('isVoidOut')), None)
                for event in move:
                    assigned_date = event.get('loadAssignedDate')
                    is_driver_assigned = event.get('driver')

                    if plan_drivers:
                        is_driver_assigned = event.get('driver') and event.get('driver') in plan_drivers

                    is_assigned_date_in_range = assigned_date and datetime.fromisoformat(assigned_date) >= from_time and datetime.fromisoformat(assigned_date) < to_time
                    is_departed_date_in_range = event.get('departed') and datetime.fromisoformat(event.get('departed')) >= from_time and datetime.fromisoformat(event.get('departed')) < to_time

                    if ((is_assigned_date_in_range or is_departed_date_in_range) and
                        not event.get('isVoidOut') and
                        is_driver_assigned): # TODO: filter by driver
                        valid_move = True
                        break
                if valid_move:
                    matched_moves.append(move_id)
            
            if len(matched_moves) == 0:
                continue

            driver_order = load.get('driverOrder', [])
            for event in driver_order:
                if event.get('moveId') in matched_moves:
                    event['is_manually_planned'] = True
            
            load['driverOrder'] = driver_order

            all_loads.append(load)

        return all_loads

    except Exception as e:
        logger.error(f"Error getting assigned moves: {str(e)}")
        raise Exception(f"Failed to get assigned moves: {str(e)}")
    
async def get_completed_moves(
    user_payload: Dict[str, Any],
    from_time: datetime,
    to_time: datetime,
    plan_branch: list = [],
    plan_drivers = None
) -> List[Dict[str, Any]]:
    """
    Get assigned moves for a given carrier and plan date.
    """
    try:
        carrier = user_payload.get('carrier')

        # if load_numbers is present get the load from that list
        query_filter = {
            "carrier": ObjectId(carrier),
            "driverOrder": {
                "$elemMatch": {
                    "departed": {
                        "$gte": from_time,
                        "$lt": to_time
                    },
                    "isVoidOut": False
                }
            },
            "isDeleted": False,
        }

        if plan_branch:
            query_filter["terminal"] = {
                "$in": [ObjectId(branch) for branch in plan_branch]
            }

        loads = await get_loads(query_filter, LOAD_PROJECTION, 1000)

        # Get all loadIds from the loads
        load_ids = [ObjectId(load["_id"]) for load in loads]

        # Find charge_groups with matching loadIds and populate the load data
        pipeline = {
            "carrier": ObjectId(carrier),
            "loadId": {"$in": load_ids},
            "isDefault": True
        }
        charge_groups = await get_loadcharges(pipeline)

        # Create lookup dict for charge groups to avoid nested loop
        charge_group_map = {str(cg["loadId"]): cg["totalAmountWithTax"] for cg in charge_groups}

        all_loads = []

        for load in loads:
            load_id = str(load["_id"])

            # Use dict lookup instead of loop
            if load_id in charge_group_map:
                load["revenue"] = charge_group_map[load_id]

            moves = get_moves_from_driver_order(load.get('driverOrder', []), { 'exclude_void_out': True, 'started_move_only': True })

            # Pre-calculate date comparisons once
            matched_moves = []
            for move in moves:
                event_completed_today_count = 0
                move_id = next((event.get('moveId') for event in move if not event.get('isVoidOut')), None)
                for event in move:
                    departed = event.get('departed')
                    if (departed and
                        datetime.fromisoformat(departed) >= from_time and
                        datetime.fromisoformat(departed) < to_time and
                        not event.get('isVoidOut') and
                        (not plan_drivers or event.get('driver') in plan_drivers)):
                        event_completed_today_count += 1

                valid_move = (event_completed_today_count / len(move)) == 1
                if valid_move:
                    is_any_event_not_departed = any(not event.get('departed') for event in move)
                    if not is_any_event_not_departed:
                        matched_moves.append(move_id)

            if len(matched_moves) == 0:
                continue

            driver_order = load.get('driverOrder', [])
            pickup_event = None
            for index, event in enumerate(driver_order):
                if event.get('moveId') in matched_moves:
                    event['is_completed_move'] = True

                if event.get('type') == 'PULLCONTAINER':
                    pickup_event = event

                if event.get('type') == 'RETURNCONTAINER' and not event.get('customerId'):
                    event['customerId'] = pickup_event.get('customerId')
                    event['company_name'] = pickup_event.get('company_name')
                    event['address'] = pickup_event.get('address')
                    event['city'] = pickup_event.get('city', '')
                    event['state'] = pickup_event.get('state', '')
                    event['country'] = pickup_event.get('country', '')
                    event['zip_code'] = pickup_event.get('zip_code', '')
                    event['distance'] = calculate_distance_between_locations(
                        driver_order[index - 1].get('address', {}),
                        event.get('address', {})
                    )

            load['driverOrder'] = driver_order

            all_loads.append(load)

        return all_loads

    except Exception as e:
        logger.error(f"Error getting assigned moves: {str(e)}")
        raise Exception(f"Failed to get assigned moves: {str(e)}")