import logging
import json
import pytz
from typing import Dict, Any, List
from datetime import datetime, timedelta
from bson.objectid import ObjectId
from app.postgres_connection import PostgresConnection
from app.services.redis_service import get_default_yard_location
from app.services.common_service import get_time_zone, get_carrier_preferences
from app.utils.distance_calc import calculate_distance_between_locations
from app.mongo_services.load_service import get_loads_collection
from app.postgres_services.driver_service import get_drivers
from app.modules.optimizer.hos_service import get_hos_data_for_drivers
from app.modules.optimizer.driver_plan import get_planning_time_windows
from app.mongo_services.load_service import get_completed_moves, get_moves_from_driver_order
from app.modules.scheduler.utility import map_loads_for_scheduler
from app.postgres_services.configurations_service import get_equipment_validations, get_container_sizes, get_container_types
from vrp_optimizer.helpers import get_chassis_activity_between_moves
from vrp_optimizer.assumptions import CHASSIS_ACTIVITIES

from app.modules.optimizer.map_loads_optimizer_service import map_actionable_moves
from load_optimizer.get_optimal_plan_v3 import get_optimal_plan_v3

logger = logging.getLogger(__name__)

async def get_driver_plan(carrier: str, plan_id: str):
    """
    Get the driver plan
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        # Find the move to remove
        async with pool.acquire() as conn:
            optimizer_loads = await conn.fetch(
                """
                SELECT * FROM optimizer_loads
                WHERE carrier = $1
                    AND plan_id = $2
                    AND is_deleted = false
                """,
                carrier,
                plan_id
            )

            if not optimizer_loads:
                raise ValueError(f"Moves not found in plan with ID {plan_id}")

            optimizer_loads = [dict(row) for row in optimizer_loads]

        return optimizer_loads

    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to get driver plan: {str(e)}")

async def get_optimizer_plan_detail(carrier: str, plan_id: str):
    """
    Get the optimizer plan details
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        # Find the move to remove
        async with pool.acquire() as conn:
            optimizer_plan = await conn.fetch(
                """
                SELECT * FROM optimizer_plans
                WHERE carrier = $1
                    AND id = $2
                """,
                carrier,
                plan_id
            )

            if not optimizer_plan:
                raise ValueError(f"Moves not found in plan with ID {plan_id}")

            optimizer_plan = [dict(row) for row in optimizer_plan]

        return optimizer_plan[0]

    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to get driver plan: {str(e)}")


def group_and_sort_plan_by_driver(plan):
    # Group plan by assigned driver
    plan_by_driver = {plan.get('driver'): [] for plan in plan}
    [plan_by_driver[plan.get('driver')].append(plan) for plan in plan]
    
    # Sort plan by move_start_time for each driver
    for driver in plan_by_driver:
        plan_by_driver[driver].sort(key=lambda x: x.get('move_start_time') if x.get('move_start_time') else datetime.max)
    return plan_by_driver


def create_plan_detailed_entries(plan_by_driver, tz, driver_features):
    try:
        detailed_entries = []
        for driver in plan_by_driver:
            # get driver details from driver features
            driver_feature = next((df for df in driver_features if df and df.get('_id') == driver), None)
            if not driver_feature:
                continue
                
            default_yard_location = {
                'lat': driver_feature.get('depot_location', {}).get('lat', 0),
                'lng': driver_feature.get('depot_location', {}).get('lng', 0),
                'address': driver_feature.get('depot_location', {}),
                'company_name': 'Yard'
            }
            last_event = default_yard_location
            for index, move in enumerate(plan_by_driver[driver]):
                load_events = json.loads(move['move'])
                for e_index, event in enumerate(load_events):
                    empty_miles = 0
                    trip_miles = event['distance']

                    if index == 0 and e_index == 0:
                        empty_miles = calculate_distance_between_locations(default_yard_location, event['address'])
                        trip_miles = 0
                    elif index == len(plan_by_driver[driver]) - 1 and e_index == len(load_events) - 1:
                        empty_miles = calculate_distance_between_locations(event['address'], default_yard_location)
                    elif e_index == 0:
                        empty_miles = calculate_distance_between_locations(last_event['address'], event['address'])
                        trip_miles = 0

                    try:
                        detailed_entries.append({
                            "driver": move.get('driver'),
                            "reference_number": move.get('reference_number'),
                            "type": event['type'],
                            "from": last_event['company_name'],
                            "to": event['company_name'],
                            "enroute": datetime.fromisoformat(event['enroute']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                            "arrived": datetime.fromisoformat(event['arrived']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                            "departed": datetime.fromisoformat(event['departed']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                            "trip_miles": int(trip_miles),
                            "empty_miles": int(empty_miles),
                        })
                        last_event = event
                    except Exception as e:
                        logger.error(e)
                        continue
        return detailed_entries
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to create plan detailed entries: {str(e)}")

async def get_driver_plan_stats_controller(user_payload: dict, plan_id: str):
    """
    Get the stats for the driver plan
    """
    try:
        # Validate inputs
        carrier = user_payload.get('carrier')
        timeZone = await get_time_zone(carrier)
        tz = pytz.timezone(timeZone)

        user_payload['timeZone'] = timeZone

        optimizer_plan_detail = await get_optimizer_plan_detail(carrier, plan_id)

        plan_date = optimizer_plan_detail.get('plan_date').isoformat()
        converted_plan_date = tz.localize(datetime.strptime(plan_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0))

        optimal_plan = await get_driver_plan(carrier, plan_id)

        # get all drivers from driver features
        driver_features = await get_drivers(carrier, converted_plan_date, [], {'exclude_account_hold': True})

        total_prepulls = 0
        for load in optimal_plan:
            load_events = json.loads(load.get('move'))
            if len(load_events) == 2:
                if load_events[0].get('type') == 'PULLCONTAINER' and load_events[1].get('type') in ['DROPCONTAINER', 'LIFTOFF']:
                    total_prepulls += 1

        optimal_plan_by_driver = group_and_sort_plan_by_driver(optimal_plan)
        
        optimal_plan_detailed_entries = create_plan_detailed_entries(optimal_plan_by_driver, tz, driver_features)

        # get the stats for the plan
        optimal_total_moves = len(optimal_plan)
        optimal_total_drivers = len(optimal_plan_by_driver)
        optimal_empty_miles = sum(entry['empty_miles'] for entry in optimal_plan_detailed_entries)
        optimal_trip_miles = sum(entry['trip_miles'] for entry in optimal_plan_detailed_entries)
        optimal_total_miles = optimal_empty_miles + optimal_trip_miles

        total_pulls = len([entry for entry in optimal_plan_detailed_entries if entry['type'] == 'PULLCONTAINER'])
        total_deliveries = len([entry for entry in optimal_plan_detailed_entries if entry['type'] == 'DELIVERLOAD'])
        total_returns = len([entry for entry in optimal_plan_detailed_entries if entry['type'] == 'RETURNCONTAINER'])

        actual_loads_appt = await get_actual_loads_appt(carrier, converted_plan_date)

        overall_stats = {
            "total_moves": optimal_total_moves,
            "total_drivers": optimal_total_drivers,
            "total_empty_miles": optimal_empty_miles,
            "total_trip_miles": optimal_trip_miles,
            "total_miles": optimal_total_miles,
            "total_pulls": total_pulls,
            "total_deliveries": total_deliveries,
            "total_returns": total_returns,
            "total_prepulls": total_prepulls,
            "actual_delivery_appt": actual_loads_appt.get('deliveryCount'),
            "actual_pickup_appt": actual_loads_appt.get('pickupCount'),
            "actual_return_appt": actual_loads_appt.get('returnCount'),
        }

        return {
            "status": "success",
            "message": "Driver plan stats retrieved successfully",
            "stats": overall_stats
        }
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to get driver plan stats: {str(e)}")

async def get_actual_loads_appt(carrier: str, plan_Date: datetime):
    """
    Get the actual loads appointments
    """
    try:
        start_time = plan_Date
        end_time = plan_Date + timedelta(days=1)

        load_collection = await get_loads_collection()
        find_aggregation = [
            {
                "$match": {
                    "carrier": ObjectId(carrier),
                    "$or": [
                        {
                            "pickupTimes.0.pickupFromTime": {
                                "$gte": start_time,
                                "$lt": end_time
                            }
                        },
                        {
                            "deliveryTimes.0.deliveryFromTime": {
                                "$gte": start_time,
                                "$lt": end_time
                            }
                        },
                        {
                            "returnFromTime": {
                                "$gte": start_time,
                                "$lt": end_time
                            }
                        }
                    ]
                }
            },
            {
                "$project": {
                    "pickupTimes": { "$arrayElemAt": ["$pickupTimes", 0] },
                    "deliveryTimes": { "$arrayElemAt": ["$deliveryTimes", 0] },
                    "returnFromTime": 1
                }
            },
            {
                "$project": {
                    "pickupMatched": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gte": ["$pickupTimes.pickupFromTime", start_time]},
                                    {"$lt": ["$pickupTimes.pickupFromTime", end_time]}
                                ]
                            }, 1, 0
                        ]
                    },
                    "deliveryMatched": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gte": ["$deliveryTimes.deliveryFromTime", start_time]},
                                    {"$lt": ["$deliveryTimes.deliveryFromTime", end_time]}
                                ]
                            }, 1, 0
                        ]
                    },
                    "returnMatched": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gte": ["$returnFromTime", start_time]},
                                    {"$lt": ["$returnFromTime", end_time]}
                                ]
                            }, 1, 0
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "pickupCount": {"$sum": "$pickupMatched"},
                    "deliveryCount": {"$sum": "$deliveryMatched"},
                    "returnCount": {"$sum": "$returnMatched"}
                }
            }
        ]

        loads = load_collection.aggregate(find_aggregation)

        loads = await loads.to_list(length=1000)

        return loads[0]
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to get actual loads appointments: {str(e)}")

async def map_loads_for_reporting(
    user_payload: Dict[str, Any],
    loads: List[Dict[str, Any]],
    converted_plan_date: datetime
):
    try:
        loads_with_actionable_moves = []

        for load in loads:
            # find actionable move from the load
            driver_order = load.get('driverOrder', None)
            moves = get_moves_from_driver_order(driver_order, {
                    "exclude_void_out": True,
                    "exclude_combined_move": True,
                })
            if not len(moves):
                continue


            actionable_moves = [
                move for move in moves 
                if any(event.get('is_completed_move') for event in move)
            ]
            if not actionable_moves:
                continue

            for move_index, actionable_move in enumerate(actionable_moves):
                load_copy = load.copy()
                del load_copy['driverOrder']

                load_copy['is_completed_move'] = True
                load_copy['is_manually_planned'] = False
                load_copy['is_assigned_move'] = True
                load_copy['assigned_driver'] = actionable_move[0].get('driver')
                load_copy['move'] = actionable_move
                load_copy['move_index'] = move_index
                loads_with_actionable_moves.append(load_copy)

        mapped_actionable_moves, _ = await map_actionable_moves(
            user_payload, 
            loads_with_actionable_moves, 
            converted_plan_date,
            options={
                "time_prediction": False
            }
        )

        return mapped_actionable_moves

    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to map loads for optimizer: {str(e)}")

def group_and_sort_plan_by_driver(plan):
    # Group plan by assigned driver
    plan_by_driver = {plan.get('assigned_driver'): [] for plan in plan}
    [plan_by_driver[plan.get('assigned_driver')].append(plan) for plan in plan]

    # Sort plan by enroute_time for each driver
    for driver in plan_by_driver:
        plan_by_driver[driver].sort(key=lambda x: x.get('enroute_time') if x.get('enroute_time') else datetime.max)
    return plan_by_driver



def create_plan_detailed_entries_compare(
    carrier,
    default_yard_locations,
    plan_by_driver,
    tz,
    driver_features,
    appointment_dict,
    container_sizes_map,
    container_types_map,
    equipment_validations
):
    try:
        detailed_entries = []
        for driver in plan_by_driver:
            driver_feature = next((df for df in driver_features if df and df.get('_id') == driver), {})
            depot_customer_id = driver_feature.get('depot_customer_id', '')
            default_yard_location = next((location for location in default_yard_locations if location.get('customerId') == depot_customer_id), None)
            if not default_yard_location:
                continue

            last_event = default_yard_location
            for index, move in enumerate(plan_by_driver[driver]):
                for e_index, event in enumerate(move['move']):
                    empty_miles = 0
                    trip_miles = event['distance']

                    event['container_size_label'] = container_sizes_map.get(move.get('containerSize'), {}).get('label', '')
                    event['container_type_label'] = container_types_map.get(move.get('containerType'), {}).get('label', '')

                    # start of move
                    if index == 0 and e_index == 0:
                        empty_miles = (
                            calculate_distance_between_locations(
                                default_yard_location.get('address'),
                                event.get('address')
                            ) if event.get('address') else 0
                        )
                        trip_miles = 0
                    # end of move
                    elif index == len(plan_by_driver[driver]) - 1 and e_index == len(move['move']) - 1:
                        empty_miles = (
                            calculate_distance_between_locations(
                                event.get('address'),
                                default_yard_location.get('address')
                            ) if event.get('address') else 0
                        )
                    
                    # middle of the move
                    elif e_index == 0:
                        source = {
                            'isDepot': False,
                            'is_free_flow_move': False,
                            'end_event': last_event,
                            'end_loc': [last_event.get('address').get('lat'), last_event.get('address').get('lng')],
                            'container_size_label': last_event.get('container_size_label', ''),
                            'container_type_label': last_event.get('container_type_label', ''),
                        }
                        destination = {
                            'isDepot': False,
                            'is_free_flow_move': False,
                            'start_event': event,
                            'start_loc': [event.get('address').get('lat'), event.get('address').get('lng')],
                            'container_size_label': event.get('container_size_label', ''),
                            'container_type_label': event.get('container_type_label', ''),
                        }
                        chassis_activity = get_chassis_activity_between_moves(
                            source,
                            destination,
                            equipment_validations,
                            default_yard_locations,
                            [],
                            carrier
                        )
                        is_chassis_activity_needed = chassis_activity.get('type') != CHASSIS_ACTIVITIES["NO_ACTION"]

                        if not is_chassis_activity_needed:
                            empty_miles = calculate_distance_between_locations(
                                last_event.get('address'),
                                event.get('address')
                            )
                        else:
                            # Calculate distance and time metrics for chassis activity
                            locations = [last_event.get('address')]
                            if chassis_activity.get('drop_yard'):
                                locations.append({
                                    'lat': chassis_activity.get('drop_yard').get('location')[0],
                                    'lng': chassis_activity.get('drop_yard').get('location')[1]
                                })
                            if chassis_activity.get('hook_yard'):
                                locations.append({
                                    'lat': chassis_activity.get('hook_yard').get('location')[0],
                                    'lng': chassis_activity.get('hook_yard').get('location')[1]
                                })
                            locations.append(event.get('address'))

                            empty_miles = 0
                            for i in range(len(locations) - 1):
                                empty_miles += calculate_distance_between_locations(
                                    locations[i],
                                    locations[i + 1]
                                )

                        trip_miles = 0

                    try:
                        detailed_entries.append({
                            "_id": event.get('_id'),
                            "driver_id": move.get('assigned_driver'),
                            "driver": move.get('assigned_driver_name'),
                            "reference_number": move.get('reference_number'),
                            "type": event['type'],
                            "from": last_event.get('company_name', ''),
                            "to": event.get('company_name', ''),
                            "enroute": datetime.fromisoformat(event['enroute']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                            "arrived": datetime.fromisoformat(event['arrived']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                            "departed": datetime.fromisoformat(event['departed']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                            'appointment': appointment_dict.get(move.get('load_id'), {}).get(event['type'], ""),
                            "trip_miles": int(trip_miles),
                            "empty_miles": int(empty_miles) if empty_miles < 500 else 30,
                        })

                        last_event = event
                    except Exception as e:
                        logger.error(e)
                        continue
        return detailed_entries
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to create plan detailed entries for comparison: {str(e)}")


async def get_container_sizes_and_types_labels(carrier: str, moves: List[Dict[str, Any]]):
    container_sizes = list(set([m.get('containerSize') for m in moves if m.get('containerSize')]))
    container_types = list(set([m.get('containerType') for m in moves if m.get('containerType')]))

    if not container_sizes and not container_types:
        return moves

    container_sizes = await get_container_sizes(carrier, container_sizes)
    container_types = await get_container_types(carrier, container_types)

    container_sizes_map = {c['_id']: c for c in container_sizes}
    container_types_map = {c['_id']: c for c in container_types}

    return container_sizes_map, container_types_map


def find_unplanned_moves(actionable_moves: list, optimal_plan: list) -> list:
    """
    Compares the list of actionable moves against the final optimal plan to find
    which moves were not scheduled.
    Args:
        actionable_moves (list): The original list of all moves that were sent
                                 to the optimizer. Each item is a dictionary
                                 with a 'reference_number' and '_id' (load_id).
        optimal_plan (list): The list of moves that were successfully scheduled
                             by the optimizer. Each item is a dictionary with a
                             'move' array containing moveId.
    Returns:
        list: A list of move dictionaries from actionable_moves that were not
              included in the optimal_plan.
    """
    # Build a set of all moveIds from the optimal plan
    planned_move_ids = set()
    for driver_assignment in optimal_plan:
        moves = driver_assignment.get('move', [])
        if moves and isinstance(moves, list):
            for move in moves:
                move_id = move.get('moveId')
                if move_id:
                    planned_move_ids.add(move_id)

    # Find all actionable moves whose moveId is not in the set of planned moveIds
    unplanned_moves = []
    for load in actionable_moves:
        # Get the unique identifier for this load (should match moveId in optimal plan)
        # For actionable moves, get moveId from the move array
        moves = load.get('move', [])
        if moves and isinstance(moves, list) and len(moves) > 0:
            move_id = moves[0].get('moveId')
            if move_id and move_id not in planned_move_ids:
                unplanned_moves.append(load)
        else:
            # If no move array or empty, treat as unplanned
            unplanned_moves.append(load)

    return unplanned_moves


async def compare_optimiser_plan(user_payload: Dict[str, Any], plan_date: str) -> Dict[str, str]:
    """
    Compare the optimiser plan with the original events and return the delta
    Args:
        user_payload: Dictionary containing user payload
        plan_date: Date of the plan to compare
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Validate inputs
        carrier = user_payload.get('carrier')
        plan_branch = user_payload.get('plan_branch', [])
        shift = user_payload.get('shift')

        if not carrier:
            raise ValueError("Carrier ID is required")
        try:
            datetime.strptime(plan_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Invalid plan_date format. Expected YYYY-MM-DD")
        
        timeZone = await get_time_zone(carrier)
        tz = pytz.timezone(timeZone)
        converted_plan_date = tz.localize(datetime.strptime(plan_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0))

        # get default yard location
        default_yard_locations = await get_default_yard_location(carrier)
        # for each yard location, add start_loc and end_loc
        for location in default_yard_locations:
            depot_customer_id = location.get('customerId')
            location['start_loc'] = [location.get('address').get('lat'), location.get('address').get('lng')]
            location['end_loc'] = [location.get('address').get('lat'), location.get('address').get('lng')]
            location['depot_customer_id'] = depot_customer_id

        carrier_preferences = await get_carrier_preferences(carrier)
        user_settings = await get_equipment_validations(carrier)
        equipment_validations = user_settings.get('equipment_validations', [])

        planning_windows = await get_planning_time_windows(carrier, converted_plan_date, shift, plan_branch, timeZone)
        planning_minutes = planning_windows.get('plan_window') or [0, 1440]
        plan_from_time = converted_plan_date + timedelta(minutes=planning_minutes[0])
        plan_to_time = converted_plan_date + timedelta(minutes=planning_minutes[1])

        user_payload['default_yard_locations'] = default_yard_locations
        user_payload['timeZone'] = timeZone
        user_payload['distanceUnit'] = carrier_preferences.get('distanceUnit', 'mi')

        # get drivers
        drivers = await get_drivers(carrier, converted_plan_date, plan_branch, {'exclude_account_hold': True, 'shift': shift})
        drivers = await get_hos_data_for_drivers(drivers, carrier)

        drivers_actual = await get_drivers(carrier, converted_plan_date, [], {'exclude_account_hold': False})
        drivers_actual = await get_hos_data_for_drivers(drivers_actual, carrier)

        # get loads for the given datef
        loads = await get_completed_moves(
            user_payload,
            from_time=plan_from_time,
            to_time=plan_to_time,
            plan_branch=plan_branch,
            plan_drivers=[driver.get('_id') for driver in drivers]
        )
        container_sizes_map, container_types_map = await get_container_sizes_and_types_labels(carrier, loads)

        mapped_loads = map_loads_for_scheduler(loads)

        # adjust appointment times to match the flexibility of what user did
        appointment_dict = {}
        for load in mapped_loads:
            driver_order = load.get('driverOrder', [])
            
            # Get actual event dates
            actual_dates = {
                'PULLCONTAINER': None,
                'DELIVERLOAD': None, 
                'RETURNCONTAINER': None
            }
            
            for event in driver_order:
                event_type = event.get('type')
                if event_type in actual_dates:
                    actual_dates[event_type] = event.get('departed')

            # Map of time fields to check and update
            time_fields = {
                'PULLCONTAINER': ('pickupFromTime', 'pickupToTime'),
                'DELIVERLOAD': ('deliveryFromTime', 'deliveryToTime'),
                'RETURNCONTAINER': ('returnFromTime', 'returnToTime')
            }
            
            is_amazon_load = load.get('consigneeName', '').startswith('AMZ') if load.get('consigneeName', '') else False

            # Update appointment windows based on actual times
            for event_type, (from_field, to_field) in time_fields.items():
                actual_date = actual_dates[event_type]
                from_time = load.get(from_field)
                to_time = load.get(to_field)

                if not all([from_time, to_time, actual_date]):
                    continue

                actual_dt = datetime.fromisoformat(actual_date)
                to_dt = datetime.fromisoformat(to_time)
                from_dt = datetime.fromisoformat(from_time)

                # Check if actual time was later than window
                diff_late = actual_dt - to_dt
                if (diff_late.total_seconds() / 60) > 15:
                    load[to_field] = (to_dt + timedelta(hours=int(diff_late.total_seconds() / 3600) + 1)).isoformat()
                    if is_amazon_load:
                        load[to_field] = datetime.fromisoformat(load[to_field]).astimezone(tz).replace(hour=23, minute=59, second=59, microsecond=999999).isoformat()
                        load[from_field] = datetime.fromisoformat(load[to_field]).astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

                # Check if actual time was earlier than window  
                diff_early = from_dt - actual_dt
                if (diff_early.total_seconds() / 60) > 15:
                    load[from_field] = (from_dt - timedelta(hours=int(diff_early.total_seconds() / 3600) + 1)).isoformat()
                    if is_amazon_load:
                        load[from_field] = datetime.fromisoformat(load[from_field]).astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                        load[to_field] = datetime.fromisoformat(load[from_field]).astimezone(tz).replace(hour=23, minute=59, second=59, microsecond=999999).isoformat()
            
            # create appointment dict for the load
            appointment_dict[load.get('_id')] = {}
            if load.get('pickupFromTime') and load.get('pickupToTime'):
                pickup_from = datetime.fromisoformat(load.get('pickupFromTime')).astimezone(tz).strftime('%Y-%m-%d %I:%M %p')
                pickup_to = datetime.fromisoformat(load.get('pickupToTime')).astimezone(tz).strftime('%I:%M %p')
                appointment_dict[load.get('_id')]['PULLCONTAINER'] = f"{pickup_from} - {pickup_to}"

            if load.get('deliveryFromTime') and load.get('deliveryToTime'):
                delivery_from = datetime.fromisoformat(load.get('deliveryFromTime')).astimezone(tz).strftime('%Y-%m-%d %I:%M %p')
                delivery_to = datetime.fromisoformat(load.get('deliveryToTime')).astimezone(tz).strftime('%I:%M %p')
                appointment_dict[load.get('_id')]['DELIVERLOAD'] = f"{delivery_from} - {delivery_to}"

            if load.get('returnFromTime') and load.get('returnToTime'):
                return_from = datetime.fromisoformat(load.get('returnFromTime')).astimezone(tz).strftime('%Y-%m-%d %I:%M %p')
                return_to = datetime.fromisoformat(load.get('returnToTime')).astimezone(tz).strftime('%I:%M %p')
                appointment_dict[load.get('_id')]['RETURNCONTAINER'] = f"{return_from} - {return_to}"


        # map loads for reporting
        actionable_moves = await map_loads_for_reporting(user_payload, mapped_loads, converted_plan_date)
        actionable_moves_copy = actionable_moves.copy()

        # map actual loads as the plan output
        actual_plan = []
        for move in actionable_moves_copy:
            move_copy = move['move'].copy()
            for index, event in enumerate(move_copy):
                event['enroute'] = event['arrived']
                event['arrived'] = event['departed']
                if index + 1 < len(move_copy):
                    event['departed'] = move_copy[index + 1]['arrived']

            driver_dict = next((driver for driver in drivers_actual if driver.get('_id') == move_copy[0].get('driver')), None)

            if not driver_dict:
                continue

            actual_plan.append({
                "load_id": move.get('_id'),
                "type_of_load": move.get('type_of_load'),
                "reference_number": move.get('reference_number'),
                "assigned_driver": driver_dict.get('_id') if driver_dict else None,
                "assigned_driver_name": f"{driver_dict.get('name')} {driver_dict.get('last_name')}" if driver_dict else '',
                "enroute_time": move_copy[0].get('enroute'),
                "move": move_copy,
            })

        # Process actual and optimal plans
        actual_plan_by_driver = group_and_sort_plan_by_driver(actual_plan)



        # filter actual plan by drivers to include only drivers that have road moves
        actual_plan_by_driver = {driver: plan for driver, plan in actual_plan_by_driver.items() if not any(each_plan.get('type_of_load') == 'ROAD' for each_plan in plan)}

        # get all reference numbers from actual plan by driver
        all_reference_numbers = [plan.get('reference_number') for driver, plans in actual_plan_by_driver.items() for plan in plans]
        actual_plan = [plan for plan in actual_plan if plan.get('reference_number') in all_reference_numbers]
        actionable_moves = [move for move in actionable_moves if move.get('reference_number') in all_reference_numbers]

        # Only keep actionable_moves where the driver performed moves (driver _id in actual_plan_by_driver)
        actual_driver_ids = set(actual_plan_by_driver.keys())
        actionable_moves = [move for move in actionable_moves if move['move'] and move['move'][0].get('driver') in actual_driver_ids]
        drivers = [driver for driver in drivers if driver.get('_id') in actual_driver_ids]

        actual_plan_detailed_entries = create_plan_detailed_entries_compare(
            carrier=carrier,
            default_yard_locations=default_yard_locations,
            plan_by_driver=actual_plan_by_driver,
            tz=tz,
            driver_features=drivers_actual,
            appointment_dict=appointment_dict,
            container_sizes_map=container_sizes_map,
            container_types_map=container_types_map,
            equipment_validations=equipment_validations
        )

        # add waiting time and travel time to actionable_moves
        for move in actionable_moves:
            for event in move['move']:
                try:
                    entry_in_plan = next(
                        (entry for entry in actual_plan_detailed_entries
                        if entry['reference_number'] == move['reference_number'] 
                        and entry['_id'] == event['_id']),
                        None
                    )

                    if entry_in_plan and entry_in_plan.get('arrived') and entry_in_plan.get('departed'):
                        departed = datetime.strptime(entry_in_plan['departed'], '%Y-%m-%d %I:%M %p')
                        arrived = datetime.strptime(entry_in_plan['arrived'], '%Y-%m-%d %I:%M %p')
                        waiting_time = int((departed - arrived).total_seconds() / 60)
                        event['waiting_time'] = waiting_time if waiting_time >= 0 else 10

                except Exception as e:
                    logger.error(e)
                    continue
        
            move['waiting_time'] = sum(event['waiting_time'] for event in move['move'])

        # add travel_time to each event as difference between current arrived and previous event's departed (index-based)
        for move in actionable_moves:
            events = move['move']
            for idx, event in enumerate(events):
                try:
                    if idx == 0:
                        event['travel_time'] = 0
                        continue

                    # current event entry
                    curr_entry = next(
                        (entry for entry in actual_plan_detailed_entries
                         if entry['reference_number'] == move['reference_number']
                         and entry['_id'] == event['_id']),
                        None
                    )

                    # previous event entry (by index)
                    prev_event = events[idx - 1]
                    prev_entry = next(
                        (entry for entry in actual_plan_detailed_entries
                         if entry['reference_number'] == move['reference_number']
                         and entry['_id'] == prev_event['_id']),
                        None
                    )

                    if curr_entry and prev_entry and curr_entry.get('arrived') and prev_entry.get('departed'):
                        arrived_dt = datetime.strptime(curr_entry['arrived'], '%Y-%m-%d %I:%M %p')
                        prev_departed_dt = datetime.strptime(prev_entry['departed'], '%Y-%m-%d %I:%M %p')
                        travel_time = int((arrived_dt - prev_departed_dt).total_seconds() / 60)
                        event['travel_time'] = travel_time if travel_time >= 0 else 0
                    else:
                        event['travel_time'] = 0

                except Exception as e:
                    logger.error(e)
                    event['travel_time'] = 0
            
        for move in actionable_moves:
            total_travel_time = sum(event.get('travel_time', 0) for event in move['move'])
            move['total_travel_time'] = total_travel_time
        # get optimal plan
        optimizer_output = await get_optimal_plan_v3(user_payload, actionable_moves, drivers, converted_plan_date, branch=plan_branch, shift=shift, allow_late_arrivals=True)
        optimal_plan = optimizer_output['optimal_plan']

        get_unplanned_moves = find_unplanned_moves(actionable_moves, optimal_plan)

        optimal_plan_by_driver = group_and_sort_plan_by_driver(optimal_plan)

        optimal_plan_detailed_entries = create_plan_detailed_entries_compare(
            carrier=carrier,
            default_yard_locations=default_yard_locations,
            plan_by_driver=optimal_plan_by_driver,
            tz=tz,
            driver_features=drivers_actual,
            appointment_dict=appointment_dict,
            container_sizes_map=container_sizes_map,
            container_types_map=container_types_map,
            equipment_validations=equipment_validations
        )

        actual_total_moves = len(actionable_moves)
        optimal_total_moves = len(optimal_plan)

        actual_total_drivers = len(actual_plan_by_driver)
        optimal_total_drivers = len(optimal_plan_by_driver)

        actual_empty_miles = sum(entry['empty_miles'] for entry in actual_plan_detailed_entries)
        optimal_empty_miles = sum(entry['empty_miles'] for entry in optimal_plan_detailed_entries)

        actual_trip_miles = sum(entry['trip_miles'] for entry in actual_plan_detailed_entries)
        optimal_trip_miles = sum(entry['trip_miles'] for entry in optimal_plan_detailed_entries)

        actual_total_miles = actual_empty_miles + actual_trip_miles
        optimal_total_miles = optimal_empty_miles + optimal_trip_miles

        return {
            "status": "success",
            "message": "Plan compared successfully",
            "total_moves": {
                "actual": actual_total_moves,
                "optimal": optimal_total_moves
            },
            "total_drivers": {
                "actual": actual_total_drivers,
                "optimal": optimal_total_drivers
            },
            "empty_miles": {
                "actual": actual_empty_miles,
                "optimal": optimal_empty_miles
            },
            "trip_miles": {
                "actual": actual_trip_miles,
                "optimal": optimal_trip_miles
            },
            "total_miles": {
                "actual": actual_total_miles,
                "optimal": optimal_total_miles
            },
            "comparison_data": {
                "actual_plan_detailed_entries": actual_plan_detailed_entries,
                "optimal_plan_detailed_entries": optimal_plan_detailed_entries,
                "unplanned_moves": json.loads(json.dumps(get_unplanned_moves, default=str))
            }
        }

    except Exception as e:
        logger.error(f"Error compare plan service : {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }