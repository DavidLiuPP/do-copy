import logging
import json
import pytz
from typing import Dict, Any, List
from datetime import datetime, timedelta
from bson.objectid import ObjectId

from app.services.redis_service import get_default_yard_location
from app.services.common_service import get_time_zone, get_carrier_preferences
from app.utils.distance_calc import calculate_distance_between_locations
from app.postgres_services.driver_service import get_drivers
from app.modules.optimizer.hos_service import get_hos_data_for_drivers
from app.modules.optimizer.driver_plan import get_shift_time
from app.mongo_services.load_service import get_completed_moves, get_moves_from_driver_order
from app.modules.scheduler.utility import map_loads_for_scheduler
from app.postgres_services.configurations_service import get_equipment_validations, get_container_sizes, get_container_types
from vrp_optimizer.helpers import generated_criteria, get_chassis_activity_between_moves, get_all_driver_default_locations
from vrp_optimizer.assumptions import CHASSIS_ACTIVITIES

from app.modules.optimizer.map_loads_optimizer_service import map_actionable_moves
from load_optimizer.get_optimal_plan_v3 import get_optimal_plan_v3
from app.mongo_services.mongo_service import get_customers

logger = logging.getLogger(__name__)


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
                load_copy['is_assigned_move'] = False
                load_copy['move'] = actionable_move
                load_copy['move_index'] = move_index
                loads_with_actionable_moves.append(load_copy)

        mapped_actionable_moves, _, _ = await map_actionable_moves(
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
    equipment_validations,
    depot_locations = []
):
    try:
        detailed_entries = []
        bobtail_transfer_count = 0
        chassis_transfer_count = 0
        for driver in plan_by_driver:
            driver_feature = next((df for df in driver_features if df and df.get('_id') == driver), {})
            depot_customer_id = driver_feature.get('depot_customer_id', '')
            default_yard_location = next((location for location in default_yard_locations if location.get('customerId') == depot_customer_id), None)
            if not default_yard_location:
                default_yard_location = next((customer for customer in depot_locations if str(customer.get('customerId')) == depot_customer_id), None)
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

                        if event.get('customerId') != default_yard_location.get('customerId'):
                            if event.get('type') != 'HOOKCONTAINER':
                                source = {
                                    'isDepot': False,
                                    'is_free_flow_move': False,
                                    'end_event': default_yard_location,
                                    'end_loc': [default_yard_location.get('address').get('lat'), default_yard_location.get('address').get('lng')],
                                    'container_size_label': default_yard_location.get('container_size_label', ''),
                                    'container_type_label': default_yard_location.get('container_type_label', ''),
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
                                if chassis_activity.get('type') != CHASSIS_ACTIVITIES["NO_ACTION"]:
                                    chassis_details = chassis_activity.get('drop_yard') or chassis_activity.get('hook_yard')
                                    try:
                                        detailed_entries.append({
                                            "_id": event.get('_id'),
                                            "driver_id": move.get('assigned_driver'),
                                            "driver": move.get('assigned_driver_name'),
                                            "reference_number": move.get('reference_number'),
                                            "type": chassis_activity.get('type'),
                                            "from": default_yard_location.get('company_name', ''),
                                            "to": chassis_details.get('company_name', ''),
                                            "enroute": datetime.fromisoformat(event['enroute']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                                            "arrived": datetime.fromisoformat(event['arrived']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                                            "departed": datetime.fromisoformat(event['departed']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                                            "trip_miles": 0,
                                            "empty_miles": int(empty_miles),
                                            "customer_id": chassis_details.get('location_id', ''),
                                            "chassis_activity": chassis_activity.get('type'),
                                            "has_chassis_pick": chassis_activity.get('type') == CHASSIS_ACTIVITIES["HOOKCHASSIS"],
                                            "has_chassis_termination": chassis_activity.get('type') == CHASSIS_ACTIVITIES["DROPCHASSIS"],
                                        })
                                    except Exception as e:
                                        logger.error(e)
                                        continue
                        trip_miles = 0
                    # end of move
                    elif index == len(plan_by_driver[driver]) - 1 and e_index == len(move['move']) - 1:
                        empty_miles = (
                            calculate_distance_between_locations(
                                event.get('address'),
                                default_yard_location.get('address')
                            ) if event.get('address') else 0
                        )

                        if event.get('customerId') != default_yard_location.get('customerId'):
                            if event.get('type') != 'DROPCONTAINER':
                                source = {
                                    'isDepot': False,
                                    'is_free_flow_move': False,
                                    'end_event': event,
                                    'end_loc': [event.get('address').get('lat'), event.get('address').get('lng')],
                                    'container_size_label': event.get('container_size_label', ''),
                                    'container_type_label': event.get('container_type_label', ''),
                                }
                                destination = {
                                    'isDepot': False,
                                    'is_free_flow_move': False,
                                    'start_event': default_yard_location,
                                    'start_loc': [default_yard_location.get('address').get('lat'), default_yard_location.get('address').get('lng')],
                                    'container_size_label': default_yard_location.get('container_size_label', ''),
                                    'container_type_label': default_yard_location.get('container_type_label', ''),
                                }
                                
                                chassis_activity = get_chassis_activity_between_moves(
                                    source,
                                    destination,
                                    equipment_validations,
                                    default_yard_locations,
                                    [],
                                    carrier
                                )
                                if chassis_activity.get('type') != CHASSIS_ACTIVITIES["NO_ACTION"]:
                                    chassis_details = chassis_activity.get('drop_yard') or chassis_activity.get('hook_yard')
                                    try:
                                        detailed_entries.append({
                                        "_id": event.get('_id'),
                                        "driver_id": move.get('assigned_driver'),
                                        "driver": move.get('assigned_driver_name'),
                                        "reference_number": move.get('reference_number'),
                                        "type": chassis_activity.get('type'),
                                        "from": event.get('company_name', ''),
                                        "to": chassis_details.get('company_name', ''),
                                        "enroute": datetime.fromisoformat(event['enroute']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                                        "arrived": datetime.fromisoformat(event['arrived']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                                        "departed": datetime.fromisoformat(event['departed']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                                        "trip_miles": int(empty_miles),
                                        "empty_miles": int(empty_miles),
                                        "customer_id": chassis_details.get('location_id', ''),
                                        "chassis_activity": chassis_activity.get('type'),
                                        "has_chassis_pick": chassis_activity.get('type') == CHASSIS_ACTIVITIES["HOOKCHASSIS"],
                                        "has_chassis_termination": chassis_activity.get('type') == CHASSIS_ACTIVITIES["DROPCHASSIS"],
                                    })
                                    except Exception as e:
                                        logger.error(e)
                                        continue
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

                        ## number of chassis activity
                        if is_chassis_activity_needed:
                            chassis_details = chassis_activity.get('drop_yard') or chassis_activity.get('hook_yard')
                            try:
                                detailed_entries.append({
                                    "_id": event.get('_id'),
                                    "driver_id": move.get('assigned_driver'),
                                    "driver": move.get('assigned_driver_name'),
                                    "reference_number": move.get('reference_number'),
                                    "type": chassis_activity.get('type'),
                                    "from": source.get('company_name', ''),
                                    "to": chassis_details.get('company_name', ''),
                                    "enroute": datetime.fromisoformat(event['enroute']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                                    "arrived": datetime.fromisoformat(event['arrived']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                                    "departed": datetime.fromisoformat(event['departed']).astimezone(tz).strftime('%Y-%m-%d %I:%M %p'),
                                    "trip_miles": 0,
                                    "empty_miles": int(empty_miles),
                                    "customer_id": chassis_details.get('location_id', ''),
                                    "chassis_activity": chassis_activity.get('type'),
                                    "has_chassis_pick": chassis_activity.get('type') == CHASSIS_ACTIVITIES["HOOKCHASSIS"],
                                    "has_chassis_termination": chassis_activity.get('type') == CHASSIS_ACTIVITIES["DROPCHASSIS"],
                                })
                            except Exception as e:
                                logger.error(e)
                                continue

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
                            "customer_id": event.get('customerId', ''),
                        })

                        last_event = event
                    except Exception as e:
                        logger.error(e)
                        continue
                
            if len(detailed_entries) > 0:
                for index, move_event in enumerate(detailed_entries):
                    if index == 0:
                        if move_event.get('type') in ['HOOKCHASSIS', 'HOOKCONTAINER']:
                            if move_event.get('customer_id') != default_yard_location.get('customerId'):
                                bobtail_transfer_count += 1
                                move_event['has_bobtail_transfer'] = True
                    else:
                        if len(detailed_entries) == index + 1:
                            if move_event.get('type') in ['DROPCONTAINER', 'DROPCHASSIS']:
                                if move_event.get('customer_id') != default_yard_location.get('customerId'):
                                    bobtail_transfer_count += 1
                                    move_event['has_bobtail_transfer'] = True
                        else:
                            if detailed_entries[index - 1].get('reference_number') != move_event.get('reference_number'):
                                prev_event = detailed_entries[index - 1]
                                current_event = move_event
                                if prev_event.get('customer_id') != current_event.get('customer_id'):
                                    if prev_event.get('type') in ['RETURNCONTAINER', 'LIFTOFF', 'DROPCHASSIS'] and current_event.get('type') in ['PULLCONTAINER', 'LIFTON', 'HOOKCHASSIS']: 
                                        chassis_transfer_count += 1
                                        move_event['has_chassis_transfer'] = True
                                    if prev_event.get('type') in ['DROPCONTAINER'] and current_event.get('type') in ['HOOKCONTAINER']:
                                        bobtail_transfer_count += 1
                                        move_event['has_bobtail_transfer'] = True
        
        return detailed_entries, bobtail_transfer_count, chassis_transfer_count
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to create plan detailed entries for comparison: {str(e)}")


async def get_container_sizes_and_types_labels(carrier: str, moves: List[Dict[str, Any]]):
    container_sizes = list(set([m.get('containerSize') for m in moves if m.get('containerSize')]))
    container_types = list(set([m.get('containerType') for m in moves if m.get('containerType')]))

    if not container_sizes and not container_types:
        return moves, {}

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


def get_kpi_data_from_plan_entries(plan_entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Count dual transactions, support moves, chassis pick and chassis termination driver-wise and overall from plan entries.
    Support moves = bobtail transfer + chassis transfer
    Dual transaction = consecutive entries with different reference_number but same location.
    Chassis pick = hook chassis
    Chassis termination = drop chassis
    Customer chassis pick = hook chassis at customer
    Customer chassis termination = drop chassis at customer
    Driver wise support moves = chassis pick + chassis termination
    Customer wise support moves = chassis pick + chassis termination
    Driver wise dual transaction = consecutive entries with different reference_number but same location.
    Customer wise dual transaction = consecutive entries with different reference_number but same location.
    Driver wise trip miles = trip miles
    Driver wise empty miles = empty miles
    Driver wise bobtail transfer = bobtail transfer
    Driver wise support moves = support moves
    Driver wise chassis pick = chassis pick
    Driver wise chassis termination = chassis termination
    Customer wise support moves = support moves
    Customer wise chassis pick = chassis pick
    Customer wise chassis termination = chassis termination
    Total dual transactions = dual transactions
    Total support moves = support moves
    Total chassis pick = chassis pick
    Total chassis termination = chassis termination
    Total empty miles = empty miles
    Total trip miles = trip miles
    Total bobtail transfer = bobtail transfer
    Total support moves = support moves
    Total chassis pick = chassis pick
    Total chassis termination = chassis termination

    """
    try:
        driver_counts = {}
        driver_trip_miles = {}
        driver_bobtail_transfer_count = {}
        driver_support_moves_count = {}
        driver_chassis_pick_count = {}
        driver_chassis_termination_count = {}
        customer_chassis_pick_count = {}
        customer_chassis_termination_count = {}
        
        # Single loop to handle all processing
        for i in range(len(plan_entries)):
            current = plan_entries[i]
            
            # Track driver-wise trip miles
            if current.get('driver_id') not in driver_trip_miles:
                driver_trip_miles[current.get('driver_id')] = {
                    'driver_id': current.get('driver_id'),
                    'driver_name': current.get('driver', ''),
                    'total_trip_miles': 0,
                    'empty_miles': 0
                }
            driver_trip_miles[current.get('driver_id')]['total_trip_miles'] += current.get('trip_miles', 0)
            driver_trip_miles[current.get('driver_id')]['empty_miles'] += current.get('empty_miles', 0)

            # Track driver-wise bobtail transfer count
            if current.get('driver_id') not in driver_bobtail_transfer_count:
                driver_bobtail_transfer_count[current.get('driver_id')] = {
                    'driver_id': current.get('driver_id'),
                    'driver_name': current.get('driver', ''),
                    'bobtail_transfer_count': 0,
                    'total_empty_miles': 0
                }
            if current.get('has_bobtail_transfer'):
                driver_bobtail_transfer_count[current.get('driver_id')]['bobtail_transfer_count'] += 1
                driver_bobtail_transfer_count[current.get('driver_id')]['total_empty_miles'] += current.get('empty_miles', 0)

            # Track driver-wise support moves count
            if current.get('driver_id') not in driver_support_moves_count:
                driver_support_moves_count[current.get('driver_id')] = {
                    'driver_id': current.get('driver_id'),
                    'driver_name': current.get('driver', ''),
                    'support_moves_count': 0,
                    'total_trip_miles': 0,
                    'total_empty_miles': 0
                }
            if current.get('has_chassis_transfer') or current.get('has_bobtail_transfer'):
                driver_support_moves_count[current.get('driver_id')]['support_moves_count'] += 1
                driver_support_moves_count[current.get('driver_id')]['total_empty_miles'] += current.get('empty_miles', 0)
            
            # Track driver-wise chassis pick count
            if current.get('driver_id') not in driver_chassis_pick_count:
                driver_chassis_pick_count[current.get('driver_id')] = {
                    'driver_id': current.get('driver_id'),
                    'driver_name': current.get('driver', ''),
                    'chassis_pick_count': 0,
                    'total_empty_miles': 0
                }
            if current.get('has_chassis_pick'):
                driver_chassis_pick_count[current.get('driver_id')]['chassis_pick_count'] += 1
                driver_chassis_pick_count[current.get('driver_id')]['total_empty_miles'] += current.get('empty_miles', 0)

            # Track driver-wise chassis termination count
            if current.get('driver_id') not in driver_chassis_termination_count:
                driver_chassis_termination_count[current.get('driver_id')] = {
                    'driver_id': current.get('driver_id'),
                    'driver_name': current.get('driver', ''),
                    'chassis_termination_count': 0,
                    'total_empty_miles': 0
                }
            if current.get('has_chassis_termination'):
                driver_chassis_termination_count[current.get('driver_id')]['chassis_termination_count'] += 1
                driver_chassis_termination_count[current.get('driver_id')]['total_empty_miles'] += current.get('empty_miles', 0)

            # Track customer-wise chassis pick count
            if current.get('customer_id') not in customer_chassis_pick_count:
                customer_chassis_pick_count[current.get('customer_id')] = {
                    'customer_id': current.get('customer_id'),
                    'customer_name': current.get('customer_name', ''),
                    'chassis_pick_count': 0
                }
            if current.get('has_chassis_pick'):
                customer_chassis_pick_count[current.get('customer_id')]['chassis_pick_count'] += 1
            
            # Track customer-wise chassis termination count
            if current.get('customer_id') not in customer_chassis_termination_count:
                customer_chassis_termination_count[current.get('customer_id')] = {
                    'customer_id': current.get('customer_id'),
                    'customer_name': current.get('customer_name', ''),
                    'chassis_termination_count': 0
                }
            if current.get('has_chassis_termination'):
                customer_chassis_termination_count[current.get('customer_id')]['chassis_termination_count'] += 1

            # Check for dual transactions (only if there's a next entry)
            if i < len(plan_entries) - 1:
                next_entry = plan_entries[i + 1]
                
                # Same driver, different move, same location = dual transaction
                if (current.get('driver_id') == next_entry.get('driver_id') and
                    current.get('reference_number') != next_entry.get('reference_number') and
                    current.get('customer_id') and
                    current.get('customer_id') == next_entry.get('customer_id') and
                    current.get('type') != next_entry.get('type') and
                    current.get('type') == 'RETURNCONTAINER' and
                    next_entry.get('type') == 'PULLCONTAINER'
                ):
                    
                    driver_id = current.get('driver_id')
                    if driver_id not in driver_counts:
                        driver_counts[driver_id] = {
                            'driver_id': driver_id,
                            'driver_name': current.get('driver', ''),
                            'dual_transaction_count': 0
                        }
                    driver_counts[driver_id]['dual_transaction_count'] += 1
                    driver_counts[driver_id].setdefault('loads', []).append((current.get('reference_number'), next_entry.get('reference_number')))
        
        # filter out drivers with 0 values
        if (len(driver_bobtail_transfer_count) > 0):
            driver_bobtail_transfer_count = [d for d in driver_bobtail_transfer_count.values() if driver_bobtail_transfer_count.get(d['driver_id'], {}).get('bobtail_transfer_count', 0) > 0]

        if (len(driver_support_moves_count) > 0):
            driver_support_moves_count = [d for d in driver_support_moves_count.values() if driver_support_moves_count.get(d['driver_id'], {}).get('support_moves_count', 0) > 0]

        if (len(driver_chassis_pick_count) > 0):
            driver_chassis_pick_count = [d for d in driver_chassis_pick_count.values() if driver_chassis_pick_count.get(d['driver_id'], {}).get('chassis_pick_count', 0) > 0]

        if (len(driver_chassis_termination_count) > 0):
            driver_chassis_termination_count = [d for d in driver_chassis_termination_count.values() if driver_chassis_termination_count.get(d['driver_id'], {}).get('chassis_termination_count', 0) > 0]

        if (len(customer_chassis_pick_count) > 0):
            customer_chassis_pick_count = [d for d in customer_chassis_pick_count.values() if customer_chassis_pick_count.get(d['customer_id'], {}).get('chassis_pick_count', 0) > 0]

        if (len(customer_chassis_termination_count) > 0):
            customer_chassis_termination_count = [d for d in customer_chassis_termination_count.values() if customer_chassis_termination_count.get(d['customer_id'], {}).get('chassis_termination_count', 0) > 0]


        return {
            'total_dual_transactions': {
                'total_dual_transactions': sum(d['dual_transaction_count'] for d in driver_counts.values()),
                'drivers_with_dual_transactions': len(driver_counts),
                'driver_wise_counts': list(driver_counts.values())
            },
            'driver_trip_miles': list(driver_trip_miles.values()),
            'driver_bobtail_transfer_count': {
                'total_bobtail_transfer_count': sum(d['bobtail_transfer_count'] for d in driver_bobtail_transfer_count),
                'drivers_with_bobtail_transfer': len(driver_bobtail_transfer_count),
                'driver_wise_counts': list(driver_bobtail_transfer_count),
                'total_empty_miles': sum(d['total_empty_miles'] for d in driver_bobtail_transfer_count)
            },
            'driver_support_moves_count': {
                'total_support_moves_count': sum(d['support_moves_count'] for d in driver_support_moves_count),
                'drivers_with_support_moves': len(driver_support_moves_count),
                'driver_wise_counts': list(driver_support_moves_count),
                'total_empty_miles': sum(d['total_empty_miles'] for d in driver_support_moves_count)
            },
            'driver_chassis_pick_count': {
                'total_chassis_pick_count': sum(d['chassis_pick_count'] for d in driver_chassis_pick_count),
                'drivers_with_chassis_pick': len(driver_chassis_pick_count),
                'driver_wise_counts': list(driver_chassis_pick_count),
                'total_empty_miles': sum(d['total_empty_miles'] for d in driver_chassis_pick_count)
            },
            'driver_chassis_termination_count': {
                'total_chassis_termination_count': sum(d['chassis_termination_count'] for d in driver_chassis_termination_count),
                'drivers_with_chassis_termination': len(driver_chassis_termination_count),
                'driver_wise_counts': list(driver_chassis_termination_count),
                'total_empty_miles': sum(d['total_empty_miles'] for d in driver_chassis_termination_count)
            },
            'customer_chassis_pick_count': {
                'total_chassis_pick_count': sum(d['chassis_pick_count'] for d in customer_chassis_pick_count),
                'customers_with_chassis_pick': len(customer_chassis_pick_count),
                'customer_wise_counts': list(customer_chassis_pick_count)
            },
            'customer_chassis_termination_count': {
                'total_chassis_termination_count': sum(d['chassis_termination_count'] for d in customer_chassis_termination_count),
                'customers_with_chassis_termination': len(customer_chassis_termination_count),
                'customer_wise_counts': list(customer_chassis_termination_count)
            }

        }
        
    except Exception as e:
        logger.error(f"Error finding dual transactions: {str(e)}")
        return {
            'total_dual_transactions': {
                'total_dual_transactions': 0,
                'drivers_with_dual_transactions': 0,
                'driver_wise_counts': 0
            },
            'driver_trip_miles': [],
            'driver_bobtail_transfer_count': {
                'total_bobtail_transfer_count': 0,
                'drivers_with_bobtail_transfer': 0,
                'driver_wise_counts': [],
                'total_empty_miles': 0
            },
            'driver_support_moves_count': {
                'total_support_moves_count': 0,
                'drivers_with_support_moves': 0,
                'driver_wise_counts': [],
                'total_empty_miles': 0
            },
            'driver_chassis_pick_count': {
                'total_chassis_pick_count': 0,
                'drivers_with_chassis_pick': 0,
                'driver_wise_counts': [],
                'total_empty_miles': 0
            },
            'driver_chassis_termination_count': {
                'total_chassis_termination_count': 0,
                'drivers_with_chassis_termination': 0,
                'driver_wise_counts': [],
                'total_empty_miles': 0
            },
            'customer_chassis_pick_count': {
                'total_chassis_pick_count': 0,
                'customers_with_chassis_pick': 0,
                'customer_wise_counts': []
            },
            'customer_chassis_termination_count': {
                'total_chassis_termination_count': 0,
                'customers_with_chassis_termination': 0,
                'customer_wise_counts': []
            }
        }


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
        default_yard_locations = await get_default_yard_location(carrier, False, plan_branch)
        # for each yard location, add start_loc and end_loc
        for location in default_yard_locations:
            depot_customer_id = location.get('customerId')
            location['start_loc'] = [location.get('address').get('lat'), location.get('address').get('lng')]
            location['end_loc'] = [location.get('address').get('lat'), location.get('address').get('lng')]
            location['depot_customer_id'] = depot_customer_id

        carrier_preferences = await get_carrier_preferences(carrier)
        user_settings = await get_equipment_validations(carrier)
        equipment_validations = user_settings.get('equipment_validations', [])

        planning_minutes = await get_shift_time(carrier, converted_plan_date, shift, plan_branch, timeZone)
        plan_from_time = converted_plan_date + timedelta(minutes=planning_minutes[0])
        plan_to_time = converted_plan_date + timedelta(minutes=planning_minutes[1])

        user_payload['default_yard_locations'] = default_yard_locations
        user_payload['timeZone'] = timeZone
        user_payload['distanceUnit'] = carrier_preferences.get('distanceUnit', 'mi')
        driver_criteria, _ = generated_criteria(plan_date=converted_plan_date, plan_branch=plan_branch, shift=shift)
        # get drivers
        drivers = await get_drivers(carrier, driver_criteria, {'exclude_account_hold': True})
        drivers = await get_hos_data_for_drivers(drivers, carrier)

        driver_default_locations = get_all_driver_default_locations(drivers)
        yard_customers = await get_customers({
            "_id": {
                "$in": [
                    ObjectId(driver_default_location.get('depot_customer_id'))
                    for driver_default_location in driver_default_locations
                ]
            }
        })

        depot_locations = []
        for y in yard_customers:
            depot_locations.append({
                "_id": str(y['_id']),
                "customerId": str(y.get("_id", "")),
                "company_name": y.get("company_name", ""),
                "address": y.get("address", {}),
                "city": y.get("city", ""),
                "state": y.get("state", ""),
                "country": y.get("country", ""),
                "zip_code": y.get("zip_code", ""),
                "id": str(y['_id']),
                "is_chassis_pick_allowed": True,
                "is_chassis_termination_allowed": True
            })   

        drivers_actual = await get_drivers(carrier, driver_criteria, {'exclude_account_hold': False})
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

        actual_plan_detailed_entries, actual_bobtail_transfer_count, actual_chassis_transfer_count = create_plan_detailed_entries_compare(
            carrier=carrier,
            default_yard_locations=default_yard_locations,
            plan_by_driver=actual_plan_by_driver,
            tz=tz,
            driver_features=drivers_actual,
            appointment_dict=appointment_dict,
            container_sizes_map=container_sizes_map,
            container_types_map=container_types_map,
            equipment_validations=equipment_validations,
            depot_locations=depot_locations
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
        optimizer_output = await get_optimal_plan_v3(user_payload, actionable_moves, drivers, converted_plan_date, branch=plan_branch, shift=shift)
        optimal_plan = optimizer_output['optimal_plan']

        get_unplanned_moves = find_unplanned_moves(actionable_moves, optimal_plan)

        optimal_plan_by_driver = group_and_sort_plan_by_driver(optimal_plan)

        optimal_plan_detailed_entries, optimal_bobtail_transfer_count, optimal_chassis_transfer_count = create_plan_detailed_entries_compare(
            carrier=carrier,
            default_yard_locations=default_yard_locations,
            plan_by_driver=optimal_plan_by_driver,
            tz=tz,
            driver_features=drivers_actual,
            appointment_dict=appointment_dict,
            container_sizes_map=container_sizes_map,
            container_types_map=container_types_map,
            equipment_validations=equipment_validations,
            depot_locations=depot_locations
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

        kpi_data_actual = get_kpi_data_from_plan_entries(actual_plan_detailed_entries)
        kpi_data_optimal = get_kpi_data_from_plan_entries(optimal_plan_detailed_entries)

        actual_dual_transactions_analysis = kpi_data_actual.get('total_dual_transactions')
        optimal_dual_transactions_analysis = kpi_data_optimal.get('total_dual_transactions')

        actual_driver_trip_miles = kpi_data_actual.get('driver_trip_miles')
        optimal_driver_trip_miles = kpi_data_optimal.get('driver_trip_miles')

        actual_driver_bobtail_transfer = kpi_data_actual.get('driver_bobtail_transfer_count')
        optimal_driver_bobtail_transfer = kpi_data_optimal.get('driver_bobtail_transfer_count')

        actual_driver_support_moves = kpi_data_actual.get('driver_support_moves_count')
        optimal_driver_support_moves = kpi_data_optimal.get('driver_support_moves_count')

        actual_driver_chassis_pick = kpi_data_actual.get('driver_chassis_pick_count')
        optimal_driver_chassis_pick = kpi_data_optimal.get('driver_chassis_pick_count')

        actual_driver_chassis_termination = kpi_data_actual.get('driver_chassis_termination_count')
        optimal_driver_chassis_termination = kpi_data_optimal.get('driver_chassis_termination_count')

        actual_customer_chassis_pick = kpi_data_actual.get('customer_chassis_pick_count')
        optimal_customer_chassis_pick = kpi_data_optimal.get('customer_chassis_pick_count')

        actual_customer_chassis_termination = kpi_data_actual.get('customer_chassis_termination_count')
        optimal_customer_chassis_termination = kpi_data_optimal.get('customer_chassis_termination_count')

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
            "dual_transactions_analysis": {
                "actual": actual_dual_transactions_analysis,
                "optimal": optimal_dual_transactions_analysis
            },
            "driver_trip_miles": {
                "actual": actual_driver_trip_miles,
                "optimal": optimal_driver_trip_miles
            },
            "driver_bobtail_transfer": {
                "actual": actual_driver_bobtail_transfer,
                "optimal": optimal_driver_bobtail_transfer
            },
            "driver_support_moves": {
                "actual": actual_driver_support_moves,
                "optimal": optimal_driver_support_moves
            },
            "comparison_data": {
                "actual_plan_detailed_entries": actual_plan_detailed_entries,
                "optimal_plan_detailed_entries": optimal_plan_detailed_entries,
                "unplanned_moves": json.loads(json.dumps(get_unplanned_moves, default=str))
            },
            "no_of_support_moves": {
                "actual": actual_bobtail_transfer_count + actual_chassis_transfer_count,
                "optimal": optimal_bobtail_transfer_count + optimal_chassis_transfer_count
            },
            "bobtail_transfer_count": {
                "actual": actual_bobtail_transfer_count,
                "optimal": optimal_bobtail_transfer_count
            },
            "chassis_transfer_count": {
                "actual": actual_chassis_transfer_count,
                "optimal": optimal_chassis_transfer_count
            },
            "chassis_pick_count": {
                "driver_wise_chassis_pick": {
                    "actual": actual_driver_chassis_pick,
                    "optimal": optimal_driver_chassis_pick
                },
                "customer_wise_chassis_pick": {
                    "actual": actual_customer_chassis_pick,
                    "optimal": optimal_customer_chassis_pick
                }
            },
            "chassis_termination_count": {
                "driver_wise_chassis_termination": {
                    "actual": actual_driver_chassis_termination,
                    "optimal": optimal_driver_chassis_termination
                },
                "customer_wise_chassis_termination": {
                    "actual": actual_customer_chassis_termination,
                    "optimal": optimal_customer_chassis_termination
                }
            },
        }

    except Exception as e:
        logger.error(f"Error compare plan service : {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }