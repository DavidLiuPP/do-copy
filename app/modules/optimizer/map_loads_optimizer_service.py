import logging
from datetime import datetime
from typing import Any, Dict, List

from app.mongo_services.load_service import get_moves_from_driver_order
from app.mongo_services.trip_service import get_trips_by_trip_ids, get_order_details, get_free_flow_trips_by_order_ids
from app.postgres_services.drayage_intelligence_service import get_drayage_intelligence
from app.postgres_services.terminal_warehouse_service import get_office_hours
from app.postgres_services.turn_around_time import get_waiting_time, add_waiting_time_to_move

from app.modules.optimizer.constants import VALID_EVENT_TYPES, CARRIER_CONFIGS
from app.modules.optimizer.utility import (
    check_move_validity, 
    modify_move_for_invalid_move, 
    populate_appointment_times_to_events
)


# Configure module logger
logger = logging.getLogger(__name__)

async def map_actionable_moves(
    user_payload: Dict[str, Any],
    loads: List[Dict[str, Any]],
    converted_plan_date: datetime,
    plan_range: Dict[str, Any] = {},
    options: Dict[str, Any] = {}
) -> List[Dict[str, Any]]:
    try:
        actionable_moves = []
        invalid_moves = []
        time_prediction = options.get('time_prediction', False)
        
        is_approved_move_ids_provided = options.get('is_approved_move_ids_provided', False)
        approved_move_ids = options.get('approved_move_ids', [])

        carrier = user_payload.get('carrier')
        timeZone = user_payload.get('timeZone')

        all_locations = list(set(event['customerId'] for load in loads for event in load['move'] if event.get('customerId')))
        location_office_hours = await get_office_hours(carrier, all_locations, timeZone)

        drayage_config_for_drops = await get_drayage_intelligence(
            carrier, 
            ['empty_drop_yard_config', 'empty_drop_logic_version']
        )
        
        user_payload['drayage_config'] = drayage_config_for_drops

        drayage_intelligence = await get_drayage_intelligence(
            carrier, 
            ['is_exclude_from_scheduler', 'exclude_locations_for_scheduler']
        )
        
        for load_details in loads:
            load_copy = load_details.copy()
            
            actionable_move = load_copy.get('move', [])

            if load_copy.get('is_active_move'):
                load_copy['route'] = '_'.join([move.get('customerId', '') for move in actionable_move])
                load_copy['distance'] = sum(event.get('distance', 0) for event in actionable_move)
                load_copy['load_assigned_date'] = actionable_move[0].get('loadAssignedDate')
                actionable_moves.append(load_copy)
                continue
            
            if not actionable_move:
                continue

            move_id = actionable_move[0].get('moveId')
            # skip if the load has previous invalid moves
            if any(move['reference_number'] == load_details['reference_number'] for move in invalid_moves):
                continue

            # set appointment times to events
            actionable_move = populate_appointment_times_to_events(
                user_payload,
                [load_copy],
                actionable_move,
                converted_plan_date,
                time_prediction,
                location_office_hours
            )

            is_valid_move, reason = check_move_validity(
                user_payload=user_payload,
                move=actionable_move,
                load=load_copy,
                converted_plan_date=converted_plan_date,
                timeZone=timeZone,
                time_prediction=time_prediction,
                drayage_intelligence=drayage_intelligence,
                plan_range=plan_range
            )

            if not is_valid_move:
                event_type = reason.split('_')[0]
                is_valid_event_type = event_type in VALID_EVENT_TYPES
                move_modification_approved = not is_approved_move_ids_provided or (is_approved_move_ids_provided and move_id in approved_move_ids)
                
                if not is_valid_event_type or not move_modification_approved:
                    invalid_moves.append({
                        'reference_number': load_copy.get('reference_number', ''),
                        'reason': reason
                    })
                    continue

                modified_move = modify_move_for_invalid_move(user_payload, actionable_move, event_type, 'end', load_copy)
                modified_move = populate_appointment_times_to_events(
                    user_payload, [load_copy], modified_move, converted_plan_date, 
                    time_prediction, location_office_hours
                )

                is_valid_move, reason = check_move_validity(
                    user_payload=user_payload,
                    move=modified_move,
                    load=load_copy,
                    converted_plan_date=converted_plan_date,
                    timeZone=timeZone,
                    time_prediction=time_prediction,
                    drayage_intelligence=drayage_intelligence,
                    plan_range=plan_range
                )

                if is_valid_move:
                    actionable_move = modified_move
                    load_copy['is_modified_move'] = True
                else:
                    invalid_moves.append({
                        'reference_number': load_copy.get('reference_number', ''),
                        'reason': reason
                    })
                    continue

            load_copy['move'] = actionable_move
            load_copy['route'] = '_'.join([move.get('customerId', '') for move in actionable_move])
            load_copy['distance'] = sum(event.get('distance', 0) for event in actionable_move)
            load_copy['load_assigned_date'] = actionable_move[0].get('loadAssignedDate')
            
            # Get earliest appointment time, filtering out None values
            apt_times = [
                datetime.fromisoformat(event['appointment_from']).replace(tzinfo=None)
                for event in actionable_move
                if event.get('appointment_from') and event.get('is_scheduled')
            ]
            load_copy['earliest_apt_time'] = min(apt_times) if apt_times else None

            actionable_moves.append(load_copy)

        if len(actionable_moves) > 0:
            # Sort actionable moves by earliest appointment time, putting None values last
            actionable_moves.sort(key=lambda x: (
                not x.get('is_manually_planned', False),
                x.get('earliest_apt_time') is None,
                x.get('earliest_apt_time', datetime.max) or datetime.max
            ))

            # add waiting time to each move
            waiting_times = await get_waiting_time(carrier, all_locations)
            actionable_moves = add_waiting_time_to_move(actionable_moves, waiting_times, timeZone)

        return actionable_moves, invalid_moves
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to map actionable moves for optimizer: {str(e)}")


async def map_actionable_combined_trips(
    user_payload: Dict[str, Any],
    combined_moves: List[Dict[str, Any]],
    converted_plan_date: datetime,
    options: Dict[str, Any] = {}
) -> List[Dict[str, Any]]:
    try:
        carrier = user_payload.get('carrier')
        timeZone = user_payload.get('timeZone')
        time_prediction = options.get('time_prediction', False)

        all_locations = list(set(event['customerId'] for move in combined_moves for event in move['move'] if event.get('customerId')))
        location_office_hours = await get_office_hours(carrier, all_locations, timeZone)

        combined_trip_ids = list(set(event.get('combineTripId') for move in combined_moves for event in move['move']))
        trips = await get_trips_by_trip_ids(carrier, combined_trip_ids)

        actionable_trips = []

        for trip in trips:
            actionable_move = trip['tripOrder']
            loads = [move for move in combined_moves if move.get('combineTripId') == trip.get('_id')]
            actionable_move = populate_appointment_times_to_events(user_payload, loads, actionable_move, converted_plan_date, time_prediction, location_office_hours)

            load_with_info = loads[0]
            load_with_info['_id'] = trip.get('_id')
            load_with_info['reference_number'] = trip.get('tripNumber')
            load_with_info['move'] = actionable_move
            load_with_info['route'] = '_'.join([move.get('customerId', '') for move in actionable_move if move.get('customerId')])
            load_with_info['distance'] = sum(event.get('distance', 0) for event in actionable_move)
            load_with_info['load_assigned_date'] = actionable_move[0].get('loadAssignedDate')

            actionable_trips.append(load_with_info)

        if len(actionable_trips) > 0:
            waiting_times = await get_waiting_time(carrier, all_locations)
            actionable_trips = add_waiting_time_to_move(actionable_trips, waiting_times, timeZone)

        return actionable_trips
    
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to map actionable combined trips for optimizer: {str(e)}")
    

async def map_actionable_free_flow_trips(
    user_payload: Dict[str, Any],
    free_flow_trips: List[Dict[str, Any]],
    converted_plan_date: datetime,
    options: Dict[str, Any] = {}
) -> List[Dict[str, Any]]:
    try:
        carrier = user_payload.get('carrier')
        timeZone = user_payload.get('timeZone')
        time_prediction = options.get('time_prediction', False)

        all_locations = list(set(event['customerId'] for trip in free_flow_trips for event in trip['tripOrder'] if event.get('customerId')))
        location_office_hours = await get_office_hours(carrier, all_locations, timeZone)
        trips = free_flow_trips

        actionable_trips = []

        for trip in trips:
            loads = [trip.get('loadDetails')] if trip.get('loadDetails') else []
            if not loads:
                continue

            actionable_move = trip['tripOrder']
            actionable_move = populate_appointment_times_to_events(user_payload, loads, actionable_move, converted_plan_date, time_prediction, location_office_hours)

            load_with_info = loads[0]
            load_with_info['related_load_id'] = load_with_info.get('_id')
            load_with_info['_id'] = trip.get('_id')
            load_with_info['reference_number'] = trip.get('tripNumber')
            load_with_info['is_free_flow_move'] = True
            load_with_info['move'] = actionable_move
            load_with_info['driverOrder'] = actionable_move
            load_with_info['move_index'] = 0
            load_with_info['route'] = '_'.join([move.get('customerId', '') for move in actionable_move if move.get('customerId')])
            load_with_info['distance'] = sum(event.get('distance', 0) for event in actionable_move)
            load_with_info['load_assigned_date'] = actionable_move[0].get('loadAssignedDate')

            actionable_trips.append(load_with_info)

        if len(actionable_trips) > 0:
            waiting_times = await get_waiting_time(carrier, all_locations)
            actionable_trips = add_waiting_time_to_move(actionable_trips, waiting_times, timeZone)

        return actionable_trips
    
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to map actionable combined trips for optimizer: {str(e)}")
    
def get_prepull_driver_for_deliver_move(moves, current_move_index):
    """
    Determines if there is a prepull move (a move consisting of a PULLCONTAINER followed by a DROPCONTAINER event)
    before the specified delivery move in the list of driver moves.

    Args:
        moves: List of moves from driver order
        current_move_index: Index of the delivery move to check against
        
    Returns:
        tuple: (True, assigned_driver) if a prepull move exists before the delivery move, otherwise (False, None).
    """
    # Copy moves parameter to a new parameter
    moves_copy = moves.copy() 
    current_move = moves_copy[current_move_index]

    is_deliver_move = any(event.get('type') == 'DELIVERLOAD' for event in current_move)

    if not is_deliver_move:
        return False, None
    
    # Get all previous moves from the current move in the array
    previous_moves = moves_copy[:current_move_index]

    # Check each previous move for exactly one PULLLOAD and one DROPLOAD event, and no other event types
    for move in previous_moves:
        event_types = [event.get('type') for event in move]
        # Check that there are exactly two events, first is PULLCONTAINER, second is DROPCONTAINER
        if len(event_types) == 2 and event_types[0] == 'PULLCONTAINER' and event_types[1] == 'DROPCONTAINER':
            # Return True and the assigned driver for this move
            assigned_driver = move[0].get('driver')
            if assigned_driver:
                return True, assigned_driver
    
    return False, None


async def map_loads_for_optimizer(
    user_payload: Dict[str, Any],
    loads: List[Dict[str, Any]],
    scheduled_plans: List[Dict[str, Any]],
    converted_plan_date: datetime,
    plan_range: Dict[str, Any] = {},
    plan_drivers = None,
    options: Dict[str, Any] = {},
    reference_numbers: List[str] = []
) -> List[Dict[str, Any]]:
    try:
        loads_with_actionable_moves = []
        loads_with_combined_trips = []
        invalid_moves = []

        replace_free_flow_trips = options.get('replace_free_flow_trips', False)

        unique_order_ids = list({load.get('orderId') for load in loads if load.get('orderId') is not None})
        free_flow_trips = []
        free_flow_orders = []
        order_datas = []
        order_related_free_flow_trips = {}

        allow_to_plan_following_moves = CARRIER_CONFIGS.get(user_payload.get('carrier'), {}).get('allow_to_plan_following_moves', False)

        if len(unique_order_ids) > 0:
            order_datas = await get_order_details(user_payload.get('carrier'), order_ids=unique_order_ids)
            free_flow_orders = [order.get('_id', '') for order in order_datas if order.get('orderType') == 'FREE_FLOW']

            if replace_free_flow_trips:
                available_free_flow_trips = await get_free_flow_trips_by_order_ids(user_payload.get('carrier'), unique_order_ids)
                for trip in available_free_flow_trips:
                    if not order_related_free_flow_trips.get(trip.get('orderId')):
                        order_related_free_flow_trips[trip.get('orderId')] = []

                    order_related_free_flow_trips[trip.get('orderId')].append(trip)


        for load in loads:
            # find scheduled plan for the load using reference number, should be an array of all that matches
            scheduled_plan = [plan for plan in scheduled_plans if plan['reference_number'] == load['reference_number']]
            is_move_included = False

            # find actionable move from the load
            driver_order = load.get('driverOrder', None)
            moves = get_moves_from_driver_order(driver_order, { "exclude_void_out": True, "exclude_unplanned_completed_move": True })
            if not len(moves):
                continue

            for move_index, actionable_move in enumerate(moves):
                # skip if the move is not in the scheduled plan or not manually planned
                is_manually_planned = any(event.get('is_manually_planned') for event in actionable_move)
                is_active_move = any(event.get('arrived') and not event.get('isVoidOut') for event in actionable_move)

                event_types = [event.get('type') for event in actionable_move]
                scheduled_events_in_move = [plan for plan in scheduled_plan if plan.get('profile_type', '') in event_types]
                has_scheduled_appointment = any(plan['scheduled_appointment_from'] for plan in scheduled_events_in_move)

                if is_manually_planned:
                    load_assigned_date = datetime.fromisoformat(actionable_move[0].get('loadAssignedDate'))
                    active_event_departed = next(
                        (datetime.fromisoformat(event.get('departed'))
                         for event in actionable_move
                         if event.get('departed') and not event.get('isVoidOut')),
                        None
                    )
                    driver_working_date = active_event_departed if active_event_departed else load_assigned_date

                    if not (plan_range.get('shift_from_time') <= driver_working_date <= plan_range.get('shift_to_time')) and not has_scheduled_appointment:
                        continue

                elif not scheduled_events_in_move:
                    continue

                # check if the move is already in progress on previous days
                arrived_event = next((event for event in actionable_move if event.get('arrived')), None)
                if (arrived_event and not arrived_event.get('isVoidOut') and arrived_event.get('driver') and
                    (datetime.fromisoformat(arrived_event.get('arrived')) < converted_plan_date)):
                    load['error_reason'] = 'MOVE_IN_PROGRESS'
                    continue

                # if previous move is not completed, skip the current move
                previous_move = moves[move_index - 1] if move_index > 0 else None
                is_previous_move_pending = previous_move and any(not event.get('departed') and not event.get('isVoidOut') for event in previous_move)
                if is_previous_move_pending:
                    if not allow_to_plan_following_moves:
                        load['error_reason'] = 'MOVE_IS_NOT_READY_TO_START'
                        continue

                    delivery_event = next((event for event in previous_move if event.get('type') == 'DELIVERLOAD' and not event.get('isVoidOut')), None)
                    drop_event = next((event for event in previous_move if event.get('type') == 'DROPCONTAINER' and event.get('prevType') == 'DELIVERLOAD' and not event.get('isVoidOut')), None)

                    was_dropped_at_warehouse = delivery_event and drop_event and delivery_event.get('customerId', '') == drop_event.get('customerId', '')

                    is_assigned = actionable_move[0].get('driver')
                    
                    # If it is going to be dropped at same location as deliver then it might not be empty
                    if was_dropped_at_warehouse and not is_assigned:
                        load['error_reason'] = 'MOVE_IS_NOT_READY_TO_START'
                        continue

                assigned_driver = actionable_move[0].get('driver')
                if plan_drivers and assigned_driver and assigned_driver not in plan_drivers:
                    load['error_reason'] = 'INVALID_DRIVER'
                    continue

                
                load_copy = load.copy()
                del load_copy['driverOrder']
                
                # Check move properties
                is_manually_planned = any(event.get('is_manually_planned') for event in actionable_move)
                is_combined_trip = any(event.get('combineTripId') for event in actionable_move)

                pickup_event = next((event for event in actionable_move if event.get('type') == 'PULLCONTAINER'), None)
                is_free_flow_move = load_copy.get('orderId') and load_copy.get('orderId') in free_flow_orders and pickup_event and load_copy['type_of_load'] == 'IMPORT' 
                
                
                load_copy['is_manually_planned'] = False
                load_copy['move'] = actionable_move
                load_copy['move_index'] = move_index

                if is_manually_planned and load['reference_number'] not in reference_numbers:
                    load_copy['is_manually_planned'] = True
                    load_copy['assigned_driver'] = actionable_move[0].get('driver')

                    if is_active_move:
                        load_copy['is_active_move'] = True

                use_prepull_driver_for_deliver_move = CARRIER_CONFIGS.get(user_payload.get('carrier'), {}).get('use_prepull_driver_for_deliver_move', None)

                if use_prepull_driver_for_deliver_move:
                    has_prepull_move, prepull_driver = get_prepull_driver_for_deliver_move(moves, move_index)
                    if has_prepull_move and prepull_driver:
                        load_copy['suggested_driver'] = prepull_driver
                
                
                if is_combined_trip:
                    load_copy['is_combined_trip'] = True
                    load_copy['combineTripId'] = actionable_move[0].get('combineTripId')
                    is_move_included = True
                    loads_with_combined_trips.append(load_copy)
                    continue

                if is_free_flow_move and not load_copy.get('is_active_move', False):
                    related_order = next((order for order in order_datas if order.get('_id') == load_copy.get('orderId')), None) or {}
                    load_copy['free_flow_order'] = related_order.get('orderNumber', '')
                    
                    if replace_free_flow_trips:
                        order_related_trips = order_related_free_flow_trips.get(load_copy.get('orderId'), [])
                        if len(order_related_trips) > 0:
                            new_free_flow_trip = next((trip for trip in order_related_trips if not trip.get('loadDetails')), None)
                            if new_free_flow_trip:
                                new_free_flow_trip['loadDetails'] = load_copy
                                is_move_included = True
                                free_flow_trips.append(new_free_flow_trip)
                        
                        # Never send the non active container moves
                        continue

                is_move_included = True
                loads_with_actionable_moves.append(load_copy)
                break;

            if not is_move_included:
                invalid_moves.append({
                    'reference_number': load.get('reference_number', ''),
                    'reason': load.get('error_reason', 'MOVE_IS_PLANNED_FOR_OTHER_DAY_OR_SHIFT')
                })

        mapped_actionable_moves, _invalid_moves = await map_actionable_moves(
            user_payload=user_payload,
            loads=loads_with_actionable_moves,
            converted_plan_date=converted_plan_date,
            plan_range=plan_range,
            options=options
        )

        invalid_moves.extend(_invalid_moves)

        # check if any of the moves are part of combined move
        if len(loads_with_combined_trips) > 0:
            mapped_combined_trips = await map_actionable_combined_trips(user_payload, loads_with_combined_trips, converted_plan_date, options)
            mapped_actionable_moves = [*mapped_combined_trips, *mapped_actionable_moves]


        if replace_free_flow_trips and len(free_flow_trips) > 0:
            mapped_free_flow_trips = await map_actionable_free_flow_trips(
                user_payload=user_payload,
                free_flow_trips=free_flow_trips,
                converted_plan_date=converted_plan_date,
                options=options
            )
            mapped_actionable_moves = [*mapped_free_flow_trips, *mapped_actionable_moves]

        return mapped_actionable_moves, invalid_moves
    
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to map loads for optimizer: {str(e)}")