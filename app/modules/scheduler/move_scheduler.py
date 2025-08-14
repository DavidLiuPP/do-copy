"""
Move Scheduler Controller

This module handles the scheduling and prediction of next moves for loads in the transportation system.
It provides functionality to transform load data and interface with the load prediction model.

The module follows a layered architecture pattern with clear separation of concerns:
- Data transformation layer: Converts raw load data into scheduler-compatible format
- Business logic layer: Classifies and processes load moves
- Integration layer: Interfaces with external prediction service

Key Features:
- Comprehensive input validation and error handling
- Immutable data transformations
- Clear type hints and documentation
- Modular design for maintainability
- Logging for observability

Classes and Functions:
    _extract_pickup_times: Safely extracts pickup time data from load
    _extract_delivery_times: Safely extracts delivery time data from load  
    _set_default_fields: Sets default values for optional load fields
    classify_current_moves: Classifies loads into different move types
    map_optimal_move_plan_to_recommended_moves: Maps model output to move recommendations
    predict_next_move: Orchestrates the end-to-end move prediction workflow
"""

import logging
import pytz
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta

from load_scheduler.getOptimalLoad import getOptimalLoad
from app.modules.scheduler.constants import ACTIONABLE_PROFILE_TYPES
from app.modules.scheduler.sanity_check_service import check_optimal_move_plan
from app.modules.optimizer.appointment_time import get_appointment_time, get_appointment_time_v3
from app.modules.scheduler.utility import (
    map_loads_for_scheduler,
    push_load_count_to_firebase, 
    calculate_duration_from_routing_events,
    get_relative_appointment_time
)

from app.services.common_service import get_time_zone, get_carrier_preferences
from app.services.mapping_service import add_recommended_returns
from app.postgres_services.drayage_intelligence_service import get_drayage_intelligence
from app.services.redis_service import get_default_yard_location
from app.mongo_services.load_service import get_active_loads, get_moves_from_driver_order
from app.postgres_services.store_schedule_input import store_mapped_loads_in_db
from app.postgres_services.terminal_warehouse_service import get_office_hours
from app.postgres_services.schedule_plan_service import (
    store_scheduled_plan,
    get_schedule_summary,
    store_updated_plan_for_load,
    get_load_scheduled_plan
)
from app.services.load_change_event_service import set_load_review_change_event
from app.postgres_services.turn_around_time import get_waiting_time

# Configure module logger
logger = logging.getLogger(__name__)

async def classify_current_moves(loads: List[Dict[str, Any]], converted_plan_date: datetime, timeZone: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Classify current moves into pickup, delivery, return, or other categories.

    Categorizes loads based on their status and previous move type.
    Creates immutable copies of loads to avoid side effects.

    Args:
        loads: List of load dictionaries to classify

    Returns:
        Tuple containing:
            - available_loads: Loads with AVAILABLE status
            - prepulled_loads: Loads that have been pulled but not delivered
            - pending_return_loads: Loads pending return after delivery

    Raises:
        ValueError: If load data is invalid or missing required fields
    """
    if not loads:
        return [], [], []

    try:
        available_loads: List[Dict[str, Any]] = []
        prepulled_loads: List[Dict[str, Any]] = []
        pending_return_loads: List[Dict[str, Any]] = []

        removable_fields = {'driverOrder', 'driverOrderId'}

        for load in loads:
            if not isinstance(load, dict):
                raise ValueError(f"Invalid load format: {load}")

            # Create clean copy without unnecessary fields
            load_copy = {k: v for k, v in load.items() if k not in removable_fields}
            
            status = load.get('status')
            
            # Get driver order and find last completed event
            driver_order = load.get('driverOrder', [])
            last_completed_event = None
            
            if isinstance(driver_order, list):
                for event in driver_order:
                    if event.get('type') == 'DROPCONTAINER' and event.get('arrived') and event.get('departed'):
                        last_completed_event = event
            
            driver_order_id = last_completed_event
            prev_type = driver_order_id.get('prevType') if driver_order_id else None

            if status == 'AVAILABLE' or status == 'PENDING':
                appt_datetime = load_copy.get('pickupFromTime', None) or (load_copy.get("pickupTimes", [{}])[0].get("pickupFromTime") if load_copy.get("pickupTimes") else None)
                converted_appt_datetime = datetime.fromisoformat(appt_datetime).replace(tzinfo=pytz.UTC).astimezone(pytz.timezone(timeZone)) if appt_datetime else None
                if converted_appt_datetime and converted_appt_datetime.date() != converted_plan_date.date():
                    continue

                load_copy['isDropMove'] = any(
                    event.get('type') == 'DROPCONTAINER' and 
                    event.get('prevType') == 'PULLCONTAINER' 
                    for event in driver_order
                ) or False
                available_loads.append(load_copy)
            
            elif status == 'DROPCONTAINER_DEPARTED' and driver_order_id:
                if prev_type == 'PULLCONTAINER':
                    appt_datetime = load_copy.get('deliveryFromTime', None) or (load_copy.get("deliveryTimes", [{}])[0].get("deliveryFromTime") if load_copy.get("deliveryTimes") else None)
                    converted_appt_datetime = datetime.fromisoformat(appt_datetime).replace(tzinfo=pytz.UTC).astimezone(pytz.timezone(timeZone)) if appt_datetime else None
                    if converted_appt_datetime and converted_appt_datetime.date() != converted_plan_date.date():
                        continue

                    load_copy['isDropMove'] = any(
                        event.get('type') == 'DROPCONTAINER' and 
                        event.get('prevType') == 'DELIVERLOAD'
                        for event in driver_order
                    ) or False
                    prepulled_loads.append(load_copy)
                
                elif prev_type == 'DELIVERLOAD':
                    appt_datetime = load_copy.get('returnFromTime', None) if load_copy.get("returnFromTime") else None
                    converted_appt_datetime = datetime.fromisoformat(appt_datetime).replace(tzinfo=pytz.UTC).astimezone(pytz.timezone(timeZone)) if appt_datetime else None
                    if converted_appt_datetime and converted_appt_datetime.date() != converted_plan_date.date():
                        continue

                    pending_return_loads.append(load_copy)

        return available_loads, prepulled_loads, pending_return_loads

    except Exception as e:
        logger.error(f"Error classifying moves: {str(e)}")
        raise


def map_optimal_move_plan_to_recommended_moves(
    optimal_move_plan: List[Dict[str, Any]],
    loads: List[Dict[str, Any]],
    converted_plan_date: datetime,
    timeZone: str
) -> List[Dict[str, Any]]:
    """
    Map optimal move plan from prediction model to recommended moves.
    
    Args:
        optimal_move_plan: List of predicted optimal moves from model
        loads: Original load data for reference
        
    Returns:
        List of recommended moves with routing details
    """
    try:
        recommended_moves = []
        tz = pytz.timezone(timeZone)

        for optimal_move in optimal_move_plan:
            # Find matching load details
            load_details = next(
                (load for load in loads 
                 if load['reference_number'] == optimal_move['reference_number']), 
                None
            )
            
            if not load_details or not load_details.get("driverOrder"):
                continue

            moves = get_moves_from_driver_order(load_details["driverOrder"], { 'exclude_void_out': True, 'exclude_combined_move': True })
            if not len(moves):
                continue

            actionable_profile_types = ACTIONABLE_PROFILE_TYPES.get(optimal_move['predicted_next_move'], [])
            current_move = next(
                (move for move in moves 
                if any(event.get('type') in actionable_profile_types for event in move)),
                None
            )

            if not current_move:
                continue

            for profile_type in actionable_profile_types:
                # Find valid event from current move
                valid_event = next(
                    (event for event in current_move 
                    if event.get("type") == profile_type),
                    None
                )
            
                if not valid_event:
                    continue

                actual_appointment_from = None
                actual_appointment_to = None

                if profile_type == "PULLCONTAINER":
                    actual_appointment_from = load_details.get('pickupFromTime', None) or (load_details.get("pickupTimes", [{}])[0].get("pickupFromTime") if load_details.get("pickupTimes") else None)
                    actual_appointment_to = load_details.get('pickupToTime', None) or (load_details.get("pickupTimes", [{}])[0].get("pickupToTime") if load_details.get("pickupTimes") else None)
                elif profile_type == "DELIVERLOAD":
                    actual_appointment_from = load_details.get('deliveryFromTime', None) or (load_details.get("deliveryTimes", [{}])[0].get("deliveryFromTime") if load_details.get("deliveryTimes") else None)
                    actual_appointment_to = load_details.get('deliveryToTime', None) or (load_details.get("deliveryTimes", [{}])[0].get("deliveryToTime") if load_details.get("deliveryTimes") else None)
                elif profile_type == "RETURNCONTAINER":
                    actual_appointment_from = load_details.get("returnFromTime") if load_details.get("returnFromTime") else None
                    actual_appointment_to = load_details.get("returnToTime") if load_details.get("returnToTime") else None
                
                # Default 1 hour appointment window
                recommended_appointment_from = converted_plan_date.isoformat()
                recommended_appointment_to = (converted_plan_date + timedelta(hours=1)).isoformat()
                
                if actual_appointment_from:
                    # Convert actual appointment times to carrier timezone while preserving exact time
                    actual_from = datetime.fromisoformat(actual_appointment_from).replace(tzinfo=pytz.UTC).astimezone(tz)
                    actual_to = datetime.fromisoformat(actual_appointment_to).replace(tzinfo=pytz.UTC).astimezone(tz)

                    if actual_from.date() != converted_plan_date.date():
                        break

                    # Use actual appointment window times while keeping the plan date
                    recommended_appointment_from = converted_plan_date.replace(hour=actual_from.hour, minute=actual_from.minute).isoformat()
                    recommended_appointment_to = converted_plan_date.replace(hour=actual_to.hour, minute=actual_to.minute).isoformat()

                recommended_move = {
                    "reference_number": optimal_move['reference_number'],
                    "predicted_next_move": optimal_move['predicted_next_move'],
                    "move_id": current_move[0].get('moveId'),
                    "move": current_move,
                    "profile_type": profile_type,
                    "profile_id": valid_event.get("customerId") or None,
                    "profile_name": valid_event.get("company_name") or None,
                    "recommended_appointment_from": recommended_appointment_from,
                    "recommended_appointment_to": recommended_appointment_to,
                    "scheduled_appointment_from": actual_appointment_from,
                    "scheduled_appointment_to": actual_appointment_to,
                    "allEvents": [event for event in load_details.get('driverOrder', []) if not event.get('isVoidOut')],
                    "lastFreeDay": load_details.get('lastFreeDay'),
                    "containerAvailableDay": load_details.get('containerAvailableDay'),
                    "plan_date": converted_plan_date.isoformat()
                }

                recommended_moves.append(recommended_move)

        return recommended_moves

    except Exception as e:
        logger.error(f"Error mapping optimal move plan: {str(e)}")
        raise Exception(f"Failed to map optimal move plan: {str(e)}")

async def add_appointment_time_to_recommended_moves(plan_date: str, recommended_moves: List[Dict[str, Any]], recommended_hours: List[Dict[str, Any]], profile_type: str, timeZone: str) -> List[Dict[str, Any]]:
    try:
        tz = pytz.timezone(timeZone)

        for move in recommended_moves:
            for recommended_hour in recommended_hours:
                if move['reference_number'] == recommended_hour['reference_number'] and move['profile_type'] == profile_type:
                    converted_plan_date = tz.localize(datetime.strptime(plan_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0))

                    if profile_type == 'RETURNCONTAINER':
                        move['recommended_appointment_from'] = converted_plan_date.replace(hour=6, minute=0, second=0).isoformat()
                        move['recommended_appointment_to'] = converted_plan_date.replace(hour=18, minute=0, second=0).isoformat()
                    else:
                        # Ensure hour is between 0 and 23
                        from_hour = max(0, min(23, recommended_hour['converted_hour']))
                        to_hour = max(0, min(23, from_hour + 1))
                        
                        move['recommended_appointment_from'] = converted_plan_date.replace(hour=from_hour, minute=0).isoformat()
                        move['recommended_appointment_to'] = converted_plan_date.replace(hour=to_hour, minute=0).isoformat()
        
        return recommended_moves
    
    except Exception as e:
        logger.error(f"Error adding pickup appointment time to recommended moves: {str(e)}")
        raise Exception(f"Failed to add pickup appointment time to recommended moves: {str(e)}")


def map_planned_moves_for_scheduler(planned_moves: List[Dict[str, Any]], loads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Map planned moves to loads
    """
    try:
        mapped_planned_moves = []
        for move in planned_moves:
            load = next((l for l in loads if l['reference_number'] == move['reference_number']), None)
            if not load:
                continue

            planned_event = next((e for e in load['driverOrder'] if e['moveId'] == move['move_id'] and e.get("customerId") == move['profile_id']), None)

            mapped_move = {
                "plan_date": move.get('plan_date', None),
                "carrier": load.get('carrier', None),
                "reference_number": load.get('reference_number', None),
                "load_id": load.get('_id', None),
                "move": move.get('move', None),
                "move_id": move.get('move_id', None),
                "profile_id": move.get('profile_id', None),
                "profile_name": move.get('profile_name', None),
                "profile_type": move.get('profile_type', None),
                "profile_address": planned_event.get('address', {}).get('address') if planned_event else None,
                "appointment_status": 'scheduled' if move.get('scheduled_appointment_from', None) else 'need_appointment',
                "predicted_next_move": move.get('predicted_next_move', None),
                "container_no": load.get('containerNo', None),
                "container_owner": load.get('containerOwnerName', None),
                "container_size": load.get('containerSizeName', None),
                "container_type": load.get('containerTypeName', None),
                "recommended_appointment_from": move.get('recommended_appointment_from', None),
                "recommended_appointment_to": move.get('recommended_appointment_to', None),
                "scheduled_appointment_from": move.get('scheduled_appointment_from', None),
                "scheduled_appointment_to": move.get('scheduled_appointment_to', None),
                "type_of_load": load.get('type_of_load', None),
                "last_free_day": load.get('lastFreeDay', None),
                "container_available_day": load.get('containerAvailableDay', None),
                "free_return_date": load.get('freeReturnDate', None),
                "cut_off": load.get('cutOff', None),
                "terminal": load.get('terminal', None)
            }

            mapped_planned_moves.append(mapped_move)

        return mapped_planned_moves
            
    except Exception as e:
        logger.error(f"Error mapping planned moves to loads: {str(e)}")
        raise Exception(f"Failed to map planned moves to loads: {str(e)}")

async def plan_loads_with_appointments_to_scheduler(
    user_payload: Dict[str, Any],
    loads_with_appointments: List[Dict[str, Any]],
    planning_week_dates: List[datetime],
    get_partially_planned_moves: bool = False
) -> List[Dict[str, Any]]:
    """
    Plan loads with appointments to scheduler
    """
    try:
        carrier = user_payload.get('carrier')
        timeZone = user_payload.get('timeZone')
        distance_unit = user_payload.get('distanceUnit', 'mi')
        tz = pytz.timezone(timeZone)
        
        actions = {'pickup': 'PULLCONTAINER', 'delivery': 'DELIVERLOAD', 'return': 'RETURNCONTAINER'}
        prediction_lables = { 'pickup': 'Pre-Pull', 'delivery': 'Deliver', 'return': 'Return'}

        optimal_plan_for_scheduled_moves = []
        partially_planned_moves = []

        all_locations = list(set(
            event['customerId']
            for load in loads_with_appointments
            for event in load['driverOrder']
            if event.get('customerId') and event.get('type') in ['PULLCONTAINER', 'DELIVERLOAD']
        ))
        location_office_hours = await get_office_hours(carrier, all_locations, timeZone)
        waiting_times_dict = await get_waiting_time(carrier, all_locations)

        for load in loads_with_appointments:
            # Extract events and moves
            driver_order = load.get('driverOrder', [])
            events = {event.get('type'): event for event in driver_order if event.get('type') in actions.values()}

            moves = get_moves_from_driver_order(driver_order, {'exclude_void_out': True, 'exclude_combined_move': True})

            move_positions = {}
            for i, move in enumerate(moves):
                for event in move:
                    event_type = event.get('type')
                    if event_type in actions.values():
                        move_type = next(k for k,v in actions.items() if v == event_type)
                        move_positions[move_type] = i

            # Get recommended times in one pass
            recommended_times = {}
            planning_date = planning_week_dates[0]

            for move_type in actions:
                from_time = load.get(f'{move_type}FromTime')
                to_time = load.get(f'{move_type}ToTime')

                # if difference between from and to time is more than 1 day
                if from_time and to_time:
                    from_time = datetime.fromisoformat(from_time).astimezone(tz)
                    to_time = datetime.fromisoformat(to_time).astimezone(tz)
                    if (to_time - from_time).days > 1 and planning_date < to_time:
                        # from time is from time or planning date whichever is greater
                        from_time = max(from_time, planning_date)

                recommended_times[move_type] = from_time.isoformat() if from_time else None

            # Calculate delivery time if only return time exists
            if recommended_times.get('return') and events.get('DELIVERLOAD') and not recommended_times.get('delivery'):
                return_time = datetime.fromisoformat(recommended_times['return'])
                
                if move_positions.get('delivery') == move_positions.get('return'):
                    loading_time = waiting_times_dict.get((events['DELIVERLOAD'].get('customerId', ''), 'DELIVERLOAD', False), { 'waiting_time': 30 })
                    duration = calculate_duration_from_routing_events(events, 'DELIVERLOAD', 'RETURNCONTAINER', distance_unit, loading_time.get('waiting_time'))

                    delivery_office_hours = next((office_hour for office_hour in location_office_hours if office_hour['_id'] == events['DELIVERLOAD'].get('customerId', '')), None)
                    recommended_times['delivery'] = get_relative_appointment_time(recommended_times['return'], delivery_office_hours, duration, tz)
                else:
                    recommended_times['delivery'] = (return_time - timedelta(days=1)).isoformat()

            # Calculate pickup time if only delivery time exists
            if recommended_times.get('delivery') and events.get('PULLCONTAINER') and not recommended_times.get('pickup'):
                delivery_time = datetime.fromisoformat(recommended_times['delivery'])
                
                if move_positions.get('pickup') == move_positions.get('delivery'):
                    loading_time = waiting_times_dict.get((events['PULLCONTAINER'].get('customerId', ''), 'PULLCONTAINER', False), { 'waiting_time': 30 })
                    duration = calculate_duration_from_routing_events(events, 'PULLCONTAINER', 'DELIVERLOAD', distance_unit, loading_time.get('waiting_time'))

                    pickup_office_hours = next((office_hour for office_hour in location_office_hours if office_hour['_id'] == events['PULLCONTAINER'].get('customerId', '')), None)
                    recommended_times['pickup'] = get_relative_appointment_time(recommended_times['delivery'], pickup_office_hours, duration, tz)
                else:
                    recommended_times['pickup'] = (delivery_time - timedelta(days=1)).isoformat()
                
                last_free_day = datetime.fromisoformat(load.get('lastFreeDay')) if load.get('lastFreeDay') else None
                if last_free_day and last_free_day >= planning_week_dates[0] and last_free_day < datetime.fromisoformat(recommended_times['pickup']):
                    recommended_times['pickup'] = last_free_day.isoformat()

            if recommended_times.get('pickup') and events.get('DELIVERLOAD') and not recommended_times.get('delivery'):
                if move_positions.get('pickup') == move_positions.get('delivery'):
                    recommended_times['delivery'] = recommended_times['pickup']
            
            if recommended_times.get('delivery') and events.get('RETURNCONTAINER') and not recommended_times.get('return'):
                if move_positions.get('delivery') == move_positions.get('return'):
                    recommended_times['return'] = recommended_times['delivery']
                else: 
                    recommended_times['return'] = max(
                        planning_week_dates[0],
                        datetime.fromisoformat(recommended_times['delivery']) + timedelta(days=1)
                    ).isoformat()
            
            # this is for dile customer
            if not recommended_times.get('pickup') and not "delivery" in move_positions and not recommended_times.get('return') and "return" in move_positions:
                if ("pickup" in move_positions and move_positions.get('return') == move_positions.get('pickup')) or not "pickup" in move_positions:
                    recommended_times['return'] = planning_week_dates[0].isoformat()

            last_action_date = None
            for move_type in actions.keys():
                if recommended_times.get(move_type):
                    if planning_week_dates[0] <= datetime.fromisoformat(recommended_times[move_type]) < planning_week_dates[-1] + timedelta(days=1):
                        optimal_plan_for_scheduled_moves.append({
                            'reference_number': load.get('reference_number'),
                            'current_load_type': 'scheduled',
                            'predicted_next_move': prediction_lables[move_type],
                            'plan_date': datetime.fromisoformat(recommended_times[move_type]).astimezone(tz).strftime('%Y-%m-%d')
                        })
                    last_action_date = datetime.fromisoformat(recommended_times[move_type])
                elif get_partially_planned_moves:
                    plan_date = max(planning_week_dates[0], last_action_date) if last_action_date else planning_week_dates[0]

                    # plan the next move for the load on next day
                    weekdate_index = next((i for i, date in enumerate(planning_week_dates) if date.astimezone(tz).date() == plan_date.astimezone(tz).date()), None)
                    if weekdate_index is not None and weekdate_index + 1 < len(planning_week_dates):
                        plan_date = planning_week_dates[weekdate_index + 1]

                    partially_planned_moves.append({
                        'reference_number': load.get('reference_number'),
                        'next_move': actions[move_type],
                        'plan_date': plan_date.astimezone(tz).strftime('%Y-%m-%d')
                    })
                    break
                else:
                    continue

        return optimal_plan_for_scheduled_moves, partially_planned_moves
    
    except Exception as e:
        logger.error(f"Error planning loads with appointments to scheduler: {str(e)}")
        raise Exception(f"Failed to plan loads with appointments to scheduler: {str(e)}")


async def generate_scheduled_plan(
    user_payload: Dict[str, Any], 
    mapped_loads: List[Dict[str, Any]], 
    plan_date: str, 
    options: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Get scheduled plan
    """
    try:
        carrier = user_payload.get('carrier')
        timeZone = user_payload.get('timeZone')

        planning_week_dates = [plan_date + timedelta(days=i) for i in range(7) if (plan_date + timedelta(days=i)).weekday() < 5][:2]
        planned_moves = []

        drayage_intelligence = await get_drayage_intelligence(carrier, ['is_exclude_from_scheduler', 'exclude_locations_for_scheduler']);
        mapped_loads = [
            load for load in mapped_loads 
            if drayage_intelligence is None or not any(event.get('customerId') in drayage_intelligence.get('exclude_locations_for_scheduler', []) for event in load.get('driverOrder', []))
        ]

        # add scheduled moves to the recommendation plan
        loads_with_appointments = [
            load for load in mapped_loads 
            if load.get('pickupFromTime') or load.get('deliveryFromTime') or load.get('returnFromTime')
        ]

        optimal_plan_for_scheduled_moves, partially_planned_moves = await plan_loads_with_appointments_to_scheduler(
            user_payload,
            loads_with_appointments,
            planning_week_dates,
            True
        )

        # run scheduler model for non scheduled moves
        loads_without_appointments = [
            load for load in mapped_loads 
            if not load.get('pickupFromTime') and not load.get('deliveryFromTime') and not load.get('returnFromTime')
                and ((load.get('type_of_load') == 'IMPORT' and load.get('lastFreeDay') and load.get('containerNo'))
                    or (load.get('type_of_load') == 'EXPORT' and load.get('containerAvailableDay')))
        ]
        
        available_loads, prepulled_loads, pending_return_loads = await classify_current_moves(loads_without_appointments, plan_date, timeZone)
        
        for planning_date in planning_week_dates:
            
            # pass scheduled loads that are half done
            pending_scheduled_moves = [move for move in partially_planned_moves if move.get('plan_date') == planning_date.strftime('%Y-%m-%d')]
            for move in pending_scheduled_moves:
                load = next((load for load in mapped_loads if load.get('reference_number') == move.get('reference_number')), None)
                load['status'] = 'DROPCONTAINER_DEPARTED'

                if move.get('next_move') == 'DELIVERLOAD':
                    prepulled_loads.append(load)
                elif move.get('next_move') == 'RETURNCONTAINER':
                    pending_return_loads.append(load)
            
            is_any_scheduled = any([
                move for move in optimal_plan_for_scheduled_moves 
                if move.get('plan_date') == planning_date.strftime('%Y-%m-%d')
            ])

            if len(available_loads) > 0 or len(prepulled_loads) > 0 or len(pending_return_loads) > 0 or is_any_scheduled:
                # Get predictions using model
                optimal_move_plan = await getOptimalLoad(
                    carrier,
                    timeZone,
                    planning_date,
                    available_loads.copy(),
                    prepulled_loads.copy(),
                    pending_return_loads.copy(),
                )

                optimal_move_plan.extend([
                    move for move in optimal_plan_for_scheduled_moves 
                    if move.get('plan_date') == planning_date.strftime('%Y-%m-%d')
                ])

                # Map optimal loads to recommended moves
                recommended_moves = map_optimal_move_plan_to_recommended_moves(
                    optimal_move_plan, mapped_loads, planning_date, timeZone
                )

                # sanity check for recommended moves
                recommended_moves = check_optimal_move_plan(recommended_moves)

                # if no recommended moves, skip this planning date
                if len(recommended_moves) == 0:
                    continue

                # predict appointment time for these loads
                reference_numbers = list(set(move['reference_number'] for move in recommended_moves))
                filtered_loads = [load for load in mapped_loads if load['reference_number'] in reference_numbers]

                if options.get('use_vrp'):
                    recommended_moves = await get_appointment_time_v3(
                        user_payload,
                        planning_date,
                        filtered_loads,
                        recommended_moves,
                        { 'store_plan': options.get('store_plan'), 'use_driver_schedule': options.get('use_driver_schedule') }
                    )
                else:
                    recommended_moves = await get_appointment_time(
                        user_payload,
                        planning_date,
                        filtered_loads,
                        recommended_moves,
                        { 'store_plan': options.get('store_plan'), 'use_driver_schedule': options.get('use_driver_schedule') }
                    )

                planned_moves.extend(recommended_moves)

                # pass remaining loads to next planning date
                # update status of loads that are half done
                paritally_done_loads = []
                recommended_moves_reference_numbers = list(set([move['reference_number'] for move in recommended_moves]))
                paritally_done_loads.extend([load for load in available_loads if load['reference_number'] in recommended_moves_reference_numbers])
                paritally_done_loads.extend([load for load in prepulled_loads if load['reference_number'] in recommended_moves_reference_numbers])

                available_loads = [load for load in available_loads if load['reference_number'] not in recommended_moves_reference_numbers]
                prepulled_loads = [load for load in prepulled_loads if load['reference_number'] not in recommended_moves_reference_numbers]
                pending_return_loads = [load for load in pending_return_loads if load['reference_number'] not in recommended_moves_reference_numbers]

                for load in paritally_done_loads:
                    load_actions = [move.get('profile_type') for move in recommended_moves if move.get('reference_number') == load.get('reference_number')]
                    load['status'] = 'DROPCONTAINER_DEPARTED'
                    
                    if 'RETURNCONTAINER' in load_actions:
                        continue
                    elif 'DELIVERLOAD' in load_actions:
                        pending_return_loads.append(load)
                    elif 'PULLCONTAINER' in load_actions:
                        prepulled_loads.append(load)

        return planned_moves
    
    except Exception as e:
        logger.error(f"Error getting scheduled plan: {str(e)}")
        raise Exception(f"Failed to get scheduled plan: {str(e)}")


async def predict_next_move(user_payload: Dict[str, Any], plan_date: str, options: Dict[str, Any] = {}) -> Dict[str, Any]:
    """
    Predict the next optimal move for a set of loads.
    
    Orchestrates the end-to-end prediction workflow:
    1. Validates inputs
    2. Fetches load data
    3. Transforms data for model
    4. Gets predictions
    5. Maps results to moves
    
    Args:
        carrier: Carrier ID to fetch and predict loads for
        plan_date: Date to plan loads for in format YYYY-MM-DD
        
    Returns:
        Dictionary containing carrier, plan date and recommended moves
        
    Raises:
        ValueError: If input parameters are invalid
        Exception: For any other errors during prediction
    """
    try:
        carrier = user_payload.get('carrier')
        
        # Validate inputs
        if not carrier:
            raise ValueError("Carrier ID is required")
        
        # get default yard location
        default_yard_locations = await get_default_yard_location(carrier)
        carrier_preferences = await get_carrier_preferences(carrier)

        user_payload['default_yard_locations'] = default_yard_locations
        user_payload['distanceUnit'] = carrier_preferences.get('distanceUnit', 'mi')
        
        # get time zone and convert plan date to local timezone
        timeZone = await get_time_zone(carrier)
        user_payload['timeZone'] = timeZone
        
        tz = pytz.timezone(timeZone)
        plan_date = (
            datetime.fromisoformat(plan_date)
            .replace(tzinfo=pytz.UTC)
            .astimezone(tz)
            .replace(hour=0, minute=0, second=0, microsecond=0)
        )
        
        # Fetch and transform loads
        loads = await get_active_loads(carrier, limit=2000)
        if not loads:
            logger.info(f"No available loads found for carrier {carrier}")
            return {
                "carrier": carrier,
                "plan_date": plan_date.isoformat(),
                "recommended_moves_count": 0
            }
        
        # map loads for scheduler
        mapped_loads = map_loads_for_scheduler(loads)

        try:
            mapped_loads = await add_recommended_returns(user_payload, plan_date.strftime('%Y-%m-%d'), mapped_loads)
        except Exception as e:
            logger.error(f"Error adding recommended returns: {str(e)}")
        
        # store mapped loads input in db
        if options.get('store_plan'):
            await store_mapped_loads_in_db(mapped_loads, {"carrier": carrier, "plan_date": plan_date })
        
        planned_moves = await generate_scheduled_plan(
            user_payload,
            mapped_loads,
            plan_date,
            options
        )

        # store generated plan to the database
        if options.get('store_plan'):
            mapped_planned_moves = map_planned_moves_for_scheduler(planned_moves, loads)
            if len(mapped_planned_moves) > 0:
                await store_scheduled_plan(mapped_planned_moves)

        return {
            "carrier": carrier,
            "plan_date": plan_date.isoformat(),
            "recommended_moves_count": len(planned_moves) if planned_moves else 0
        }
        
    except Exception as e:
        logger.error(f"Error predicting next move: {str(e)}")
        raise Exception(f"Failed to predict next move: {str(e)}")














def load_update_action_validate(old_load: Dict[str, Any], load: Dict[str, Any]) -> Tuple[bool, str, str, str]:
    try:
        # check if pickup appointment is updated
        if old_load.get('pickupTimes', [{}])[0].get('pickupFromTime') != load.get('pickupTimes', [{}])[0].get('pickupFromTime'):
            return True, 'pickup_appt'
        
        # check if delivery appointment is updated
        if old_load.get('deliveryTimes', [{}])[0].get('deliveryFromTime') != load.get('deliveryTimes', [{}])[0].get('deliveryFromTime'):
            return True, 'delivery_appt'
        
        # check if any other related fields are updated
        other_related_fields = [
            'returnFromTime',
            'lastFreeDay', 
            'freeReturnDate',
            'containerAvailableDay',
            'cutOff',
            'appointmentNo',
        ]
        if load.get('type_of_load') == 'IMPORT':
            other_related_fields.append('containerNo')

        for field in other_related_fields:
            if old_load.get(field) != load.get(field):
                return True, field
        
        # check if load status is changed to available or dropped
        if ((old_load.get('status', '') != 'AVAILABLE' and load.get('status', '') == 'AVAILABLE') or 
            (old_load.get('status', '') != 'DROPCONTAINER_DEPARTED' and load.get('status', '') == 'DROPCONTAINER_DEPARTED')):
            return True, 'status'
        
        return False, None
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to validate load update action: {str(e)}")

## Replan Modified Move
def is_valid_action_for_replan(action: str, old_load: Dict[str, Any], load: Dict[str, Any]) -> bool:
    try:
        # First check simple action types that always return True
        if action in ['load.created', 'load.deleted', 'load.completed', 'routing.event.address.updated', 'routing.event.added', 'routing.event.deleted']:
            return True
        
        if action == 'routing.event.status.changed' and load.get('status') == 'DROPCONTAINER_DEPARTED':
            return True
        
        if action == 'load.updated':
            is_action_changed = load_update_action_validate(old_load, load)
            return is_action_changed
        
        return False
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to check if action is valid for replan: {str(e)}")


async def replan_modified_move(carrier: str, action: str, load_payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        old_load = load_payload.get('oldData')
        load = load_payload.get('updatedData')
        
        is_valid_action = is_valid_action_for_replan(action, old_load, load)

        if not is_valid_action:
            return

        # get schedule summary
        schedule_summary = await get_schedule_summary(carrier)
        plan_date = schedule_summary.get('plan_date')

        user_payload = { 'carrier': carrier }
        
        # get default yard location
        default_yard_locations = await get_default_yard_location(carrier)
        user_payload['default_yard_locations'] = default_yard_locations

        carrier_preferences = await get_carrier_preferences(carrier)
        user_payload['distanceUnit'] = carrier_preferences.get('distanceUnit', 'mi')
        
        # get time zone and convert plan date to local timezone
        timeZone = await get_time_zone(carrier)
        user_payload['timeZone'] = timeZone

        tz = pytz.timezone(timeZone)
        plan_date = plan_date.astimezone(tz)
        
        is_missing_container_no = load.get('type_of_load') == 'IMPORT' and not load.get('containerNo')
        is_dropcontainer_departed = action == 'routing.event.status.changed' and load.get('status') == 'DROPCONTAINER_DEPARTED'
        is_valid_action = action in ['load.created', 'load.updated', 'routing.event.address.updated', 'routing.event.added', 'routing.event.deleted'] or is_dropcontainer_departed

        mapped_planned_moves = []
        if is_valid_action and not is_missing_container_no:
            # map loads for scheduler
            mapped_loads = map_loads_for_scheduler([load])

            # add recommended returns if returnlocation is not present
            if not load.get('emptyOriginName'):
                mapped_loads = await add_recommended_returns(user_payload, plan_date.strftime('%Y-%m-%d'), mapped_loads)

            await store_mapped_loads_in_db(mapped_loads, {"carrier": carrier, "plan_date": plan_date })

            planned_moves = await generate_scheduled_plan(
                user_payload,
                mapped_loads,
                plan_date,
                { 'store_plan': True, 'use_driver_schedule': True, 'use_vrp': False }
            )

            mapped_planned_moves = map_planned_moves_for_scheduler(planned_moves, [load])

        plan_id = schedule_summary.get('id')
        reference_number = load.get('reference_number')
        terminal = load.get('terminal', '')
        load_old_planned = []
        if reference_number and plan_id:
            load_old_planned = await get_load_scheduled_plan(carrier, reference_number, plan_id)

        inserted_moves = await store_updated_plan_for_load(schedule_summary, load.get('reference_number'), mapped_planned_moves)

        print(f"Planned move: {load.get('reference_number')}")
        
        if len(inserted_moves) > 0:
            # store change action in db
            await set_load_review_change_event(
                carrier,
                plan_id,
                action,
                old_load,
                load
            )

        # push update to firebase
        await push_load_count_to_firebase(carrier, reference_number, load_old_planned, list(inserted_moves), tz, terminal)
        
        return
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to replan modified move: {str(e)}")



async def retrieve_scheduled_moves_for_optimizer(
    user_payload: Dict[str, Any],
    plan_branch: list,
    converted_plan_date: datetime,
    plan_from_time: datetime,
    plan_to_time: datetime,
    shift: str = None
):
    try:
        carrier = user_payload.get('carrier')
        timeZone = user_payload.get('timeZone')

        scheduled_loads = await get_active_loads(
            carrier,
            2000,
            plan_branch,
            { "ignored_pending_loads": True }
        )

        # filter scheduled loads if type_of_loads is IMPORT then containerNo should be present
        scheduled_loads = [
            load for load in scheduled_loads
            if (load.get('type_of_load') == 'IMPORT' and load.get('containerNo')) or 
               load.get('type_of_load') == 'EXPORT'
        ]
        
        mapped_loads = map_loads_for_scheduler(scheduled_loads)
        
        formatted_plan_date = converted_plan_date.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d')
        mapped_loads = await add_recommended_returns(user_payload, formatted_plan_date, mapped_loads)

        planning_week_dates = [converted_plan_date]
        if shift:
            planning_week_dates.append(converted_plan_date + timedelta(days=1))
        
        scheduled_loads, _ = await plan_loads_with_appointments_to_scheduler(
            user_payload=user_payload,
            loads_with_appointments=mapped_loads,
            planning_week_dates=planning_week_dates
        )

        scheduled_plans = map_optimal_move_plan_to_recommended_moves(
            optimal_move_plan=scheduled_loads,
            loads=mapped_loads,
            converted_plan_date=converted_plan_date,
            timeZone=timeZone
        )

        scheduled_plans = check_optimal_move_plan(scheduled_plans)

        if shift:
            scheduled_plans = [
                move for move in scheduled_plans
                if move.get('scheduled_appointment_from') and 
                    plan_to_time >= datetime.fromisoformat(move.get('scheduled_appointment_from')) and
                    plan_from_time <= datetime.fromisoformat(move.get('scheduled_appointment_to'))
            ]

        return scheduled_plans
    
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to retrieve scheduled moves for optimizer: {str(e)}")