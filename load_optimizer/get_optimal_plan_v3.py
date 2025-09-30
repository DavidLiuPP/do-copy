import pandas as pd
import logging
import pytz
import asyncio
from copy import deepcopy
import time
import asyncio
import json
from typing import Dict, Any, List
from datetime import datetime, timedelta

from vrp_optimizer.optimizer import Optimizer
from vrp_optimizer.assumptions import LATE_ARRIVAL_MINUTES
from vrp_optimizer.helpers import calculate_distance, minute_from_distance, get_all_driver_default_locations, get_yard_data, get_warehouse_visits
from vrp_optimizer.services import handle_assigned_moves, get_completion_time, get_current_plan
from vrp_optimizer.routing_distance import get_location_distance_matrix_bulk, get_unique_coordinates
from vrp_optimizer.unplanned_moves import get_unplanned_moves

from app.services.redis_service import get_default_yard_location
from app.postgres_services.configurations_service import get_equipment_validations, get_container_sizes, get_container_types
from app.postgres_services.drayage_intelligence_service import get_drayage_intelligence

from load_optimizer.get_optimal_plan_v2 import get_formatted_time
from load_optimizer.work_rotation import get_driver_history

from app.modules.optimizer.constants import (
    ONE_DAY_IN_MINUTES,
    CARRIER_CONFIGS,
    PLANNING_ASSUMPTIONS
)
from app.modules.optimizer.in_day_service import (
    get_restrict_load_move,
    get_schedule_info_from_driver_schedules,
    update_actionable_moves_for_inday_plan
)

from vrp_optimizer.chassis_capacity_helpers import map_chassis_into_moves
from app.services.redis_service import get_carrier_settings

logger = logging.getLogger(__name__)

def get_minute(date, timezone):
    dt = datetime.fromisoformat(date).astimezone(pytz.timezone(timezone))
    return dt.hour * 60 + dt.minute

def get_days_difference(date1, date2, timezone):
    dt1 = datetime.fromisoformat(date1).astimezone(pytz.timezone(timezone))
    dt2 = datetime.fromisoformat(date2).astimezone(pytz.timezone(timezone))
    return (dt2.date() - dt1.date()).days

def get_minute_from_time(time):
    return datetime.strptime(time, "%H:%M").hour * 60 + datetime.strptime(time, "%H:%M").minute

async def get_and_map_container_sizes_and_types(carrier: str, moves: List[Dict[str, Any]]):
    try:
        container_sizes = list(set([m.get('containerSize') for m in moves if m.get('containerSize')]))
        container_types = list(set([m.get('containerType') for m in moves if m.get('containerType')]))

        if not container_sizes and not container_types:
            return moves

        container_sizes = await get_container_sizes(carrier, container_sizes)
        container_types = await get_container_types(carrier, container_types)

        container_sizes_map = {c['_id']: c for c in container_sizes}
        container_types_map = {c['_id']: c for c in container_types}

        for move in moves:
            if container_sizes_map.get(move.get('containerSize')):
                move['container_size_label'] = container_sizes_map.get(move.get('containerSize')).get('label')
            if container_types_map.get(move.get('containerType')):
                move['container_type_label'] = container_types_map.get(move.get('containerType')).get('label')

        return moves
    except Exception as e:
        logger.error(f"Error getting container sizes and types: {str(e)}")
        return moves

async def get_optimal_plan_v3(
    user_payload: Dict[str, Any],
    actionable_moves: List[Dict[str, Any]],
    drivers: List[Dict[str, Any]],
    converted_plan_date: datetime,
    additional_depot_locations: List[Dict[str, Any]] = [],
    return_schedule: bool = False,
    time_limit: int = 5 * 60,
    branch: Any = None,
    shift: Any = None,
    behind_schedule_moves: List[Dict[str, Any]] = [],
    is_in_day_plan: bool = False,
    driver_schedules: List[Dict[str, Any]] = [],
    invalid_moves: List[Dict[str, Any]] = [],
    dispatch_plan_params: Dict[str, Any] = {},
    add_to_existing_plan: bool = False
):
    try:
        if len(drivers) == 0:
            return {
                'optimal_plan': [],
                'invalid_moves': [],
                'driver_schedule': {},
                'optimizer_input': {}
            }

        if is_in_day_plan:
            time_limit = 3 * 60

        carrier = user_payload.get('carrier')
        timeZone = user_payload.get('timeZone')
        distance_unit = user_payload.get('distanceUnit', 'mi')

        user_settings = await get_equipment_validations(carrier)
        equipment_validations = user_settings.get('equipment_validations', [])

        carrier_settings = await get_carrier_settings(carrier)

        CARRIER_CONFIG = CARRIER_CONFIGS.get(carrier, {})
        plan_start_minute = CARRIER_CONFIG.get('plan_times', [0, 1440])[0]
        plan_end_minute = CARRIER_CONFIG.get('plan_times', [0, 1440])[1]
        route_types_for_in_day = CARRIER_CONFIG.get('route_types_for_in_day', {}) if is_in_day_plan else None

        # map driver data to vehicle data
        vehicle_data = []
        max_vehicle_end_minute = 0
        for v in drivers:

            if not v.get('depot_customer_id'):
                continue

            lat = v.get('depot_location', {}).get('lat', 0)
            lng = v.get('depot_location', {}).get('lng', 0)
            depot_hash_key = f"{lat}-{lng}-{v.get('depot_customer_id', '')}"
            start_minute = int(get_minute_from_time(v.get('start_time', "00:00"))) if not v.get('missing_working_days_hours', False) else plan_start_minute
            end_minute = int(get_minute_from_time(v.get('end_time', "23:59"))) if not v.get('missing_working_days_hours', False) else plan_end_minute

            # if end time is less than start time, it means the driver is working on next day
            if end_minute < start_minute:
                end_minute += ONE_DAY_IN_MINUTES

            current_time = datetime.now(pytz.timezone(timeZone))
            is_planning_for_today = converted_plan_date.date() == current_time.date()

            effective_plan_start_minute = plan_start_minute
            if is_planning_for_today:
                current_time_minutes = current_time.hour * 60 + current_time.minute
                effective_plan_start_minute = max(start_minute, current_time_minutes)
            
            start_minute = max(start_minute, effective_plan_start_minute)

            skip_driver_for_optimizer = False

            if start_minute >= end_minute:
                # This scenario will happen when driver's working hours are passed but the shift is still active.
                # Example: Driver can work max upto 10 PM, but the shift is ending on 2 AM next day, and we are replanning on the same day at 11 PM. 
                print("Incorrect shift time for driver: ", v.get('_id', ''))
                start_minute = plan_start_minute
                end_minute = plan_end_minute

                skip_driver_for_optimizer = True

            max_vehicle_end_minute = max(max_vehicle_end_minute, end_minute)

            data = {
                **v,
                'start_minute': start_minute,
                'end_minute': end_minute,
                'max_working_minutes': int(v.get('total_shift_hours', 14.0) * 60),
                'max_miles_per_move': int(v.get('max_mileage', 10000) or 10000),
                'min_miles_per_move': int(v.get('min_mileage', 0) or 0),
                'restricted_locations': list(filter(None, v.get('restricted_locations', []))),
                'start_location': v.get('depot_customer_id'),
                'end_location': v.get('depot_customer_id'),
                'depot_hash_key': depot_hash_key,
                'manual_location': [v.get('depot_location', {}).get('lat', 0), v.get('depot_location', {}).get('lng', 0)],
                'new_terminal': v.get('new_terminal', False),
                'owner_score': int(v.get('owner_score', 1) or 1)
            }

            driver_route_types = []
            if route_types_for_in_day:
                driver_tags = set(v.get('tags', []))
                if any(tag in route_types_for_in_day.get('Local', []) for tag in driver_tags):
                    driver_route_types.append('Local')
                if any(tag in route_types_for_in_day.get('Highway', []) for tag in driver_tags):
                    driver_route_types.append('Highway')
            else:
                if v.get('local'):
                    driver_route_types.append('Local')
                if v.get('highway'):
                    driver_route_types.append('Highway')
            data['load_route_type'] = driver_route_types

            if skip_driver_for_optimizer:
                data['skip_driver_for_optimizer'] = True

            vehicle_data.append(data)
        
        max_time_dimension_minutes = max(plan_end_minute, max_vehicle_end_minute)

        if is_in_day_plan:
            actionable_moves = update_actionable_moves_for_inday_plan(actionable_moves, behind_schedule_moves)


        fixed_move_field = 'is_manually_planned' if add_to_existing_plan else 'is_active_move'
        assigned_moves = [m for m in actionable_moves if m.get(fixed_move_field, False)]
        actionable_moves = [m for m in actionable_moves if not m.get(fixed_move_field, False)]

        if not add_to_existing_plan:
            # Remove assigned driver from actionable moves if any (This Case will only be there while planning from scratch as we are trying to assign new driver)
            for m in actionable_moves:
                m.pop('assigned_driver', None)

        all_assigned_moves = deepcopy(assigned_moves)

        driver_default_locations = get_all_driver_default_locations(drivers)

        fixed_plan = []
        restrict_load_move_and_that_driver = []
        if is_in_day_plan:
            vehicle_data, additional_depot_locations = await get_schedule_info_from_driver_schedules(vehicle_data, additional_depot_locations, driver_schedules)
            # get in-day load (is_rejected is true)
            moves = await get_restrict_load_move(carrier, converted_plan_date, branch)
            restrict_load_move_and_that_driver = rejected_driver_moves(moves) if moves else []
        else:
            vehicle_data, additional_depot_locations, fixed_plan = handle_assigned_moves(
                user_payload,
                vehicle_data,
                driver_default_locations,
                additional_depot_locations,
                all_assigned_moves,
                plan_date=converted_plan_date,
                plan_start_minute=plan_start_minute
            )

        
        ALL_DEPOT_LOCATIONS = driver_default_locations + additional_depot_locations
        default_yard_locations = await get_default_yard_location(carrier, False, branch)
        ALL_YARD_LOCATIONS = [get_yard_data(yard) for yard in default_yard_locations]

        moves, skipped_moves = await map_actionable_moves_for_optimizer(
            user_payload,
            actionable_moves,
            timeZone,
            distance_unit,
            converted_plan_date,
            plan_end_minute,
            plan_start_minute,
            ALL_YARD_LOCATIONS
        )
        print(f"Skipped moves: {len(skipped_moves)}")
        
        for m in skipped_moves:
            invalid_moves.append({
                'reference_number': m.get('reference_number', ''),
                'reason': m['reason']
            })
        
        optimal_plan = []
        optimizer_input = {}
        optimizer = None

        if equipment_validations:
            # get container size and type for every moves
            moves = await get_and_map_container_sizes_and_types(carrier, moves)

        # Get the optimal plan if there are moves
        if moves:
            start_time = time.time()
            optimal_plan = []
            driver_schedule = {}

            total_locations = moves + ALL_DEPOT_LOCATIONS + ALL_YARD_LOCATIONS
            unique_coords = get_unique_coordinates(total_locations)
            location_distance_matrix = await get_location_distance_matrix_bulk(carrier, unique_coords, distance_unit)


            mapped_chassis_limits = {}
            if carrier_settings.get('isChassisInventoryByYardEnabled', False):
                moves, mapped_chassis_limits = await map_chassis_into_moves(
                    carrier=carrier, 
                    plan_date=converted_plan_date,
                    nodes=moves, 
                    yards=ALL_YARD_LOCATIONS, 
                    equipment_validations=equipment_validations, 
                    location_distance_matrix=location_distance_matrix
                )

            # Run the optimization in a thread pool to prevent blocking
            try:
                # We'll not pass the drivers who can't work in the planning time
                filtered_vehicle_data = [v for v in vehicle_data if not v.get('skip_driver_for_optimizer', False)]

                # Fetch driver history for rotating the work
                start_history_time = time.time()
                driver_history = await get_driver_history(carrier, converted_plan_date, timeZone, vehicle_data)
                end_history_time = time.time()
                print(f"Time taken to get driver history: {end_history_time - start_history_time} seconds")

                moves_copy = deepcopy(moves)
                d_schedule = {}

                optimizer_input = {
                    "moves": moves_copy,
                    "drivers": filtered_vehicle_data,
                    "depot_locations": ALL_DEPOT_LOCATIONS,
                    "yard_locations": ALL_YARD_LOCATIONS,
                    "timezone": timeZone,
                    "distance_unit": distance_unit,
                    "time_limit": time_limit,
                    "equipment_validations": equipment_validations,
                    "location_distance_matrix": location_distance_matrix,
                    "plan_start_minute": plan_start_minute,
                    "plan_end_minute": plan_end_minute,
                    "carrier_id": user_payload.get('carrier'),
                    "plan_date": converted_plan_date,
                    "max_time_dimension_minutes": max_time_dimension_minutes,
                    "chassis_limits": mapped_chassis_limits,
                    "driver_history": driver_history,
                    "is_in_day_plan": is_in_day_plan,
                    "dispatch_plan_params": dispatch_plan_params
                }

                optimizer = Optimizer(
                    moves = moves_copy,
                    drivers = filtered_vehicle_data,
                    depot_locations = ALL_DEPOT_LOCATIONS,
                    yard_locations = ALL_YARD_LOCATIONS,
                    timezone = timeZone,
                    distance_unit = distance_unit,
                    time_limit = time_limit,
                    equipment_validations = equipment_validations,
                    location_distance_matrix = location_distance_matrix,
                    plan_start_minute = plan_start_minute,
                    plan_end_minute = plan_end_minute,
                    carrier_id = user_payload.get('carrier'),
                    plan_date = converted_plan_date,
                    max_time_dimension_minutes = max_time_dimension_minutes,
                    chassis_limits = mapped_chassis_limits,
                    driver_history = driver_history,
                    is_in_day_plan=is_in_day_plan,
                    dispatch_plan_params=dispatch_plan_params,
                    restrict_load_move_and_that_driver=restrict_load_move_and_that_driver
                )
                
                # Run the OR-Tools optimization in a thread pool to prevent blocking
                loop = asyncio.get_event_loop()
                _optimal_plan, _d_schedule = await loop.run_in_executor(
                    None,  # Use default thread pool
                    optimizer.optimize
                )

                optimal_plan.extend(_optimal_plan)
                d_schedule.update(_d_schedule)

                if return_schedule and optimal_plan and d_schedule:
                    driver_schedule = d_schedule
            
            except Exception as e:
                logger.error(f"Error during optimization for carrier: {carrier}: {str(e)}")
            
            end_time = time.time()
            print(f"Time taken by vrp optimizer: {end_time - start_time} seconds for {len(moves)} moves")

        # Return the current assignements as new plan if no new optimal plan is found
        if not optimal_plan:
            
            if is_in_day_plan or not all_assigned_moves:
                return {
                    'optimal_plan': [],
                    'invalid_moves': invalid_moves,
                    'driver_schedule': {},
                    'optimizer_input': optimizer_input
                }

            current_plan = get_current_plan(all_assigned_moves)
            formatted_current_plan = map_fixed_plan_to_driver_plan(user_payload, current_plan, drivers, timeZone, converted_plan_date, distance_unit)
            return {
                'optimal_plan': formatted_current_plan,
                'invalid_moves': invalid_moves,
                'driver_schedule': {},
                'optimizer_input': optimizer_input
            }

        for m in actionable_moves:
            m['is_new_move'] = True

        optimal_plan = map_route_summary_to_driver_plan(user_payload, assigned_moves + actionable_moves, optimal_plan, drivers, timeZone, converted_plan_date)

        if is_in_day_plan:
            return {
            'optimal_plan': optimal_plan,
            'invalid_moves': invalid_moves,
            'driver_schedule': driver_schedule,
            'optimizer_input': optimizer_input
        }

        formatted_fixed_plan = map_fixed_plan_to_driver_plan(user_payload, fixed_plan, drivers, timeZone, converted_plan_date, distance_unit)

        # get unplanned moves and the reason for unplanned moves
        if len(moves_copy) > len(optimal_plan):
            planned_reference_numbers = [plan['reference_number'] for plan in optimal_plan]
            unplanned_reference_numbers = [m['reference_number'] for m in moves_copy if m.get('reference_number') not in planned_reference_numbers]

            _invalid_moves = get_unplanned_moves(optimizer, unplanned_reference_numbers)
            invalid_moves.extend(_invalid_moves)

        return {
            'optimal_plan': formatted_fixed_plan + optimal_plan,
            'invalid_moves': invalid_moves,
            'driver_schedule': driver_schedule,
            'optimizer_input': optimizer_input
        }
    except Exception as e:
        logger.error(f"Failed to get optimal plan v3 for carrier: {carrier}: {str(e)}")
        raise Exception(f"Failed to get optimal plan v3: {str(e)}")

async def map_actionable_moves_for_optimizer(
    user_payload: Dict[str, Any],    
    actionable_moves: List[Dict[str, Any]],
    timeZone: str,
    distance_unit: str,
    plan_date: datetime,
    plan_end_minute: int,
    plan_start_minute: int,
    yards: List[Dict[str, Any]]
):
    try:
        carrier = user_payload.get('carrier')
        moves = []
        skipped_moves = []

        last_visit_locations = CARRIER_CONFIGS.get(carrier, {}).get('last_visit_locations', [])
        
        for move in actionable_moves:
            try:
                appointment = None
                has_invalid_appts = False
                appointment_diff = float('inf')
                
                travel_time = (
                    move.get('total_travel_time')
                    if move.get('total_travel_time') is not None
                    else sum(
                        minute_from_distance(ev.get('distance', 0), distance_unit)
                        for ev in move.get('move')
                    )
                )
                waiting_time_at_locations = move.get('waiting_time', 0)
                total_duration = travel_time + waiting_time_at_locations
                
                # if driver is early to the appointment, he will wait.
                early_arrival_waiting = 0

                # check if the total duration is more than 14 hours
                if total_duration > 14 * 60:
                    copied_move = move.copy()
                    copied_move['reason'] = 'MULTIDAY_TRIP' 
                    skipped_moves.append(copied_move)
                    continue

                for event in move.get('move'):
                    if appointment:
                        # Add travel time to existing appointment window
                        minute_to_reach = event.get('travel_time', 0) if event.get('travel_time') is not None else minute_from_distance(event.get('distance', 0), distance_unit)
                        appointment[0] += minute_to_reach
                        appointment[1] += minute_to_reach
                    
                    # Get appointment times if they exist
                    if event.get('appointment_from'):
                        # Calculate appointment window accounting for queue waiting time
                        from_date_day_diff = get_days_difference(plan_date.isoformat(), event.get('appointment_from'), timeZone)
                        to_date_day_diff = get_days_difference(plan_date.isoformat(), event.get('appointment_to'), timeZone)

                        if from_date_day_diff < 0 and to_date_day_diff < 0:
                            # Skip the move if it's appointment was in the past
                            has_invalid_appts = True
                            break

                        event_appt_from = max(
                            get_minute(event.get('appointment_from'), timeZone) +
                                (from_date_day_diff * ONE_DAY_IN_MINUTES),
                            0
                        )
                        event_appt_to = max(
                            get_minute(event.get('appointment_to'), timeZone) +
                                (to_date_day_diff * ONE_DAY_IN_MINUTES),
                            0
                        )
                    
                        appointment_diff = min(appointment_diff, event_appt_to - event_appt_from)

                        if appointment:
                            # Check if appointment windows are compatible
                            will_driver_reach_after_appointment = appointment[0] >= (event_appt_to + 30)
                            
                            # Allow Driver to reach 6 hours before appointment
                            will_driver_reach_before_appointment = event_appt_from >= (appointment[1] + 360)
                            
                            if will_driver_reach_before_appointment or will_driver_reach_after_appointment:
                                if  appointment:
                                    # shift the appointment window by 15 minutes and reduce the appointment window by the least difference between the appointment window
                                    appointment[0] = appointment[0] - 15 if appointment[0] > 0 else 0
                                    appointment[1] = appointment[0] + appointment_diff

                                    # add waiting time to the appointment window
                                    wait_time = event.get('waiting_time', 0)
                                    appointment[0] += wait_time
                                    appointment[1] += wait_time

                                    # use the assumed appointment window
                                    continue
                                
                                has_invalid_appts = True
                                break

                            # Calculate waiting time on road
                            if event_appt_from - appointment[1] > 0:
                                additional_waiting_time = event_appt_from - appointment[1] + 30 # 30 minutes buffer
                                event['early_arrival_waiting'] = additional_waiting_time
                                early_arrival_waiting += additional_waiting_time


                            appointment[0] = max(appointment[0], event_appt_from)
                            
                            if appointment[0] > event_appt_to:
                                # If driver is going to be late than instead of "to window" of appt, take current max arrival time.
                                event_appt_to = appointment[1]
                            appointment[1] = min(appointment[0] + appointment_diff, event_appt_to)
                        else:
                            # Initialize first appointment window
                            appointment = [event_appt_from, event_appt_to]

                    # Add waiting time if there's an appointment window
                    if appointment:
                        wait_time = event.get('waiting_time', 0)
                        appointment[0] += wait_time
                        appointment[1] += wait_time

                if has_invalid_appts:
                    copied_move = move.copy()
                    copied_move['reason'] = 'INVALID_APPOINTMENT_WINDOW'
                    skipped_moves.append(copied_move)
                    continue

                expected_from_minute = appointment[0] if appointment else 0
                expected_to_minute = appointment[1] if appointment else ONE_DAY_IN_MINUTES

                if expected_from_minute >= plan_end_minute + LATE_ARRIVAL_MINUTES:
                    copied_move = move.copy()
                    copied_move['reason'] = 'NEXT_DAY_TRIP'
                    skipped_moves.append(copied_move)
                    continue
                
                move_data = {
                    **move,
                    "load_id": move.get('_id', ''),
                    "_id": move.get('reference_number', ''),
                    "start_loc": [
                        move.get('move', [])[0].get('address', {}).get("lat", 0),
                        move.get('move', [])[0].get('address', {}).get("lng", 0)
                    ],
                    "end_loc": [
                        move.get('move', [])[-1].get('address', {}).get("lat", 0),
                        move.get('move', [])[-1].get('address', {}).get("lng", 0)
                    ],
                    "start_event": move.get('move', [])[0],
                    "end_event": move.get('move', [])[-1],
                    "locations": [e.get('customerId', '') or '' for e in move.get('move') if e.get('customerId', '')],
                    "route_distance": max([ e.get('distance', 0) for e in move.get('move')]),
                    "minutes_on_road": travel_time,
                    "early_arrival_waiting": early_arrival_waiting,
                    "total_waiting_time": move.get('waiting_time'),
                    'total_duration': travel_time + early_arrival_waiting + move.get('waiting_time', 0),
                    "expected_from_minute": int(expected_from_minute),
                    "expected_to_minute": int(expected_to_minute),
                    'is_highway': move.get('routeType') == 'Highway',
                    'is_local': move.get('routeType') == 'Local',
                    'route_type': move.get('routeType', '')
                }

                if move.get('is_assigned_move', False):
                    move_data['assigned_driver'] = move.get('assigned_driver', '')

                warehouse_visits = get_warehouse_visits(move.get('move', []), [yard.get('depot_customer_id') for yard in yards if yard.get('depot_customer_id')]) 

                if warehouse_visits:
                    move_data['warehouse_ids'] = list(filter(None, [e.get('customerId') for e in warehouse_visits]))
                
                if move.get('suggested_driver', None) or move.get('assigned_driver', None):
                    move_data['suggested_driver'] = move.get('assigned_driver', move.get('suggested_driver', ''))

                if move.get('is_free_flow_move', False):
                    move_data['last_move_by_driver'] = True
                    move_data['is_free_flow_move'] = True

                if len(last_visit_locations) > 0:
                    is_last_visit_location_present = any(m for m in move.get('move', []) if m.get('customerId') and m.get('customerId') in last_visit_locations)
                    if is_last_visit_location_present:
                        move_data['last_move_by_driver'] = True

                if move.get('move', []):
                    load_moves = move.get('move', [])
                    event_types = [event.get('type') for event in load_moves if event.get('type')]
                    customer_ids = [event.get('customerId') for event in load_moves if event.get('customerId')]
                    # First try DELIVERLOAD events
                    over_weight_state = [f"{event.get('state').upper()}, {event.get('country').upper()}" for event in load_moves if event.get('type') == 'DELIVERLOAD' and event.get('state') and event.get('country')]
                    
                    # If DELIVERLOAD not present, fallback to PICKUP or RETURN events
                    if not over_weight_state:
                        over_weight_state = [f"{event.get('state').upper()}, {event.get('country').upper()}" for event in load_moves if event.get('type') in ['PULLCONTAINER', 'RETURNCONTAINER'] and event.get('state') and event.get('country')]
                    
                    move_data['over_weight_state'] = over_weight_state[0] if over_weight_state else ''
                    move_data['customer_ids'] = customer_ids
                    move_data['event_types'] = event_types
                    move_data['preferred_states'] = [f"{event.get('state')}, {event.get('country')}" for event in load_moves if event.get('state') and event.get('country')]
                    move_data['max_distance'] = int(max([event.get('distance', 0) for event in load_moves]))

                if move_data.get('expected_from_minute') > move_data.get('expected_to_minute'):
                    copied_move = move.copy()
                    copied_move['reason'] = 'APT_FROM_TIME_GREATER_THAN_TO_TIME'
                    skipped_moves.append(copied_move)
                    continue

                moves.append(move_data)
            except Exception as e:
                copied_move = move.copy()
                copied_move['reason'] = 'UNEXPECTED_ERROR' 
                skipped_moves.append(copied_move)
                continue

        drayage_configuration = await get_drayage_intelligence(carrier, ['drop_hook_locations'])
        drop_hook_locations = drayage_configuration.get('drop_hook_locations', [])

        # force moves together if 1-1 drlivery - empty returns
        if drayage_configuration.get('drop_hook_locations'):
            deliveries_with_no_empties = []

            hook_empties = [
                m for m in moves
                if m['start_event'].get('customerId') and m['start_event'].get('customerId') in drop_hook_locations
                and m['start_event'].get('type') == 'HOOKCONTAINER'
            ]

            for move in moves:
                try:
                    # is deliverying to a drop hook location
                    is_drop_hook_location = move['end_event'].get('customerId') and move['end_event'].get('customerId') in drop_hook_locations
                    if is_drop_hook_location:
                        # find the hook empty
                        hook_empty = next((h for h in hook_empties if h['start_event'].get('customerId') and move['end_event'].get('customerId') and h['start_event'].get('customerId') == move['end_event'].get('customerId')), None)
                        
                        if hook_empty:
                            # force the moves together
                            move['strictly_coupled_move'] = hook_empty['_id']
                            # pull the hook empty from the moves list
                            hook_empties.remove(hook_empty)
                        else:
                            move['reason'] = 'SKIP_ONE_TO_ONE_MOVE'
                            skipped_moves.append(move)
                            deliveries_with_no_empties.append(move['_id'])
                except Exception as e:
                    move['reason'] = 'UNEXPECTED_ERROR'
                    skipped_moves.append(move)
                    deliveries_with_no_empties.append(move['_id'])
      
            # remove the deliveries with no empties from the moves list
            moves = [m for m in moves if m['_id'] not in deliveries_with_no_empties]
                
        # free flow and empty return same location assigned to as couple moves
        moves = await free_flow_empty_return_assigned_couple_moves(carrier, moves, timeZone, plan_date)

        return moves, skipped_moves
    except Exception as e:
        logger.error(f"Failed to map move data for plan v2 for carrier: {carrier}: {str(e)}")
        raise Exception(f"Failed to map move data for plan v2: {str(e)}")
    
    
async def free_flow_empty_return_assigned_couple_moves(carrier: str, moves: List[Dict[str, Any]], timeZone: str, plan_date: datetime):
    
    """
    Couple free flow moves with empty return moves for specific locations.
    
    This function identifies free flow moves that start from locations configured
    for coupling and finds corresponding empty return moves to the same location.
    When found, it couples these moves together and synchronizes their appointment
    windows to ensure they are scheduled within the same time frame.
    
    Args:
        carrier (str): The carrier identifier
        moves (List[Dict[str, Any]]): List of move objects to process
        timeZone (str): Timezone string for date calculations
        plan_date (datetime): The planning date for the moves
        
    Returns:
        List[Dict[str, Any]]: Updated list of moves with coupling relationships
    """
    free_flow_empty_return_couple_moves_locations = CARRIER_CONFIGS.get(carrier, {}).get('free_flow_empty_return_couple_moves_locations', [])
    if len(free_flow_empty_return_couple_moves_locations) > 0:
        
        for move in moves:
            if move.get('is_free_flow_move', False) and not move.get('is_assigned_move', False):
                pickup_customer_id = move.get('start_event').get('customerId')
                if pickup_customer_id in free_flow_empty_return_couple_moves_locations:
                    empty_return_move = next(
                        (
                            m for m in moves
                            if m.get('end_event').get('customerId') == pickup_customer_id
                            and m.get('end_event').get('type') == 'RETURNCONTAINER'
                            and not m.get('strictly_coupled_move')
                        ),
                        None
                    )
                    
                    if empty_return_move:
                        empty_return_move['strictly_coupled_move'] = move['_id']
                        
                        # Set the appointment window for the free flow move to match the empty return move's appointment window
                        # This ensures the free flow move is scheduled within the same time window as the empty return
                        appointment_from = datetime.fromisoformat(empty_return_move.get('end_event').get('appointment_from')).astimezone(pytz.timezone(timeZone))
                        appointment_to = datetime.fromisoformat(empty_return_move.get('end_event').get('appointment_to')).astimezone(pytz.timezone(timeZone))
                        
                        if appointment_to <= appointment_from:
                            appointment_to = appointment_to + timedelta(days=1)
                        
                        from_date_day_diff = get_days_difference(plan_date.isoformat(), appointment_from.isoformat(), timeZone)
                        to_date_day_diff = get_days_difference(plan_date.isoformat(), appointment_to.isoformat(), timeZone)
                        
                        event_appt_from = max(
                            get_minute(appointment_from.isoformat(), timeZone) +
                                (from_date_day_diff * ONE_DAY_IN_MINUTES),
                            0
                        )
                        event_appt_to = max(
                            get_minute(appointment_to.isoformat(), timeZone) +
                                (to_date_day_diff * ONE_DAY_IN_MINUTES),
                            0
                        )
                        
                        # free flow move have pull event only, modified appointment window to return appointment window
                        event = move.get('move', [])[0]
                        
                        waiting_time = event.get('waiting_time', 0)
                        event_appt_from += waiting_time
                        event_appt_to += waiting_time

                        event['appointment_from'] = appointment_from.isoformat()
                        event['appointment_to'] = appointment_to.isoformat()
                        event['is_scheduled'] = False
                        event['is_recommended'] = True
                        move['expected_from_minute'] = event_appt_from
                        move['expected_to_minute'] = event_appt_to
    return moves

def map_route_summary_to_driver_plan(user_payload: Dict[str, Any], loads: List[Dict[str, Any]], optimal_plan: List[Dict[str, Any]], drivers: List[Dict[str, Any]], timeZone: str, converted_plan_date: datetime):
    try:
        recommended_moves = []
        distance_unit = user_payload.get('distanceUnit', 'mi')
        for i, dData in enumerate(optimal_plan):
            for mData in dData.get('node'):
                m_reference_number = mData.get('reference_number')
                m_driver = dData.get('Driver Name')
                driver = next((d for d in drivers if d.get('_id') == m_driver), None)
                load = next((move for move in loads if move.get('reference_number') == m_reference_number and move.get('move_index') == mData.get('move_index')), None)
                driver_name = f"{driver['name']} {driver['last_name']}" if pd.notna(driver['name']) else ""

                pickup_appt_time = get_formatted_time(load.get('pickupFromTime'), timeZone) if load.get('pickupFromTime') else None
                delivery_appt_time = get_formatted_time(load.get('deliveryFromTime'), timeZone) if load.get('deliveryFromTime') else None 
                return_appt_time = get_formatted_time(load.get('returnFromTime'), timeZone) if load.get('returnFromTime') else None

                move_copy = load['move'].copy()

                move_copy = [
                    {k: v for k, v in event.items() if k not in ['waiting_time_distribution']}
                    for event in move_copy
                ]

                for e, event in enumerate(move_copy):
                    # get event data from mData based on index
                    event_data = mData['event_times'][e]
                    
                    event['enroute'] = (converted_plan_date + timedelta(minutes=event_data['minutes']['recommended_enroute'])).isoformat()
                    event['arrived'] = (converted_plan_date + timedelta(minutes=event_data['minutes']['recommended_arrived'])).isoformat()
                    event['departed'] = (converted_plan_date + timedelta(minutes=event_data['minutes']['recommended_departed'])).isoformat()
                    event['distance'] = event_data['distance']
                    event['late_arrived_minutes'] = 0
                    # Calculate late_arrived_minutes based on appointment_to vs recommended_arrived
                    if event_data.get('appointment_to') and event_data['minutes'].get('recommended_arrived'):
                        appointment_to_minutes = event_data['appointment_to']
                        recommended_arrived_minutes = event_data['minutes']['recommended_arrived']
                        
                        if recommended_arrived_minutes <= appointment_to_minutes:
                            event['late_arrived_minutes'] = 0
                        else:
                            event['late_arrived_minutes'] = recommended_arrived_minutes - appointment_to_minutes
                
                is_late_arrival_move = any(ev.get('late_arrived_minutes', 0) > 0 for ev in move_copy)
                plan_assumptions = PLANNING_ASSUMPTIONS.get('LATE_ARRIVAL_MOVE') if is_late_arrival_move else None
                is_new_move = load.get('is_new_move', False)
                is_move_affected = False

                if not is_new_move:
                    old_load_assigned_minute = get_minute(load.get('load_assigned_date'), timeZone) if load.get('load_assigned_date') else 0
                    new_load_assigned_minute = mData['start_move_minute']
                    is_move_affected = old_load_assigned_minute != new_load_assigned_minute
                
                chassis_pick_event = mData.get('chassis_pick_event', None)
                chassis_termination_event = mData.get('chassis_termination_event', None)

                del_chassis_event_keys = ['start_loc', 'end_loc', 'recommended_enroute', 'recommended_arrived', 'recommended_departed']
                if chassis_pick_event:
                    chassis_pick_event['enroute'] = (converted_plan_date + timedelta(minutes=chassis_pick_event['recommended_enroute'])).isoformat()
                    chassis_pick_event['arrived'] = (converted_plan_date + timedelta(minutes=chassis_pick_event['recommended_arrived'])).isoformat()
                    chassis_pick_event['departed'] = (converted_plan_date + timedelta(minutes=chassis_pick_event['recommended_departed'])).isoformat()
                    for key in del_chassis_event_keys:
                        del chassis_pick_event[key]

                if chassis_termination_event:
                    chassis_termination_event['enroute'] = (converted_plan_date + timedelta(minutes=chassis_termination_event['recommended_enroute'])).isoformat()
                    chassis_termination_event['arrived'] = (converted_plan_date + timedelta(minutes=chassis_termination_event['recommended_arrived'])).isoformat()
                    chassis_termination_event['departed'] = (converted_plan_date + timedelta(minutes=chassis_termination_event['recommended_departed'])).isoformat()
                    for key in del_chassis_event_keys:
                        del chassis_termination_event[key]

                if len(load.get('actual_driver_order', [])) > 0:
                    load['revenue'] = calculate_per_mile_revenue(load, move_copy, distance_unit)

                mapped_assignment = {
                    'load_id': load['_id'],
                    'reference_number': load['reference_number'],
                    'customer': load['callerName'],
                    'assigned_driver': driver['_id'],
                    'assigned_driver_name': driver_name,
                    'move': move_copy,
                    'arrival_time': (converted_plan_date + timedelta(minutes=mData['start_move_minute'])).isoformat(),
                    'enroute_time': (converted_plan_date + timedelta(minutes=mData['start_move_minute'])).isoformat(),
                    'completion_time': (converted_plan_date + timedelta(minutes=mData['move_completed_minute'])).isoformat(),
                    'pickup_time': pickup_appt_time,
                    'delivery_time': delivery_appt_time,
                    'return_time': return_appt_time,
                    'distance': load['distance'],
                    'revenue': load.get('revenue', 0),
                    'driver_pay': 0,
                    'is_modified_move': False if pd.isna(load.get('is_modified_move')) else bool(load.get('is_modified_move')),
                    'modification_reason': load.get('modification_reason', ''),
                    'is_move_affected': is_move_affected,
                    'is_assigned_move': not is_new_move,
                    'is_new_move': is_new_move,
                    'driver_index': i,
                    'chassis_pick_event': chassis_pick_event,
                    'chassis_termination_event': chassis_termination_event,
                    'is_free_flow_move': load.get('is_free_flow_move', False),
                    'is_combined_trip': load.get('is_combined_trip', False),
                    'plan_assumptions': plan_assumptions,
                    'terminal': load.get('terminal', '')
                }
                recommended_moves.append(mapped_assignment)

        return recommended_moves
    except Exception as e:
        carrier = user_payload.get('carrier')
        logger.error(f"Failed to get optimal plan v2 data mapper for carrier: {carrier}: {str(e)}")
        raise Exception(f"Failed to get optimal plan v2 data mapper: {str(e)}")

def map_fixed_plan_to_driver_plan(
    user_payload: Dict[str, Any],
    fixed_plan: List[Dict[str, Any]],
    drivers: List[Dict[str, Any]],
    timeZone: str,
    converted_plan_date: datetime,
    distance_unit: str
):
    try:
        formatted_fixed_plan = []
        for driver_id, driver_plan in fixed_plan.items():
            driver = next((d for d in drivers if d.get('_id') == driver_id), None)
            driver_name = f"{driver['name']} {driver['last_name']}" if pd.notna(driver['name']) else ""


            for load in driver_plan:
                pickup_appt_time = get_formatted_time(load.get('pickupFromTime'), timeZone) if load.get('pickupFromTime') else None
                delivery_appt_time = get_formatted_time(load.get('deliveryFromTime'), timeZone) if load.get('deliveryFromTime') else None 
                return_appt_time = get_formatted_time(load.get('returnFromTime'), timeZone) if load.get('returnFromTime') else None

                completion_minute = get_completion_time(load, timeZone, distance_unit, plan_date=converted_plan_date)
                completion_time = (converted_plan_date + timedelta(minutes=completion_minute))
                arrival_time = datetime.fromisoformat(load.get('load_assigned_date'))

                first_enroute_time = load.get('move', [])[0].get('arrived') or arrival_time.isoformat()
                first_arrival_time = load.get('move', [])[0].get('departed') or first_enroute_time

                move_copy = load['move'].copy()
                move_copy = [
                    {k: v for k, v in event.items() if k not in ['waiting_time_distribution']}
                    for event in move_copy
                ]

                
                mapped_move = []
                for e_index, event in enumerate(move_copy):
                    db_arrived = event.get('arrived')
                    db_departed = event.get('departed')

                    next_db_event = move_copy[e_index + 1] if e_index + 1 < len(move_copy) else None

                    previous_event = mapped_move[-1] if mapped_move else None

                    if db_arrived:
                        event['enroute'] = db_arrived
                        event['is_enrouted'] = True
                    else:
                        event['enroute'] = previous_event.get('departed') if previous_event else load.get('load_assigned_date')

                    if db_departed:
                        event['arrived'] = db_departed
                        event['is_arrived'] = True
                    else:
                        event['arrived'] = (datetime.fromisoformat(event['enroute']) + timedelta(minutes=minute_from_distance(event.get('distance', 0), distance_unit))).isoformat()


                    if next_db_event:
                        next_db_arrived = next_db_event.get('arrived')
                        if next_db_arrived:
                            event['departed'] = next_db_arrived
                            event['is_departed'] = True
                        else:
                            event['departed'] = (datetime.fromisoformat(event['arrived']) + timedelta(minutes=event.get('waiting_time', 0))).isoformat()


                    mapped_move.append(event)
                
                if len(load.get('actual_driver_order', [])) > 0:
                    load['revenue'] = calculate_per_mile_revenue(load, move_copy, distance_unit)

                mapped_assignment = {
                            'load_id': load['_id'],
                            'reference_number': load['reference_number'],
                            'customer': load['callerName'],
                            'assigned_driver': driver['_id'],
                            'assigned_driver_name': driver_name,
                            'move': mapped_move,
                            'arrival_time': first_arrival_time,
                            'enroute_time': first_enroute_time,
                            'completion_time': completion_time.isoformat(),
                            'pickup_time': pickup_appt_time,
                            'delivery_time': delivery_appt_time,
                            'return_time': return_appt_time,
                            'distance': load['distance'],
                            'revenue': load['revenue'],
                            'driver_pay': 0,
                            'is_modified_move': False,
                            'is_assigned_move': True,
                            'is_new_move': False,
                            'driver_index': -1,
                            'terminal': load.get('terminal', '')
                        }
                
                formatted_fixed_plan.append(mapped_assignment)

        return formatted_fixed_plan
    except Exception as e:
        print(f"Error in map_fixed_plan_to_driver_plan: {str(e)}")
        raise Exception(f"Failed to map fixed plan to driver plan: {str(e)}")

def rejected_driver_moves(restricted_moves: List[Dict[str, Any]]):
    try:
        restricted_driver_for_moves = [dict(move) for move in restricted_moves]
        result = []
        driver_moves = {}
        
        for r_move in restricted_driver_for_moves:
            driver_id = r_move.get('suggested_driver')
            
            # check driver id is already in the driver_moves
            if not driver_id in driver_moves:
                driver_moves[driver_id] = []

            move_data = {
                "load_id": r_move.get('load_id')
            }
            if not r_move.get('is_trip', False):
                move_detail = json.loads(r_move.get('move', []))
                move_id = move_detail[-1].get('moveId') if len(move_detail) > 0 else None
                if move_id:
                    move_data['move_id'] = move_id

            driver_moves[driver_id].append(move_data)

        # Convert dict to array format
        for driver_id, moves in driver_moves.items():
            result.append({
                "driver_id": driver_id,
                "moves": moves
            })

        return result
    except Exception as e:
        print(f"Error in restricted_driver_for_moves: {str(e)}")
        return []
def calculate_per_mile_revenue(load, suggested_driver_order, distance_unit: str):
    try:
        # Create lookup dict for suggested events to avoid repeated loops
        suggested_events_map = {e.get('_id'): (i, e) for i, e in enumerate(suggested_driver_order)}
        
        original_driver_order = load.get('actual_driver_order', [])
        new_driver_order = []
        is_new_event_added = False

        for event in original_driver_order:
            # O(1) lookup instead of O(n) search
            match_event_data = suggested_events_map.get(event.get('_id'))
            
            if match_event_data:
                match_event_index, match_event = match_event_data
                new_driver_order.append(match_event)

                # Check next event
                next_index = match_event_index + 1
                if next_index < len(suggested_driver_order):
                    next_event = suggested_driver_order[next_index]
                    # O(1) lookup instead of O(n) search
                    if next_event and not any(e.get('_id') == next_event.get('_id') for e in original_driver_order):
                        new_driver_order.append(next_event)
                        is_new_event_added = True
            else:
                if is_new_event_added:
                    last_event = new_driver_order[-1]
                    last_addr = last_event.get('address', {})
                    curr_addr = event.get('address', {})
                    
                    # Check coordinates exist before calculation
                    if all((last_addr.get('lat'), last_addr.get('lng'), 
                           curr_addr.get('lat'), curr_addr.get('lng'))):
                        event['distance'] = calculate_distance(
                            last_addr['lat'], last_addr['lng'],
                            curr_addr['lat'], curr_addr['lng'],
                            distance_unit
                        )
                        is_new_event_added = False
                new_driver_order.append(event)

        # Calculate total distance excluding first event distance
        total_distance = sum(event.get('distance', 0) for event in new_driver_order)
        total_distance -= suggested_driver_order[0].get('distance', 0) if suggested_driver_order else 0

        # Calculate revenue only if both distance and revenue exist
        revenue = load.get('revenue', 0)
        if total_distance > 0 and revenue > 0:
            per_mile_revenue = revenue / total_distance
            return per_mile_revenue * load['distance']

        return revenue

    except Exception as e:
        logger.error(f"Error in calculate_per_mile_revenue: {str(e)}")
        return load.get('revenue', 0)
