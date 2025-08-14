import pandas as pd
import logging
import pytz
from copy import deepcopy
import time
import asyncio
import multiprocessing
from typing import Dict, Any, List
from datetime import datetime, timedelta
from vrp_optimizer.optimizer import Optimizer
from load_optimizer.get_optimal_plan_v2 import get_formatted_time
from vrp_optimizer.helpers import minute_from_distance, get_all_driver_default_locations, get_yard_data, get_warehouse_visits
from vrp_optimizer.services import handle_assigned_moves, get_completion_time, get_current_plan
from app.postgres_services.configurations_service import get_equipment_validations, get_container_sizes, get_container_types
from vrp_optimizer.routing_distance import get_location_distance_matrix_bulk, get_unique_coordinates
from app.postgres_services.drayage_intelligence_service import get_drayage_intelligence
from app.services.redis_service import get_default_yard_location
from app.modules.optimizer.utility import get_planning_time_windows
from app.postgres_services.driver_rotation_service import apply_driver_rotation_sorting
from app.modules.optimizer.constants import (
    ONE_DAY_IN_MINUTES,
    CARRIER_CONFIGS
)

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

async def get_optimal_plan_v3(
    user_payload: Dict[str, Any],
    actionable_moves: List[Dict[str, Any]],
    drivers: List[Dict[str, Any]],
    converted_plan_date: datetime,
    additional_depot_locations: List[Dict[str, Any]] = [],
    return_schedule: bool = False,
    time_limit: int = 3 * 60,
    branch: Any = None,
    shift: Any = None
):
    try:
        if len(drivers) == 0:
            return [], [], {}

        carrier = user_payload.get('carrier')
        timeZone = user_payload.get('timeZone')
        distance_unit = user_payload.get('distanceUnit', 'mi')

        user_settings = await get_equipment_validations(carrier)
        equipment_validations = user_settings.get('equipment_validations', [])

        planning_windows = await get_planning_time_windows(carrier, converted_plan_date, shift, branch, timeZone)
        planning_minutes = planning_windows.get('plan_window') or [0, 1440]
        plan_start_minute = planning_minutes[0]
        plan_end_minute = planning_minutes[1]

        # map driver data to vehicle data
        vehicle_data = []
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

            end_minute = min(end_minute, plan_end_minute)
            start_minute = max(start_minute, plan_start_minute)

            skip_driver_for_optimizer = False

            if start_minute >= end_minute:
                # This scenario will happen when driver's working hours are passed but the shift is still active.
                # Example: Driver can work max upto 10 PM, but the shift is ending on 2 AM next day, and we are replanning on the same day at 11 PM. 
                print("Incorrect shift time for driver: ", v.get('_id', ''))
                start_minute = plan_start_minute
                end_minute = plan_end_minute

                skip_driver_for_optimizer = True

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
                'owner_score': int(v.get('owner_score', 1) or 1),
            }

            if skip_driver_for_optimizer:
                data['skip_driver_for_optimizer'] = True

            vehicle_data.append(data)

        assigned_moves = [m for m in actionable_moves if m.get('is_manually_planned', False)]
        actionable_moves = [m for m in actionable_moves if not m.get('is_manually_planned', False)]

        all_assigned_moves = deepcopy(assigned_moves)

        driver_default_locations = get_all_driver_default_locations(drivers)

        vehicle_data, assigned_moves, additional_depot_locations, fixed_plan = handle_assigned_moves(
            user_payload,
            vehicle_data,
            driver_default_locations,
            additional_depot_locations,
            all_assigned_moves,
            plan_date=converted_plan_date
        )

        
        ALL_DEPOT_LOCATIONS = driver_default_locations + additional_depot_locations
        default_yard_locations = await get_default_yard_location(carrier)
        ALL_YARD_LOCATIONS = [get_yard_data(yard) for yard in default_yard_locations]

        for m in assigned_moves:
            m['is_assigned_move'] = True

        moves, skipped_moves = await map_actionable_moves_for_optimizer(
            user_payload,
            assigned_moves + actionable_moves,
            timeZone,
            distance_unit,
            converted_plan_date,
            plan_end_minute,
            plan_start_minute,
            ALL_YARD_LOCATIONS
        )
        print(f"Skipped moves: {len(skipped_moves)}")

        optimal_plan = []

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
            
            # Create a process pool for running the optimization
            with multiprocessing.Pool(1) as pool:
                # Run the optimization in a separate process with timeout
                try:
                    VEHICLE_CHUNK_SIZE = 500
                    
                    vehicle_data = await apply_driver_rotation_sorting(
                        vehicle_data, 
                        carrier, 
                        converted_plan_date,
                        rotation_order=CARRIER_CONFIGS.get(carrier, {}).get('rotation_order', ['owner_score'])
                    )

                    # We'll not pass the drivers who can't work in the planning time
                    filtered_vehicle_data = [v for v in vehicle_data if not v.get('skip_driver_for_optimizer', False)]
                    vehicle_data_chunks = [
                        vehicle_data[i:i+VEHICLE_CHUNK_SIZE]
                        for i in range(0, len(filtered_vehicle_data), VEHICLE_CHUNK_SIZE)
                    ]

                    moves_copy = deepcopy(moves)
                    d_schedule = {}
                    
                    for vehicle_chunk in vehicle_data_chunks:
                        optimizer = Optimizer(
                            moves = moves_copy,
                            drivers = vehicle_chunk,
                            depot_locations = ALL_DEPOT_LOCATIONS,
                            yard_locations = ALL_YARD_LOCATIONS,
                            timezone = timeZone,
                            distance_unit = distance_unit,
                            time_limit = time_limit,
                            equipment_validations = equipment_validations,
                            location_distance_matrix = location_distance_matrix,
                            plan_start_minute = plan_start_minute,
                            plan_end_minute = plan_end_minute,
                            carrier_id = user_payload.get('carrier')
                        )
                        
                        _optimal_plan, _d_schedule = pool.apply_async(optimizer.optimize).get(timeout=time_limit + 30)

                        optimal_plan.extend(_optimal_plan)
                        d_schedule.update(_d_schedule)

                        planned_moves = [
                            node['reference_number']
                            for plan in _optimal_plan
                            for node in plan['node']
                            if 'reference_number' in node
                        ]
                        moves_copy = [m for m in moves_copy if m.get('_id') not in planned_moves]

                    if return_schedule and optimal_plan and d_schedule:
                        driver_schedule = d_schedule

                except multiprocessing.TimeoutError:
                    logger.warning(f"Optimization timed out after {time_limit} seconds")
                
                except Exception as e:
                    logger.error(f"Error during optimization: {str(e)}")
            
            end_time = time.time()
            print(f"Time taken by vrp optimizer: {end_time - start_time} seconds for {len(moves)} moves")

        # Return the current assignements as new plan if no new optimal plan is found
        if not optimal_plan:
            if not all_assigned_moves:
                return [], skipped_moves, {}

            current_plan = get_current_plan(all_assigned_moves)
            formatted_current_plan = map_fixed_plan_to_driver_plan(current_plan, drivers, timeZone, converted_plan_date, distance_unit)
            return formatted_current_plan, skipped_moves, {}

        for m in actionable_moves:
            m['is_new_move'] = True

        optimal_plan = map_route_summary_to_driver_plan(assigned_moves + actionable_moves, optimal_plan, drivers, timeZone, converted_plan_date)

        formatted_fixed_plan = map_fixed_plan_to_driver_plan(fixed_plan, drivers, timeZone, converted_plan_date, distance_unit)

        return formatted_fixed_plan + optimal_plan, skipped_moves, driver_schedule
    except Exception as e:
        logger.error(e)

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

        allow_mismatched_appointment_times = CARRIER_CONFIGS.get(carrier, {}).get('allow_mismatched_appointment_times', False)
        last_visit_locations = CARRIER_CONFIGS.get(carrier, {}).get('last_visit_locations', [])
        
        for move in actionable_moves:
            appointment = None
            has_invalid_appts = False
            appointment_diff = float('inf')
            
            travel_time = sum(
                minute_from_distance(ev.get('distance', 0), distance_unit)
                for ev in move.get('move')
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
                    minute_to_reach = minute_from_distance(event.get('distance', 0), distance_unit)
                    appointment[0] += minute_to_reach
                    appointment[1] += minute_to_reach
                
                # Get appointment times if they exist
                if event.get('appointment_from'):
                    # Calculate appointment window accounting for queue waiting time
                    from_date_day_diff = get_days_difference(plan_date.isoformat(), event.get('appointment_from'), timeZone)
                    to_date_day_diff = get_days_difference(plan_date.isoformat(), event.get('appointment_to'), timeZone)

                    if from_date_day_diff < 0 or to_date_day_diff < 0:
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
                        will_driver_reach_after_appointment = appointment[0] >= (event_appt_to + 15)
                        
                        # Allow Driver to reach 6 hours before appointment
                        will_driver_reach_before_appointment = event_appt_from >= (appointment[1] + 360)
                        if will_driver_reach_before_appointment or will_driver_reach_after_appointment:
                            if allow_mismatched_appointment_times:
                                if appointment:
                                    wait_time = event.get('waiting_time', 0)
                                    appointment[0] += wait_time
                                    appointment[1] += wait_time
                                    continue
                            has_invalid_appts = True
                            break

                        # Calculate waiting time on road
                        if event_appt_from - appointment[1] > 0:
                            additional_waiting_time = event_appt_from - appointment[1] + 30 # 30 minutes buffer
                            event['early_arrival_waiting'] = additional_waiting_time
                            early_arrival_waiting += additional_waiting_time

                        appointment[0] = max(appointment[0], event_appt_from)
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

            if expected_from_minute >= plan_end_minute:
                copied_move = move.copy()
                copied_move['reason'] = 'NEXT_DAY_TRIP'
                skipped_moves.append(copied_move)
                continue
            
            move_data = {
                **move,
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
                "route_distance": sum([ e.get('distance', 0) for e in move.get('move')]),
                "minutes_on_road": travel_time,
                "early_arrival_waiting": early_arrival_waiting,
                "total_waiting_time": move.get('waiting_time'),
                "expected_from_minute": int(expected_from_minute),
                "expected_to_minute": int(expected_to_minute),
            }

            if move.get('is_assigned_move', False):
                move_data['assigned_driver'] = move.get('assigned_driver', '')

            warehouse_visits = get_warehouse_visits(move.get('move', []), [yard.get('depot_customer_id') for yard in yards if yard.get('depot_customer_id')]) 

            if warehouse_visits:
                move_data['warehouse_ids'] = list(filter(None, [e.get('customerId') for e in warehouse_visits]))
            
            if move.get('is_free_flow_move', False):
                move_data['last_move_by_driver'] = True

            if len(last_visit_locations) > 0:
                is_last_visit_location_present = any(m for m in move.get('move', []) if m.get('customerId') and m.get('customerId') in last_visit_locations)
                if is_last_visit_location_present:
                    move_data['last_move_by_driver'] = True

            if move_data.get('expected_from_minute') > move_data.get('expected_to_minute'):
                copied_move = move.copy()
                copied_move['reason'] = 'APT_FROM_TIME_GREATER_THAN_TO_TIME'
                skipped_moves.append(copied_move)
                continue

            moves.append(move_data)

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
                        deliveries_with_no_empties.append(move['_id'])

            # remove the deliveries with no empties from the moves list
            moves = [m for m in moves if m['_id'] not in deliveries_with_no_empties]

        return moves, skipped_moves
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to map move data for plan v2: {str(e)}")

def map_route_summary_to_driver_plan(loads: List[Dict[str, Any]], optimal_plan: List[Dict[str, Any]], drivers: List[Dict[str, Any]], timeZone: str, converted_plan_date: datetime):
    try:
        recommended_moves = []
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
                    'is_move_affected': is_move_affected,
                    'is_assigned_move': not is_new_move,
                    'is_new_move': is_new_move,
                    'driver_index': i,
                    'chassis_pick_event': chassis_pick_event,
                    'chassis_termination_event': chassis_termination_event,
                    'is_free_flow_move': load.get('is_free_flow_move', False)
                }
                recommended_moves.append(mapped_assignment)

        return recommended_moves
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to get optimal plan v2 data mapper: {str(e)}")


def map_fixed_plan_to_driver_plan(
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
                            'driver_index': -1
                        }
                
                formatted_fixed_plan.append(mapped_assignment)

        return formatted_fixed_plan
    except Exception as e:
        print(f"Error in map_fixed_plan_to_driver_plan: {str(e)}")
        raise Exception(f"Failed to map fixed plan to driver plan: {str(e)}")
