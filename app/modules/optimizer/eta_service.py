import logging
import pytz
from datetime import datetime, timedelta
from typing import Any, Dict, List

from app.services.common_service import get_time_zone, get_carrier_preferences
from app.mongo_services.load_service import get_loads_with_reference_numbers, get_moves_from_driver_order
from app.postgres_services.turn_around_time import get_waiting_time, add_waiting_time_to_move
from app.postgres_services.terminal_warehouse_service import get_office_hours
from app.postgres_services.driver_eta_service import save_driver_eta_details
from app.mongo_services.trip_service import get_trips_by_trip_ids

from app.utils.distance_calc import calculate_duration_from_distance, calculate_distance
from app.modules.scheduler.utility import map_loads_for_scheduler
from app.modules.optimizer.utility import populate_appointment_times_to_events

logger = logging.getLogger(__name__)

def estimate_move_start_time(move: Dict[str, Any], timeZone: str, distance_unit: str) -> datetime:
    try:
        appointment = None
        appointment_diff = float('inf')
        total_duration = timedelta(hours=0)

        for event in move:
            # Add travel time
            minute_to_reach = calculate_duration_from_distance(event.get('distance', 0), distance_unit)
            total_duration += minute_to_reach

            if appointment:
                appointment[0] += minute_to_reach
                appointment[1] += minute_to_reach
                    
            # Get appointment times if they exist
            if event.get('appointment_from'):
                event_appt_from = event.get('appointment_from')
                event_appt_to = event.get('appointment_to')
                    
                event_appt_from = datetime.fromisoformat(event_appt_from)
                event_appt_to = datetime.fromisoformat(event_appt_to)
                appointment_diff = min(appointment_diff, (event_appt_to - event_appt_from).total_seconds() / 60)

                if appointment:
                    # Calculate waiting time on road
                    if (event_appt_from - appointment[1]).total_seconds() > 0:
                        additional_waiting_time = (event_appt_from - appointment[1]) + timedelta(minutes=30) # 30 minutes buffer
                        event['early_arrival_waiting'] = additional_waiting_time
                        total_duration += additional_waiting_time

                    appointment[0] = max(appointment[0], event_appt_from)
                    appointment[1] = min(appointment[0] + timedelta(minutes=appointment_diff), event_appt_to)
                else:
                    # Initialize first appointment window
                    appointment = [event_appt_from, event_appt_to]

            # Add waiting time if there's an appointment window
            wait_time = event.get('waiting_time', 0)
            total_duration += timedelta(minutes=wait_time)

            if appointment:
                appointment[0] += timedelta(minutes=wait_time)
                appointment[1] += timedelta(minutes=wait_time)

        tz = pytz.timezone(timeZone)

        if not appointment:
            day_start_time = tz.localize(datetime.now()).replace(hour=0, minute=0, second=0)
            day_end_time = tz.localize(datetime.now()).replace(hour=0, minute=0, second=0) + timedelta(days=1) - timedelta(seconds=1)
            appointment = [day_start_time, day_end_time]

        expected_start_time_range = [appointment[0] - total_duration, appointment[1] - total_duration]

        return expected_start_time_range
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to estimate completion time of move: {str(e)}")


def fill_estimated_route_times(
    move: Dict[str, Any],
    start_time: datetime,
    distance_unit: str
) -> Dict[str, Any]:
    try:
        for index, event in enumerate(move):
            if not event.get('arrived'):
                if index == 0:
                    event['arrived'] = start_time
                else:
                    departed_time = move[index - 1].get('departed')
                    if isinstance(departed_time, str):
                        departed_time = datetime.fromisoformat(departed_time)
                    event['arrived'] = departed_time + timedelta(minutes=move[index - 1].get('waiting_time', 0))
            
            if not event.get('departed'):
                travel_time = calculate_duration_from_distance(event.get('distance', 0), distance_unit)
                arrived_time = event.get('arrived')
                if isinstance(arrived_time, str):
                    arrived_time = datetime.fromisoformat(arrived_time)
                event['departed'] = arrived_time + travel_time

        return move

    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to fill estimated route times: {str(e)}")
    
def calculate_move_eta(
    load: Dict[str, Any],
    timeZone: str,
    distance_unit: str
) -> Dict[str, Any]:
    try:
        move_eta = {
            "loadId": load.get('_id'),
            "reference_number": load.get('reference_number'),
            "start_time_window": [],
            "total_duration": 0,
            "move": []
        }

        if not load.get('isTrip'):
            move_eta['moveId'] = load.get('moveId')
        else:
            move_eta['isTrip'] = True

        first_event = load.get('move')[0]
        if not first_event.get('arrived'):
            start_times = estimate_move_start_time(load.get('move'), timeZone, distance_unit)
            move_eta['start_time_window'] = [t.isoformat() for t in start_times]
        
        move = load.get('move')
        for index, event in enumerate(move):
            # Create base event dict to avoid repetition
            base_event = {
                "_id": event.get('_id'),
                "type": event.get('type'),
                "waiting_time": event.get('waiting_time', 0),
                "appointment_from": event.get('appointment_from'),
                "appointment_to": event.get('appointment_to'),
                "travel_time": 0
            }

            # Handle subsequent events
            if index > 0 and event.get('type') != 'CHASSISTRANSFER':
                prev_event = move[index - 1]
                move_eta['total_duration'] += prev_event.get('waiting_time', 30)

                # Calculate travel time based on whether previous event had customer
                travel_time = 30
                if prev_event.get('customerId'):
                    travel_time = calculate_duration_from_distance(
                        event.get('distance', 0), 
                        distance_unit
                    ).total_seconds() / 60

                move_eta['total_duration'] += travel_time
                base_event["travel_time"] = travel_time
                
            move_eta['move'].append(base_event)
        
        return move_eta

    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to fill estimated route times: {str(e)}")

def get_eta_for_next_move(
    active_load: Dict[str, Any],
    next_load: Dict[str, Any],
    timeZone: str,
    distance_unit: str
) -> Dict[str, Any]:
    try:
        # get next move start time
        next_move_start_time_range = estimate_move_start_time(next_load['move'], timeZone, distance_unit)

        # get current move completion time
        current_move_last_event = active_load['move'][-1]
        next_move_first_event = next_load['move'][0]

        last_event = active_load['move'][-1]
        move_completion_time = last_event.get('departed')
        if isinstance(move_completion_time, str):
            move_completion_time = datetime.fromisoformat(move_completion_time)

        if current_move_last_event.get('customerId') and next_move_first_event.get('customerId'):
            if last_event.get('type') != 'CHASSISTRANSFER':
                move_completion_time = move_completion_time + timedelta(minutes=last_event.get('waiting_time', 0))

            current_event_location = current_move_last_event.get('address')
            next_event_location = next_move_first_event.get('address')
            distance = calculate_distance(
                current_event_location.get('lat'),
                current_event_location.get('lng'),
                next_event_location.get('lat'),
                next_event_location.get('lng'),
                distance_unit
            )
            travel_time = calculate_duration_from_distance(distance, distance_unit)
            move_completion_time = move_completion_time + travel_time

        return move_completion_time, next_move_start_time_range
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to get eta for next move: {str(e)}")

def process_event(event: Dict[str, Any], timeZone: str):
    tz = pytz.timezone('UTC')
    for field in ['waiting_time_distribution', 'early_arrival_waiting']:
        event.pop(field, None)
            
    for field in ['arrived', 'departed']:
        if value := event.get(field):
            if isinstance(value, str):
                value = datetime.fromisoformat(value)
            event[field] = value.astimezone(tz).isoformat()

async def store_eta_details(
    user_payload: Dict[str, Any],
    active_move: Dict[str, Any],
    next_move: Dict[str, Any],
    driver: str,
    eta: str,
    timeZone: str,
    carrier_preferences: Dict[str, Any]
) -> Dict[str, Any]:
    try:
        carrier = user_payload.get('carrier')
        
        user_payload['timeZone'] = timeZone
        user_payload['distance_unit'] = carrier_preferences.get('distance_unit', 'mi')

        # get load details for active and next load
        reference_numbers = []
        trip_ids = []
        move_ids = []
        if active_move.get('move_type') == 'load':
            reference_numbers.append(active_move.get('reference_number'))
            move_ids.append(active_move.get('moveId'))
        elif active_move.get('tripId'):
            trip_ids.append(active_move.get('tripId'))
        
        if next_move.get('move_type') == 'load':
            reference_numbers.append(next_move.get('reference_number'))
            move_ids.append(next_move.get('moveId'))
        elif next_move.get('tripId'):
            trip_ids.append(next_move.get('tripId'))
        
        loads = await get_loads_with_reference_numbers(
            carrier=carrier,
            reference_numbers=reference_numbers
        )

        loads = map_loads_for_scheduler(loads)

        is_same_moves = False
        if next_move.get('move_type') == 'load' and active_move.get('move_type') == 'load' and next_move.get('reference_number') == active_move.get('reference_number'):
            is_same_moves = True

        moves = []
        for load in loads:
            moves = get_moves_from_driver_order(load['driverOrder'], { 'exclude_void_out': True, 'exclude_combined_move': True })
            del load['driverOrder']

            selected_move = next((move for move in moves if move[0].get('moveId') in move_ids), {})
            if selected_move:
                load['move'] = selected_move
            else:
                raise Exception("Move not found")

        if is_same_moves:
            selected_move_id = loads[0]['move'][0].get('moveId')
            if selected_move_id == move_ids[0]:
                move_ids.remove(selected_move_id)
                selected_move = next((move for move in moves if move[0].get('moveId') in move_ids), {})
                if selected_move:
                    load = loads[0].copy()
                    load['move'] = selected_move
                    loads.append(load)
            else:
                move_ids.remove(selected_move_id)
                selected_move = next((move for move in moves if move[0].get('moveId') in move_ids), {})
                if selected_move:
                    load = loads[0].copy()
                    load['move'] = selected_move
                    loads.insert(0, load)
                    

        all_locations = list(set(event['customerId'] for load in loads for event in load['move'] if event.get('customerId')))
        
        trip_load_reference_numbers = []
        trips = []
        if len(trip_ids) > 0:
            trips = await get_trips_by_trip_ids(carrier, trip_ids)
            
            for trip in trips:
                for combined_move in trip['combinedMoves']:
                    trip_load_reference_numbers.append(combined_move['reference_number'])
                trip['move'] = trip['tripOrder']
                trip['reference_number'] = trip['tripNumber']
                # get customerIds from trip['move']
                all_locations.extend(list(set(event['customerId'] for event in trip['move'] if event.get('customerId'))))

        all_locations = list(set(all_locations))
        trip_loads = []
        if len(trip_load_reference_numbers) > 0:
            trip_loads = await get_loads_with_reference_numbers(
                carrier=carrier,
                reference_numbers=trip_load_reference_numbers
            )

            trip_loads = map_loads_for_scheduler(trip_loads)

        # add waiting time to each of the moves    
        waiting_times = await get_waiting_time(carrier, all_locations)
        loads = add_waiting_time_to_move([*loads, *trips], waiting_times, timeZone)

        # add appointment times to each of the moves
        tz = pytz.timezone(timeZone)
        converted_plan_date = tz.localize(datetime.now()).replace(hour=0, minute=0, second=0)
        location_office_hours = await get_office_hours(carrier, all_locations, timeZone)
        for load in loads:
            attach_loads = [load]
            if load.get('tripType') == 'COMBINED':
                attach_loads = []
                for combined_move in load.get('combinedMoves', []):
                    a_load = next((trip_load for trip_load in trip_loads if trip_load.get('reference_number') == combined_move.get('reference_number')), {})
                    attach_loads.append(a_load)

            if not load.get('tripType') or load.get('tripType') == 'COMBINED':
                actionable_move = populate_appointment_times_to_events(
                    user_payload=user_payload,
                    loads=attach_loads,
                    actionable_move=load['move'],
                    converted_plan_date=converted_plan_date,
                    location_office_hours=location_office_hours
                )

                load['move'] = actionable_move

        active_load = next((load for load in loads if load.get('reference_number') == active_move.get('reference_number')), {})

        if is_same_moves:
            next_load = loads[1]
        else:
            next_load = next((load for load in loads if load.get('reference_number') == next_move.get('reference_number')), {})

        # check move feasibility
        is_move_feasible = True

        # add eta as arrived time
        for event in active_load['move']:
            if event.get('arrived') and not event.get('departed'):
                event['departed'] = eta

        # fill estimated route times for active move
        active_load['move'] = fill_estimated_route_times(active_load['move'], eta, user_payload['distance_unit'])

        # get next move start time
        if next_load.get('move') and len(next_load['move']) > 0:
            active_move_completion_time, next_move_start_time_range = get_eta_for_next_move(
                active_load=active_load,
                next_load=next_load,
                timeZone=timeZone,
                distance_unit=user_payload['distance_unit']
            )

            next_move_start_time = next_move_start_time_range[0]
            if active_move_completion_time > next_move_start_time_range[1]:
                is_move_feasible = False
                next_move_start_time = active_move_completion_time
            elif active_move_completion_time > next_move_start_time_range[0]:
                next_move_start_time = active_move_completion_time

            # fill estimated route times for next move
            next_load['move'] = fill_estimated_route_times(
                next_load['move'],
                next_move_start_time,
                user_payload['distance_unit']
            )

        # Process events for both active and next loads
        for event in active_load['move']:
            process_event(event, timeZone)
        
        if next_load.get('move'):
            for event in next_load['move']:
                process_event(event, timeZone)
        
        calculate_next_move_behind_by = True
        if next_move.get('move_type') == 'load' and not next_load.get('type_of_load') in ['IMPORT', 'EXPORT']:
            calculate_next_move_behind_by = False
        
        next_move_behind_by = 0
        if calculate_next_move_behind_by and not is_move_feasible and next_load.get('move'):
            for event in next_load['move']:
                time_diff = (datetime.fromisoformat(event.get('departed')) - 
                           datetime.fromisoformat(event.get('appointment_to'))).total_seconds() / 60
                if time_diff > 0:
                    next_move_behind_by = int(time_diff)
                    break

        # save driver eta details
        eta_payload = {
            'carrier': carrier,
            'driver': driver,
            'current_move_id': active_load.get('_id'),
            'current_move': active_load['move'],
            'next_move_id': next_load.get('_id', None),
            'next_move': next_load.get('move', []),
            'eta': eta,
            'is_move_feasible': is_move_feasible,
            'next_move_behind_by': next_move_behind_by
        }

        driver_eta_result = await save_driver_eta_details(eta_payload)

        return driver_eta_result
    except Exception as e:
        logger.error(e)
        return []

async def store_driver_eta_details(
    user_payload: Dict[str, Any],
    eta_payloads: List[Dict[str, Any]]
) -> Dict[str, Any]:
    try:
        driver_eta_results = []
        carrier = user_payload.get('carrier')

        timeZone = await get_time_zone(carrier)
        carrier_preferences = await get_carrier_preferences(carrier)

        for eta_payload in eta_payloads:
            active_move = eta_payload.get('active_move')
            next_move = eta_payload.get('next_move', {})
            driver = eta_payload.get('driver')
            eta = eta_payload.get('eta')

            if not (
                active_move.get('reference_number') and 
                ((active_move.get('moveId') and active_move.get('move_type') == 'load') or (active_move.get('tripId') and active_move.get('move_type') == 'trip')) and 
                (not next_move.get('reference_number') or (next_move.get('reference_number') and 
                ((next_move.get('moveId') and next_move.get('move_type') == 'load') or (next_move.get('tripId') and next_move.get('move_type') == 'trip')))) and 
                driver and 
                eta
            ):
                continue

            driver_eta_result = await store_eta_details(
                user_payload=user_payload,
                active_move=active_move,
                next_move=next_move,
                driver=driver,
                eta=eta,
                timeZone=timeZone,
                carrier_preferences=carrier_preferences
            )
            if len(driver_eta_result) > 0:
                driver_eta_results.append(driver_eta_result[0])

        return driver_eta_results
    except Exception as e:
        logger.error(e)
        return []

async def get_eta_details(
    user_payload: Dict[str, Any],
    eta_payload: List[Dict[str, Any]]
) -> Dict[str, Any]:
    try:
        carrier = user_payload.get('carrier')
        timeZone = await get_time_zone(carrier)
        user_payload['timeZone'] = timeZone

        carrier_preferences = await get_carrier_preferences(carrier)
        user_payload['distance_unit'] = carrier_preferences.get('distance_unit', 'mi')

        # merge all loads and trips into a single list from eta_payload has multiple drivers
        loads = []
        trips = []
        for driver_eta in eta_payload:
            eta_loads = driver_eta.get('loads', [])
            for eta_load in eta_loads:
                if eta_load.get('isTrip'):
                    trips.append(eta_load)
                else:
                    loads.append(eta_load)

        all_locations = []

        # get trips from mongodb
        if len(trips) > 0:
            trip_ids = [trip['loadId'] for trip in trips]
            trip_details = await get_trips_by_trip_ids(carrier, trip_ids)

            for trip in trip_details:
                if trip.get('tripType') == 'COMBINED':
                    for combined_move in trip.get('combinedMoves', []):
                        combined_trip_load = {
                            'reference_number': combined_move.get('reference_number'),
                            'moveId': combined_move.get('moveId'),
                            'loadId': combined_move.get('loadId'),
                            'isTrip': False
                        }
                        loads.append(combined_trip_load)
                
                trip['move'] = trip['tripOrder']
                trip['reference_number'] = trip['tripNumber']
                all_locations.extend(list(set(event['customerId'] for event in trip['move'] if event.get('customerId'))))

        # get loads from mongodb
        final_loads = []
        if len(loads) > 0:
            reference_numbers = [load['reference_number'] for load in loads]
            load_details = await get_loads_with_reference_numbers(carrier, reference_numbers)
            
            load_details = map_loads_for_scheduler(load_details)

            for load in loads:
                selected_load = next((load_detail for load_detail in load_details if load_detail.get('reference_number') == load.get('reference_number')), {})
                if selected_load:
                    moves = get_moves_from_driver_order(selected_load['driverOrder'], { 'exclude_void_out': True, 'exclude_combined_move': True })
                    # find same moveId move from moves
                    selected_move = next((move for move in moves if move[0].get('moveId') == load.get('moveId')), {})
                    if selected_move:
                        load_copy = selected_load.copy()
                        load_copy['isTrip'] = False
                        load_copy['move'] = selected_move
                        load_copy['moveId'] = load.get('moveId')
                        del load_copy['driverOrder']
                        final_loads.append(load_copy)
                        all_locations.extend(list(set(event['customerId'] for event in selected_move if event.get('customerId'))))

        tz = pytz.timezone(timeZone)
        converted_plan_date = tz.localize(datetime.now()).replace(hour=0, minute=0, second=0)
        all_locations = list(set(all_locations))
        location_office_hours = await get_office_hours(carrier, all_locations, timeZone)
        
        # map loads and trips for scheduler
        final_trips = []
        if len(trips) > 0:
            for trip in trips:
                selected_trip = next((trip_detail for trip_detail in trip_details if trip_detail.get('_id') == trip.get('loadId')), {})
                if selected_trip:
                    trip_copy = selected_trip.copy()
                    trip_copy['isTrip'] = True
                    combined_trip_load = []
                    for combined_move in selected_trip.get('combinedMoves', []):
                        c_load = next((load for load in load_details if load.get('reference_number') == combined_move.get('reference_number')), {})
                        combined_trip_load.append(c_load)
                    if (len(combined_trip_load) > 0 and selected_trip.get('tripType') == 'COMBINED') or selected_trip.get('tripType') != 'COMBINED':
                        actionable_move = populate_appointment_times_to_events(
                            user_payload=user_payload,
                            loads=combined_trip_load,
                            actionable_move=trip_copy['move'],
                            converted_plan_date=converted_plan_date,
                            location_office_hours=location_office_hours
                        )
                        trip_copy['move'] = actionable_move
                        final_trips.append(trip_copy)
            
        for load in final_loads:
            actionable_move = populate_appointment_times_to_events(
                user_payload=user_payload,
                loads=[load],
                actionable_move=load['move'],
                converted_plan_date=converted_plan_date,
                location_office_hours=location_office_hours
            )
            load['move'] = actionable_move

        # Now calculate ETA for each of loads and trips
        waiting_times = await get_waiting_time(carrier, all_locations)
        all_moves = add_waiting_time_to_move([*final_loads, *final_trips], waiting_times, timeZone)

        move_eta_details = []
        for active_move in all_moves:
            active_move['move'] = [event for event in active_move['move'] if not event.get('isVoidOut')]
            move_duration = calculate_move_eta(active_move, timeZone, user_payload['distance_unit'])
            move_eta_details.append(move_duration)

        return move_eta_details
    except Exception as e:
        logger.error(e)
        return []