import pandas as pd
import pytz
from functools import lru_cache
from datetime import datetime, timedelta
from copy import deepcopy
from line_profiler import LineProfiler

from app.utils.distance_calc import calculate_distance_between_locations, calculate_duration_from_distance
from app.postgres_services.driver_rotation_service import apply_driver_rotation_sorting
from app.modules.optimizer.constants import CARRIER_CONFIGS

profiler = LineProfiler()

# Add these constants at the top of the file with other imports
DEFAULT_WORK_START = datetime.strptime('06:00', '%H:%M').time()
DEFAULT_WORK_END = datetime.strptime('23:59', '%H:%M').time()
START_EARLY_TIME = datetime.strptime('00:00', '%H:%M').time()

@lru_cache(maxsize=1024)
def isMileageAllowed(min_mileage, max_mileage, miles):
    try:
        min_miles = 0 if min_mileage is None else min_mileage
        max_miles = 1000 if max_mileage is None or pd.isna(max_mileage) else max_mileage
        return min_miles <= miles <= max_miles
    except Exception as e:
        print(f"Error in isMileageAllowed: {str(e)}")
        raise Exception(f"Error in isMileageAllowed: {str(e)}")


@lru_cache(maxsize=1024)
def isRestricted(restricted_locations_tuple, company_names_tuple):
    try:
        if not restricted_locations_tuple or (len(restricted_locations_tuple) == 1 and restricted_locations_tuple[0] is None):
            return False
        
        restricted_locations = set(restricted_locations_tuple)
        company_names = set(company_names_tuple)
        
        return bool(company_names & restricted_locations)
    except Exception as e:
        print(f"Error in isRestricted: {str(e)}")
        raise Exception(f"Error in isRestricted: {str(e)}")


def check_hos_constraints(driver, trip_duration, drive_hours):
    try:
        driver_total_drive_hours = getattr(driver, 'total_drive_hours', 11) - 0.25
        driver_total_shift_hours = getattr(driver, 'total_shift_hours', 14) - 0.25
        
        if drive_hours > driver_total_drive_hours or trip_duration > driver_total_shift_hours:
            return False
        
        return True
    except Exception as e:
        print(f"Error in check_hos_constraints: {str(e)}")
        raise Exception(f"Error in check_hos_constraints: {str(e)}")

def get_start_time_from_scheduled_move(move):
    try:
        hours_to_reach_appointment = timedelta(hours=0)
        appointment_time = None
        
        for event in move:
            # added travel time to reach the appointment
            hours_to_reach_appointment += calculate_duration_from_distance(event['distance'])
            
            if event.get('appointment_from') and event.get('is_scheduled'):
                appointment_time = event.get('appointment_from')
                break
            
            # added waiting time at the appointment
            hours_to_reach_appointment += timedelta(minutes=event.get('waiting_time', 30))

        return datetime.fromisoformat(appointment_time) - hours_to_reach_appointment

    except Exception as e:
        print(f"Error in get_start_time_from_scheduled_move: {str(e)}")
        raise Exception(f"Error in get_start_time_from_scheduled_move: {str(e)}")

def get_start_time_from_unscheduled_move(move, current_window, previous_window, next_window):
    try:
        if not previous_window and not next_window:
            appointment_time = next((m['appointment_from'] for m in move if m.get('appointment_from')), None)
            if appointment_time:
                return datetime.fromisoformat(appointment_time)
        
        if not next_window:
            return current_window['start']
        else:
            hours_to_reach_appointment = timedelta(hours=0)
            for event in move:
                # added travel time to reach the appointment
                hours_to_reach_appointment += calculate_duration_from_distance(event['distance'])
                # added waiting time at the appointment
                hours_to_reach_appointment += timedelta(minutes=event.get('waiting_time', 30))

            current_last_location = move[-1].get('address', {})
            next_location = next_window['start_location']
            proximity_distance = calculate_distance_between_locations(current_last_location, next_location)
            hours_to_reach_appointment += calculate_duration_from_distance(proximity_distance)

            return next_window['start'] - hours_to_reach_appointment
    except Exception as e:
        print(f"Error in get_start_time_from_unscheduled_move: {str(e)}")
        raise Exception(f"Error in get_start_time_from_unscheduled_move: {str(e)}")

def check_window_time_constraints(current_enroute_time, current_completion_time, previous_window, current_window, next_window):
    try:
        if current_enroute_time < current_window['start'] or current_completion_time > current_window['end']:
            return True

        if previous_window and current_enroute_time - previous_window['end'] > timedelta(hours=1):
            return True # Driver has a break of more than 1 hour between the two windows

        if next_window and next_window['start'] - current_completion_time > timedelta(hours=1):
            return True # Driver has a break of more than 1 hour between the two windows

        return False
    except Exception as e:
        print(f"Error in check_window_time_constraints: {str(e)}")
        raise Exception(f"Error in check_window_time_constraints: {str(e)}")


def check_appointment_constraints(
    move_copy,
    current_time,
    proximity_distance,
    timeZone,
    is_manually_planned
):
    try:
        # Calculate the time required to reach the next destination
        is_satisfying_appointment_constraints = True
        event_enroute_time = current_time
        waiting_time_delta = timedelta(minutes=0)
        
        for idx, event in enumerate(move_copy):
            try:
                if idx == 0:
                    event['distance'] = proximity_distance
                
                move_duration = calculate_duration_from_distance(event['distance'])
                event_arrived_time = event_enroute_time + move_duration

                waiting_time = timedelta(minutes=event.get('waiting_time', 30))
                if event.get('waiting_time_distribution') and event.get('waiting_time_distribution')[f"{event_arrived_time.astimezone(pytz.timezone(timeZone)).hour:02d}"]:
                    hourly_waiting_time = timedelta(minutes=int(event.get('waiting_time_distribution')[f"{event_arrived_time.astimezone(pytz.timezone(timeZone)).hour:02d}"]))
                    if waiting_time > hourly_waiting_time:
                        waiting_time_delta += waiting_time - hourly_waiting_time
                    waiting_time = hourly_waiting_time
                
                event_departed_time = event_arrived_time + waiting_time
                move_copy[idx]['enroute'] = event_enroute_time.isoformat()
                move_copy[idx]['arrived'] = event_arrived_time.isoformat()
                move_copy[idx]['departed'] = event_departed_time.isoformat()

                if event.get('appointment_from') and event.get('appointment_to') and not is_manually_planned:
                    appointment_from = datetime.fromisoformat(event.get('appointment_from'))
                    appointment_to = datetime.fromisoformat(event.get('appointment_to'))

                    # subtract waiting time delta from appointment from time & 2 hour buffer if customer has scheduled another appointment with some buffer time
                    appointment_from = appointment_from - waiting_time_delta - timedelta(hours=2)

                    if not (
                        appointment_from <= 
                        datetime.fromisoformat(move_copy[idx]['arrived']) <= 
                        appointment_to
                    ):
                        is_satisfying_appointment_constraints = False
                        break

                # for next event, enroute time is the departure time of the current event
                event_enroute_time = event_departed_time
            except (ValueError, TypeError) as e:
                continue
        
        return is_satisfying_appointment_constraints, move_copy;
    except Exception as e:
        print(f"Error in check_appointment_constraints: {str(e)}")
        raise Exception(f"Error in check_appointment_constraints: {str(e)}")


def get_move_start_time(move_copy, window_to_assign, previous_window, next_window, move_properties):
    """
    Get the start time for the move based on whether it's a scheduled move or not.
    Returns datetime object representing the move start time.
    """
    try:
        is_scheduled_move = move_properties.get('is_scheduled_move')
        is_manually_planned = move_properties.get('is_manually_planned')

        # Handle manually planned moves
        if is_manually_planned:
            selected_start_time = move_copy[0].get('arrived') or move_copy[0].get('loadAssignedDate')
            driver_start_time = datetime.fromisoformat(selected_start_time)
            
            # For scheduled + manually planned moves, take later of scheduled or manual time
            if is_scheduled_move:
                scheduled_start = get_start_time_from_scheduled_move(move_copy)
                return max(scheduled_start, driver_start_time)
                
            return driver_start_time

        # Handle scheduled moves
        if is_scheduled_move:
            return get_start_time_from_scheduled_move(move_copy)
            
        # Handle unscheduled moves
        return get_start_time_from_unscheduled_move(
            move_copy,
            window_to_assign, 
            previous_window,
            next_window
        )

    except Exception as e:
        print(f"Error in get_move_start_time: {str(e)}")
        raise Exception(f"Error in get_move_start_time: {str(e)}")


def find_available_window(working_windows, move_duration, is_manually_planned):
    """
    Find the first available window that can accommodate the given move duration.
    """
    for indx, window in enumerate(working_windows):
        duration = window['end'] - window['start']
        if (not window.get('start_location')) and duration >= move_duration and not window.get('is_active'):
            return deepcopy(window), indx
    
    if is_manually_planned:
        for indx, window in enumerate(working_windows):
            if (not window.get('start_location')) and not window.get('is_active'):
                return deepcopy(window), indx
    
    return None, None


def calculate_proximity_distances(yard_location, move_copy, previous_window, next_window, move_start_time, plan_date):
    """
    Calculate proximity distances for a move based on previous window and start time.
    """
    try:
        default_address = yard_location
        move_start_address = move_copy[0].get('address', None)
        move_end_address = move_copy[-1].get('address', None)

        if not move_start_address or not move_end_address:
            return None, None

        # Initialize distances
        proximity_distance = 0
        bobtail_start_distance = calculate_distance_between_locations(default_address, move_start_address)
        bobtail_end_distance = calculate_distance_between_locations(move_end_address, default_address)

        # Check if we have a previous window with end location
        if previous_window and previous_window.get('end_location'):
            proximity_distance = calculate_distance_between_locations(previous_window['end_location'], move_start_address)
            move_copy[0]['proximity_included'] = True
            bobtail_start_distance = 0

            is_from_default_location = int(calculate_distance_between_locations(previous_window['end_location'], default_address)) == 0
            if not is_from_default_location and proximity_distance > 30:
                proximity_distance = None
        
        # Check if move starts before work day
        elif move_start_time <= plan_date.replace(hour=DEFAULT_WORK_START.hour, minute=DEFAULT_WORK_START.minute):
            proximity_distance = calculate_distance_between_locations(default_address, move_start_address)
            move_copy[0]['proximity_included'] = True
            bobtail_start_distance = 0

        # Zero out bobtail end distance if next window exists with start location
        bobtail_end_distance = 0 if next_window and next_window.get('start_location') else bobtail_end_distance
        
        return proximity_distance, bobtail_start_distance + bobtail_end_distance
    
    except Exception as e:
        print(f"Error in calculate_proximity_distances: {str(e)}")
        raise Exception(f"Error in calculate_proximity_distances: {str(e)}")


def calculate_idle_time(move_start_time, completion_time, previous_window, next_window):
    """
    Calculate the idle time between windows for a driver.
    """
    try:
        idle_time = timedelta(hours=0)
        if previous_window:
            idle_time += move_start_time - previous_window['end']
        elif next_window:
            idle_time += next_window['start'] - completion_time
        return idle_time.total_seconds() / 3600
    
    except Exception as e:
        print(f"Error in calculate_idle_time: {str(e)}")
        raise Exception(f"Error in calculate_idle_time: {str(e)}")


def get_optimal_move(yard_location, moves, driver, working_windows, plan_date, timeZone):
    try:
        best_move = None
        min_obj_fun_val = float('inf')

        for load in moves.itertuples():
            if load.assigned:
                continue

            is_manually_planned = load.is_manually_planned
            is_scheduled_move = any(event.get('is_scheduled') for event in load.move)

            if is_manually_planned and load.assigned_driver != driver.id:
                continue

            window_to_assign, window_to_assign_index = find_available_window(working_windows, load.move_duration, is_manually_planned)
            if not window_to_assign:
                continue # no available window to assign

            move_copy = deepcopy(load.move)

            # Check the restricted drivers
            company_names = tuple(move.get('company_name', '') for move in move_copy)
            restricted_locations = tuple(getattr(driver, 'restricted_locations', []))
            if isRestricted(restricted_locations, company_names) and not is_manually_planned:
                continue  # Skip this move

            # Check the mileage band
            # total distance will be sum of distance in the move
            total_distance = sum(event.get('distance', 0) for event in move_copy)
            if not isMileageAllowed(None, getattr(driver, 'max_mileage', None), total_distance) and not is_manually_planned:
                continue  # Skip this move

            # Check HOS constraints and violation
            trip_duration = load.move_duration.total_seconds() / 3600
            drive_hours = (load.move_duration - timedelta(minutes=load.waiting_time)).total_seconds() / 3600
            if not check_hos_constraints(driver, trip_duration, drive_hours) and not is_manually_planned:
                continue  # Skip this move

            # Check if the current load can be assigned to any available driver
            if (is_scheduled_move or is_manually_planned) and window_to_assign.get('start').time() == DEFAULT_WORK_START:
                start_early_time = datetime.strptime(driver.start_time, "%H:%M").time() if driver.start_time else START_EARLY_TIME
                window_to_assign['start'] = plan_date.replace(hour=start_early_time.hour, minute=start_early_time.minute)

            previous_window = working_windows[window_to_assign_index - 1] if window_to_assign_index > 0 else None
            next_window = working_windows[window_to_assign_index + 1] if window_to_assign_index < len(working_windows) - 1 else None

            move_start_time = get_move_start_time(
                move_copy,
                window_to_assign,
                previous_window,
                next_window,
                {
                    'is_scheduled_move': is_scheduled_move,
                    'is_manually_planned': is_manually_planned
                }
            )

            # Calculate proximity distances
            proximity_distance, bobtail_distance = calculate_proximity_distances(
                yard_location,
                move_copy, 
                previous_window, 
                next_window,
                move_start_time, 
                plan_date
            )

            # Skip this move if proximity distance is not valid
            if proximity_distance is None:
                continue

            # Check the appointment constraints
            is_satisfying_appointment_constraints, move_with_action_times = check_appointment_constraints(
                move_copy,
                move_start_time,
                proximity_distance,
                timeZone,
                is_manually_planned
            )
            if not is_satisfying_appointment_constraints:
                continue  # Skip this move

            if move_with_action_times[-1]['departed']:
                completion_time = datetime.fromisoformat(move_with_action_times[-1]['departed'])
            
            if next_window and next_window.get('start_location', None):
                next_location = next_window['start_location']
                proximity_distance = calculate_distance_between_locations(move_with_action_times[-1]['address'], next_location)
            
            # final move duration and drive hours
            final_move_duration = completion_time - move_start_time
            trip_duration = final_move_duration.total_seconds() / 3600
            drive_hours = (final_move_duration - timedelta(minutes=load.waiting_time)).total_seconds() / 3600

            # Check HOS constraints and violation with updated hours & bobtail distance
            extra_drive_hours = calculate_duration_from_distance(bobtail_distance).total_seconds() / 3600
            if not check_hos_constraints(driver, trip_duration + extra_drive_hours, drive_hours + extra_drive_hours) and not is_manually_planned:
                continue  # Skip this move
            
            # check the window time constraints
            if (check_window_time_constraints(
                move_start_time,
                completion_time,
                previous_window,
                window_to_assign,
                next_window
            ) and not is_manually_planned):
                continue

            # calculate the idle time
            idle_time_hours = calculate_idle_time(move_start_time, completion_time, previous_window, next_window)

            # calculate the objective function value
            obj_fun_val = calculate_duration_from_distance(proximity_distance).total_seconds() # +load_wait_time
            
            if obj_fun_val < min_obj_fun_val or is_scheduled_move:
                min_obj_fun_val = obj_fun_val
                best_move = load._asdict()
                best_move['enroute_time'] = move_start_time
                best_move['completion_time'] = completion_time
                best_move['assigned'] = True
                best_move['trip_duration'] = trip_duration + idle_time_hours
                best_move['drive_hours'] = drive_hours
                best_move['move'] = move_with_action_times

            if int(obj_fun_val) == 0 or is_manually_planned:
                break

        return best_move

    except Exception as e:
        print(f"Error in get_optimal_move: {str(e)}")
        raise Exception(f"Error in get_optimal_move: {str(e)}")
    
def get_driver_working_windows(driver_schedule, plan_date, work_start, work_end, timeZone):
    try:
        # Sort occupied windows by start time
        working_windows = [];
        occupied_windows = sorted(driver_schedule, key=lambda x: x['enroute_time'])
        for indx, window in enumerate(occupied_windows):
            if indx == 0 and window['enroute_time'] > work_start:
                working_windows.append({
                    'start': plan_date.replace(hour=work_start.hour, minute=work_start.minute),
                    'end': window['enroute_time'].astimezone(pytz.timezone(timeZone)),
                    'is_active': window.get('is_active_move', False)
                })
            
            working_windows.append({
                'start': window['enroute_time'].astimezone(pytz.timezone(timeZone)),
                'end': window['completion_time'].astimezone(pytz.timezone(timeZone)),
                'start_location': window['start_location'],
                'end_location': window['end_location'],
                'is_active': window.get('is_active_move', False)
            })
            
            if indx == len(occupied_windows) - 1 and window['completion_time'] < work_end:
                working_windows.append({
                    'start': window['completion_time'].astimezone(pytz.timezone(timeZone)),
                    'end': plan_date.replace(hour=work_end.hour, minute=work_end.minute),
                    'is_active': False
                })

        if len(working_windows) == 0:
            working_windows = [{
                'start': plan_date.replace(hour=work_start.hour, minute=work_start.minute),
                'end': plan_date.replace(hour=work_end.hour, minute=work_end.minute),
                'is_active': False
            }]

        # filter working_windows with less than 1 hour
        working_windows = [
            window for window in working_windows 
            if window['end'] - window['start'] > timedelta(hours=1) or window.get('start_location', None)
        ]
        return working_windows
    except Exception as e:
        print(f"Error in get_driver_working_windows: {str(e)}")
        raise Exception(f"Error in get_driver_working_windows: {str(e)}")

def process_moves(
    user_payload,
    moves,
    drivers,
    plan_date
):
    try:
        assignments = []
        driver_schedule = {}

        timeZone = user_payload.get('timeZone')
        assigned_drivers = []

        for index, driver in drivers.iterrows():
            if not driver.id in driver_schedule:
                driver_schedule[driver.id] = deepcopy(driver.get('schedule', []))
            
            assigned_drivers.append(driver.id)

            yard_location = driver.get('depot_location', { 'lat': 0, 'lng': 0 })
            
            driver_work_start = plan_date.replace(hour=driver.work_start.hour, minute=driver.work_start.minute)
            default_work_start = plan_date.replace(hour=DEFAULT_WORK_START.hour, minute=DEFAULT_WORK_START.minute)
            
            work_start = max(driver_work_start, default_work_start)
            work_end = plan_date.replace(hour=driver.work_end.hour, minute=driver.work_end.minute)

            working_windows = get_driver_working_windows(driver_schedule[driver.id], plan_date, work_start, work_end, timeZone)

            while driver.total_drive_hours > 1 or driver.total_shift_hours > 1:
                # get unassigned moves
                unassigned_moves = moves[~moves['assigned']]

                if unassigned_moves.empty:
                    break

                move = get_optimal_move(yard_location, unassigned_moves, driver, working_windows, plan_date, timeZone)
                if move:
                    move['move'] = [
                        {k: v for k, v in event.items() if k not in ['waiting_time_distribution']}
                        for event in move['move']
                    ]
                    mapped_assignment = map_assignment(move, driver, timeZone)
                    assignments.append(mapped_assignment)

                    driver_schedule[driver.id].append({
                        'enroute_time': move['enroute_time'],
                        'completion_time': move['completion_time'],
                        'start_location': move['move'][0].get('address', {}),
                        'end_location': move['move'][-1].get('address', {})
                    })

                    driver['total_drive_hours'] = driver.total_drive_hours - move['drive_hours']
                    driver['total_shift_hours'] = driver.total_shift_hours - move['trip_duration']

                    # update the moves dataframe
                    move_idx = moves.index[(moves['id'] == move['id']) & (moves['route'] == move['route'])].values[0]
                    moves.at[move_idx, 'assigned'] = True
                    moves.at[move_idx, 'enroute_time'] = move['enroute_time']
                    moves.at[move_idx, 'completion_time'] = move['completion_time']
                    moves.at[move_idx, 'trip_duration'] = move['trip_duration']
                    moves.at[move_idx, 'drive_hours'] = move['drive_hours']
                    moves.at[move_idx, 'move'] = move['move']

                    working_windows = get_driver_working_windows(driver_schedule[driver.id], plan_date, work_start, work_end, timeZone)
                else:
                    break

        # Sort assignments based on driver name and enroute time
        assignments.sort(key=lambda x: (
            assigned_drivers.index(x['assigned_driver']), 
            x['enroute_time']
        ))

        # link the assignments for a driver
        current_driver = None
        for assignment in assignments:
            assignment['driver_index'] = assigned_drivers.index(assignment['assigned_driver'])
            
            if current_driver != assignment['assigned_driver']:
                current_driver = assignment['assigned_driver']
                last_location = yard_location
            
            if assignment['move'][0].get('proximity_included', False):
                continue

            if not last_location:
                continue

            proximity_distance = calculate_distance_between_locations(last_location, assignment['move'][0].get('address', {}))

            proximity_duration = calculate_duration_from_distance(proximity_distance)

            assignment['empty_miles'] = proximity_distance
            assignment['enroute_time'] = assignment['enroute_time'] - proximity_duration
            assignment['move'][0]['enroute'] = assignment['enroute_time'].isoformat()
            last_location = assignment['move'][-1]['address']
        
        return assignments, driver_schedule
    
    except Exception as e:
        print(f"Error in process_moves: {str(e)}")
        raise Exception(f"Error in process_moves: {str(e)}")

def get_formatted_time(time, timeZone):
    try:
        dt = datetime.fromisoformat(time)
        if dt.tzinfo is None:
            # If no timezone info, assume UTC
            dt = dt.replace(tzinfo=pytz.UTC)
        return dt.astimezone(pytz.timezone(timeZone)).strftime('%m/%d/%Y %H:%M')
    except Exception as e:
        print(f"Error in get_formatted_time: {str(e)}")
        raise Exception(f"Error in get_formatted_time: {str(e)}")

def map_assignment(load, driver, timeZone, is_assigned_move=False):
    try:
        # get pickup, delivery and return times
        pickup_appt_time = get_formatted_time(load.get('pickupFromTime'), timeZone) if load.get('pickupFromTime') else None
        delivery_appt_time = get_formatted_time(load.get('deliveryFromTime'), timeZone) if load.get('deliveryFromTime') else None 
        return_appt_time = get_formatted_time(load.get('returnFromTime'), timeZone) if load.get('returnFromTime') else None
        
        driver_name = f"{driver['name']} {driver['last_name']}" if pd.notna(driver['name']) else ""

        mapped_assignment = {
            'load_id': load['id'],
            'reference_number': load['reference_number'],
            'customer': load['callerName'],
            'assigned_driver': driver['id'],
            'assigned_driver_name': driver_name,
            'move': load['move'],
            'enroute_time': load['enroute_time'],
            'completion_time': load['completion_time'],
            'pickup_time': pickup_appt_time,
            'delivery_time': delivery_appt_time,
            'return_time': return_appt_time,
            'distance': load['distance'],
            'revenue': load['revenue'],
            'driver_pay': 0,
            'is_modified_move': False if pd.isna(load.get('is_modified_move')) else bool(load.get('is_modified_move')),
            'is_assigned_move': is_assigned_move
        }
        
        return mapped_assignment
    except Exception as e:
        print(f"Error in map_assignment: {str(e)}")
        raise Exception(f"Error in map_assignment: {str(e)}")


async def get_optimal_plan(user_payload, moves, drivers, plan_date):
    try:
        # Pre-process drivers DataFrame to set default values and apply driver rotation sorting
        drivers = pd.DataFrame(drivers)
        drivers['id'] = drivers['_id']
        drivers['work_start'] = drivers['start_time'].apply(lambda x: datetime.strptime(x, "%H:%M").time() if pd.notna(x) else DEFAULT_WORK_START)
        drivers['work_end'] = drivers['end_time'].apply(lambda x: datetime.strptime(x, "%H:%M").time() if pd.notna(x) else DEFAULT_WORK_END)
        drivers['per_mile_pay'] = pd.to_numeric(drivers['per_mile_pay'], errors='coerce')
        drivers['owner_score'] = pd.to_numeric(drivers['owner_score'], errors='coerce').fillna(5)
        drivers['driver_score'] = 100 * drivers['per_mile_pay'] * (1 + (5 - drivers['owner_score']) / 5)
        
        # Apply driver rotation sorting based on carrier configuration
        carrier = user_payload.get('carrier')
        drivers_dict = drivers.to_dict('records')
        
        sorted_drivers_dict = await apply_driver_rotation_sorting(
            drivers_dict, 
            carrier, 
            plan_date,
            rotation_order=CARRIER_CONFIGS.get(carrier, {}).get('rotation_order', ['owner_score'])
        )
        drivers = pd.DataFrame(sorted_drivers_dict)

        # Add move_duration field for each move
        moves = pd.DataFrame(moves)
        moves['id'] = moves['_id']
        moves['assigned'] = False
        moves['is_manually_planned'] = moves['is_manually_planned'].fillna(False).infer_objects(copy=False)
        moves['distance'] = moves['move'].apply(lambda x: sum(move['distance'] for move in x))
        moves['move_duration'] = moves.apply(
            lambda x: timedelta(minutes=x['waiting_time']) + 
                     calculate_duration_from_distance(x['distance']),
            axis=1
        )

        # Call process_moves with the pre-processed data
        assignments, driver_schedule = process_moves(
            user_payload,
            moves,
            drivers,
            plan_date,
        )

        # Convert datetime objects to ISO format strings
        assigned_moves_dict = {ref: 1 for ref in moves[moves['is_manually_planned']]['reference_number']}
        for assignment in assignments:
            assignment['arrival_time'] = assignment['enroute_time'].isoformat()
            assignment['enroute_time'] = assignment['enroute_time'].isoformat()
            assignment['completion_time'] = assignment['completion_time'].isoformat()
            assignment['is_new_move'] = assignment['reference_number'] not in assigned_moves_dict

        # # Get reference numbers from all_assignments
        # assigned_refs = set(assignment['reference_number'] for assignment in assignments)
        # all_refs = set(moves['reference_number'].unique())
        # unassigned_refs = all_refs - assigned_refs
        # unassigned_moves = moves[moves['reference_number'].isin(unassigned_refs)].to_dict('records')
        # unassigned_moves = [move for move in unassigned_moves if move['move_duration'] <= timedelta(hours=14)]
        # unassigned_moves = [(move['reference_number'], move['distance'], move['move_duration'], move['move']) for move in unassigned_moves]

        driver_schedule = {k: v for k, v in driver_schedule.items() if v}

        return assignments, driver_schedule

    except Exception as e:
        raise Exception(f"Failed to get optimal plan: {str(e)}")

def get_unassigned_moves(optimal_plan, all_loads, invalid_moves, skipped_moves, plan_date, timeZone, actionable_moves):
    try:
        # Added unassigned load's reason

        # Freeflow trips
        free_flow_moves = [move for move in actionable_moves if move.get('is_free_flow_move')]

        if free_flow_moves:
            related_load_ids = list(set(move.get('related_load_id') for move in free_flow_moves))

            # remove the moves for which the free flow trip was used for planning
            all_loads = [load for load in all_loads if load.get('_id') not in related_load_ids]
            
            # Add the free flow trips
            all_loads = all_loads + free_flow_moves

        for move in all_loads:
            invalid_move = next((im for im in invalid_moves if im.get('reference_number') == move.get('reference_number')), None)
            if invalid_move:
                move['reason'] = invalid_move.get('reason', '')
            skipped_move = next((im for im in skipped_moves if im.get('reference_number') == move.get('reference_number')), None)
            if skipped_move:
                move['reason'] = skipped_move.get('reason', '')

        assigned_refs = set(assignment['reference_number'] for assignment in optimal_plan)
        all_refs = set(load['reference_number'] for load in all_loads)
        unassigned_refs = all_refs - assigned_refs

        unassigned_records = [load for load in all_loads if load['reference_number'] in unassigned_refs]
        unassigned_loads = []
        tz = pytz.timezone(timeZone)

        for move in unassigned_records:
            # only consider records that do have scheduled appointments and are not planned by the optimizer
            pickup_apt_time = datetime.fromisoformat(move.get('pickupFromTime')) if move.get('pickupFromTime') else None
            delivery_apt_time = datetime.fromisoformat(move.get('deliveryFromTime')) if move.get('deliveryFromTime') else None
            return_apt_time = datetime.fromisoformat(move.get('returnFromTime')) if move.get('returnFromTime') else None

            is_pickup_scheduled_on_plan_date = pickup_apt_time and pickup_apt_time.astimezone(tz).date() == plan_date.date() and not move.get('is_manually_planned')
            is_delivery_scheduled_on_plan_date = delivery_apt_time and delivery_apt_time.astimezone(tz).date() == plan_date.date() and not move.get('is_manually_planned')
            is_return_scheduled_on_plan_date = return_apt_time and return_apt_time.astimezone(tz).date() == plan_date.date() and not move.get('is_manually_planned')
            
            if not is_pickup_scheduled_on_plan_date and not is_delivery_scheduled_on_plan_date and not is_return_scheduled_on_plan_date:
                continue

            if move.get('move'):
                for event in move['move']:
                    event.pop('waiting_time_distribution', None)

            mapped_move = {
                'load_id': move['_id'],
                'reference_number': move['reference_number'], 
                'customer': move['callerName'],
                'assigned_driver': None,
                'assigned_driver_name': None,
                'move': move.get('driverOrder') or move.get('move') or [],
                'enroute_time': None,
                'completion_time': None,
                'pickup_time': move['pickupFromTime'],
                'delivery_time': move['deliveryFromTime'], 
                'return_time': move['returnFromTime'],
                'distance': move['distance'],
                'revenue': move['revenue'],
                'driver_pay': 0,
                'is_modified_move': False,
                'is_assigned_move': False,
                'is_deleted': True,
                'is_free_flow_move': move.get('is_free_flow_move', False),
                'reason': move.get('reason', '')
                        or move.get('error_reason', '')
                        or 'SKIPPED_BY_OPTIMIZER'
            }
            unassigned_loads.append(mapped_move)

        return unassigned_loads
    except Exception as e:
        raise Exception(f"Failed to process unassigned moves: {str(e)}")
