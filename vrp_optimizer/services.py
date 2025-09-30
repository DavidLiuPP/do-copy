
import pytz
from copy import deepcopy
from datetime import datetime, timedelta

from app.modules.optimizer.eta_service import estimate_move_start_time
from vrp_optimizer.helpers import (
    minute_from_distance,
    calculate_distance_between_locations,
    get_yard_data,
    get_days_difference,
    get_minute_according_to_start_time
)

def get_available_drivers_from_existing_driver_schedule(existing_driver_schedule, drivers):
    available_drivers = []
    additional_depot_locations = {}
    for driver in deepcopy(drivers):
        if driver.get('_id') in existing_driver_schedule:
            driver_plan = existing_driver_schedule.get(driver.get('_id'))

            remaining_shift_hours = driver.get('total_shift_hours', 14) - driver_plan.get('total_working_hours', 0)

            if remaining_shift_hours <= 0:
                continue


            start_node = driver_plan.get('last_node')
            start_minute = driver_plan.get('last_move_end_minute')

            if start_minute >= (driver.get('end_minute', 24 * 60)):
                continue

            driver['total_shift_hours'] = remaining_shift_hours
            driver['start_node_customer_id'] = start_node.get('customer_id') if start_node else None
            driver['start_minute'] = start_minute

            if start_node:
                additional_depot_locations[start_node.get('customer_id')] = {
                    "start_loc": start_node.get('location'),
                    "end_loc": start_node.get('location'),
                    "depot_customer_id": start_node.get('customer_id'),
                    "company_name": start_node.get('company_name'),

                    "route_distance": 0,
                    "total_waiting_time": 0,
                    "minutes_on_road": 0,
                    "early_arrival_waiting": 0,
                    "locations": [],
                    "_id": "Yard"
                }
        
        available_drivers.append(driver)

        depots = []

        for customer_id in additional_depot_locations:
            depots.append(additional_depot_locations[customer_id])

    return available_drivers, depots


def get_total_journey_time(driver_default_start_location, driver_default_end_location, driver_plan, distance_unit):
    total_journey_time = 0

     # Start from the yard
    last_location = driver_default_start_location if driver_default_start_location else []
    for m in driver_plan:
        move = m.get('move', [])
        total_waiting_time = sum([e.get('waiting_time', 0) for e in move])
        total_distance = sum([e.get('distance', 0) for e in move])

        total_journey_time += minute_from_distance(total_distance, distance_unit) + total_waiting_time

        move_first_location = move[0].get('address', {})

        if last_location and move_first_location:
            empty_miles = calculate_distance_between_locations(last_location, [move_first_location.get('lat', 0), move_first_location.get('lng', 0)], distance_unit)
            total_journey_time += minute_from_distance(empty_miles, distance_unit)

        last_location = [move[-1].get('address', {}).get('lat', 0), move[-1].get('address', {}).get('lng', 0)]


    # Add distance to return to default end location
    if driver_default_end_location:
        miles_to_return = calculate_distance_between_locations(last_location, driver_default_end_location, distance_unit)
        total_journey_time += minute_from_distance(miles_to_return, distance_unit)

    return total_journey_time

def calculate_driver_minute_activity(driver_assigned_moves, timeZone, distance_unit, plan_date, plan_start_minute):
    """Calculate start and end minutes for driver's assigned moves.
    
    Args:
        driver_assigned_moves (list): List of moves assigned to driver
        timeZone (str): Timezone string
        distance_unit (str): Unit for distance calculation
        plan_date (datetime): Reference date for calculations
        
    Returns:
        tuple: (last_move_end_minute, first_move_start_minute)
        
    Raises:
        ValueError: If driver_assigned_moves is empty
        KeyError: If required move data is missing
    """
    try:
        if not driver_assigned_moves:
            print(f"No moves assigned to driver")
            return 0, 0
            
        first_move = driver_assigned_moves[0]

        # Get first move data
        first_move_data = first_move.get('move', [])
        if not first_move_data:
            print(f"Missing move data for first move")
            return 0, 0
            
        # Get start time of first move
        move_start_time = first_move_data[0].get('arrived')
        if not move_start_time:
            start_time_window = estimate_move_start_time(first_move_data, timeZone, distance_unit)
            if start_time_window[0]:
                move_start_time = start_time_window[0].isoformat()
            else:
                move_start_time = (datetime.fromisoformat(plan_date.isoformat()).astimezone(pytz.timezone('UTC')).replace(hour=0, minute=0) + timedelta(minutes=plan_start_minute)).isoformat()

        driver_assigned_moves[0]['move'][0]['arrived'] = driver_assigned_moves[0]['move'][0].get('arrived', move_start_time)
        first_move_start_minute = get_minute_according_to_start_time(
            time=move_start_time,
            start_time=plan_date.isoformat(),
            timeZone=timeZone
        )

        last_move_end_minute = first_move_start_minute

        # calculate last move end time based on all assigned moves
        for index, load in enumerate(driver_assigned_moves):
            previous_event = None
            if index != 0:
                previous_event = driver_assigned_moves[index - 1].get('move', [])[-1]
                move_copy = load.get('move', []).copy()
                move_start_time = estimate_move_start_time(move_copy, timeZone, distance_unit)
                if move_start_time[0]:
                    load['move'][0]['arrived'] = load['move'][0].get('arrived', move_start_time[0].isoformat())

            last_move_end_minute = get_completion_time(load, timeZone, distance_unit, plan_date, previous_event, start_time = last_move_end_minute)

        return last_move_end_minute, first_move_start_minute, driver_assigned_moves
    except (IndexError, KeyError, ValueError) as e:
        print(f"Error calculating driver minute activity: {str(e)}")
        return 0, 0
    except Exception as e:
        print(f"Unexpected error in calculate_driver_minute_activity: {str(e)}")
        return 0, 0

def get_completion_time(load, timeZone, distance_unit, plan_date, previous_event = None, start_time = None):

    move = [e for e in load.get('move', []) if not e.get('isVoidOut', False)]

    if not start_time:
        load_assigned_date = load.get('load_assigned_date') if load.get('load_assigned_date') else plan_date.isoformat()
        start_time = get_minute_according_to_start_time(
            time=load_assigned_date, 
            start_time=plan_date.isoformat(), 
            timeZone=timeZone
        )

    clock = start_time
    for index, event in enumerate(move):
        if index != 0:
            previous_event = move[index - 1]

        if event.get('arrived'):
            clock = get_minute_according_to_start_time(
                time=event.get('arrived'), 
                start_time=plan_date.isoformat(), 
                timeZone=timeZone
            )
        else:
            clock += previous_event.get('waiting_time', 0) if previous_event else 0

        if event.get('departed'):
            clock = get_minute_according_to_start_time(
                time=event.get('departed'), 
                start_time=plan_date.isoformat(), 
                timeZone=timeZone
            )
        else:
            clock += minute_from_distance(event.get('distance', 0), distance_unit)

    clock += event.get('waiting_time', 0)

    return int(clock)


def get_current_plan(all_assigned_moves):
    try:
        current_plan = {}
        for move in all_assigned_moves:
            driver_id = move.get('assigned_driver')
            if driver_id not in current_plan:
                current_plan[driver_id] = []
            current_plan[driver_id].append(move)

        # Sort moves by assigned date
        for d in current_plan:
            current_plan[d].sort(
                key=lambda x: datetime.fromisoformat(
                    x.get('move', [])[0].get('arrived') or 
                    x.get('move', [])[0].get('loadAssignedDate')
                ).timestamp()
            )

        return current_plan
    
    except Exception as e:
        print(f"Error in get_current_plan: {str(e)}")
        raise e


def handle_assigned_moves(user_payload, drivers, default_yard_locations, additional_depot_locations, all_assigned_moves, plan_date, plan_start_minute):
    """Process assigned moves and update driver availability
    
    1. Identifies drivers with active moves
    2. Updates driver start times and locations based on active moves
    3. Filters out drivers who exceed shift limits
    4. Returns available drivers, unassigned moves, and updated depot locations
    """
    try:
        timeZone = user_payload.get('timeZone')
        distance_unit = user_payload.get('distanceUnit', 'mi')

        # Group moves by driver
        current_plan = get_current_plan(all_assigned_moves)
        
        # Initialize depot locations and driver data
        depot_locations = {d.get('depot_customer_id'): d for d in additional_depot_locations}
        driver_data = {driver.get('_id'): driver for driver in drivers}

        # Process each driver's plan
        skipped_drivers = {}
        available_drivers = []
        fixed_plan = {}
        
        for driver_id, driver_details in driver_data.items():
            try:
                driver_plan = current_plan.get(driver_id, [])

                if not driver_plan:
                    available_drivers.append(driver_details)
                    continue


                driver_default_start_location = driver_details.get('manual_location', [0, 0])
                driver_default_end_location = driver_details.get('manual_location', [0, 0])

                # Skip drivers whose total journey exceeds shift hours
                total_journey_time = get_total_journey_time(driver_default_start_location, driver_default_end_location, driver_plan, distance_unit)
                if total_journey_time + 2 * 60 > driver_details.get('total_shift_hours', 14) * 60:
                    skipped_drivers[driver_id] = True
                    fixed_plan[driver_id] = driver_plan
                    continue
                
                # Separate active and assigned moves
                driver_assigned_moves = [m for m in driver_plan if m.get('assigned_driver')]
                
                # Process active moves if they exist
                if driver_assigned_moves:
                    last_move_end_minute, first_move_start_minute, driver_assigned_moves = calculate_driver_minute_activity(driver_assigned_moves, timeZone, distance_unit, plan_date, plan_start_minute)
                    
                    driver_minute_activity = max(last_move_end_minute - first_move_start_minute, 0)
                    # Skip if driver exceeds time constraints
                    last_event = driver_assigned_moves[-1].get('move', [])[-1]
                    location = get_yard_data(last_event)
                    distance_to_yard = calculate_distance_between_locations(location.get('start_loc', []), driver_default_end_location, distance_unit)
                    time_to_reach_to_yard = minute_from_distance(distance_to_yard, distance_unit)

                    time_to_start_shift = driver_details.get('start_minute', 0) - last_move_end_minute

                    if time_to_start_shift > 0:
                        # If driver has done some moves before his shit starts, then plan from shift starting and consider the waiting time of driver as working time.
                        driver_details['start_minute'] = driver_details.get('start_minute', 0)
                        driver_details['max_working_minutes'] = int(driver_details.get('max_working_minutes', 14 * 60) - time_to_start_shift - driver_minute_activity)
                    else:
                        # If driver has already worked upon some moves in the shift, then plan from the last move's ending minute 
                        driver_details['start_minute'] = int(last_move_end_minute)
                        driver_details['max_working_minutes'] = int(driver_details.get('max_working_minutes', 14 * 60) - driver_minute_activity)

                    is_shift_over = last_move_end_minute >= driver_details.get('end_minute', 24 * 60)
                    unable_to_reach_to_yard = driver_details.get('max_working_minutes', 14 * 60) < time_to_reach_to_yard + 2 * 60
                    
                    if is_shift_over or unable_to_reach_to_yard:
                        skipped_drivers[driver_id] = True
                        fixed_plan[driver_id] = driver_plan
                        continue
                    
                    # Update driver details based on last active move
                    driver_details['start_location'] = last_event.get('customerId', None)
                    driver_details['depot_hash_key'] = location.get('hash_key')
                    driver_details['is_working'] = True
                    
                    # Add last location as a depot
                    depot_locations[driver_details['start_location']] = location
                    fixed_plan[driver_id] = driver_assigned_moves
                
                # Add available moves and driver
                available_drivers.append(driver_details)
            except Exception as e:
                continue
        
        # Convert depot locations dictionary to list

        for d in default_yard_locations:
            if d.get('depot_customer_id') and d.get('depot_customer_id') in depot_locations:
                del depot_locations[d.get('depot_customer_id')]

        additional_depot_locations = list(depot_locations.values())
        
        return available_drivers, additional_depot_locations, fixed_plan
    
    except Exception as e:
        print(f"Error in handle_assigned_moves: {str(e)}")
        raise e
    