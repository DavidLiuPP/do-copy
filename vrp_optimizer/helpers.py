from typing import Tuple, List
from datetime import datetime, timedelta
import pytz
from functools import lru_cache
from geopy.distance import geodesic
from copy import deepcopy
from vrp_optimizer.assumptions import CHASSIS_ACTIVITIES
from app.modules.optimizer.constants import DISTANCE_MULTIPLIER, CHASSIS_YARD_CONFIGS
    
from vrp_optimizer.routing_distance import (
    get_distance_between_locations_from_matrix
)

@lru_cache(maxsize=1024)
def calculate_distance(lat1, lng1, lat2, lng2, unit = 'mi'):
    if unit == 'km':
        return geodesic((lat1, lng1), (lat2, lng2)).kilometers * DISTANCE_MULTIPLIER
    return geodesic((lat1, lng1), (lat2, lng2)).miles * DISTANCE_MULTIPLIER

def calculate_distance_between_locations(loc1: Tuple[float, float], loc2: Tuple[float, float], unit = 'mi'):
    if loc1[0] and loc2[0] and loc1[1] and loc2[1]:
        miles = calculate_distance(loc1[0], loc1[1], loc2[0], loc2[1], unit)
        return miles
    return 9999

def calculate_distance_between_locations_list(locations: List[Tuple[float, float]], unit = 'mi'):
    distance = 0
    for i in range(len(locations) - 1):
        distance += calculate_distance_between_locations(locations[i], locations[i + 1], unit)
    return distance

def get_days_difference(date1, date2, timezone):
    dt1 = datetime.fromisoformat(date1).astimezone(pytz.timezone(timezone))
    dt2 = datetime.fromisoformat(date2).astimezone(pytz.timezone(timezone))
    return (dt2.date() - dt1.date()).days

def get_minute_according_to_start_time(time, start_time, timeZone):
    minute_of_date = get_minute(time, timeZone)

    days_difference = get_days_difference(start_time, time, timeZone)
    days_difference_minutes = days_difference * 24 * 60

    return minute_of_date + days_difference_minutes


@lru_cache(maxsize=1024)
def minute_from_distance(distance, unit = 'mi'):
    speed = (
        20 if distance <= 10 else
        40 if distance <= 50 else
        60
    )

    if unit == 'km':
        speed = speed * 1.60934

    return (distance / speed) * 60 + 10


def show_time_from_minute_of_day(minute_of_day):

    is_next_day = False
    if minute_of_day > 24 * 60:
        minute_of_day = minute_of_day - 24 * 60
        is_next_day = True

    hour = int(int(minute_of_day)/60)
    minute = int(minute_of_day%60)
    return f"{"0" if hour < 10 else ""}{hour}:{"0" if minute < 10 else ""}{minute}{" (Next Day)" if is_next_day else ""}"


def get_minute(date, timezone):
    dt = datetime.fromisoformat(date).astimezone(pytz.timezone(timezone))
    return dt.hour * 60 + dt.minute


def get_cost_from_distance(distance):
    try:
        cost = 0
        remaining_distance = distance

        for i in range(4):
            if remaining_distance <= 0:
                break

            segment = min(remaining_distance, 10)
            multiplier = i + 1  # starts from 1
            cost += segment * multiplier
            remaining_distance -= segment

        if remaining_distance > 0:
            cost = 1000

        return cost
    except Exception as e:
        print(e)
        return 0


def get_yard_data(yard_locations):

    lat = yard_locations.get('address', {}).get('lat', 0)
    lng = yard_locations.get('address', {}).get('lng', 0)
    customer_id = yard_locations.get('customerId')

    hash_key = f"{lat}-{lng}-{customer_id}"

    return {
            'start_loc': [ lat, lng ],
            'end_loc': [ lat, lng ],
            'hash_key': hash_key,
            'depot_customer_id': customer_id,
            'company_name': yard_locations.get('company_name'),
            'yard_id': yard_locations.get('id'),
            'is_chassis_pick_allowed': yard_locations.get('is_chassis_pick_allowed', True),
            'is_chassis_termination_allowed': yard_locations.get('is_chassis_termination_allowed', True),
            'route_distance': 0,
            'total_waiting_time': 0,
            'minutes_on_road': 0,
            'early_arrival_waiting': 0,
            'locations': [],
            '_id': 'Yard'
        }


def get_yard_data_for_driver(driver):
    lat = driver.get('depot_location', {}).get('lat', 0)
    lng = driver.get('depot_location', {}).get('lng', 0)
    customer_id = driver.get('depot_customer_id', '')

    hash_key = f"{lat}-{lng}-{customer_id}"

    return {
            'start_loc': [ lat, lng ],
            'end_loc': [ lat, lng ],
            'depot_customer_id': customer_id,
            'is_manual_location': not customer_id,
            'hash_key': hash_key,
            'company_name': "",
            'yard_id': "",
            'route_distance': 0,
            'total_waiting_time': 0,
            'minutes_on_road': 0,
            'early_arrival_waiting': 0,
            'locations': [],
            '_id': 'Yard'
        }


def get_all_driver_default_locations(drivers):
    data = set()

    location_data = []
    for d in drivers:
        lat = d.get('depot_location', {}).get('lat', 0)
        lng = d.get('depot_location', {}).get('lng', 0)
        customer_id = d.get('depot_customer_id', '')

        hash_key = f"{lat}-{lng}-{customer_id}"

        if hash_key not in data:
            data.add(hash_key)
            location_data.append(get_yard_data_for_driver(d))

    return location_data


def get_chassis_activity_between_moves(
        source,
        destination,
        equipment_validations,
        yards,
        location_distance_matrix,
        carrier_id: str = None
    ):

    if source.get('isDepot') and destination.get('isDepot'):
        return { 'type': CHASSIS_ACTIVITIES["NO_ACTION"] }
    
    if source.get('is_free_flow_move', False) or destination.get('is_free_flow_move', False):
        return { 'type': CHASSIS_ACTIVITIES["NO_ACTION"] }

    source_end_event = source.get('end_event')
    dest_start_event = destination.get('start_event')

    source_end = source.get('end_loc')
    dest_start = destination.get('start_loc')
    
    # Get best yard for pickup
    best_yard_for_pickup = get_best_yard_for_chassis_action(source_end, dest_start, yards, location_distance_matrix, is_termination=False, carrier_id=carrier_id, destination_move=destination)
    
    # Get best yard for termination
    best_yard_for_termination = get_best_yard_for_chassis_action(source_end, dest_start, yards, location_distance_matrix, is_termination=True, carrier_id=carrier_id, destination_move=destination)

    if source.get('isDepot') and dest_start_event and dest_start_event.get('type') in ['PULLCONTAINER', 'LIFTON']:
        return { 'type': CHASSIS_ACTIVITIES["HOOKCHASSIS"], 'hook_yard': best_yard_for_pickup }
    
    if destination.get('isDepot') and source_end_event and source_end_event.get('type') in ['RETURNCONTAINER', 'LIFTOFF']:
        return { 'type': CHASSIS_ACTIVITIES["DROPCHASSIS"], 'drop_yard': best_yard_for_termination }

    if source_end_event and dest_start_event:
        end_type = source_end_event.get('type')
        start_type = dest_start_event.get('type')

        # For roadex, handle port chassis
        additional_chassis_activity = []
        if (
            source.get('chassisNo') and 
            not source.get('chassisId') and 
            not "AIMZ" in source.get('chassisNo') and 
            end_type in ['RETURNCONTAINER', 'LIFTOFF']
        ):
            
            end_type = 'CHASSISTERMINATION'
            additional_chassis_activity.append({
                'type': CHASSIS_ACTIVITIES["DROPCHASSIS"],
                'drop_yard': {
                    'location_id': source_end_event['customerId'],
                    'location': source_end
                }
            })
        
        if (
            destination.get('chassisNo') and 
            not destination.get('chassisId') and 
            not "AIMZ" in destination.get('chassisNo') and 
            start_type in ['PULLCONTAINER', 'LIFTON']
        ):
            
            start_type = 'CHASSISPICK'
            additional_chassis_activity.append({
                'type': CHASSIS_ACTIVITIES["HOOKCHASSIS"],
                'hook_yard': {
                    'location_id': dest_start_event['customerId'],
                    'location': dest_start
                }
            })

        # Map of end_type -> list of start_types that require chassis
        connected_moves = {
            'RETURNCONTAINER': ['PULLCONTAINER', 'LIFTON'],
            'DROPCONTAINER': ['HOOKCONTAINER', 'CHASSISPICK'], 
            'LIFTOFF': ['PULLCONTAINER', 'LIFTON'],
            'CHASSISTERMINATION': ['HOOKCONTAINER', 'CHASSISPICK']
        }

        is_connected_move = start_type in connected_moves.get(end_type, [])

        # if not connected move, then chassis activity is required
        if not is_connected_move:
            does_last_event_have_chassis = end_type in ['RETURNCONTAINER', 'LIFTOFF']
            chassis_activity = (
                { 'type': CHASSIS_ACTIVITIES["DROPCHASSIS"], 'drop_yard': best_yard_for_termination }
                if does_last_event_have_chassis
                else { 'type': CHASSIS_ACTIVITIES["HOOKCHASSIS"], 'hook_yard': best_yard_for_pickup }
            )

            additional_chassis_activity.append(chassis_activity)
        
        if additional_chassis_activity:
            if len(additional_chassis_activity) == 1:
                return additional_chassis_activity[0]
            else:
                return {
                    'type': CHASSIS_ACTIVITIES["DROP_AND_HOOK_CHASSIS"],
                    'drop_yard': next(activity for activity in additional_chassis_activity if activity.get('drop_yard')).get('drop_yard'),
                    'hook_yard': next(activity for activity in additional_chassis_activity if activity.get('hook_yard')).get('hook_yard')
                }

        # if connected move, then chassis activity is required if the destination is a yard
        source_container_size = source.get('container_size_label')
        source_container_type = source.get('container_type_label')
        dest_container_size = destination.get('container_size_label')
        dest_container_type = destination.get('container_type_label')

        if equipment_validations and source_container_size and source_container_type and dest_container_size and dest_container_type:
            source_chassis = [
                e['chassisSize'] + e['chassisType'] 
                for e in equipment_validations 
                if e.get('containerSize') == source_container_size 
                and e.get('containerType') == source_container_type
            ]
            dest_chassis = [
                e['chassisSize'] + e['chassisType']
                for e in equipment_validations
                if e.get('containerSize') == dest_container_size
                and e.get('containerType') == dest_container_type
            ]

            if not source_chassis or not dest_chassis:
                return { 'type': CHASSIS_ACTIVITIES["NO_ACTION"] }

            # if there is no common chassis in source and dest, then chassis activity is required
            if not any(chassis in source_chassis for chassis in dest_chassis):
                return {
                    'type': CHASSIS_ACTIVITIES["DROP_AND_HOOK_CHASSIS"],
                    'drop_yard': best_yard_for_termination,
                    'hook_yard': best_yard_for_pickup
                }

    return { 'type': CHASSIS_ACTIVITIES["NO_ACTION"] }


def get_best_yard_for_chassis_action(source_location: Tuple[float, float], dest_location: Tuple[float, float], yard_locations: list, location_distance_matrix: list, is_termination: bool = False, carrier_id: str = None, destination_move: dict = None):

    _yard_locations = deepcopy(yard_locations)
    
    # Filter yards based on chassis operation permissions from database
    if is_termination:
        # For chassis termination, only use yards where is_chassis_termination_allowed is True (default True if not present)
        _yard_locations = [
            yard for yard in _yard_locations 
            if yard.get('is_chassis_termination_allowed', True)
        ]
    else:
        # For chassis pickup, only use yards where is_chassis_pick_allowed is True (default True if not present)
        _yard_locations = [
            yard for yard in _yard_locations 
            if yard.get('is_chassis_pick_allowed', True)
        ]

    # Check if carrier has specific chassis yard configurations
    if not is_termination and carrier_id and destination_move and carrier_id in CHASSIS_YARD_CONFIGS:
        carrier_config = CHASSIS_YARD_CONFIGS[carrier_id]
        load_type_detector = carrier_config.get('load_type_detector', {})
        
        # Determine load type based on consignee name
        consignee_name = destination_move.get(load_type_detector.get('field', 'consigneeName'), '')
        consignee_name_upper = consignee_name.upper() if consignee_name else ''
        
        load_type = None
        
        prefixes = load_type_detector.get('prefixes', {})
        for load_type_name, prefix in prefixes.items():
            if consignee_name_upper.startswith(prefix):
                load_type = load_type_name
                break
        
        if not load_type:
            load_type = 'default_yard'
        
        # Get the appropriate yard configuration
        yard_config = carrier_config.get(load_type, {})
        preferred_yard_id = yard_config.get('yard_customer_id')
        
        if preferred_yard_id:
            # Find the preferred yard in available yards
            preferred_yard = next((yard for yard in _yard_locations if yard.get('depot_customer_id') == preferred_yard_id), None)
            if preferred_yard:
                return { 'location_id': preferred_yard.get('depot_customer_id'), 'location': preferred_yard.get('start_loc') }

    # Fallback to distance-based selection
    for yard in _yard_locations:
        yard_location = yard.get('start_loc')
        
        source_to_yard = get_distance_between_locations_from_matrix(source_location, yard_location, location_distance_matrix)
        yard_to_destination = get_distance_between_locations_from_matrix(yard_location, dest_location, location_distance_matrix)

        journey_distance = source_to_yard + yard_to_destination

        yard['journey_distance'] = journey_distance

    best_yard = min(_yard_locations, key=lambda x: x.get('journey_distance'))

    del best_yard['journey_distance']

    return { 'location_id': best_yard.get('depot_customer_id'), 'location': best_yard.get('start_loc') }


def get_chassis_event_for_nodes(previous_node, current_node, yard_locations, distance_unit, time_to_switch_chassis):
    # TIME_TO_SWITCH_CHASSIS is passed as parameter from assumptions.py
    def get_chassis_event(type, chassis_yard):
        distance = calculate_distance_between_locations(
            previous_node.get('end_loc'),
            chassis_yard.get('location'),
            distance_unit
        )

        event_obj = {
            'type': type,
            'customerId': chassis_yard.get('location_id'),
            'start_loc': chassis_yard.get('location'),
            'end_loc': chassis_yard.get('location'),
            'distance': distance
        }

        if type == 'CHASSISTERMINATION':
            time_to_cover_distance = minute_from_distance(event_obj['distance'], distance_unit)
            event_obj['recommended_enroute'] = previous_node.get('event_times', [])[-1].get('minutes').get('recommended_departed')
            event_obj['recommended_arrived'] = event_obj['recommended_enroute'] + time_to_cover_distance
            event_obj['recommended_departed'] = event_obj['recommended_arrived'] + time_to_switch_chassis

        return event_obj

    try:
        if current_node.get('chassis_activity') == CHASSIS_ACTIVITIES["HOOKCHASSIS"]:
            current_node['chassis_pick_event'] = get_chassis_event('CHASSISPICK', current_node.get('hook_yard'))
            # distance = calculate_distance_between_locations(
            #     current_node['chassis_pick_event']['end_loc'],
            #     current_node['start_loc'],
            #     distance_unit
            # )
            # if current_node.get('move'):
            #     current_node['move'][0]['distance'] = distance

        if current_node.get('chassis_activity') == CHASSIS_ACTIVITIES["DROPCHASSIS"]:
            previous_node['chassis_termination_event'] = get_chassis_event('CHASSISTERMINATION', current_node.get('drop_yard'))
            distance = calculate_distance_between_locations(
                previous_node['chassis_termination_event']['end_loc'],
                current_node['start_loc'],
                distance_unit
            )
            if current_node.get('move'):
                current_node['move'][0]['distance'] = distance

        if current_node.get('chassis_activity') == CHASSIS_ACTIVITIES["DROP_AND_HOOK_CHASSIS"]:
            previous_node['chassis_termination_event'] = get_chassis_event('CHASSISTERMINATION', current_node.get('drop_yard'))
            current_node['chassis_pick_event'] = get_chassis_event('CHASSISPICK', current_node.get('hook_yard'))
            current_node['chassis_pick_event']['distance'] = calculate_distance_between_locations(
                previous_node['chassis_termination_event']['end_loc'],
                current_node['chassis_pick_event']['start_loc'],
                distance_unit
            )
            # distance = calculate_distance_between_locations(
            #     current_node['chassis_pick_event']['end_loc'],
            #     current_node['start_loc'],
            #     distance_unit
            # )
            # if current_node.get('move'):
            #     current_node['move'][0]['distance'] = distance

        return current_node, previous_node
    
    except Exception as e:
        print(e)
        return current_node, previous_node
    

def get_warehouse_visits(moves: List[dict], yards: List[dict]) -> List[dict]:
    
    delivery_events = [e for e in moves if e.get('type') == 'DELIVERLOAD' and e.get('customerId')]
    hook_events = [e for e in moves if e.get('type') == 'HOOKCONTAINER' and e.get('customerId') not in yards]

    return delivery_events + hook_events