import logging
import pytz
import json
from copy import deepcopy
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

from app.modules.optimizer.constants import EVENT_TIME_MAP, CARRIER_CONFIGS, YARD_ALLOWED_OPERATIONS
from app.utils.distance_calc import calculate_distance_between_locations
from app.services.redis_service import get_shift_times
from app.postgres_services.drayage_intelligence_service import get_drayage_intelligence
from app.postgres_connection import get_postgres_client
from app.synced_db_connection import get_synced_db_client

logger = logging.getLogger(__name__)

def appointment_times():
    try:
        with open('app/data/appointment_times.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading appointment times: {str(e)}")
        return []
    
def get_standard_ports() -> str:
    try:
        with open('app/data/standard_ports.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error getting terminal name: {str(e)}")
        return None

STANDARD_PORTS = get_standard_ports()
APPOINTMENT_TIMES = appointment_times()
port_codes = set([port['portCode'] for port in APPOINTMENT_TIMES])

TERMINAL_NAMES = [alias for port in STANDARD_PORTS if any(alias in port_codes for alias in port['aliases']) for alias in port['aliases']]

DEFAULT_OFFICE_START = datetime.strptime('00:00', '%H:%M').time()
DEFAULT_OFFICE_END = datetime.strptime('23:59', '%H:%M').time()

def get_datetime_from_isodate(iso_date: str, time_zone: str) -> datetime:
    try:
        return (
            datetime.fromisoformat(iso_date)
                .replace(tzinfo=pytz.UTC)
                .astimezone(pytz.timezone(time_zone))
        )
    except Exception as e:
        logger.error(f"Error getting datetime from ISO date: {str(e)}")
        raise

def check_time_window(from_time, to_time, time):
    
    day = date.today()

    from_time = datetime.combine(day, from_time)
    to_time = datetime.combine(day, to_time)
    time = datetime.combine(day, time)

    if to_time <= from_time:
        to_time = to_time + timedelta(days=1)

    return from_time <= time <= to_time


def check_move_validity(
    user_payload: Dict[str, Any],
    move: List[Dict[str, Any]], 
    load: Dict[str, Any],
    converted_plan_date: datetime,
    timeZone: str,
    time_prediction: bool = False,
    drayage_intelligence: Dict[str, Any] = {},
    plan_range: Dict[str, Any] = {}
) -> bool:
    """
    Validates if the move events' appointment times match the plan date and if the appointment times are within the correct time window.
    """
    try:
        has_scheduled_appointment = False
        has_pickup_event = False

        carrier_id = user_payload.get('carrier')
        distance_unit = user_payload.get('distanceUnit', 'mi')

        if load.get('is_completed_move'):
            return True, ''

        for event_index, event in enumerate(move):
            event_type = event.get('type')
            if not event_type:
                continue

            # ignore TBA/TBD locations
            if (drayage_intelligence and event.get('customerId') in drayage_intelligence.get('exclude_locations_for_scheduler', [])) and not time_prediction:
                parsed_lfd = datetime.fromisoformat(load.get('lastFreeDay')).replace(tzinfo=pytz.UTC).astimezone(pytz.timezone(timeZone)) if load.get('lastFreeDay') else None
                is_hitting_lfd = parsed_lfd.date() <= converted_plan_date.date() if parsed_lfd and has_pickup_event else False

                if has_scheduled_appointment or is_hitting_lfd:
                    return False, event_type + '_LOCATION_MISSING'

                return False, 'INVALID_MOVE_LOCATION_MISSING'

            if event_type == 'RETURNCONTAINER' and not event.get('customerId'):
                if CARRIER_CONFIGS.get(carrier_id, {}).get('wait_for_empty_appt', False):
                    is_hook_return_move = move[event_index - 1].get('type') == 'HOOKCONTAINER' and event.get('type') == 'RETURNCONTAINER' if event_index > 0 else False
                    if is_hook_return_move:
                        should_wait = is_waiting_for_empty_appt(
                            carrier_id, 
                            move[event_index - 1], 
                            event, 
                            distance_unit
                        )
                        if should_wait:
                            return False, 'WAITING_FOR_EMPTY_APPT'

                return False, event_type + '_LOCATION_MISSING'

            appt_time = None
            if event_type in EVENT_TIME_MAP:
                if load.get(EVENT_TIME_MAP[event_type]):
                    appt_time = load.get(EVENT_TIME_MAP[event_type])
                    has_scheduled_appointment = True

                if event_type == 'PULLCONTAINER':
                    has_pickup_event = True

            if appt_time:
                appt_datetime = get_datetime_from_isodate(event.get('appointment_from'), timeZone)
                appt_datetime_to = get_datetime_from_isodate(event.get('appointment_to'), timeZone)

                if plan_range:
                    allowed_from_time = plan_range.get('from_time')
                    allowed_to_time = plan_range.get('to_time')

                    if (allowed_from_time and allowed_to_time and 
                            (allowed_to_time < appt_datetime or allowed_from_time > appt_datetime_to)):
                        return False, event_type + '_APPT_DATE_MISMATCH'
                else:
                    if appt_datetime.date() != converted_plan_date.date():
                        return False, event_type + '_APPT_DATE_MISMATCH'

            # check appointments
            if event_type in ['PULLCONTAINER', 'DELIVERLOAD', 'RETURNCONTAINER'] and not time_prediction:
                actual_appt = datetime.fromisoformat(appt_time).astimezone(pytz.timezone(timeZone)) if appt_time else None
                day_of_week = converted_plan_date.strftime('%A')
                terminal_name = event.get('company_name', '')

                # Handle appointments
                if terminal_name in port_codes:
                    port_code = terminal_name
                    port_data = next((port for port in APPOINTMENT_TIMES if port['portCode'] == port_code and day_of_week in port['day_of_week']), {})

                    # Check mandatory appointment window
                    if port_data.get("mandatory_appt") and not port_data.get("freeflow_appt"):
                        if not actual_appt:
                            if CARRIER_CONFIGS.get(carrier_id, {}).get('wait_for_empty_appt', False):
                                is_hook_return_move = move[event_index - 1].get('type') == 'HOOKCONTAINER' and event.get('type') == 'RETURNCONTAINER' if event_index > 0 else False
                                if is_hook_return_move:
                                    should_wait = is_waiting_for_empty_appt(
                                        carrier_id, 
                                        move[event_index - 1], 
                                        event, 
                                        distance_unit
                                    )
                                    if should_wait:
                                        return False, 'WAITING_FOR_EMPTY_APPT'

                            return False, event_type + '_APPT_CONSTRAINT_FAILED'

                        mandatory_start = datetime.strptime(port_data["mandatory_appt"]["start_time"], "%H:%M").time()
                        mandatory_end = datetime.strptime(port_data["mandatory_appt"]["end_time"], "%H:%M").time()

                        is_valid_time = check_time_window(mandatory_start, mandatory_end, actual_appt.time())

                        if is_valid_time:
                            continue

                    if port_data.get("freeflow_appt") and not actual_appt:
                        freeflow_start = datetime.strptime(port_data["freeflow_appt"]["start_time"], "%H:%M").time()
                        freeflow_end = datetime.strptime(port_data["freeflow_appt"]["end_time"], "%H:%M").time()

                        event['appointment_from'] = converted_plan_date.replace(hour=freeflow_start.hour, minute=freeflow_start.minute).isoformat()
                        appointment_to = converted_plan_date.replace(hour=freeflow_end.hour, minute=freeflow_end.minute)

                        if freeflow_end <= freeflow_start:
                            appointment_to = freeflow_end + timedelta(days=1)

                        event['appointment_to'] = appointment_to.isoformat()
                        continue
                    
                    if (port_data.get("mandatory_appt_no") and not load.get('callerPONo') and 
                            event_type == 'PULLCONTAINER' and load['type_of_load'] == 'IMPORT'):
                        return False, event_type + '_APPT_CONSTRAINT_FAILED'

            # Delivery appointment time is missing
            if event_type == 'DELIVERLOAD' and not appt_time and not time_prediction:
                parsed_lfd = datetime.fromisoformat(load.get('lastFreeDay')).replace(tzinfo=pytz.UTC).astimezone(pytz.timezone(timeZone)) if load.get('lastFreeDay') else None
                is_hitting_lfd = parsed_lfd.date() <= converted_plan_date.date() if parsed_lfd and has_pickup_event else False

                if has_scheduled_appointment or is_hitting_lfd:
                    return False, 'DELIVERLOAD_APPT_MISSING'

                return False, 'INVALID_MOVE_NO_PICKUP_NO_DELIVERY'
        
        return True, ''

    except (ValueError, pytz.exceptions.UnknownTimeZoneError) as e:
        logger.error(f"Error validating move: {str(e)}")
        raise


async def modify_move_for_invalid_move(
    user_payload: Dict[str, Any], 
    move: List[Dict[str, Any]], 
    type: str, 
    position: str = 'end',
    load: Dict[str, Any] = None
) -> List[Dict[str, Any]]:
    """
    Modified to fetch and use drop yard configuration from database.
    """
    try:
        carrier_id = user_payload.get('carrier')
        use_nearest_to_delivery_yard = CARRIER_CONFIGS.get(carrier_id, {}).get('use_nearest_to_delivery_yard', False)

        copied_move = deepcopy(move)
        default_yard_locations = user_payload.get('default_yard_locations')
        yard_locations = [
            yard for yard in default_yard_locations
            if yard.get('allowed_operations') and YARD_ALLOWED_OPERATIONS['CONTAINER_DROP_HOOK'] in yard.get('allowed_operations')
        ]
        
        distance_unit = user_payload.get('distanceUnit', 'mi')

        drayage_config = user_payload.get('drayage_config', {})

        ref_location_to_drop = None
        if use_nearest_to_delivery_yard:
            ref_location_to_drop = next((event for event in copied_move if event.get('type') == 'DELIVERLOAD'), None)

        # Find index of event with matching type
        event_index = next((i for i, event in enumerate(copied_move) if event.get('type') == type), -1)

        if position == 'end':
            copied_move = copied_move[:event_index] if event_index >= 0 else copied_move
        else:
            copied_move = copied_move[event_index:] if event_index >= 0 else copied_move

        # Ensure at least one event is PULLCONTAINER or DELIVERLOAD
        has_required_event = any(event.get('type') in ['PULLCONTAINER', 'DELIVERLOAD'] for event in copied_move)

        if not has_required_event and type != 'RETURNCONTAINER':
            return move

        if position == 'end':
            # Add DROPCONTAINER event
            last_event = copied_move[-1]

            if not ref_location_to_drop:
                ref_location_to_drop = last_event

            # Get appropriate yard based on configuration
            nearest_yard_location = await get_recommended_yard_location(
                move,
                drop_event_index=event_index,
                yard_locations=yard_locations, 
                location=ref_location_to_drop, 
                distance_unit=distance_unit,
                carrier_id=carrier_id,
                load=load,
                drayage_config=drayage_config  # Pass config to avoid another DB call
            )
            
            if len(copied_move) == 1 and last_event.get('customerId') == nearest_yard_location.get('customerId'):
                return move

            copied_move.append({
                'type': 'DROPCONTAINER',
                'distance': calculate_distance_between_locations(
                    last_event['address'], 
                    nearest_yard_location['address'], 
                    distance_unit
                ),
                'moveId': last_event['moveId'],
                **nearest_yard_location
            })
        else:
            # Add HOOKCONTAINER event
            first_event = copied_move[0]

            if not ref_location_to_drop:
                ref_location_to_drop = first_event

            nearest_yard_location = get_nearest_yard_location(
                yard_locations, 
                ref_location_to_drop, 
                distance_unit,
                carrier_id=carrier_id,
                load=load,
                drayage_config=drayage_config
            )
            
            if first_event.get('customerId') == nearest_yard_location.get('customerId'):
                return move

            copied_move.insert(0, {
                'type': 'HOOKCONTAINER',
                'distance': 0,
                'moveId': first_event['moveId'],
                **nearest_yard_location
            })
            copied_move[1]['distance'] = calculate_distance_between_locations(
                nearest_yard_location['address'], 
                first_event['address'], 
                distance_unit
            )

        return copied_move
    except Exception as e:
        logger.error(f"Error modifying move for invalid move: {str(e)}")
        raise


def populate_appointment_times_to_events(
    user_payload: Dict[str, Any],
    loads: Dict[str, Any],
    actionable_move: List[Dict[str, Any]],
    converted_plan_date: datetime,
    time_prediction: bool = False,
    location_office_hours: List[Dict[str, Any]] = []
) -> Dict[str, Any]:
    """
    Extract recommended appointment times from a scheduled plan for different container operations.
    """
    try:
        timeZone = user_payload.get('timeZone')
        carrier = user_payload.get('carrier')
        tz = pytz.timezone(timeZone)
        base_date = converted_plan_date

        time_map = {
            'PULLCONTAINER': ('pickupFromTime', 'pickupToTime'),
            'DELIVERLOAD': ('deliveryFromTime', 'deliveryToTime'), 
            'RETURNCONTAINER': ('returnFromTime', 'returnToTime')
        }

        # add appointment times to move
        for event in actionable_move:
            event_type = event.get('type')
            load_copy = loads[0] if len(loads) > 0 else {}
            type_of_load = load_copy.get('type_of_load', '')

            if len(loads) > 0 and event.get('reference_number'):
                found_load = next(
                    (load for load in loads 
                        if load.get('reference_number') == event.get('reference_number')),
                    None
                )
                if found_load:
                    load_copy = found_load
            
            if event_type in time_map:
                from_key, to_key = time_map[event_type]
                if load_copy.get(from_key):
                    event.update({
                        'type_of_load': type_of_load,
                        'appointment_from': load_copy.get(from_key),
                        'appointment_to': load_copy.get(to_key),
                        'is_scheduled': True
                    })
        
        for index, event in enumerate(actionable_move):
            # Handle midnight start time
            if (event.get('type') == 'DELIVERLOAD' and 
                event.get('appointment_to') and 
                type_of_load == 'IMPORT'):

                import_deliverload_grace_time = CARRIER_CONFIGS.get(carrier, {}).get('IMPORT_DELIVERLOAD_GRACE_TIME', 0)

                if import_deliverload_grace_time > 0:
                    appointment_to_dt = datetime.fromisoformat(event['appointment_to'])
                    appointment_to_dt += timedelta(minutes=import_deliverload_grace_time)
                    event['appointment_to'] = appointment_to_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            appt_from = event.get('appointment_from', None) and datetime.fromisoformat(event['appointment_from']).astimezone(tz)
            appt_to = event.get('appointment_to', None) and datetime.fromisoformat(event['appointment_to']).astimezone(tz)
            is_midnight_start = appt_from and appt_from.hour == 0
            is_midnight_end = appt_to and appt_to.hour <= 1
            office_hours = next((office_hour for office_hour in location_office_hours if office_hour['_id'] == event.get('customerId', '')), None)
            
            if ((is_midnight_start and is_midnight_end) or not event.get('is_scheduled')):
                office_start_time = DEFAULT_OFFICE_START if office_hours is None else office_hours.get('office_hours_start', DEFAULT_OFFICE_START)
                office_end_time = DEFAULT_OFFICE_END if office_hours is None else office_hours.get('office_hours_end', DEFAULT_OFFICE_END)

                effective_base_date = base_date
                if appt_to and appt_to > base_date:
                    effective_base_date = appt_to

                appointment_from = effective_base_date.replace(hour=office_start_time.hour, minute=office_start_time.minute)
                appointment_to =  effective_base_date.replace(hour=office_end_time.hour, minute=office_end_time.minute)

                if appointment_to <= appointment_from:
                    appointment_to = appointment_to + timedelta(days=1)

                event.update({
                    'appointment_from': appointment_from.isoformat(),
                    'appointment_to': appointment_to.isoformat(),
                    'is_scheduled': False,
                    'is_recommended': True
                })
            
            # If appointment window is less than 1 hour, set appointment from to 30 minutes back
            if event.get('appointment_from') and event.get('appointment_to'):
                appt_from = datetime.fromisoformat(event['appointment_from'])
                appt_to = datetime.fromisoformat(event['appointment_to'])

                if (appt_to - appt_from).total_seconds() < 3599:
                    event['appointment_from'] = (appt_from - timedelta(minutes=15)).isoformat()
                    event['appointment_to'] = (appt_to + timedelta(minutes=0)).isoformat()

                grace_period_before_appt = office_hours.get('grace_period_before_appt', 0) if office_hours else 0
                grace_period_after_appt = office_hours.get('grace_period_after_appt', 0) if office_hours else 0

                if grace_period_before_appt > 0 and appt_from.hour > 0:
                    event['appointment_from'] = (appt_from - timedelta(minutes=grace_period_before_appt)).isoformat()

                if grace_period_after_appt > 0 and appt_to.hour < 23:
                    event['appointment_to'] = (appt_to + timedelta(minutes=grace_period_after_appt)).isoformat()
            
            if event.get('tripAssignedDate'):
                event['loadAssignedDate'] = event['tripAssignedDate']

            if event.get('type') == 'DELIVERLOAD' and (
                index + 1 < len(actionable_move)) and (
                actionable_move[index + 1].get('type') != 'DROPCONTAINER' or 
                event.get('customerId') != actionable_move[index + 1].get('customerId')
            ):
                event['is_liveunload'] = True
            
            base_date = event.get('appointment_to', None) and datetime.fromisoformat(event['appointment_to']).astimezone(tz)

        return actionable_move
        
    except Exception as e:
        logger.error(f"Error extracting recommended appointment times: {str(e)}")
        raise Exception(f"Failed to extract recommended appointment times: {str(e)}")

def get_nearest_yard_location(
    yard_locations: List[Dict[str, Any]], 
    location: Dict[str, Any], 
    distance_unit: str = 'mi',
    carrier_id: str = None,
    load: Dict[str, Any] = None,
    drayage_config: Dict[str, Any] = None  # Pass config to avoid multiple DB calls
) -> Dict[str, Any]:
    """
    Get the appropriate yard for DROPCONTAINER events.
    Uses database-driven configuration for yard selection logic.
    """
    try:
        # If only one yard available, return it
        if len(yard_locations) == 1:
            return yard_locations[0]
        
        # Use passed config or fetch from database
        if not drayage_config:
            logger.warning("Drayage config not passed, fetching from database")
            pass
        
        # Check for new drop yard configuration
        if drayage_config and drayage_config.get('empty_drop_yard_config'):
            yard_config = drayage_config.get('empty_drop_yard_config')
            logic_type = yard_config.get('logic_type')
            
            if logic_type == 'EAST_WEST_SPLIT':
                return get_drop_yard_east_west_split(
                    yard_locations, 
                    location, 
                    yard_config, 
                    load
                )
            elif logic_type == 'NEAREST_TO_DELIVERY':
                # Quality Container logic - find nearest yard
                return min(yard_locations, key=lambda x: calculate_distance_between_locations(
                    location['address'], 
                    x['address'], 
                    distance_unit
                ))
            elif logic_type == 'SINGLE_YARD':
                # Use configured single yard
                single_yard_id = yard_config.get('yards', {}).get('default', {}).get('yard_id')
                if single_yard_id:
                    matching_yard = next(
                        (y for y in yard_locations if y.get('customerId') == single_yard_id),
                        None
                    )
                    if matching_yard:
                        return matching_yard
        
        # Fallback to default behavior based on carrier
        if carrier_id == '641a10875b159a160742327e':  # RoadEx
            # Hardcoded fallback for RoadEx if config is missing
            return get_roadex_yard_fallback(yard_locations, location, load)
        
        # Default: nearest yard
        return min(yard_locations, key=lambda x: calculate_distance_between_locations(
            location['address'], 
            x['address'], 
            distance_unit
        ))
    
    except Exception as e:
        logger.error(f"Error in get_nearest_yard_location: {str(e)}")
        # Safe fallback to first available yard
        return yard_locations[0] if yard_locations else None

async def get_recommended_yard_location(
    move: List[Dict[str, Any]],
    drop_event_index: int,
    yard_locations: List[Dict[str, Any]],
    location: Dict[str, Any],
    distance_unit: str = 'mi',
    carrier_id: str = None,
    load: Dict[str, Any] = None,
    drayage_config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Get the recommended yard location for a given location.
    First tries to get yard recommendations from database based on move events,
    then falls back to nearest yard if no recommendations found.
    """
    try:
        # Try to get yard recommendations from database
        from_location, to_location, recommendation_type = analyze_move_pattern(move, drop_event_index)
        
        recommended_yard_customer_id = await get_yard_recommendation_from_db(from_location, to_location, recommendation_type, carrier_id, yard_locations)
        
        synced_db = get_synced_db_client()
        pool = await synced_db.get_pool()
        
        sql_query = "SELECT * FROM public.customers where carrier = $1 and _id=$2"
        
        recommended_yard = None
        async with pool.acquire() as conn:
            record = await conn.fetchrow(
                sql_query, 
                carrier_id, 
                recommended_yard_customer_id
            )
            
            if record:
                recommended_yard = {
                    "customerId": record.get("_id", ""),
                    "company_name": record.get("company_name", ""),
                    "address": {
                        "lat": record.get("lat", ""),
                        "lng": record.get("lng", ""),
                        "address": record.get("address", ""),
                        "city": record.get("city", ""),
                        "state": record.get("state", ""),
                        "country": record.get("country", ""),
                        "zip_code": record.get("zip_code", ""),
                    },
                    "city": record.get("city", ""),
                    "state": record.get("state", ""),
                    "country": record.get("country", ""),
                    "zip_code": record.get("zip_code", ""),
                    "is_chassis_pick_allowed": record.get("is_chassis_pick_allowed", True) if record else True,
                    "is_chassis_termination_allowed": record.get("is_chassis_termination_allowed", True) if record else True,
                    "allowed_operations": YARD_ALLOWED_OPERATIONS
                }
            
        if not recommended_yard:
            # Fallback to nearest yard if no recommendations found
            recommended_yard = get_nearest_yard_location(yard_locations, location, distance_unit, carrier_id, load, drayage_config)        
        return recommended_yard
        
    except Exception as e:
        logger.error(f"Error in get_recommended_yard_location: {str(e)}")
        # Safe fallback to nearest yard
        return get_nearest_yard_location(yard_locations, location, distance_unit, carrier_id, load, drayage_config)
    
def analyze_move_pattern(move: List[Dict[str, Any]], drop_event_index: int) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Analyze move events to determine recommendation pattern.
    Returns (recommendation_type, from_customer_id, to_customer_id)
    """
    try:
        from_event = move[drop_event_index-1]
        to_event = move[drop_event_index]
        
        from_location = from_event.get('customerId')
        to_location = to_event.get('customerId')
        
        # INSERT_YOUR_CODE
        recommendation_type = None

        if from_event.get('type') == 'PULLCONTAINER' and to_event.get('type') == 'DELIVERLOAD':
            recommendation_type = 'PULL_DELIVER'
        elif from_event.get('type') == 'DELIVERLOAD' and to_event.get('type') == 'RETURNCONTAINER':
            recommendation_type = 'DELIVER_RETURN'
        elif from_event.get('type') == 'HOOKCONTAINER' and to_event.get('type') == 'RETURNCONTAINER':
            recommendation_type = 'HOOK_RETURN'
        
        return from_location, to_location, recommendation_type
    except Exception as e:
        logger.error(f"Error in analyze_move_pattern: {str(e)}")
        return None, None, None

async def get_yard_recommendation_from_db(
    from_location: str,
    to_location: str,
    recommendation_type: str,
    carrier_id: str, 
    yard_locations: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """
    Get yard recommendation from database based on move events.
    Analyzes the move pattern and looks up recommendations in yard_recommendations table.
    """
    try:
        if not from_location or not to_location or not carrier_id or not recommendation_type:
            return None
        
        # Query yard recommendations from database
        postgres = get_postgres_client()
        pool = await postgres.get_pool()
        
        query = """
        SELECT 
            recommended_event_type,
            recommended_customer_id,
            event_counts
        FROM public.yard_recommendations 
        WHERE carrier = $1 
        AND recommendation_type = $2 
        AND from_customer_id = $3 
        AND to_customer_id = $4
        ORDER BY created_at DESC
        LIMIT 1
        """
        
        async with pool.acquire() as conn:
            record = await conn.fetchrow(
                query, 
                carrier_id, 
                recommendation_type, 
                from_location, 
                to_location
            )
            
            if not record:
                logger.debug(f"No yard recommendation found for pattern: {recommendation_type} {from_location} -> {to_location}")
                return None
            
            # Find the recommended yard in available yard locations
            recommended_customer_id = record['recommended_customer_id']
            
            return recommended_customer_id
                
    except Exception as e:
        logger.error(f"Error getting yard recommendation from database: {str(e)}")
        return None

def get_drop_yard_east_west_split(
    yard_locations: List[Dict[str, Any]],
    delivery_location: Dict[str, Any],
    yard_config: Dict[str, Any],
    load: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Implements east/west split logic for DROPCONTAINER yard selection.
    Used by RoadEx for their specific business rules.
    """
    try:
        dividing_longitude = yard_config.get('dividing_longitude', -117.624773)
        yards_config = yard_config.get('yards', {})
        
        # Get configured yard IDs
        east_yard_id = yards_config.get('east', {}).get('yard_id')
        west_default_yard_id = yards_config.get('west_default', {}).get('yard_id')
        west_amazon_config = yards_config.get('west_amazon', {})
        west_amazon_yard_id = west_amazon_config.get('yard_id')
        
        # Find yards in available locations
        east_yard = next(
            (y for y in yard_locations if y.get('customerId') == east_yard_id),
            None
        )
        west_default_yard = next(
            (y for y in yard_locations if y.get('customerId') == west_default_yard_id),
            None
        )
        west_amazon_yard = next(
            (y for y in yard_locations if y.get('customerId') == west_amazon_yard_id),
            None
        )
        
        # Get delivery longitude
        delivery_lng = delivery_location.get('address', {}).get('lng')
        
        if not delivery_lng:
            logger.warning("No longitude for delivery location, using west default yard")
            return west_default_yard if west_default_yard else yard_locations[0]
        
        # Apply east/west logic
        if delivery_lng > dividing_longitude:
            # East of dividing line - use Ontario yard
            logger.debug(f"Delivery lng {delivery_lng} > {dividing_longitude}, using east yard")
            return east_yard if east_yard else yard_locations[0]
        else:
            # West of dividing line - check for special conditions
            if west_amazon_config and load:
                condition = west_amazon_config.get('condition', {})
                if check_drop_yard_condition(load, condition):
                    logger.debug(f"Amazon load detected, using Amazon yard")
                    return west_amazon_yard if west_amazon_yard else west_default_yard if west_default_yard else yard_locations[0]
            
            logger.debug(f"Using west default yard")
            return west_default_yard if west_default_yard else yard_locations[0]
    
    except Exception as e:
        logger.error(f"Error in get_drop_yard_east_west_split: {str(e)}")
        return yard_locations[0] if yard_locations else None


def check_drop_yard_condition(load: Dict[str, Any], condition: Dict[str, Any]) -> bool:
    """
    Check if a load meets the condition for specific drop yard selection.
    """
    if not condition:
        return False
    
    field = condition.get('field')
    operator = condition.get('operator')
    value = condition.get('value')
    
    if not all([field, operator, value]):
        return False
    
    load_value = str(load.get(field, ''))
    
    if operator == 'starts_with':
        return load_value.upper().startswith(str(value).upper())
    elif operator == 'equals':
        return load_value.upper() == str(value).upper()
    elif operator == 'contains':
        return str(value).upper() in load_value.upper()
    elif operator == 'ends_with':
        return load_value.upper().endswith(str(value).upper())
    
    return False


def get_roadex_yard_fallback(
    yard_locations: List[Dict[str, Any]],
    delivery_location: Dict[str, Any],
    load: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Fallback logic for RoadEx if database configuration is missing.
    This ensures the system continues to work even without DB config.
    """
    # Hardcoded yard IDs as fallback
    ONTARIO_YARD_ID = '67460e1a105855c40130a2dc'
    Q_STREET_YARD_ID = '641a138d4afd3216030be8ec'
    AMAZON_YARD_ID = '66d8e4486f8e3a829bd63aa5'
    ONTARIO_LNG = -117.624773
    
    ontario_yard = next((y for y in yard_locations if y.get('customerId') == ONTARIO_YARD_ID), None)
    q_street_yard = next((y for y in yard_locations if y.get('customerId') == Q_STREET_YARD_ID), None)
    amazon_yard = next((y for y in yard_locations if y.get('customerId') == AMAZON_YARD_ID), None)
    
    delivery_lng = delivery_location.get('address', {}).get('lng')
    
    if not delivery_lng:
        return q_street_yard if q_street_yard else yard_locations[0]
    
    if delivery_lng > ONTARIO_LNG:
        return ontario_yard if ontario_yard else yard_locations[0]
    
    # Check if Amazon load
    if load and str(load.get('consigneeName', '')).upper().startswith('AMZN'):
        return amazon_yard if amazon_yard else q_street_yard if q_street_yard else yard_locations[0]
    
    return q_street_yard if q_street_yard else yard_locations[0]


def filter_drivers_by_shift(drivers: List[Dict[str, Any]], shift: str) -> List[Dict[str, Any]]:
    try:
        if shift == 'day':
            return [driver for driver in drivers if driver.get('is_day_driver', True)]
        elif shift == 'night':
            return [driver for driver in drivers if not driver.get('is_day_driver', True)]
        return drivers
    except Exception as e:
        logger.error(f"Error filtering drivers by shift: {str(e)}")
        raise


def get_shift_start_end_minutes(shift_times: List[Dict[str, Any]], shift: str, branch: str = None) -> Dict[str, Any]:
    try:
        selected_shift_times = next((s for s in shift_times if s.get('name') == shift and (not branch or str(s.get('terminal', '')) == branch)), {})

        if selected_shift_times:
            shift_start_minute = selected_shift_times.get('start_minute', 0)
            shift_end_minute = selected_shift_times.get('end_minute', 1440)
            return int(shift_start_minute), int(shift_end_minute)

        return None, None
    except Exception as e:
        logger.error(f"Error getting shift start and end minutes: {str(e)}")
        raise


async def get_shift_time(
    carrier: str,
    plan_date: datetime,
    shift: str,
    plan_branch: list = [],
    time_zone: str = None
) -> Dict[str, Any]:
    try:
        shift_start_minute = CARRIER_CONFIGS.get(carrier, {}).get('plan_times', [0, 1440])[0]
        shift_end_minute = CARRIER_CONFIGS.get(carrier, {}).get('plan_times', [0, 1440])[1]

        if shift:
            shift_times = await get_shift_times(carrier)
            _shift_start_minute, _shift_end_minute = get_shift_start_end_minutes(
                shift_times,
                shift,
                plan_branch[0] if plan_branch else None
            )
            if _shift_start_minute:
                shift_start_minute = _shift_start_minute
            if _shift_end_minute:
                shift_end_minute = _shift_end_minute


        current_time = datetime.now(pytz.timezone(time_zone))
        is_planning_for_today = plan_date.date() == current_time.date()
        current_minute_of_day = current_time.hour * 60 + current_time.minute

        if is_planning_for_today:
            # if the shift is over, then raise an error
            if current_minute_of_day > shift_end_minute:
                raise Exception("Cannot plan for today after the shift is over.")


        return [shift_start_minute, shift_end_minute]
    except Exception as e:
        logger.error(e)
        raise Exception(f"{str(e)}")
    

def is_waiting_for_empty_appt(
        carrier_id: str,
        hook_event: Dict[str, Any],
        return_event: Dict[str, Any],
        distance_unit: str = 'mi'
    ) -> bool:
    try:

        # If empty is more than 10 miles away then don't wait at that warehouse
        distance_to_return = 0

        if return_event.get('customerId'):
            # Use route distance if location is there
            distance_to_return = return_event.get('distance', 0)
        else:
            # If empty location is not there, then use empty group location if defined
            empty_group_location = CARRIER_CONFIGS.get(carrier_id, {}).get('empty_group_location', None)
            if empty_group_location:
                distance_to_return = calculate_distance_between_locations(hook_event.get('address'), empty_group_location, distance_unit)
            else:
                return False

        if distance_to_return > 10:
            return False
        
        ## If the warehouse is not opened for 24 hours then don't wait at that warehouse 
        
        # If appoitment is not there then skip waiting
        if not hook_event.get('appointment_from') or not hook_event.get('appointment_to'):
            return False
        

        office_from = datetime.fromisoformat(hook_event.get('appointment_from'))
        office_to = datetime.fromisoformat(hook_event.get('appointment_to'))
        total_office_hours = (office_to - office_from).total_seconds() / 3600
        
        # skip waiting for appt, if location is not available for 24 hours
        if total_office_hours < 23:
            return False
        
        return True
    except Exception as e:
        logger.error(f"Error checking if waiting for empty appt at warehouse: {str(e)}")
        raise