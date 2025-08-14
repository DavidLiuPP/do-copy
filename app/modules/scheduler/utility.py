import logging
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

from app.firebase_connection import get_firebase_client
from app.modules.scheduler.constants import REQUIRED_LOAD_FIELDS, OPTIONAL_LOAD_FIELDS, APPOINTMENT_STATUS
from app.utils.distance_calc import calculate_distance_between_locations, calculate_duration_from_distance

# logger
logger = logging.getLogger(__name__)

def _extract_pickup_times(load: Dict[str, Any]) -> Dict[str, Optional[datetime]]:
    """
    Extract and format pickup times from load data.
    
    Args:
        load: Load dictionary containing pickup time information
        
    Returns:
        Dictionary with formatted pickup from/to times
    """
    times = {
        'pickupFromTime': None,
        'pickupToTime': None
    }
    
    pickup_times = load.get('pickupTimes', [])
    if pickup_times and len(pickup_times) > 0:
        pickup_time = pickup_times[0]
        times.update({
            'pickupFromTime': pickup_time.get('pickupFromTime'),
            'pickupToTime': pickup_time.get('pickupToTime')
        })
    
    return times


def _extract_delivery_times(load: Dict[str, Any]) -> Dict[str, Optional[datetime]]:
    """
    Extract and format delivery times from load data.
    
    Args:
        load: Load dictionary containing delivery time information
        
    Returns:
        Dictionary with formatted delivery from/to times
    """
    times = {
        'deliveryFromTime': None,
        'deliveryToTime': None
    }
    
    delivery_times = load.get('deliveryTimes', [])
    if delivery_times and len(delivery_times) > 0:
        delivery_time = delivery_times[0]
        times.update({
            'deliveryFromTime': delivery_time.get('deliveryFromTime'),
            'deliveryToTime': delivery_time.get('deliveryToTime')
        })
    
    return times


def _set_default_fields(load: Dict[str, Any]) -> None:
    """
    Set default None values for optional fields if not present.
    
    Args:
        load: Load dictionary to set defaults on
    """
    for field in OPTIONAL_LOAD_FIELDS:
        if field not in load:
            load[field] = None

def map_loads_for_scheduler(loads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform load data structure to match scheduler requirements.
    
    Performs immutable transformations on load data:
    - Flattens nested time fields
    - Standardizes field names
    - Sets default values for optional fields
    - Validates required fields
    
    Args:
        loads: List of load dictionaries containing pickup and delivery times
        
    Returns:
        List of transformed load dictionaries with flattened time fields
        
    Raises:
        ValueError: If load data is malformed or missing required fields
    """
    if not loads:
        return []

    try:
        transformed_loads = []

        for load in loads:
            # Validate required fields
            missing_fields = REQUIRED_LOAD_FIELDS - set(load.keys())
            if missing_fields:
                raise ValueError(f"Load missing required fields: {missing_fields}")

            # Create deep copy to avoid modifying original
            load_copy = deepcopy(load)
            
            # Remove carrier field
            load_copy.pop('carrier', None)
                
            # Extract and update times
            pickup_times = _extract_pickup_times(load_copy)
            delivery_times = _extract_delivery_times(load_copy)
            load_copy.update(pickup_times)
            load_copy.update(delivery_times)
            
            # Remove original time fields
            load_copy.pop('pickupTimes', None)
            load_copy.pop('deliveryTimes', None)

            # Standardize distance field
            load_copy['distance'] = load_copy.pop('totalMiles', 0)
            load_copy['revenue'] = load_copy.pop('revenue', 0)
            
            # Fields to pop with default None value
            fields_to_pop = [
                'availableDate', 'shipperName', 'consigneeName', 'emptyOriginName',
                'hazmat', 'hot', 'liquor', 'lastFreeDay', 'containerAvailableDay', 'emptyDay',
                'returnFromTime', 'returnToTime', 'dischargedDate', 'cutOff', 'freeReturnDate',
                'containerSizeName', 'totalWeight', 'outgateDate', 'chassisPickName',
                'consigneeAddress', 'containerTypeName', 'containerOwnerName',
                'caller', 'freight', 'isLive', 'isReUse', 'isReadyForPickup',
                'isHot', 'overWeight', 'allowDriverCompletion', 'isLastFreeDay', 'appointmentNo'
            ]

            for field in fields_to_pop:
                load_copy[field] = load_copy.pop(field, None)

            vessel = load_copy.pop('vessel', None)
            load_copy['vessel'] = vessel.get('eta') if vessel else None

            # Set defaults for optional fields
            _set_default_fields(load_copy)

            if load_copy.get('type_of_load') == 'IMPORT':
                # if discharge date is not present, derive it from vessel eta
                if not load_copy.get('dischargedDate') and load_copy.get('vessel'):
                    load_copy['dischargedDate'] = load_copy['vessel']

                # if lastFreeDay is not present, derive it from vessel eta
                if not load_copy.get('lastFreeDay') and load_copy.get('vessel') and not load_copy.get('pickupFromTime') and not load_copy.get('deliveryFromTime'):
                    load_copy['lastFreeDay'] = (datetime.fromisoformat(load_copy['vessel']) + timedelta(days=7)).isoformat()

            transformed_loads.append(load_copy)
            
        return transformed_loads
        
    except Exception as e:
        logger.error(f"Error mapping loads: {str(e)}")
        raise


def calculate_count_for_load(schedule_plan: Dict[str, str]) -> Dict[str, Any]:
    """
    Calculate the time slots for a given load based on the provided schedule plan.

    Parameters:
    - schedule_plan (Dict[str, str]): A dictionary containing scheduling information.
      Expected keys:
        - "recommended_appointment_from": Optional recommended appointment datetime (ISO format).
        - "scheduled_appointment_from": Optional scheduled appointment datetime (ISO format).

    Returns:
    - Dict[str, Any]: A dictionary containing:
        - "scheduled_time_slot": Time slot for the scheduled appointment (if available).
        - "recommended_time_slot": Time slot for the recommended appointment (fallback).

    If no valid appointment is found, an empty dictionary is returned.
    """
    try:
        # Extract relevant date values
        recommended_appointment_from = schedule_plan.get("recommended_appointment_from")
        scheduled_appointment_from = schedule_plan.get("scheduled_appointment_from")

        # Determine the appointment date based on availability
        appointment_date = scheduled_appointment_from or recommended_appointment_from
        
        # If no appointment date is available, return empty result
        if not appointment_date:
            return {}
        # Helper function to generate time slot range
        def create_time_slot(start_time):
            if isinstance(start_time, str):
                start_datetime = datetime.fromisoformat(start_time)
            elif isinstance(start_time, datetime):
                start_datetime = start_time
            else:
                return None  # Handle invalid input gracefully
            
            return start_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")


        # Create time slots
        scheduled_time_slot = create_time_slot(scheduled_appointment_from)
        recommended_time_slot = create_time_slot(appointment_date)

        return {
            "scheduled_time_slot": scheduled_time_slot,
            "recommended_time_slot": recommended_time_slot
        }

    except Exception as e:
        logger.error(f"Error in calculate_count_for_load: {str(e)}")
        return {}

def update_count_for_load(load_schedule_count: Dict[str, Any], scheduled_time_slot: Dict[str, Any], recommended_time_slot: Dict[str, Any], schedule_plan: Dict[str, Any], user_time_zone:str) -> Dict[str, Any]:
    """
    Update load schedule counts based on appointment status and time slots.

    Parameters:
    - load_schedule_count (Dict[str, Any]): A dictionary to store the updated counts.
    - scheduled_time_slot (Dict[str, Any]): Time slot for the scheduled appointment (if available).
    - recommended_time_slot (Dict[str, Any]): Time slot for the recommended appointment (if available).
    - schedule_plan (Dict[str, Any]): A dictionary containing scheduling information. Expected keys:
        - "profile_type": Type of the appointment profile (e.g., "PICKUP", "DELIVERY").
        - "appointment_status": Current status of the appointment.
    - user_time_zone (str): The timezone used to check if the appointment is on the same day.

    Returns:
    - Dict[str, Any]: Updated `load_schedule_count` dictionary with the following possible keys:
        - "TOTAL_{profile_type}_SCHEDULED_APT_COUNT": Updated with the `scheduled_time_slot` if present.
        - "TOTAL_{profile_type}_NEED_APT_COUNT": Updated with the `recommended_time_slot` if the status is "NEED_APPOINTMENT".
        - "TOTAL_{profile_type}_REQUESTED_COUNT": Updated with the `recommended_time_slot` if the status is "REQUESTED".

    If an error occurs during processing, an empty dictionary is returned.
    """

    try: 
        _is_appointment_date_in_same_day = is_appointment_date_in_same_day(schedule_plan, user_time_zone)
        profile_type = schedule_plan.get('profile_type')
        if scheduled_time_slot and _is_appointment_date_in_same_day:
            load_schedule_count[f'TOTAL_{profile_type}_SCHEDULED_APT_COUNT'] = scheduled_time_slot
            
        if schedule_plan.get("appointment_status") == APPOINTMENT_STATUS['NEED_APPOINTMENT'] and _is_appointment_date_in_same_day:
            load_schedule_count[f'TOTAL_{profile_type}_NEED_APT_COUNT'] = recommended_time_slot
            
        if schedule_plan.get("appointment_status") == APPOINTMENT_STATUS['REQUESTED'] and _is_appointment_date_in_same_day:
            load_schedule_count[f'TOTAL_{profile_type}_REQUESTED_COUNT'] = recommended_time_slot
        return load_schedule_count
    except Exception as e:
        logger.error(f"Error in update_count_for_load: {str(e)}")
        return {}

def is_appointment_date_in_same_day(schedule_plan: Dict[str, Any], user_time_zone: str):
    """
    Check if the appointment date and plan date are on the same day based on the provided schedule.

    Parameters:
    - schedule_plan (Dict[str, Any]): A dictionary containing scheduling information. Expected keys:
        - "recommended_appointment_from": Recommended appointment datetime (ISO format).
        - "scheduled_appointment_from": Scheduled appointment datetime (ISO format).
        - "plan_date": Plan date for the appointment (ISO format).
    - user_time_zone (str): The timezone used to convert both the appointment and plan date.

    Returns:
    - bool: `True` if both the appointment date and plan date are on the same day in the given timezone, `False` otherwise.

    If an error occurs during processing, `False` is returned.
    """

    try: 
        recommended_appointment_from = schedule_plan.get("recommended_appointment_from")
        scheduled_appointment_from = schedule_plan.get("scheduled_appointment_from")
        plan_date = schedule_plan.get("plan_date")
        # appointment_from = appointment_dates.get('appointmentFrom') or appointment_dates.get('recommendedAppointmentFrom')
        appointment_from = scheduled_appointment_from or recommended_appointment_from
        
        if appointment_from:

            if isinstance(appointment_from, str):
                appointment_from = datetime.fromisoformat(appointment_from)
            # Convert both appointment date and plan date to the desired timezone
            if isinstance(plan_date, str):
                plan_date = datetime.fromisoformat(plan_date)

            # Convert both appointment date and plan date to the desired timezone  
            appointment_from_tz = appointment_from.astimezone(user_time_zone) #.localize(appointment_from_format)
            plan_date_tz = plan_date.astimezone(user_time_zone) # Adjust the format as necessary
            
            # Check if both dates are the same day
            return appointment_from_tz.date() == plan_date_tz.date()
        
        return False
    except Exception as e:
        logger.error(f"Error in is_appointment_date_in_same_day: {str(e)}")
        return False



async def push_load_count_to_firebase(carrier: str, reference_number: str, old_schedule_plan: List[Dict[str, Any]], new_schedule_plan: List[Dict[str, Any]], user_time_zone:str = "America/New_York", terminal = ''):
    """
    Push load count updates to Firebase based on the changes in the schedule plan.

    Parameters:
    - old_schedule_plan (List[Dict[str, Any]]): A list of dictionaries representing the old schedule plan. Each dictionary contains scheduling information.
    - new_schedule_plan (List[Dict[str, Any]]): A list of dictionaries representing the new schedule plan. Each dictionary contains scheduling information.
    - timezone (str): The timezone to calculate the time slots and handle date comparisons. Default is "America/New_York".

    Returns:
    - bool: `True` if the update to Firebase was successful, `False` if there was an error.

    The function calculates the load count differences between the old and new schedule plans, then pushes the changes (decrease and increase in count) to Firebase with additional schedule information:
        - 'decrease_count': The count of the old schedule plan.
        - 'increase_count': The count of the new schedule plan.
        - 'reference_number': Reference number of the schedule plan.
        - 'plan_date': Plan date from the schedule plan.
        - 'carrier': Carrier from the schedule plan.
        - 'terminal': Terminal from the schedule plan.

    If an error occurs during processing, `False` is returned.
    """

    try:
        firebasePayload: Dict[str, Any] = {}

        # Calculate old schedule plan count
        if len(old_schedule_plan) > 0:
            old_load_count: Dict[str, Any] = {}
            for schedule_plan in old_schedule_plan:
                if isinstance(schedule_plan.get('plan_date'), datetime):
                    schedule_plan['plan_date'] = schedule_plan['plan_date'].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

                if isinstance(schedule_plan.get('recommended_appointment_from'), datetime):
                    schedule_plan['recommended_appointment_from'] = schedule_plan['recommended_appointment_from'].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                
                if isinstance(schedule_plan.get('scheduled_appointment_from'), datetime):
                    schedule_plan['scheduled_appointment_from'] = schedule_plan['scheduled_appointment_from'].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    
                data_slots =  calculate_count_for_load(schedule_plan)
                old_load_count = update_count_for_load(old_load_count, data_slots.get("scheduled_time_slot"), data_slots.get("recommended_time_slot"), schedule_plan, user_time_zone)
            firebasePayload['decrease_count'] = old_load_count

        # Calculate new schedule plan count
        if len(new_schedule_plan) > 0:
            updated_load_count: Dict[str, Any] = {}
            for schedule_plan in new_schedule_plan:
                # Convert string dates to UTC datetime
                if isinstance(schedule_plan.get('plan_date'), datetime):
                    schedule_plan['plan_date'] = schedule_plan['plan_date'].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

                if isinstance(schedule_plan.get('recommended_appointment_from'), datetime):
                    schedule_plan['recommended_appointment_from'] = schedule_plan['recommended_appointment_from'].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                
                if isinstance(schedule_plan.get('scheduled_appointment_from'), datetime):
                    schedule_plan['scheduled_appointment_from'] = schedule_plan['scheduled_appointment_from'].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                
                data_slots =  calculate_count_for_load(schedule_plan)
                updated_load_count = update_count_for_load(updated_load_count, data_slots.get("scheduled_time_slot"), data_slots.get("recommended_time_slot"), schedule_plan, user_time_zone)
            firebasePayload['increase_count'] = updated_load_count

        firebasePayload['reference_number'] = reference_number;  
        firebasePayload['terminal'] = terminal;
        firebasePayload['lastUpdatedAt']= datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

        # push update to firebase
        fb_client = get_firebase_client()
        await fb_client.push_to_channel(f'{carrier}/updated_plan', firebasePayload)
        return True
    except Exception as e:
        logger.error(f"Error in push_load_count_to_firebase: {str(e)}")
        return False


def calculate_duration_from_routing_events(
    events: Dict[str, Any], 
    from_event: str, 
    to_event: str,
    distance_unit: str = 'mi',
    loading_time: int = 60,
) -> timedelta:
    """
    Calculate the total duration between two routing events including travel and loading time.

    Args:
        events: Dictionary containing routing event information with addresses
        from_event: Name of the starting event
        to_event: Name of the destination event 
        loading_time: Default loading/unloading time in minutes (default: 120)

    Returns:
        timedelta representing total duration between events

    Raises:
        KeyError: If either event is missing from events dict
        ValueError: If address information is invalid
    """
    try:
        # Validate input events exist
        if from_event not in events or to_event not in events:
            raise KeyError(f"Missing required events: {from_event} or {to_event}")

        # Get addresses, defaulting to empty dict if not found
        from_address = events[from_event].get('address', {})
        to_address = events[to_event].get('address', {})

        # Calculate travel distance and duration
        distance = calculate_distance_between_locations(from_address, to_address, distance_unit)
        travel_duration = calculate_duration_from_distance(distance, distance_unit)
        
        # Add loading/unloading buffer time
        total_duration = travel_duration + timedelta(minutes=loading_time)
        
        return total_duration

    except Exception as e:
        logger.error(f"Error calculating duration between {from_event} and {to_event}: {str(e)}")
        raise

def get_relative_appointment_time(
    appointment_time: str, 
    office_hours: Dict[str, Any], 
    duration: timedelta,
    tz: timezone
) -> str:
    """
    Calculate the relative appointment time based on office hours and duration.
    
    This function determines whether an appointment should be scheduled for the current day
    or previous day based on office hours and required duration.

    Args:
        appointment_time: ISO format string representing the appointment time
        office_hours: Dictionary containing office schedule information
            Expected format: {'office_hours_start': datetime.time object}
        duration: Time duration needed for the appointment

    Returns:
        str: ISO format string of the calculated appointment time

    Raises:
        ValueError: If appointment_time is not a valid ISO format string
        TypeError: If duration is not a timedelta object
    """
    try:
        # Convert ISO string to datetime object
        parsed_appointment = datetime.fromisoformat(appointment_time).astimezone(tz)

        # If appointment is at midnight, set it to same day
        if parsed_appointment.hour == 0:
            return parsed_appointment.isoformat()

        # Default office start time (6:00 AM)
        DEFAULT_START_TIME = datetime.strptime('00:00', '%H:%M').time()
        
        # Get office start time, use default if not specified
        office_start_time = DEFAULT_START_TIME
        if office_hours is not None:
            office_start_time = office_hours.get('office_hours_start', DEFAULT_START_TIME)
        
        # Create datetime at office start time on appointment date
        office_start_datetime = parsed_appointment.astimezone(tz).replace(
            hour=office_start_time.hour,
            minute=office_start_time.minute,
            second=0,
            microsecond=0
        )

        # Calculate cutoff time for previous day scheduling, 30 minutes buffer
        cutoff_time = parsed_appointment - duration + timedelta(minutes=30)

        # Return same day if after office start, previous day otherwise
        if office_start_datetime < cutoff_time:
            return parsed_appointment.isoformat()
        else:
            return (parsed_appointment - timedelta(days=1)).isoformat()

    except ValueError as e:
        raise ValueError(f"Invalid appointment time format: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Error calculating relative appointment time: {str(e)}")
