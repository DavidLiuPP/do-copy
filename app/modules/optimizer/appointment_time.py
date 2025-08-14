import pytz
from datetime import datetime, timedelta
from app.modules.optimizer.map_loads_optimizer_service import map_loads_for_optimizer
from app.postgres_services.driver_service import get_drivers
from app.modules.optimizer.hos_service import get_hos_data_for_drivers
from app.postgres_services.driver_schedule_service import store_driver_schedule, get_driver_schedule, get_driver_schedule_v3, store_driver_schedule_v3
from app.modules.optimizer.constants import VALID_EVENT_TYPES
from load_optimizer.get_optimal_plan_v2 import calculate_duration_from_distance, get_optimal_plan
from load_optimizer.get_optimal_plan_v3 import get_optimal_plan_v3
from vrp_optimizer.services import get_available_drivers_from_existing_driver_schedule

def map_appointment_times_to_recommended_moves(
    move_lookup: dict,
    tz: pytz.timezone,
    ref_num: str,
    move: dict = [],
    load_detail: dict = None,
    distance_unit: str = 'mi'
) -> list:
    """Map appointment times to recommended moves."""
    try:
        last_hour = None
        for event_type in VALID_EVENT_TYPES:
            event_with_appt = next(
                (event for event in move if event.get('type') == event_type and event.get('arrived')),
                None
            )
            lookup_key = (ref_num, event_type)

            recommended_move = move_lookup.get(lookup_key)
            if not recommended_move:
                continue

            if recommended_move.get('scheduled_appointment_from'):
                scheduled_time = datetime.fromisoformat(recommended_move['scheduled_appointment_from']).astimezone(tz)
                if scheduled_time.hour:
                    last_hour = scheduled_time.hour
                continue

            if event_with_appt:
                arrived_time = datetime.fromisoformat(event_with_appt['arrived']).astimezone(tz)
                last_hour = arrived_time.hour
                recommended_move['recommended_appointment_from'] = arrived_time.replace(
                    minute=0, second=0, microsecond=0
                ).isoformat()
                recommended_move['recommended_appointment_to'] = arrived_time.replace(
                    minute=59, second=59, microsecond=999999
                ).isoformat()

            elif last_hour:
                move_event = next(
                    (event for event in load_detail.get('driverOrder', []) if event.get('type') == event_type),
                    None
                )
                if move_event:
                    travel_time = calculate_duration_from_distance(move_event.get('distance'), distance_unit)
                    waiting_time = timedelta(minutes=move_event.get('waiting_time', 60))
                    total_time = (travel_time + waiting_time).total_seconds() / 3600
                    last_hour = int(last_hour + total_time)

                    recommended_move['recommended_appointment_from'] = (
                        datetime.fromisoformat(recommended_move['recommended_appointment_from']) +
                        timedelta(hours=last_hour)
                    ).isoformat()
                    recommended_move['recommended_appointment_to'] = (
                        datetime.fromisoformat(recommended_move['recommended_appointment_to']) +
                        timedelta(hours=last_hour)
                    ).isoformat()

            else:
                recommended_move['recommended_appointment_from'] = (
                    datetime.fromisoformat(recommended_move['recommended_appointment_from']) +
                    timedelta(hours=6)
                ).isoformat()
                recommended_move['recommended_appointment_to'] = (
                    datetime.fromisoformat(recommended_move['recommended_appointment_to']) +
                    timedelta(hours=6)
                ).isoformat()

    except Exception as e:
        raise Exception(f"Failed to map appointment times to recommended moves: {str(e)}")


async def get_appointment_time(
    user_payload: dict,
    plan_date: str,
    mapped_loads: list,
    recommended_moves: list,
    options: dict
) -> list:
    """Predict appointment times for a set of loads."""
    try:
        carrier = user_payload.get('carrier')
        timeZone = user_payload.get('timeZone')
        store_plan = options.get('store_plan')
        use_driver_schedule = options.get('use_driver_schedule')
        distance_unit = user_payload.get('distanceUnit', 'mi')

        # Get actionable moves
        actionable_moves, _ = await map_loads_for_optimizer(
            user_payload=user_payload,
            loads=mapped_loads,
            scheduled_plans=recommended_moves,
            converted_plan_date=plan_date,
            options={'time_prediction': True}
        )

        if len(actionable_moves) <= 0:
            return recommended_moves

        # Get drivers and map with HOS
        drivers = await get_drivers(carrier, plan_date)
        drivers = await get_hos_data_for_drivers(drivers, carrier)

        existing_driver_schedule = {}
        if use_driver_schedule:
            existing_driver_schedule = await get_driver_schedule(carrier, plan_date)

        for driver in drivers:
            driver['max_mileage'] = 1000
            driver['schedule'] = existing_driver_schedule.get(driver['_id'], [])

        # Get optimal plan
        optimal_plan, driver_schedule = await get_optimal_plan(user_payload, actionable_moves, drivers, plan_date)

        # Handle unplanned moves
        planned_reference_numbers = [move.get('reference_number') for move in optimal_plan]
        unplanned_moves = [
            move for move in actionable_moves
            if move.get('reference_number') not in planned_reference_numbers
        ]

        if len(unplanned_moves) > 0:
            optimal_plan_next_iteration, driver_schedule_next_iteration = await get_optimal_plan(
                user_payload, unplanned_moves, drivers, plan_date
            )
            if len(optimal_plan_next_iteration) > 0:
                optimal_plan.extend(optimal_plan_next_iteration)
                driver_schedule = driver_schedule_next_iteration

        # Create lookup dictionary for recommended moves
        move_lookup = {
            (move['reference_number'], move['profile_type']): move
            for move in recommended_moves
        }

        # Process each load and event
        tz = pytz.timezone(timeZone)
        for load in optimal_plan:
            move = load['move']
            ref_num = load['reference_number']
            load_detail = next(
                (load for load in mapped_loads if load.get('reference_number') == ref_num),
                None
            )
            map_appointment_times_to_recommended_moves(move_lookup, tz, ref_num, move, load_detail, distance_unit)

        # Handle remaining unplanned moves
        planned_reference_numbers = [move.get('reference_number') for move in optimal_plan]
        unplanned_moves = list(set([
            move.get('reference_number') for move in recommended_moves
            if move.get('reference_number') not in planned_reference_numbers
        ]))

        if len(unplanned_moves) > 0:
            for ref_num in unplanned_moves:
                load_detail = next(
                    (load for load in mapped_loads if load.get('reference_number') == ref_num),
                    None
                )
                map_appointment_times_to_recommended_moves(move_lookup, tz, ref_num, [], load_detail, distance_unit)

        if store_plan and len(optimal_plan) > 0:
            await store_driver_schedule({
                'carrier': carrier,
                'plan_date': plan_date,
                'driver_schedule': driver_schedule
            })

        return recommended_moves

    except Exception as e:
        raise Exception(f"Failed to predict appointment times: {str(e)}")
    

async def get_appointment_time_v3(
    user_payload: dict,
    plan_date: str,
    mapped_loads: list,
    recommended_moves: list,
    options: dict
) -> list:
    """Predict appointment times for a set of loads."""
    try:
        carrier = user_payload.get('carrier')
        timeZone = user_payload.get('timeZone')
        store_plan = options.get('store_plan')
        use_driver_schedule = options.get('use_driver_schedule')
        distance_unit = user_payload.get('distanceUnit', 'mi')

        # Get actionable moves
        actionable_moves, _ = await map_loads_for_optimizer(
            user_payload=user_payload,
            loads=mapped_loads,
            scheduled_plans=recommended_moves,
            converted_plan_date=plan_date,
            options={'time_prediction': True}
        )

        if len(actionable_moves) <= 0:
            return recommended_moves

        # Get drivers and map with HOS
        drivers = await get_drivers(carrier, plan_date)
        drivers = await get_hos_data_for_drivers(drivers, carrier)

        driver_schedule = {}
        if use_driver_schedule:
            driver_schedule = await get_driver_schedule_v3(carrier, plan_date)

        available_drivers = drivers
        additional_depot_locations = []

        if driver_schedule:
            available_drivers, additional_depot_locations = get_available_drivers_from_existing_driver_schedule(driver_schedule, drivers)  

        # Get optimal plan
        optimal_plan, skipped_moves, new_driver_schedule = await get_optimal_plan_v3(
            user_payload=user_payload, 
            actionable_moves=actionable_moves, 
            drivers=available_drivers, 
            converted_plan_date=plan_date, 
            additional_depot_locations=list(additional_depot_locations), 
            return_schedule=True,
            time_limit=30 if use_driver_schedule else 300
        )

        driver_schedule.update(new_driver_schedule)
        
        # Handle unplanned moves
        planned_reference_numbers = [move.get('reference_number') for move in optimal_plan]
        skipped_reference_numbers = [move.get('reference_number') for move in skipped_moves]
        unplanned_moves = [
            move for move in actionable_moves
            if move.get('reference_number') not in planned_reference_numbers and move.get('reference_number') not in skipped_reference_numbers
        ]

        if len(unplanned_moves) > 0:

            optimal_plan_next_iteration, skipped_moves_next_iteration, driver_schedule_next_iteration = await get_optimal_plan_v3(
                user_payload=user_payload, 
                actionable_moves=unplanned_moves, 
                drivers=drivers, 
                converted_plan_date=plan_date, 
                return_schedule=True,
                time_limit=30
            )
            if len(optimal_plan_next_iteration) > 0:
                optimal_plan.extend(optimal_plan_next_iteration)
                driver_schedule.update(driver_schedule_next_iteration)

        if len(optimal_plan) == 0:
            return recommended_moves

        # Create lookup dictionary for recommended moves
        move_lookup = {
            (move['reference_number'], move['profile_type']): move
            for move in recommended_moves
        }

        # Process each load and event
        tz = pytz.timezone(timeZone)
        for load in optimal_plan:
            move = load['move']
            ref_num = load['reference_number']
            load_detail = next(
                (load for load in mapped_loads if load.get('reference_number') == ref_num),
                None
            )
            map_appointment_times_to_recommended_moves(move_lookup, tz, ref_num, move, load_detail, distance_unit)

        # Handle remaining unplanned moves
        planned_reference_numbers = [move.get('reference_number') for move in optimal_plan]
        unplanned_moves = list(set([
            move.get('reference_number') for move in recommended_moves
            if move.get('reference_number') not in planned_reference_numbers
        ]))

        if len(unplanned_moves) > 0:
            for ref_num in unplanned_moves:
                load_detail = next(
                    (load for load in mapped_loads if load.get('reference_number') == ref_num),
                    None
                )
                map_appointment_times_to_recommended_moves(move_lookup, tz, ref_num, [], load_detail, distance_unit)

        if store_plan:
            await store_driver_schedule_v3({
                'carrier': carrier,
                'plan_date': plan_date,
                'driver_schedule': driver_schedule
            })

        return recommended_moves
    except Exception as e:
        raise Exception(f"Failed to predict appointment times: {str(e)}")

# # Initialize hours counter dictionary
# hours_count = {i: 0 for i in range(24)}
# for move in recommended_moves:
#     if move.get('recommended_appointment_from'):
#         hour = datetime.fromisoformat(move['recommended_appointment_from']).hour
#         hours_count[hour] += 1
