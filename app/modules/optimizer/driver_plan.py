import logging
import time
import pytz
from typing import Dict, Any
from datetime import datetime, timedelta
from fastapi import BackgroundTasks

from app.modules.optimizer.driver_plan_service import save_plan_details, upload_plan_json_to_s3
from app.modules.optimizer.hos_service import get_hos_data_for_drivers
from app.modules.scheduler.utility import map_loads_for_scheduler
from app.modules.scheduler.move_scheduler import retrieve_scheduled_moves_for_optimizer
from app.modules.optimizer.map_loads_optimizer_service import map_loads_for_optimizer
from app.services.common_service import get_time_zone, get_carrier_preferences
from app.services.mapping_service import add_recommended_returns
from app.services.redis_service import get_default_yard_location
from app.postgres_services.driver_service import get_drivers
from app.mongo_services.load_service import (
    get_available_loads,
    get_moves_from_driver_order,
    get_assigned_moves
)

from load_optimizer.get_optimal_plan_v2 import get_unassigned_moves
from load_optimizer.get_optimal_plan_v3 import get_optimal_plan_v3
from app.modules.optimizer.utility import get_planning_time_windows


# Configure module logger
logger = logging.getLogger(__name__)


def validate_optimizer_parameters(
    carrier: str,
    plan_date: str,
    shift: str = None,
    plan_branch: list = []
):
    if not carrier:
        raise ValueError("Carrier ID is required")

    try:
        datetime.strptime(plan_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Invalid plan_date format. Expected YYYY-MM-DD")

    if shift and plan_branch and len(plan_branch) > 1:
        raise ValueError("Multiple branches are not supported for shift")


async def retrieve_loads_for_planning(
    user_payload: Dict[str, Any],
    converted_plan_date: datetime,
    reference_numbers: list = [],
    plan_from_time: datetime = None,
    plan_to_time: datetime = None,
    plan_drivers: list = [],
    plan_branch: list = [],
    add_to_existing_plan: bool = False,
    shift: str = None,
    is_specific_load: bool = False,
    shift_window: list = [0, 1440]
):
    try:
        carrier = user_payload.get('carrier')

        # retrieve loads scheduled for the given date
        start_scheduled_plans_time = time.time()
        scheduled_plans = await retrieve_scheduled_moves_for_optimizer(
            user_payload=user_payload,
            plan_branch=plan_branch,
            converted_plan_date=converted_plan_date,
            plan_from_time=plan_from_time,
            plan_to_time=plan_to_time,
            shift=shift
        )
        end_scheduled_plans_time = time.time()
        print(f"Time taken to get scheduled plans: {end_scheduled_plans_time - start_scheduled_plans_time} seconds")

        # if reference numbers are provided, filter the scheduled plans
        if len(reference_numbers) > 0:
            scheduled_plans = [plan for plan in scheduled_plans if plan.get('reference_number') in reference_numbers]

        if len(scheduled_plans) == 0:
            raise Exception("No scheduled plans found for the given date")

        # now get the unique load numbers from scheduled plans
        reference_numbers = list(set(plan['reference_number'] for plan in scheduled_plans))

        # Get the moves assigned or arrived in the shift time window.
        assigned_moves = await get_assigned_moves(
            user_payload=user_payload,
            from_time=converted_plan_date + timedelta(minutes=shift_window[0]),
            to_time=converted_plan_date + timedelta(minutes=shift_window[1]),
            add_to_existing_plan=add_to_existing_plan,
            plan_branch=plan_branch,
            plan_drivers=plan_drivers
        )

        if is_specific_load:
            assigned_moves = [move for move in assigned_moves if move.get('reference_number') in reference_numbers]

        assigned_reference_numbers = list(set(load.get('reference_number') for load in assigned_moves))
        reference_numbers = [ref for ref in reference_numbers if ref not in assigned_reference_numbers]
        
        available_loads = await get_available_loads(carrier, reference_numbers, plan_branch, 2000)
        loads = [*assigned_moves, *available_loads]

        return loads, scheduled_plans

    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to retrieve loads for planning: {str(e)}")


async def get_optmized_driver_plan(
    user_payload: Dict[str, Any],
    plan_date: str,
    reference_numbers: list = [],
    save_plan: bool = False,
    add_to_existing_plan: bool = False,
    is_approved_move_ids_provided: bool = False,
    approved_modified_move_ids: list = [],
    plan_branch: list = [],
    shift: str = None,
    background_tasks: BackgroundTasks = None,
    allow_late_arrivals: bool = False
) -> Dict[str, Any]:
    try:
        # Validate inputs
        carrier = user_payload.get('carrier')
        validate_optimizer_parameters(carrier, plan_date, shift, plan_branch)
        
        # get loads for the given date
        timeZone = await get_time_zone(carrier)
        tz = pytz.timezone(timeZone)
        converted_plan_date = tz.localize(datetime.strptime(plan_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0))

        # get default yard location
        default_yard_locations = await get_default_yard_location(carrier)
        carrier_preferences = await get_carrier_preferences(carrier)

        planning_windows = await get_planning_time_windows(carrier, converted_plan_date, shift, plan_branch, timeZone)
        planning_minutes = planning_windows.get('plan_window') or [0, 1440]
        plan_from_time = converted_plan_date + timedelta(minutes=planning_minutes[0])
        plan_to_time = converted_plan_date + timedelta(minutes=planning_minutes[1])
        
        user_payload['default_yard_locations'] = default_yard_locations
        user_payload['timeZone'] = timeZone
        user_payload['distanceUnit'] = carrier_preferences.get('distanceUnit', 'mi')

        # get drivers
        start_driver_time = time.time()
        drivers = await get_drivers(
            carrier=carrier,
            plan_date=converted_plan_date,
            plan_branch=plan_branch,
            settings={'exclude_account_hold': True, 'shift': shift}
        )

        if not drivers:
            raise Exception("No drivers found for the given date")

        # map drivers with HOS
        drivers = await get_hos_data_for_drivers(drivers, carrier)
        plan_drivers = [driver.get('_id') for driver in drivers]
        end_driver_time = time.time()
        print(f"Time taken to get drivers: {end_driver_time - start_driver_time} seconds")

        # get loads for the given date
        start_loads_time = time.time()
        loads, scheduled_plans = await retrieve_loads_for_planning(
            user_payload=user_payload,
            converted_plan_date=converted_plan_date,
            reference_numbers=reference_numbers,
            plan_from_time=plan_from_time,
            plan_to_time=plan_to_time,
            plan_drivers=plan_drivers,
            plan_branch=plan_branch,
            add_to_existing_plan=add_to_existing_plan,
            shift=shift,
            shift_window=planning_windows.get('shift_window', {}) or [0, 1440]
        )
        end_loads_time = time.time()
        print(f"Time taken to get loads: {end_loads_time - start_loads_time} seconds")

        start_mapped_loads_time = time.time()
        mapped_loads = map_loads_for_scheduler(loads)
        end_mapped_loads_time = time.time()
        print(f"Time taken to map loads: {end_mapped_loads_time - start_mapped_loads_time} seconds")

        start_add_recommended_returns_time = time.time()
        try:
            mapped_loads = await add_recommended_returns(user_payload, plan_date, mapped_loads)
        except Exception as e:
            logger.error(f"Error adding recommended returns: {str(e)}")
        end_add_recommended_returns_time = time.time()
        print(f"Time taken to add recommended returns: {end_add_recommended_returns_time - start_add_recommended_returns_time} seconds")

        start_map_loads_for_optimizer_time = time.time()    
        actionable_moves, invalid_moves = await map_loads_for_optimizer(
            user_payload=user_payload,
            loads=mapped_loads,
            scheduled_plans=scheduled_plans,
            converted_plan_date=converted_plan_date,
            plan_range={
                'from_time': plan_from_time,
                'to_time': plan_to_time,
                'shift_from_time':  converted_plan_date + timedelta(minutes=planning_windows.get('shift_window')[0]),
                'shift_to_time':  converted_plan_date + timedelta(minutes=planning_windows.get('shift_window')[1]),
            },
            options={
                'is_approved_move_ids_provided': is_approved_move_ids_provided,
                'approved_move_ids': approved_modified_move_ids,
                'replace_free_flow_trips': True
            },
            plan_drivers=plan_drivers,
            reference_numbers=reference_numbers
        )
        end_map_loads_for_optimizer_time = time.time()
        print(f"Time taken to map loads for optimizer: {end_map_loads_for_optimizer_time - start_map_loads_for_optimizer_time} seconds")
        
        # get optimal plan
        start_optimal_plan_time = time.time()
        optimizer_output = await get_optimal_plan_v3(
            user_payload,
            actionable_moves,
            drivers,
            converted_plan_date,
            branch=plan_branch,
            shift=shift,
            allow_late_arrivals=allow_late_arrivals
        )
        optimal_plan = optimizer_output['optimal_plan']
        skipped_moves = optimizer_output['skipped_moves']
        optimizer_input = optimizer_output['optimizer_input']
        
        end_optimal_plan_time = time.time()
        print(f"Time taken to get optimal plan: {end_optimal_plan_time - start_optimal_plan_time} seconds")
        plan_id = ""

        # Save Plan Details
        try:
            if save_plan and optimal_plan:
                unassigned_plans = get_unassigned_moves(
                    optimal_plan=optimal_plan,
                    all_loads=mapped_loads,
                    invalid_moves=invalid_moves,
                    skipped_moves=skipped_moves,
                    plan_date=converted_plan_date,
                    timeZone=timeZone,
                    actionable_moves=actionable_moves
                )
                
                start_save_plan_details_time = time.time()
                plan_id = await save_plan_details(
                    user_payload=user_payload,
                    converted_plan_date=converted_plan_date,
                    optimal_plan=optimal_plan,
                    unassigned_plans=unassigned_plans,
                    plan_branch=plan_branch,
                    shift=shift,
                    background_tasks=background_tasks,
                )
                
                # Upload plan input JSON to S3
                try:
                    optimizer_input['plan_id'] = str(plan_id)
                    await upload_plan_json_to_s3(optimizer_input, carrier, str(plan_id))
                except Exception as upload_err:
                    logger.error(f"Failed to upload plan JSON to S3: {str(upload_err)}")

                end_save_plan_details_time = time.time()
                print(f"Time taken to save plan details: {end_save_plan_details_time - start_save_plan_details_time} seconds")
        except Exception as e:
            logger.error("Failed to save plan details", e)

        return {
            "carrier": carrier,
            "plan_date": plan_date,
            "recommended_moves": optimal_plan,
            "plan_id": str(plan_id) if plan_id else ""
        }
    
    except Exception as e:
        logger.error(e)
        raise e


async def get_optimizer_review_recommendation(
    user_payload: Dict[str, Any],
    plan_date: str,
    reference_numbers: list = [],
    plan_branch: list = [],
    shift: Any = None
) -> Dict[str, Any]:
    try:
        # get loads for the given date
        carrier = user_payload.get('carrier')
        validate_optimizer_parameters(carrier, plan_date, shift, plan_branch)

        timeZone = await get_time_zone(carrier)
        tz = pytz.timezone(timeZone)
        converted_plan_date = tz.localize(datetime.strptime(plan_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0))

        # get default yard location
        default_yard_locations = await get_default_yard_location(carrier)
        carrier_preferences = await get_carrier_preferences(carrier)

        user_payload['default_yard_locations'] = default_yard_locations
        user_payload['timeZone'] = timeZone
        user_payload['distanceUnit'] = carrier_preferences.get('distanceUnit', 'mi')


        planning_windows = await get_planning_time_windows(carrier, converted_plan_date, shift, plan_branch, timeZone)
        planning_minutes = planning_windows.get('plan_window') or [0, 1440]
        plan_from_time = converted_plan_date + timedelta(minutes=planning_minutes[0])
        plan_to_time = converted_plan_date + timedelta(minutes=planning_minutes[1])

        plan_drivers = None
        if shift:
            drivers = await get_drivers(carrier, converted_plan_date, plan_branch, {'exclude_account_hold': True, 'shift': shift})
            if not drivers:
                raise Exception("No drivers found for the given shift")
            plan_drivers = [driver.get('_id') for driver in drivers]

        loads, scheduled_plans = await retrieve_loads_for_planning(
            user_payload=user_payload,
            converted_plan_date=converted_plan_date,
            reference_numbers=reference_numbers,
            plan_from_time=plan_from_time,
            plan_to_time=plan_to_time,
            plan_drivers=plan_drivers,
            plan_branch=plan_branch,
            add_to_existing_plan=True,
            shift=shift,
            is_specific_load=True,
            shift_window=planning_windows.get('shift_window', {}) or [0, 1440]
        )

        mapped_loads = map_loads_for_scheduler(loads)
        mapped_loads = await add_recommended_returns(user_payload, plan_date, mapped_loads)

        actionable_moves, _ = await map_loads_for_optimizer(
            user_payload=user_payload,
            loads=mapped_loads,
            scheduled_plans=scheduled_plans,
            converted_plan_date=converted_plan_date,
            plan_range={
                'from_time': plan_from_time,
                'to_time': plan_to_time,
                'shift_from_time':  converted_plan_date + timedelta(minutes=planning_windows.get('shift_window')[0]),
                'shift_to_time':  converted_plan_date + timedelta(minutes=planning_windows.get('shift_window')[1]),
            },
            plan_drivers=plan_drivers
        )

        total_moves = len(actionable_moves)

        modified_moves = [move for move in actionable_moves if move.get('is_modified_move')]

        recommendations = []
        for modified_move in modified_moves:
            moveId = modified_move.get('move', [])[0].get('moveId')

            # get the move from the mapped loads
            actual_load = next((load for load in mapped_loads if load.get('reference_number') == modified_move.get('reference_number')), None)
            if actual_load:
                
                moves = get_moves_from_driver_order(actual_load.get('driverOrder'), { "exclude_void_out": True, "exclude_completed_move": True })
                actual_move = next((move for move in moves if any(event.get('moveId') == moveId for event in move)), None)

                if actual_move:
                    recommendations.append({
                        '_id': actual_load.get('_id'),
                        'reference_number': actual_load.get('reference_number'),
                        'move_id': moveId,
                        'actual_move': actual_move,
                        'recommended_move': [
                            {k: v for k, v in event.items() if k not in ['waiting_time_distribution']}
                            for event in modified_move.get('move')
                        ],
                    })

        return {
            "total_moves": total_moves,
            "recommendations": recommendations
        }

    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to get optimizer plan recommendation: {str(e)}")

async def get_in_day_actions(
    user_payload: Dict[str, Any],
    plan_date: str,
    reference_numbers: list = [],
    plan_branch: list = [],
    shift: Any = None,
) -> Dict[str, Any]:
    try:
        # get loads for the given date
        carrier = user_payload.get('carrier')
        validate_optimizer_parameters(carrier, plan_date, shift, plan_branch)

        timeZone = await get_time_zone(carrier)
        tz = pytz.timezone(timeZone)
        converted_plan_date = tz.localize(datetime.strptime(plan_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0))

        # get default yard location
        default_yard_locations = await get_default_yard_location(carrier)
        carrier_preferences = await get_carrier_preferences(carrier)

        user_payload['default_yard_locations'] = default_yard_locations
        user_payload['timeZone'] = timeZone
        user_payload['distanceUnit'] = carrier_preferences.get('distanceUnit', 'mi')

        planning_windows = await get_planning_time_windows(carrier, converted_plan_date, shift, plan_branch, timeZone)
        planning_minutes = planning_windows.get('plan_window') or [0, 1440]
        plan_from_time = converted_plan_date + timedelta(minutes=planning_minutes[0])
        plan_to_time = converted_plan_date + timedelta(minutes=planning_minutes[1])
        

        plan_drivers = None

        loads, scheduled_plans = await retrieve_loads_for_planning(
            user_payload=user_payload,
            converted_plan_date=converted_plan_date,
            reference_numbers=reference_numbers,
            plan_from_time=plan_from_time,
            plan_to_time=plan_to_time,
            plan_drivers=plan_drivers,
            plan_branch=plan_branch,
            add_to_existing_plan=True,
            shift=shift,
            shift_window=planning_windows.get('shift_window', {}) or [0, 1440]
        )

        mapped_loads = map_loads_for_scheduler(loads)
        mapped_loads = await add_recommended_returns(user_payload, plan_date, mapped_loads)

        actionable_moves, _ = await map_loads_for_optimizer(
            user_payload=user_payload,
            loads=mapped_loads,
            scheduled_plans=scheduled_plans,
            converted_plan_date=converted_plan_date,
            plan_range={
                'from_time': plan_from_time,
                'to_time': plan_to_time,
                'shift_from_time':  converted_plan_date + timedelta(minutes=planning_windows.get('shift_window')[0]),
                'shift_to_time':  converted_plan_date + timedelta(minutes=planning_windows.get('shift_window')[1]),
            },
            plan_drivers=plan_drivers
        )

        modified_moves = []
        for move in actionable_moves:
            # if driver is assigned to the move, then ignore the move
            is_assigned_move = any(event.get('driver') for event in move.get('move'))
            if is_assigned_move:
                continue

            if move.get('is_modified_move'):
                move_data = {
                    '_id': move.get('_id'),
                    'reference_number': move.get('reference_number'),
                    'move_id': move.get('move', [])[0].get('moveId'),
                    'recommended_move': [
                        {k: v for k, v in event.items() if k not in ['waiting_time_distribution']}
                        for event in move.get('move')
                    ],
                }

                if move.get('free_flow_order'):
                    move_data['free_flow_order'] = move.get('free_flow_order')

                modified_moves.append(move_data)
            else:
                move_data = {
                    '_id': move.get('_id'),
                    'reference_number': move.get('reference_number'),
                    'move_id': move.get('move', [])[0].get('moveId'),
                    'recommended_move': []
                }

                if move.get('free_flow_order'):
                    move_data['free_flow_order'] = move.get('free_flow_order')

                modified_moves.append(move_data)

        return modified_moves
    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to get optimizer plan recommendation: {str(e)}")
