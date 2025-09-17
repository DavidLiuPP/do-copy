import logging
import time
import pytz
from typing import Dict, Any, List
from datetime import datetime, timedelta
from fastapi import BackgroundTasks

from app.modules.optimizer.driver_plan_service import save_plan_details, upload_plan_json_to_s3
from app.modules.optimizer.hos_service import get_hos_data_for_drivers
from app.modules.scheduler.utility import map_loads_for_scheduler, manage_generate_plan_status
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
from app.modules.optimizer.utility import get_shift_time
from app.modules.optimizer.in_day_service import save_suggestions, get_behind_schedule_moves_for_in_day_plan
from vrp_optimizer.helpers import generated_criteria


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
    load_criteria: Dict[str, Any],
    plan_from_time: datetime = None,
    plan_to_time: datetime = None,
    plan_drivers: list = [],
    add_to_existing_plan: bool = False,
    is_specific_load: bool = False,
    shift_window: list = [0, 1440],
    is_in_day_plan: bool = False,
    behind_schedule_moves: list = [],
):
    try:
        carrier = user_payload.get('carrier')
        reference_numbers = load_criteria.get('reference_numbers', [])

        # retrieve loads scheduled for the given date
        start_scheduled_plans_time = time.time()
        scheduled_plans = await retrieve_scheduled_moves_for_optimizer(
            user_payload=user_payload,
            load_criteria=load_criteria,
            plan_from_time=plan_from_time,
            plan_to_time=plan_to_time,
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

        assigned_moves = []
        if is_in_day_plan:
            assigned_moves = await get_behind_schedule_moves_for_in_day_plan(
                user_payload, 
                behind_schedule_moves=behind_schedule_moves
            )

        else:
            # Get the moves assigned or arrived in the shift time window.
            assigned_moves = await get_assigned_moves(
                user_payload=user_payload,
                load_criteria=load_criteria,
                from_time=load_criteria.get('plan_date') + timedelta(minutes=shift_window[0]),
                to_time=load_criteria.get('plan_date') + timedelta(minutes=shift_window[1]),
                add_to_existing_plan=add_to_existing_plan,
                plan_drivers=plan_drivers,
            )

            # set is_active_move to True for the assigned moves
            for move in assigned_moves:
                move['is_active_move'] = True

            if is_specific_load:
                assigned_moves = [move for move in assigned_moves if move.get('reference_number') in reference_numbers]

        assigned_reference_numbers = list(set(load.get('reference_number') for load in assigned_moves))
        reference_numbers = [ref for ref in reference_numbers if ref not in assigned_reference_numbers]
        
        available_loads = await get_available_loads(carrier, load_criteria = {**load_criteria, 'reference_numbers': reference_numbers}, limit=2000)
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
    behind_schedule_moves: list = [],
    is_in_day_plan: bool = False,
    driver_schedules: List[Dict[str, Any]] = [],
    driver_tags: list = [],
    route_type: list = []
) -> Dict[str, Any]:
    is_manage_plan_status = True if not is_in_day_plan and save_plan else False
    try:
        # Validate inputs
        carrier = user_payload.get('carrier')
        validate_optimizer_parameters(carrier, plan_date, shift, plan_branch)
        
        # get loads for the given date
        timeZone = await get_time_zone(carrier)
        tz = pytz.timezone(timeZone)
        converted_plan_date = tz.localize(datetime.strptime(plan_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0))
        
        if is_manage_plan_status:
            await manage_generate_plan_status(carrier, plan_date, "RUNNING", plan_branch)

        # get default yard location
        default_yard_locations = await get_default_yard_location(carrier, False, plan_branch)
        carrier_preferences = await get_carrier_preferences(carrier)

        planning_minutes = await get_shift_time(carrier, converted_plan_date, shift, plan_branch, timeZone)
        plan_from_time = converted_plan_date + timedelta(minutes=planning_minutes[0])
        plan_to_time = converted_plan_date + timedelta(minutes=planning_minutes[1])
        
        user_payload['default_yard_locations'] = default_yard_locations
        user_payload['timeZone'] = timeZone
        user_payload['distanceUnit'] = carrier_preferences.get('distanceUnit', 'mi')

        driver_criteria, load_criteria = generated_criteria(
            plan_date=converted_plan_date,
            plan_branch=plan_branch,
            shift=shift,
            driver_tags=driver_tags,
            route_type=route_type
        )

        # get drivers
        start_driver_time = time.time()
        drivers = await get_drivers(
            carrier=carrier,
            driver_criteria=driver_criteria,
            settings={'exclude_account_hold': True}
        )

        if not drivers:
            if is_manage_plan_status:
                await manage_generate_plan_status(carrier, plan_date, "ERROR", plan_branch)
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
            load_criteria=load_criteria,
            plan_from_time=plan_from_time,
            plan_to_time=plan_to_time,
            plan_drivers=plan_drivers,
            add_to_existing_plan=add_to_existing_plan,
            shift_window=planning_minutes,
            is_in_day_plan=is_in_day_plan,
            behind_schedule_moves=behind_schedule_moves,
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
        actionable_moves, invalid_moves, removed_moves = await map_loads_for_optimizer(
            user_payload=user_payload,
            loads=mapped_loads,
            scheduled_plans=scheduled_plans,
            converted_plan_date=converted_plan_date,
            plan_range={
                'from_time': plan_from_time,
                'to_time': plan_to_time,
                'shift_from_time':  converted_plan_date + timedelta(minutes=planning_minutes[0]),
                'shift_to_time':  converted_plan_date + timedelta(minutes=planning_minutes[1]),
            },
            options={
                'is_approved_move_ids_provided': is_approved_move_ids_provided,
                'approved_move_ids': approved_modified_move_ids,
                'replace_free_flow_trips': True
            },
            plan_drivers=plan_drivers,
            reference_numbers=reference_numbers,
            is_in_day_plan=is_in_day_plan
        )
        end_map_loads_for_optimizer_time = time.time()
        print(f"Time taken to map loads for optimizer: {end_map_loads_for_optimizer_time - start_map_loads_for_optimizer_time} seconds")

        valid_mapped_loads = [load for load in mapped_loads if load.get('reference_number') not in removed_moves]
        
        # get optimal plan
        start_optimal_plan_time = time.time()
        optimizer_output = await get_optimal_plan_v3(
            user_payload,
            actionable_moves,
            drivers,
            converted_plan_date,
            branch=plan_branch,
            shift=shift,
            behind_schedule_moves=behind_schedule_moves,
            is_in_day_plan=is_in_day_plan,
            driver_schedules=driver_schedules,
            invalid_moves=invalid_moves
        )
        optimal_plan = optimizer_output['optimal_plan']
        invalid_moves = optimizer_output['invalid_moves']
        optimizer_input = optimizer_output['optimizer_input']
        
        end_optimal_plan_time = time.time()
        print(f"Time taken to get optimal plan: {end_optimal_plan_time - start_optimal_plan_time} seconds")

        unassigned_plans = get_unassigned_moves(
            optimal_plan=optimal_plan,
            all_loads=valid_mapped_loads,
            invalid_moves=invalid_moves,
            plan_date=converted_plan_date,
            timeZone=timeZone,
            actionable_moves=actionable_moves
        )

        plan_id = ""
        # Save Plan Details
        try:
            if save_plan and optimal_plan:
                start_save_plan_details_time = time.time()
                plan_id = await save_plan_details(
                    user_payload=user_payload,
                    converted_plan_date=converted_plan_date,
                    optimal_plan=optimal_plan,
                    unassigned_plans=unassigned_plans,
                    plan_branch=plan_branch,
                    shift=shift,
                    background_tasks=background_tasks,
                    driver_tags=driver_tags,
                    route_type=route_type
                )
                if is_manage_plan_status:
                    await manage_generate_plan_status(carrier, plan_date, "COMPLETED", plan_branch, plan_id)
                
                # Upload plan input JSON to S3
                try:
                    optimizer_input['plan_id'] = str(plan_id)
                    await upload_plan_json_to_s3(optimizer_input, carrier, str(plan_id))
                except Exception as upload_err:
                    logger.error(f"Failed to upload plan JSON to S3: {str(upload_err)}")

                end_save_plan_details_time = time.time()
                print(f"Time taken to save plan details: {end_save_plan_details_time - start_save_plan_details_time} seconds")
            else:
                if is_manage_plan_status:
                    logger.info(f"Background task: No optimal plan generate for {plan_date} and carrier {carrier}" )
                    await manage_generate_plan_status(carrier, plan_date, "ERROR", plan_branch)
        except Exception as e:
            if is_manage_plan_status:
                logger.info(f"Background task: Failed to save plan details {plan_date} and carrier {carrier}" )
                await manage_generate_plan_status(carrier, plan_date, "ERROR", plan_branch)
            logger.error("Failed to save plan details", e)

        return {
            "carrier": carrier,
            "plan_date": plan_date,
            "recommended_moves": optimal_plan,
            "unassigned_plans": unassigned_plans,
            "plan_id": str(plan_id) if plan_id else ""
        }
    
    except Exception as e:
        logger.error(e)
        if is_manage_plan_status:
            logger.info(f"Background task: Failed to generate optimal plan for {plan_date} and carrier {carrier}" )
            await manage_generate_plan_status(carrier, plan_date, "ERROR", plan_branch)
        raise e

async def manage_in_day_plan(
    user_payload: Dict[str, Any],
    driver_schedules: List[Dict[str, Any]] = [],
    behind_schedule_moves: List[Dict[str, Any]] = [],
    branch: str | None = None,
    shift: str | None = None
):
    try:
        carrier = user_payload.get('carrier')
        if not carrier:
            raise Exception("Carrier ID is required")
        
        timeZone = await get_time_zone(carrier)
        tz = pytz.timezone(timeZone)
        today = datetime.now(tz).strftime('%Y-%m-%d')
        converted_plan_date = tz.localize(datetime.strptime(today, '%Y-%m-%d').replace(hour=0, minute=0, second=0))
        
        optimal_plan = await get_optmized_driver_plan(
            user_payload=user_payload,
            plan_date=today,
            save_plan=False,
            add_to_existing_plan=True,
            plan_branch=[branch],
            shift=shift,
            behind_schedule_moves=behind_schedule_moves,
            is_in_day_plan=True,
            driver_schedules=driver_schedules
        )

        recommended_moves = optimal_plan.get('recommended_moves', [])

        await save_suggestions(
            user_payload=user_payload,
            payload={
                "branch": branch,
                "shift": shift,
                "behind_schedule_moves": behind_schedule_moves
            }, 
            plan_date=today, 
            converted_plan_date=converted_plan_date, 
            suggestions=recommended_moves
        )

        return True

    except Exception as e:
        logger.error(e)
        raise Exception(f"Failed to generate in-day plan: {str(e)}")



async def get_optimizer_review_recommendation(
    user_payload: Dict[str, Any],
    plan_date: str,
    reference_numbers: list = [],
    plan_branch: list = [],
    shift: Any = None,
    driver_tags: list = [],
    route_type: list = []
) -> Dict[str, Any]:
    try:
        # get loads for the given date
        carrier = user_payload.get('carrier')
        validate_optimizer_parameters(carrier, plan_date, shift, plan_branch)

        timeZone = await get_time_zone(carrier)
        tz = pytz.timezone(timeZone)
        converted_plan_date = tz.localize(datetime.strptime(plan_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0))

        # get default yard location
        default_yard_locations = await get_default_yard_location(carrier, False, plan_branch)
        carrier_preferences = await get_carrier_preferences(carrier)

        user_payload['default_yard_locations'] = default_yard_locations
        user_payload['timeZone'] = timeZone
        user_payload['distanceUnit'] = carrier_preferences.get('distanceUnit', 'mi')


        planning_minutes = await get_shift_time(carrier, converted_plan_date, shift, plan_branch, timeZone)
        plan_from_time = converted_plan_date + timedelta(minutes=planning_minutes[0])
        plan_to_time = converted_plan_date + timedelta(minutes=planning_minutes[1])

        driver_criteria, load_criteria = generated_criteria(
            plan_branch=plan_branch,
            shift=shift, 
            reference_numbers=reference_numbers,
            driver_tags=driver_tags,
            route_type=route_type,
            plan_date=converted_plan_date
        )
        plan_drivers = None
        if shift or driver_tags:
            driver_settings = {'exclude_account_hold': True}
            drivers = await get_drivers(carrier, driver_criteria, driver_settings)
            if not drivers:
                raise Exception("No drivers found for the given shift or driver tags")
            plan_drivers = [driver.get('_id') for driver in drivers]

        loads, scheduled_plans = await retrieve_loads_for_planning(
            user_payload=user_payload,
            load_criteria=load_criteria,
            plan_from_time=plan_from_time,
            plan_to_time=plan_to_time,
            plan_drivers=plan_drivers,
            add_to_existing_plan=True,
            is_specific_load=True,
            shift_window=planning_minutes,
        )

        mapped_loads = map_loads_for_scheduler(loads)
        mapped_loads = await add_recommended_returns(user_payload, plan_date, mapped_loads)

        actionable_moves, _, _ = await map_loads_for_optimizer(
            user_payload=user_payload,
            loads=mapped_loads,
            scheduled_plans=scheduled_plans,
            converted_plan_date=converted_plan_date,
            plan_range={
                'from_time': plan_from_time,
                'to_time': plan_to_time,
                'shift_from_time':  converted_plan_date + timedelta(minutes=planning_minutes[0]),
                'shift_to_time':  converted_plan_date + timedelta(minutes=planning_minutes[1]),
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
