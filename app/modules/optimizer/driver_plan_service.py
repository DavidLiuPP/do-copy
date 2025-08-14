import logging
import json
from datetime import datetime
from decimal import Decimal
from copy import deepcopy
from app.postgres_connection import PostgresConnection
from app.postgres_services.store_driver_plan import save_summary_of_optimal_plan, save_optimizer_loads
from app.modules.optimizer.sanity_check import run_sanity_check_automation
import asyncio
from fastapi import BackgroundTasks


logger = logging.getLogger(__name__)

async def get_latest_version_for_driver_plan(carrier, converted_plan_date):
    try:
        SQL = """
            SELECT MAX(version) FROM optimizer_plans WHERE carrier = $1 AND plan_date = $2
        """
        
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        
        async with pool.acquire() as conn:
            row = await conn.fetchrow(SQL, carrier, converted_plan_date)
            max_version = row['max'] if row else None
            return 1 if max_version is None else max_version + 1
    except Exception as e:
        logger.error(f"Failed to get latest version for driver plan: {str(e)}")
        raise Exception(f"Failed to get latest version for driver plan: {str(e)}")
    

async def get_actionable_moves_from_db(carrier, converted_plan_date):
    try:
        SQL = """
            WITH max_version AS (
                SELECT 
                    MAX(version) as max_ver 
                FROM 
                    optimizer_plans 
                WHERE 
                    carrier = $1 AND 
                    plan_date = $2
            )
            SELECT 
                opi.* 
            FROM 
                optimizer_plan_inputs opi,
                max_version
            WHERE 
                opi.carrier = $1 AND 
                opi.plan_date = $2 AND 
                opi.version = max_version.max_ver
        """

        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        async with pool.acquire() as conn:
            data = await conn.fetch(SQL, carrier, converted_plan_date)

            mapped_loads = [dict(row) for row in data]


            for load in mapped_loads:
                driver_order = load.get('move', None)

                # Convert datetime objects to ISO format strings
                for key, value in load.items():
                    if isinstance(value, datetime): 
                        load[key] = value.isoformat()
                    if isinstance(value, Decimal):
                        # Convert to float first
                        float_val = float(value)
                        # If it's a whole number, convert to int
                        if float_val.is_integer():
                            load[key] = int(float_val)
                        else:
                            load[key] = float_val

                if driver_order and isinstance(driver_order, str):
                    load['move'] = json.loads(driver_order)
                
            return mapped_loads
    except Exception as e:
        logger.error(f"Failed to get actionable moves from db: {str(e)}")
        raise Exception(f"Failed to get actionable moves from db: {str(e)}")

async def save_plan_details(
    user_payload,
    converted_plan_date,
    optimal_plan,
    unassigned_plans = [],
    plan_branch = [],
    shift = None,
    background_tasks: BackgroundTasks = None,
):
    # Save Summary of optimal_plan to optimizer_plans

    carrier = user_payload.get('carrier')

    latest_version = await get_latest_version_for_driver_plan(carrier, converted_plan_date)

    try:
        summary_of_optimal_plan = await save_summary_of_optimal_plan(
            user_payload=user_payload,
            optimal_plan=optimal_plan,
            latest_version=latest_version,
            converted_plan_date=converted_plan_date,
            plan_branch=plan_branch,
            shift=shift
        )
        plan_id = summary_of_optimal_plan.get('id', None)
    except Exception as e:
        raise Exception(f"Failed to save summary of optimal plan: {str(e)}")

    if not plan_id:
        raise Exception("Failed to save summary of optimal plan")
    
    # Save optimal_plan to optimizer_loads table
    try:
        # merge optimal_plan and unassigned_plans
        merged_optimal_plan = optimal_plan + unassigned_plans
        await save_optimizer_loads(carrier, converted_plan_date, merged_optimal_plan, plan_id, latest_version)
    except Exception as e:
        raise Exception(f"Failed to save optimal plan to optimizer_loads: {str(e)}")

    # Run sanity check as background task
    try:
        if background_tasks:
            background_tasks.add_task(run_sanity_check_automation,
                plan_date=converted_plan_date.strftime('%Y-%m-%d'),
                version=latest_version,
                carrier=carrier
            )
    except Exception as e:
        logger.error(f"Failed to start sanity check background task: {str(e)}")
    
    return plan_id
