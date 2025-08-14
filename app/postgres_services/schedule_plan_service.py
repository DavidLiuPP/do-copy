import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from app.postgres_connection import PostgresConnection
from app.services.load_change_event_service import set_load_review_change_for_schedule_plan

logger = logging.getLogger(__name__)

# Convert times to timestamps
def get_timestamp(dt):
    if not dt:
        return None
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    return dt.timestamp()


async def store_scheduled_plan(planned_moves: List[Dict[str, Any]]) -> None:
    """
    Store scheduled plan data into PostgreSQL.
    
    Args:
        planned_moves: List of dictionaries containing the scheduled plan data
    
    Raises:
        Exception: If there's an error during database operations
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        # if data is already present for plan_date, return
        carrier = planned_moves[0].get('carrier')
        plan_date = planned_moves[0].get('plan_date')

        async with pool.acquire() as conn:
            # store plan summary
            summary_query = """
                INSERT INTO schedule_summary_v2 (plan_date, carrier)
                VALUES ($1, $2)
                RETURNING id
            """
            result = await conn.fetchrow(summary_query, datetime.fromisoformat(plan_date), carrier)
            plan_id = result.get('id')

            all_fields = ['plan_id'] + list(planned_moves[0].keys())
            bulk_insert_values = []

            for move in planned_moves:
                row_values = []
                for field in all_fields:
                    value = move.get(field, None)
                    
                    if field == 'plan_id':
                        value = plan_id
                    
                    if value is None:
                        formatted_value = 'NULL'
                    elif isinstance(value, (bool, int, float)):
                        formatted_value = str(value)
                    elif isinstance(value, (dict, list)):
                        json_string = json.dumps(value, separators=(',', ':'))
                        escaped_string = json_string.replace("'", "''") 
                        formatted_value = f"'{escaped_string}'"
                    else:
                        escaped_string = str(value).replace("'", "''") 
                        formatted_value = f"'{escaped_string}'"
                    row_values.append(formatted_value)
                
                bulk_insert_values.append(f"({', '.join(row_values)})")

            if bulk_insert_values:
                query_part = ', '.join([f'"{field}"' for field in all_fields])
                bulk_insert_query = f"""
                    INSERT INTO schedule_plan_v2 ({query_part})
                    VALUES {', '.join(bulk_insert_values)}
                """
                await conn.execute(bulk_insert_query)

            if plan_id:
                await set_load_review_change_for_schedule_plan(carrier, plan_id)
            
            logger.info(f"Successfully stored {len(planned_moves)} planned moves in schedule_plan_v2 table.")
    except Exception as e:
        logger.error(f"Error storing scheduled plan: {str(e)}")
        raise


async def store_updated_plan_for_load(schedule_summary: Dict[str, Any], reference_number: str, planned_moves: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Store updated plan for a specific load in the schedule_plan_v2 table.
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        carrier = schedule_summary.get('carrier')
        plan_id = schedule_summary.get('id')

        async with pool.acquire() as conn:
            remove_query = """
                DELETE FROM schedule_plan_v2
                WHERE carrier = $1 AND plan_id = $3 AND reference_number = $2 
            """
            await conn.execute(remove_query, carrier, reference_number, str(plan_id))
            result = []
            if len(planned_moves) > 0:
                # store new entries
                all_fields = ['plan_id'] + list(planned_moves[0].keys())
                bulk_insert_values = []

                for move in planned_moves:
                    row_values = []

                    for field in all_fields:
                        value = move.get(field, None)
                        
                        if field == 'plan_id':
                            value = plan_id
                        
                        if value is None:
                            formatted_value = 'NULL'
                        elif isinstance(value, (bool, int, float)):
                            formatted_value = str(value)
                        elif isinstance(value, (dict, list)):
                            json_string = json.dumps(value, separators=(',', ':'))
                            escaped_string = json_string.replace("'", "''") 
                            formatted_value = f"'{escaped_string}'"
                        else:
                            escaped_string = str(value).replace("'", "''") 
                            formatted_value = f"'{escaped_string}'"
                        row_values.append(formatted_value)
                    
                    bulk_insert_values.append(f"({', '.join(row_values)})")

                if bulk_insert_values:
                    query_part = ', '.join([f'"{field}"' for field in all_fields])
                    bulk_insert_query = f"""
                        INSERT INTO schedule_plan_v2 ({query_part})
                        VALUES {', '.join(bulk_insert_values)}
                        RETURNING reference_number, carrier, plan_date, recommended_appointment_from, scheduled_appointment_from, profile_type, appointment_status, move_id
                    """

                    result = await conn.fetch(bulk_insert_query)
            return [dict(row) for row in result]

    except Exception as e:
        logger.error(f"Error storing scheduled plan: {str(e)}")
        raise


async def get_schedule_summary(carrier: str) -> Dict[str, Any]:
    """
    Retrieve schedule summary data from PostgreSQL for a given carrier.
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        async with pool.acquire() as conn:
            query = """
                SELECT * FROM schedule_summary_v2
                WHERE carrier = $1
                ORDER BY created_at DESC
                LIMIT 1
            """
            result = await conn.fetchrow(query, carrier)
            
            return result
    except Exception as e:
        logger.error(f"Error retrieving schedule summary: {str(e)}")
        raise

async def get_scheduled_plan(
    carrier: str,
    plan_date: str,
    reference_numbers: list = [],
    plan_from_time = None,
    plan_to_time = None,
    scheduled_moves_only: bool = False
) -> List[Dict[str, Any]]:
    """
    Retrieve scheduled plan data from PostgreSQL for a given carrier and plan date.
    
    Args:
        carrier: Carrier ID to fetch plan for
        plan_date: Date to fetch plan for in format YYYY-MM-DD
        
    Returns:
        List of dictionaries containing the scheduled plan data
        
    Raises:
        Exception: If there's an error during database operations
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        
        async with pool.acquire() as conn:
            # fetch plan summary
            schedule_summary = await get_schedule_summary(carrier)
            plan_id = schedule_summary.get('id')

            # fetch plan details
            rows = []
            planned_moves = []
            conditions = []
            params = [carrier, str(plan_id)]

            if scheduled_moves_only and plan_from_time and plan_to_time:
                conditions.append(f"""AND ( 
                    (scheduled_appointment_from >= ${len(params) + 1} AND scheduled_appointment_from <= ${len(params) + 2}) OR 
                    (scheduled_appointment_to >= ${len(params) + 1} AND scheduled_appointment_to <= ${len(params) + 2})
                )""")
                params.append(plan_from_time)
                params.append(plan_to_time)
            else:
                conditions.append(f'AND plan_date::date = ${len(params) + 1}::date')
                params.append(plan_date)
            
            if reference_numbers:
                conditions.append(f'AND reference_number = ANY(${len(params) + 1})')
                params.append(reference_numbers)
            
            query = """
                SELECT * FROM schedule_plan_v2
                WHERE carrier = $1 AND plan_id = $2
                {}
                ORDER BY "createdAt" DESC
            """.format(' '.join(conditions))
            
            rows = await conn.fetch(query, *params)
            planned_moves = [dict(row) for row in rows]

            # fetch other empties
            if len(planned_moves) > 0:
                reference_numbers = [move.get('reference_number') for move in planned_moves]
                reference_numbers_str = ','.join(f"'{ref}'" for ref in reference_numbers)
                
                other_empties_query = f"""
                    SELECT *
                    FROM schedule_plan_v2 sp
                    WHERE carrier = $1 
                    AND plan_id = $2
                    AND profile_type = 'RETURNCONTAINER'
                    AND type_of_load = 'IMPORT'
                    AND reference_number NOT IN ({reference_numbers_str})
                    AND scheduled_appointment_from IS NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM schedule_plan_v2 sp2 
                        WHERE sp2.carrier = sp.carrier
                            AND sp2.plan_id = sp.plan_id
                            AND sp2.reference_number = sp.reference_number
                            AND sp2.profile_type != 'RETURNCONTAINER'
                    );
                """
                other_empties = await conn.fetch(other_empties_query, carrier, str(plan_id))
                other_empties_plans = [dict(row) for row in other_empties]
                planned_moves = [*planned_moves, *other_empties_plans]

            return planned_moves
            
    except Exception as e:
        logger.error(f"Error retrieving scheduled plans from database: {str(e)}")
        raise


async def get_load_scheduled_plan(carrier: str, reference_number: str, plan_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve load scheduled plan data from PostgreSQL for a given carrier and plan id, reference_number.
    
    Args:
        carrier: Carrier ID to fetch plan for
        reference_number: Load reference number
        plan_id: Id of plan for fetch plan details
        
    Returns:
        List of dictionaries containing the scheduled plan data
        
    Raises:
        Exception: If there's an error during database operations
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        
        async with pool.acquire() as conn:
            # fetch plan details
            query = """
                SELECT plan_id, carrier, appointment_status, scheduled_appointment_from, scheduled_appointment_to, recommended_appointment_from, recommended_appointment_to, profile_type, plan_date 
                FROM schedule_plan_v2
                WHERE carrier = $1 AND plan_id = $2 AND reference_number=$3
            """
            
            rows = await conn.fetch(query, carrier, str(plan_id), reference_number)
            plans = [dict(row) for row in rows]
            return plans
            
    except Exception as e:
        logger.error(f"Error retrieving load scheduled plans from database: {str(e)}")
        return []
