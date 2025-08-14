from app.postgres_connection import PostgresConnection
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)


async def save_summary_of_optimal_plan(
    user_payload,
    optimal_plan,
    latest_version,
    converted_plan_date,
    plan_branch = [],
    shift = None
):
    # Define column order as a constant
    # Define fields for optimizer plans table

    carrier = user_payload.get('carrier')
    user_id = user_payload.get('userId')

    unique_drivers = set()
    total_miles = 0
    minimum_arrival_time = None
    maximum_completion_time = None
    total_revenue = 0
    total_driver_pay = 0

    for move in optimal_plan:
        if move.get('assigned_driver') is not None:
            unique_drivers.add(move.get('assigned_driver'))
        total_miles += move.get('distance', 0)
        arrival_time = move.get('arrival_time')
        completion_time = move.get('completion_time')
        if minimum_arrival_time is None or arrival_time < minimum_arrival_time:
            minimum_arrival_time = arrival_time
        if maximum_completion_time is None or completion_time > maximum_completion_time:
            maximum_completion_time = completion_time
        total_revenue += move.get('revenue', 0)
        total_driver_pay += move.get('driver_pay', 0)


    # Build row values list
    optimal_plan_values = {
        "carrier": carrier,
        "user_id": user_id,
        "version": latest_version,
        "plan_date": converted_plan_date.strftime('%Y-%m-%d'),
        "no_of_driver": len(unique_drivers),
        "no_of_moves": len(optimal_plan),
        "start_time": minimum_arrival_time,
        "end_time": maximum_completion_time,
        "total_miles": total_miles,
        "empty_miles": 0, # TODO: Implement empty miles
        "revenue": total_revenue,
        "expenses": 0, # TODO: Implement expenses
        "driver_pay": total_driver_pay,
        "unscheduled_moves": 0, # TODO: Implement unscheduled moves
        "branch": '{' + ','.join(plan_branch) + '}',  # Format as Postgres array literal
        "shift": shift
    }

    all_fields = optimal_plan_values.keys()

    row_values = []
    for field in all_fields:
        value = optimal_plan_values[field]
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

    # Construct SQL query
    query_part = ', '.join([f'"{field}"' for field in all_fields])
    
    SQL = f"""
        INSERT INTO optimizer_plans ({query_part})
        VALUES ({', '.join(row_values)})
        RETURNING id;
    """

    postgres = PostgresConnection()
    pool = await postgres.get_pool()

    async with pool.acquire() as conn:
        # Execute insert
        row = await conn.fetchrow(SQL)
        
        return {'id': row['id']}

async def save_optimizer_plan_inputs(carrier, converted_plan_date, actionable_moves, plan_id, latest_version):
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        general_values = {
            'carrier': carrier,
            'optimiser_plan_id': plan_id,
            'version': latest_version,
            'plan_date': converted_plan_date.strftime('%Y-%m-%d')
        }

        async with pool.acquire() as conn:
            bulk_insert_values = []

            for move in actionable_moves:
                row_values = []
                move_values = {
                    **general_values,
                    '_id': move.get('_id'),
                    'move': move.get('move'),
                    'waiting_time': move.get('waiting_time'),
                    'reference_number': move.get('reference_number'),
                    'callerName': move.get('callerName'),
                    'revenue': move.get('revenue'),
                    'act_pickup_appt': move.get('act_pickup_appt'),
                    'act_delivery_appt': move.get('act_delivery_appt'),
                    'act_return_appt': move.get('act_return_appt'),
                    'appointment_from': move.get('appointment_from'),
                    'appointment_to': move.get('appointment_to')
                }

                for value in move_values.values():
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
                fields = list(move_values.keys())
                query_part = ', '.join([f'"{field}"' for field in fields])
                
                SQL = f"""
                    INSERT INTO optimizer_plan_inputs ({query_part})
                    VALUES {', '.join(bulk_insert_values)}
                """
                await conn.execute(SQL)
                
    except Exception as e:
        logger.error(f"Failed to save optimizer plan inputs: {str(e)}")
        raise Exception(f"Failed to save optimizer plan inputs: {str(e)}")



async def save_optimizer_loads(carrier, converted_plan_date, optimal_plan, plan_id, latest_version):
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        async with pool.acquire() as conn:
            bulk_insert_values = []
            
            for move in optimal_plan:
                row_values = []
                if move.get('move')[0].get('combineTripId') is not None:
                    continue
                move_id = next(m.get('moveId', None) for m in move["move"])
                move_values = {
                    'plan_id': plan_id,
                    'carrier': carrier,
                    'reference_number': move.get('reference_number'),
                    'driver': move.get('assigned_driver'),
                    'move_id': move_id,
                    'load_id': move.get('load_id'),
                    'move': move.get('move'),
                    'pickup_time': move.get('pickup_time'),
                    'delivery_time': move.get('delivery_time'), 
                    'return_time': move.get('return_time'),
                    'load_assigned_date': move.get('enroute_time'),
                    'move_complete_time': move.get('completion_time'),
                    'move_start_time': move.get('arrival_time'),
                    'caller': move.get('customer'),
                    'plan_date': converted_plan_date.strftime('%Y-%m-%d'),
                    'version': latest_version,
                    'distance': move.get('distance', 0),
                    'driver_pay': move.get('driver_pay', 0),
                    'revenue': move.get('revenue', 0),
                    'expense': move.get('expense', 0),
                    'is_manual': False,
                    'is_modified_move': move.get('is_modified_move', False),
                    'is_assigned_move': move.get('is_assigned_move', False),
                    'is_deleted': move.get('is_deleted', False),
                    'driver_index': move.get('driver_index', None),
                    'is_new_move': move.get('is_new_move', True),
                    'is_move_affected': move.get('is_move_affected', False),
                    'is_free_flow_move': move.get('is_free_flow_move', False),
                    'reason': move.get('reason', ''),
                    'chassis_pick_event': move.get('chassis_pick_event', None),
                    'chassis_termination_event': move.get('chassis_termination_event', None)
                }

                for value in move_values.values():
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
                fields = list(move_values.keys())
                query_part = ', '.join([f'"{field}"' for field in fields])
                
                SQL = f"""
                    INSERT INTO optimizer_loads ({query_part})
                    VALUES {', '.join(bulk_insert_values)}
                """
                await conn.execute(SQL)

    except Exception as e:
        logger.error(f"Failed to save optimizer loads: {str(e)}")
        raise Exception(f"Failed to save optimizer loads: {str(e)}")
    

async def save_optimizer_inputs_loads(carrier, loads, plan_id, converted_plan_date, shift, plan_branch):
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        general_values = {
            'carrier': carrier,
            'optimiser_plan_id': plan_id,
            'plan_date': converted_plan_date.strftime('%Y-%m-%d'),
            'shift': shift,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        async with pool.acquire() as conn:
            bulk_insert_values = []

            for load in loads:
                row_values = []
                load_values = {
                    **general_values,
                    'load_id': load.get('_id'),
                    'reference_number': load.get('reference_number'),
                    'callername': load.get('callerName'),
                    'move': load.get('driverOrder'),
                }

                for value in load_values.values():
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
                fields = list(load_values.keys())
                query_part = ', '.join([f'"{field}"' for field in fields])
                
                SQL = f"""
                    INSERT INTO optimizer_inputs_loads ({query_part})
                    VALUES {', '.join(bulk_insert_values)}
                """
                await conn.execute(SQL)
                
    except Exception as e:
        logger.error(f"Failed to save optimizer inputs loads: {str(e)}")
        raise Exception(f"Failed to save optimizer inputs loads: {str(e)}")



