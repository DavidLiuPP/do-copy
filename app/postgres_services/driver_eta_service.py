import json
import logging
from app.postgres_connection import PostgresConnection

logger = logging.getLogger(__name__)

async def save_driver_eta_details(eta_payload):
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        result = []
        async with pool.acquire() as conn:
            row_values = []
            for value in eta_payload.values():
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

            fields = list(eta_payload.keys())
            query_part = ', '.join([f'"{field}"' for field in fields])
            update_part = ', '.join([
                f'"{field}" = EXCLUDED."{field}"'
                for field in fields 
                if field not in ['carrier', 'driver']
            ])

            SQL = f"""
                INSERT INTO driver_eta_details ({query_part})
                VALUES ({', '.join(row_values)})
                ON CONFLICT (carrier, driver) DO UPDATE
                SET {update_part}
            """
            await conn.execute(SQL)

            get_sql = f"""
                SELECT driver, 
                       current_move_id, 
                       current_move, 
                       next_move_id, 
                       next_move,
                       is_move_feasible,
                       next_move_behind_by 
                FROM driver_eta_details 
                WHERE carrier = '{eta_payload.get('carrier')}' 
                AND driver = '{eta_payload.get('driver')}'
            """
            result = await conn.fetch(get_sql)
            return [dict(row) for row in result]
                
    except Exception as e:
        logger.error(f"Failed to save driver eta details: {str(e)}")
        raise Exception(f"Failed to save driver eta details: {str(e)}")
