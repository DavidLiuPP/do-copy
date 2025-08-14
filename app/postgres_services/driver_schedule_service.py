import json
import logging
from datetime import datetime
from app.postgres_connection import PostgresConnection

logger = logging.getLogger(__name__)

async def store_driver_schedule(mapped_schedule):
    """
    Store driver schedule data into a PostgreSQL table named driver_schedule.
    
    Args:
        mapped_schedule: dictionary containing the mapped schedule.
        
    Raises:
        Exception: If there's an error during database operations.
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        
        async with pool.acquire() as conn:
            carrier = mapped_schedule.get('carrier')
            plan_date = mapped_schedule.get('plan_date')
            driver_schedule = mapped_schedule.get('driver_schedule')

            # Convert datetime fields to isoformat for each driver's schedule
            for driver_id, events in driver_schedule.items():
                for event in events:
                    if 'enroute_time' in event:
                        event['enroute_time'] = event['enroute_time'].isoformat()
                    if 'completion_time' in event:
                        event['completion_time'] = event['completion_time'].isoformat()

            upsert_query = f"""
                INSERT INTO driver_schedule (carrier, plan_date, driver_schedule)
                VALUES ($1, $2, $3)
                ON CONFLICT (carrier, plan_date) 
                DO UPDATE SET
                    driver_schedule = $3,
                    updated_at = NOW()
            """
            await conn.execute(upsert_query, carrier, plan_date, json.dumps(driver_schedule))
                    
    except Exception as e:
        logger.error(f"Error storing driver schedule: {str(e)}")
        raise


async def get_driver_schedule(carrier: str, plan_date: str) -> dict:
    """
    Retrieve driver schedule data from the database.
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        async with pool.acquire() as conn:
            query = """
                SELECT driver_schedule
                FROM driver_schedule
                WHERE carrier = $1 AND plan_date = $2
            """

            row = await conn.fetchrow(query, carrier, plan_date)
            if not row:
                return {}
            
            driver_schedule = json.loads(row.get('driver_schedule'))
            
            # Convert isoformat datetime strings back to datetime objects
            for driver_id, events in driver_schedule.items():
                for event in events:
                    if 'enroute_time' in event:
                        event['enroute_time'] = datetime.fromisoformat(event['enroute_time'])
                    if 'completion_time' in event:
                        event['completion_time'] = datetime.fromisoformat(event['completion_time'])
            
            return driver_schedule

    except Exception as e:
        logger.error(f"Error getting driver schedule: {str(e)}")
        raise


async def store_driver_schedule_v3(mapped_schedule):
    """
    Store driver schedule data into a PostgreSQL table named driver_schedule.
    
    Args:
        mapped_schedule: dictionary containing the mapped schedule.
        
    Raises:
        Exception: If there's an error during database operations.
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        
        async with pool.acquire() as conn:
            carrier = mapped_schedule.get('carrier')
            plan_date = mapped_schedule.get('plan_date')
            driver_schedule = mapped_schedule.get('driver_schedule')

            upsert_query = f"""
                INSERT INTO driver_schedule_v3 (carrier, plan_date, driver_schedule)
                VALUES ($1, $2, $3)
                ON CONFLICT (carrier, plan_date) 
                DO UPDATE SET
                    driver_schedule = $3,
                    updated_at = NOW()
            """
            await conn.execute(upsert_query, carrier, plan_date, json.dumps(driver_schedule))
                    
    except Exception as e:
        logger.error(f"Error storing driver schedule: {str(e)}")
        raise

async def get_driver_schedule_v3(carrier: str, plan_date: str) -> dict:
    """
    Retrieve driver schedule data from the database.
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        async with pool.acquire() as conn:
            query = """
                SELECT driver_schedule
                FROM driver_schedule_v3
                WHERE carrier = $1 AND plan_date = $2
            """

            row = await conn.fetchrow(query, carrier, plan_date)
            if not row:
                return {}
            
            driver_schedule = json.loads(row.get('driver_schedule'))
            
            # Convert isoformat datetime strings back to datetime objects
            for driver_id, events in driver_schedule.items():
                for event in events:
                    if 'enroute_time' in event:
                        event['enroute_time'] = datetime.fromisoformat(event['enroute_time'])
                    if 'completion_time' in event:
                        event['completion_time'] = datetime.fromisoformat(event['completion_time'])
            
            return driver_schedule

    except Exception as e:
        logger.error(f"Error getting driver schedule: {str(e)}")
        raise