import logging
import pytz
import json

from datetime import datetime
from typing import Dict, Any, List
from app.synced_db_connection import get_synced_db_client

logger = logging.getLogger(__name__)

async def get_office_hours(carrier: str, customer_ids: str, timeZone: str) -> List[Dict[str, Any]]:
    """
    Retrieve office hours data from PostgreSQL for a given carrier and customer IDs.
    
    Args:
        carrier: Carrier ID to fetch office hours for
        customer_ids: List of customer IDs to fetch office hours for
        
    Returns:
        List of dictionaries containing the office hours data
        
    Raises:
        Exception: If there's an error during database operations
    """
    try:
        if not carrier or not customer_ids:
            return []

        synced_db = get_synced_db_client()
        pool = await synced_db.get_pool()
        
        async with pool.acquire() as conn:
            query = """
                SELECT
                    "_id",
                    company_name
                FROM customers
                WHERE carrier = $1
                    AND _id = ANY($2)
            """
            
            rows = await conn.fetch(query, carrier, customer_ids)
            
            locations = [dict(row) for row in rows]


            with open('app/data/appointment_times.json', 'r') as f:
                APPOINTMENT_TIMES = json.load(f)


            location_office_hours = []

            for location in locations:
                port_code = location['company_name']
                day_of_week = datetime.now(pytz.timezone(timeZone)).strftime('%A')

                port_appt_data = next(
                    (port for port in APPOINTMENT_TIMES 
                     if port['portCode'].upper() == port_code.upper() 
                     and day_of_week in port['day_of_week']),
                    {}
                )

                if port_appt_data:

                    freeflow_appt = port_appt_data.get('freeflow_appt')
                    mandatory_appt = port_appt_data.get('mandatory_appt')

                    if freeflow_appt:
                        office_hours_start = datetime.strptime(freeflow_appt["start_time"], "%H:%M").time()
                        office_hours_end = datetime.strptime(freeflow_appt["end_time"], "%H:%M").time()
                    elif mandatory_appt:
                        office_hours_start = datetime.strptime(mandatory_appt["start_time"], "%H:%M").time()
                        office_hours_end = datetime.strptime(mandatory_appt["end_time"], "%H:%M").time()
                    else:
                        continue

                    data = {
                        '_id': location['_id'],
                        'office_hours_start': office_hours_start,
                        'office_hours_end': office_hours_end
                    }

                    if port_appt_data.get('grace_period_before_appt'):
                        data['grace_period_before_appt'] = port_appt_data.get('grace_period_before_appt')
                    if port_appt_data.get('grace_period_after_appt'):
                        data['grace_period_after_appt'] = port_appt_data.get('grace_period_after_appt')

                    location_office_hours.append(data)

            return location_office_hours
            
    except Exception as e:
        logger.error(f"Error retrieving office hours from database: {str(e)}")
        raise
