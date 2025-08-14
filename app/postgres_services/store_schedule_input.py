import json
import logging
from app.postgres_connection import PostgresConnection

logger = logging.getLogger(__name__)

LOAD_PROJECTION = {
    "_id": 0,
    "reference_number": 1, 
    "status": 1,
    "carrier": 1,
    "type_of_load": 1,
    "availableDate": 1,
    "callerName": 1,
    "shipperName": 1,
    "consigneeName": 1,
    "emptyOriginName": 1,
    "hazmat": 1,
    "hot": 1,
    "liquor": 1,
    "lastFreeDay": 1,
    "containerAvailableDay": 1,
    "emptyDay": 1,
    "pickupTimes": 1,
    "deliveryTimes": 1,
    "returnFromTime": 1,
    "returnToTime": 1,
    "dischargedDate": 1,
    "cutOff": 1,
    'containerAvailableDay': 1,
    "freeReturnDate": 1,
    "driverOrder": 1,
    "driverOrderId": 1,
    "totalMiles": 1,
    "containerSizeName": 1,
    "totalWeight": 1,
    "outgateDate": 1,
    "chassisPickName": 1,
    "consigneeAddress": 1,
    "containerTypeName": 1, 
    "containerOwnerName": 1,
    "caller": 1,
    "freight": 1,
    "distance": 1,
    "isLive": 1,
    "isReUse": 1,
    "isReadyForPickup": 1,
    "isHot": 1,
    "overWeight": 1, 
    "allowDriverCompletion": 1, 
    "isLastFreeDay": 1,
    "vessel": 1,
    "pickupFromTime": 1,
    "pickupToTime": 1,
    "deliveryFromTime": 1,
    "deliveryToTime": 1,
    "revenue": 1,
    "appointmentNo": 1,
}

async def store_mapped_loads_in_db(mapped_loads, planDetails):
    """
    Store mapped loads data into a PostgreSQL table named schedule_input_v2.
    
    Args:
        mapped_loads: List of dictionaries containing mapped load data.
        planDetails: Dictionary containing plan details such as 'plan_date'.
        
    Raises:
        Exception: If there's an error during database operations.
    """
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        
        async with pool.acquire() as conn:
            filtered_mapped_loads = [
                {key: value for key, value in mapped_load.items() if key in LOAD_PROJECTION}
                for mapped_load in mapped_loads
            ]

            carrier = planDetails.get('carrier')
            plan_date = planDetails.get('plan_date')

            all_fields = set()
            for mapped_load in filtered_mapped_loads:
                skip_fields = ['_id']
                for field in skip_fields:
                    if field in mapped_load:
                        del mapped_load[field]
                all_fields.update(mapped_load.keys())
            all_fields.update(["plan_date", "carrier"])
            all_fields = list(all_fields) 

            bulk_insert_values = []
            for mapped_load in filtered_mapped_loads:
                try:
                    mapped_load['plan_date'] = plan_date
                    mapped_load['carrier'] = carrier

                    row_values = []
                    for field in all_fields:
                        value = mapped_load.get(field, None)
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
                except Exception as insert_error:
                    logger.error(f"Failed to prepare mapped load for bulk insert: {mapped_load}. Error: {insert_error}")

            if bulk_insert_values:
                # Construct the SQL query for bulk insert
                query_part = ', '.join([f'"{field}"' for field in all_fields])  # Use all_fields
                bulk_insert_query = f"""
                    INSERT INTO schedule_input_v2 ({query_part})
                    VALUES {', '.join(bulk_insert_values)}
                    ON CONFLICT (reference_number, carrier, plan_date) 
                    DO UPDATE SET {', '.join([f'"{field}" = EXCLUDED."{field}"' for field in all_fields])}
                """
                await conn.execute(bulk_insert_query)
    except Exception as e:
        logger.error(f"Error initializing database operation: {str(e)}")


async def get_latest_input_by_referenceNo_from_db(carrier: str, plan_date: str, reference_numbers):
    """
    Retrieve the latest data of the schedule input data for a given carrier and plan date.
    
    Args:
        carrier: Carrier ID to fetch the latest Input for
        plan_date: Date to fetch the latest Input for in format YYYY-MM-DD
        reference_numbers: List of reference numbers to filter on
        
    Returns:
        Latest Input row or None if not found
        
    Raises:
        Exception: If there's an error during database operations
    """
    try:
        # Initialize database connection
        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        
        async with pool.acquire() as conn:
            # Log the inputs for debugging
            logger.debug(f"Parameters - carrier: {carrier}, plan_date: {plan_date}, reference_numbers: {reference_numbers}")
            
            # Check if reference_numbers is an empty list
            if not reference_numbers:
                logger.warning("No reference numbers provided")
                return None
            
            # Build the placeholders for the reference numbers dynamically
            placeholders = ', '.join([f'${i+3}' for i in range(len(reference_numbers))])
            query = f"""
                SELECT *
                FROM schedule_input_v2 
                WHERE carrier = $1 AND plan_date = $2 AND reference_number IN ({placeholders})
                ORDER BY reference_number DESC
            """
            
            # Log the final query for debugging
            logger.debug(f"Executing query: {query} with parameters: {carrier}, {plan_date}, {reference_numbers}")
            
            # Execute the query and fetch the latest row
            row = await conn.fetch(query, carrier, plan_date, *reference_numbers)
            
            if row:
                input = [dict(rows) for rows in row]
                return input
            else:
                return None

    except Exception as e:
        logger.error(f"Error retrieving latest input from database: {str(e)}")
        raise

