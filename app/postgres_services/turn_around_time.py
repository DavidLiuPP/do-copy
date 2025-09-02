import logging
from typing import Dict, Any, List
from app.synced_db_connection import PostgresConnection

logger = logging.getLogger(__name__)

async def get_waiting_time(carrier: str, customer_ids: List[str]) -> Dict[str, Any]:
    """
    Retrieve waiting time data from PostgreSQL for a given carrier and customer IDs.
    
    Args:
        carrier: Carrier ID to fetch waiting time for
        customer_ids: List of customer IDs to fetch waiting time for
        
    Returns:
        Dictionary containing the waiting time data
        
    Raises:
        Exception: If there's an error during database operations
    """
    try:
        if not carrier or not customer_ids:
            return {}

        postgres = PostgresConnection()
        pool = await postgres.get_pool()
        
        async with pool.acquire() as conn:
            # Build query with proper parameterization
            placeholders = [f'${i+2}' for i in range(len(customer_ids))]
            placeholder_str = ','.join(placeholders)
            
            query = f"""
                SELECT * FROM waiting_time 
                WHERE carrier = $1 AND customer_id IN ({placeholder_str})
            """

            # Pass all parameters as a list
            params = [carrier] + customer_ids
            rows = await conn.fetch(query, *params)
            waiting_times = [dict(row) for row in rows]
            
            # Create dictionary mapping customer_id to waiting_time
            waiting_time_dict = {}
            for wt in waiting_times:
                # Calculate average waiting time for hours 6-12
                total = 0
                for hour in range(6, 13):
                    total += wt.get(f"{hour:02d}", 60)
                avg_wait = int(total / 7)
                
                # Ensure all key components are hashable (convert to string if needed)
                customer_id = str(wt['customer_id']) if wt['customer_id'] is not None else ''
                event_type = str(wt['type']) if wt['type'] is not None else ''
                is_liveunload = bool(wt['is_liveunload']) if wt['is_liveunload'] is not None else False
                
                waiting_time_dict[(customer_id, event_type, is_liveunload)] = {
                    'waiting_time': avg_wait,
                    'hourly_distribution': wt
                }

            return waiting_time_dict
            
    except Exception as e:
        logger.error(f"Error retrieving waiting times from database: {str(e)}")
        raise

def add_waiting_time_to_move(loads: Dict[str, Any], waiting_times: Dict[str, Any], timeZone: str) -> Dict[str, Any]:
    try:
        for load in loads:
            move = load.get('move')

            for indx, event in enumerate(move):
                customer_id = event.get('customerId')
                event_type = event.get('type')

                if indx > 0 and customer_id == move[indx - 1].get('customerId'):
                    move[indx]['waiting_time'] = 0
                    continue

                customer_wait_times = waiting_times.get((customer_id, event_type, False), {'waiting_time': 60})
                if event.get('is_liveunload'):
                    customer_wait_times = waiting_times.get((customer_id, event_type, True), customer_wait_times)
                
                move[indx]['waiting_time'] = customer_wait_times.get('waiting_time', 60)
                move[indx]['waiting_time_distribution'] = customer_wait_times.get('hourly_distribution', {})

            total_waiting_time = sum(event.get('waiting_time', 0) for event in move)
            load['move'] = move
            load['waiting_time'] = total_waiting_time
        
        return loads
    except Exception as e:
        logger.error(f"Error adding waiting time to move: {str(e)}")
        raise