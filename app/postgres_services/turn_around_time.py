# app/postgres_services/turn_around_time.py

import pandas as pd
import os
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)

# Global cache for CSV data to avoid reloading on every call
_csv_cache = None

def load_waiting_time_csv(carrier: str):
    """
    Load the waiting time CSV from root directory.
    Uses caching to avoid repeated file reads.
    """
    global _csv_cache
    
    # Return cached data if available
    if _csv_cache is not None:
        return _csv_cache
    
    # Load CSV from root directory
    csv_filename = f'{carrier}_port_wait_time_label.csv'
    csv_path = csv_filename  # File in root directory
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"CSV file '{csv_path}' not found in root directory. "
            f"Please ensure the file is placed in: {os.path.abspath('.')}"
        )
    
    try:
        # Load CSV
        df = pd.read_csv(csv_path)
        
        # Cache the dataframe for quick access
        _csv_cache = df
        
        logger.info(f"Successfully loaded {len(df)} waiting time records from CSV")
        return df
        
    except Exception as e:
        logger.error(f"Error loading waiting time CSV: {e}")
        raise


def clear_cache():
    """
    Clear the cached CSV data. 
    Useful if the CSV file is updated during runtime.
    """
    global _csv_cache
    _csv_cache = None
    logger.info("Waiting time cache cleared")


async def get_turn_around_time(carrier: str, from_id: str, to_id: str) -> int:
    """
    Retrieve turn around time data.
    This function can remain as-is or be modified to use CSV if needed.
    
    Args:
        carrier: Carrier ID to fetch turn around time for
        from_id: Source customer ID
        to_id: Destination customer ID
        
    Returns:
        Turn around time in minutes
    """
    # For now, return default value
    # You can modify this to load from CSV if you have turn around time data there
    return 40


async def get_turn_around_time_for_move(carrier: str, move: List[Dict[str, Any]]) -> int:
    """
    Calculate total turn around time for a move.
    """
    try:
        for indx, event in enumerate(move):
            prev_event = move[indx - 1] if indx > 0 else None
            
            if prev_event is None:
                if event.get('customerId') is not None:
                    move[indx]['turn_around_time'] = await get_turn_around_time(carrier, None, event.get('customerId'))
            else:
                if prev_event is not None and prev_event.get('customerId') is not None and event.get('customerId') is not None:
                    move[indx]['turn_around_time'] = await get_turn_around_time(carrier, prev_event.get('customerId'), event.get('customerId'))

        total_turn_around_time = sum(event.get('turn_around_time', 0) for event in move)
        return total_turn_around_time
    except Exception as e: 
        logger.error(f"Error retrieving turn around time for move: {str(e)}")
        raise


async def get_waiting_time(carrier: str, customer_ids: List[str]) -> Dict[str, Any]:
    """
    Retrieve waiting time data from CSV instead of PostgreSQL.
    
    Args:
        carrier: Carrier ID to fetch waiting time for
        customer_ids: List of customer IDs to fetch waiting time for
        
    Returns:
        Dictionary with key as (customer_id, type, is_liveunload) tuple and 
        value as dict containing waiting_time and hourly_distribution
        
    Raises:
        Exception: If there's an error during CSV operations
    """
    try:
        if not carrier or not customer_ids:
            return {}

        # Load CSV data
        df = load_waiting_time_csv(carrier)
        
        # Filter by carrier and customer_ids
        filtered_df = df[
            (df['carrier'] == carrier) & 
            (df['customer_id'].isin(customer_ids))
        ]
        
        # Create dictionary mapping (customer_id, type, is_liveunload) to waiting_time data
        waiting_time_dict = {}
        
        for _, row in filtered_df.iterrows():
            # Calculate average waiting time for hours 6-12 (matching original logic)
            total = 0
            for hour in range(6, 13):
                hour_col = f"{hour:02d}"
                total += row.get(hour_col, 60)
            avg_wait = int(total / 7)
            
            # Create the hourly distribution dictionary from the row
            hourly_distribution = {}
            for col in row.index:
                # Include all original columns in hourly_distribution
                hourly_distribution[col] = row[col]
            
            # Ensure all key components are hashable (convert to string if needed)
            try:
                customer_id = str(row['customer_id']) if row['customer_id'] is not None else ''
                event_type = str(row['type']) if row['type'] is not None else ''
                is_liveunload = bool(row['is_liveunload']) if row['is_liveunload'] is not None else False
            except (TypeError, ValueError) as e:
                logger.error(f"Error processing waiting time record: {str(e)}")
                continue
            
            # Store in the same format as the original function
            waiting_time_dict[(customer_id, event_type, is_liveunload)] = {
                'waiting_time': avg_wait,
                'hourly_distribution': hourly_distribution.to_dict() if hasattr(hourly_distribution, 'to_dict') else hourly_distribution
            }

        return waiting_time_dict
            
    except Exception as e:
        logger.error(f"Error retrieving waiting times from CSV: {str(e)}")
        raise


def add_waiting_time_to_move(loads: List[Dict[str, Any]], waiting_times: Dict[str, Any], timeZone: str) -> List[Dict[str, Any]]:
    """
    Add waiting time to each event in the moves.
    This function remains the same as it just uses the waiting_time_dict.
    
    Args:
        loads: List of load dictionaries containing moves
        waiting_times: Dictionary from get_waiting_time function
        timeZone: Timezone string
    
    Returns:
        Updated loads with waiting times added
    """
    try:
        for load in loads:
            move = load.get('move', [])

            for indx, event in enumerate(move):
                customer_id = str(event.get('customerId', ''))
                event_type = str(event.get('type', ''))

                # Skip waiting time for consecutive events with the same customerId
                if indx > 0 and customer_id == move[indx - 1].get('customerId'):
                    move[indx]['waiting_time'] = 0
                    continue
                
                # Get waiting time from dictionary
                customer_wait_times = waiting_times.get((customer_id, event_type, False), None)

                # If not found in the dictionary, fallback to default waiting time based on event type
                if customer_wait_times is None:
                    if event_type in ['PULLCONTAINER', 'DELIVERLOAD', 'RETURNCONTAINER']:
                        customer_wait_times = {'waiting_time': 60}
                    else:
                        customer_wait_times = {'waiting_time': 15}

                # Handle live unload case
                if event.get('is_liveunload'):
                    customer_wait_times = waiting_times.get((customer_id, event_type, True), customer_wait_times)

                # Assign waiting time and hourly distribution to the event
                move[indx]['waiting_time'] = customer_wait_times.get('waiting_time', 60)
                move[indx]['waiting_time_distribution'] = customer_wait_times.get('hourly_distribution', {})

            # Calculate total waiting time for the load
            total_waiting_time = sum(event.get('waiting_time', 0) for event in move)
            
            # Update the load with total waiting time and modified moves
            load['move'] = move
            load['waiting_time'] = total_waiting_time
        
        return loads
    except Exception as e:
        logger.error(f"Error adding waiting time to move: {str(e)}")
        raise


# Utility function to get waiting time for a specific hour
def get_waiting_time_for_hour(waiting_time_dict: Dict[str, Any], customer_id: str, event_type: str, 
                              is_liveunload: bool, hour: int) -> int:
    """
    Get waiting time for a specific hour from the waiting_time_dict.
    
    Args:
        waiting_time_dict: Dictionary from get_waiting_time
        customer_id: Customer ID
        event_type: Event type
        is_liveunload: Whether it's live unload
        hour: Hour (0-23)
    
    Returns:
        Waiting time in minutes for that hour
    """
    key = (str(customer_id), str(event_type), bool(is_liveunload))
    
    if key in waiting_time_dict:
        hourly_dist = waiting_time_dict[key].get('hourly_distribution', {})
        hour_col = f"{hour:02d}"
        return hourly_dist.get(hour_col, waiting_time_dict[key].get('waiting_time', 60))
    
    # Default based on event type
    if event_type in ['PULLCONTAINER', 'DELIVERLOAD', 'RETURNCONTAINER']:
        return 60
    return 15


# Example usage for testing
if __name__ == "__main__":
    import asyncio
    
    async def test():
        # Test the get_waiting_time function
        carrier = '60196e05993170084efd0f4d'
        customer_ids = ['6724e91833ff32f44a6d4bb9', '64f732ff69c90816509edd3e']
        
        waiting_time_dict = await get_waiting_time(carrier, customer_ids)
        
        print(f"Retrieved waiting times for {len(waiting_time_dict)} combinations")
        
        # Show sample entry
        for key, value in list(waiting_time_dict.items())[:2]:
            print(f"\nKey: {key}")
            print(f"Waiting time: {value['waiting_time']} minutes")
            print(f"Has hourly distribution: {'hourly_distribution' in value}")
    
    asyncio.run(test())