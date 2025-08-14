import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from app.synced_db_connection import get_synced_db_client

logger = logging.getLogger(__name__)

async def get_drivers_worked_yesterday(carrier: str, plan_date: datetime) -> List[str]:
    """
    Get list of driver IDs who worked yesterday based on events table.
    
    Args:
        carrier: Carrier ID
        plan_date: The planning date (we'll query for the day before this)
        
    Returns:
        List of driver IDs who worked yesterday
    """
    try:
        synced_db = get_synced_db_client()
        pool = await synced_db.get_pool()
        
        if isinstance(plan_date, str):
            plan_date = datetime.strptime(plan_date, '%Y-%m-%d')
            
        yesterday = plan_date - timedelta(days=1)
        
        async with pool.acquire() as conn:
            query = """
                SELECT DISTINCT driver 
                FROM events 
                WHERE carrier = $1 
                AND enroute::date = $2
                AND driver IS NOT NULL
            """
            
            rows = await conn.fetch(query, carrier, yesterday.date())
            driver_ids = [row['driver'] for row in rows if row['driver']]
            
            return driver_ids
            
    except Exception as e:
        logger.error(f"Error getting drivers who worked yesterday: {str(e)}")
        return []

async def get_drivers_distance(carrier: str, plan_date: datetime) -> Dict[str, float]:
    """
    Get the distance the driver completed based on events table.
    
    Args:
        carrier: Carrier ID
        plan_date: The planning date (we'll query for the day before this)
        
    Returns:
        Dictionary mapping driver IDs to the distance they completed
    """
    try:
        synced_db = get_synced_db_client()
        pool = await synced_db.get_pool()
        
        if isinstance(plan_date, str):
            plan_date = datetime.strptime(plan_date, '%Y-%m-%d')
            
        yesterday = plan_date - timedelta(days=1)
        
        async with pool.acquire() as conn:
            query = """
                SELECT driver, SUM(distance) as total_distance
                FROM events
                WHERE carrier = $1
                AND enroute::date = $2
                AND driver IS NOT NULL
                GROUP BY driver
            """
            
            rows = await conn.fetch(query, carrier, yesterday.date())
            driver_distances = {}
            for row in rows:
                if row['driver']:
                    driver_distances[row['driver']] = float(row['total_distance'])

            return driver_distances

    except Exception as e:
        logger.error(f"Error getting drivers distance: {str(e)}")
        return {}

async def get_drivers_loads_count(carrier: str, plan_date: datetime) -> Dict[str, int]:
    """
    Get the number of moves each driver completed based on events table.
    
    Args:
        carrier: Carrier ID
        plan_date: The planning date (we'll query for the day before this)
        
    Returns:
        Dictionary mapping driver IDs to the number of moves they completed
    """
    try:
        synced_db = get_synced_db_client()
        pool = await synced_db.get_pool()
        
        if isinstance(plan_date, str):
            plan_date = datetime.strptime(plan_date, '%Y-%m-%d')
            
        yesterday = plan_date - timedelta(days=1)
        
        async with pool.acquire() as conn:
            query = """
                SELECT driver, COUNT(DISTINCT reference_number) AS load_count
                FROM events
                WHERE carrier = $1
                AND enroute::date = $2
                AND driver IS NOT NULL
                GROUP BY driver
            """
            
            rows = await conn.fetch(query, carrier, yesterday.date())
            driver_moves = {}
            for row in rows:
                if row['driver']:
                    driver_moves[row['driver']] = int(row['load_count'])

            return driver_moves

    except Exception as e:
        logger.error(f"Error getting drivers loads count: {str(e)}")
        return {}

def create_sorting_key(rotation_order: List[str]):
    """
    Create a sorting key function based on the rotation order.
    
    Args:
        rotation_order: List of sorting criteria in order of priority
        
    Returns:
        Function that returns a tuple for sorting
    """
    try:
        def sorting_key(driver: Dict[str, Any]) -> tuple:
            key_parts = []

            for criterion in rotation_order:
                if criterion == 'worked_yesterday':
                    # Lower values (0) get higher priority
                    key_parts.append(driver.get('worked_yesterday', 0))
                elif criterion == 'distance':
                    # Lower distances get higher priority
                    key_parts.append(driver.get('distance', 0))
                elif criterion == 'num_loads':
                    # Lower number of moves get higher priority
                    key_parts.append(driver.get('num_loads', 0))

                elif criterion == 'owner_score':
                    # Higher owner scores get higher priority (negative for reverse sort)
                    key_parts.append(-driver.get('owner_score', 0))
                elif criterion == 'name':
                    # Sort by driver name (alphabetical order)
                    driver_name = driver.get('name', '')
                    if driver_name is None:
                        driver_name = ''
                    key_parts.append(driver_name.lower())
                else:
                    # Add criterion as is
                    key_parts.append(criterion)

            return tuple(key_parts)

        return sorting_key
    except Exception as e:
        logger.error(f"Error creating sorting key function: {str(e)}")
        # Return a default sorting function that just returns an empty tuple
        return lambda x: tuple()


    
async def apply_driver_rotation_sorting(
    drivers: List[Dict[str, Any]], 
    carrier: str, 
    plan_date: datetime,
    rotation_order: List[str] = ['owner_score']
) -> List[Dict[str, Any]]:
    """
    Apply driver rotation sorting based on configurable rotation order.
    
    Args:
        drivers: List of driver dictionaries
        carrier: Carrier ID
        plan_date: Planning date
        rotation_order: List of sorting criteria in order of priority
        names_order: List of driver names in priority order (only needed if 'name' is in rotation_order)
        
    Returns:
        Sorted list of drivers with additional fields added
    """
    try:       
        drivers_worked_yesterday = await get_drivers_worked_yesterday(carrier, plan_date)
        drivers_distance = await get_drivers_distance(carrier, plan_date)
        drivers_loads = await get_drivers_loads_count(carrier, plan_date)

        for driver in drivers:
            driver['worked_yesterday'] = 1 if driver.get('_id') in drivers_worked_yesterday else 0
            driver['distance'] = drivers_distance.get(driver.get('_id'), 0)
            driver['num_loads'] = drivers_loads.get(driver.get('_id'), 0)
            # Round distance to nearest 100
            driver['distance'] = round(driver['distance'] / 100) * 100

        sorting_key = create_sorting_key(rotation_order)
        sorted_drivers = sorted(drivers, key=sorting_key)
        return sorted_drivers
        
    except Exception as e:
        logger.error(f"Error applying driver rotation sorting: {str(e)}")
        return drivers 