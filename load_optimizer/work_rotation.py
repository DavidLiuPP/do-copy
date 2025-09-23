import logging
from datetime import timedelta

from app.postgres_services.load_move_service import get_move_history
from app.modules.optimizer.constants import CARRIER_CONFIGS

logger = logging.getLogger(__name__)

async def get_driver_history(carrier: str, plan_date: str, timeZone: str, vehicle_data: list) -> dict:
    """
    Get driver history for the given carrier and plan date.
    """
    try:
        driver_ids = [v['_id'] for v in vehicle_data]
        driver_owner_score = {v['_id']: (v['owner_score'], v['name']) for v in vehicle_data}
        
        # Get fully aggregated move history from database
        move_history = await get_move_history(carrier, plan_date, driver_ids, timeZone)
        
        driver_history = {}
        
        # Process each driver's aggregated data
        for row in move_history:
            driver_id = row['driver']
            worked_dates_set = set(row['worked_dates']) if row['worked_dates'] else set()
            
            # Initialize driver history with data from query
            driver_history[driver_id] = {
                'days_worked': row['days_worked'],
                'last_worked_date': row['last_worked_date'],
                'consecutive_days': 0,
                'worked_dates': worked_dates_set,  # Temporarily keep for consecutive calculation
                'owner_score': driver_owner_score.get(driver_id, (None, None))[0],
                'name': driver_owner_score.get(driver_id, (None, None))[1],
                'zone_counts': {
                    'local': row['local_count'],
                    'medium': row['medium_count'],
                    'outofzone': row['outofzone_count'],
                    'longhaul': row['longhaul_count'],
                }
            }

        # Calculate metrics for each driver (your original second loop)
        for history in driver_history.values():
            # Calculate consecutive days
            if history['last_worked_date']:
                consecutive = 0
                current_date = (plan_date - timedelta(days=1)).date()  # Start from plan date
                
                # Go backwards checking each weekday
                while current_date in history['worked_dates']:
                    # If they worked this day, increment consecutive count
                    consecutive += 1
                    current_date -= timedelta(days=1)

                history['consecutive_days'] = consecutive
                
            del history['worked_dates']  # Remove temporary field

            # Normalize zone counts to give more weight to longer distance moves
            zone_counts = history['zone_counts']
            history['zone_counts'] = {
                'local': zone_counts['local'] // 4,
                'medium': zone_counts['medium'] // 3, 
                'outofzone': zone_counts['outofzone'] // 2,
                'longhaul': zone_counts['longhaul']
            }

        return driver_history
    
    except Exception as e:
        logger.error(f"Error getting driver history: {str(e)}")
        return {}
