import logging
from typing import Dict, Any, List
from app.synced_db_connection import get_synced_db_client
from app.services.redis_service import get_default_yard_location
import json

logger = logging.getLogger(__name__)

async def get_drivers(
    carrier: str,
    plan_date: str,
    plan_branch: list = [],
    settings: Dict[str, Any] = {'exclude_account_hold': True}
) -> List[Dict[str, Any]]:
    """
    Retrieve driver data from PostgreSQL for a given carrier and driver ID.
    
    Args:
        carrier: Carrier ID to fetch driver for
        plan_date: Plan date to check driver hold status
        plan_branch: List of plan branches to filter by
        settings: Dictionary containing query settings
        
    Returns:
        List of dictionaries containing the driver data
        
    Raises:
        Exception: If there's an error during database operations
    """
    try:
        synced_db = get_synced_db_client()
        pool = await synced_db.get_pool()
        
        async with pool.acquire() as conn:
            # Build query dynamically with proper parameter tracking
            conditions = []
            params = [carrier]
            
            # Base query
            query = """
                SELECT 
                    drivers.*,
                    users._id,
                    driver_features.owner_score,
                    driver_features.per_mile_pay,
                    driver_features.min_mileage,
                    driver_features.max_mileage,
                    driver_features.depot_location,
                    driver_features.depot_customer_id,
                    driver_features.whitelisted_warehouses,
                    equipments.is_oog_endorsement,
                    equipments.is_cng_endorsement,
                    equipments.is_daycab_truck,
                    equipments.is_sleeper_truck
                FROM drivers 
                LEFT JOIN users 
                    ON drivers._id = users.driver
                LEFT JOIN driver_features 
                    ON users._id = driver_features.driver_id
                LEFT JOIN equipments 
                    on  drivers._id = equipments."_id"    
                WHERE drivers.carrier = $1 
                    AND drivers.is_deleted = false
            """
            
            # Add account hold conditions if exclude_account_hold is True
            if settings.get('exclude_account_hold', True):
                conditions.append("""
                    AND drivers.account_hold = false
                    AND (
                        drivers.driver_hold_start_dt IS NULL 
                        OR drivers.driver_hold_end_dt IS NULL 
                        OR NOT (
                            drivers.driver_hold_start_dt::date <= ${}::date 
                            AND drivers.driver_hold_end_dt::date >= ${}::date
                        )
                    )
                """.format(len(params) + 1, len(params) + 1))
                params.append(plan_date)
            
            # Add plan branch condition if provided
            if len(plan_branch) > 0:
                conditions.append(" AND drivers.new_terminal::jsonb ?| ${}".format(len(params) + 1))
                params.append(plan_branch)
            
            # Add shift conditions
            if settings.get('shift') == 'day':
                conditions.append(" AND drivers.is_day_driver = true")
            elif settings.get('shift') == 'night':
                conditions.append(" AND drivers.is_day_driver = false")
                
            # Add conditions to query
            query += ' '.join(conditions)
            query += """
                ORDER BY drivers.created_at DESC
            """
            
            # Execute query with dynamic parameters
            rows = await conn.fetch(query, *params)
            
            drivers = [dict(row) for row in rows]

            # remove unnecessary fields from drivers
            driver_unnecessary_fields = ['permissions', 'billing_email', 'invoice_currency_with_branch', 'invoice_currency_with_carrier',
                                         'accounting_info']

            # default_yard_locations = await get_default_yard_location(carrier)
            # first_yard_location = default_yard_locations[0]

            # map the working days hours to the drivers
            current_day_of_week = plan_date.strftime("%A")

            driver_list_keys = ['pickup_prefferred', 'preferred_states', 'preferred_distance', 'new_terminal',
                                'truck_type', 'tags', 'preferred_types_of_load', 'delivery_prefferred',
                                'ower_weight_states', 'profile_type', 'working_days_hours', 'preferred_move_types',
                                'over_weight_country', 'restricted_locations', 'preferred_chassis_types', 'preferred_country',
                                'whitelisted_warehouses']
            driver_dict_keys = ['depot_location']
            
            for driver in drivers:
                for field in driver_unnecessary_fields:
                    driver.pop(field, None)

                for key in driver_list_keys:
                    try:
                        driver[key] = json.loads(driver.get(key, "[]") or "[]")
                    except Exception as e:
                        driver[key] = []
                        logger.error(f"Error parsing {key} for driver {driver.get('_id')}: {str(e)}")
                
                for key in driver_dict_keys:
                    try:
                        driver[key] = json.loads(driver.get(key, "{}") or "{}")
                    except Exception as e:
                        driver[key] = {}
                        logger.error(f"Error parsing {key} for driver {driver.get('_id')}: {str(e)}")

                # if not driver.get('depot_location'):
                #     driver['depot_location'] = {
                #         "lat": first_yard_location.get("address", {}).get("lat", 0),
                #         "lng": first_yard_location.get("address", {}).get("lng", 0)
                #     }
                #     driver['depot_customer_id'] = first_yard_location.get("customerId", "")

                all_working_days_hours = next(
                    (e for e in driver.get('working_days_hours', []) 
                     if isinstance(e, dict) and e.get('day') in ['All', current_day_of_week]),
                    {}
                )

                start_time = all_working_days_hours.get('in_time', "00:00")
                end_time = all_working_days_hours.get('out_time', "23:59")

                if not all_working_days_hours.get('in_time'):
                    driver['missing_working_days_hours'] = True

                driver['start_time'] = start_time
                driver['end_time'] = end_time

            # filter the drivers without depot location or depot customer id
            drivers = [
                driver for driver in drivers 
                if driver.get('depot_location') and driver.get('depot_customer_id')
            ]

            return drivers
    except Exception as e:
        logger.error(f"Error retrieving driver from database: {str(e)}")
        raise
