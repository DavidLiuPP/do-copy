import logging
from typing import Any, Dict, List
from app.services.redis_service import get_shift_times
from datetime import datetime
from bson import ObjectId
from app.mongo_services.load_service import get_loads, get_loadcharges
from app.mongo_services.constant import LOAD_PROJECTION
from vrp_optimizer.helpers import get_yard_datas_from_customer_ids
from copy import deepcopy
from app.postgres_connection import PostgresConnection
import json

logger = logging.getLogger(__name__)



def update_actionable_moves_for_inday_plan(actionable_moves: List[Dict[str, Any]], behind_schedule_moves: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        actionable_moves_copy = deepcopy(actionable_moves)

        def check_if_behind_schedule(move):
            related_container_moves = []
            if move.get('is_combined_trip'):
                for event in move.get('move', []):
                    related_container_moves.append((str(event.get('loadId')), str(event.get('moveId'))))

            else:
                move_id = next(m.get('moveId', None) for m in move["move"])
                related_container_moves = [(str(move.get('_id')), str(move_id))]

            is_behind_schedule = all(
                any(bm.get('loadId') == load_id and bm.get('moveId') == move_id 
                    for bm in behind_schedule_moves)
                for load_id, move_id in related_container_moves
            )


            return is_behind_schedule         

        for move in actionable_moves_copy:
            assigned_driver = move.get('assigned_driver', None)
            if not assigned_driver:
                continue

            is_behind_schedule = check_if_behind_schedule(move)

            if is_behind_schedule:
                del move['is_manually_planned']
                del move['assigned_driver']
            else:
                move['is_active_move'] = True

        return actionable_moves_copy

    except Exception as e:
        logger.error(f"Error updating actionable moves for inday plan: {str(e)}")
        raise


async def get_behind_schedule_moves_for_in_day_plan(
        user_payload: Dict[str, Any], 
        behind_schedule_moves: List[Dict[str, Any]] = []
) -> List[Dict[str, Any]]:
    try:

        if not behind_schedule_moves:
            return []

        # Get the moves assigned or arrived in the shift time window.
        carrier = user_payload.get('carrier')

        # if load_numbers is present get the load from that list
        load_ids = [ObjectId(move.get('loadId')) for move in behind_schedule_moves]
        query_filter = {
            "_id": { "$in": load_ids },
            "carrier": ObjectId(carrier),
            "isDeleted": False,
        }

        loads = await get_loads(query_filter, LOAD_PROJECTION, 1000)

        charge_groups = await get_loadcharges({
            "carrier": ObjectId(carrier),
            "loadId": {"$in": load_ids},
            "isDefault": True
        })

        # Create lookup dict for charge groups to avoid nested loop
        charge_group_map = {str(cg["loadId"]): cg["totalAmountWithTax"] for cg in charge_groups}


        assigned_moves = []
        for move in behind_schedule_moves:
            load_data = next((load for load in loads if str(load['_id']) == str(move.get('loadId'))), None)
            if load_data:
                driver_order = load_data.get('driverOrder', [])
                move = [e for e in driver_order if str(e.get('moveId')) == str(move.get('moveId'))]
                load_assigned_date = move[0].get('loadAssignedDate') if move else None

                if not load_assigned_date:
                    continue

                for m in move:
                    m['is_manually_planned'] = True

                assigned_moves.append({
                    **load_data,
                    'revenue': charge_group_map.get(str(load_data['_id'])),
                    'driverOrder': move
                })



        return assigned_moves
    except Exception as e:
        logger.error(f"Error getting behind schedule moves for in day plan: {str(e)}")
        raise
   

async def get_schedule_info_from_driver_schedules(vehicle_data: List[Dict[str, Any]], additional_depot_locations: List[Dict[str, Any]], driver_schedules: List[Dict[str, Any]]):
    try:
        customer_ids = [v.get('last_location', {}) for v in driver_schedules if v.get('last_location')]

        depot_locations = await get_yard_datas_from_customer_ids(customer_ids)

        new_depots = {}
        vehicles = []
        for v in vehicle_data:
            matched_driver_schedule = next((ds for ds in driver_schedules if ds.get('driverId') == v.get('_id')), None)

            if not matched_driver_schedule:
                vehicles.append(v)
                continue

            if matched_driver_schedule.get('last_location_eta'):
                v['start_minute'] = int(matched_driver_schedule.get('last_location_eta'))

            v['start_location'] = matched_driver_schedule.get('last_location')
            v['max_working_minutes'] = int(v['max_working_minutes'] - matched_driver_schedule.get('working_minutes', 0))

            if v['start_minute'] >= v['end_minute']:
                continue

            if v['max_working_minutes'] <= 0:
                continue

            if not matched_driver_schedule.get('last_location'):
                vehicles.append(v)
                continue

            matched_depot = next((d for d in depot_locations if d.get('depot_customer_id') == matched_driver_schedule.get('last_location')), None)

            new_depot_hash_key = matched_depot.get('hash_key')

            depot_data = deepcopy(matched_depot)

            new_depots[new_depot_hash_key] = depot_data

            v['depot_hash_key'] = new_depot_hash_key

            vehicles.append(v)

        additional_depot_locations = additional_depot_locations + list(new_depots.values())


        return vehicles, additional_depot_locations
    except Exception as e:
        logger.error(f"Error getting schedule info from driver schedules: {str(e)}")
        raise


async def save_suggestions(
        user_payload: Dict[str, Any], 
        payload: Dict[str, Any], 
        plan_date: str, 
        converted_plan_date: datetime, 
        suggestions: List[Dict[str, Any]]
    ):
    try:

        branch = payload.get('branch')
        shift = payload.get('shift')

        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        async with pool.acquire() as conn:
            REMOVE_SUGGESTIONS_QUERY = "DELETE FROM in_day_plan_moves WHERE carrier = $1 AND plan_date::date = $2 AND branch = $3 AND shift = $4 AND is_rejected = false"
            await conn.execute(REMOVE_SUGGESTIONS_QUERY, user_payload.get('carrier'), converted_plan_date, branch or '', shift or '')

            bulk_insert_values = []
            for suggestion in suggestions:
                obj = {
                    "carrier": user_payload.get('carrier'),
                    "plan_date": plan_date,
                    "branch": branch or '',
                    "shift": shift or '',
                    "move": suggestion.get('move', []),
                    "load_id": suggestion.get('load_id'),
                    "suggested_driver": suggestion.get('assigned_driver'),
                    "load_assigned_date": suggestion.get('enroute_time', None),
                    "is_modified_move": suggestion.get('is_modified_move', False),
                    "modification_reason": suggestion.get('modification_reason', ''),
                    "chassis_pick_event": suggestion.get('chassis_pick_event', None),
                    "chassis_termination_event": suggestion.get('chassis_termination_event', None),
                    "is_rejected": False,
                    "is_trip": (suggestion.get('is_combined_trip', False) or suggestion.get('is_free_flow_move', False))
                }

                row_values = []

                
                for value in obj.values():
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
                try:
                    fields = list(obj.keys())
                    query_part = ', '.join([f'"{field}"' for field in fields])
                
                    SQL = f"""
                        INSERT INTO in_day_plan_moves ({query_part})
                        VALUES {', '.join(bulk_insert_values)}
                    """
                    await conn.execute(SQL)
                except Exception as e:
                    logger.error(f"Error saving suggestions in bulk: {str(e)}")
                    raise e
    
        return
    except Exception as e:
        logger.error(f"Error saving suggestions: {str(e)}")
        raise e