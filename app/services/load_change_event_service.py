from typing import Any
import logging
from bson import ObjectId
from app.postgres_connection import PostgresConnection
from collections import defaultdict
from app.mongo_services.load_service import get_loads

logger = logging.getLogger(__name__)

async def set_load_review_change_event(carrier: str, plan_id: str, action: str, old_load: int, load: int) -> Any:
    try:
        from app.modules.scheduler.move_scheduler import load_update_action_validate
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        updated_action = None
        recommended_action = "CHANGE_ROUTING"
        suggested_move = None

        async with pool.acquire() as conn:

            schedule_plan_query = """
                SELECT * FROM schedule_plan_v2
                WHERE carrier = $1 AND plan_id = $2 AND reference_number = $3
            """
            schedule_plan = await conn.fetch(schedule_plan_query, carrier, str(plan_id), load.get('reference_number'))

            if len(schedule_plan) == 0:
                return True
            
            schedule_plan = [dict(row) for row in schedule_plan]

            grouped = defaultdict(list)
            for item in schedule_plan:
                grouped[item["plan_date"]].append(item)

            for plan_date, plan in grouped.items():
                schedule_plan = plan
                if len(schedule_plan) > 0:
                    schedule_plan = [dict(row) for row in schedule_plan]
                    move_id = schedule_plan[0].get('move_id')
                    
                    suggested_move, new_driver_order  = get_suggested_move(load, schedule_plan, move_id)

                    if suggested_move is None:
                        continue

                    # get all current event types
                    current_move = [event for event in new_driver_order if not event.get('isVoidOut') and event.get('moveId') == move_id]
                    current_event_types = "_".join([event.get('type') for event in current_move if event.get('type') not in ['STOPOFF']])

                    if current_event_types == suggested_move:
                        await remove_review_change_event(carrier, str(plan_id), load.get('reference_number'), plan_date)
                        continue
                else:
                    await remove_review_change_event(carrier, str(plan_id), load.get('reference_number'), plan_date)
                    continue

                if action == "routing.event.address.updated":
                    # ive old_load and load now check which event address changed
                    old_driver_order = old_load.get('driverOrder')
                    new_driver_order = load.get('driverOrder')
                    # old_driver_order and new_driver_order are list of dicts and need to find the event where address changed
                    for event in old_driver_order:
                        if not event.get('isVoidOut') and event.get('type') in ['PULLCONTAINER', 'DELIVERLOAD', 'RETURNCONTAINER']:
                            for new_event in new_driver_order:
                                if new_event.get('_id') == event.get('_id') and new_event.get('customerId') != event.get('customerId'):
                                    updated_action = event.get('type')
                                    break
                elif action == "routing.event.status.changed":
                    updated_action = "CHANGE_STATUS"
                elif action == "load.updated":
                    is_action_changed, updated_field = load_update_action_validate(old_load, load)
                    if not is_action_changed:
                        await remove_review_change_event(carrier, str(plan_id), load.get('reference_number'), plan_date)
                        continue
                    updated_action = updated_field

                async with pool.acquire() as conn:
                    store_query = """
                        UPDATE schedule_plan_v2
                        SET recommended_move = $5, action = $6, recommended_action = $7
                        WHERE carrier = $1 AND reference_number = $2 AND load_id = $3 AND plan_id = $4 AND plan_date::date = $8::date
                    """
                    
                    await conn.execute(
                        store_query, 
                        carrier, 
                        load.get('reference_number'), 
                        load.get('_id'), 
                        str(plan_id), 
                        suggested_move,
                        updated_action,
                        recommended_action,
                        plan_date
                    )

        # get loads from drayos
        return True
    except Exception as e:
        logger.error(f"Failed to set load change event: {str(e)}")
        return True

async def remove_review_change_event(carrier: str, plan_id: str, reference_number: str, plan_date: str) -> Any:
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        async with pool.acquire() as conn:
            remove_query = """
                UPDATE schedule_plan_v2
                SET recommended_move = NULL, action = NULL, recommended_action = NULL, is_routing_approved = False
                WHERE carrier = $1 AND plan_id = $2 AND reference_number = $3 AND plan_date::date = $4::date
            """
            await conn.execute(remove_query, carrier, str(plan_id), reference_number, plan_date)

        return True
    except Exception as e:
        logger.error(f"Failed to remove review change event: {str(e)}")
        return False

def get_suggested_move(load: dict, schedule_plan: list, move_id: str) -> str:
    try:
        new_driver_order = load.get('driverOrder')
        valid_driver_order = [event for event in new_driver_order if event.get('isVoidOut') is False or event.get('isVoidOut') is None]

        schedule_event_order = ["PULLCONTAINER", "DELIVERLOAD", "RETURNCONTAINER"]
        schedule_plan_event_types = [event.get('profile_type') for event in schedule_plan]

        # remove duplicate events from schedule_plan_event_types
        schedule_plan_event_types = list(set(schedule_plan_event_types))
        order_index = { key: index for index, key in enumerate(schedule_event_order) }

        sort_schedule_events = sorted(schedule_plan_event_types, key=lambda x: order_index.get(x, float('inf')))
        # join sort_schedule_events into string
        suggested_move = '_'.join(sort_schedule_events)

        if suggested_move in ['PULLCONTAINER', 'PULLCONTAINER_DELIVERLOAD']:
            lift_off_event_index = next((i for i, item in enumerate(valid_driver_order) if item.get("type") == 'LIFTOFF' and item.get('moveId') == move_id), -1)
            next_event_type = 'DROPCONTAINER' if lift_off_event_index == -1 else 'LIFTOFF'
            suggested_move = '_'.join([suggested_move, next_event_type])
        elif suggested_move in ['DELIVERLOAD_RETURNCONTAINER', 'RETURNCONTAINER']:
            lift_on_event_index = next((i for i, item in enumerate(valid_driver_order) if item.get("type") == 'LIFTON' and item.get('moveId') == move_id), -1)
            prev_event_type = 'HOOKCONTAINER' if lift_on_event_index == -1 else 'LIFTON'
            suggested_move = "_".join([prev_event_type, suggested_move])
        elif suggested_move in ['DELIVERLOAD']:
            lift_on_event_index = next((i for i, item in enumerate(valid_driver_order) if item.get("type") == 'LIFTON' and item.get('moveId') == move_id), -1)
            prev_event_type = 'HOOKCONTAINER' if lift_on_event_index == -1 else 'LIFTON'
            lift_off_event_index = next((i for i, item in enumerate(valid_driver_order) if item.get("type") == 'LIFTOFF' and item.get('moveId') == move_id), -1)
            next_event_type = 'DROPCONTAINER' if lift_off_event_index == -1 else 'LIFTOFF'
            suggested_move = "_".join([prev_event_type, suggested_move, next_event_type])

        return suggested_move, new_driver_order
    except Exception as e:
        logger.error(f"Failed to get suggested move: {str(e)}")
        return None, None
    
async def set_load_review_change_for_schedule_plan(carrier: str, plan_id: str) -> Any:
    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        updated_action = None
        recommended_action = "CHANGE_ROUTING"
        suggested_move = None

        async with pool.acquire() as conn:

            schedule_plan_query = """
                SELECT * FROM schedule_plan_v2
                WHERE carrier = $1 AND plan_id = $2
            """
            schedule_plan_loads = await conn.fetch(schedule_plan_query, carrier, str(plan_id))

            if len(schedule_plan_loads) == 0:
                return True
            
            schedule_plan_loads = [dict(row) for row in schedule_plan_loads]

            # get all loads reference numbers and unique
            reference_numbers = list(set([item.get('reference_number') for item in schedule_plan_loads]))

            # get loads from drayos
            load_criteria = {
                "carrier": ObjectId(carrier),
                "reference_number": { "$in": reference_numbers },
                "isDeleted": False
            }

            load_projection = {
                "_id": 1,
                "reference_number": 1,
                "driverOrder": 1
            }
            drayos_loads = await get_loads(load_criteria, load_projection, 1000)

            # group by reference number schedule plan loads
            grouped_loads = defaultdict(list)
            for item in schedule_plan_loads:
                grouped_loads[item.get('reference_number')].append(item)

            for reference_number, schedule_loads in grouped_loads.items():
                load = next((item for item in drayos_loads if item.get('reference_number') == reference_number), None)
                if load is None:
                    continue

                # group by plan_date
                grouped_schedule_loads = defaultdict(list)
                for item in schedule_loads:
                    grouped_schedule_loads[item.get('plan_date')].append(item)

                for plan_date, schedule_loads in grouped_schedule_loads.items():
                    schedule_plan = schedule_loads
                    if len(schedule_plan) > 0:
                        move_id = schedule_plan[0].get('move_id')

                        suggested_move, new_driver_order  = get_suggested_move(load, schedule_plan, move_id)

                        if suggested_move is None:
                            continue
                            
                        # get all current event types
                        current_move = [event for event in new_driver_order if not event.get('isVoidOut') and event.get('moveId') == move_id]
                        current_event_types = "_".join([event.get('type') for event in current_move if event.get('type') not in ['STOPOFF']])

                        if current_event_types == suggested_move:
                            continue

                        store_query = """
                            UPDATE schedule_plan_v2
                            SET recommended_move = $5, action = $6, recommended_action = $7
                            WHERE carrier = $1 AND reference_number = $2 AND load_id = $3 AND plan_id = $4 AND plan_date::date = $8::date
                        """
                        
                        await conn.execute(
                            store_query, 
                            carrier, 
                            load.get('reference_number'), 
                            load.get('_id'), 
                            str(plan_id), 
                            suggested_move,
                            updated_action,
                            recommended_action,
                            plan_date
                        )
                    else:
                        continue

        return True
    except Exception as e:
        logger.error(f"Failed to set load review change for schedule plan: {str(e)}")
        return False
