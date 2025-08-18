import logging
import json
from datetime import datetime
from typing import Dict, Any, List
from app.postgres_connection import PostgresConnection
from app.mongo_services.load_service import get_loads_with_reference_numbers
from app.mongo_services.trip_service import get_trips
from bson import ObjectId

# Configure module logger
logger = logging.getLogger(__name__)


async def _get_load_data(carrier: str, reference_number: str) -> Dict[str, Any]:
    """Fetch and validate load data from MongoDB."""
    loads = await get_loads_with_reference_numbers(
        carrier=carrier,
        reference_numbers=[reference_number],
        plan_branch=[],
        limit=1
    )
    if not loads:
        raise ValueError(f"Load not found for reference {reference_number}")
    
    load = loads[0]
    if not load.get('driverOrder'):
        raise ValueError("Driver order not found in load data")
    
    return load

def _extract_time_fields(load: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and validate time fields from load data."""
    return {
        'pickup_time': (
            load.get('pickupFromTime') or 
            (load.get("pickupTimes", [{}])[0].get("pickupFromTime") 
             if load.get("pickupTimes") else None)
        ),
        'delivery_time': (
            load.get('deliveryFromTime') or
            (load.get("deliveryTimes", [{}])[0].get("deliveryFromTime")
             if load.get("deliveryTimes") else None)
        ),
        'return_time': load.get("returnFromTime")
    }

async def _get_plan_details(conn, plan_id: str) -> Dict[str, Any]:
    """Fetch and validate plan details from database."""
    plan_details = await conn.fetchrow(
        "SELECT * FROM optimizer_plans WHERE id = $1",
        plan_id
    )
    if not plan_details:
        raise ValueError(f"Plan not found with ID {plan_id}")
    return plan_details

def _prepare_optimizer_load(
    carrier: str,
    payload: Dict[str, Any],
    load: Dict[str, Any],
    move: List[Dict[str, Any]],
    time_fields: Dict[str, Any],
    plan_details: Dict[str, Any]
) -> Dict[str, Any]:
    """Prepare optimizer load record with all required fields."""
    return {
        "plan_id": payload['plan_id'],
        "carrier": carrier,
        "reference_number": payload['reference_number'],
        "driver": payload['driver'],
        "move_id": payload['move_id'],
        "load_id": load.get('_id'),
        "move": json.dumps(move),
        "pickup_time": datetime.fromisoformat(time_fields['pickup_time']).replace(tzinfo=None) if time_fields['pickup_time'] else None,
        "delivery_time": datetime.fromisoformat(time_fields['delivery_time']).replace(tzinfo=None) if time_fields['delivery_time'] else None,
        "return_time": datetime.fromisoformat(time_fields['return_time']).replace(tzinfo=None) if time_fields['return_time'] else None,
        "load_assigned_date": datetime.fromisoformat(payload['load_assigned_date']),
        "move_start_time": datetime.fromisoformat(payload['load_assigned_date']),
        "caller": str(load.get('caller')),
        "plan_date": plan_details['plan_date'].strftime('%Y-%m-%d') if isinstance(plan_details['plan_date'], datetime) else plan_details['plan_date'],
        "version": plan_details['version'],
        "distance": 0,
        "driver_pay": 0,
        "revenue": 0,
        "expense": 0,
        "is_manual": True,
        "chassis_pick_event": payload.get('chassis_pick_event', None),
        "chassis_termination_event": payload.get('chassis_termination_event', None)
    }

async def _insert_optimizer_load(conn, optimizer_load: Dict[str, Any]) -> None:
    """Insert optimizer load record into database."""
    fields = ', '.join([f'"{field}"' for field in optimizer_load.keys()])
    placeholders = ', '.join([f'${i+1}' for i in range(len(optimizer_load))])
    values = list(optimizer_load.values())
    
    sql = f"""
        INSERT INTO optimizer_loads ({fields})
        VALUES ({placeholders})
        RETURNING id;
    """
    await conn.execute(sql, *values)

async def _delete_existing_optimizer_load(conn, plan_id: str, carrier: str, reference_number: str) -> None:
    """Delete existing optimizer load if it exists."""
    if not carrier or not reference_number:
        return

    await conn.execute(
        """
        DELETE FROM optimizer_loads 
        WHERE plan_id = $1 
            AND carrier = $2 
            AND reference_number = $3
            AND is_deleted = true
        """,
        plan_id,
        carrier,
        reference_number
    )

async def _update_plan_metrics(conn, plan_id: str) -> None:
    """Update optimizer plan metrics."""
    await conn.execute("""
        UPDATE optimizer_plans
        SET no_of_moves = no_of_moves + 1
        WHERE id = $1
    """, plan_id)


async def add_move_to_optimizer_plan(carrier: str, payload: Dict[str, Any]) -> Dict[str, str]:
    """
    Add a move to an existing optimizer plan with validation and error handling.

    Args:
        carrier (str): The carrier identifier
        payload (Dict[str, Any]): Dictionary containing move details:
            - reference_number (str): Load reference number
            - move_id (str): Unique identifier of the move
            - driver (str): Driver identifier
            - load_assigned_date (str): ISO format date when load is assigned
            - plan_id (str): Unique identifier of the optimizer plan

    Returns:
        Dict[str, str]: Response containing:
            - status: "success" or "error"
            - message: Description of the operation result

    Raises:
        ValueError: If payload validation fails or required data is missing
        Exception: If database operations fail or other errors occur
    """
    try:
        # Validate required payload fields
        """Validate all required fields are present in payload."""
        required_fields = [
            'reference_number', 'driver',
            'load_assigned_date', 'plan_id'
        ]
        missing_fields = [field for field in required_fields if not payload.get(field)]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
        
        if not payload.get('move_id') and not payload.get('is_free_flow_move'):
            raise ValueError("Move ID is required for container moves")
        
        if payload.get('is_free_flow_move'):
            # Fetch and validate load data
            criteria = {
                "carrier": ObjectId(carrier),
                "tripNumber": payload['reference_number'],
                "isDeleted": False
            }
            trip = await get_trips(criteria, 1)
            if not trip:
                raise ValueError(f"Trip not found for reference {payload['reference_number']}")
            
            move = trip[0].get('tripOrder', [])

            postgres = PostgresConnection()
            async with (await postgres.get_pool()).acquire() as conn:
                await conn.execute(
                        """
                            UPDATE optimizer_loads
                            SET driver = $1,
                                move = $2,
                                load_assigned_date = $3,
                                move_start_time = $4,
                                is_manual = true,
                                is_deleted = false
                            WHERE 
                                plan_id = $5
                                AND carrier = $6
                                AND reference_number = $7
                                AND is_free_flow_move = true
                        """,
                        payload['driver'],
                        json.dumps(move),
                        datetime.fromisoformat(payload['load_assigned_date']),
                        datetime.fromisoformat(payload['load_assigned_date']),
                        payload['plan_id'],
                        carrier,
                        payload['reference_number']
                    )
                await _update_plan_metrics(conn, payload['plan_id'])

        else:
            # Fetch and validate load data
            load = await _get_load_data(carrier, payload['reference_number'])
            """Validate and extract move from driver order."""
            move = [m for m in load['driverOrder'] if m.get('moveId') == payload['move_id']]
            if not move:
                raise ValueError(f"Move {payload['move_id']} not found in driver order")

            # Extract time fields with validation
            time_fields = _extract_time_fields(load)

            # Get plan details and validate
            postgres = PostgresConnection()
            async with (await postgres.get_pool()).acquire() as conn:
                plan_details = await _get_plan_details(conn, payload['plan_id'])
                
                # Prepare and insert optimizer load
                optimizer_load = _prepare_optimizer_load(
                    carrier=carrier,
                    payload=payload,
                    load=load,
                    move=move,
                    time_fields=time_fields,
                    plan_details=plan_details
                )

                await _delete_existing_optimizer_load(conn, payload['plan_id'], carrier, payload['reference_number'])
                await _insert_optimizer_load(conn, optimizer_load)
                await _update_plan_metrics(conn, payload['plan_id']) # increment total moves

        return {
            "status": "success",
            "message": "Move successfully added to optimizer plan"
        }

    except ValueError as e:
        logger.error(f"Validation error in add_move_to_optimizer_plan: {str(e)}")
        raise ValueError(str(e))
    except Exception as e:
        logger.error(f"Failed to add move to plan: {str(e)}")
        raise Exception(f"Failed to add move to plan: {str(e)}")


async def reassign_move_to_optimizer_plan(
    carrier: str,
    body: Dict[str, Any]
) -> Dict[str, str]:
    """
    Re-assigns a load to a new driver in the optimizer plan.

    Args:
        carrier: The carrier ID/name
        body: Dictionary containing:
            - reference_number (str): Load reference number
            - plan_id (str): Plan ID
            - move_id (str): Move ID (for container moves)
            - optimizer_load_id (str): Optimizer load ID
            - prev_driver_id (str): Previous driver ID
            - driver (str): New driver ID
            - load_assigned_date (str): New assigned date
            - is_free_flow_move (bool): If the move is free flow

    Returns:
        Dict containing status and message

    Raises:
        ValueError: If validation fails
        Exception: For any other errors
    """
    try:

        # get payload from body
        payload = {
            "reference_number": body.get("reference_number"),
            "plan_id": body.get("plan_id"),
            "optimizer_load_id": body.get("optimizer_load_id"),
            "move_id": body.get("move_id"),
            "is_free_flow_move": body.get("is_free_flow_move", False),
            "prev_driver_id": body.get("prev_driver_id"),
            "driver": body.get("driver"),
            "load_assigned_date": body.get("load_assigned_date")
        }

        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        prev_load_data = None

        # remove from previous driver if needed
        if payload.get('prev_driver_id') and payload.get('driver'):
            async with pool.acquire() as conn:
                prev_load_data = await conn.fetchrow(
                    """
                    SELECT * FROM optimizer_loads
                    WHERE reference_number = $1
                    AND plan_id = $2
                    """,
                    payload['reference_number'],
                    payload['plan_id']
                )
                
                for event_type in ['chassis_pick_event', 'chassis_termination_event']:
                    if prev_load_data[event_type]:
                        event_data = json.loads(prev_load_data[event_type])
                        for field in ['enroute', 'arrived', 'departed']:
                            event_data.pop(field, None)
                        payload[event_type] = json.dumps(event_data)

            await remove_move_from_optimizer_plan(carrier, {
                "reference_number": payload['reference_number'],
                "plan_id": payload['plan_id'],
                "move_id": payload.get('move_id'),
                "optimizer_load_id": payload.get('optimizer_load_id'),
                "is_free_flow_move": payload.get('is_free_flow_move', False)
            })

        # Add to new driver
        add_payload = dict(payload)
        add_payload.pop('prev_driver_id', None)
        result = await add_move_to_optimizer_plan(carrier, add_payload)
        return {
            "status": "success",
            "message": "Load successfully reassigned to new driver"
        }
    except ValueError as e:
        logger.error(f"Validation error in reassign_load_driver: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to reassign load driver: {str(e)}")
        raise Exception(f"Failed to reassign load driver: {str(e)}")


async def remove_move_from_optimizer_plan(
    carrier: str,
    payload: Dict[str, Any]
) -> Dict[str, str]:
    """
    Removes a move from an optimizer plan and updates related metrics.
    
    Args:
        carrier: The carrier ID/name
        payload: Dictionary containing move details:
            - reference_number (str): Load reference number
            - move_id (str): Unique identifier of the move
            - plan_id (str): Unique identifier of the optimizer plan
        
    Returns:
        Dict containing status and message
        
    Raises:
        ValueError: If move is not found or validation fails
        Exception: For any other errors during execution
    """
    try:
        reference_number = payload['reference_number']
        move_id = payload.get('move_id')
        plan_id = payload['plan_id']
        optimizer_load_id = payload['optimizer_load_id']
        is_free_flow_move = payload.get('is_free_flow_move', False)

        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        # Find the move to remove
        async with pool.acquire() as conn:
            optimizer_plan = await conn.fetchrow(
                """
                SELECT * FROM optimizer_plans
                WHERE id = $1
                """,
                plan_id
            )

            if not optimizer_plan:
                raise ValueError(f"Plan not found with ID {plan_id}")
            

            optimizer_load = None
            if is_free_flow_move:
                optimizer_load = await conn.fetchrow(
                    """
                    SELECT * FROM optimizer_loads
                    WHERE plan_id = $1
                        AND id = $2
                        AND carrier = $3 
                        AND reference_number = $4
                        AND is_deleted = false
                        AND is_free_flow_move = true
                    """,
                    plan_id,
                    optimizer_load_id,
                    str(carrier),
                    reference_number
                )
            else:
                optimizer_load = await conn.fetchrow(
                    """
                    SELECT * FROM optimizer_loads
                    WHERE plan_id = $1
                        AND id = $2
                        AND carrier = $3 
                        AND move_id = $4
                        AND reference_number = $5
                        AND is_deleted = false
                    """,
                    plan_id,
                    optimizer_load_id,
                    str(carrier),
                    move_id, 
                    reference_number
                )

            if not optimizer_load:
                raise ValueError(f"Move not found in plan with ID {plan_id}")

            # Soft delete the move
            await conn.execute(
                """
                UPDATE optimizer_loads 
                SET is_deleted = true
                WHERE id = $1
                """,
                optimizer_load['id']
            )

            # Get remaining moves in plan
            remaining_moves = await conn.fetch(
                """
                SELECT * FROM optimizer_loads
                WHERE plan_id = $1 
                    AND is_deleted = false
                """,
                plan_id
            )

            # Calculate updated plan metrics
            plan_metrics = {
                'unique_drivers': set(),
                'total_miles': 0,
                'min_arrival': None,
                'max_completion': None, 
                'total_revenue': 0,
                'total_driver_pay': 0
            }

            for move in remaining_moves:
                if move['driver']:
                    plan_metrics['unique_drivers'].add(move['driver'])
                
                plan_metrics['total_miles'] += move['distance']
                arrival_time = move['move_start_time']
                completion_time = move['move_complete_time']
                
                if move['move_start_time'] and (not plan_metrics['min_arrival'] or arrival_time < plan_metrics['min_arrival']):
                    plan_metrics['min_arrival'] = arrival_time
                if move['move_complete_time'] and (not plan_metrics['max_completion'] or completion_time > plan_metrics['max_completion']):
                    plan_metrics['max_completion'] = completion_time
                    
                plan_metrics['total_revenue'] += move['revenue']
                plan_metrics['total_driver_pay'] += move['driver_pay']

            # Update plan with new metrics
            await conn.execute(
                """
                UPDATE optimizer_plans
                SET driver_pay = $1,
                    revenue = $2,
                    no_of_moves = $3,
                    no_of_driver = $4,
                    start_time = $5,
                    end_time = $6,
                    total_miles = $7
                WHERE id = $8
                """,
                plan_metrics['total_driver_pay'],
                plan_metrics['total_revenue'],
                len(remaining_moves),
                len(plan_metrics['unique_drivers']),
                plan_metrics['min_arrival'] if plan_metrics['min_arrival'] is not None else optimizer_plan['start_time'],
                plan_metrics['max_completion'] if plan_metrics['max_completion'] is not None else optimizer_plan['end_time'],
                plan_metrics['total_miles'],
                plan_id
            )

        return {
            "status": "success",
            "message": "Move successfully removed from optimizer plan"
        }

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to remove move from plan: {str(e)}")
        raise Exception(f"Failed to remove move from plan: {str(e)}")
