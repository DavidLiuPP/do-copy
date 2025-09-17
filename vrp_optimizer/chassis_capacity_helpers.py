from typing import Dict, List, Any
from copy import deepcopy
from app.postgres_services.configurations_service import get_container_sizes, get_container_types
from vrp_optimizer.routing_distance import get_distance_between_locations_from_matrix
from app.postgres_connection import PostgresConnection


# TODO: get chassis limits from the db 'chassis_inventory_by_yard'
async def get_chassis_limits(carrier, plan_date):

    try:
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        async with pool.acquire() as conn:
            QUERY = """
                SELECT * FROM chassis_inventory_by_yard
                WHERE carrier_id = $1
                AND date::date = $2
                AND is_deleted = false
                AND chassis_count > 0
            """
            rows = await conn.fetch(QUERY, carrier, plan_date)
            chassis_data = [dict(row) for row in rows]

            chassis_limits = {}

            for chassis in chassis_data:
                yard = chassis.get('yard_location')
                chassis_size = chassis.get('chassis_size')
                chassis_type = chassis.get('chassis_type')
                chassis_count = chassis.get('chassis_count', 0) or 0

                if chassis_size and chassis_type:
                    if yard not in chassis_limits:
                        chassis_limits[yard] = {}
                    chassis_limits[yard][(chassis_size, chassis_type)] = chassis_count


            return chassis_limits
    except Exception as e:
        print('error in get_chassis_limits', e)
        raise e


def get_chassis_for_container(best_yard_for_chassis, containerSizeLabel, containerTypeLabel, chassis_limits_copy, equipment_validations):

    yard_chassis = chassis_limits_copy.get(best_yard_for_chassis.get('location_id'), {})

    all_compatible_chassis = [
        e for e in equipment_validations if e.get('containerSize') == containerSizeLabel and e.get('containerType') == containerTypeLabel
    ]

    chassis_with_most_count = max(
        ((c.get('chassisSize'), c.get('chassisType')) 
         for c in all_compatible_chassis),
        key=lambda k: yard_chassis.get(k, 0),
        default=None
    )

    return chassis_with_most_count


def get_nearest_yard_from_location(location: List[float], yards: List[dict], location_distance_matrix: Dict[str, Any]) -> dict:

    nearest_yard = None
    nearest_distance = 1000000
    for yard in yards:
        distance = get_distance_between_locations_from_matrix(location, yard.get('start_loc'), location_distance_matrix)
        if distance < nearest_distance:
            nearest_yard = yard
            nearest_distance = distance

    return nearest_yard

def get_chassis_and_yard(containerSizeLabel, containerTypeLabel, location, yards, location_distance_matrix, equipment_validations, chassis_limits, initial_chassis_limit):
    try: 
        yard_clone = deepcopy(yards)

        for yard in yard_clone:
            distance = get_distance_between_locations_from_matrix(location, yard.get('start_loc'), location_distance_matrix)
            yard['distance_from_location'] = distance

        # sort by distance_from_location
        yard_clone.sort(key=lambda x: x.get('distance_from_location'))

        if not (containerSizeLabel and containerTypeLabel):
            # get the nearest yard's most available chassis
            yard = yard_clone[0]
            yard_chassis_limits = chassis_limits.get(yard.get('depot_customer_id'), {})
            chassis_with_most_count = max(
                (c for c in yard_chassis_limits),
                key=lambda k: yard_chassis_limits.get(k, 0),
                default=None
            )

            return yard, chassis_with_most_count or ('N/A', 'N/A')

        all_compatible_chassis = [
            e for e in equipment_validations if e.get('containerSize') == containerSizeLabel and e.get('containerType') == containerTypeLabel
        ]

        if not all_compatible_chassis:
            return yard_clone[0], ('N/A', 'N/A')

        for yard in yard_clone:
            yard_chassis_limits = chassis_limits.get(yard.get('depot_customer_id'), {})
            chassis_with_most_count = max(
                ((c.get('chassisSize'), c.get('chassisType')) 
                for c in all_compatible_chassis),
                key=lambda k: yard_chassis_limits.get(k, 0),
                default=None
            )
            count_at_location = yard_chassis_limits.get(chassis_with_most_count, 0)

            if count_at_location > 0:
                chassis_limits[yard.get('depot_customer_id')][chassis_with_most_count] = count_at_location - 1
                return yard, chassis_with_most_count


        for yard in yard_clone:
            yard_chassis_limits = initial_chassis_limit.get(yard.get('depot_customer_id'), {})
            chassis_with_most_count = max(
                ((c.get('chassisSize'), c.get('chassisType')) 
                for c in all_compatible_chassis),
                key=lambda k: yard_chassis_limits.get(k, 0),
                default=None
            )
            count_at_location = yard_chassis_limits.get(chassis_with_most_count, 0)

            if count_at_location > 0:
                return yard, chassis_with_most_count

        # if no chassis is available at any location, then return the nearest yard's most available chassis
        nearest_yard = yard_clone[0]
        return nearest_yard, (all_compatible_chassis[0].get('chassisSize'), all_compatible_chassis[0].get('chassisType'))
    except Exception as e:
        print('error in get_chassis_and_yard', e)
        return {}, ('N/A', 'N/A')

async def populate_chassis_in_nodes(
    carrier: str,
    nodes: List[dict], 
    chassis_limits: Dict[str, int], 
    yards: List[dict], 
    equipment_validations: List[dict],
    location_distance_matrix: Dict[str, Any]
) -> List[dict]:
    
    chassis_limits_copy = deepcopy(chassis_limits)

    all_container_sizes = await get_container_sizes(carrier, list(set([node.get('containerSize') for node in nodes if node.get('containerSize')])))
    all_container_types = await get_container_types(carrier, list(set([node.get('containerType') for node in nodes if node.get('containerType')])))

    for node in nodes:

        containerSize = node.get('containerSize')
        containerType = node.get('containerType')

        containerSizeLabel = next((size for size in all_container_sizes if size.get('_id') == containerSize), None) if containerSize else None
        containerTypeLabel = next((type for type in all_container_types if type.get('_id') == containerType), None) if containerType else None

        containerSizeLabel = containerSizeLabel.get('label') if containerSizeLabel else None
        containerTypeLabel = containerTypeLabel.get('label') if containerTypeLabel else None

        if node.get('isDepot'):
            continue

        does_first_event_needs_chassis = node.get('move', [])[0].get('type') in ['PULLCONTAINER', 'LIFTON']

        node.setdefault('does_first_event_needs_chassis', does_first_event_needs_chassis)

        if does_first_event_needs_chassis:
            yard, chassis = get_chassis_and_yard(
                containerSizeLabel, 
                containerTypeLabel, 
                node.get('start_loc'), 
                yards, 
                location_distance_matrix, 
                equipment_validations, 
                chassis_limits_copy,
                initial_chassis_limit=chassis_limits
            )
            yard_id = yard.get('depot_customer_id')
            node.setdefault('chassis', chassis)
            node.setdefault('nearest_yard_for_chassis', yard_id)
    return nodes

async def map_chassis_into_moves(
    carrier,
    plan_date,
    nodes,
    yards,
    equipment_validations,
    location_distance_matrix,
):
    try:
        nodes_copy = deepcopy(nodes)
        chassis_limits = await get_chassis_limits(
            carrier=carrier, 
            plan_date=plan_date
        )

        mapped_moves = await populate_chassis_in_nodes(
            carrier=carrier,
            nodes=nodes_copy, 
            chassis_limits=chassis_limits, 
            yards=yards, 
            equipment_validations=equipment_validations, 
            location_distance_matrix=location_distance_matrix
        )

        mapped_chassis_limits = {}
        for yard, yard_limits in chassis_limits.items():
            for chassis, count in yard_limits.items():
                chassis_size, chassis_type = chassis
                key = f"{yard}_{chassis_size}_{chassis_type}"
                mapped_chassis_limits[key] = count

        return mapped_moves, mapped_chassis_limits
    except Exception as e:
        print('error in map_chassis_into_moves', e)
        return nodes, {}