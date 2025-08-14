import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Dynamic constraint definitions
CONSTRAINT_RULES: List[Dict[str, Any]] = [
    {
        'constraint_name': 'oog',
        'node_field': 'oog',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'is_oog_endorsement',
    },
    {
        'constraint_name': 'hot',
        'node_field': 'hot',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'hot',
    },
    {
        'constraint_name': 'scale',
        'node_field': 'scale',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'scale',
    },
    {
        'constraint_name': 'gdp',
        'node_field': 'gdp',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'gdp',
    },
    {
        'constraint_name': 'ev',
        'node_field': 'ev',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'ev',
    },   
    {
        'constraint_name': 'bonded',
        'node_field': 'bonded',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'bonded',
    }, 
    {
        'constraint_name': 'waste',
        'node_field': 'waste',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'waste',
    }, 
    {
        'constraint_name': 'genset',
        'node_field': 'isgenset',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'genset',
    },    
    {
        'constraint_name': 'demestic',
        'node_field': 'demestic',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'demestic',
    },    
    {
        'constraint_name': 'over_height',
        'node_field': 'over_height',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'over_height',
    },   
    {
        'constraint_name': 'liquor',
        'node_field': 'liquor',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'liquor',
    },
    {
        'constraint_name': 'overweight',
        'node_field': 'overweight',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'overweight',
    },
    {
        'constraint_name': 'hazmat',
        'node_field': 'hazmat',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'hazmat',
    },
    {
        'constraint_name': 'b-train',
        'node_field': 'is_combined_trip',
        'operator': 'REQUIRES_IF_TRUE',
        'vehicle_field': 'b_train',
    },
    {
        'constraint_name': 'max_miles_per_move',
        'node_field': 'route_distance',
        'operator': 'LESS_THAN',
        'vehicle_field': 'max_miles_per_move',
    },
    {
        'constraint_name': 'min_miles_per_move',
        'node_field': 'route_distance',
        'operator': 'GREATER_THAN_EQUALS',
        'vehicle_field': 'min_miles_per_move',
    },
    {
        'constraint_name': 'terminal_compatibility',
        'node_field': 'terminal',
        'operator': 'IN',
        'vehicle_field': 'new_terminal',
    },
    {
        'constraint_name': 'restricted_locations',
        'node_field': 'locations',
        'operator': 'NOT_IN',
        'vehicle_field': 'restricted_locations',
    },
    {
        'constraint_name': 'whitelisted_warehouses',
        'node_field': 'warehouse_ids',
        'operator': 'OPTIONAL_IN',
        'vehicle_field': 'whitelisted_warehouses',
    },
    {
        'constraint_name': 'preferred_states',
        'node_field': 'preferred_states',
        'operator': 'SUBSET_OF',
        'vehicle_field': 'preferred_states',
    }
]

def operator_fn(operator: str, left_val, right_val) -> bool:
    if operator == 'GREATER_THAN':
        return left_val > right_val
    
    elif operator == 'GREATER_THAN_EQUALS':
        return left_val >= right_val
    
    elif operator == 'LESS_THAN':
        return left_val < right_val

    elif operator == 'IN':
        left_set = set(left_val) if isinstance(left_val, (list, set)) else {left_val}
        right_set = set(right_val) if isinstance(right_val, (list, set)) else {right_val}
        return bool(left_set.intersection(right_set))
    
    elif operator == 'OPTIONAL_IN':
        if not right_val or not left_val:
            return True
        
        left_set = set(left_val) if isinstance(left_val, (list, set)) else {left_val}
        right_set = set(right_val) if isinstance(right_val, (list, set)) else {right_val}
        return bool(left_set.intersection(right_set))

    elif operator == 'NOT_IN':
        # Convert both values to sets and ensure they are disjoint
        left_set = set(left_val) if isinstance(left_val, (list, set)) else {left_val}
        right_set = set(right_val) if isinstance(right_val, (list, set)) else {right_val}
        return left_set.isdisjoint(right_set)
    
    elif operator == 'EQUALS':
        return left_val == right_val
    
    elif operator == 'NOT_EQUALS':
        return left_val != right_val
    
    elif operator == 'BOOLEAN_EQUALS':
        if right_val == 'false':
            right_val = False
        return bool(left_val) == bool(right_val)
        
    elif operator == 'REQUIRES_IF_TRUE':
        # If the node requires the capability (left_val == True), the vehicle must have it (right_val == True)
        if right_val == 'false':
            right_val = False
        if right_val == 'true':
            right_val = True    
        return not left_val or right_val  
    
    elif operator == 'BOOLEAN_NOT_EQUALS':
        if right_val == 'false':
            right_val = False
        return bool(left_val) != bool(right_val)
    
    elif operator == 'SUBSET_OF':
        left_set = set(left_val) if isinstance(left_val, (list, set)) else {left_val}
        right_set = set(right_val) if isinstance(right_val, (list, set)) else {right_val}
        return bool(left_set.issubset(right_set))

    raise ValueError(f"Unsupported operator: {operator}")


def is_vehicle_compatible_with_node(vehicle: dict, node: dict) -> bool:
    for rule in CONSTRAINT_RULES:
        # Assumption: left_operand is the node field and right_operand is the vehicle field ALWAYS
        try:
            left_val = node.get(rule['node_field'])
            right_val = vehicle.get(rule['vehicle_field'])

            if left_val in [None, '', [], {}] or right_val in [None, '', [], {}]:
                continue

            if not operator_fn(rule['operator'], left_val, right_val):
                return False

        except Exception as e:
            logger.error(f"Error evaluating constraint '{rule['constraint_name']}': {str(e)}")
            return True

    return True
