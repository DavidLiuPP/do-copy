import logging
import inspect
from typing import List, Dict, Any, Callable
from vrp_optimizer.assumptions import SKIP_NODE_PENALTY

logger = logging.getLogger(__name__)


NodeConstraintRule = {
    'carrier': List[str],   # List of carrier IDs where the constraint applies
    'description': str,     # Human-readable description of the constraint
    'expression': str,      # The actual implementation logic (rule definition)
    'priority': int,        # (1-10) 10 = hard constraint (never violate), 1 = soft (can be violated for small gain)
}

NODE_CONSTRAINT_RULES: List[NodeConstraintRule] = [
    {
        'carrier': ['63039f613d347315e2a02a2d'], # Tripoint Intermodal Services
        'description': "For any move where the one-way distance exceeds 100 miles, avoid assigning that move to a company driver.",
        'expression': """
def should_avoid_assignment(node, vehicle):
    is_company_driver = vehicle.get('is_company_driver', False)
    max_distance = node.get('max_distance', 0)
    if is_company_driver and max_distance > 100:
        return True
    return False
""",
        'priority': 5
    },
    {
        'carrier': ['6500ac3f5b4e7715cea4a2fe'], # Seaport
        'description': "Prioritize overweight moves to company drivers",
        'expression': """
def should_avoid_assignment(node, vehicle):
    is_company_driver = vehicle.get('is_company_driver', False)
    overweight = node.get('overWeight', False)
    if not is_company_driver and overweight:
        return True
    return False
""",
        'priority': 7
    },
    {
        'carrier': ['6478bad770a34316adb76c24'],  # Alpha Cargo
        'description': "Do not allow drivers to start their day with a bobtail to an empty",
        'expression': """
def should_avoid_assignment(from_node, node, vehicle, base_distance):
    is_from_depot = from_node.get('isDepot', False)
    move_types = [event['type'] for event in node.get('move', [])]
    is_empty_move = len(move_types) == 2 and move_types[0] in ['HOOKCONTAINER', 'PULLCONTAINER'] and move_types[1] == 'RETURNCONTAINER'
    if is_from_depot and is_empty_move and base_distance > 0:
        return True
    return False
""",
        'priority': 8
    },
    {
        'carrier': [],  # Empty list means apply to all carriers
        'description': "Do not allow drivers to have multiple port pickups for import loads back to back",
        'expression': """
def should_avoid_assignment(from_node, node, vehicle, base_distance):
    # Skip if coming from depot
    if from_node.get('isDepot', False):
        return False

    if vehicle.get('is_port_operator', False):
        return False
        
    # Check if current node is an import port pickup
    current_move_events = node.get('move', [])
    is_current_import_pickup = any(
        event.get('type') == 'PULLCONTAINER' and 
        event.get('type_of_load') == 'IMPORT'
        for event in current_move_events
    )
    
    # Check if previous node was also an import port pickup
    prev_move_events = from_node.get('move', [])
    was_prev_import_pickup = any(
        event.get('type') == 'PULLCONTAINER' and
        event.get('type_of_load') == 'IMPORT'
        for event in prev_move_events
    )
    
    # Avoid back-to-back import port pickups
    if was_prev_import_pickup and is_current_import_pickup:
        return True
        
    return False
""",
        'priority': 7
    }
]


def compile_rule_function(expression: str) -> Callable:
    """
    Compile a constraint rule expression (string) into a callable Python function.
    The expression must define exactly one function.
    """
    local_scope: Dict[str, Any] = {}
    try:
        exec(expression, {}, local_scope)
    except Exception as e:
        raise ValueError(f"Failed to compile rule: {e}")

    # Extract the function from the scope
    funcs = [v for v in local_scope.values() if callable(v)]
    if len(funcs) != 1:
        raise ValueError("Rule must define exactly one function")

    return funcs[0]


def run_rule_function(func: Callable, available_args: Dict[str, Any]) -> bool:
    """
    Run a compiled rule function, passing only the required arguments.
    """
    try:
        sig = inspect.signature(func)
        needed_args = {
            name: available_args[name]
            for name in sig.parameters.keys()
            if name in available_args
        }
        return bool(func(**needed_args))
    except Exception as e:
        logger.error(f"Error running rule function {func.__name__}: {e}")
        return False


def get_node_penalty_from_rules(
    carrier: str,
    from_node: Dict[str, Any],
    node: Dict[str, Any],
    vehicle: Dict[str, Any],
    base_distance: float
) -> int:
    """
    Evaluate applicable node constraint rules and compute total penalty.
    """
    try:
        applicable_rules = [rule for rule in NODE_CONSTRAINT_RULES if not rule.get("carrier", []) or carrier in rule.get("carrier", [])]
        penalty = 0

        available_args = {
            "from_node": from_node,
            "node": node,
            "vehicle": vehicle,
            "base_distance": base_distance
        }

        for rule in applicable_rules:
            try:
                func = compile_rule_function(rule["expression"])
                violated = run_rule_function(func, available_args)

                if violated:
                    priority = rule.get("priority", 5)
                    penalty += (SKIP_NODE_PENALTY / 10) * priority

            except Exception as e:
                logger.error(f"Error evaluating rule {rule.get('description', '')}: {e}")
                continue

        return int(penalty)

    except Exception as e:
        logger.error(f"Unexpected error in get_node_penalty_from_rules: {e}")
        return 0
