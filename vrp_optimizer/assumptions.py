from ortools.constraint_solver import routing_enums_pb2
from app.modules.optimizer.constants import CARRIER_CONFIGS

# Default assumptions (fallback values)
PREFERRED_START_TIME = 6 * 60
SKIP_NODE_PENALTY = 2000
ADDITIONAL_PENALTY_FOR_ASSIGNED_MOVE = 2000
PENALTY_FOR_EARLY_INVOCATION = 10
VEHICLE_USE_PENALTY = 100
PENALTY_FOR_EMPTY_MILES = 10
MAX_EMPTY_MILES = 30
BUFFER_TIME_FOR_NEXT_NODE = 10 # in minutes
TIME_TO_SWITCH_CHASSIS = 15 # in minutes
MAX_MINUTES = 1440
MAXIMIZE_WORKING_MINUTES = True
MAX_WAITING_TIME_BETWEEN_NODES = 60  # in minutes

# Default VRP assumptions
DEFAULT_VRP_ASSUMPTIONS = {
    'PREFERRED_START_TIME': PREFERRED_START_TIME,
    'SKIP_NODE_PENALTY': SKIP_NODE_PENALTY,
    'ADDITIONAL_PENALTY_FOR_ASSIGNED_MOVE': ADDITIONAL_PENALTY_FOR_ASSIGNED_MOVE,
    'PENALTY_FOR_EARLY_INVOCATION': PENALTY_FOR_EARLY_INVOCATION,
    'VEHICLE_USE_PENALTY': VEHICLE_USE_PENALTY,
    'PENALTY_FOR_EMPTY_MILES': PENALTY_FOR_EMPTY_MILES,
    'MAX_EMPTY_MILES': MAX_EMPTY_MILES,
    'BUFFER_TIME_FOR_NEXT_NODE': BUFFER_TIME_FOR_NEXT_NODE,
    'TIME_TO_SWITCH_CHASSIS': TIME_TO_SWITCH_CHASSIS,
    'MAX_MINUTES': MAX_MINUTES,
    'MAXIMIZE_WORKING_MINUTES': MAXIMIZE_WORKING_MINUTES,
    'MAX_WAITING_TIME_BETWEEN_NODES': MAX_WAITING_TIME_BETWEEN_NODES
}

def get_carrier_vrp_assumptions(carrier_id: str = None) -> dict:
    """
    Get carrier-specific VRP assumptions from constants.py or default assumptions if carrier not found.
    
    Args:
        carrier_id: The carrier ID to get assumptions for
        
    Returns:
        Dictionary containing the VRP assumptions for the carrier
    """
    if carrier_id and carrier_id in CARRIER_CONFIGS:
        carrier_config = CARRIER_CONFIGS[carrier_id]
        if 'vrp_assumptions' in carrier_config:
            # Merge default assumptions with carrier-specific ones
            assumptions = DEFAULT_VRP_ASSUMPTIONS.copy()
            assumptions.update(carrier_config['vrp_assumptions'])
            return assumptions
    
    return DEFAULT_VRP_ASSUMPTIONS

CHASSIS_ACTIVITIES = {
    "NO_ACTION": "NO_ACTION",
    "HOOKCHASSIS": "HOOKCHASSIS",
    "DROPCHASSIS": "DROPCHASSIS",
    "DROP_AND_HOOK_CHASSIS": "DROP_AND_HOOK_CHASSIS"
}

VRP_ALGORITHMS = {
    # """Lets the solver detect which strategy to use according to the model being
    # solved.
    # """
    "AUTOMATIC": routing_enums_pb2.FirstSolutionStrategy.AUTOMATIC,



    # """--- Path addition heuristics ---
    # Starting from a route "start" node, connect it to the node which produces
    # the cheapest route segment, then extend the route by iterating on the
    # last node added to the route.
    # """
    "PATH_CHEAPEST_ARC": routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC,



    # """Same as PATH_CHEAPEST_ARC, but arcs are evaluated with a comparison-based
    # selector which will favor the most constrained arc first. To assign a
    # selector to the routing model, see
    # RoutingModel::ArcIsMoreConstrainedThanArc() in routing.h for details.
    # """
    "PATH_MOST_CONSTRAINED_ARC": routing_enums_pb2.FirstSolutionStrategy.PATH_MOST_CONSTRAINED_ARC,



    # """Same as PATH_CHEAPEST_ARC, except that arc costs are evaluated using the
    # function passed to RoutingModel::SetFirstSolutionEvaluator()
    # (cf. routing.h).
    # """
    "EVALUATOR_STRATEGY": routing_enums_pb2.FirstSolutionStrategy.EVALUATOR_STRATEGY,



    # """Savings algorithm (Clarke & Wright).
    # Reference: Clarke, G. & Wright, J.W.:
    # "Scheduling of Vehicles from a Central Depot to a Number of Delivery
    # Points", Operations Research, Vol. 12, 1964, pp. 568-581
    # """
    "SAVINGS": routing_enums_pb2.FirstSolutionStrategy.SAVINGS,



    # """Parallel version of the Savings algorithm.
    # Instead of extending a single route until it is no longer possible,
    # the parallel version iteratively considers the next most improving
    # feasible saving and possibly builds several routes in parallel.
    # """
    "PARALLEL_SAVINGS": routing_enums_pb2.FirstSolutionStrategy.PARALLEL_SAVINGS,



    # """Sweep algorithm (Wren & Holliday).
    # Reference: Anthony Wren & Alan Holliday: Computer Scheduling of Vehicles
    # from One or More Depots to a Number of Delivery Points Operational
    # Research Quarterly (1970-1977),
    # Vol. 23, No. 3 (Sep., 1972), pp. 333-344
    # """
    "SWEEP": routing_enums_pb2.FirstSolutionStrategy.SWEEP,



    # """Christofides algorithm (actually a variant of the Christofides algorithm
    # using a maximal matching instead of a maximum matching, which does
    # not guarantee the 3/2 factor of the approximation on a metric travelling
    # salesman). Works on generic vehicle routing models by extending a route
    # until no nodes can be inserted on it.
    # Reference: Nicos Christofides, Worst-case analysis of a new heuristic for
    # the travelling salesman problem, Report 388, Graduate School of
    # """
    "CHRISTOFIDES": routing_enums_pb2.FirstSolutionStrategy.CHRISTOFIDES,



    # """--- Path insertion heuristics ---
    # Make all nodes inactive. Only finds a solution if nodes are optional (are
    # element of a disjunction constraint with a finite penalty cost).
    # """
    "ALL_UNPERFORMED": routing_enums_pb2.FirstSolutionStrategy.ALL_UNPERFORMED,



    # """Iteratively build a solution by inserting the cheapest node at its
    # cheapest position; the cost of insertion is based on the global cost
    # function of the routing model. As of 2/2012, only works on models with
    # optional nodes (with finite penalty costs).
    # """
    "BEST_INSERTION": routing_enums_pb2.FirstSolutionStrategy.BEST_INSERTION,



    # """Iteratively build a solution by inserting the cheapest node at its
    # cheapest position; the cost of insertion is based on the arc cost
    # function. Is faster than BEST_INSERTION.
    # """
    "PARALLEL_CHEAPEST_INSERTION": routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION,



    # """Iteratively build a solution by constructing routes sequentially, for
    # each route inserting the cheapest node at its cheapest position until the
    # route is completed; the cost of insertion is based on the arc cost
    # function. Is faster than PARALLEL_CHEAPEST_INSERTION.
    # """
    "SEQUENTIAL_CHEAPEST_INSERTION": routing_enums_pb2.FirstSolutionStrategy.SEQUENTIAL_CHEAPEST_INSERTION,



    # """Iteratively build a solution by inserting each node at its cheapest
    # position; the cost of insertion is based on the arc cost function.
    # Differs from PARALLEL_CHEAPEST_INSERTION by the node selected for
    # insertion; here nodes are considered in decreasing order of distance to
    # the start/ends of the routes, i.e. farthest nodes are inserted first.
    # Is faster than SEQUENTIAL_CHEAPEST_INSERTION.
    # """
    "LOCAL_CHEAPEST_INSERTION": routing_enums_pb2.FirstSolutionStrategy.LOCAL_CHEAPEST_INSERTION,



    # """Same as LOCAL_CHEAPEST_INSERTION except that the cost of insertion is
    # based on the routing model cost function instead of arc costs only.
    # """
    "LOCAL_CHEAPEST_COST_INSERTION": routing_enums_pb2.FirstSolutionStrategy.LOCAL_CHEAPEST_COST_INSERTION,



    # """--- Variable-based heuristics ---
    # Iteratively connect two nodes which produce the cheapest route segment.
    # """
    "GLOBAL_CHEAPEST_ARC": routing_enums_pb2.FirstSolutionStrategy.GLOBAL_CHEAPEST_ARC,



    # """Select the first node with an unbound successor and connect it to the
    # node which produces the cheapest route segment.
    # """
    "LOCAL_CHEAPEST_ARC": routing_enums_pb2.FirstSolutionStrategy.LOCAL_CHEAPEST_ARC,   



    # """Select the first node with an unbound successor and connect it to the
    # first available node.
    # This is equivalent to the CHOOSE_FIRST_UNBOUND strategy combined with
    # ASSIGN_MIN_VALUE (cf. constraint_solver.h).
    # """
    "FIRST_UNBOUND_MIN_VALUE": routing_enums_pb2.FirstSolutionStrategy.FIRST_UNBOUND_MIN_VALUE,
}