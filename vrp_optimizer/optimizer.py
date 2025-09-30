import math
import json

from ortools.constraint_solver import pywrapcp
from settings import settings
from vrp_optimizer.mapping import get_event_times
from vrp_optimizer.constraint_components import is_vehicle_compatible_with_node
from vrp_optimizer.constraint_components_v2 import get_node_penalty_from_rules
from vrp_optimizer.helpers import (
    minute_from_distance,
    show_time_from_minute_of_day,
    get_chassis_activity_between_moves,
    get_chassis_event_for_nodes,
    get_zone_from_distance
)
from vrp_optimizer.routing_distance import (
    get_distance_between_locations_from_matrix,
    distance_between_locations_list_from_matrix
)
from vrp_optimizer.assumptions import (
    VRP_ALGORITHMS,
    VRP_METAHEURISTICS,
    CHASSIS_ACTIVITIES,
    LATE_ARRIVAL_MINUTES,
    MAX_OWNER_SCORE,
    get_carrier_vrp_assumptions
)

class Optimizer:
    def __init__(
        self,
        moves,
        drivers,
        depot_locations,
        yard_locations,
        timezone,
        distance_unit='mi',
        ALGORITHM=VRP_ALGORITHMS["PARALLEL_CHEAPEST_INSERTION"],
        METAHEURISTIC_STRATEGY=VRP_METAHEURISTICS["GUIDED_LOCAL_SEARCH"],
        time_limit = 5 * 60,  # in seconds (5 minutes)
        equipment_validations = [],
        location_distance_matrix = [],
        plan_start_minute = 0,
        plan_end_minute = 1440,
        carrier_id = None,
        plan_date = None,
        max_time_dimension_minutes = 1440,
        chassis_limits = {},
        driver_history = {},
        is_in_day_plan = False,
        dispatch_plan_params = {},
        restrict_load_move_and_that_driver = []
    ):
        """
            Optimizer is a class that optimizes the moves across the drivers efficiently.
            It uses the OR-Tools library to solve the VRP problem.

            At the Core, it is a Constraint Programming Problem (CP), Which means that we first define the constraints and then the solver finds the best solution.

            Terminology:
            Moves -> "Nodes"
            Drivers -> "Vehicles"
            Yard Locations -> "Depots"

            The Optimizer is initialized with the following parameters:
            moves: List[Dict[str, Any]]
            drivers: List[Dict[str, Any]]
            depot_locations: List[Dict[str, Any]]
            timezone: str
            carrier_id: str (optional) - Carrier ID for carrier-specific assumptions

            To use the Optimizer, the following steps are followed:
            1. Create an instance of the Optimizer with the required parameters.
            2. Call the optimize() method to get the optimized moves.

            The optimize() method returns the time on which each move is need to be visited.
        """
        for depot in depot_locations:
            depot['isDepot'] = True

        # Get carrier-specific assumptions
        self.assumptions = get_carrier_vrp_assumptions(carrier_id)
        self.carrier_id = carrier_id
        self.chassis_limits = chassis_limits
        self.dispatch_plan_params = dispatch_plan_params
        
        self.distance_unit = distance_unit
        self.time_limit = time_limit
        self.YARDS = yard_locations
        self.EQUIPMENT_VALIDATIONS = equipment_validations
        self.LOCATION_DISTANCE_MATRIX = location_distance_matrix
        self.DEPOTS = self.filter_depots(depot_locations, drivers)
        self.data = self.create_data(self.DEPOTS + moves, drivers, restrict_load_move_and_that_driver)
        self.VEHICLES = self.data['VEHICLES']
        self.NODE_DATA = self.data['NODE_DATA']
        self.MINUTE_MATRIX = self.data['MINUTE_MATRIX']
        self.DISTANCE_MATRIX = self.data['DISTANCE_MATRIX']
        self.CHASSIS_MATRIX = self.data['CHASSIS_MATRIX']
        self.route_index_dict = self.data['route_index_dict']
        self.timezone = timezone
        self.ALGORITHM = ALGORITHM
        self.METAHEURISTIC_STRATEGY = METAHEURISTIC_STRATEGY
        self.plan_start_minute = plan_start_minute
        self.plan_end_minute = plan_end_minute
        self.plan_date = plan_date
        self.max_time_dimension_minutes = max_time_dimension_minutes
        self.driver_history = driver_history
        self.is_in_day_plan = is_in_day_plan

        if settings.NODE_ENV == "local":
            self.time_limit = 30

    def filter_depots(self, depot_locations, drivers):
        """
            This method filters the depots. It only returns the depots that have atleast one vehicle.
        """

        depots = []

        vehicle_depots = set([v.get('depot_hash_key') for v in drivers])
        for depot in depot_locations:
            hash_key = depot.get('hash_key')
            is_depot_already_present = any(d.get('hash_key') == hash_key for d in depots)
            if hash_key in vehicle_depots and not is_depot_already_present:
                depots.append(depot)

        return depots

    def create_data(self, nodes, vehicles, restrict_load_move_and_that_driver):
        """
            This method creates the data for the Optimizer.
            It creates the minute matrix and the distance matrix.
            Usage of Each Matrix:
                - Minute Matrix:
                    - This matrix is used to store the time taken to travel from one node to another.
                    - Travel time from source to destination is calculated like this:
                        - Time to cover the distance between source move's end location and destination move's start location
                        - Time to cover the distance in the destination move
                        - Time to wait at the destination move
                - Distance Matrix:
                    - This matrix is used to store the distance between two nodes. ( Excluding from and to the depot )

            IMPORTANT: As we are also considering the destination move's waiting time and it's distance, "Visiting a Node" means that, the visited node is completed.
            It also creates the node data.
        """
        try:
            minute_matrix = []
            distance_matrix = []
            chassis_matrix = []

            self.hook_arcs = {}
            self.forbidden_arcs = []

            # MATRIX
            for i in range(len(nodes)):
                minute_matrix.append([0] * len(nodes))
                distance_matrix.append([0] * len(nodes))
                chassis_matrix.append([{ "activity": CHASSIS_ACTIVITIES["NO_ACTION"] }] * len(nodes))

            for i1, source in enumerate(nodes):
                for i2, destination in enumerate(nodes):

                    if i1 == i2:
                        continue

                    is_chassis_activity_needed = False

                    # Get locations once to avoid repeated dict lookups
                    source_end = source.get('end_loc')
                    dest_start = destination.get('start_loc')
                    
                    # Calculate distance and time metrics
                    distance_between_nodes = get_distance_between_locations_from_matrix(source_end, dest_start, self.LOCATION_DISTANCE_MATRIX)

                    chassis_activity = get_chassis_activity_between_moves(
                        source,
                        destination,
                        self.EQUIPMENT_VALIDATIONS,
                        self.YARDS,
                        self.LOCATION_DISTANCE_MATRIX,
                        self.carrier_id
                    )

                    if chassis_activity.get('type') != CHASSIS_ACTIVITIES["NO_ACTION"]:
                        is_chassis_activity_needed = True

                        if chassis_activity.get('type') in [CHASSIS_ACTIVITIES["HOOKCHASSIS"], CHASSIS_ACTIVITIES["DROP_AND_HOOK_CHASSIS"]] and chassis_activity.get('chassis'):
                            location_id = chassis_activity.get('hook_yard', {}).get('location_id')
                            chassis = '_'.join(chassis_activity.get('chassis'))

                            if location_id and chassis != '_':
                                self.hook_arcs.setdefault(location_id+'_'+chassis, []).append((i1, i2))

                        # Calculate distance and time metrics for chassis activity
                        locations = [source_end]
                        if chassis_activity.get('drop_yard'):
                            locations.append(chassis_activity.get('drop_yard').get('location'))
                        if chassis_activity.get('hook_yard'):
                            locations.append(chassis_activity.get('hook_yard').get('location'))
                        locations.append(dest_start)

                        distance_between_nodes = distance_between_locations_list_from_matrix(locations, self.LOCATION_DISTANCE_MATRIX)

                        chassis_matrix[i1][i2] = {
                            "activity": chassis_activity['type'],
                            "yard_location": chassis_activity.get('drop_yard') or chassis_activity.get('hook_yard'),
                            "drop_yard": chassis_activity.get('drop_yard'),
                            "hook_yard": chassis_activity.get('hook_yard')
                        }

                    minute_to_cover_distance = minute_from_distance(distance_between_nodes, self.distance_unit)
                    
                    # Get destination metrics once
                    dest_minutes = destination.get('minutes_on_road', 0)
                    dest_waiting = destination.get('total_waiting_time', 0)
                    dest_early_arrival_waiting = destination.get('early_arrival_waiting', 0)
                    
                    # Calculate total minutes and store in matrix

                    minutes = 0

                    if source.get('isDepot') and destination.get('isDepot') and source.get('hash_key') == destination.get('hash_key'):
                        minutes = 0
                    elif source.get('last_move_by_driver', False) and not destination.get('isDepot'):
                        # Make all the container moves (not the depots) unaccessible from the free flow trip.
                        minutes = self.assumptions.get('MAX_MINUTES')
                    else:
                        minutes = int(
                            minute_to_cover_distance +
                            (self.assumptions.get('TIME_TO_SWITCH_CHASSIS') if is_chassis_activity_needed else 0) +
                            dest_minutes +
                            dest_waiting +
                            dest_early_arrival_waiting +
                            (self.assumptions.get('BUFFER_TIME_FOR_NEXT_NODE') if minute_to_cover_distance > 0 else 0)
                        )

                    minute_matrix[i1][i2] = minutes

                    # Set distance matrix value based on depot status
                    distance_matrix[i1][i2] = (
                        0 if source.get('isDepot') and destination.get('isDepot') and source.get('hash_key') == destination.get('hash_key')
                        else math.ceil(distance_between_nodes)
                    )

                    # Track forbidden arcs while building the matrix
                    if distance_matrix[i1][i2] > self.assumptions.get('MAX_EMPTY_MILES'):
                        self.forbidden_arcs.append((i1, i2))
            
            # NODE DATA
            node_data = []
            depot_index_dict = {}
            route_index_dict = {}
            move_driver_restrictions = {}
            for restriction in restrict_load_move_and_that_driver:
                driver_id = restriction.get('driver_id')
                for move in restriction.get('moves', []):
                    # check same key existing in self.move_driver_restrictions
                    load_id = move.get('load_id')

                    if move.get('move_id'):
                        load_id = f"{load_id}-{move.get('move_id')}"

                    if not load_id in move_driver_restrictions:
                        move_driver_restrictions[load_id] = []

                    move_driver_restrictions[load_id].append(driver_id)

            for node_index, node in enumerate(nodes):
                total_waiting_time = node.get('total_waiting_time', 0)
                minutes_on_road = node.get('minutes_on_road', 0)
                early_arrival_waiting = node.get('early_arrival_waiting', 0)
                route_distance = node.get('route_distance', 0)

                node_details = {
                    **node,
                    'route_distance': int(route_distance),
                    'locations': node.get('locations', []),
                    'time_to_process_move': int(
                        total_waiting_time +
                        minutes_on_road +
                        early_arrival_waiting +
                        self.assumptions.get('BUFFER_TIME_FOR_NEXT_NODE')
                    ),
                    'reference_number': node.get('_id', ''),
                    'move': node.get('move', []),
                    'move_index': node.get('move_index', 0),
                    'available_range': (
                        node.get('expected_from_minute', 0),
                        node.get('expected_to_minute', 1439)
                    ),
                    'start_loc': node.get('start_loc', []),
                    'end_loc': node.get('end_loc', []),
                    'terminal': node.get('terminal', ""),
                    'isDepot': node.get('isDepot', False),
                    'strictly_coupled_move': node.get('strictly_coupled_move', None),
                }

                route_index_dict[node.get('_id')+'_'+str(node.get('move_index'))] = node_index

                if node.get('isDepot'):
                    depot_index_dict[node.get('hash_key')] = node_index

                move_key = node.get('load_id', None)
                if not node.get('is_combined_trip', False) and not node.get('is_free_flow_move', False):
                    move_id = node.get('move')[-1]['moveId'] if node.get('move') else None
                    if move_id:
                        move_key = f"{move_key}-{move_id}"
                    
                # find this move_key in move_driver_restrictions
                if move_key in move_driver_restrictions:
                    node_details['restricted_driver_for_move'] = move_driver_restrictions[move_key]

                node_data.append(node_details)

            for node_index, node in enumerate(node_data):
                node['previous_node'] = route_index_dict.get(node.get('reference_number')+'_'+str(node.get('move_index')-1), None)
                if node.get('previous_node') is None:
                    del node['previous_node']

            for v in vehicles:
                v['start_node'] = depot_index_dict.get(v.get('depot_hash_key')) if v.get('depot_hash_key') else 0
                v['end_node'] = depot_index_dict.get(v.get('depot_hash_key')) if v.get('depot_hash_key') else 0

            return {
                'MINUTE_MATRIX': minute_matrix,
                'DISTANCE_MATRIX': distance_matrix,
                'CHASSIS_MATRIX': chassis_matrix,
                'NODE_DATA': node_data,
                'VEHICLES': vehicles,
                'route_index_dict': route_index_dict
            }
        except Exception as e:
            print(f"Carrier: {self.carrier_id}, Error in create_data: {str(e)}")
            raise e
    
    def init_solver(self):
        """
            This method initializes the solver.
        """
        self.manager = pywrapcp.RoutingIndexManager(
            len(self.MINUTE_MATRIX), 
            len(self.VEHICLES), 
            [d.get('start_node') for d in self.VEHICLES],  # list of start depots for each vehicle
            [d.get('end_node') for d in self.VEHICLES]   # list of end depots for each vehicle
        )
        self.routing = pywrapcp.RoutingModel(self.manager)
    
    def _calculate_vehicle_rotation_penalty(self, work_stats):
        """
        Calculate rotation penalty based on driver's recent work history.
        
        Args:
            vehicle: Vehicle/driver dictionary
            
        Returns:
            int: Rotation penalty value
        """
        # Calculate penalty
        days_worked = work_stats['days_worked']
        if days_worked == 0:
            # Driver hasn't worked recently - no penalty (prioritize them)
            return 0
        
        # Base penalty for each day worked
        base_penalty = self.assumptions.get('ROTATION_PENALTY_BASE', 20)
        penalty = base_penalty * days_worked


        # Add exponential penalty for consecutive days
        multiplier = self.assumptions.get('ROTATION_PENALTY_MULTIPLIER', 1.2)
        consecutive_days = work_stats['consecutive_days']
        if consecutive_days > 1:
            penalty += base_penalty * (multiplier ** (consecutive_days - 2))
        
        # Cap the penalty
        max_penalty = self.assumptions.get('MAX_ROTATION_PENALTY', 300)
        penalty = min(penalty, max_penalty)
        
        return int(penalty)

    def add_disjunctions_and_penalties(self):
        """
            This method adds the disjunctions and penalties to the solver.

            For the Nodes:
            By Adding Disjunction to a node, we are saying that, a node is skippable but it will have a penalty.
            The penalty is according to the distance of the node because the longer distance nodes need to be looked earlier so that the vehicle with better mileage can be used.
            For Assigned Moves, we add a penalty of max_skip_penalty + ADDITIONAL_PENALTY_FOR_ASSIGNED_MOVE

            For the Nodes and Vehicles Relation:
            Restrict the vehicle from visiting a node:
                - If the node is assigned to a vehicle, then restrict other vehicles from visiting it. ( Only allowing one vehicle to visit the node was causing no result issues )
                - If the node's internal distance is greater than the vehicle's max miles per move.
                - If the node's locations are restricted for the vehicle.

            For the Vehicles:
            Set the penalty for using a vehicle. ( It helps in reducing the number of vehicles used ) 
        """

        for node in range(len(self.DEPOTS), len(self.NODE_DATA)):
            node_spec = self.NODE_DATA[node]

            skip_penalty = self.assumptions.get('SKIP_NODE_PENALTY')   # Prioritize longer routes
            
            if node_spec.get('is_optional_move', False):
                skip_penalty = self.assumptions.get('SKIP_NODE_PENALTY_FOR_OPTIONAL_MOVE')
            
            if node_spec.get('suggested_driver', False):
                skip_penalty = skip_penalty + self.assumptions.get('ADDITIONAL_PENALTY_FOR_ASSIGNED_MOVE')

            self.routing.AddDisjunction([self.manager.NodeToIndex(node)], skip_penalty, True)

        for vehicle_id, vehicle in enumerate(self.VEHICLES):
            for node_id, node_specification in enumerate(self.NODE_DATA):
                # Skip depot nodes in vehicle constraints
                if node_specification.get('isDepot', False):
                    continue

                index = self.manager.NodeToIndex(node_id)
                
                assigned_driver = node_specification.get('suggested_driver', '')
                if assigned_driver:
                    if assigned_driver != vehicle['_id']:
                        self.routing.solver().Add(self.routing.VehicleVar(index) != vehicle_id)
                    continue

                restricted_driver = node_specification.get('restricted_driver_for_move', [])
                if restricted_driver:
                    if vehicle['_id'] in restricted_driver:
                        self.routing.solver().Add(self.routing.VehicleVar(index) != vehicle_id)
                        continue


                is_compatible, _ = is_vehicle_compatible_with_node(vehicle, node_specification)
                if not is_compatible:
                    self.routing.solver().Add(self.routing.VehicleVar(index) != vehicle_id)

        for vehicle_id, vehicle in enumerate(self.VEHICLES):
            if vehicle.get('is_working', False):
                self.routing.SetFixedCostOfVehicle(0, vehicle_id)
                continue

            owner_score = vehicle.get('owner_score', 1)
            scale = MAX_OWNER_SCORE - owner_score + 1
            SCALED_VEHICLE_USE_PENALTY = scale * self.assumptions.get('VEHICLE_USE_PENALTY')

            if self.dispatch_plan_params.get('distribute_work', False):
                SCALED_VEHICLE_USE_PENALTY = 0
            
            # NEW: Calculate rotation penalty
            work_stat = self.driver_history.get(vehicle.get('_id'), {})
            if work_stat:
                rotation_penalty = self._calculate_vehicle_rotation_penalty(work_stat)
                
                # Set total penalty including rotation
                total_penalty = SCALED_VEHICLE_USE_PENALTY + rotation_penalty
                
                work_stat['penalty'] = total_penalty
                self.routing.SetFixedCostOfVehicle(total_penalty, vehicle_id)
            else:
                self.routing.SetFixedCostOfVehicle(SCALED_VEHICLE_USE_PENALTY, vehicle_id)

    def add_forbidden_long_distance_sequences(self):
        """
        Apply hard constraints for all forbidden arcs including depot transitions.
        """
        for from_node_id, to_node_id in self.forbidden_arcs:
            from_node = self.NODE_DATA[from_node_id]            
            from_index = self.manager.NodeToIndex(from_node_id)
            to_index = self.manager.NodeToIndex(to_node_id)
            
            # Handle depot-to-node case (first move from depot)
            if from_node.get('isDepot'):
                for vehicle_id, vehicle in enumerate(self.VEHICLES):
                    if vehicle.get('start_node') == from_node_id:
                        start_index = self.routing.Start(vehicle_id)
                        self.routing.solver().Add(
                            self.routing.NextVar(start_index) != to_index
                        )
            
            # Handle regular node-to-node case
            else:
                self.routing.solver().Add(
                    self.routing.NextVar(from_index) != to_index
                )

    def add_time_dimension(self):
        """
            This method adds the time dimension to the solver.
            The RoutingModel allows us to track the cumulative units as the vehicle visits the nodes, which is called as "Dimensions".

            Here we are adding the time dimension to the solver.

            This time dimension is used to track the time taken to visit the nodes in a vehicle's journey.
        """
        def travel_time(from_index, to_index):
            from_node = self.manager.IndexToNode(from_index)
            to_node = self.manager.IndexToNode(to_index)
            return self.MINUTE_MATRIX[from_node][to_node]

        transit_callback_index = self.routing.RegisterTransitCallback(travel_time)
        self.routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        # Add Time Dimension (Tracks Vehicle Time)
        time = 'Time'
        self.routing.AddDimension(
            transit_callback_index,
            self.assumptions.get('MAX_WAITING_TIME_BETWEEN_NODES', 60),           # The time in minutes to wait before visiting the next node
            self.max_time_dimension_minutes,              # Max time
            False,             # Don't force start at zero
            time
        )
        self.time_dimension = self.routing.GetDimensionOrDie(time)

    def add_time_dimension_constraints(self):
        """
            This method adds the constraints to the time dimension.

            As Now we can track the time taken to visit the nodes in a vehicle's journey, we can add the constraints to the time dimension.

            For the Nodes:
                - Set the range of the time dimension for the node. ( According to the appointment time )
                - If the appointment window is smaller then, make vehicle reach earlier than window's end.

            For the Vehicles:
                - Set a Penalty If woke up early
                - Try to maximize the working minutes of a driver
        """
        for location_idx, node_data in enumerate(self.NODE_DATA):
            index = self.manager.NodeToIndex(location_idx)
            window = node_data['available_range']

            allow_late_arrivals_upto_n_minutes = LATE_ARRIVAL_MINUTES
            # Check if you want to allow lateness up to a certain limit
            if allow_late_arrivals_upto_n_minutes > 0:
                # 1. Set the PENALTY (the "fine") starting at the ideal deadline.
                #    It's better to use a fixed penalty from your assumptions for easier tuning.
                penalty_for_late_arrival = round(self.assumptions['SKIP_NODE_PENALTY'] / allow_late_arrivals_upto_n_minutes)
                self.time_dimension.SetCumulVarSoftUpperBound(
                    index,
                    window[1] + 1,
                    penalty_for_late_arrival
                )

                # 2. Set the HARD LIMIT (the "physical barrier").
                #    This is the original deadline plus your allowed buffer.
                max_allowed_time = window[1] + allow_late_arrivals_upto_n_minutes
                self.time_dimension.CumulVar(index).SetMax(max_allowed_time)

                # 3. Ensure the earliest time is still respected.
                self.time_dimension.CumulVar(index).SetMin(window[0] + 1)
            else:
                # Your original hard constraint logic
                self.time_dimension.CumulVar(index).SetRange(window[0] + 1, window[1] + 1)

            # Ensure parent node is visited before child node
            if node_data.get('previous_node') is not None:
                previous_node = node_data.get('previous_node')
                previous_node_index = self.manager.NodeToIndex(previous_node)
                node_index = self.manager.NodeToIndex(location_idx)

                previous_node_time = self.time_dimension.CumulVar(previous_node_index)
                node_time = self.time_dimension.CumulVar(node_index)
                
                self.routing.solver().Add(previous_node_time <= node_time)

                # ADD THIS to ensure the previous move must also be performed.
                self.routing.solver().Add(
                    self.routing.ActiveVar(node_index) <= self.routing.ActiveVar(previous_node_index)
                )


        total_working_minutes = None
        # Set vehicle working hours, max working minutes, and max weight constraints
        for vehicle_id, vehicle in enumerate(self.VEHICLES):
            start_index = self.routing.Start(vehicle_id)
            end_index = self.routing.End(vehicle_id)

            vehicle_start_minute = vehicle.get('start_minute') + 1
            vehicle_end_minute = vehicle.get('end_minute') + 1

            self.time_dimension.CumulVar(start_index).SetRange(vehicle_start_minute, vehicle_end_minute)
            self.time_dimension.CumulVar(end_index).SetRange(vehicle_start_minute, vehicle_end_minute)

            
            diff_of_vehicle_start_and_end_time = vehicle_end_minute - vehicle_start_minute
            is_full_day_vehicle = diff_of_vehicle_start_and_end_time > 1380
            
            is_hos_affected = vehicle.get('is_hos_affected', False)
            if is_hos_affected and is_full_day_vehicle:
                is_hos_affected = vehicle_start_minute > self.assumptions.get('PREFERRED_START_TIME')


            max_vehicle_start_minute = vehicle_end_minute
            if vehicle.get('is_working', False) or is_hos_affected:
                # For working vehicles, add 30 min buffer to start time
                max_vehicle_start_minute = vehicle_start_minute + 30
            else:
                # For non-working vehicles, calculate max start time with buffer
                preferred_start = self.assumptions.get('PREFERRED_START_TIME') + 1
                max_start = max(vehicle_start_minute, preferred_start)
                buffer_time = 3 * 60 - self.assumptions.get('MAX_WAITING_TIME_BETWEEN_NODES', 60)
                max_vehicle_start_minute = max(max_start + buffer_time, vehicle_start_minute + 1)

            self.time_dimension.SetCumulVarSoftUpperBound(start_index, max_vehicle_start_minute, self.assumptions.get('PENALTY_FOR_LATE_START'))


            if is_full_day_vehicle:
                # TODO: We should have dynamic soft upper bound here, for all drivers PREFERRED_START_TIME cannot be same to prevent early invocation
                self.time_dimension.SetCumulVarSoftLowerBound(start_index, self.assumptions.get('PREFERRED_START_TIME') + 1, self.assumptions.get('PENALTY_FOR_EARLY_INVOCATION'))

            # Max working minutes constraint
            working_minutes = self.time_dimension.CumulVar(end_index) - self.time_dimension.CumulVar(start_index)
            
            if not self.is_in_day_plan or vehicle.get('is_working', False):
                if total_working_minutes is None:
                    total_working_minutes = working_minutes
                else:
                    total_working_minutes += working_minutes

            # Limit working time to max allowed for this vehicle
            self.routing.solver().Add(working_minutes <= vehicle['max_working_minutes'])

        if total_working_minutes and not self.dispatch_plan_params.get('distribute_work', False): # If distribute work is true, then we don't need to limit the working minutes:
            self.routing.AddVariableMaximizedByFinalizer(total_working_minutes)

    def add_distance_dimension(self):
        """
            This method adds the distance dimension to the solver.

            Using this dimesion, we can track the distance between the nodes in a vehicle's journey. ( Excluding the distance from and to the depots )
        """
        def travel_distance(from_index, to_index):
            from_node = self.manager.IndexToNode(from_index)
            to_node = self.manager.IndexToNode(to_index)
            return self.DISTANCE_MATRIX[from_node][to_node]

        transit_callback_index = self.routing.RegisterTransitCallback(travel_distance)
        self.routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        # Add Distance Dimension (Tracks Vehicle Distance)
        distance = 'TravelDistance'
        self.routing.AddDimension(
            transit_callback_index,
            0,
            100000,
            True,
            distance
        )
        self.distance_dimension = self.routing.GetDimensionOrDie(distance)

    def add_dual_transaction_constraints(self):
        """
        Add a dimension that counts dual transactions and maximizes them
        """
        def dual_transaction_callback(from_index, to_index):
            from_node = self.manager.IndexToNode(from_index)
            to_node = self.manager.IndexToNode(to_index)
            
            base_distance = self.DISTANCE_MATRIX[from_node][to_node]
            
            # Return 1 for dual transactions, 0 otherwise
            return 1 if base_distance == 0 else 0
        
        callback_index = self.routing.RegisterTransitCallback(dual_transaction_callback)
        
        # Add dimension to track dual transactions
        self.routing.AddDimension(
            callback_index,
            0,  # null slack
            1000,  # max cumulative dual transactions per vehicle
            True,  # start at zero
            'DualTransactions'
        )

        dual_transaction_dimension = self.routing.GetDimensionOrDie('DualTransactions')
        
        # Create a sum variable for all vehicles
        total_dual_transactions = None
        for vehicle_id in range(len(self.VEHICLES)):
            end_index = self.routing.End(vehicle_id)
            vehicle_dual_transactions = dual_transaction_dimension.CumulVar(end_index)
            
            if total_dual_transactions is None:
                total_dual_transactions = vehicle_dual_transactions
            else:
                total_dual_transactions += vehicle_dual_transactions
        
        # Maximize the TOTAL across all vehicles
        if total_dual_transactions is not None:
            self.routing.AddVariableMaximizedByFinalizer(total_dual_transactions)


    def add_chassis_limit(self):
        for chassis, arcs in self.hook_arcs.items():
            max_chassis_available = self.chassis_limits.get(chassis, 0)
            
            # Convert arcs list to set of tuples for O(1) lookup
            arcs_set = set((arc[0], arc[1]) for arc in arcs)
            
            # Create a callback that counts chassis usage
            def make_demand_callback(current_arcs_set):
                def demand_callback(from_index, to_index):
                    from_node = self.manager.IndexToNode(from_index)
                    to_node = self.manager.IndexToNode(to_index)
                    # Check if this arc uses this chassis
                    if (from_node, to_node) in current_arcs_set:
                        return 1
                    return 0
                return demand_callback
            
            demand_callback = make_demand_callback(arcs_set)
            demand_callback_index = self.routing.RegisterTransitCallback(demand_callback)
            
            # Add dimension
            dimension_name = f'chassis_{chassis}_capacity'
            self.routing.AddDimension(
                demand_callback_index,
                0,  # null capacity slack
                10000,  # max cumul value per vehicle route
                True,  # start cumul to zero
                dimension_name
            )
            
            # Get the dimension
            chassis_dimension = self.routing.GetDimensionOrDie(dimension_name)
            
            # chassis_dimension.SetGlobalSpanCostCoefficient(max_chassis_available)
            
            solver = self.routing.solver()
            end_cumuls = []
            for vehicle_id in range(self.routing.vehicles()):
                end_index = self.routing.End(vehicle_id)
                end_cumuls.append(chassis_dimension.CumulVar(end_index))
            
            # Sum of all end cumuls must be <= max_chassis_available
            solver.Add(solver.Sum(end_cumuls) <= max_chassis_available)

    def _calculate_zone_penalty(self, work_stats, current_zone):
        """
        Calculate penalty for zone repetition to encourage variety.
        
        Penalties:
        - Repeating same zone type: Incremental penalty
        - Fatigue factor: Extra penalty after consecutive hard days
        """
        # Get driver's zone history
        zone_counts = work_stats.get('zone_counts', {})
        current_zone_count = zone_counts.get(current_zone, 0)
        consecutive_days = work_stats.get('consecutive_days', 0)
        
        penalty = 0
        base_penalty = self.assumptions.get('ZONE_REPEAT_PENALTY_BASE', 10)
        
        # 1. Penalty for repeating the same zone
        penalty += base_penalty * (1.5 ** current_zone_count)
        
        # 2. Zone imbalance penalty (encourage even distribution)
        total_moves = sum(zone_counts.values())
        if total_moves > 0:
            current_zone_percentage = current_zone_count / total_moves
            if current_zone_percentage > 0.4:  # If more than 40% of moves are in same zone
                penalty += base_penalty * current_zone_percentage * 2
        
        # 3. Fatigue penalty (avoid giving hard routes to tired drivers)
        if consecutive_days >= 3 and current_zone in ['outofzone', 'longhaul']:
            penalty += base_penalty * consecutive_days
        
        # 4. Fresh driver bonus for hard routes
        if consecutive_days == 0 and current_zone in ['outofzone', 'longhaul']:
            penalty -= base_penalty  # Encourage fresh drivers to take longer routes
        
        # Cap the zone penalty to avoid overwhelming other constraints
        max_zone_penalty = self.assumptions.get('MAX_ZONE_PENALTY', 100)
        penalty = max(min(penalty, max_zone_penalty), 0)
        
        return int(penalty)


    def add_arc_constraint_per_vehicle(self):
        """
        Add a custom arc cost evaluator per vehicle.
        It includes the following penalties:
        - Zone repetition and variety penalty
        - Carrier specific rules constraints penalty
        - Penalty for empty miles
        - Penalty for long distances
        - Penalty for non-dual moves
        """

        def make_distance_callback(vehicle_id):
            def distance_callback(from_index, to_index):
                try:
                    vehicle = self.VEHICLES[vehicle_id]
                    from_node = self.manager.IndexToNode(from_index)
                    to_node = self.manager.IndexToNode(to_index)
                    base_distance = self.DISTANCE_MATRIX[from_node][to_node]
                    is_chassis_change = self.CHASSIS_MATRIX[from_node][to_node].get('activity') != CHASSIS_ACTIVITIES["NO_ACTION"]

                    from_node_spec = self.NODE_DATA[from_node]
                    to_node_spec = self.NODE_DATA[to_node]
                    max_distance = to_node_spec.get('max_distance', 0)
                    
                    # get the cost from the distance
                    cost = int(base_distance)

                    # Apply graduated penalty between preferred and max empty miles
                    if base_distance > self.assumptions.get('PREFERRED_MAX_EMPTY_MILES'):
                        preferred_max = self.assumptions.get('PREFERRED_MAX_EMPTY_MILES') # soft upper bound for empty miles
                        max_empty = self.assumptions.get('MAX_EMPTY_MILES') # hard upper bound for empty miles
                        max_penalty = self.assumptions.get('SKIP_NODE_PENALTY')
                        
                        # Calculate penalty that scales linearly from 0 to max_penalty
                        penalty_ratio = (base_distance - preferred_max) / (max_empty - preferred_max)
                        penalty = max_penalty * min(penalty_ratio, 1.0)

                        if base_distance > max_empty:
                            cost += int(max_penalty * 2)

                        cost = int(penalty)

                    # Penalties for carrier specific rules constraints
                    cost += get_node_penalty_from_rules(
                        carrier=self.carrier_id,
                        from_node=from_node_spec,
                        node=to_node_spec,
                        vehicle=vehicle,
                        base_distance=base_distance
                    )

                    # add the penalty for chassis change
                    if is_chassis_change and not from_node_spec.get('isDepot') and not to_node_spec.get('isDepot'):
                        cost += self.assumptions.get('CHASSIS_CHANGE_PENALTY')

                    # if the base_distance is zero, then it's a dual move
                    if base_distance == 0:
                        return cost
                    
                    # otherwise add the penalty for non-dual move
                    cost += self.assumptions.get('NON_DUAL_MOVE_PENALTY')

                    # Penalties for zone repetition and variety
                    work_stats = self.driver_history.get(vehicle.get('_id'), {})
                    if work_stats and max_distance > 0 and base_distance > 0:
                        zone = get_zone_from_distance(max_distance)
                        zone_penalty = self._calculate_zone_penalty(work_stats, zone)
                        cost += zone_penalty

                    # this is for slow season, to encourage more assignments
                    if self.dispatch_plan_params.get('distribute_work', False):
                        # Reduce penalty for first visits to encourage more assignments.
                        if from_node_spec.get('isDepot') and not to_node_spec.get('isDepot'):
                            cost = max(min(base_distance, 5), cost - 100)
                    
                    return cost
                except Exception as e:
                    print(f"Carrier: {self.carrier_id}, Error in distance callback: {e}")
                    return self.assumptions.get('SKIP_NODE_PENALTY')
            return distance_callback

        # Register and assign individual callbacks
        for vehicle_id in range(len(self.VEHICLES)):
            callback = make_distance_callback(vehicle_id)
            callback_index = self.routing.RegisterTransitCallback(callback)
            self.routing.SetArcCostEvaluatorOfVehicle(callback_index, vehicle_id)


    def add_strictly_coupled_moves(self):
        """
            This method adds the strictly coupled moves to the solver. For example: 1-1 delivery and return pickup. 
        """
        for node in self.NODE_DATA:
            if node.get('strictly_coupled_move'):
                coupled_move = next((m for m in self.NODE_DATA if m['reference_number'] == node.get('strictly_coupled_move')), None)
                
                if coupled_move:
                    node_key = node.get('reference_number')+'_'+str(node.get('move_index'))
                    coupled_move_key = coupled_move.get('reference_number')+'_'+str(coupled_move.get('move_index'))

                    index1 = self.manager.NodeToIndex(self.route_index_dict.get(node_key))
                    index2 = self.manager.NodeToIndex(self.route_index_dict.get(coupled_move_key))

                    self.routing.AddPickupAndDelivery(index1, index2)
                    self.routing.solver().Add(self.routing.VehicleVar(index1) == self.routing.VehicleVar(index2))
    
    def set_search_parameters(self):
        """
            This method sets the search parameters for the solver.

            First Solution Strategy:
                - It is the strategy that the solver will use to find the first solution.
                - It is faster than the metaheuristic search.

            Local Search Metaheuristic:
                - It is the strategy that the solver will use to find the best solution.
                - It is slower than the first solution strategy, but it can find a better solution.
                - If used with the first solution strategy, it'll try to optimize the solution found by the first strategy. which is better than solving the problem from scratch.

            Time Limit:
                - It is the time limit for the solver to find the solution.
                - It is set to 5 minutes.
        """
        self.search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        self.search_parameters.first_solution_strategy = self.ALGORITHM # PATH_MOST_CONSTRAINED_ARC, PATH_CHEAPEST_ARC, AUTOMATIC, PARALLEL_CHEAPEST_INSERTION
        self.search_parameters.local_search_metaheuristic = self.METAHEURISTIC_STRATEGY # TABU_SEARCH, SIMULATED_ANNEALING, GUIDED_LOCAL_SEARCH
        self.search_parameters.time_limit.seconds = int(self.time_limit)
        self.search_parameters.log_search = True

    def get_solution(self):
        """
            This method gets the solution from the solver.
        """
        solution = self.routing.SolveWithParameters(self.search_parameters)
        return solution
    
    def manage_solution(self, solution):
        """
        Processes the solution from the solver and generates route details for each vehicle.

        This method:
        1. Extracts time and distance information from the routing solution
        2. For each vehicle, builds a sequence of visited nodes with:
            - Event timing details
            - Location coordinates
            - Distance calculations
            - Driver schedule information
        3. Calculates metrics like empty miles, total hours, and early start penalties
        """
        try:
            route_summaries = []
            self.time_dimension = self.routing.GetDimensionOrDie('Time')
            self.distance_dimension = self.routing.GetDimensionOrDie('TravelDistance')
            
            def get_route_distance(vehicle_id):
                """Calculate total distance for a vehicle's route"""
                route_end = self.routing.End(vehicle_id)
                return solution.Value(self.distance_dimension.CumulVar(route_end))

            for vehicle_id in range(len(self.VEHICLES)):
                route_details = self._process_vehicle_route(
                    vehicle_id=vehicle_id,
                    solution=solution,
                    get_route_distance=get_route_distance
                )
                if route_details:
                    route_summaries.append(route_details)

            return route_summaries

        except (AttributeError, IndexError) as e:
            # Log error and re-raise with more context
            error_msg = f"Failed to process routing solution: {str(e)}"
            raise RuntimeError(error_msg) from e

    def _process_vehicle_route(self, vehicle_id, solution, get_route_distance):
        """
        Process route details for a single vehicle.
        
        Args:
            vehicle_id: ID of the vehicle being processed
            solution: Routing solution object
            get_route_distance: Function to calculate route distance
            
        Returns:
            Dict containing route summary or None if route is invalid
        """
        try:
            visited_nodes = []
            empty_miles = 0
            index = self.routing.Start(vehicle_id)
            prev_node_id = None

            # Process each node in vehicle's route
            while not self.routing.IsEnd(index):
                node_id = self.manager.IndexToNode(index)
                time_var = self.time_dimension.CumulVar(index)
                completion_time = solution.Value(time_var)
                
                node_details = self._process_node(
                    node_id=node_id,
                    completion_time=completion_time,
                    visited_nodes=visited_nodes,
                    prev_node_id=prev_node_id
                )
                
                if node_details:
                    empty_miles += node_details.get('empty_miles', 0)
                    visited_nodes.append(node_details['node_data'])
                    prev_node_id = node_id
                index = solution.Value(self.routing.NextVar(index))

            # Only process routes with actual stops
            if len(visited_nodes) > 1:
                
                # add chassis termination event for the last move
                prev_node_id = node_id
                node_id = self.manager.IndexToNode(index)
                node_data = self.NODE_DATA[node_id]
                chassis_activity_for_node = self.CHASSIS_MATRIX[prev_node_id][node_id]
                if chassis_activity_for_node.get('activity') != CHASSIS_ACTIVITIES["NO_ACTION"]:
                    node_data['chassis_activity'] = chassis_activity_for_node.get('activity')
                    node_data['drop_yard'] = chassis_activity_for_node.get('drop_yard')
                    node_data['hook_yard'] = chassis_activity_for_node.get('hook_yard')

                _, _ = get_chassis_event_for_nodes(
                    visited_nodes[-1], node_data, self.YARDS, self.distance_unit, self.assumptions.get('TIME_TO_SWITCH_CHASSIS')
                )

                if node_data.get('isDepot'):
                    node_data.pop('chassis_activity', None)
                    node_data.pop('drop_yard', None)
                    node_data.pop('hook_yard', None)

                return self._create_route_summary(
                    vehicle_id=vehicle_id,
                    visited_nodes=visited_nodes,
                    empty_miles=empty_miles,
                    get_route_distance=get_route_distance
                )
            return None
        except Exception as e:
            print(f"Carrier: {self.carrier_id}, Error in _process_vehicle_route: {str(e)}")
            return None

    def _process_node(self, node_id, completion_time, visited_nodes, prev_node_id):
        """
        Process details for a single node in the route.
        
        Returns node data and any empty miles to previous node.
        """
        try:
            node_data = self.NODE_DATA[node_id]
            if node_data.get('isDepot', False):
                return {
                    'node_data': {
                        'move_completed_minute': completion_time,
                        'start_loc': node_data['start_loc'],
                        'end_loc': node_data['end_loc'],
                        'isDepot': True
                    }
                }
                
            if not visited_nodes:
                return None
                
            previous_node = visited_nodes[-1]

            chassis_activity_for_node = self.CHASSIS_MATRIX[prev_node_id][node_id]
            if chassis_activity_for_node.get('activity') != CHASSIS_ACTIVITIES["NO_ACTION"]:
                node_data['chassis_activity'] = chassis_activity_for_node.get('activity')
                node_data['drop_yard'] = chassis_activity_for_node.get('drop_yard')
                node_data['hook_yard'] = chassis_activity_for_node.get('hook_yard')

            node_data, previous_node = get_chassis_event_for_nodes(
                previous_node, node_data, self.YARDS, self.distance_unit, self.assumptions.get('TIME_TO_SWITCH_CHASSIS')
            )

            empty_miles = get_distance_between_locations_from_matrix(
                previous_node['end_loc'], 
                node_data['start_loc'],
                self.LOCATION_DISTANCE_MATRIX
            )
            
            event_times = get_event_times(
                node_data=node_data,
                _move=node_data['move'],
                prev_completed_minute=previous_node['move_completed_minute'],
                endTime=completion_time,
                timezone=self.timezone,
                proximity_to_node=empty_miles,
                distance_unit=self.distance_unit,
                time_to_switch_chassis=self.assumptions.get('TIME_TO_SWITCH_CHASSIS'),
                plan_date=self.plan_date
            )

            _node_data = {
                'start_move_minute': event_times[0]['minutes']['recommended_enroute'],
                'move_completed_minute': completion_time,
                'event_times': event_times,
                'start_loc': node_data['start_loc'],
                'end_loc': node_data['end_loc'],
                'reference_number': node_data['reference_number'],
                'move_index': node_data['move_index'],
                'chassis_pick_event': node_data.get('chassis_pick_event'),
            }

            return {
                'empty_miles': empty_miles,
                'node_data': _node_data
            }
        except Exception as e:
            print(f"Carrier: {self.carrier_id}, Error in _process_node: {str(e)}")
            return None

    def _create_route_summary(self, vehicle_id, visited_nodes, empty_miles, get_route_distance):
        """
        Create summary of route including timing, distance and schedule metrics.
        """
        try:
            depot_location = visited_nodes.pop(0)
            first_node = visited_nodes[0]
            last_node = visited_nodes[-1]
            
            # Calculate schedule metrics
            total_time = last_node['move_completed_minute'] - first_node['start_move_minute']
            was_early_start = first_node['start_move_minute'] <= self.assumptions.get('PREFERRED_START_TIME')
            start_time = show_time_from_minute_of_day(first_node['start_move_minute'])

            return {
                'Driver Name': self.VEHICLES[vehicle_id]['_id'],
                'node': visited_nodes,
                'empty_miles': empty_miles,
                'empty_miles_d': get_route_distance(vehicle_id),
                'total_hours': round(total_time / 60, 2),
                'total_moves': len(visited_nodes),
                'was_invoked_early': was_early_start,
                'woke_up_at': start_time
            }
        except Exception as e:
            print(f"Carrier: {self.carrier_id}, Error in _create_route_summary: {str(e)}")
            return None
    
    def optimize(self):
        """
            This method is the main method that optimizes the solution.
        """
        try:
            self.init_solver()
            self.add_disjunctions_and_penalties()
            self.add_forbidden_long_distance_sequences()
            self.add_time_dimension()
            self.add_time_dimension_constraints()
            self.add_distance_dimension()
            self.add_dual_transaction_constraints()
            if self.chassis_limits:
                self.add_chassis_limit()
            self.add_arc_constraint_per_vehicle()
            self.add_strictly_coupled_moves()
            self.set_search_parameters()
            solution = self.get_solution()
            self.answer = self.manage_solution(solution) if solution else []

            if self.answer:
                driver_schedule = self.get_driver_schedule()
                
                print(f"Carrier: {self.carrier_id}, Solution found")
                print(f"Carrier: {self.carrier_id}, Out of {len(self.VEHICLES)} vehicles, {len(self.answer)} vehicles were used")
                print(f"Carrier: {self.carrier_id}, Out of {len(self.NODE_DATA) - len(self.DEPOTS)} moves, {sum([len(m.get('node')) for m in self.answer])} moves were optimized")

                return self.answer, driver_schedule
            else:
                print(f"Carrier: {self.carrier_id}, No solution found")
                print(f"Carrier: {self.carrier_id}, Unplanned Moves: {len(self.NODE_DATA) - len(self.DEPOTS)}")
                print(f"Carrier: {self.carrier_id}, Unplanned Vehicles: {len(self.VEHICLES)}")
                return [], {}
        except Exception as e:
            print(f"Carrier: {self.carrier_id}, Error during vrp optimization: {str(e)}")
            return [], {}
        
    def get_driver_schedule(self):
        """
            This method gets the driver schedule.
        """
        if not self.answer:
            raise Exception("call optimize() first")
        
        driver_schedule = {}
        for driver_data in self.answer:
            visited_loads = driver_data.get('node', [])
            driver_id = driver_data.get('Driver Name', '')
            last_visited_node = visited_loads[-1]
            last_visited_event = last_visited_node.get('event_times', [])[-1]
            driver_schedule[driver_id] = {
                "last_node": {
                    "customer_id": last_visited_event.get('customer_id', ''),
                    "location": last_visited_event.get('map_location', []),
                    "company_name": last_visited_event.get('company_name', '')
                },
                "last_move_end_minute": int(last_visited_node.get('move_completed_minute', 0)),
                "total_working_hours": float(driver_data.get('total_hours', 0))
            }


        return driver_schedule
