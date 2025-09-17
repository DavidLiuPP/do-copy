from vrp_optimizer.constraint_components import is_vehicle_compatible_with_node

def get_unplanned_moves(optimizer, unplanned_reference_numbers):
    invalid_moves = []
    NODE_DATA = optimizer.NODE_DATA
    VEHICLES = optimizer.VEHICLES
    assumptions = optimizer.assumptions

    for node in NODE_DATA:
        try:
            is_unplanned = node.get('reference_number') in unplanned_reference_numbers
            if not is_unplanned:
                continue

            # check driver compatibility first
            constraint_counts = {}
            incompatible_count = 0
            total_vehicles = len(VEHICLES)

            for vehicle in VEHICLES:
                is_compatible, constraint_name = is_vehicle_compatible_with_node(vehicle, node)
                if not is_compatible:
                    incompatible_count += 1
                    if constraint_name:
                        constraint_counts[constraint_name] = constraint_counts.get(constraint_name, 0) + 1
                else:
                    node_complete_window = node.get('available_range', [])
                    node_start_limit = node_complete_window[1] - node.get('time_to_process_move', 0) - 30
                    
                    if vehicle.get('start_minute') > node_start_limit:
                        incompatible_count += 1
                        constraint_counts['WORKING_HOURS_VIOLATION'] = constraint_counts.get('WORKING_HOURS_VIOLATION', 0) + 1

            # Check if more than 80% of drivers are incompatible
            if incompatible_count / total_vehicles > 0.75:
                # Find most common constraint if it appears in >50% of incompatible cases
                most_common_constraint = []
                for constraint, count in constraint_counts.items():
                    if count / incompatible_count > 0.25:
                        most_common_constraint.append(constraint)
                
                if most_common_constraint:
                    invalid_moves.append({
                        'reference_number': node['reference_number'],
                        'reason': f'DRIVER_CONSTRAINT_{"_".join(most_common_constraint).upper()}'
                    })
                    continue

            # If no specific constraint found, check for common failure patterns
            failure_reason = _detect_common_failure_reason(node, VEHICLES, assumptions)
            if failure_reason:
                invalid_moves.append({
                    'reference_number': node['reference_number'],
                    'reason': failure_reason
                })
                continue
        
            # Default fallback
            invalid_moves.append({
                'reference_number': node['reference_number'],
                'reason': 'SKIPPED_BY_OPTIMIZER'
            })
        
        except Exception as e:
            continue

    return invalid_moves

def _detect_common_failure_reason(node, vehicles, assumptions):
    """Detect 70% most common failure reasons with minimal logic"""
    try:
        node_window = node.get('available_range', [0, 1440])
        node_duration = node.get('time_to_process_move', 0)
        suggested_driver = node.get('suggested_driver', '')
        
        if not vehicles or len(vehicles) == 0:
            if suggested_driver:
                return 'SUGGESTED_DRIVER_NOT_AVAILABLE'  # More specific when driver is suggested but no drivers available
            return 'NO_DRIVERS_AVAILABLE'  # General case when no drivers available
        
        # Check if suggested driver exists in available drivers
        if suggested_driver:
            assigned_vehicle = next((v for v in vehicles if v.get('_id') == suggested_driver), None)
            if not assigned_vehicle:
                return 'SUGGESTED_DRIVER_NOT_FOUND'
            
            # Check if suggested driver can handle timing
            driver_end = assigned_vehicle.get('end_minute', 1440)
            if node_window[0] + node_duration > driver_end:
                return 'SUGGESTED_DRIVER_UNAVAILABLE'                                                                
        
        # Check if any driver has enough working hours
        non_compatible_drivers = 0
        total_vehicles = len(vehicles)
        for vehicle in vehicles:
            driver_available_time = vehicle.get('end_minute', 1440) - vehicle.get('start_minute', 0)
            if driver_available_time <= node_duration + 30:
                non_compatible_drivers += 1
        
        if non_compatible_drivers / total_vehicles >= 0.8:
            return 'WORKING_HOURS_EXCEEDED'

        # Check if most vehicles start too late for node's window
        late_start_violations = 0
        node_start = node_window[0]
        for vehicle in vehicles:
            vehicle_start = vehicle.get('start_minute', 0)
            if vehicle_start > node_start:
                late_start_violations += 1
        
        if late_start_violations / total_vehicles >= 0.8:
            return 'WORKING_HOURS_VIOLATION'

        # Check for late arrival penalty violations
        allow_late_arrivals_upto_n_minutes = assumptions.get('LATE_ARRIVAL_MINUTES', 0)
        if allow_late_arrivals_upto_n_minutes > 0:
            skip_node_penalty = assumptions.get('SKIP_NODE_PENALTY', 5000)
            penalty_for_late_arrival = round(skip_node_penalty / allow_late_arrivals_upto_n_minutes)
            penalty_threshold = penalty_for_late_arrival
            
            window_start, window_end = node_window[0], node_window[1]
            max_allowed_time = window_end + allow_late_arrivals_upto_n_minutes
            
            # Check if any vehicle would violate the penalty threshold
            penalty_violations = 0
            for vehicle in vehicles:
                vehicle_start = vehicle.get('start_minute', 0)
                vehicle_end = vehicle.get('end_minute', 1440)
                
                if vehicle_start > window_end and penalty_threshold > skip_node_penalty * 0.5:
                    penalty_violations += 1
                elif vehicle_start > max_allowed_time:
                    penalty_violations += 1
            
            if penalty_violations / len(vehicles) > 0.7:
                return 'LATE_ARRIVAL_PENALTY_EXCEEDED'
                
        return 'SKIPPED_BY_OPTIMIZER'
        
    except Exception as e:
        print(f"Error in _detect_common_failure_reason: {str(e)}")
        return 'SKIPPED_BY_OPTIMIZER'
        
