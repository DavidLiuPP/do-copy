from vrp_optimizer.constraint_components import is_vehicle_compatible_with_node

def get_unplanned_moves(optimizer, unplanned_reference_numbers):
    invalid_moves = []
    NODE_DATA = optimizer.NODE_DATA
    VEHICLES = optimizer.VEHICLES

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
        
            invalid_moves.append({
                'reference_number': node['reference_number'],
                'reason': 'SKIPPED_BY_OPTIMIZER'
            })
        
        except Exception as e:
            continue

    return invalid_moves
