"""
VRP Optimizer Debugging Module
Add this to your vrp_optimizer package for comprehensive debugging
"""

import logging
from typing import Dict, List, Any, Tuple
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class OptimizerDebugger:
    """Helper class to debug VRP optimizer issues"""
    
    def __init__(self, optimizer):
        self.optimizer = optimizer
        self.debug_info = {
            'total_moves': 0,
            'total_drivers': 0,
            'constraint_violations': {},
            'time_window_conflicts': [],
            'capacity_issues': [],
            'incompatible_pairs': [],
            'solver_status': None
        }
    
    def analyze_pre_optimization(self):
        """Analyze the problem setup before optimization"""
        self.debug_info['total_moves'] = len(self.optimizer.NODE_DATA) - len(self.optimizer.DEPOTS)
        self.debug_info['total_drivers'] = len(self.optimizer.VEHICLES)
        
        # Check move-driver compatibility
        compatibility_matrix = self._check_compatibility_matrix()
        
        # Check time windows feasibility
        time_feasibility = self._check_time_windows()
        
        # Check capacity constraints
        capacity_analysis = self._analyze_capacity()
        
        # Check for impossible sequences
        sequence_issues = self._check_move_sequences()
        
        return {
            'compatibility': compatibility_matrix,
            'time_feasibility': time_feasibility,
            'capacity': capacity_analysis,
            'sequences': sequence_issues
        }
    
    def _check_compatibility_matrix(self) -> Dict[str, Any]:
        """Check how many drivers can serve each move"""
        from vrp_optimizer.constraint_components import is_vehicle_compatible_with_node
        
        move_compatibility = {}
        completely_incompatible = []
        
        for node in self.optimizer.NODE_DATA:
            if node.get('isDepot'):
                continue
                
            compatible_drivers = []
            incompatible_reasons = {}
            
            for vehicle_id, vehicle in enumerate(self.optimizer.VEHICLES):
                is_compatible, reason = is_vehicle_compatible_with_node(vehicle, node)
                
                if is_compatible:
                    # Also check time compatibility
                    node_window = node.get('available_range', [0, 1440])
                    vehicle_window = [vehicle.get('start_minute', 0), vehicle.get('end_minute', 1440)]
                    
                    # Check if vehicle can physically reach the node in time
                    if vehicle_window[1] >= node_window[0] and vehicle_window[0] <= node_window[1]:
                        compatible_drivers.append(vehicle['_id'])
                    else:
                        incompatible_reasons[vehicle['_id']] = 'TIME_WINDOW_CONFLICT'
                else:
                    incompatible_reasons[vehicle['_id']] = reason or 'UNKNOWN'
            
            move_compatibility[node['reference_number']] = {
                'compatible_count': len(compatible_drivers),
                'compatible_drivers': compatible_drivers,
                'total_drivers': len(self.optimizer.VEHICLES),
                'compatibility_ratio': len(compatible_drivers) / len(self.optimizer.VEHICLES) if self.optimizer.VEHICLES else 0
            }
            
            if len(compatible_drivers) == 0:
                completely_incompatible.append({
                    'reference_number': node['reference_number'],
                    'reasons': incompatible_reasons
                })
        
        return {
            'move_compatibility': move_compatibility,
            'completely_incompatible': completely_incompatible,
            'avg_compatibility_ratio': sum(m['compatibility_ratio'] for m in move_compatibility.values()) / len(move_compatibility) if move_compatibility else 0
        }
    
    def _check_time_windows(self) -> Dict[str, Any]:
        """Check time window feasibility"""
        time_issues = []
        
        for node in self.optimizer.NODE_DATA:
            if node.get('isDepot'):
                continue
                
            window = node.get('available_range', [0, 1440])
            processing_time = node.get('time_to_process_move', 30)
            
            # Check if window is too narrow
            if window[1] - window[0] < processing_time:
                time_issues.append({
                    'reference_number': node['reference_number'],
                    'issue': 'WINDOW_TOO_NARROW',
                    'window_size': window[1] - window[0],
                    'required_time': processing_time
                })
            
            # Check if any driver can reach in time
            can_reach = False
            for vehicle in self.optimizer.VEHICLES:
                vehicle_start = vehicle.get('start_minute', 0)
                if vehicle_start <= window[1] - processing_time:
                    can_reach = True
                    break
            
            if not can_reach:
                time_issues.append({
                    'reference_number': node['reference_number'],
                    'issue': 'UNREACHABLE_IN_TIME',
                    'latest_start': window[1] - processing_time,
                    'earliest_driver': min(v.get('start_minute', 0) for v in self.optimizer.VEHICLES)
                })
        
        return {
            'time_issues': time_issues,
            'issue_count': len(time_issues),
            'issue_rate': len(time_issues) / (len(self.optimizer.NODE_DATA) - len(self.optimizer.DEPOTS)) if self.optimizer.NODE_DATA else 0
        }
    
    def _analyze_capacity(self) -> Dict[str, Any]:
        """Analyze if there's enough driver capacity"""
        total_move_time = 0
        total_move_distance = 0
        
        for node in self.optimizer.NODE_DATA:
            if node.get('isDepot'):
                continue
            total_move_time += node.get('time_to_process_move', 30)
            total_move_distance += node.get('route_distance', 0)
        
        total_driver_time = sum(
            v.get('max_working_minutes', 600) for v in self.optimizer.VEHICLES
        )
        total_driver_distance = sum(
            v.get('max_miles', 500) for v in self.optimizer.VEHICLES
        )
        
        return {
            'total_move_time': total_move_time,
            'total_driver_time': total_driver_time,
            'time_utilization': total_move_time / total_driver_time if total_driver_time else float('inf'),
            'total_move_distance': total_move_distance,
            'total_driver_distance': total_driver_distance,
            'distance_utilization': total_move_distance / total_driver_distance if total_driver_distance else float('inf'),
            'feasible': total_move_time <= total_driver_time and total_move_distance <= total_driver_distance
        }
    
    def _check_move_sequences(self) -> Dict[str, Any]:
        """Check for problematic move sequences (coupled moves, dependencies)"""
        sequence_issues = []
        coupled_moves = {}
        
        for node in self.optimizer.NODE_DATA:
            if node.get('isDepot'):
                continue
                
            # Check strictly coupled moves
            if node.get('strictly_coupled_move'):
                coupled_ref = node.get('strictly_coupled_move')
                coupled_node = next(
                    (n for n in self.optimizer.NODE_DATA if n.get('reference_number') == coupled_ref),
                    None
                )
                
                if not coupled_node:
                    sequence_issues.append({
                        'reference_number': node['reference_number'],
                        'issue': 'MISSING_COUPLED_MOVE',
                        'missing_ref': coupled_ref
                    })
                else:
                    coupled_moves[node['reference_number']] = coupled_ref
            
            # Check previous node dependencies
            if node.get('previous_node') is not None:
                prev_idx = node.get('previous_node')
                if prev_idx >= len(self.optimizer.NODE_DATA):
                    sequence_issues.append({
                        'reference_number': node['reference_number'],
                        'issue': 'INVALID_PREVIOUS_NODE',
                        'prev_idx': prev_idx
                    })
        
        return {
            'sequence_issues': sequence_issues,
            'coupled_moves': coupled_moves,
            'has_dependencies': len(sequence_issues) > 0 or len(coupled_moves) > 0
        }
    
    def analyze_solver_failure(self):
        """Analyze why the solver failed to find a solution"""
        # Get solver status
        status = self.optimizer.routing.status()
        status_name = self._get_status_name(status)
        
        # Check if problem is over-constrained
        analysis = {
            'solver_status': status_name,
            'status_code': status,
            'vehicles_used': 0,
            'dropped_nodes': [],
            'time_limit_reached': False
        }
        
        # Check search log for insights
        if hasattr(self.optimizer, 'search_parameters'):
            analysis['time_limit'] = self.optimizer.search_parameters.time_limit.seconds
            analysis['algorithm'] = str(self.optimizer.search_parameters.first_solution_strategy)
        
        # Try to identify specific bottlenecks
        bottlenecks = self._identify_bottlenecks()
        analysis['bottlenecks'] = bottlenecks
        
        return analysis
    
    def _identify_bottlenecks(self) -> List[Dict[str, Any]]:
        """Identify specific bottlenecks in the problem"""
        bottlenecks = []
        
        # Check for time bottlenecks (rush hours)
        time_distribution = {}
        for node in self.optimizer.NODE_DATA:
            if node.get('isDepot'):
                continue
            window = node.get('available_range', [0, 1440])
            hour = window[0] // 60
            time_distribution[hour] = time_distribution.get(hour, 0) + 1
        
        # Find peak hours
        if time_distribution:
            avg_per_hour = sum(time_distribution.values()) / 24
            for hour, count in time_distribution.items():
                if count > avg_per_hour * 2:  # More than 2x average
                    bottlenecks.append({
                        'type': 'TIME_BOTTLENECK',
                        'hour': hour,
                        'move_count': count,
                        'avg_count': avg_per_hour
                    })
        
        # Check for geographic bottlenecks
        # (would need location data to implement fully)
        
        return bottlenecks
    
    def _get_status_name(self, status: int) -> str:
        """Convert OR-Tools status code to readable name"""
        status_names = {
            0: 'ROUTING_NOT_SOLVED',
            1: 'ROUTING_SUCCESS',
            2: 'ROUTING_FAIL',
            3: 'ROUTING_FAIL_TIMEOUT',
            4: 'ROUTING_INVALID'
        }
        return status_names.get(status, f'UNKNOWN_{status}')
    
    def generate_debug_report(self) -> str:
        """Generate comprehensive debug report"""
        pre_analysis = self.analyze_pre_optimization()
        
        report = []
        report.append("=" * 60)
        report.append("VRP OPTIMIZER DEBUG REPORT")
        report.append("=" * 60)
        report.append(f"Timestamp: {datetime.now().isoformat()}")
        report.append(f"Total Moves: {self.debug_info['total_moves']}")
        report.append(f"Total Drivers: {self.debug_info['total_drivers']}")
        report.append("")
        
        # Compatibility Analysis
        report.append("COMPATIBILITY ANALYSIS:")
        report.append("-" * 40)
        compat = pre_analysis['compatibility']
        report.append(f"Average Compatibility Ratio: {compat['avg_compatibility_ratio']:.2%}")
        report.append(f"Completely Incompatible Moves: {len(compat['completely_incompatible'])}")
        
        if compat['completely_incompatible']:
            report.append("\nCompletely Incompatible Moves:")
            for move in compat['completely_incompatible'][:5]:  # Show first 5
                report.append(f"  - {move['reference_number']}: {list(set(move['reasons'].values()))}")
        
        # Time Window Analysis
        report.append("\nTIME WINDOW ANALYSIS:")
        report.append("-" * 40)
        time_analysis = pre_analysis['time_feasibility']
        report.append(f"Time Issues Found: {time_analysis['issue_count']}")
        report.append(f"Issue Rate: {time_analysis['issue_rate']:.2%}")
        
        if time_analysis['time_issues']:
            report.append("\nTime Window Issues (first 5):")
            for issue in time_analysis['time_issues'][:5]:
                report.append(f"  - {issue['reference_number']}: {issue['issue']}")
        
        # Capacity Analysis
        report.append("\nCAPACITY ANALYSIS:")
        report.append("-" * 40)
        capacity = pre_analysis['capacity']
        report.append(f"Time Utilization: {capacity['time_utilization']:.2%}")
        report.append(f"Distance Utilization: {capacity['distance_utilization']:.2%}")
        report.append(f"Theoretically Feasible: {capacity['feasible']}")
        
        # Sequence Analysis
        report.append("\nSEQUENCE ANALYSIS:")
        report.append("-" * 40)
        sequences = pre_analysis['sequences']
        report.append(f"Sequence Issues: {len(sequences['sequence_issues'])}")
        report.append(f"Coupled Moves: {len(sequences['coupled_moves'])}")
        
        report.append("=" * 60)
        
        return "\n".join(report)


def debug_optimizer_incrementally(optimizer_class, moves, drivers, depot_locations, 
                                 yard_locations, timezone, **kwargs):
    """
    Debug optimizer by incrementally adding moves to find the breaking point
    """
    results = []
    
    # Start with small batches
    batch_sizes = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, len(moves)]
    
    for batch_size in batch_sizes:
        if batch_size > len(moves):
            break
            
        batch_moves = moves[:batch_size]
        
        try:
            optimizer = optimizer_class(
                moves=batch_moves,
                drivers=drivers,
                depot_locations=depot_locations,
                yard_locations=yard_locations,
                timezone=timezone,
                **kwargs
            )
            
            # Create debugger
            debugger = OptimizerDebugger(optimizer)
            pre_analysis = debugger.analyze_pre_optimization()
            
            # Try to optimize
            solution, schedule = optimizer.optimize()
            
            success = len(solution) > 0
            
            results.append({
                'batch_size': batch_size,
                'success': success,
                'moves_optimized': sum(len(s['node']) for s in solution) if solution else 0,
                'drivers_used': len(solution) if solution else 0,
                'compatibility_ratio': pre_analysis['compatibility']['avg_compatibility_ratio'],
                'time_issues': pre_analysis['time_feasibility']['issue_count'],
                'capacity_feasible': pre_analysis['capacity']['feasible']
            })
            
            print(f"Batch {batch_size}: {'SUCCESS' if success else 'FAILED'}")
            
            if not success and batch_size > 1:
                # Found the breaking point
                print(f"Breaking point found at batch size {batch_size}")
                print("Debug Report:")
                print(debugger.generate_debug_report())
                
                # Try to identify the problematic moves
                if batch_size <= len(moves):
                    problem_moves = moves[batch_size-10:batch_size] if batch_size > 10 else moves[:batch_size]
                    print("\nPotentially problematic moves:")
                    for move in problem_moves:
                        print(f"  - {move.get('reference_number', 'Unknown')}")
                
                break
                
        except Exception as e:
            print(f"Error at batch size {batch_size}: {str(e)}")
            results.append({
                'batch_size': batch_size,
                'success': False,
                'error': str(e)
            })
            break
    
    return results