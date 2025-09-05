import json
import multiprocessing
import time
import os
from datetime import datetime
from vrp_optimizer.optimizer import Optimizer

def normalize_for_comparison(data):
    """
    Simple normalization: only convert False to false, True to true, single quotes to double quotes
    """
    if isinstance(data, str):
        return data.replace("'", '"').replace('False', 'false').replace('True', 'true')
    elif isinstance(data, dict):
        return {k: normalize_for_comparison(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [normalize_for_comparison(item) for item in data]
    else:
        return data

def simple_compare(actual, expected):
    """
    Simple comparison: normalize both and check if they match exactly
    """
    try:
        # Only normalize the 3 conditions: False=false, True=true, '="
        actual_normalized = normalize_for_comparison(actual)
        expected_normalized = normalize_for_comparison(expected)
        
        # Check if they match exactly after normalization
        return actual_normalized == expected_normalized
    except Exception as e:
        return False

def find_data_differences(actual, expected, path=""):
    """
    Find specific differences between actual and expected data
    Returns list of differences with paths and values
    """
    differences = []
    
    if type(actual) != type(expected):
        differences.append({
            'path': path,
            'actual_type': type(actual).__name__,
            'expected_type': type(expected).__name__,
            'actual_value': actual,
            'expected_value': expected,
            'message': f"Type mismatch: {type(actual).__name__} vs {type(expected).__name__}"
        })
        return differences
    
    if isinstance(actual, dict):
        # Compare dictionary keys and values
        actual_keys = set(actual.keys())
        expected_keys = set(expected.keys())
        
        # Check for missing keys
        for key in expected_keys - actual_keys:
            differences.append({
                'path': f"{path}.{key}" if path else key,
                'actual_value': "MISSING",
                'expected_value': expected[key],
                'message': f"Missing key: {key}"
            })
        
        # Check for extra keys
        for key in actual_keys - expected_keys:
            differences.append({
                'path': f"{path}.{key}" if path else key,
                'actual_value': actual[key],
                'expected_value': "EXTRA",
                'message': f"Extra key: {key}"
            })
        
        # Compare common keys
        for key in actual_keys & expected_keys:
            new_path = f"{path}.{key}" if path else key
            differences.extend(find_data_differences(actual[key], expected[key], new_path))
    
    elif isinstance(actual, list):
        # Compare list elements
        if len(actual) != len(expected):
            differences.append({
                'path': f"{path}[LENGTH]",
                'actual_value': len(actual),
                'expected_value': len(expected),
                'message': f"Length mismatch: {len(actual)} vs {len(expected)}"
            })
        else:
            for i, (act_item, exp_item) in enumerate(zip(actual, expected)):
                new_path = f"{path}[{i}]"
                differences.extend(find_data_differences(act_item, exp_item, new_path))
    
    else:
        # Compare primitive values
        if actual != expected:
            differences.append({
                'path': path,
                'actual_value': actual,
                'expected_value': expected,
                'message': f"Value mismatch: {actual} vs {expected}"
            })
    
    return differences

def display_detailed_differences(actual, expected, test_name):
    """
    Display detailed differences between actual and expected data
    """
    print(f"\n🔍 DETAILED DIFFERENCES FOR {test_name.upper()}:")
    print("=" * 60)
    
    differences = find_data_differences(actual, expected)
    
    if not differences:
        print("✅ No differences found - data matches perfectly!")
        return True
    
    print(f"❌ Found {len(differences)} difference(s):")
    print("-" * 60)
    
    for i, diff in enumerate(differences, 1):
        print(f"\n{i}. Path: {diff['path']}")
        print(f"   Message: {diff['message']}")
        print(f"   Actual:   {diff['actual_value']}")
        print(f"   Expected: {diff['expected_value']}")
    
    return False

def test_optimizer():
    """
    Test function to load test data and call the Optimizer function
    """
    try:
        # Load test data from JSON file
        with open('tests/inputData/simple_test_data.json', 'r') as file:
            test_data = json.load(file)
        
        print("Loaded test data successfully!")
        print(f"Number of moves: {len(test_data['moves'])}")
        print(f"Number of drivers: {len(test_data['drivers'])}")
        print(f"Carrier ID: {test_data['carrier_id']}")
        print(f"Plan date: {test_data['plan_date']}")
        print(f"Time limit: {test_data['time_limit']} seconds")
        print(f"Timezone: {test_data['timezone']}")
        print(f"Distance unit: {test_data['distance_unit']}")
        print("-" * 50)
        
        # Extract data for optimizer
        moves = test_data['moves']
        drivers = test_data['drivers']
        depot_locations = test_data['depot_locations']
        yard_locations = test_data['yard_locations']
        timezone = test_data['timezone']
        distance_unit = test_data['distance_unit']
        time_limit = test_data['time_limit']
        equipment_validations = test_data['equipment_validations']
        location_distance_matrix = test_data['location_distance_matrix']
        plan_start_minute = test_data['plan_start_minute']
        plan_end_minute = test_data['plan_end_minute']
        carrier_id = test_data['carrier_id']
        plan_date = datetime.fromisoformat(test_data['plan_date'].replace('Z', '+00:00'))
        
        print("Creating Optimizer instance...")
        
        # Create optimizer instance
        optimizer = Optimizer(
            moves=moves,
            drivers=drivers,
            depot_locations=depot_locations,
            yard_locations=yard_locations,
            timezone=timezone,
            distance_unit=distance_unit,
            time_limit=time_limit,
            equipment_validations=equipment_validations,
            location_distance_matrix=location_distance_matrix,
            plan_start_minute=plan_start_minute,
            plan_end_minute=plan_end_minute,
            carrier_id=carrier_id,
            plan_date=plan_date
        )
        
        print("Optimizer instance created successfully!")
        print("Starting optimization process...")
        print("-" * 50)
        
        # Create multiprocessing pool and run optimization
        with multiprocessing.Pool(processes=1) as pool:
            start_time = time.time()
            
            # Call the optimizer.optimize function
            _optimal_plan, _d_schedule = pool.apply_async(optimizer.optimize).get(timeout=time_limit + 30)
            
            end_time = time.time()
            
            print(f"Optimization completed in {end_time - start_time:.2f} seconds")
            print("-" * 50)
            
            # Print the optimal plan
            print("OPTIMAL PLAN:")
            print("=" * 50)
            print(f"Number of optimal plans: {len(_optimal_plan) if _optimal_plan else 0}")
            print(f"Returned optimal plan first===========================: {_optimal_plan}")
            
            if _optimal_plan:
                for i, plan in enumerate(_optimal_plan):
                    print(f"\nPlan {i+1}:")
                    print(f"  Reference Number: {plan.get('reference_number', 'N/A')}")
                    print(f"  Driver ID: {plan.get('assigned_driver', 'N/A')}")
                    print(f"  Distance: {plan.get('distance', 'N/A')}")
                    print(f"  Revenue: {plan.get('revenue', 'N/A')}")
                    print(f"  Completion Time: {plan.get('completion_time', 'N/A')}")
            else:
                print("No optimal plan found!")
            
            print("\n" + "=" * 50)
            
            # Print driver schedule
            print("DRIVER SCHEDULE:")
            print("=" * 50)
            print(f"Number of driver schedules: {len(_d_schedule) if _d_schedule else 0}")
            
            if _d_schedule:
                for driver_id, schedule in _d_schedule.items():
                    print(f"\nDriver {driver_id}:")
                    print(f"  Schedule: {schedule}")
            else:
                print("No driver schedule found!")
            
            print("\n" + "=" * 50)
            
            return _optimal_plan, _d_schedule
            
    except FileNotFoundError:
        print("Error: tests/inputData/simple_test_data.json file not found!")
        print("Make sure the file is in the tests directory.")
        return None, None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in inputData/simple_test_data.json: {e}")
        return None, None
    except multiprocessing.TimeoutError:
        print(f"Error: Optimization timed out after {time_limit + 30} seconds")
        return None, None
    except Exception as e:
        print(f"Error during optimization: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None

def test_and_compare(test_name):
    """
    Test function that runs optimizer and compares results with expected output
    """
    try:
        print(f"Running test: {test_name}")
        print("=" * 60)
        
        # Load input data
        input_file = f'tests/inputData/{test_name}Input.json'
        with open(input_file, 'r') as file:
            test_data = json.load(file)
        
        print(f"✓ Loaded {test_name} input data successfully!")
        print(f"Number of moves: {len(test_data['moves'])}")
        print(f"Number of drivers: {len(test_data['drivers'])}")
        print(f"Carrier ID: {test_data.get('carrier_id', 'N/A')}")
        print(f"Time limit: {test_data.get('time_limit', 300)} seconds")
        print("-" * 50)
        
        # Extract data for optimizer
        moves = test_data['moves']
        drivers = test_data['drivers']
        depot_locations = test_data.get('depot_locations', [])
        yard_locations = test_data.get('yard_locations', [])
        timezone = test_data.get('timezone', 'UTC')
        distance_unit = test_data.get('distance_unit', 'miles')
        time_limit = test_data.get('time_limit', 300)
        equipment_validations = test_data.get('equipment_validations', {})
        location_distance_matrix = test_data.get('location_distance_matrix', {})
        plan_start_minute = test_data.get('plan_start_minute', 0)
        plan_end_minute = test_data.get('plan_end_minute', 1440)
        carrier_id = test_data.get('carrier_id', 'test_carrier')
        plan_date = datetime.fromisoformat(test_data['plan_date'].replace('Z', '+00:00')) if 'plan_date' in test_data else datetime.now()
        
        print("Creating Optimizer instance...")
        
        # Create optimizer instance
        optimizer = Optimizer(
            moves=moves,
            drivers=drivers,
            depot_locations=depot_locations,
            yard_locations=yard_locations,
            timezone=timezone,
            distance_unit=distance_unit,
            time_limit=time_limit,
            equipment_validations=equipment_validations,
            location_distance_matrix=location_distance_matrix,
            plan_start_minute=plan_start_minute,
            plan_end_minute=plan_end_minute,
            carrier_id=carrier_id,
            plan_date=plan_date
        )
        
        print("Optimizer instance created successfully!")
        print("Starting optimization process...")
        print("-" * 50)
        
        # Create multiprocessing pool and run optimization
        with multiprocessing.Pool(processes=1) as pool:
            start_time = time.time()
            
            # Call the optimizer.optimize function
            _optimal_plan, _d_schedule = pool.apply_async(optimizer.optimize).get(timeout=time_limit + 30)
            
            end_time = time.time()
            
            print(f"✓ Optimization completed in {end_time - start_time:.2f} seconds")
            print("-" * 50)
            
            # Load expected output for comparison
            output_file = f'tests/resultData/{test_name}Output.json'
            expected_output = None
            try:
                # Check if file exists and has content
                if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                    with open(output_file, 'r') as file:
                        expected_output = json.load(file)
                    print(f"✓ Loaded expected output for comparison")
                else:
                    print(f"⚠ Output file {output_file} is empty or doesn't exist")
            except FileNotFoundError:
                print(f"⚠ No expected output file found at {output_file}")
            except json.JSONDecodeError as e:
                print(f"✗ Error reading expected output: {e}")
            except Exception as e:
                print(f"✗ Unexpected error reading output file: {e}")
            
            # Simple comparison with expected output if available
            if expected_output:
                # Check the structure of expected_output to determine comparison method
                if isinstance(expected_output, list):
                    # Output is an array (like chassisActivityOutput.json)
                    # Compare directly with _optimal_plan if it's also an array
                    if isinstance(_optimal_plan, list):
                        is_match = simple_compare(_optimal_plan, expected_output)
                        status = "✓ PASS" if is_match else "✗ FAIL"
                    else:
                        status = "✗ FAIL"
                        reason = "Expected array output but got different data structure"
                else:
                    # Output has optimal_plan and driver_schedule structure
                    # Combine actual results into one object for comparison
                    actual_result = {
                        'optimal_plan': _optimal_plan,
                        'driver_schedule': _d_schedule
                    }
                    
                    # Compare entire expected_output with entire actual_result
                    is_match = simple_compare(actual_result, expected_output)
                    status = "✓ PASS" if is_match else "✗ FAIL"
            else:
                status = "⚠ SKIP"
                reason = "No output file found for comparison"
            
            return _optimal_plan, _d_schedule
            
    except FileNotFoundError:
        print(f"Error: {input_file} file not found!")
        return None, None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in {input_file}: {e}")
        return None, None
    except multiprocessing.TimeoutError:
        print(f"Error: Optimization timed out after {time_limit + 30} seconds")
        return None, None
    except Exception as e:
        print(f"Error during optimization: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None

def run_all_tests():
    """
    Run all available tests and compare results with expected outputs
    """
    print("Running All Available Tests...")
    print("=" * 80)
    
    # Get all available test names from input files
    input_dir = 'tests/inputData'
    test_names = []
    
    try:
        for filename in os.listdir(input_dir):
            if filename.endswith('Input.json'):
                test_name = filename.replace('Input.json', '')
                test_names.append(test_name)
    except FileNotFoundError:
        print(f"Error: Directory {input_dir} not found!")
        return
    
    if not test_names:
        print("No test files found!")
        return
    
    print(f"Found {len(test_names)} test(s): {', '.join(test_names)}")
    print("=" * 80)
    
    # Run each test
    test_results = {}
    for test_name in test_names:
        print(f"Running {test_name}...", end=" ")
        
        # Capture the comparison result for this test
        try:
            optimal_plan, driver_schedule = test_and_compare(test_name)
            
            # Determine test result and reason
            if optimal_plan is not None:
                # Check if there was a comparison and what the result was
                output_file = f'tests/resultData/{test_name}Output.json'
                if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                    try:
                        with open(output_file, 'r') as file:
                            expected_output = json.load(file)
                        
                        # Determine comparison result based on structure
                        if isinstance(expected_output, list):
                            if isinstance(optimal_plan, list):
                                is_match = simple_compare(optimal_plan, expected_output)
                                if is_match:
                                    status = "✓ PASS"
                                    reason = "Data matches expected output perfectly"
                                else:
                                    status = "✗ FAIL"
                                    reason = "Data structure matches but content differs from expected output"
                            else:
                                status = "✗ FAIL"
                                reason = "Expected array output but got different data structure"
                        else:
                            # Object structure comparison
                            actual_result = {
                                'optimal_plan': optimal_plan,
                                'driver_schedule': driver_schedule
                            }
                            is_match = simple_compare(actual_result, expected_output)
                            if is_match:
                                status = "✓ PASS"
                                reason = "Data matches expected output perfectly"
                            else:
                                status = "✗ FAIL"
                                reason = "Data structure matches but content differs from expected output"
                    except Exception as e:
                        status = "⚠ SKIP"
                        reason = f"Could not compare with output file: {str(e)}"
                else:
                    status = "⚠ SKIP"
                    reason = "No output file found for comparison"
            else:
                status = "✗ FAIL"
                reason = "Optimizer failed to return results"
            
            test_results[test_name] = {
                'optimal_plan': optimal_plan,
                'driver_schedule': driver_schedule,
                'success': optimal_plan is not None,
                'status': status,
                'reason': reason
            }
            
            print(f"{status}")
            
        except Exception as e:
            status = "✗ ERROR"
            reason = f"Test execution failed: {str(e)}"
            test_results[test_name] = {
                'optimal_plan': None,
                'driver_schedule': None,
                'success': False,
                'status': status,
                'reason': reason
            }
            print(f"{status}")
    
    print("\n" + "=" * 80)
    
    # Print detailed summary
    print("\n" + "=" * 80)
    print("FINAL TEST RESULTS SUMMARY")
    print("=" * 80)
    
    # Count by status
    status_counts = {}
    for result in test_results.values():
        status = result['status']
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print("STATUS BREAKDOWN:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")
    
    print(f"\nTOTAL TESTS: {len(test_results)}")
    
    # Store detailed differences for final summary
    all_differences = {}
    
    for test_name, result in test_results.items():
        # If failed, find and store detailed differences
        if result['status'] == "✗ FAIL" and result['optimal_plan']:
            output_file = f'tests/resultData/{test_name}Output.json'
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                try:
                    with open(output_file, 'r') as file:
                        expected_output = json.load(file)
                    
                    if isinstance(expected_output, list):
                        differences = find_data_differences(result['optimal_plan'], expected_output)
                    else:
                        actual_result = {
                            'optimal_plan': result['optimal_plan'],
                            'driver_schedule': result['driver_schedule']
                        }
                        differences = find_data_differences(actual_result, expected_output)
                    
                    if differences:
                        all_differences[test_name] = differences
                except:
                    pass
    
    # Show final summary with specific differences
    print("\n" + "=" * 80)
    print("FINAL SUMMARY WITH SPECIFIC DIFFERENCES")
    print("=" * 80)
    
    summary_lines = []
    for test_name, result in test_results.items():
        status = result['status']
        if status == "✓ PASS":
            summary_lines.append(f"{test_name} - PASS")
        elif status == "✗ FAIL":
            summary_lines.append(f"{test_name} - FAIL")
            if test_name in all_differences:
                for diff in all_differences[test_name]:
                    summary_lines.append(f"  key = {diff['path']} value ==> {diff['actual_value']}(A) != {diff['expected_value']}(E)")
        elif status == "⚠ SKIP":
            summary_lines.append(f"{test_name} - SKIP ({result['reason']})")
        elif status == "✗ ERROR":
            summary_lines.append(f"{test_name} - ERROR ({result['reason']})")
    return {
        "summary": summary_lines
    }

if __name__ == "__main__":
    print("Starting Optimizer Test...")
    print("=" * 60)
    
    # Check if specific test is requested
    import sys
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        print(f"Running specific test: {test_name}")
        optimal_plan, driver_schedule = test_and_compare(test_name)
        
        if optimal_plan is not None:
            print(f"\nTest '{test_name}' completed successfully!")
        else:
            print(f"\nTest '{test_name}' failed!")
    else:
        # Run all tests by default
        test_results = run_all_tests()
        
        if test_results:
            print("\nAll tests completed!")
        else:
            print("\nNo tests were run!")
    
    print("\n" + "=" * 60)
