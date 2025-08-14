import pandas as pd
import numpy as np
import plotly.express as px
import plotly.figure_factory as ff
from datetime import datetime, timedelta, time
import requests


# Define parameters
horizon_start = datetime(2024, 7, 11, 0)
horizon_end = datetime(2024, 7, 14, 0)
time_step = timedelta(seconds=300)
current_time = horizon_start

# Example data for loads (both imports and exports) with revenue, time windows, and handling time
loads = [
{'id': 'BDCQ_E102928', 'type': 'export', 'pickup': (37.5987215, -120.9685234), 'pickup address': '12828 Rd 26, Madera, CA 93637', 'delivery': (37.806776582174926, -122.3398143784745),  'delivery address': '5190 7th Street, Oakland, CA, 94607',  'return': (37.5987215, -120.9685234), 'return address': '12828 Rd 26, Madera, CA 93637', 'pickup_window': (datetime(2024, 7, 11, 10, 30), datetime(2024, 7, 12, 23, 59)), 'delivery_window': (datetime(2024, 7, 11, 8, 0), datetime(2024, 7, 12, 23, 59)), 'return_window': (datetime(2024, 7, 11, 8, 0), datetime(2024, 7, 12, 23, 59)), 'size': '40ft', 'revenue': 1900, 'hazmat': 'no'},
{'id': 'BDCQ_E102929', 'type': 'export', 'pickup': (37.5987215, -120.9685234), 'pickup address': '850 Commerce Dr, Madera, CA 93637', 'delivery': (37.806776582174926, -122.3398143784745), 'delivery address': '5190 7th Street, Oakland, CA, 94607', 'return': (37.5987215, -120.9685234), 'return address':'850 Commerce Dr, Madera, CA 93637', 'pickup_window': (datetime(2024, 7, 12, 9, 0), datetime(2024, 7, 13, 23, 59)), 'delivery_window': (datetime(2024, 7, 12, 8, 0), datetime(2024, 7, 14, 23, 59)), 'return_window': (datetime(2024, 7, 12, 8, 0), datetime(2024, 7, 14, 23, 59)), 'size': '40ft', 'revenue': 2100, 'hazmat': 'yes'},
{'id': 'BDCQ_E102930', 'type': 'export', 'pickup': (37.5987215, -120.9685234), 'pickup address': '850 Commerce Dr, Madera, CA 93637', 'delivery': (37.806776582174926, -122.3398143784745), 'delivery address': '5190 7th Street, Oakland, CA, 94607', 'return': (37.5987215, -120.9685234), 'return address': '850 Commerce Dr, Madera, CA 93637', 'pickup_window': (datetime(2024, 7, 13, 11, 0), datetime(2024, 7, 14, 23, 59)), 'delivery_window': (datetime(2024, 7, 13, 8, 0), datetime(2024, 7, 15, 23, 59)), 'return_window': (datetime(2024, 7, 13, 8, 0), datetime(2024, 7, 15, 23, 59)), 'size': '40ft', 'revenue': 1800, 'hazmat': 'no'},
{'id': 'BDCQ_M107808', 'type': 'import', 'pickup': (37.7968784, -122.3043294), 'pickup address': '1717 Middle Harbor Rd, Oakland, CA 94607', 'delivery': (36.68367140000001,-119.7100828), 'delivery address': '3701 S Minnewawa Ave, Fresno, CA 93725', 'return': (37.7968784, -122.3043294), 'return address': '1717 Middle Harbor Rd, Oakland, CA 94607', 'pickup_window': (datetime(2024, 7, 11, 12, 0), datetime(2024, 7, 12, 23, 59)), 'delivery_window': (datetime(2024, 7, 11, 8, 0), datetime(2024, 7, 12, 23, 59)), 'return_window': (datetime(2024, 7, 11, 8, 0), datetime(2024, 7, 12, 23, 59)), 'size': '20ft', 'revenue': 1600, 'hazmat': 'no'},
{'id': 'BDCQ_E107925', 'type': 'export', 'pickup': (36.9193399, -120.0458046), 'pickup address': '8800 S Minturn Rd, Le Grand, CA 95333', 'delivery': (37.8073906, -122.3252458), 'delivery address': '1717 Middle Harbor Rd, Oakland, CA 94607', 'return': (36.9193399, -120.0458046), 'return address': '8800 S Minturn Rd, Le Grand, CA 95333', 'pickup_window': (datetime(2024, 7, 13, 14, 30), datetime(2024, 7, 13, 23, 59)), 'delivery_window': (datetime(2024, 7, 13, 8, 0), datetime(2024, 7, 14, 23, 59)), 'return_window': (datetime(2024, 7, 13, 8, 0), datetime(2024, 7, 14, 23, 59)), 'size': '20ft', 'revenue': 1650, 'hazmat': 'no'},
{'id': 'BDCQ_M107807', 'type': 'import', 'pickup': (37.7968784, -122.3043294), 'pickup address': '1717 Middle Harbor Rd, Oakland, CA 94607', 'delivery': (36.68367140000001,-119.7100828), 'delivery address': '3701 S Minnewawa Ave, Fresno, CA 93725', 'return': (37.7968784, -122.3043294), 'return address': '1717 Middle Harbor Rd, Oakland, CA 94607', 'pickup_window': (datetime(2024, 7, 11, 10, 0), datetime(2024, 7, 11, 23, 59)), 'delivery_window': (datetime(2024, 7, 11, 8, 0), datetime(2024, 7, 12, 23, 59)), 'return_window': (datetime(2024, 7, 11, 8, 0), datetime(2024, 7, 12, 23, 59)), 'size': '40ft', 'revenue': 1800, 'hazmat': 'no'},
{'id': 'BDCQ_M107806', 'type': 'import', 'pickup': (37.7968784, -122.3043294), 'pickup address': '1717 Middle Harbor Rd, Oakland, CA 94607', 'delivery': (36.68367140000001,-119.7100828), 'delivery address': '3701 S Minnewawa Ave, Fresno, CA 93725', 'return': (37.7968784, -122.3043294), 'return address': '1717 Middle Harbor Rd, Oakland, CA 94607', 'pickup_window': (datetime(2024, 7, 12, 8, 0), datetime(2024, 7, 13, 23, 59)), 'delivery_window': (datetime(2024, 7, 12, 8, 0), datetime(2024, 7, 13, 23, 59)), 'return_window': (datetime(2024, 7, 13, 8, 0), datetime(2024, 7, 13, 23, 59)), 'size': '20ft', 'revenue': 1900, 'hazmat': 'no'},
{'id': 'BDCQ_E107911', 'type': 'export', 'pickup': (36.97993348667062, -120.86899218691745), 'pickup address': '21888 Ave 14, Madera, CA 93637', 'delivery': (37.7968784, -122.3043294), 'delivery address': '1195A Middle Harbor Rd, Oakland, CA, 94607', 'return': (36.97993348667062, -120.86899218691745), 'return address': '21888 Ave 14, Madera, CA 93637', 'pickup_window': (datetime(2024, 7, 13, 12, 0), datetime(2024, 7, 13, 23, 59)), 'delivery_window': (datetime(2024, 7, 13, 8, 0), datetime(2024, 7, 13, 23, 59)), 'return_window': (datetime(2024, 7, 13, 8, 0), datetime(2024, 7, 13, 23, 59)), 'size': '40ft', 'revenue': 1580, 'hazmat': 'no'},
{'id': 'BDCQ_M107805', 'type': 'import', 'pickup': (37.7968784, -122.3043294), 'pickup address': '1717 Middle Harbor Rd, Oakland, CA 94607', 'delivery': (36.68367140000001,-119.7100828), 'delivery address': '3701 S Minnewawa Ave, Fresno, CA 93725', 'return': (37.7968784, -122.3043294), 'return address': '1717 Middle Harbor Rd, Oakland, CA 94607', 'pickup_window': (datetime(2024, 7, 11, 11, 0), datetime(2024, 7, 12, 23, 59)), 'delivery_window': (datetime(2024, 7, 11, 8, 0), datetime(2024, 7, 12, 23, 59)), 'return_window': (datetime(2024, 7, 11, 8, 0), datetime(2024, 7, 12, 23, 59)), 'size': '40ft', 'revenue': 1800, 'hazmat': 'no'},
{'id': 'BDCQ_M107804', 'type': 'import', 'pickup': (37.7968784, -122.3043294), 'pickup address': '1717 Middle Harbor Rd, Oakland, CA 94607', 'delivery': (36.68367140000001,-119.7100828),  'delivery address': '3701 S Minnewawa Ave, Fresno, CA 93725', 'return': (37.7968784, -122.3043294), 'return address': '1717 Middle Harbor Rd, Oakland, CA 94607', 'pickup_window': (datetime(2024, 7, 13, 8, 0), datetime(2024, 7, 13, 23, 59)), 'delivery_window': (datetime(2024, 7, 13, 8, 0), datetime(2024, 7, 14, 23, 59)), 'return_window': (datetime(2024, 7, 13, 8, 0), datetime(2024, 7, 14, 23, 59)), 'size': '40ft', 'revenue': 1900, 'hazmat': 'no'}
]

# # Example data for drivers with capability, location, availability, and pay rate
drivers = [
{'id': 'Ethan Harrison', 'location': (37.25774718031816, -120.96485433393083), 'address': '1651 Fairview Dr, Ceres, CA 95307, USA', 'availability_start': time(8,0), 'availability_end': time(17,0), 'pay_rate': 10, 'capability': ['20ft'], 'hazmat_certified': 'no'},
{'id': 'Liam Bennett', 'location': (37.25774718031816, -120.96485433393083), 'address': '1651 Fairview Dr, Ceres, CA 95307, USA', 'availability_start': time(8,0), 'availability_end': time(17,0), 'pay_rate': 15, 'capability': ['20ft', '40ft'], 'hazmat_certified': 'yes'},
{'id': 'Noah Sullivan', 'location': (37.25774718031816, -120.96485433393083), 'address': '1651 Fairview Dr, Ceres, CA 95307, USA', 'availability_start': time(8,0), 'availability_end': time(17,0), 'pay_rate': 12, 'capability': ['20ft', '40ft'], 'hazmat_certified': 'no'},
{'id': 'Mason Turner', 'location': (37.25774718031816, -120.96485433393083), 'address': '1651 Fairview Dr, Ceres, CA 95307, USA', 'availability_start': time(8,0), 'availability_end': time(17,0), 'pay_rate': 12, 'capability': ['20ft', '40ft'], 'hazmat_certified': 'yes'},
{'id': 'Aiden Cooper', 'location': (37.25774718031816, -120.96485433393083), 'address': '1651 Fairview Dr, Ceres, CA 95307, USA', 'availability_start': time(8,0), 'availability_end': time(17,0), 'pay_rate': 15, 'capability': ['20ft', '40ft'], 'hazmat_certified': 'no'}]



df_loads = pd.DataFrame(loads)
df_drivers = pd.DataFrame(drivers)



Distances={}
def get_distance_time(origin, destination):
    # return 20, timedelta(minutes=20)
    if (origin, destination) in Distances:
        return Distances[(origin, destination)]
    else:
        base_url = 'http://router.project-osrm.org/route/v1/driving/'
        coordinates = f'{origin[1]},{origin[0]};{destination[1]},{destination[0]}'
        url = f'{base_url}{coordinates}?overview=false'
        
        response = requests.get(url)
        data = response.json()
        
        distance_meters = data['routes'][0]['distance']
        travel_time_seconds = data['routes'][0]['duration']
        
        # Convert meters to miles
        distance_miles = round(distance_meters * 0.000621371, 2)
        
        # Convert seconds to minutes
        travel_time_minutes = round(travel_time_seconds / 60.0, 2) 
        
        Distances[(origin, destination)]=distance_miles, timedelta(minutes=travel_time_minutes)
        return distance_miles, timedelta(minutes=travel_time_minutes)



def calculate_handling_time(load, driver): # add the drivers current to pick up leg duration
    # return timedelta(hours=1)
    trip_time1 = get_distance_time(driver['location'], load['pickup'])[1]
    trip_time2 = get_distance_time(load['pickup'], load['delivery'])[1]
    if load['return']:
        trip_time3 = get_distance_time(load['delivery'], load['return'])[1]
    else:
        trip_time3 = timedelta(hours=0)
    return trip_time1 + trip_time2 + trip_time3


# Initialize state
def initialize_state():
    return {
        'time': current_time,
        'driver_locations': {d['id']: d['location'] for d in drivers},
        'load_statuses': {load['id']: 'unassigned' for load in daily_loads},
        'driver_load_completion_time': {d['id']: current_time for d in drivers}
    }



# Define the reward function
def reward_function(state, action):
    driver_id, load_id = action
    load = next(load for load in daily_loads if load['id'] == load_id)
    driver = next(driver for driver in drivers if driver['id'] == driver_id)
    if load['return']:
        distance = get_distance_time(driver['location'], load['pickup'])[0] + get_distance_time(load['pickup'], load['delivery'])[0] + get_distance_time(load['delivery'], load['return'])[0]
    else:
        distance = get_distance_time(driver['location'], load['pickup'])[0] + get_distance_time(load['pickup'], load['delivery'])[0]
    cost = distance * driver['pay_rate']
    revenue = load['revenue']
    return revenue - cost

# Define the transition function
def transition_function(state, action):
    driver_id, load_id = action
    new_state = {
        'time': state['time'] + time_step,
        'driver_locations': state['driver_locations'].copy(),
        'load_statuses': state['load_statuses'].copy(),
        'driver_load_completion_time': state['driver_load_completion_time'].copy()
    }
    load = next(load for load in daily_loads if load['id'] == load_id)
    driver = next(driver for driver in drivers if driver['id'] == driver_id)
    new_state['load_statuses'][load_id] = 'assigned'
    if load['return']:
        new_state['driver_locations'][driver_id] = load['return']
    else:
        new_state['driver_locations'][driver_id] = load['delivery']
    new_state['driver_load_completion_time'][driver_id] = state['time'] + calculate_handling_time(load, driver)
    return new_state

# Memorization dictionary for value function
V = {}

# DataFrame to store actions and states
df_results = pd.DataFrame(columns=['time', 'state','driver_id', 'load_id', 'new_state','value'])

# Define possible actions with compatibility check
def get_actions(state):
    # Set of drivers who are free at the current time and have the capability for the load
    free_drivers = {driver['id'] for driver in drivers
                    if state['time'] >= state['driver_load_completion_time'][driver['id']]
                    and (set(driver['capability']) & {load['size'] for load in daily_loads if state['load_statuses'][load['id']] == 'unassigned'})
                    and driver['availability_start'] <= state['time'].time() < driver['availability_end']}

    actions = []
    for driver in drivers:
        for load in daily_loads:
            if state['load_statuses'][load['id']] == 'unassigned' and driver['id'] in free_drivers and load['size'] in driver['capability']:
                if load['hazmat'] == 'yes' and driver['hazmat_certified'] == 'no':
                    continue
                pickup_time = state['time'] + get_distance_time(driver['location'], load['pickup'])[1]
                delivery_time = pickup_time + get_distance_time(load['pickup'], load['delivery'])[1]
                if load['return']:
                    return_time = delivery_time + get_distance_time(load['delivery'], load['return'])[1]
                else:
                    return_time = None
                if load['pickup_window'][0] <= pickup_time <= load['pickup_window'][1] and load['delivery_window'][0] <= delivery_time <= load['delivery_window'][1]:
                    if load['return'] is None or (load['return'] and load['return_window'][0] <= return_time <= load['return_window'][1]):
                        actions.append((driver['id'], load['id']))
    return actions

# Recursive function to compute the value function
def value_function(state):
    state_tuple = (
        state['time'],
        tuple(sorted(state['driver_locations'].items())),
        tuple(sorted(state['load_statuses'].items())),
        tuple(sorted(state['driver_load_completion_time'].items()))
    )

    if state_tuple in V:
        return V[state_tuple]

    if all(status == 'assigned' for status in state['load_statuses'].values()) or state['time'] >= horizon_end:
        return 0  # No more loads to assign or time horizon reached

    max_value = float('-inf')
    actions = get_actions(state)

    if not actions:
        new_state = state.copy()
        new_state['time'] += time_step
        df_results.loc[len(df_results)] = [state['time'], state, 'NoAction', 'NoAction', new_state, 0]
        V[state_tuple] = value_function(new_state)
        return V[state_tuple]

    for action in actions:
        new_state = transition_function(state, action)
        reward = reward_function(state, action)
        future_value = value_function(new_state)
        total_value = reward + future_value
        if total_value >= max_value:
            max_value = total_value
            df_results.loc[len(df_results)] = [state['time'], state, action[0], action[1], new_state, total_value]

    V[state_tuple] = max_value
    return max_value

def extract_optimal_policy(_df_results, _initial_state):
    # Initialize the current state with the initial state
    _current_state = _initial_state
    
    # Create an empty DataFrame to store the optimal policy
    _df_policy = pd.DataFrame(columns=['time', 'state', 'driver_id', 'load_id', 'new_state', 'value'])
    
    while True:
        # Filter rows where the state is the current state
        _state_rows = _df_results[_df_results['state'] == _current_state]
        
        # If there are no rows with the current state, break the loop
        if _state_rows.empty:
            break
        
        # Find the row with the highest value for the current state
        _max_value_row = _state_rows.loc[_state_rows['value'].idxmax()]
        
        # Append the row with the highest value to the policy DataFrame
        _df_policy = pd.concat([_df_policy, _max_value_row.to_frame().T], ignore_index=True)
        
        # Update the current state to the new state
        _current_state = _max_value_row['new_state']
    
    return _df_policy





# Main loop for daily dispatching
all_results = pd.DataFrame(columns=['time', 'driver_id', 'load_id'])
dispatched_loads = set()

while current_time < horizon_end:
    # Update time window for the current day
    # print('dispatching for '+ str(current_time))
    day_start = current_time
    day_end = current_time + timedelta(days=1)
    
    # Filter loads for the current day
    daily_loads = [load for load in loads if load['id'] not in dispatched_loads and load['pickup_window'][0] < day_end and load['delivery_window'][1] > day_start]

    if not daily_loads:
        current_time += timedelta(days=1)
        continue

    # Initialize state
    initial_state = initialize_state()
    df_loads_filtered = pd.DataFrame(daily_loads)
    V = {}  # Reset value function memorization for each day
    
    # Compute the optimal value for the current day
    optimal_value = value_function(initial_state)
       
    optimal_policy = extract_optimal_policy(df_results, initial_state)
  
   
    # Filter out 'NoAction' entries
    optimal_policy = optimal_policy[optimal_policy['driver_id'] != 'NoAction']
    daily_results = optimal_policy.loc[:, ['time', 'driver_id', 'load_id']]
    # print(daily_results)
    all_results = pd.concat([all_results, daily_results], ignore_index=True)
    
    dispatched_loads = set()
    for _, row in optimal_policy.iterrows():
        dispatched_loads.add(row['load_id'])


    # Move to the next day
    current_time += timedelta(days=1)

results = all_results.loc[:, ['load_id', 'driver_id', 'time']]
results=results.rename(columns={'time': 'Best Start Time', 'driver_id': 'Driver', 'load_id': 'Load #'})