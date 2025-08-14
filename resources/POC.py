import pandas as pd
import numpy as np
import plotly.express as px
import plotly.figure_factory as ff
from datetime import datetime, timedelta, time

# Define parameters
horizon_start = datetime(2024, 7, 11, 8)
horizon_end = datetime(2024, 7, 12, 0)
time_step = timedelta(seconds=60)

# Example data for loads (both imports and exports) with revenue, time windows, and handling time
loads = [
    {'id': 1, 'type': 'import', 'pickup': 'Port A', 'delivery': 'Warehouse A', 'return': 'Port A', 'pickup_window': (datetime(2024, 7, 11, 8), datetime(2024, 7, 11, 12)), 'delivery_window': (datetime(2024, 7, 11, 8), datetime(2024, 7, 11, 18)), 'return_window': (datetime(2024, 7, 11, 11), datetime(2024, 7, 11, 20)), 'size': '20ft', 'revenue': 1500, 'hazmat': 'no'},
    {'id': 2, 'type': 'import', 'pickup': 'Port B', 'delivery': 'Warehouse B', 'return': 'Port B', 'pickup_window': (datetime(2024, 7, 11, 8), datetime(2024, 7, 11, 11)), 'delivery_window': (datetime(2024, 7, 11, 8), datetime(2024, 7, 11, 15)), 'return_window': (datetime(2024, 7, 11, 11), datetime(2024, 7, 11, 20)), 'size': '40ft', 'revenue': 1700, 'hazmat': 'yes'},
    {'id': 3, 'type': 'export', 'pickup': 'Warehouse A', 'delivery': 'Port A', 'return': None, 'pickup_window': (datetime(2024, 7, 11, 10), datetime(2024, 7, 11, 14)), 'delivery_window': (datetime(2024, 7, 11, 10), datetime(2024, 7, 11, 18)), 'return_window': None, 'size': '20ft', 'revenue': 1600, 'hazmat': 'no'},
    {'id': 4, 'type': 'export', 'pickup': 'Warehouse B', 'delivery': 'Port B', 'return': None, 'pickup_window': (datetime(2024, 7, 11, 10), datetime(2024, 7, 11, 13)), 'delivery_window': (datetime(2024, 7, 11, 10), datetime(2024, 7, 11, 18)), 'return_window': None, 'size': '40ft', 'revenue': 1800, 'hazmat': 'yes'},
]

# Example data for drivers with capability, location, availability, and pay rate
drivers = [
    {'id': 1, 'location': 'Port A', 'availability_start': time(8,0), 'availability_end': time(17,0), 'pay_rate': 30, 'capability': ['20ft'], 'hazmat_certified': 'no'},
    {'id': 2, 'location': 'Warehouse B', 'availability_start': time(8,0), 'availability_end': time(17,0), 'pay_rate': 35, 'capability': ['20ft', '40ft'], 'hazmat_certified': 'yes'},
    {'id': 3, 'location': 'Port B', 'availability_start': time(8,0), 'availability_end': time(17,0), 'pay_rate': 32, 'capability': ['20ft', '40ft'], 'hazmat_certified': 'yes'}
]


def calculate_distance(location1, location2):
    distances = {
        ('Port A', 'Warehouse A'): 10,
        ('Port A', 'Warehouse B'): 20,
        ('Port B', 'Warehouse A'): 15,
        ('Port B', 'Warehouse B'): 10,
        ('Warehouse A', 'Port A'): 10,
        ('Warehouse A', 'Port B'): 15,
        ('Warehouse B', 'Port A'): 20,
        ('Warehouse B', 'Port B'): 10
    }
    if location1 == location2:
        return 0
    else:
        return distances.get((location1, location2), 20)  # Default to 10 if not found


def calculate_trip_time(location1, location2):
    distance = calculate_distance(location1, location2)
    speed = 45  
    trip_time = timedelta(hours=distance / speed)
    return trip_time


def calculate_handling_time(load, driver): # add the drivers current to pick up leg duration
    trip_time1 = calculate_trip_time(driver['location'], load['pickup'])
    trip_time2 = calculate_trip_time(load['pickup'], load['delivery'])
    if load['return']:
        trip_time3 = calculate_trip_time(load['delivery'], load['return'])
    else:
        trip_time3 = timedelta(hours=0)
    return trip_time1 + trip_time2 + trip_time3


# Initialize state
initial_state = {
    'time': horizon_start,
    'driver_locations': {d['id']: d['location'] for d in drivers},
    'load_statuses': {load['id']: 'unassigned' for load in loads},
    'driver_load_completion_time': {d['id']: horizon_start for d in drivers}  # Tracks when each driver will be free
}

# Define the reward function
def reward_function(state, action):
    driver_id, load_id = action
    load = next(load for load in loads if load['id'] == load_id)
    driver = next(driver for driver in drivers if driver['id'] == driver_id)
    if load['return']:
        distance = calculate_distance(driver['location'], load['pickup']) + calculate_distance(load['pickup'], load['delivery']) + calculate_distance(load['delivery'], load['return'])
    else:
        distance = calculate_distance(driver['location'], load['pickup']) + calculate_distance(load['pickup'], load['delivery'])
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
    load = next(load for load in loads if load['id'] == load_id)
    driver = next(driver for driver in drivers if driver['id'] == driver_id)
    new_state['load_statuses'][load_id] = 'assigned'
    if load['return']:
        new_state['driver_locations'][driver_id] = load['return']
    else:
        new_state['driver_locations'][driver_id] = load['delivery']
    new_state['driver_load_completion_time'][driver_id] = state['time'] + calculate_handling_time(load, driver)
    return new_state

# Memoization dictionary for value function
V = {}

# DataFrame to store actions and states
df_results = pd.DataFrame(columns=['time', 'state','driver_id', 'load_id', 'new_state','value'])

# Define possible actions with compatibility check
def get_actions(state):
    # Set of drivers who are free at the current time and have the capability for the load
    free_drivers = {driver['id'] for driver in drivers
                    if state['time'] >= state['driver_load_completion_time'][driver['id']]
                    and (set(driver['capability']) & {load['size'] for load in loads if state['load_statuses'][load['id']] == 'unassigned'})
                    and driver['availability_start'] <= state['time'].time() < driver['availability_end']}

    actions = []
    for driver in drivers:
        for load in loads:
            if state['load_statuses'][load['id']] == 'unassigned' and driver['id'] in free_drivers and load['size'] in driver['capability']:
                if load['hazmat'] == 'yes' and driver['hazmat_certified'] == 'no':
                    continue
                pickup_time = state['time'] + calculate_trip_time(driver['location'], load['pickup'])
                delivery_time = pickup_time + calculate_trip_time(load['pickup'], load['delivery'])
                if load['return']:
                    return_time = delivery_time + calculate_trip_time(load['delivery'], load['return'])
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

# Compute the optimal value starting from the initial state
optimal_value = value_function(initial_state)
print(f"Optimal Value: {optimal_value}")

optimal_policy = extract_optimal_policy(df_results, initial_state)
print(optimal_policy)

# Filter out 'NoAction' entries
df_visualize = optimal_policy[optimal_policy['driver_id'] != 'NoAction']

# Merge the load data to get details
df_loads = pd.DataFrame(loads)
df_drivers = pd.DataFrame(drivers)

# Join to get additional details
df_visualize = df_visualize.merge(df_loads, left_on='load_id', right_on='id', suffixes=('', '_load'))
df_visualize = df_visualize.merge(df_drivers, left_on='driver_id', right_on='id', suffixes=('', '_driver'))

# Create the Gantt chart data
gantt_data = []
for _, row in df_visualize.iterrows():
    start_time = row['time']
    end_time = row['new_state']['driver_load_completion_time'][row['driver_id']]
    gantt_data.append(dict(Task=f"Load {row['load_id']}",
                           Start=start_time,
                           Finish=end_time,
                           Resource=f"Driver {row['driver_id']}"))

# Create the Gantt chart
fig = ff.create_gantt(gantt_data, index_col='Resource', show_colorbar=True, group_tasks=True, showgrid_x=True, showgrid_y=True)

# Add driver names on each bar
for gantt_task in gantt_data:
    start_time = gantt_task['Start']
    end_time = gantt_task['Finish']
    task_name = gantt_task['Task']
    resource_name = gantt_task['Resource']
    fig.add_annotation(x=start_time + (end_time - start_time) / 2,
                       y=task_name,
                       text=resource_name,
                       showarrow=False,
                       font=dict(color='black'))

# Show the plot
fig.update_layout(
    title='Drivers Dispatching Chart',
    xaxis_title='Time',
    yaxis_title='Loads'
)
fig.show()