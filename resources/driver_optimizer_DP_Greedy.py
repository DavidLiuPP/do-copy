import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch
import random
import plotly.figure_factory as ff
from geopy.distance import geodesic
from app.modules.optimizer.constants import DISTANCE_MULTIPLIER





# Calculate Haversine distance for realistic distance computation
def calculate_distance(lat1, lng1, lat2, lng2):
    return geodesic((lat1, lng1), (lat2, lng2)).miles * DISTANCE_MULTIPLIER


# Calculate travel time (assuming 40 mph speed)
def calculate_travel_time(lat1, lng1, lat2, lng2):
    if pd.isna(lat1) or pd.isna(lng1) or pd.isna(lat2) or pd.isna(lng2):
        return 0, 0
    distance_miles = geodesic((lat1, lng1), (lat2, lng2)).miles * DISTANCE_MULTIPLIER
    travel_time_minutes = 60 * (distance_miles / 40)  # Assuming 40 mph speed
    return distance_miles, travel_time_minutes


# Add move duration to moves_df
def add_move_duration(moves_df):  
    for index, row in moves_df.iterrows():
        if pd.notna(row['destination2_lat']) and pd.notna(row['destination2_lng']):
            move_duration = (calculate_travel_time(row['origin_lat'], row['origin_lng'], row['destination1_lat'], row['destination1_lng'])[1]
                             + calculate_travel_time(row['destination1_lat'], row['destination1_lng'], row['destination2_lat'], row['destination2_lng'])[1])
        else:
            move_duration = calculate_travel_time(row['origin_lat'], row['origin_lng'], row['destination1_lat'], row['destination1_lng'])[1]
        moves_df.at[index, 'move_duration'] = move_duration
    return moves_df


# Reward Function
def reward_function(state, action, time_step):
    """Calculate the reward of taking an action in a given state."""
    current_time, drivers, moves = state
    driver_id, move_id = action

    # Extract move and driver information
    move = moves.loc[move_id]
    driver = drivers.loc[driver_id]

    # Calculate cost (distance to origin * pay rate)
    distance_to_origin = calculate_distance(
        driver['current_lat'], driver['current_lng'],
        move['origin_lat'], move['origin_lng']
    )
    cost = distance_to_origin * driver['pay_rate']

    # Calculate revenue
    revenue = move['revenue']

    # Penalties for deviating from time windows
    origin_start = move['origin_from_time']
    origin_end = move['origin_to_time']
    destination_start = move['destination1_from_time']
    destination_end = move['destination1_to_time']
    move_duration_minutes = move['move_duration']
    handling_time = timedelta(minutes=move_duration_minutes) 

    # Calculate arrival times
    arrival_at_origin = current_time + timedelta(hours=distance_to_origin / 40)  # Assume 40 mph speed
    arrival_at_destination = arrival_at_origin + handling_time

    # Time window penalties
    origin_penalty = max(0, (arrival_at_origin - origin_end).total_seconds() / 3600) * 1000  # Penalty per hour late
    destination_penalty = max(0, (arrival_at_destination - destination_end).total_seconds() / 3600) * 1000

    # Total reward
    reward = revenue - cost - origin_penalty - destination_penalty
    return reward

# Transition Function
def transition_function(state, action, time_step):
    """Update the state based on the action taken."""
    current_time, drivers, moves = state
    driver_id, move_id = action

    # Update driver information
    move = moves.loc[move_id]
    distance_to_origin = calculate_distance(
        drivers.loc[driver_id, 'current_lat'],
        drivers.loc[driver_id, 'current_lng'],
        move['origin_lat'], move['origin_lng']
    )
    move_duration_minutes = move['move_duration']
    handling_time = timedelta(minutes=move_duration_minutes) 
    travel_time_to_origin = timedelta(hours=distance_to_origin / 40)  # Assume 40 mph speed

    # Update driver location and availability
    drivers.at[driver_id, 'current_lat'] = move['destination1_lat']
    drivers.at[driver_id, 'current_lng'] = move['destination1_lng']
    drivers.at[driver_id, 'available_time'] = current_time + travel_time_to_origin + handling_time

    # Mark move as assigned
    moves.at[move_id, 'assigned'] = True

    # Increment time step
    new_time = current_time + time_step
    return new_time, drivers, moves

# Value Function
def value_function(state, actions, time_step):
    """Evaluate the best action based on the reward function."""
    best_action = None
    best_reward = -float('inf')

    for action in actions:
        reward = reward_function(state, action, time_step)
        if reward > best_reward:
            best_reward = reward
            best_action = action

    return best_action, best_reward



# Assignment Algorithm (Includes Gantt Chart Integration)
def greedy_dynamic_assignment(drivers_df, moves_df, time_step_minutes=1):
    """Greedy dynamic programming to assign drivers to moves."""
    time_step = timedelta(minutes=time_step_minutes)
    current_time = datetime(2024, 8, 26, 7, 0, 0)  ###change it to the begining of the day or now
    end_time = current_time + timedelta(hours=24)   ###change it to the end of the day or now

    # Initialize driver and move states
    drivers_df['available_time'] = current_time ###change it to the work start
    drivers_df['current_lat'] = drivers_df['current_location_lat']
    drivers_df['current_lng'] = drivers_df['current_location_lng']
    moves_df['assigned'] = False

    # Initialize state
    state = (current_time, drivers_df, moves_df)
    assignments = []

    while state[0] < end_time:
        current_time, drivers, moves = state

        # Generate all possible actions
        actions = [
            (driver_id, move_id)
            for driver_id in drivers.index
            for move_id in moves[moves['assigned'] == False].index
            if drivers.loc[driver_id, 'available_time'] <= current_time
        ]

        # Get the best action and its reward
        best_action, best_reward = value_function(state, actions, time_step)

        if best_action:
            # Record the assignment
            driver_id, move_id = best_action
            move = moves.loc[move_id]
            distance_to_origin = calculate_distance(
                drivers.loc[driver_id, 'current_lat'],
                drivers.loc[driver_id, 'current_lng'],
                move['origin_lat'],
                move['origin_lng']
            )
            move_duration_minutes = move['move_duration']
            handling_time = timedelta(minutes=move_duration_minutes) 
            travel_time_to_origin = timedelta(hours=distance_to_origin / 40)

            start_time = current_time + travel_time_to_origin
            end_time = start_time + handling_time

            assignments.append({
                'time': current_time,
                'driver_id': drivers.loc[driver_id, 'id'],
                'move_reference': move['reference_number'],
                'revenue': move['revenue'],
                'cost': distance_to_origin * drivers.loc[driver_id, 'pay_rate'],
                'start_time': start_time,
                'end_time': end_time
            })
            # Apply the transition function to update the state
            state = transition_function(state, best_action, time_step)
        else:
            # Increment time step if no actions are possible
            state = (current_time + time_step, drivers, moves)

    assignments_df = pd.DataFrame(assignments)

    return assignments_df






# Function to plot Gantt chart using Plotly
def plot_gantt(assignments_df):
    # Prepare data for Plotly Gantt
    gantt_data = []
    for _, row in assignments_df.iterrows():
        gantt_data.append({
            'Task': f" {row['driver_id']}",
            'Start': row['start_time'],
            'Finish': row['end_time'],
            'Resource': f" {row['move_reference']}"
        })

    # Generate enough colors for unique move references
    unique_resources = len(assignments_df['move_reference'].unique())
    colors = [f"rgb({i*15%255},{i*40%255},{i*60%255})" for i in range(unique_resources)]

    # Create Gantt chart
    fig = ff.create_gantt(
        gantt_data,
        index_col='Resource',
        show_colorbar=False,
        group_tasks=True,
        title='Driver Assignments Chart',
        showgrid_x=True,
        showgrid_y=True,
        colors=colors
    )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Drivers",
        xaxis=dict(
            type="date",
            tickformat="%Y-%m-%d %H:%M",
        ),
        height=600,
        margin=dict(l=150, r=50, t=50, b=50)
    )

    # Show Gantt chart
    fig.show()




# load input files
pre_mapped_moves = pd.read_csv('pre_mapped_moves_20240826.csv', parse_dates=['availableDate', 'lastFreeDay', 'emptyDay',
       'freeReturnDate', 'pickupFromTime', 'pickupToTime', 'deliveryFromTime','deliveryToTime', 'returnFromTime', 'returnToTime'])
drivers_df = pd.read_csv('drivers_df_20240826.csv', parse_dates=['work_start', 'work_end'])

#################
# Define the mapping logic
mapping_dict = {
    ('Pending Pickup', 'Pre-Pull'): {
        'origin_lat': 'shipper_lat',
        'origin_lng': 'shipper_lng',
        'destination1_lat': 'yard_lat',
        'destination1_lng': 'yard_lng',
        'destination2_lat': None,
        'destination2_lng': None,
        'origin_from_time': 'pickupFromTime',
        'origin_to_time': 'pickupToTime',
        'destination1_from_time': None,
        'destination1_to_time': None,
        'destination2_from_time': None,
        'destination2_to_time': None,
    },
    ('Pending Pickup', 'Deliver'): {
        'origin_lat': 'shipper_lat',
        'origin_lng': 'shipper_lng',
        'destination1_lat': 'consignee_lat',
        'destination1_lng': 'consignee_lng',
        'destination2_lat': None,
        'destination2_lng': None,
        'origin_from_time': 'pickupFromTime',
        'origin_to_time': 'pickupToTime',
        'destination1_from_time': 'deliveryFromTime',
        'destination1_to_time': 'deliveryToTime',
        'destination2_from_time': None,
        'destination2_to_time': None,
    },
    ('Pending Pickup', 'Deliver and Return'): {
        'origin_lat': 'shipper_lat',
        'origin_lng': 'shipper_lng',
        'destination1_lat': 'consignee_lat',
        'destination1_lng': 'consignee_lng',
        'destination2_lat': 'shipper_lat',
        'destination2_lng': 'shipper_lng',
        'origin_from_time': 'pickupFromTime',
        'origin_to_time': 'pickupToTime',
        'destination1_from_time': 'deliveryFromTime',
        'destination1_to_time': 'deliveryToTime',
        'destination2_from_time': 'returnFromTime',
        'destination2_to_time': 'returnToTime',
    },
    ('At Yard', 'Deliver'): {
        'origin_lat': 'yard_lat',
        'origin_lng': 'yard_lng',
        'destination1_lat': 'consignee_lat',
        'destination1_lng': 'consignee_lng',
        'destination2_lat': None,
        'destination2_lng': None,
        'origin_from_time': None,
        'origin_to_time': None,
        'destination1_from_time': 'deliveryFromTime',
        'destination1_to_time': 'deliveryToTime',
        'destination2_from_time': None,
        'destination2_to_time': None,
    },
    ('At Yard', 'Deliver and Return'): {
        'origin_lat': 'yard_lat',
        'origin_lng': 'yard_lng',
        'destination1_lat': 'consignee_lat',
        'destination1_lng': 'consignee_lng',
        'destination2_lat': 'shipper_lat',
        'destination2_lng': 'shipper_lng',
        'origin_from_time': None,
        'origin_to_time': None,
        'destination1_from_time': 'deliveryFromTime',
        'destination1_to_time': 'deliveryToTime',
        'destination2_from_time': 'returnFromTime',
        'destination2_to_time': 'returnToTime',
    },
    ('Pending Return', 'Return'): {
        'origin_lat': 'consignee_lat',
        'origin_lng': 'consignee_lng',
        'destination1_lat': 'shipper_lat',
        'destination1_lng': 'shipper_lng',
        'destination2_lat': None,
        'destination2_lng': None,
        'origin_from_time': None,
        'origin_to_time': None,
        'destination1_from_time': 'returnFromTime',
        'destination1_to_time': 'returnToTime',
        'destination2_from_time': None,
        'destination2_to_time': None,
    },
}


# Function to update a row based on the mapping dictionary
def update_moves(row):
    key = (row['current_status'], row['recommended_next_move'])
    if key in mapping_dict:
        for target_col, source_col in mapping_dict[key].items():
            if source_col is not None and pd.notna(row[source_col]):  # Only update if source_col is not None or NaN
                row[target_col] = row[source_col]  # Update target_col with source_col value
    return row



# Applying the function to update Moves DataFrame
moves_df = pre_mapped_moves.apply(update_moves, axis=1)



# Add move duration in minutes to moves_df
moves_df = add_move_duration(moves_df)
filtered_moves_df = moves_df.dropna(subset=['origin_lat', 'origin_lng', 
                                            'destination1_lat', 'destination1_lng',
                                            'origin_from_time', 'origin_to_time',
                                            'destination1_from_time', 'destination1_to_time' ])


drivers_df=drivers_df[:20]

assignments_df = greedy_dynamic_assignment(drivers_df, filtered_moves_df)

# Print the plan
print("Assignment Plan:")
print(assignments_df)

# Plot the chart
plot_gantt(assignments_df)


