import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import ConfusionMatrixDisplay
import xgboost as xgb
from sklearn.metrics import classification_report, accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.utils import resample
import matplotlib.dates as mdates
from geopy.distance import geodesic
from datetime import datetime, timedelta
from app.modules.optimizer.constants import DISTANCE_MULTIPLIER


engine = create_engine('postgresql://drayos:AxZcKG5D63iQzpq@embedded-email-cluster-dev.cluster-c3k5hvhtcivp.us-east-1.rds.amazonaws.com/drayos')



moves_source_sql = '''
SELECT 
    reference_number, 
    %(current_status)s AS current_status,
    %(recommended_next_move)s AS recommended_next_move, 
    hazmat, 
    hot, 
    liquor, 
    revenue, 
    "availableDate", 
    "lastFreeDay", 
    "emptyDay", 
    "freeReturnDate", 
    "pickupFromTime", 
    "pickupToTime", 
    "deliveryFromTime", 
    "deliveryToTime", 
    "returnFromTime", 
    "returnToTime", 
    "containerNo", 
    "callerName", 
    "shipperName", 
    "consigneeName", 
    "emptyOriginName", 
    1 as priority,
    60 as dwell_time,
    'no' as assigned,
    "shipperInfo" -> 'address' ->> 'lat' AS shipper_lat,  
    "shipperInfo" -> 'address' ->> 'lng' AS shipper_lng,
    "consigneeInfo" -> 'address' ->> 'lat' AS consignee_lat,  
    "consigneeInfo" -> 'address' ->> 'lng' AS consignee_lng,
    null AS latest_lat,
    null AS latest_lng,
    null AS yard_lat,
    null AS yard_lng,
    null AS origin_lat,
    null AS origin_lng,
    null AS destination1_lat,
    null AS destination1_lng,
    null AS destination2_lat,
    null AS destination2_lng,
    null AS origin_from_time,
    null AS origin_to_time,
    null AS destination1_from_time,
    null AS destination1_to_time,
    null AS destination2_from_time,
    null AS destination2_to_time
FROM 
    "loadData" ld 
WHERE 
    carrier = %(carrier)s
    AND reference_number = %(reference_number)s
;'''


latest_address_source_sql = '''
select  dor.address, dor.address ->> 'lat' as latest_lat, dor.address ->> 'lng' as latest_lng
from  "driverOrder" dor left join "loadData" ld 
on dor."loadId" = ld."id" 
where ld.reference_number = %(reference_number)s
and dor.enroute < '2024-08-26'
order by indx desc
limit 1 
;'''


drivers_source_sql = '''
select  dp.driver as id, dp."driverName" as name , 
ROUND(CAST((sum(dp."finalAmount") / sum(dor.distance)) AS NUMERIC), 2) AS pay_rate,
time '06:00' as work_start,
time '18:00' as work_end,
0.9 as reliability,
37.8015832 as current_location_lat,
-122.2904205 as current_location_lng
from "driverOrder" dor left join "loadData" ld 
on dor."loadId" = ld."id" 
left join driverpays dp
on dp."toEventId" = dor.id 
where ld.carrier =  %(carrier)s
and  ld.type_of_load in ('IMPORT', 'EXPORT')
and dor."isVoidOut" = false  
GROUP by dp.driver, dp."driverName" 

;'''


driver_params = {
    'carrier': '641a10875b159a160742327e'
}
drivers_df = pd.read_sql(drivers_source_sql, engine, params=driver_params)



yards_source_sql = '''
select distinct company_name as name, address ->> 'lat' as lat, address ->> 'lng' as lng
from "driverOrder"
where company_name like %(company_pattern1)s and company_name like %(company_pattern2)s
;'''


yard_params = {'company_pattern1': '%ROADEX%', 'company_pattern2': '%YARD%'}
yards_df = pd.read_sql(yards_source_sql, engine, params=yard_params)


next_move_recoms = pd.read_csv('NextMovesOutput.csv')


moves_list = []
for index, row in next_move_recoms.iterrows():
    moves_params = {
        'carrier': '641a10875b159a160742327e',
        'reference_number': row['reference_number'],
        'current_status': row['current_status'],
        'recommended_next_move': row['recommended_next_move']
    }

    address_params = {
        'reference_number': row['reference_number']
    }
    
    # Fetch the load data as a DataFrame
    load = pd.read_sql(moves_source_sql, engine, params=moves_params)
    # latest_add=pd.read_sql(latest_address_source_sql, engine, params=address_params)
    
    # Check if the DataFrame is not empty before appending
    if not load.empty:
        # if not latest_add.empty:
        #     load['latest_lat']=latest_add['latest_lat'] 
        #     load['latest_lng']=latest_add['latest_lng']
        moves_list.append(load)

# Concatenate only if there are DataFrames in the list
if moves_list:
    pre_mapped_moves = pd.concat(moves_list, ignore_index=True)
else:
    pre_mapped_moves = pd.DataFrame()  # Return an empty DataFrame if no loads were found






#################
# Define the mapping in a nested dictionary
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






################################################
# Function to calculate travel time (assuming 60 mph speed)
def calculate_travel_time(lat1, lng1, lat2, lng2):
    if pd.isna(lat1) or pd.isna(lng1) or pd.isna(lat2) or pd.isna(lng2):
        return 0, 0  # No travel if one of the locations is missing
    distance_miles = geodesic((lat1, lng1), (lat2, lng2)).miles * DISTANCE_MULTIPLIER
    travel_time_hours = distance_miles / 60  # Assuming 60 miles per hour speed
    return distance_miles, travel_time_hours

# Function to find the closest yard with available capacity
def find_closest_available_yard(driver_lat, driver_lng, yards_df):
    min_distance = float('inf')
    closest_yard = None

    for _, yard in yards_df.iterrows():
        if yard['capacity'] > 0:  # Check if the yard has capacity
            distance, _ = calculate_travel_time(driver_lat, driver_lng, yard['lat'], yard['lng'])
            if distance < min_distance:
                min_distance = distance
                closest_yard = yard
    
    return closest_yard

# Function to check driver availability
def is_driver_available(driver, move, assigned_moves, current_time):
    for am in assigned_moves:
        if driver['id'] == am['driver_id']:
            # Calculate total time (move duration + travel)
            travel_time = calculate_travel_time(am['destination1_lat'], am['destination1_lng'], move['origin_lat'], move['origin_lng'])[1]
            total_move_time = (am['end_time'] - am['start_time']).total_seconds() / 3600 + travel_time
            if current_time < am['end_time'] + timedelta(hours=total_move_time):
                return False

    # Check if driver is within their work hours
    return driver['work_start'] <= current_time.time() <= driver['work_end']

# Function to assign the best available driver to a move
def assign_best_driver(drivers, move, assigned_moves, current_time, yards_df):
    available_drivers = []
    
    for _, driver in drivers.iterrows():
        if is_driver_available(driver, move, assigned_moves, current_time):
            if len(assigned_moves) > 0:
                last_move = next((m for m in assigned_moves if m['driver_id'] == driver['id']), None)
                if last_move:
                    travel_time = calculate_travel_time(last_move['destination1_lat'], last_move['destination1_lng'], 
                                                        move['origin_lat'], move['origin_lng'])[1]
                else:
                    travel_time = 0  # No previous assignment
            else:
                travel_time = 0  # No previous assignment
                
            available_drivers.append({
                'driver_id': driver['id'],
                'travel_time': travel_time,
                'pay_rate': driver['pay_rate'],
                'reliability': driver['reliability'],
                'current_location_lat': driver['current_location_lat'],  # Track driver's current lat
                'current_location_lng': driver['current_location_lng']   # Track driver's current lng
            })
    
    if available_drivers:
        best_driver = sorted(available_drivers, key=lambda x: (x['travel_time'], x['pay_rate'], -x['reliability']))[0]
        return best_driver['driver_id']
    return None

# Function to handle driver return to the closest yard if no immediate next move and check yard capacity
def return_driver_to_closest_yard(driver, assigned_moves, yards_df, current_time):
    last_move = next((m for m in assigned_moves if m['driver_id'] == driver['id']), None)
    if last_move:
        closest_yard = find_closest_available_yard(last_move['destination1_lat'], last_move['destination1_lng'], yards_df)
        if closest_yard is not None:
            travel_to_yard_time = calculate_travel_time(last_move['destination1_lat'], last_move['destination1_lng'], 
                                                        closest_yard['lat'], closest_yard['lng'])[1]
            closest_yard['capacity'] -= 1  # Reduce yard capacity when driver returns
            return {
                'driver_id': driver['id'],
                'yard_name': closest_yard['name'],
                'yard_lat': closest_yard['lat'],
                'yard_lng': closest_yard['lng'],
                'arrival_time': current_time + timedelta(hours=travel_to_yard_time)
            }
    return None

# Function to create recommendation plan with yard return and second destination logic
def generate_recommendation_plan(drivers, moves, yards, date):
    assigned_moves = []
    recommendation_plan = []
    current_time = datetime.combine(date, datetime.min.time())
    end_time = datetime.combine(date, datetime.max.time())

    while current_time <= end_time:
        # Select unassigned moves in the time step
        moves_in_time_step = moves[(moves['origin_from_time'] <= current_time) & 
                                   (moves['origin_to_time'] >= current_time) & 
                                   (moves['assigned'] == 'no')]
        
        # Sort moves by revenue and priority
        moves_in_time_step = moves_in_time_step.sort_values(by=['revenue', 'priority'], ascending=[False, False])
        
        for _, move in moves_in_time_step.iterrows():
            best_driver_id = assign_best_driver(drivers, move, assigned_moves, current_time, yards)
            if best_driver_id:
                move_duration_hours = move['move_duration_minutes'] / 60
                destination1_time = current_time + timedelta(hours=move_duration_hours)
                
                # Handle second destination (if exists)
                if pd.notna(move['destination2_lat']) and pd.notna(move['destination2_lng']):
                    stop_duration = timedelta(minutes=move['dwell_time'])
                    travel_time_to_second_dest = calculate_travel_time(move['destination1_lat'], move['destination1_lng'], 
                                                                       move['destination2_lat'], move['destination2_lng'])[1]
                    destination2_time = destination1_time + stop_duration + timedelta(hours=travel_time_to_second_dest)
                    end_time_for_move = destination2_time
                else:
                    end_time_for_move = destination1_time
                
                # Mark the move as assigned
                moves.at[move.name, 'assigned'] = 'yes'
                
                # Store assigned move in the recommendation plan
                recommendation_plan.append({
                    'move_id': move['id'],
                    'driver_id': best_driver_id,
                    'start_time': current_time,
                    'destination1_time': destination1_time,
                    'destination2_time': destination2_time if pd.notna(move['destination2_lat']) else None,
                    'end_time': end_time_for_move
                })
                
                # Add move to assigned moves
                assigned_moves.append({
                    'move_id': move['id'],
                    'driver_id': best_driver_id,
                    'start_time': current_time,
                    'end_time': end_time_for_move,
                    'destination1_lat': move['destination2_lat'] if pd.notna(move['destination2_lat']) else move['destination1_lat'],
                    'destination1_lng': move['destination2_lng'] if pd.notna(move['destination2_lng']) else move['destination1_lng']
                })

                # Increase yard capacity when the driver departs for the move
                last_move = next((m for m in assigned_moves if m['driver_id'] == best_driver_id), None)
                if last_move:
                    yard_name = last_move['yard_name'] if 'yard_name' in last_move else None
                    if yard_name:
                        yards.loc[yards['name'] == yard_name, 'capacity'] += 1  # Increase yard capacity
                        
        # Check if any drivers need to return to a yard
        for _, driver in drivers.iterrows():
            has_next_move = any(am['driver_id'] == driver['id'] and am['start_time'] <= current_time + timedelta(hours=1) for am in assigned_moves)
            if not has_next_move:
                yard_return_plan = return_driver_to_closest_yard(driver, assigned_moves, yards, current_time)
                if yard_return_plan:
                    recommendation_plan.append({
                        'move_id': None,  # No move, just returning to yard
                        'driver_id': driver['id'],
                        'start_time': current_time,
                        'destination1_time': yard_return_plan['arrival_time'],
                        'yard_name': yard_return_plan['yard_name']
                    })
        
        current_time += timedelta(hours=1)
    
    return pd.DataFrame(recommendation_plan)

# Adding move duration in minutes
def add_move_duration(moves_df):
    moves_df['move_duration_minutes'] = (moves_df['destination1_to_time'] - moves_df['origin_from_time']).dt.total_seconds() / 60
    return moves_df

# # Example drivers data with current location
# drivers_df = pd.DataFrame({
#     'id': [1, 2, 3, 4, 5],
#     'name': ['Driver1', 'Driver2', 'Driver3', 'Driver4', 'Driver5'],
#     'pay_rate': [20, 25, 22, 23, 24],
#     'work_start': [datetime.strptime('08:00', '%H:%M').time()] * 5,
#     'work_end': [datetime.strptime('18:00', '%H:%M').time()] * 5,
#     'reliability': [0.9, 0.85, 0.95, 0.8, 0.88],
#     'current_location_lat': [34.0522, 34.0522, 36.7783, 34.0522, 34.0522],  # Driver's initial location (lat)
#     'current_location_lng': [-118.2437, -118.2437, -119.4179, -118.2437, -118.2437]  # Driver's initial location (lng)
# })

# Example moves data with possible second destination and stop duration
# moves_df = pd.DataFrame({
#     'id': [1, 2, 3, 4, 5],
#     'ref_number': ['A001', 'A002', 'A003', 'A004', 'A005'],
#     'origin_from_time': [datetime(2024, 10, 13, 8), datetime(2024, 10, 13, 9),
#                           datetime(2024, 10, 13, 10), datetime(2024, 10, 13, 12),
#                           datetime(2024, 10, 13, 13)],
#     'origin_to_time': [datetime(2024, 10, 13, 10), datetime(2024, 10, 13, 11),
#                         datetime(2024, 10, 13, 12), datetime(2024, 10, 13, 13),
#                         datetime(2024, 10, 13, 14)],
#     'destination1_to_time': [datetime(2024, 10, 13, 11), datetime(2024, 10, 13, 12),
#                              datetime(2024, 10, 13, 13), datetime(2024, 10, 13, 14),
#                              datetime(2024, 10, 13, 15)],
#     'revenue': [500, 600, 550, 450, 700],
#     'priority': [1, 2, 1, 3, 2],
#     'origin_lat': [34.0522, 34.0522, 34.0522, 34.0522, 34.0522],
#     'origin_lng': [-118.2437, -118.2437, -118.2437, -118.2437, -118.2437],
#     'destination1_lat': [36.7783, 35.0522, 34.0522, 33.0522, 32.0522],
#     'destination1_lng': [-119.4179, -119.2437, -118.2437, -118.2437, -118.2437],
#     'destination2_lat': [None, 34.0522, None, 33.0522, None],  # Some moves have a second destination
#     'destination2_lng': [None, -118.2437, None, -118.2437, None],
#     'dwell_time': [None, 30, None, 20, None],  # Stop duration at destination 1
#     'assigned': ['no'] * 5  # Initially, all moves are unassigned
# })

# # Example yards data
# yards_df = pd.DataFrame({
#     'name': ['Yard1', 'Yard2', 'Yard3'],
#     'lat': [34.0522, 36.7783, 35.0522],
#     'lng': [-118.2437, -119.4179, -119.2437],
#     'capacity': [10, 15, 20]
# })

# Add move duration in minutes to moves_df
moves_df = add_move_duration(moves_df)

# Set the date
date = datetime(2024, 8, 26)

# Generate recommendation plan
recommendation_plan_df = generate_recommendation_plan(drivers_df, moves_df, yards_df, date)

print(recommendation_plan_df)
