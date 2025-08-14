"""
Load classifer:
Inputs:
List of potential carriers: DHE, Quality_Container, Loyalty
planDate: A string representing the date in the format 'yyyy-mm-dd', e.g., '2024-12-05'.
available_loads: A list of loads pending pickup, currently located at the terminal or warehouse.
prePulled_loads: A list of loads that have been pre-pulled to the yard and are awaiting delivery.
pendingReturn_loads: A list of loads where the delivery has been completed, but the empty container still needs to be returned.

(Note. For all loads the following data fileds are necessary:
 reference_number,'distance', 'callerName', 'shipperName', 'consigneeName', 'hazmat',
'hot', 'liquor', 'revenue', 'type_of_load', 'totalWeight',
'containerSizeName', 'lastFreeDay', 'emptyDay', 'freeReturnDate',
'pickupFromTime', 'pickupToTime', 'deliveryFromTime', 'deliveryToTime',
'returnFromTime', 'returnToTime', 'dischargedDate', 'availableDate')

output: 
list of (ref_numbers, current_load_type, predicted_next_move)
"""

import os
import pandas as pd
import numpy as np
import xgboost as xgb
import pickle  # For saving/loading pre-trained encoders
import newrelic.agent
import warnings
from app.modules.upload_model.upload_file import get_file_content
from app.modules.upload_model.upload_file_service import get_model_files
from load_scheduler.utility import process_date_fields, get_label_from_appointment_date, finalize_prediction_label
from sklearn.exceptions import InconsistentVersionWarning

from settings import settings

warnings.filterwarnings("ignore", category=InconsistentVersionWarning)


@newrelic.agent.function_trace(name='get_optimal_load', group='Custom')
async def getOptimalLoad(carrier, timeZone, planDate, available_loads, prePulled_loads, pendingReturn_loads):
    planDate = pd.Timestamp(planDate)
    dow = planDate.dayofweek + 1  # Monday=1

    # Prepare data for processing
    load_data = [
        (available_loads, 'available'),
        (prePulled_loads, 'pre_pulled'),
        (pendingReturn_loads, 'pending_return')
    ]
    
    predicted_pickups = pd.DataFrame()
    predicted_deliveries = pd.DataFrame()

    results = []
    for loads, load_type in load_data:
        if not loads:  # Skip if there are no loads for this type
            continue

        df_all = pd.DataFrame(loads)
        df_all['carrier'] = carrier
        df_all['current_load_type'] = load_type
        df_all['DOW'] = dow  # Add day of week
        df_all['deliveryFromTime_hour'] = pd.to_datetime(df_all['deliveryFromTime'], errors='coerce', utc=True).dt.tz_convert(timeZone).dt.hour

        xgb_results_list = []
        X_test_list = []
        for type_of_load in ['IMPORT', 'EXPORT']:
            df = df_all[df_all['type_of_load'] == type_of_load].copy()
            if len(df) == 0:
                continue

            if df.empty:
                continue

            # Process date fields to convert to day differences
            df = process_date_fields(df, planDate, timeZone)

            ref_numbers = df['reference_number']
            current_load_type = df['current_load_type']
            X_test = df.drop(['reference_number', 'current_load_type'], axis=1)

            # # Load the specific model for this load type
            # uploaded_file = await get_model_files(carrier, 'schedule', load_type)

            # if len(uploaded_file) > 0:
            #     model_filename = uploaded_file[0]["file_url"]
            #     model_filename = model_filename.split("/")[-1]
            # else:
            #     raise FileNotFoundError(f"Model file {model_filename} not found. Error: {str(e)}")

            if settings.READ_MODELS_FROM_LOCAL:
                model_filename = f"model_schedule_{load_type}_{type_of_load}.pkl"
            else:
                model_file_details = await get_model_files(carrier, "schedule", type_of_load, file_name=load_type, ignore_carrier=True)
                if not model_file_details:
                    raise FileNotFoundError(f"Model file for {load_type} not found.")
                model_file_detail = model_file_details[0]
                if model_file_detail["carrier"]:
                    model_filename = f"model_schedule_{carrier}_{load_type}_{type_of_load}_{model_file_details[0]["version"]}.pkl"
                else:
                    model_filename = f"model_schedule_{load_type}_{type_of_load}_{model_file_details[0]["version"]}.pkl"

            try:
                raw_model, encoders = await get_file_content(model_filename, type_of_load)
                # loaded_model = await load_model_content(model_content, loaded_model)
                loaded_model = xgb.Booster()
                loaded_model.load_model(raw_model)

            except Exception as e:
                raise FileNotFoundError(f"Model file {model_filename} not found. Error: {str(e)}")

            # Retrieve expected features from model metadata
            expected_features = loaded_model.feature_names

            # Reorder columns to match expected feature order
            X_test = X_test[expected_features]

            # Encoding categorical columns using pre-trained LabelEncoders
            categorical_columns = X_test.select_dtypes(include=['object']).columns
            # Automatically detect the subset of categorical columns from the loaded encoders
            subset_categorical_columns = [col for col in X_test.columns if col in encoders.cols]
            try:
                X_test[subset_categorical_columns] = encoders.transform(X_test[subset_categorical_columns])
            except ValueError as e:
                raise ValueError(f"Error encoding columns: {str(e)}")
            
            for col in categorical_columns:
                if X_test[col].dtype == 'object': X_test[col] = pd.to_numeric(X_test[col], errors='coerce')

            dtest = xgb.DMatrix(X_test)

            # Retrieve class labels from metadata
            class_labels = loaded_model.attr('class_labels').split(',')

            # Predict next moves
            y_pred_prob = loaded_model.predict(dtest)
            y_pred = np.argmax(y_pred_prob, axis=1)

            # Decode predictions
            y_pred_labels = [class_labels[i] for i in y_pred]

            # Combine predictions with reference numbers and load type
            xgb_results = pd.DataFrame({
                'reference_number': ref_numbers,
                'current_load_type': current_load_type,
                'predicted_next_move': y_pred_labels,
                'revenue': df['revenue']
            })

            xgb_results_list.append(xgb_results)
            # Add back necessary columns if they are not in X_test
            for col in ['deliveryFromTime', 'lastFreeDay', 'pickupFromTime', 'freeReturnDate', 'cutOff', 'distance', 'containerAvailableDay', 'type_of_load']:
                if col not in X_test.columns:
                    X_test[col] = pd.Series(pd.to_numeric(df[col], errors='coerce'), dtype=np.float64)
                else:
                    X_test[col] = df[col]
            X_test['deliveryFromTime_hour'] = df['deliveryFromTime_hour']
            X_test_list.append(X_test.copy())

        xgb_results = pd.concat(xgb_results_list, ignore_index=True)
        X_test = pd.concat(X_test_list, ignore_index=True)

        if load_type == 'available':
            if dow == 5:
                xgb_results.loc[((X_test['freeReturnDate'] <= 2) & (X_test['type_of_load'] == 'IMPORT')), 'predicted_next_move'] = 'Pull-Deliver-Return'
                xgb_results.loc[((X_test['cutOff'] <= 2) & (X_test['type_of_load'] == 'EXPORT') & (X_test['containerAvailableDay'].notna())), 'predicted_next_move'] = 'Pull-Deliver-Return'
            else:
                xgb_results.loc[((X_test['freeReturnDate'] <= 0) & (X_test['type_of_load'] == 'IMPORT')), 'predicted_next_move'] = 'Pull-Deliver-Return'
                xgb_results.loc[((X_test['cutOff'] <= 0) & (X_test['type_of_load'] == 'EXPORT') & (X_test['containerAvailableDay'].notna())), 'predicted_next_move'] = 'Pull-Deliver-Return'
            
            xgb_results.loc[(xgb_results['predicted_next_move'] == 'Pre-Pull'), 'predicted_next_move'] = 'Pull-Deliver'
            
            if dow == 5:  # Friday
                xgb_results.loc[(((X_test['lastFreeDay'] <= 2) | (X_test['pickupFromTime'] <= 2)) & (xgb_results['predicted_next_move'] == 'Do Nothing')) , 'predicted_next_move'] = 'Pull-Deliver'
            else:  # Other weekdays
                xgb_results.loc[(((X_test['lastFreeDay'] <= 0) | (X_test['pickupFromTime'] <= 0)) & (xgb_results['predicted_next_move'] == 'Do Nothing')) , 'predicted_next_move'] = 'Pull-Deliver'
            
            xgb_results.loc[((X_test['deliveryFromTime'] > 0) & ((X_test['pickupFromTime'] <= 0) | (X_test['lastFreeDay'] <= 0))), 'predicted_next_move'] = 'Pre-Pull'

        if load_type == 'pre_pulled':
            if dow == 5:
                xgb_results.loc[((X_test['freeReturnDate'] <= 2) & (X_test['type_of_load'] == 'IMPORT')), 'predicted_next_move'] = 'Deliver-Return'
                xgb_results.loc[((X_test['cutOff'] <= 2) & (X_test['type_of_load'] == 'EXPORT') & (X_test['containerAvailableDay'].notna())), 'predicted_next_move'] = 'Deliver-Return'
            else:
                xgb_results.loc[((X_test['freeReturnDate'] <= 0) & (X_test['type_of_load'] == 'IMPORT')), 'predicted_next_move'] = 'Deliver-Return'
                xgb_results.loc[((X_test['cutOff'] <= 0) & (X_test['type_of_load'] == 'EXPORT') & (X_test['containerAvailableDay'].notna())), 'predicted_next_move'] = 'Deliver-Return'

        if load_type == 'pending_return':
            if dow == 5:
                xgb_results.loc[((X_test['freeReturnDate'] <= 2) & (X_test['type_of_load'] == 'IMPORT')), 'predicted_next_move'] = 'Return'
                xgb_results.loc[((X_test['cutOff'] <= 2) & (X_test['type_of_load'] == 'EXPORT') & (X_test['containerAvailableDay'].notna())), 'predicted_next_move'] = 'Return'
            else:
                xgb_results.loc[((X_test['freeReturnDate'] <= 0) & (X_test['type_of_load'] == 'IMPORT')), 'predicted_next_move'] = 'Return'
                xgb_results.loc[((X_test['cutOff'] <= 0) & (X_test['type_of_load'] == 'EXPORT') & (X_test['containerAvailableDay'].notna())), 'predicted_next_move'] = 'Return'
            
            xgb_results.loc[((X_test['type_of_load'] == 'EXPORT') & (X_test['containerAvailableDay'].notna())), 'predicted_next_move'] = 'Do Nothing'
        
        # Filter out 'Do Nothing' predictions
        xgb_results = xgb_results[xgb_results['predicted_next_move'] != 'Do Nothing']
        results.append(xgb_results)
        
        # Add logic for scheduled predictions
        remaining_loads = df[~df['reference_number'].isin(xgb_results['reference_number'])]
        appointment_label_results = pd.DataFrame()
        if not remaining_loads.empty:
            appointment_label_results = get_label_from_appointment_date(remaining_loads, load_type)
            results.append(appointment_label_results)
        
        if load_type == 'available':
            predicted_for_pickup = df_all[
                (df_all['reference_number'].isin(xgb_results['reference_number']) | 
                 (not appointment_label_results.empty and df_all['reference_number'].isin(appointment_label_results['reference_number']))) & 
                 ('isDropMove' in df_all.columns and df_all['isDropMove'] == False)
            ]

            if not predicted_for_pickup.empty:
                predicted_for_pickup = predicted_for_pickup.copy()
                for col in ['pickupFromTime', 'pickupToTime', 'actualPickupDate']:
                    if col not in predicted_for_pickup.columns:
                        predicted_for_pickup[col] = 0
                    else:
                        predicted_for_pickup.loc[:, col] = 0
                predicted_pickups = pd.concat([predicted_pickups, predicted_for_pickup], ignore_index=True)
        
        if load_type == 'pre_pulled':
            predicted_for_delivery = df_all[
                (df_all['reference_number'].isin(xgb_results['reference_number']) |
                 (not appointment_label_results.empty and df['reference_number'].isin(appointment_label_results['reference_number']))) & 
                 ('isDropMove' in df_all.columns and df_all['isDropMove'] == False)
            ]

            if not predicted_for_delivery.empty:
                predicted_for_delivery = predicted_for_delivery.copy()
                for col in ['deliveryFromTime', 'deliveryToTime', 'actualDeliveryDate']:
                    if col not in predicted_for_delivery.columns:
                        predicted_for_delivery[col] = 0
                    else:
                        predicted_for_delivery.loc[:, col] = 0
                predicted_deliveries = pd.concat([predicted_deliveries, predicted_for_delivery], ignore_index=True)

    if results:
        final_results = pd.concat(results, ignore_index=True).drop_duplicates()
        grouped_results = final_results.groupby('reference_number').agg({
            'current_load_type': 'first',
            'predicted_next_move': list,
            'revenue': 'first'
        }).reset_index()
        grouped_results['predicted_next_move'] = grouped_results['predicted_next_move'].apply(finalize_prediction_label)
        final_results = grouped_results
    else:
        final_results = pd.DataFrame(columns=['reference_number', 'current_load_type', 'predicted_next_move'])

    final_results = final_results.drop_duplicates(subset=['reference_number', 'predicted_next_move'], keep='first')
    return final_results.to_dict(orient='records')
