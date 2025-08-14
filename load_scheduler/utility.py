import pandas as pd
from typing import Set

# List of date fields to process
date_fields = [
    'lastFreeDay', 'emptyDay', 'freeReturnDate', 'cutOff',
    'pickupFromTime', 'pickupToTime',
    'deliveryFromTime', 'deliveryToTime',
    'returnFromTime', 'returnToTime',
    'dischargedDate', 'availableDate',
    'actualPickupDate', 'actualDeliveryDate',
    'containerAvailableDay'
]

# Helper function to transform date fields
def process_date_fields(df, planDate, timeZone):
    for field in date_fields:
        if field in df.columns:
            # Convert to datetime with coerce to handle None/NaT values
            # First convert to UTC, then convert to America/Los_Angeles
            df.loc[:, field] = pd.to_datetime(df[field], errors='coerce', utc=True).dt.tz_convert(timeZone)
            
            # Only process non-null values
            mask = df[field].notna()
            if mask.any():
                date_series = pd.DatetimeIndex(df.loc[mask, field])
                days_diff = (date_series - planDate).days
                df.loc[mask, field] = days_diff

            df[field] = pd.to_numeric(df[field], errors='coerce')
        else:
            df[field] = pd.NaT
    return df

def get_label_from_appointment_date(df, load_type):
    # Only update predictions for rows where predicted_next_move is "Do Nothing"
    df = df.copy()
    df.loc[:, 'predicted_next_move'] = 'Do Nothing'

    # Predict "Pre-pull"
    if load_type == 'available':
        df.loc[
            ((df['pickupFromTime'] == 0) | (df['pickupToTime'] == 0)),
            'predicted_next_move'
        ] = 'Pre-Pull'
    
    # Predict "Deliver"
    if load_type == 'pre_pulled':
        df.loc[
            ((df['deliveryFromTime'] == 0) | (df['deliveryToTime'] == 0)),
            'predicted_next_move'
        ] = 'Deliver'
    
    # Predict "Return"
    if load_type == 'pending_return':
        df.loc[
            ((df['returnFromTime'] == 0) | (df['returnToTime'] == 0)),
            'predicted_next_move'
        ] = 'Return'

    # Combine predictions with reference numbers and load type
    df = pd.DataFrame({
        'reference_number': df['reference_number'],
        'current_load_type': df['current_load_type'],
        'predicted_next_move': df['predicted_next_move'],
        'revenue': df['revenue']
    })

    # Filter out 'Do Nothing' predictions
    df = df[df['predicted_next_move'] != 'Do Nothing']

    return df


def finalize_prediction_label(next_move_labels):
    # If only one label, return it directly
    if len(next_move_labels) == 1:
        return next_move_labels[0]

    valid_actions = ['Pull', 'Deliver', 'Return']
    actions: Set[str] = set()
    for label in next_move_labels:
        # Split label by '-' and add each valid action
        for action in label.split('-'):
            action = action.strip()  # Remove any whitespace
            if action in valid_actions:
                actions.add(action)

    # sort actions in ['Pull', 'Deliver', 'Return'] this order
    actions = sorted(actions, key=lambda x: valid_actions.index(x))
    predicted_next_move = '-'.join(actions)

    if predicted_next_move == 'Pull':
        predicted_next_move = 'Pre-Pull'
    
    return predicted_next_move
