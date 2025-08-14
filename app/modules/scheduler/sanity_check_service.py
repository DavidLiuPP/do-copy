from datetime import datetime, timedelta

def get_predicted_move(move_type_matches: list) -> str:
    """
    Determine the predicted next move based on the combination of move types present.
    
    Args:
        move_type_matches: List of move types that are present
        
    Returns:
        str: The predicted next move type
    """

    valid_actions = {
        'PULLCONTAINER': 'Pull',
        'DELIVERLOAD': 'Deliver',
        'RETURNCONTAINER': 'Return'
    }
    move_type_matches = sorted(move_type_matches, key=lambda x: list(valid_actions.keys()).index(x))

    if len(move_type_matches) == 1 and move_type_matches[0] == 'PULLCONTAINER':
        return 'Pre-Pull'

    return '-'.join([valid_actions[move_type] for move_type in move_type_matches])

def check_optimal_move_plan(recommended_moves):
    event_not_present = []
    already_completed = []
    early_pickup = []
    filtered_moves = []
    PICKUP_BEFORE_LAST_FREE_DAY = 4

    for r_move in recommended_moves:
        # Only process moves that need combination checking
        if r_move.get('predicted_next_move') in ['Pull-Deliver-Return', 'Pull-Deliver', 'Deliver-Return']:
            # Find all move types present in the current move
            move_types = ['PULLCONTAINER', 'DELIVERLOAD', 'RETURNCONTAINER']
            move_type_matches = [
                move_type for move_type in move_types 
                if any(m['type'] == move_type for m in r_move['move'])
            ]
            # Update the predicted move based on actual move types present
            r_move['predicted_next_move'] = get_predicted_move(move_type_matches)


        # 1. Check if the recommended moves profile_type is there in moves fields
        if not any(m['type'] == r_move['profile_type'] and not m.get('isVoidOut') for m in r_move['move']):
            event_not_present.append(r_move)
            r_move['error_reason'] = 'event_not_present'
        
        # 2. Check if the recommended moves are already arrived
        if any(m.get('departed') and m.get('type') == r_move['profile_type'] and not m.get('isVoidOut') for m in r_move['move']):
            already_completed.append(r_move)
            r_move['error_reason'] = 'already_completed'
        
        if r_move.get('profile_type') == 'RETURNCONTAINER' and not r_move.get('profile_name'):
            r_move['error_reason'] = 'return_container_not_accepted'
        
        # 3. For pickup loads, check if the day diffrence between recommended_appointment_from and move's lastFreeDay is greater than 1
        if r_move['profile_type'] == 'PULLCONTAINER' and r_move.get('lastFreeDay'):
            recommended_appointment_from = datetime.fromisoformat(r_move['recommended_appointment_from']).replace(tzinfo=None)
            lastFreeDay = datetime.fromisoformat(r_move['lastFreeDay']).replace(tzinfo=None)
            days_between = (lastFreeDay - recommended_appointment_from).days
            working_days = sum(1 for i in range(days_between + 1) if (recommended_appointment_from + timedelta(days=i)).weekday() < 5)
            if working_days > PICKUP_BEFORE_LAST_FREE_DAY:
                early_pickup.append(r_move)
                r_move['error_reason'] = 'early_pickup'

        # remove all moves with same reference number from early_pickup
        reference_numbers = list(set(move['reference_number'] for move in early_pickup))
        if r_move.get('reference_number') in reference_numbers:
            r_move['error_reason'] = 'early_pickup'

        if r_move.get('allEvents'):
            del r_move['allEvents']
        if r_move.get('lastFreeDay'):
            del r_move['lastFreeDay']
        if not r_move.get('error_reason'):
            filtered_moves.append(r_move)
        
    return filtered_moves