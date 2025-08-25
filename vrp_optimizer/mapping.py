from copy import deepcopy

from vrp_optimizer.helpers import minute_from_distance, show_time_from_minute_of_day, get_minute, get_days_difference
from app.modules.optimizer.constants import ONE_DAY_IN_MINUTES
# TIME_TO_SWITCH_CHASSIS is passed as parameter from assumptions

def get_event_times(node_data, _move, prev_completed_minute, endTime, timezone, proximity_to_node, distance_unit, time_to_switch_chassis, plan_date):

    current_time = endTime

    events = []

    move = deepcopy(_move)

    move.reverse()

    for event in move:
        obj = {
            "type": event["type"],
            "waiting_time": event["waiting_time"],
            "distance": event["distance"],
            "location": event["address"]["address"],
            "company_name": event["company_name"],
            "customer_id": event["customerId"],
            "map_location": [
                event["address"]["lat"],
                event["address"]["lng"]
            ]
        }

        enroute = current_time - minute_from_distance(event["distance"], distance_unit) - event["waiting_time"]
        arrived = current_time - event["waiting_time"]
        departed = current_time

        obj["recommended_enroute"] = show_time_from_minute_of_day(enroute)
        obj["recommended_arrived"] = show_time_from_minute_of_day(arrived)
        obj["recommended_departed"] = show_time_from_minute_of_day(departed)
        obj["minutes"] = {
            "recommended_enroute": enroute,
            "recommended_arrived": arrived,
            "recommended_departed": departed
        }

        current_time = enroute - event.get('early_arrival_waiting', 0)

        # Calculate appointment window accounting for queue waiting time
        from_date_day_diff = get_days_difference(plan_date.isoformat(), event.get('appointment_from'), timezone)
        to_date_day_diff = get_days_difference(plan_date.isoformat(), event.get('appointment_to'), timezone)

        if event.get('appointment_from'):
            obj["appointment_from"] = max(get_minute(event.get('appointment_from'), timezone) + (from_date_day_diff * ONE_DAY_IN_MINUTES), 0)
            obj["appointment_to"] = max(get_minute(event.get('appointment_to'), timezone) + (to_date_day_diff * ONE_DAY_IN_MINUTES), 0)
        events.append(obj)


    if node_data.get('chassis_pick_event'):
        travel_to_chassis_yard = minute_from_distance(node_data['chassis_pick_event']['distance'], distance_unit)
        node_data['chassis_pick_event']['recommended_departed'] = events[-1]['minutes']['recommended_enroute']
        node_data['chassis_pick_event']['recommended_arrived'] = node_data['chassis_pick_event']['recommended_departed'] - time_to_switch_chassis
        node_data['chassis_pick_event']['recommended_enroute'] = node_data['chassis_pick_event']['recommended_arrived'] - travel_to_chassis_yard
    else:
        first_event_enroute = int(events[-1]['minutes']['recommended_enroute'] - minute_from_distance(proximity_to_node, distance_unit))
        events[-1]['recommended_enroute'] = show_time_from_minute_of_day(first_event_enroute) 
        events[-1]['minutes']['recommended_enroute'] = first_event_enroute
        events[-1]['distance'] = proximity_to_node
        if (prev_completed_minute > first_event_enroute):
            first_event_enroute = prev_completed_minute

        if (first_event_enroute > events[-1]['minutes']['recommended_arrived']):
            events[-1]['recommended_arrived'] = show_time_from_minute_of_day(first_event_enroute)
            events[-1]['minutes']['recommended_arrived'] = first_event_enroute

    events.reverse()

    return events
