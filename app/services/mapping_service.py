from typing import List, Dict, Any
import re
import json
import os
import newrelic.agent
from datetime import datetime
from app.services.drayos_service import get_recommended_returns
from settings import settings
from bson.objectid import ObjectId
from app.mongo_services.mongo_service import get_mongo_db
from app.utils.distance_calc import calculate_distance_between_locations
from app.postgres_services.drayage_intelligence_service import get_drayage_intelligence, EMPTY_RETURN_OPTIONS

@newrelic.agent.function_trace(name='add_recommended_returns', group='Custom')
async def add_recommended_returns(user_payload: Dict[str, Any], plan_date: str, loads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    carrier = user_payload.get('carrier')
    distance_unit = user_payload.get('distanceUnit', 'mi')

    # get the empty return logic from postgres db
    placeholder_return_location = None
    drayage_intelligence = await get_drayage_intelligence(carrier, ['empty_logic_type', 'empty_return_location'])
    
    if (drayage_intelligence.get('empty_logic_type') == EMPTY_RETURN_OPTIONS['ASSIGN_PLACEHOLDER_LOCATION']
            and drayage_intelligence.get('empty_return_location')):
        placeholder_return_location = drayage_intelligence.get('empty_return_location')

    if (not settings.TRACKOS_API_URL or not settings.TRACKOS_API_KEY) and not placeholder_return_location:
        return loads
    
    filtered_loads = [load for load in loads if not load.get('emptyOriginName', '')]

    for load in filtered_loads:
        if load.get('containerSizeName'):
            load['containerSizeName'] = load.get('containerSizeName', '').strip().replace("'", '')
        if load.get('containerTypeName'):
            load['containerTypeName'] = load.get('containerTypeName', '').strip().replace("'", '')

    if len(filtered_loads) == 0:
        return loads

    # 1. Get All Stadard Ports and Container Owners
    current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    ## 1.1 Get All Standard Ports from json file
    ports_path = os.path.join(current_dir, 'app', 'data', 'standard_ports.json')
    with open(ports_path, 'r') as file:
        all_standard_ports = json.load(file)

    ## 1.2 Get All Standard SSL from json file
    ssl_path = os.path.join(current_dir, 'app', 'data', 'ssl.json')
    with open(ssl_path, 'r') as file:
        all_standard_ssl = json.load(file)

    # 2. Map shippers and container owners

    ## 2.1 Map Shippers to Market
    unique_shipper_names = list(set(load.get('shipperName', '') for load in filtered_loads if load.get('shipperName', '')))

    shippers_and_market = {}
    for shipper_name in unique_shipper_names:
        market = next((port for port in all_standard_ports 
                      if any(re.match(f"^{alias}$", shipper_name, re.I) 
                            for alias in port['aliases'])), None)
        if market:
            shippers_and_market[shipper_name] = market['market']

    ## 2.2 Map Container Owners to Standard SSL
    unique_container_owner_names = list(set(load.get('containerOwnerName', '') for load in filtered_loads if load.get('containerOwnerName', '')))

    container_owner_names_and_standard_ssl = {}
    for container_owner_name in unique_container_owner_names:
        standard_ssl = next((ssl for ssl in all_standard_ssl 
                           if any(re.match(f"^{value}$", container_owner_name, re.I) 
                                 for value in ssl['values'])), None)
        if standard_ssl:
            container_owner_names_and_standard_ssl[container_owner_name] = standard_ssl['label']

    # 3. Create Payloads
    unique_payloads = {}

    payloads = {}
    plan_date_format = datetime.strptime(plan_date, '%Y-%m-%d').strftime("%d/%m/%Y")
    for load in filtered_loads:
        market = shippers_and_market.get(load.get('shipperName', ''))
        standard_ssl = container_owner_names_and_standard_ssl.get(load.get('containerOwnerName', ''))
        payload_key = f"{market}_{standard_ssl}_{load.get('containerSizeName', '')}_{load.get('containerTypeName', '')}_{plan_date_format}"
        
        # Skip if any of the required fields are missing
        if (not market or not standard_ssl or not load.get('containerSizeName', '') or not load.get('containerTypeName', '')):
            continue

        ## 3.1 Create Unique Payloads
        unique_payloads[payload_key] = {
            "containerSize": load.get('containerSizeName', ''),
            "containerType": load.get('containerTypeName', ''), 
            "containerOwner": standard_ssl,
            "portName": market,
            "date": plan_date_format
        }
        
        ## 3.2 Record the payload key for each load
        payloads[load.get('reference_number', '')] = payload_key

    payload_values = list(unique_payloads.values())

    if len(payload_values) == 0:
        return loads

    recommended_returns = {}
    if settings.TRACKOS_API_URL and settings.TRACKOS_API_KEY and not placeholder_return_location:
        recommended_returns = await get_recommended_returns(carrier, payload_values)

    # Create mapping of port codes to their aliases
    recommended_returns_with_aliases = {}
    port_code_aliases = {}
    for payload_key, return_locations in recommended_returns.items():
        if return_locations and len(return_locations) > 0:
            # Get the first recommended return location
            primary_return = return_locations[0]
            
            # Find matching port in all_standard_ports
            matching_port = next((port for port in all_standard_ports 
                                if port['portCode'] == primary_return), None)
            
            if matching_port:
                recommended_returns_with_aliases[payload_key] = primary_return
                # Store unique port code aliases mapping
                if primary_return not in port_code_aliases:
                    port_code_aliases[primary_return] = matching_port['aliases']

    # Get customer records for port aliases
    db = await get_mongo_db()
    port_customer_mapping = {}
    
    # Create a combined regex pattern for all aliases
    all_aliases = []
    for aliases in port_code_aliases.values():
        all_aliases.extend(aliases)
    
    regex_pattern = "^(" + "|".join(all_aliases) + ")$"

    all_customers = await db.customers.find({
        "customerType": {"$in": ['shipper', 'ALL']},
        "carrier": ObjectId(carrier),
        "isDeleted": False,
        "company_name": {"$regex": regex_pattern, "$options": "i"}
    }).to_list(None)

    for port_code, aliases in port_code_aliases.items():
        matching_customer = next((customer for customer in all_customers if customer.get('company_name', '').lower() in (alias.lower() for alias in aliases)), None)
        if matching_customer:
            port_customer_mapping[port_code] = matching_customer
        
    # Create mapping of recommended returns to their customer records
    recommended_returns_customer_mapping = {}
    for payload_key, port_code in recommended_returns_with_aliases.items():
        if port_code in port_customer_mapping:
            recommended_returns_customer_mapping[payload_key] = port_customer_mapping[port_code]

    # 4. Add Recommended Returns to Loads
    mapped_loads = []
    for load in loads:
        # Create a copy of the load to modify
        mapped_load = load.copy()
        
        # Get the payload key for this load
        payload_key = payloads.get(mapped_load.get('reference_number', ''))

        return_location = None
        if not return_location and placeholder_return_location:
            return_location = placeholder_return_location
        
        if not return_location and payload_key and payload_key in recommended_returns_customer_mapping:
            return_location = recommended_returns_customer_mapping[payload_key]
        
        # Add recommended return empty origin if available
        if return_location and mapped_load.get('driverOrder'):
            # Set customerId for RETURNCONTAINER driverOrder
            for indx, order in enumerate(mapped_load['driverOrder']):
                if order.get('type') == 'RETURNCONTAINER' and not order.get('customerId'):
                    mapped_load['emptyOriginName'] = return_location.get('company_name', '')
                    
                    order['customerId'] = str(return_location.get('_id', ''))
                    order['address'] = return_location.get('address', '')
                    order['city'] = return_location.get('city', '')
                    order['state'] = return_location.get('state', '')
                    order['country'] = return_location.get('country', '')
                    order['zip_code'] = return_location.get('zip_code', '')
                    order['company_name'] = return_location.get('company_name', '').upper()

                    prev_order = mapped_load['driverOrder'][indx - 1] if indx > 0 else None
                    if prev_order and prev_order.get('address', {}).get('lat') and order.get('address', {}).get('lat'):
                        order['distance'] = calculate_distance_between_locations(
                            prev_order.get('address', {}),
                        return_location.get('address', {}),
                        distance_unit
                    )
        
        # Add the mapped load
        mapped_loads.append(mapped_load)

    return mapped_loads

