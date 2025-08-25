from typing import List

VALID_EVENT_TYPES: List[str] = [
    "PULLCONTAINER",
    "DELIVERLOAD",
    "RETURNCONTAINER"
]

EVENT_TIME_MAP = {
    'PULLCONTAINER': 'pickupFromTime',
    'DELIVERLOAD': 'deliveryFromTime', 
    'RETURNCONTAINER': 'returnFromTime'
}

DROPPED_LOADED = 'Dropped - Loaded'
DROPPED_EMPTY = 'Dropped - Empty'

PREFERRED_DISTANCE_FOR_COMPANY_DRIVERS = 100

DISTANCE_MULTIPLIER = 1.5

ONE_DAY_IN_MINUTES = 24 * 60

LATE_ARRIVAL_MINUTES = 120

# Chassis yard configurations for different carriers
CHASSIS_YARD_CONFIGS = {
    '641a10875b159a160742327e': {
        'amazon_loads': {
            'yard_customer_id': '66d8e4486f8e3a829bd63aa5'  # AMZN GYW9-ODY (3400 PIER S LB)
        },
        'default_yard': {
            'yard_customer_id': '641a138d4afd3216030be8ec'  # ROADEX Q STREET YARD
        },
        'load_type_detector': {
            'field': 'consigneeName', 
            'prefixes': {
                'amazon_loads': 'AMZN'  # Prefix to identify Amazon loads 
            }
        }
    }
}

CARRIER_CONFIGS = {
    # Tripoint Intermodal Services
    '63039f613d347315e2a02a2d': {
        'rotation_order': ['owner_score', 'worked_yesterday', 'distance'],
        'allow_mismatched_appointment_times': True,
        'vrp_assumptions': {
            'PENALTY_FOR_COMPANY_DRIVER_LONG_DISTANCE_MOVE': 1000
        }
    },
    # Quality Container
    '653a6813f7eb901615236816': {
        'rotation_order': ['owner_score', 'worked_yesterday','distance', 'name'],
        'use_nearest_to_delivery_yard': True,
        'allow_mismatched_appointment_times': True,
        'vrp_assumptions': {
            'VEHICLE_USE_PENALTY': 0,
            'MAXIMIZE_WORKING_MINUTES': False,
            'MAX_WAITING_TIME_BETWEEN_NODES': 30  # in minutes
        },
        'last_visit_locations': [
            '669e99cbace8bcfd64a5b656', # SLG
            '661800ed8b11526e42dcf208', # LINEAGE LOGISTICS #2
            '66196336e092a76d62cf1a14', # CITY FURNITURE
            '67d1ed387417a281cccb522d', # SENTURY TIRE USA
        ]
    },
    # RoadEx America
    '641a10875b159a160742327e': {
        'use_nearest_to_delivery_yard': True,
        'wait_for_empty_appt': True,
        'empty_group_location': { "lat": 33.7610191, "lng": -118.2384082 }, # AMZN GYW9-ODY (3400 PIER S LB) | 3400 New Dock St, Long Beach, CA 90802
        'allow_to_plan_following_moves': True
    },
    # DILE-Trucking
    '623a1a0ae85bec6eacd5096d': {
        'rotation_order': ['owner_score', 'worked_yesterday', 'distance'],
        'allow_to_plan_following_moves': True
    },
    
    # Alpha Cargo - USE PRE-PULL DRIVER FOR DELIVER MOVE
    '6478bad770a34316adb76c24': {
        'use_prepull_driver_for_deliver_move': True,
        'allow_to_plan_following_moves': True
    }
}


PLANNING_ASSUMPTIONS = {
    'LATE_ARRIVAL_MOVE': 'LATE_ARRIVAL_MOVE'
}
