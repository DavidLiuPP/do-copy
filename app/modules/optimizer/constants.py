from typing import List

VALID_EVENT_TYPES: List[str] = [
    "PULLCONTAINER",
    "DELIVERLOAD",
    "RETURNCONTAINER"
]

DROP_EVENT_TYPES: List[str] = [
    "DROPCONTAINER",
    "LIFTOFF"
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
        'ROTATION_ENABLED': True,
        'vrp_assumptions': {
            'PENALTY_FOR_COMPANY_DRIVER_LONG_DISTANCE_MOVE': 1000
        }
    },
    # Quality Container
    '653a6813f7eb901615236816': {
        'ROTATION_ENABLED': True,
        'IMPORT_DELIVERLOAD_GRACE_TIME': 45,
        'use_nearest_to_delivery_yard': True,
        'allow_to_plan_following_moves': True,
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
        'allow_to_plan_following_moves': True,
        
        'free_flow_empty_return_couple_moves_locations': [
            '641a10885b159a160742328a', # TRAPAC
            '641a10885b159a160742328c' # YTI
        ],
        'plan_times': [210, 1650]
    },
    # DILE-Trucking
    '623a1a0ae85bec6eacd5096d': {
        'ROTATION_ENABLED': True,
        'allow_to_plan_following_moves': True
    },
    
    # Alpha Cargo - USE PRE-PULL DRIVER FOR DELIVER MOVE
    '6478bad770a34316adb76c24': {
        'use_prepull_driver_for_deliver_move': True,
        'allow_to_plan_following_moves': True
    },

    # Seaport Intermodal
    '6500ac3f5b4e7715cea4a2fe': {
        'allow_to_plan_following_moves': True,
        
        'free_flow_empty_return_couple_moves_locations': [
            '6500ac5a5b4e7715cea4a446', # CN BRAMPTON
            '6500ac585b4e7715cea4a43a', # CN Malport
            '654e5d2cac10fb15a38d303a', # CN Misc Yard
            '650339fbe6761016124a61b9', # CN MONTREAL
            '650fe544807f7b15992b97d4', # CN - Vancouver
            '66384b4e2828c1aae4be3607', # CN VANCOUVER
            '6500ac575b4e7715cea4a425', # CN CALGARY
            '650fd61b6cc5eb15f2d90f3f' # CN EDMONTON      
        ],
        
        'ROTATION_ENABLED': True,
        'allow_to_plan_following_moves': True
    }
}


PLANNING_ASSUMPTIONS = {
    'LATE_ARRIVAL_MOVE': 'LATE_ARRIVAL_MOVE'
}

YARD_ALLOWED_OPERATIONS = {
    'CHASSIS_DROP_HOOK': 'CHASSIS_DROP_HOOK',
    'CONTAINER_DROP_HOOK': 'CONTAINER_DROP_HOOK'
}