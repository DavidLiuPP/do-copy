from typing import Set, List, Dict
from datetime import timedelta

REQUIRED_LOAD_FIELDS: Set[str] = {
    'carrier',
    'status', 
    'reference_number'
}

OPTIONAL_LOAD_FIELDS: List[str] = [
    'returnFromTime',
    'returnToTime',
    'availableDate',
    'callerName',
    'shipperName',
    'consigneeName', 
    'hazmat',
    'hot',
    'liquor',
    'lastFreeDay',
    'containerAvailableDay',
    'emptyDay',
    'dischargedDate',
    'cutOff',
    'containerAvailableDay',
    'freeReturnDate',
    'containerSizeName',
    'totalWeight',
    'revenue',
    'chassisPickName',
    'chassisTerminationName'
]

ACTIONABLE_PROFILE_TYPES: Dict[str, List[str]] = {
    "Pre-Pull": ["PULLCONTAINER"],
    "Pull-Deliver": ["PULLCONTAINER", "DELIVERLOAD"],
    "Pull-Deliver-Return": ["PULLCONTAINER", "DELIVERLOAD", "RETURNCONTAINER"],
    "Deliver": ["DELIVERLOAD"],
    "Deliver-Return": ["DELIVERLOAD", "RETURNCONTAINER"],
    "Return": ["RETURNCONTAINER"]
}

APPOINTMENT_STATUS = {
    "NEED_APPOINTMENT": 'need_appointment',
    "SCHEDULED": 'scheduled',
    "REQUESTED": 'requested',
}

