"""
This service will make an api requet to ELD service to get HOS data for the driver lists. After getting the data, the data will have cycle, shift, break and driver. Using this data we need to find out how many hours the driver can drive today.

Terminology definitions:
Cycle: How long the driver can work in the week
Shift: How long a shift can last for a driver
Drive: How long has the driver driven
Break: How many total hours the driver needs to take a break

# sample curl request to get HOS data
curl --location 'https://<url>/v1/bulk/hos-availability' \
--header 'xapikey: <api key goes here>' \
--header 'x-api-key: <api key goes here>' \
--header 'Content-Type: application/json' \
--data '{
    "driverIds":["<driver eld_identifier goes here>"],
    "carrier":"<carrier id goes here>"
}'


which returns the data in the following format:
{
    "data": [
        {
            "id": "a05ce515-32ab-4ea2-8056-9f6ce35eeed8",
            "driverId": "4248865",
            "fleetId": "17493",
            "carrier": "5b7138ab0c29d2783bfb80c4",
            "tspSource": "samsara",
            "breakSeconds": 28800,
            "cycleSeconds": 222239,
            "shiftSeconds": 0,
            "driveSeconds": 0,
            "createdAt": "2024-12-11T07:47:18.147Z",
            "updatedAt": "2025-01-21T05:01:18.542Z"
        }
    ],
    "message": "HOS DATA"
}


for each driver, we need to find out how many hours they can drive per day and then add that to the driver object in the driver list.
"""
import logging
import requests
from typing import List, Dict
from settings import settings

logger = logging.getLogger(__name__)

async def get_hos_data_for_drivers(driver_list: List[Dict], carrier_id: str) -> List[Dict]:
    """
    Fetches HOS data for a list of drivers and calculates their available drive hours for the day.

    :param driver_list: List of driver dictionaries containing their identifiers.
    :param carrier_id: The carrier ID to be used in the API request.
    :return: Updated list of drivers with available drive hours added.
    """
    api_url = f'{settings.ELD_API_URL}/v1/bulk/hos-availability'

    # Extract driver IDs from the driver list
    driver_ids = [driver['eld_identifier'] for driver in driver_list if driver.get('eld_identifier')]

    if not settings.ELD_API_KEY or not settings.ELD_API_URL or len(driver_ids) == 0:
        if driver_list:
            for driver in driver_list:
                driver['available_drive_hours'] = 10.0
                driver['total_drive_hours'] = 10.0
                driver['total_shift_hours'] = 13.0
        return driver_list
    
    headers = {
        "xapikey": settings.ELD_API_KEY,
        "x-api-key": settings.ELD_API_KEY,
        "Content-Type": "application/json",
    }

    # Prepare the payload for the API request
    payload = {
        "driverIds": driver_ids,
        "carrier": carrier_id,
    }

    try:
        # Make the API request
        response = requests.post(api_url, headers=headers, json=payload)
        response.raise_for_status()
        hos_data = response.json().get("data", [])

        # Process the response and update driver list
        for driver in driver_list:
            driver_id = driver['eld_identifier']
            driver_hos_data = next((item for item in hos_data if item['driverId'] == driver_id), None)

            if driver_hos_data:
                # Calculate available drive hours for the day
                # Convert seconds to hours for each component
                drive_hours = driver_hos_data['driveSeconds'] / 3600  # Hours already driven
                # Update the driver object with available drive hours
                driver['available_drive_hours'] = float(drive_hours)
                driver['total_shift_hours'] = 13.0
                driver['total_drive_hours'] = 10.0
            
            # if not present return 11 hours
            else:
                driver['available_drive_hours'] = 10.0
                driver['total_shift_hours'] = 13.0
                driver['total_drive_hours'] = 10.0

    except requests.RequestException as e:
        logger.error(f"Failed to fetch HOS data: {e}")

    return driver_list