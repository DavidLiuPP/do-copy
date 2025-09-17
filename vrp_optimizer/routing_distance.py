from typing import Tuple, List, Dict, Any
import logging
from functools import lru_cache
from geopy.distance import geodesic
import asyncio
import time
import requests
from settings import settings
import aiohttp

logger = logging.getLogger(__name__)

@lru_cache(maxsize=1024)
def minute_from_distance(distance, unit = 'mi'):
    speed = (
        20 if distance <= 10 else
        40 if distance <= 50 else
        60
    )

    if unit == 'km':
        speed = speed * 1.60934

    return (distance / speed) * 60 + 10

@lru_cache(maxsize=1024)
def calculate_distance(lat1, lng1, lat2, lng2, unit = 'mi'):
    if unit == 'km':
        return geodesic((lat1, lng1), (lat2, lng2)).kilometers
    if unit == 'm':
        return geodesic((lat1, lng1), (lat2, lng2)).meters
    return geodesic((lat1, lng1), (lat2, lng2)).miles


def calculate_distance_between_locations(loc1: Tuple[float, float], loc2: Tuple[float, float], unit = 'mi'):
    if loc1[0] and loc2[0] and loc1[1] and loc2[1]:
        miles = calculate_distance(loc1[0], loc1[1], loc2[0], loc2[1], unit)
        return miles
    return 9999


async def get_road_distance_between_locations(
    matrix: List[Tuple[float, float]], 
    carrier: str,
    distance_unit: str = 'mi'
) -> List[Dict[str, Any]]:
    """
    Get the road distance between locations using OSRM API.
    """
    try:
        if not settings.DISTANCE_ROUTING_URL:
            raise Exception('Distance routing URL is not set')

        url = f"{settings.DISTANCE_ROUTING_URL}/table/v1/truck"
        
        # Convert tuples to arrays and swap lat/lon to lon/lat as required by OSRM
        coordinates = [[lon, lat] for lat, lon in matrix]
        
        data = {
            "coordinates": coordinates,
            "annotations": [
                "distance",
                "duration"
            ]
        }

        for attempt in range(2):  # Try up to 2 times
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=data, timeout=5) as api_response:
                        if api_response.status == 200:
                            response_data = await api_response.json()
                            if response_data.get('code') != 'Ok':
                                raise Exception('Invalid response from OSRM API')
                            
                            distances = response_data.get('distances', [])
                            durations = response_data.get('durations', [])
                            
                            mapped_data = []
                            for i, source in enumerate(matrix):
                                for j, dest in enumerate(matrix):
                                    distance = distances[i][j]
                                    duration = durations[i][j]
                                    # Convert distance based on unit
                                    distance_in_unit = distance * {
                                        'km': 0.001,
                                        'm': 1,
                                        'mi': 0.000621371
                                    }[distance_unit]
                                    if duration > 0:
                                        avg_speed_kmh = distance / (duration / 3600)
                                    else:
                                        avg_speed_kmh = 0  # Avoid division by zero

                                    mapped_data.append({
                                        'source_coords': source,
                                        'dest_coords': dest,
                                        'distance': distance_in_unit,
                                        'duration': duration,
                                        'distance_miles': distance * 0.000621371,
                                        'distance_km': distance * 0.001,
                                        'distance_meters': distance,
                                        'travel_time_minutes': duration / 60,
                                        'avg_speed_kmh': avg_speed_kmh,
                                        'routing_method': 'OSRM_API',
                                        'carrier': carrier
                                    })
                            return mapped_data
                        else:
                            logger.error(f'Failed to get road distance between locations: {api_response.status}')
                            if attempt == 0:  # If this is the first attempt
                                await asyncio.sleep(2)  # Wait 2 seconds before retrying
                                continue  # Try again
                            break  # If this is the second attempt, break and use air distance fallback
            
            except Exception as e:
                logger.error(f'Error in get_road_distance_between_locations_bulk: {str(e)}')
                if attempt == 0:  # If this is the first attempt
                    await asyncio.sleep(2)  # Wait 2 seconds before retrying
                    continue  # Try again
                break  # If this is the second attempt, break and use air distance fallback

    except Exception as e:
        logger.error(f'Error in get_road_distance_between_locations: {str(e)}')
    
    finally:
        # Fallback to air distance calculation if both attempts failed
        # Convert air distance to different units
        conversion = {
            'mi': {'meters': 1609.34, 'km': 1.60934},
            'km': {'meters': 1000, 'miles': 0.621371},
            'm': {'miles': 0.000621371, 'km': 0.001}
        }[distance_unit]

        mapped_data = []
        for i, source in enumerate(matrix):
            for j, dest in enumerate(matrix):
                # Calculate air distance with multiplier
                air_distance = calculate_distance_between_locations(source, dest, distance_unit) * 1.5

                air_distances = {
                    'meters': air_distance * conversion.get('meters', 1000),
                    'miles': air_distance * conversion.get('miles', 1) if distance_unit != 'mi' else air_distance,
                    'km': air_distance * conversion.get('km', 1) if distance_unit != 'km' else air_distance
                }

                mapped_data.append({
                    'source_coords': source,
                    'dest_coords': dest,
                    'distance': air_distance,
                    'distance_miles': air_distances['miles'],
                    'distance_km': air_distances['km'],
                    'distance_meters': air_distances['meters'],
                    'travel_time_minutes': minute_from_distance(air_distance, distance_unit),
                    'avg_speed_kmh': air_distance / (minute_from_distance(air_distance, distance_unit) / 3600),
                    'routing_method': 'ERROR_AIR_FALLBACK',
                    'carrier': carrier
                })
        return mapped_data

async def get_location_distance_matrix_bulk(carrier: str, coordinates: List[Tuple[float, float]], distance_unit: str) -> List[Dict[str, Any]]:
    """
    For all pairs, get the road distance from OSRM API, and return a location_distance_matrix.
    """
    try:
        start_loads_time = time.time()
        new_results = await get_road_distance_between_locations(coordinates, carrier, distance_unit)
        end_loads_time = time.time()
        print(f'Time taken to get road distances from osm routing: {end_loads_time - start_loads_time} seconds')
        return new_results
    except Exception as e:
        logger.error(f'Error in get_location_distance_matrix_bulk: {str(e)}')
        return []

def get_pairs_from_locations(locations: List[Dict[str, Any]]) -> List[Tuple[Tuple[float, float], Tuple[float, float]]]:
    pairs = set()
    for location in locations:
        if location.get('start_loc') and location.get('end_loc'):
            start_loc = tuple(location['start_loc'])
            end_loc = tuple(location['end_loc'])
            print(f'start_loc: {start_loc}, end_loc: {end_loc}')
            pair = tuple(sorted([start_loc, end_loc]))
            pairs.add(pair)
    return list(pairs) if pairs else []

def get_unique_coordinates(nodes):
        """
        Extracts all unique latitude-longitude coordinates from nodes that have both source and destination.
        Returns a list of tuples containing (latitude, longitude) pairs.
        """
        unique_coords = set()

        for node in nodes:
            # Only process nodes that have both start and end locations
            if node.get('start_loc'):
                unique_coords.add(tuple(node['start_loc']))
            if node.get('end_loc'):
                unique_coords.add(tuple(node['end_loc']))

        return list(unique_coords)

def get_pairs_from_coordinates(coordinates: List[Tuple[float, float]]) -> List[Tuple[Tuple[float, float], Tuple[float, float]]]:
    pairs = []
    for i in range(len(coordinates)):
        for j in range(len(coordinates)):
            pairs.append((coordinates[i], coordinates[j]))
    return pairs if pairs else []

def get_distance_between_locations_from_matrix(
    source_location: Tuple[float, float], 
    dest_location: Tuple[float, float], 
    location_distance_matrix: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Description:
        This function returns the distance between two locations from the location_distance_matrix.
        If the distance is not found, it returns 9999.
    """
    if tuple(source_location) == tuple(dest_location):
        return 0

    matching_entry = next(
        (entry for entry in location_distance_matrix 
        if entry['source_coords'] == tuple(source_location) and entry['dest_coords'] == tuple(dest_location)),
        None
    )

    if matching_entry:
        return matching_entry.get('distance')
    else:
        return calculate_distance_between_locations(source_location, dest_location)

def distance_between_locations_list_from_matrix(locations: List[Tuple[float, float]], location_distance_matrix: List[Dict[str, Any]]):
    distance = 0
    for i in range(len(locations) - 1):
        distance += get_distance_between_locations_from_matrix(locations[i], locations[i + 1], location_distance_matrix)
    return distance