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
    source_location: Tuple[float, float], 
    dest_location: Tuple[float, float], 
    carrier: str,
    distance_unit: str = 'mi'
) -> Dict[str, Any]:
    """
    Get the road distance between two locations using the OSRM API.
    
    Args:
        source_location: Tuple of (latitude, longitude) for source
        dest_location: Tuple of (latitude, longitude) for destination  
        distance_unit: Unit of distance ('mi' for miles, 'km' for kilometers, 'm' for meters)
        
    Returns:
        Dict containing distance metrics and routing information
    """
    # Initialize response with default values
    response = {
        'distance': 0.0,
        'distance_miles': 0.0, 
        'distance_km': 0.0,
        'distance_meters': 0.0,
        'travel_time_minutes': 0.0,
        'avg_speed_kmh': 0.0,
        'source_coords': source_location,
        'dest_coords': dest_location,
        'success': False,
        'routing_method': None,
        'carrier': carrier
    }

    # Calculate air distance with multiplier
    air_distance = calculate_distance_between_locations(source_location, dest_location, distance_unit) * 1.5
    
    # Convert air distance to different units
    conversion = {
        'mi': {'meters': 1609.34, 'km': 1.60934},
        'km': {'meters': 1000, 'miles': 0.621371},
        'm': {'miles': 0.000621371, 'km': 0.001}
    }[distance_unit]
    
    air_distances = {
        'meters': air_distance * conversion.get('meters', 1000),
        'miles': air_distance * conversion.get('miles', 1) if distance_unit != 'mi' else air_distance,
        'km': air_distance * conversion.get('km', 1) if distance_unit != 'km' else air_distance
    }

    try:
        if not settings.DISTANCE_ROUTING_URL:
            raise Exception('Distance routing URL is not set')

        url = f"{settings.DISTANCE_ROUTING_URL}/route/v1/driving/{source_location[1]},{source_location[0]};{dest_location[1]},{dest_location[0]}?overview=full"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as api_response:
                    if api_response.status == 200:
                        data = await api_response.json()
                        first_route = data.get('routes', [{}])[0]
                        distance_meters = first_route.get('distance', 0)
                        duration = first_route.get('duration', 0)

                        # Convert distance based on unit
                        distance = distance_meters * {
                            'km': 0.001,
                            'm': 1,
                            'mi': 0.000621371
                        }[distance_unit]

                        # Calculate speed if duration exists
                        avg_speed_kmh = (distance_meters / (duration / 3600)) if duration > 0 else 0

                        response.update({
                            'distance': distance,
                            'distance_miles': distance_meters * 0.000621371,
                            'distance_km': distance_meters * 0.001,
                            'distance_meters': distance_meters,
                            'travel_time_minutes': duration / 60,
                            'avg_speed_kmh': avg_speed_kmh,
                            'routing_method': 'OSRM_API',
                            'success': True
                        })
                    else:
                        # Fallback to air distance
                        response.update({
                            'distance': air_distance,
                            'distance_miles': air_distances['miles'],
                            'distance_km': air_distances['km'], 
                            'distance_meters': air_distances['meters'],
                            'routing_method': 'OSRM_API_ERROR_AIR_FALLBACK',
                            'success': True
                        })
        except asyncio.TimeoutError:
            # Timeout occurred, fallback to air distance
            print("Timeout occurred, fallback to air distance", source_location, dest_location)
            response.update({
                'distance': air_distance,
                'distance_miles': air_distances['miles'],
                'distance_km': air_distances['km'], 
                'distance_meters': air_distances['meters'],
                'routing_method': 'OSRM_API_TIMEOUT_AIR_FALLBACK',
                'success': True
            })

    except Exception as e:
        # Fallback to air distance on error
        response.update({
            'distance': air_distance,
            'distance_miles': air_distances['miles'],
            'distance_km': air_distances['km'],
            'distance_meters': air_distances['meters'],
            'routing_method': 'ERROR_AIR_FALLBACK',
            'success': True
        })

    return response

async def get_location_distance_matrix_bulk(carrier: str, coordinates: List[Tuple[float, float]], distance_unit: str) -> List[Dict[str, Any]]:
    """
    For all pairs, get the road distance from OSRM API, and return a location_distance_matrix.
    """
    pairs = get_pairs_from_coordinates(coordinates)
    start_loads_time = time.time()
    new_results = []
    print(f'pairs length: {len(pairs)}')
    if pairs:
        chunk_size = 100
        for i in range(0, len(pairs), chunk_size):
            chunk = pairs[i:i+chunk_size]   
            start_chunk_time = time.time()
            chunk_results = await asyncio.gather(*(
                get_road_distance_between_locations(
                    start,
                    end,
                    carrier,
                    distance_unit
                ) for start, end in chunk
            ))
            # print(f'Time taken for chunk {i // chunk_size} of {len(pairs) // chunk_size} chunks: {time.time() - start_chunk_time} seconds')
            new_results.extend(chunk_results)
    print(f'new_results length: {len(new_results)}')
    end_loads_time = time.time()
    print(f'Time taken to get road distances from osm routing: {end_loads_time - start_loads_time} seconds')
    location_distance_matrix = []
    for result in new_results:
        location_distance_matrix.append({
            'source_coords': result['source_coords'],
            'dest_coords': result['dest_coords'],
            'distance': result.get('distance'),
            'distance_miles': result.get('distance_miles'),
            'distance_km': result.get('distance_km'),
            'distance_meters': result.get('distance_meters'),
            'travel_time_minutes': result.get('travel_time_minutes'),
            'avg_speed_kmh': result.get('avg_speed_kmh'),
            'reason': result.get('error_message'),
            'carrier': carrier
        })
    return location_distance_matrix

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