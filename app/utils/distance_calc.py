"""Distance calculation utilities for geographic coordinates and travel time estimation.

This module provides functions for calculating distances between geographic coordinates
and estimating travel durations based on distances. All functions are optimized for
performance with caching where appropriate.
"""

from datetime import timedelta
from functools import lru_cache
from typing import Dict, Union
from geopy.distance import geodesic
from app.modules.optimizer.constants import DISTANCE_MULTIPLIER

# Constants
DEFAULT_DISTANCE = 9999.0  # Miles returned when coordinates are invalid
SPEED_THRESHOLDS = {
    10: 20,  # Speed (mph) for distances <= 10 miles
    50: 40,  # Speed (mph) for distances <= 50 miles
    float('inf'): 60  # Speed (mph) for distances > 50 miles
}

@lru_cache(maxsize=1024)
def calculate_distance(lat1: float, lng1: float, lat2: float, lng2: float, unit = 'mi') -> float:
    """Calculate the distance in miles between two geographic coordinates.

    Args:
        lat1: Latitude of the first location
        lng1: Longitude of the first location
        lat2: Latitude of the second location
        lng2: Longitude of the second location
        unit: Unit of distance to return (mi or km)

    Returns:
        float: Distance in miles between the coordinates

    Raises:
        ValueError: If any coordinate is invalid
    """
    try:
        if unit == 'km':
            return geodesic((lat1, lng1), (lat2, lng2)).kilometers * DISTANCE_MULTIPLIER
        return geodesic((lat1, lng1), (lat2, lng2)).miles * DISTANCE_MULTIPLIER

    except ValueError as e:
        raise ValueError(f"Invalid coordinates provided: {e}") from e

def calculate_distance_between_locations(
    location1: Dict[str, float],
    location2: Dict[str, float],
    unit = 'mi'
) -> float:
    """Calculate distance between two locations represented as dictionaries.

    Args:
        location1: Dictionary containing 'lat' and 'lng' keys for first location
        location2: Dictionary containing 'lat' and 'lng' keys for second location
        unit: Unit of distance to return (mi or km)

    Returns:
        float: Distance in miles between locations, or DEFAULT_DISTANCE if coordinates are invalid
    """
    try:
        if all(location1.get(key) and location2.get(key) for key in ('lat', 'lng')):
            return calculate_distance(
                location1['lat'],
                location1['lng'],
                location2['lat'],
                location2['lng'],
                unit
            )
        return DEFAULT_DISTANCE
    except (TypeError, KeyError):
        return DEFAULT_DISTANCE

@lru_cache(maxsize=1024)
def calculate_duration_from_distance(distance: Union[int, float], unit: str = 'mi') -> timedelta:
    """Calculate travel duration based on distance using variable speed thresholds.

    Args:
        distance: Distance in miles

    Returns:
        timedelta: Estimated travel duration

    Raises:
        ValueError: If distance is negative
    """
    if distance < 0:
        raise ValueError("Distance cannot be negative")

    for threshold, speed in sorted(SPEED_THRESHOLDS.items()):
        if distance <= threshold:
            if unit == 'km':
                speed = speed * 1.60934
            return timedelta(hours=float(distance) / speed)