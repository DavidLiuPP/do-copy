import os
import json
from typing import Any, Dict, List, Union
from bson import json_util

def flatten_bson(obj: Union[Dict, List, Any]) -> Union[Dict, List, Any]:
    """Convert BSON types to simple Python types recursively.
    
    This function traverses through nested dictionaries and lists containing BSON types
    and converts them to their Python equivalents. Handles ObjectId ($oid) and 
    DateTime ($date) BSON types.
    
    Args:
        obj: Input object that may contain BSON types. Can be:
            - A dictionary with BSON values
            - A list containing dictionaries with BSON values 
            - Any other type that will be returned as-is
            
    Returns:
        Union[Dict, List, Any]: The input object with all BSON types converted to Python types
        
    Examples:
        >>> flatten_bson({'_id': {'$oid': '507f1f77bcf86cd799439011'}})
        {'_id': '507f1f77bcf86cd799439011'}
        
        >>> flatten_bson({'date': {'$date': 1582671600000}})
        {'date': 1582671600000}
    """
    try:

        serialized_obj = json.loads(json_util.dumps(obj))

        if isinstance(serialized_obj, dict):
            result = {}
            for key, value in serialized_obj.items():
                if isinstance(value, dict):
                    if len(value) == 1:
                        if '$oid' in value:
                            result[key] = str(value['$oid'])
                            continue
                        elif '$date' in value:
                            result[key] = value['$date']
                            continue
                    result[key] = flatten_bson(value)
                elif isinstance(value, list):
                    result[key] = [flatten_bson(item) for item in value]
                else:
                    result[key] = value
            return result
        elif isinstance(serialized_obj, list):
            return [flatten_bson(item) for item in serialized_obj]
        return serialized_obj
        
    except Exception as e:
        raise ValueError(f"Error flattening BSON object: {str(e)}") from e


def mkdir(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except NotADirectoryError:
            pass


def extract_user_id(carrier_data):
    if isinstance(carrier_data, dict):
        return carrier_data.get('_id')
    elif isinstance(carrier_data, str):
        return carrier_data
    return None
