from app.mongo_services.mongo_service import get_mongo_db
from bson.objectid import ObjectId
import logging
import tempfile
import os
import json
import xgboost as xgb
import os
from typing import Union, Any, Dict

logger = logging.getLogger(__name__)

async def get_time_zone(carrier_id: str) -> str:
    try:
        db = await get_mongo_db()
        if db is None:
            raise ConnectionError("Failed to get database connection")

        user_collection = db.users
        carrier_collection = db.carriers

        # mongodb populate carrier when we are finding user
        
        user = await user_collection.find_one({"_id": ObjectId(carrier_id)}, {"carrier": 1})
        if not user or not user.get("carrier"):
            raise ValueError(f"Carrier not found for user with ID: {carrier_id}")

        carrier = await carrier_collection.find_one({"_id": ObjectId(user["carrier"])}, {"homeTerminalTimezone": 1})
        if not carrier or not carrier.get("homeTerminalTimezone"):
            return "America/New_York"

        return carrier["homeTerminalTimezone"]

    except ConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    except ValueError as e:
        logger.error(f"Value error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error retrieving time zone: {str(e)}")
        raise Exception(f"Failed to retrieve time zone: {str(e)}")


async def get_carrier_preferences(carrier_id: str) -> Dict[str, str]:
    """
    Retrieve carrier preferences from the carrier collection for a specific carrier.
    
    Args:
        carrier_id: The user ID associated with the carrier
        
    Returns:
        Dict containing the distanceMeasure and weightMeasure preferences
        
    Raises:
        ConnectionError: If database connection fails
        ValueError: If carrier not found for the given ID
        Exception: For other errors
    """
    try:
        db = await get_mongo_db()
        if db is None:
            raise ConnectionError("Failed to get database connection")

        user_collection = db.users
        carrier_collection = db.carriers

        # Find the user to get the carrier reference
        user = await user_collection.find_one({"_id": ObjectId(carrier_id)}, {"carrier": 1})
        if not user or not user.get("carrier"):
            raise ValueError(f"Carrier not found for user with ID: {carrier_id}")

        # Get carrier preferences
        carrier = await carrier_collection.find_one(
            {"_id": ObjectId(user["carrier"])}, 
            {"distanceMeasure": 1, "weightMeasure": 1}
        )

        # Set default values if preferences not found
        result = {
            "distanceUnit": carrier.get("distanceMeasure", "mi"),
            "weightUnit": carrier.get("weightMeasure", "lbs")
        }

        if result.get("distanceUnit") == "ml":
            result["distanceUnit"] = "mi"

        return result

    except ConnectionError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    except ValueError as e:
        logger.error(f"Value error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error retrieving carrier preferences: {str(e)}")
        raise RuntimeError(f"Failed to retrieve carrier preferences: {str(e)}")


async def load_model_content(model_content: Union[Dict, Any], loaded_model: xgb.Booster = None) -> xgb.Booster:
    """
    Takes model content (as a dictionary or byte content), writes it to a temporary file,
    and loads it into an XGBoost Booster model.
    Args:
        model_content: The content of the model, either as a dictionary or bytes.
        loaded_model: xgb.Booster: Loaded XGBoost model.
    Returns:
        xgb.Booster: Loaded XGBoost model.
    """
    try:
        # If the model content is a dictionary (for JSON content), serialize it to bytes
        if isinstance(model_content, dict):
            model_content = json.dumps(model_content).encode('utf-8')
        
        # Create temporary file without delete=False to prevent Windows permission issues
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.model')
        try:
            # Write the model content to the temporary file
            temp_file.write(model_content)
            temp_file.flush()
            temp_file.close()  # Close the file before loading the model
            
            # Load the model from the closed file
            loaded_model.load_model(temp_file.name)
            
            return loaded_model
            
        finally:
            # Clean up the temporary file manually
            try:
                os.unlink(temp_file.name)
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {temp_file.name}: {e}")

    except Exception as e:
        raise ValueError(f"Error while loading model: {str(e)}")