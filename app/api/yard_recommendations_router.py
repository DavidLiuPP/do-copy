from fastapi import APIRouter, HTTPException, BackgroundTasks, Request
from app.modules.yard_recommendations.store_yard_recommendations import process_yard_recommendations

# This assumes your analysis script is in a directory called 'scripts'
# Adjust the path if your project structure is different

router = APIRouter()

@router.post("/store-yard-recommendations", status_code=202) # 202 Accepted is standard for background tasks
async def store_yard_recommendations(
    request: Request, 
    background_tasks: BackgroundTasks
):
    """
    Accepts a carrier_id and starts the recommendation analysis as a background task.
    """
    try:
        # Add the long-running function to the background tasks
        # The API will return a response immediately without waiting for this to finish
        content = await request.json()

        result = await process_yard_recommendations(carrier_id=content.get('carrier'), save_to_db=content.get('save_to_db', True), min_frequency=content.get('min_frequency', 1))
        # Return an immediate response to the client
        return {
            "status": "success",
            "message": "Yard recommendations completed.",
            "result": result
        }
    except Exception as e:
        # This will catch errors during the initial setup, not the background task itself
        raise HTTPException(status_code=500, detail=f"Failed to start analysis: {str(e)}")