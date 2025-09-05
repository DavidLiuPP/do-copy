from fastapi import APIRouter, HTTPException
import sys
import os

# Add the project root to the Python path to import the test module
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from tests.test_optimizer import run_all_tests

router = APIRouter()

@router.post("/run-tests")
async def run_all_optimizer_tests():
    """
    Run all available optimizer tests
    """
    try:
        test_results = run_all_tests()
        return test_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Test execution failed: {str(e)}")
