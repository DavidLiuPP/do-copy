from fastapi import APIRouter
import platform
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/")
async def healthcheck():
    logger.info(f"Healthcheck request: {platform.python_version()}")
    return {"status": "healthy"}
