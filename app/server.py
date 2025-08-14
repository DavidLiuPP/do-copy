"""
FastAPI application factory and configuration module.

This module sets up the FastAPI application with middleware, routers, and database connections.
It follows a factory pattern for application creation and implements proper lifecycle management.
"""
import logging
import asyncio
from typing import List, AsyncGenerator
from fastapi import FastAPI
from fastapi.middleware import Middleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from app.middlewares.authorization import AuthorizationMiddleware
from app.middlewares.newrelic import NewRelicMiddleware
from contextlib import asynccontextmanager

from app.mongo_connection import get_mongo_client
from app.services.newrelic import initialize_newrelic, setup_logging_with_newrelic
from app.postgres_connection import get_postgres_client
from app.synced_db_connection import get_synced_db_client
from settings import settings
from app.queue_service import EventService

from app.api.main_router import router as main_router
from app.api.health_router import router as health_router
from app.api.redis_router import router as redis_router
from app.api.report_router import router as report_router
from app.api.upload_router import router as upload_router
from app.api.agent_router import router as agent_router

# Configure logging with proper format and level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

initialize_newrelic()

# # Set up logging with New Relic
setup_logging_with_newrelic()

def make_middleware() -> List[Middleware]:
    """
    Create and configure application middleware stack.
    
    Returns:
        List[Middleware]: List of configured middleware instances
    """
    middleware = [
        # Restrict CORS to known origins in production
        Middleware(
            CORSMiddleware,
            allow_origins=["*"],  # TODO: Replace with specific origins in production
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type"],
            expose_headers=["X-Total-Count"],
        ),
        # Protect against host header attacks
        Middleware(
            TrustedHostMiddleware,
            allowed_hosts=["*"]  # TODO: Replace with specific hosts in production
        ),
        Middleware(NewRelicMiddleware),
        Middleware(AuthorizationMiddleware)  # Add the authorization middleware
    ]
    return middleware

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    try:
        mongo_client = get_mongo_client()
        await mongo_client.initialize()
        app.state.mongo = mongo_client

        if settings.POSTGRES_URL:
            postgres_client = get_postgres_client()
            await postgres_client.initialize()
            app.state.postgres = postgres_client
        
        if settings.SYNCED_DB_URL:
            synced_db_client = get_synced_db_client()
            await synced_db_client.initialize()
            app.state.synced_db = synced_db_client

        if settings.QUEUE_URL and settings.ENABLE_QUEUE:
            loop = asyncio.get_running_loop()
            event_service = EventService(loop, settings.QUEUE_URL, settings.EXCHANGE_NAME)
            app.state.event_service = event_service

            if settings.PRE_QUEUE_URL:
                pre_event_service = EventService(loop, settings.PRE_QUEUE_URL, settings.PRE_EXCHANGE_NAME)
                app.state.pre_event_service = pre_event_service
    
        logger.info("Application startup completed successfully")
    except Exception as e:
        logger.critical(f"Failed to initialize application: {str(e)}")
        raise
    
    yield

    try:
        if hasattr(app.state, "mongo"):
            await app.state.mongo.close()
        
        if settings.QUEUE_URL:
            if hasattr(app.state, "event_service"):
                await app.state.event_service.close()

        if settings.PRE_QUEUE_URL:
            if hasattr(app.state, "pre_event_service"):
                await app.state.pre_event_service.close()

        logger.info("Application shutdown completed successfully")
    except Exception as e:
        logger.error(f"Error during application shutdown: {str(e)}")
        raise

def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application instance.
    
    Returns:
        FastAPI: Configured application instance
    
    Note:
        This factory function enables easier testing and deployment configuration
    """
    app = FastAPI(
        title="Hide API",
        description="Secure and scalable API for the Hide platform",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        middleware=make_middleware(),
        lifespan=lifespan
    )

    # Register routers with proper prefixes
    app.include_router(health_router, tags=["Health"])
    app.include_router(main_router, prefix="/v1", tags=["Main"])
    app.include_router(report_router, tags=["Report"])
    app.include_router(upload_router, tags=["Upload"])
    app.include_router(redis_router, tags=["Redis"])
    app.include_router(agent_router, tags=["Agent"])
    
    return app

# Create application instance
app = create_app()
