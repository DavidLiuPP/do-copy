from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from dotenv import load_dotenv
import jwt
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.mongo_connection import get_mongo_db
from settings import settings
load_dotenv()

VALID_X_API_KEY_PATHS = [
    "/upload_file",
    "/set_carrier_settings",
    "/set_default_yard_location",
    "/v1/get_driver_plan_stats",
    "/upload_waiting_time",
    "/upload_driver_features",
    "/v1/update_db"
]

class AuthorizationMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self.jwt_secret = settings.JWT_SECRET
        self.x_api_key = settings.X_API_KEY

    def verify_jwt(self, token: str) -> Optional[dict]:
        try:
            payload = jwt.decode(token, self.jwt_secret, algorithms=["HS256"])
            return payload
        except jwt.InvalidTokenError:
            return None

    async def verify_user_in_mongodb(self, db: AsyncIOMotorDatabase, user_id: str) -> bool:
        try:
            # Convert string ID to ObjectId if needed
            from bson.objectid import ObjectId
            user = await db.users.find_one({ "_id": ObjectId(user_id), "isDeleted": False }, { "_id": 1 })
            return user is not None
        
        except Exception as e:
            print(f"MongoDB verification error: {str(e)}")
            return False

    async def dispatch(self, request: Request, call_next):
        # Disable authentication for the root path and health check
        if request.url.path == "/" or request.url.path == "/health":
            response = await call_next(request)
            return response
        
        # Check for API key first
        if request.url.path in VALID_X_API_KEY_PATHS and request.headers.get('x-api-key') == self.x_api_key:
            response = await call_next(request)
            return response
        
        auth_header = request.headers.get('Authorization')
        
        if not auth_header:
            return JSONResponse(
                content={"message": "Missing authorization header"},
                status_code=401
            )

        # Check for API key first
        if auth_header == self.x_api_key:
            response = await call_next(request)
            return response

        # Check JWT token
        if auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
            payload = self.verify_jwt(token)
            
            if payload:
                # Get MongoDB connection
                db = await get_mongo_db()
                
                # Verify user exists in MongoDB
                user_id = payload.get('userId')
                if not user_id:
                    return JSONResponse(
                        content={"message": "User ID not found in token"},
                        status_code=401
                    )

                user_exists = await self.verify_user_in_mongodb(db, user_id)
                if not user_exists:
                    return JSONResponse(
                        content={"message": "User not found in database"},
                        status_code=401
                    )

                # Add the payload to request state for use in routes
                request.state.user = {
                    "carrier": payload.get("carrier"),
                    "userId": payload.get("userId")
                }
                response = await call_next(request)
                return response

        return JSONResponse(
            content={"message": "Invalid authorization token"},
            status_code=401,
            headers={"WWW-Authenticate": "Bearer"}
        )