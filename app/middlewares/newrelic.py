import newrelic.agent
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request

class NewRelicMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        
        # Process the request and return the response
        response = await call_next(request)

        # Set the transaction name to the API endpoint path
        endPoint = request.url.path
        newrelic.agent.set_transaction_name(endPoint)

        # Add custom attributes to the transaction
        user_data = getattr(request.state, "user", None)
        if user_data and isinstance(user_data, dict):
            if user_data.get("userId"):
                newrelic.agent.add_custom_parameter("user_id", user_data.get("userId"))
            if user_data.get("carrier"):
                newrelic.agent.add_custom_parameter("carrier", user_data.get("carrier"))

        return response