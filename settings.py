from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Server settings
    PORT: int = 9000
    
    # Database settings
    MONGO_URL: str = "mongodb://localhost:27017"
    MONGO_DB: str = "live"
    POSTGRES_URL: str = ""
    SYNCED_DB_URL: str = ""
    SYNCED_WRITE_DB_URL: str = ""
    # API settings
    X_API_KEY: str = "x-api-key"
    JWT_SECRET: str = "jwt-secret"
    
    # New Relic settings
    NEW_RELIC_APP_NAME: str = ""
    NEW_RELIC_LICENSE_KEY: str = ""

    # S3 settings
    S3_BUCKET_NAME: str = "bucket-name"
    S3_ENDPOINT: str = "http://localhost:9000" 
    S3_AWS_ACCESS_KEY_ID: str = "boto3"
    S3_AWS_SECRET_ACCESS_KEY: str = "boto3"
    
    # App settings
    ENVIRONMENT: str = "Production"
    INSERT_SCHEDULE_INPUT_DATA: bool = False
    
    # Read models from local for testing purposes
    READ_MODELS_FROM_LOCAL: bool = False

    # Drayos settings
    DRAYOS_API_URL: str = "https://pre.api.axle.network"

    # Trackos settings
    TRACKOS_API_URL: str = "https://trackos-api.pre-prod.portpro.io"
    TRACKOS_DB: str = "trackos"
    TRACKOS_API_KEY: str = ""

    # Redis settings
    REDIS_URL: str = ""

    # ELD settings
    ELD_API_URL: str = "https://api.eld.portpro.io"
    ELD_API_KEY: str = ""

    # Queue settings
    QUEUE_URL: str = ""
    EXCHANGE_NAME: str = ""

    # Pre Queue settings
    PRE_QUEUE_URL: str = ""
    PRE_EXCHANGE_NAME: str = ""

    # Firebase settings
    FIREBASE_DATABASEURL: str = ""
    FIREBASE_APIKEY: str = ""

    # No of parallel execution
    NO_OF_PARALLEL_EXECUTION: int = 1

    ENABLE_QUEUE: bool = False

    # Email settings
    SMTP_SERVER: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    MANDRILL_USER: str = ""
    MANDRILL_PASSWORD: str = ""
    
    # Distance routing settings
    DISTANCE_ROUTING_URL: str = ""

    class Config:
        env_file = ".env"
        # Allow extra fields in env file that aren't defined in the model
        extra = "ignore"


settings = Settings()
