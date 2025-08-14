import logging
import newrelic.agent
from settings import settings
from newrelic.agent import NewRelicContextFormatter


def initialize_newrelic():
    """
    Initialize New Relic agent if environment variables are set.
    :param app_name: Name of your application for New Relic
    """
    try:
        newrelic_license_key = settings.NEW_RELIC_LICENSE_KEY
        if newrelic_license_key:
            # Configure New Relic with environment variables
            newrelic.agent.initialize()
            logging.info("New Relic initialized.")
        else:
            logging.warning("New Relic license key not found. Skipping initialization.")
    except Exception as e:
        logging.warning("New relic Issue")

def setup_logging_with_newrelic():
    """
    Configure logging with New Relic log handler if available.
    """
    try:
        # Set up logger
        logger = logging.getLogger("uvicorn")
        handler = logging.StreamHandler()
        handler.setFormatter(NewRelicContextFormatter())
        logger.addHandler(handler)

        logging.info("New Relic logging configured.")
    except ImportError:
        logging.warning("New Relic logging handler not found. Proceeding with standard logging.")
