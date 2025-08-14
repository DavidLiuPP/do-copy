import pika
import logging
import json
import asyncio
from typing import Optional, Callable, Dict
from pika.exceptions import AMQPConnectionError
from pika.adapters.asyncio_connection import AsyncioConnection
from app.modules.scheduler.move_scheduler import replan_modified_move
from app.services.redis_service import get_carrier_settings
from settings import settings
from app.utils.common_utils import extract_user_id
logger = logging.getLogger(__name__)

class EventService:
    """
    Service for handling asynchronous event messaging using RabbitMQ.
    
    This class manages connections to a RabbitMQ message broker, handles topic-based
    pub/sub messaging, and provides methods for subscribing to various event types.
    
    Attributes:
        queue_url (str): RabbitMQ connection URL from environment
        exchange_name (str): Name of the exchange to use
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, queue_url: str, exchange_name: str):
        """Initialize the EventService with configuration from environment variables."""
        self.queue_url = queue_url
        self.exchange_name = exchange_name
        self.loop = loop
        self.handlers: Dict[str, Callable] = {}

        if not self.queue_url or not self.exchange_name:
            logger.info("Required environment variables missing - queue listener will not be triggered")
            return
        
        self.connection: Optional[AsyncioConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.queue_name: Optional[str] = None
        self._consuming = False
        
        try:
            self._setup_connection()
        except AMQPConnectionError as e:
            logger.error(f"Failed to establish RabbitMQ connection: {str(e)}")
            raise

    def _setup_connection(self) -> None:
        """Establish connection to RabbitMQ and set up channel."""
        parameters = pika.URLParameters(self.queue_url)
        
        # Create connection with async adapter
        self.connection = AsyncioConnection(
            parameters=parameters,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_error,
            on_close_callback=self._on_connection_closed
        )

    def _on_connection_open(self, connection):
        """Called when connection is established"""
        logger.info("RabbitMQ connection opened")
        self.connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        """Called when channel is opened"""
        logger.info("RabbitMQ channel opened")
        self.channel = channel
        
        # Set prefetch count to control message distribution
        self.channel.basic_qos(prefetch_count=settings.NO_OF_PARALLEL_EXECUTION)  # Process N message at a time
        
        # Setup exchange
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type='topic',
            durable=True,
            auto_delete=False,
            callback=self._on_exchange_declare_ok
        )

    def _on_exchange_declare_ok(self, _unused_frame):
        """Called when exchange declare is complete"""
        logger.info("Exchange declared")
        
        # Create a shared, durable queue with a fixed name
        self.channel.queue_declare(
            queue='dispatch_optimization_load_processing',  # Fixed queue name shared across consumers
            exclusive=False,  # Allow multiple consumers
            auto_delete=False,  # Queue should persist
            durable=True,  # Queue survives broker restarts
            callback=self._on_queue_declare_ok
        )

    def _on_queue_declare_ok(self, method_frame):
        """Called when queue declare is complete"""
        self.queue_name = method_frame.method.queue
        logger.info(f"Queue {self.queue_name} declared")
        
        # Now that queue is ready, bind it and set up consumer
        self.subscribe_to_event('load.#', self.process_load_message)
        self.subscribe_to_event('routing.event.address.updated', self.process_load_message)
        self.subscribe_to_event('routing.event.status.changed', self.process_load_message)
        self.subscribe_to_event('routing.event.added', self.process_load_message)
        self.subscribe_to_event('routing.event.deleted', self.process_load_message)

    def _on_connection_error(self, connection, error):
        """Called if connection cannot be established"""
        logger.error(f"RabbitMQ connection error: {error}")
        self.loop.call_later(5, self._setup_connection)

    def _on_connection_closed(self, connection, reason):
        """Called when connection is closed"""
        logger.warning(f"RabbitMQ connection closed: {reason}")
        self.loop.call_later(5, self._setup_connection)
        self._consuming = False

    async def close(self):
        """Cleanup and close connection"""
        if self._consuming:
            if self.channel:
                self.channel.close()
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def _wrap_async_callback(self, callback):
        """Wrap async callback to be used with pika's basic_consume"""
        async def process_with_ack(ch, method, properties, body):
            try:
                await callback(ch, method, properties, body)
            except Exception as e:
                logger.error(f"Error in wrapped callback: {str(e)}")
            finally:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        def wrapped_callback(ch, method, properties, body):
            asyncio.run_coroutine_threadsafe(
                process_with_ack(ch, method, properties, body),
                self.loop
            )
        return wrapped_callback

    def subscribe_to_event(self, event_type: str, callback: Callable) -> None:
        """
        Subscribe to a specific event type and begin processing messages.
        """
        if not self.channel or not self.connection:
            raise RuntimeError("Connection not established. Call _setup_connection() first.")
        
        self.channel.queue_bind(
            exchange=self.exchange_name,
            queue=self.queue_name,
            routing_key=event_type
        )

        # Configure consumer with manual acknowledgment
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self._wrap_async_callback(callback),
            auto_ack=False 
        )

    async def process_message_async(self, routing_key: str, body: bytes):
        """Process messages asynchronously"""
        try:
            message = json.loads(body.decode('utf-8'))
            userId = extract_user_id(message.get('updatedData', {}).get('carrier'))
            if not userId:
                userId = extract_user_id(message.get('oldData', {}).get('carrier'))
            
            if not userId or not isinstance(userId, str):
                print(f"Invalid userId: {userId}")
                print(f"routing_key: {routing_key}")
                return

            carrier_settings = await get_carrier_settings(userId)

            if carrier_settings and carrier_settings.get('isDispatchAutomationEnabled', False):
                await replan_modified_move(
                    userId,
                    routing_key,
                    message
                )
            return True
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            logger.info(f"Message: {message}")
            return False

    async def process_load_message(self, ch, method, properties, body) -> None:
        """
        Process incoming load messages.
        """
        try:
            try:
                result = await asyncio.wait_for(
                    self.process_message_async(method.routing_key, body),
                    timeout=30
                )
                return result
            except asyncio.TimeoutError:
                logger.warning("Message processing timed out after 30 seconds")
                return None
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            return None