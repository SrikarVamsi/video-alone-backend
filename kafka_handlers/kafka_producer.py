# """
# Kafka Producer with Circuit Breaker Pattern and Advanced Features

# This module provides:
# - Robust Kafka message publishing with circuit breaker protection
# - Batch processing capabilities for high throughput
# - Message compression and serialization
# - Retry mechanisms with exponential backoff
# - Metrics collection and monitoring
# - Schema validation before publishing
# - Correlation ID support for distributed tracing
# """

# import json
# import time
# import uuid
# import asyncio
# from typing import Dict, List, Optional, Any, Callable
# from dataclasses import dataclass, asdict
# from datetime import datetime
# from enum import Enum
# import threading
# from queue import Queue, Empty
# import hashlib

# import kafka as kafka_lib
# KafkaProducer = kafka_lib.KafkaProducer
# from kafka.errors import KafkaError, KafkaTimeoutError
# import redis
# from prometheus_client import Counter, Histogram, Gauge

# from config.settings import get_settings
# from utils.logger import get_logger, LogContext
# from kafka_handlers.topics import TopicType, TopicManager, MessageSchemaValidator

# # Initialize components
# logger = get_logger(__name__)
# settings = get_settings()
# topic_manager = TopicManager()

# # Prometheus metrics
# producer_messages_total = Counter(
#     'kafka_producer_messages_total', 
#     'Total messages sent to Kafka',
#     ['topic', 'status']
# )
# producer_latency = Histogram(
#     'kafka_producer_latency_seconds',
#     'Time spent sending messages to Kafka',
#     ['topic']
# )
# producer_batch_size = Histogram(
#     'kafka_producer_batch_size',
#     'Size of message batches sent to Kafka',
#     ['topic']
# )
# circuit_breaker_state = Gauge(
#     'kafka_producer_circuit_breaker_state',
#     'Circuit breaker state (0=closed, 1=open, 2=half-open)',
#     ['topic']
# )


# class CircuitBreakerState(Enum):
#     """Circuit breaker states"""
#     CLOSED = 0      # Normal operation
#     OPEN = 1        # Circuit is open, failing fast
#     HALF_OPEN = 2   # Testing if service is back


# @dataclass
# class ProducerConfig:
#     """Configuration for Kafka producer"""
#     bootstrap_servers: str
#     client_id: str
#     compression_type: str = 'gzip'
#     batch_size: int = 16384
#     linger_ms: int = 10
#     buffer_memory: int = 33554432
#     max_request_size: int = 1048576
#     acks: str = 'all'
#     retries: int = 3
#     retry_backoff_ms: int = 100
#     request_timeout_ms: int = 30000
#     max_in_flight_requests_per_connection: int = 5
#     # enable_idempotence: bool = True


# @dataclass
# class CircuitBreakerConfig:
#     """Configuration for circuit breaker"""
#     failure_threshold: int = 5
#     recovery_timeout: int = 60
#     half_open_max_calls: int = 3
#     timeout: int = 30


# class CircuitBreaker:
#     """Circuit breaker implementation for Kafka producer"""
    
#     def __init__(self, config: CircuitBreakerConfig, topic: str):
#         self.config = config
#         self.topic = topic
#         self.state = CircuitBreakerState.CLOSED
#         self.failure_count = 0
#         self.last_failure_time = 0
#         self.half_open_calls = 0
#         self.lock = threading.Lock()
        
#         # Update metrics
#         circuit_breaker_state.labels(topic=topic).set(self.state.value)
    
#     def call(self, func: Callable, *args, **kwargs):
#         """Execute function with circuit breaker protection"""
#         with self.lock:
#             if self.state == CircuitBreakerState.OPEN:
#                 if time.time() - self.last_failure_time > self.config.recovery_timeout:
#                     self.state = CircuitBreakerState.HALF_OPEN
#                     self.half_open_calls = 0
#                     logger.info(f"Circuit breaker for {self.topic} moved to HALF_OPEN")
#                     circuit_breaker_state.labels(topic=self.topic).set(self.state.value)
#                 else:
#                     raise Exception(f"Circuit breaker is OPEN for topic {self.topic}")
            
#             if self.state == CircuitBreakerState.HALF_OPEN:
#                 if self.half_open_calls >= self.config.half_open_max_calls:
#                     raise Exception(f"Circuit breaker HALF_OPEN limit reached for topic {self.topic}")
#                 self.half_open_calls += 1
        
#         try:
#             result = func(*args, **kwargs)
#             self._on_success()
#             return result
#         except Exception as e:
#             self._on_failure()
#             raise e
    
#     def _on_success(self):
#         """Handle successful call"""
#         with self.lock:
#             if self.state == CircuitBreakerState.HALF_OPEN:
#                 self.state = CircuitBreakerState.CLOSED
#                 logger.info(f"Circuit breaker for {self.topic} moved to CLOSED")
            
#             self.failure_count = 0
#             circuit_breaker_state.labels(topic=self.topic).set(self.state.value)
    
#     def _on_failure(self):
#         """Handle failed call"""
#         with self.lock:
#             self.failure_count += 1
#             self.last_failure_time = time.time()
            
#             if self.failure_count >= self.config.failure_threshold:
#                 self.state = CircuitBreakerState.OPEN
#                 logger.warning(f"Circuit breaker for {self.topic} moved to OPEN")
#                 circuit_breaker_state.labels(topic=self.topic).set(self.state.value)


# class MessageBatch:
#     """Batch of messages for efficient processing"""
    
#     def __init__(self, max_size: int = 100, max_wait_time: int = 5):
#         self.max_size = max_size
#         self.max_wait_time = max_wait_time
#         self.messages: List[Dict[str, Any]] = []
#         self.created_at = time.time()
#         self.lock = threading.Lock()
    
#     def add_message(self, message: Dict[str, Any]) -> bool:
#         """Add message to batch, return True if batch is full"""
#         with self.lock:
#             self.messages.append(message)
#             return len(self.messages) >= self.max_size
    
#     def is_ready(self) -> bool:
#         """Check if batch is ready for processing"""
#         with self.lock:
#             return (len(self.messages) >= self.max_size or 
#                    (len(self.messages) > 0 and time.time() - self.created_at > self.max_wait_time))
    
#     def get_messages(self) -> List[Dict[str, Any]]:
#         """Get all messages and clear batch"""
#         with self.lock:
#             messages = self.messages.copy()
#             self.messages.clear()
#             return messages


# class KafkaProducerClient:
#     """Enhanced Kafka producer with circuit breaker and advanced features"""
    
#     def __init__(self, settings_obj: Optional[Any] = None):
#         self.settings = settings_obj or get_settings()  # Store settings reference
#         self.config = self._get_default_config()
#         self.producer = None
#         self.circuit_breakers: Dict[str, CircuitBreaker] = {}
#         self.message_batches: Dict[str, MessageBatch] = {}
#         self.redis_client = None
#         self.batch_processor_thread = None
#         self.stop_event = threading.Event()
#         self.started = False  # Track initialization state
        
#         # Don't initialize immediately - wait for start() call
    
#     async def start(self):
#         """Initialize and start the Kafka producer"""
#         if self.started:
#             logger.warning("Kafka producer already started")
#             return
        
#         try:
#             self._initialize_producer()
#             self._initialize_redis()
#             self._start_batch_processor()
#             self.started = True
#             logger.info("Kafka producer started successfully")
#         except Exception as e:
#             logger.error(f"Failed to start Kafka producer: {e}")
#             raise
    
#     async def shutdown(self):
#         """Shutdown the Kafka producer gracefully"""
#         if not self.started:
#             return
        
#         logger.info("Shutting down Kafka producer...")
#         self.close()
#         self.started = False
#         logger.info("Kafka producer shutdown completed")
    
#     def _get_default_config(self) -> ProducerConfig:
#         """Get default producer configuration"""
#         return ProducerConfig(
#             bootstrap_servers=self.settings.kafka.bootstrap_servers,
#             client_id=f"producer_{uuid.uuid4().hex[:8]}",
#             compression_type=self.settings.kafka.compression_type,
#             batch_size=self.settings.kafka.batch_size,
#             linger_ms=self.settings.kafka.linger_ms,
#             acks=self.settings.kafka.acks,
#             retries=self.settings.kafka.retries,
#             # enable_idempotence=self.settings.kafka.enable_idempotence
#         )
    
#     def _initialize_producer(self):
#         """Initialize Kafka producer with configuration"""
#         try:
#             producer_config = {
#                 'bootstrap_servers': self.settings.kafka.bootstrap_servers,
#                 'client_id': self.config.client_id,
#                 'compression_type': self.settings.kafka.compression_type,
#                 'batch_size': self.settings.kafka.batch_size,
#                 'linger_ms': self.settings.kafka.linger_ms,
#                 'buffer_memory': getattr(self.settings.kafka, 'buffer_memory', 33554432),
#                 'max_request_size': getattr(self.settings.kafka, 'max_request_size', 1048576),
#                 'acks': self.settings.kafka.acks,
#                 'retries': self.settings.kafka.retries,
#                 'retry_backoff_ms': getattr(self.settings.kafka, 'retry_backoff_ms', 100),
#                 'request_timeout_ms': getattr(self.settings.kafka, 'request_timeout_ms', 30000),
#                 'max_in_flight_requests_per_connection': getattr(self.settings.kafka, 'max_in_flight_requests_per_connection', 5),
#                 'value_serializer': lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v,
#                 'key_serializer': lambda k: str(k).encode('utf-8') if k else None
#             }
            
#             # Add security configuration if needed
#             if hasattr(self.settings.kafka, 'security_protocol') and self.settings.kafka.security_protocol != "PLAINTEXT":
#                 producer_config.update({
#                     'security_protocol': self.settings.kafka.security_protocol,
#                     'sasl_mechanism': self.settings.kafka.sasl_mechanism,
#                     'sasl_plain_username': self.settings.kafka.sasl_username,
#                     'sasl_plain_password': self.settings.kafka.sasl_password,
#                 })
            
#             self.producer = KafkaProducer(**producer_config)
#             logger.info("Kafka producer initialized successfully")
            
#         except Exception as e:
#             logger.error(f"Failed to initialize Kafka producer: {e}")
#             raise
    
#     def _initialize_redis(self):
#         """Initialize Redis client for caching"""
#         try:
#             # Check if Redis is enabled - use getattr with default False if enabled field doesn't exist
#             redis_enabled = getattr(self.settings.redis, 'enabled', True)  # Default to True if not specified
            
#             if redis_enabled:
#                 self.redis_client = redis.Redis(
#                     host=self.settings.redis.host,
#                     port=self.settings.redis.port,
#                     db=self.settings.redis.db,
#                     password=self.settings.redis.password,
#                     decode_responses=True,
#                     socket_timeout=self.settings.redis.socket_timeout,
#                     socket_connect_timeout=self.settings.redis.socket_connect_timeout,
#                     retry_on_timeout=True,
#                     max_connections=self.settings.redis.max_connections
#                 )
#                 # Test connection
#                 self.redis_client.ping()
#                 logger.info("Redis client initialized successfully")
#             else:
#                 logger.info("Redis is disabled in configuration")
#         except Exception as e:
#             logger.warning(f"Failed to initialize Redis client: {e}")
#             self.redis_client = None
    
#     def _start_batch_processor(self):
#         """Start background thread for batch processing"""
#         self.batch_processor_thread = threading.Thread(
#             target=self._process_batches,
#             daemon=True
#         )
#         self.batch_processor_thread.start()
#         logger.info("Batch processor thread started")
    
#     def _process_batches(self):
#         """Background process for handling message batches"""
#         while not self.stop_event.is_set():
#             try:
#                 ready_topics = []
                
#                 # Check which batches are ready
#                 for topic, batch in self.message_batches.items():
#                     if batch.is_ready():
#                         ready_topics.append(topic)
                
#                 # Process ready batches
#                 for topic in ready_topics:
#                     batch = self.message_batches[topic]
#                     messages = batch.get_messages()
                    
#                     if messages:
#                         self._send_batch(topic, messages)
#                         producer_batch_size.labels(topic=topic).observe(len(messages))
                
#                 # Sleep briefly to avoid busy waiting
#                 time.sleep(0.1)
                
#             except Exception as e:
#                 logger.error(f"Error in batch processor: {e}")
#                 time.sleep(1)
    
#     def _send_batch(self, topic: str, messages: List[Dict[str, Any]]):
#         """Send a batch of messages to Kafka"""
#         try:
#             # Get or create circuit breaker for this topic
#             if topic not in self.circuit_breakers:
#                 self.circuit_breakers[topic] = CircuitBreaker(
#                     CircuitBreakerConfig(), topic
#                 )
            
#             circuit_breaker = self.circuit_breakers[topic]
            
#             # Send messages through circuit breaker
#             def send_messages():
#                 futures = []
#                 for message in messages:
#                     future = self.producer.send(
#                         topic=topic,
#                         value=message['value'],
#                         key=message.get('key'),
#                         partition=message.get('partition'),
#                         timestamp_ms=message.get('timestamp_ms')
#                     )
#                     futures.append(future)
                
#                 # Wait for all messages to be sent
#                 for future in futures:
#                     future.get(timeout=30)
                
#                 return len(futures)
            
#             sent_count = circuit_breaker.call(send_messages)
            
#             # Update metrics
#             producer_messages_total.labels(topic=topic, status='success').inc(sent_count)
#             logger.debug(f"Sent batch of {sent_count} messages to {topic}")
            
#         except Exception as e:
#             producer_messages_total.labels(topic=topic, status='error').inc(len(messages))
#             logger.error(f"Failed to send batch to {topic}: {e}")
    
#     def _get_circuit_breaker(self, topic: str) -> CircuitBreaker:
#         """Get or create circuit breaker for topic"""
#         if topic not in self.circuit_breakers:
#             self.circuit_breakers[topic] = CircuitBreaker(
#                 CircuitBreakerConfig(), topic
#             )
#         return self.circuit_breakers[topic]
    
#     def _cache_message(self, cache_key: str, message: Dict[str, Any], ttl: int = 300):
#         """Cache message in Redis for deduplication"""
#         if self.redis_client:
#             try:
#                 self.redis_client.setex(
#                     cache_key, 
#                     ttl, 
#                     json.dumps(message, default=str)
#                 )
#             except Exception as e:
#                 logger.warning(f"Failed to cache message: {e}")
    
#     def _is_duplicate_message(self, cache_key: str) -> bool:
#         """Check if message is duplicate using Redis cache"""
#         if self.redis_client:
#             try:
#                 return self.redis_client.exists(cache_key)
#             except Exception as e:
#                 logger.warning(f"Failed to check duplicate: {e}")
#         return False
    
#     def _generate_message_key(self, topic: str, message: Dict[str, Any]) -> str:
#         """Generate unique key for message deduplication"""
#         # Create hash of message content for deduplication
#         content = json.dumps(message, sort_keys=True, default=str)
#         hash_obj = hashlib.md5(content.encode())
#         return f"msg:{topic}:{hash_obj.hexdigest()}"
    
#     def _add_correlation_id(self, message: Dict[str, Any]) -> Dict[str, Any]:
#         """Add correlation ID for distributed tracing"""
#         if 'correlation_id' not in message:
#             message['correlation_id'] = str(uuid.uuid4())
#         return message
    
#     def _validate_message(self, topic_type: TopicType, message: Dict[str, Any]) -> bool:
#         """Validate message schema"""
#         try:
#             if topic_type == TopicType.DETECTION_EVENTS:
#                 return MessageSchemaValidator.validate_detection_event(message)
#             elif topic_type == TopicType.ALERTS:
#                 return MessageSchemaValidator.validate_alert(message)
#             elif topic_type == TopicType.METRICS:
#                 return MessageSchemaValidator.validate_metric(message)
#             else:
#                 return True  # Skip validation for other message types
#         except Exception as e:
#             logger.error(f"Message validation failed: {e}")
#             return False
    
#     def send_message(self, 
#                     topic_type: TopicType, 
#                     message: Dict[str, Any],
#                     key: Optional[str] = None,
#                     partition: Optional[int] = None,
#                     enable_deduplication: bool = True,
#                     batch_mode: bool = False) -> bool:
#         """
#         Send a single message to Kafka topic
        
#         Args:
#             topic_type: Type of topic to send to
#             message: Message payload
#             key: Message key for partitioning
#             partition: Specific partition to send to
#             enable_deduplication: Whether to check for duplicates
#             batch_mode: Whether to use batch processing
        
#         Returns:
#             bool: True if message was sent successfully
#         """
#         if not self.started:
#             logger.error("Kafka producer not started. Call start() first.")
#             return False
            
#         try:
#             # Get topic name
#             topic_name = topic_manager.get_topic_name(topic_type)
            
#             # Add correlation ID and metadata
#             message = self._add_correlation_id(message)
#             message['timestamp'] = datetime.utcnow().isoformat()
#             message['producer_id'] = self.config.client_id
            
#             # Validate message schema
#             if not self._validate_message(topic_type, message):
#                 logger.error(f"Message validation failed for topic {topic_name}")
#                 return False
            
#             # Check for duplicates
#             if enable_deduplication:
#                 cache_key = self._generate_message_key(topic_name, message)
#                 if self._is_duplicate_message(cache_key):
#                     logger.info(f"Duplicate message detected for topic {topic_name}")
#                     return True
#                 self._cache_message(cache_key, message)
            
#             if batch_mode:
#                 # Add to batch
#                 if topic_name not in self.message_batches:
#                     self.message_batches[topic_name] = MessageBatch()
                
#                 batch_message = {
#                     'value': message,
#                     'key': key,
#                     'partition': partition,
#                     'timestamp_ms': int(time.time() * 1000)
#                 }
                
#                 self.message_batches[topic_name].add_message(batch_message)
#                 return True
#             else:
#                 # Send immediately
#                 return self._send_single_message(topic_name, message, key, partition)
                
#         except Exception as e:
#             logger.error(f"Error sending message to {topic_type.value}: {e}")
#             return False
    
#     def _send_single_message(self, topic: str, message: Dict[str, Any], 
#                            key: Optional[str] = None, 
#                            partition: Optional[int] = None) -> bool:
#         """Send a single message immediately"""
#         try:
#             circuit_breaker = self._get_circuit_breaker(topic)
            
#             def send_message():
#                 start_time = time.time()
                
#                 future = self.producer.send(
#                     topic=topic,
#                     value=message,
#                     key=key,
#                     partition=partition,
#                     timestamp_ms=int(time.time() * 1000)
#                 )
                
#                 # Wait for acknowledgment
#                 record_metadata = future.get(timeout=30)
                
#                 # Record metrics
#                 latency = time.time() - start_time
#                 producer_latency.labels(topic=topic).observe(latency)
                
#                 return record_metadata
            
#             record_metadata = circuit_breaker.call(send_message)
            
#             # Update metrics
#             producer_messages_total.labels(topic=topic, status='success').inc()
            
#             logger.debug(f"Message sent to {topic} at partition {record_metadata.partition}, offset {record_metadata.offset}")
#             return True
            
#         except Exception as e:
#             producer_messages_total.labels(topic=topic, status='error').inc()
#             logger.error(f"Failed to send message to {topic}: {e}")
#             return False
    
#     def send_detection_event(self, detection_data: Dict[str, Any]) -> bool:
#         """Send detection event message"""
#         return self.send_message(
#             TopicType.DETECTION_EVENTS,
#             detection_data,
#             key=detection_data.get('camera_id'),
#             batch_mode=True
#         )
    
#     def send_alert(self, alert_data: Dict[str, Any]) -> bool:
#         """Send alert message"""
#         return self.send_message(
#             TopicType.ALERTS,
#             alert_data,
#             key=alert_data.get('camera_id'),
#             batch_mode=False  # Alerts should be sent immediately
#         )
    
#     def send_metric(self, metric_data: Dict[str, Any]) -> bool:
#         """Send metric message"""
#         return self.send_message(
#             TopicType.METRICS,
#             metric_data,
#             key=metric_data.get('service_name'),
#             batch_mode=True
#         )
    
#     def flush(self, timeout: int = 30) -> bool:
#         """Flush all pending messages"""
#         try:
#             # Process any remaining batches
#             for topic, batch in self.message_batches.items():
#                 messages = batch.get_messages()
#                 if messages:
#                     self._send_batch(topic, messages)
            
#             # Flush producer
#             if self.producer:
#                 self.producer.flush(timeout=timeout)
#             logger.info("Producer flushed successfully")
#             return True
            
#         except Exception as e:
#             logger.error(f"Failed to flush producer: {e}")
#             return False
    
#     async def health_check(self) -> bool:
#         """Health check for the producer"""
#         if not self.started:
#             return False
        
#         try:
#             # Check if producer is still functional
#             if self.producer and hasattr(self.producer, 'bootstrap_connected'):
#                 return self.producer.bootstrap_connected()
#             return True
#         except Exception as e:
#             logger.error(f"Producer health check failed: {e}")
#             return False
    
#     def get_metrics(self) -> Dict[str, Any]:
#         """Get producer metrics"""
#         return {
#             'started': self.started,
#             'circuit_breakers': {
#                 topic: {
#                     'state': cb.state.name,
#                     'failure_count': cb.failure_count,
#                     'half_open_calls': cb.half_open_calls
#                 }
#                 for topic, cb in self.circuit_breakers.items()
#             },
#             'active_batches': {
#                 topic: len(batch.messages)
#                 for topic, batch in self.message_batches.items()
#             },
#             'producer_config': {
#                 'client_id': self.config.client_id,
#                 'batch_size': self.config.batch_size,
#                 'linger_ms': self.config.linger_ms,
#                 'compression_type': self.config.compression_type
#             }
#         }
    
#     def close(self):
#         """Close producer and cleanup resources"""
#         logger.info("Closing Kafka producer...")
        
#         # Stop batch processor
#         self.stop_event.set()
#         if self.batch_processor_thread:
#             self.batch_processor_thread.join(timeout=5)
        
#         # Flush remaining messages
#         self.flush()
        
#         # Close producer
#         if self.producer:
#             self.producer.close()
        
#         # Close Redis client
#         if self.redis_client:
#             self.redis_client.close()
        
#         logger.info("Kafka producer closed")
    
#     def __enter__(self):
#         return self
    
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.close()


# # Global producer instance
# _producer_instance = None
# _producer_lock = threading.Lock()


# def get_kafka_producer() -> KafkaProducerClient:
#     """Get or create global Kafka producer instance"""
#     global _producer_instance
    
#     if _producer_instance is None:
#         with _producer_lock:
#             if _producer_instance is None:
#                 _producer_instance = KafkaProducerClient()
    
#     return _producer_instance


# def cleanup_producer():
#     """Cleanup global producer instance"""
#     global _producer_instance
    
#     if _producer_instance:
#         _producer_instance.close()
#         _producer_instance = None


# # Convenience functions for different message types
# def send_detection_event(detection_data: Dict[str, Any]) -> bool:
#     """Send detection event to Kafka"""
#     producer = get_kafka_producer()
#     return producer.send_detection_event(detection_data)


# def send_alert(alert_data: Dict[str, Any]) -> bool:
#     """Send alert to Kafka"""
#     producer = get_kafka_producer()
#     return producer.send_alert(alert_data)


# def send_metric(metric_data: Dict[str, Any]) -> bool:
#     """Send metric to Kafka"""
#     producer = get_kafka_producer()
#     return producer.send_metric(metric_data)


# if __name__ == "__main__":
#     # Test the producer
#     import time
    
#     # Test detection event
#     detection_event = {
#         'detection_id': str(uuid.uuid4()),
#         'timestamp': datetime.utcnow().isoformat(),
#         'camera_id': 'camera_001',
#         'objects': [
#             {
#                 'class': 'person',
#                 'confidence': 0.95,
#                 'bbox': [100, 100, 200, 300],
#                 'tracking_id': 'track_001'
#             }
#         ],
#         'frame_id': 'frame_12345',
#         'confidence': 0.95,
#         'processing_time': 0.15
#     }
    
#     # Test alert
#     alert = {
#         'alert_id': str(uuid.uuid4()),
#         'timestamp': datetime.utcnow().isoformat(),
#         'camera_id': 'camera_001',
#         'alert_type': 'intrusion',
#         'severity': 'high',
#         'message': 'Person detected in restricted area',
#         'correlation_id': str(uuid.uuid4())
#     }
    
#     # Test metric
#     metric = {
#         'metric_name': 'detection_latency',
#         'value': 0.15,
#         'timestamp': datetime.utcnow().isoformat(),
#         'service_name': 'detection_service'
#     }
    
#     print("Testing Kafka producer...")
    
#     # Send test messages
#     print("Sending detection event:", send_detection_event(detection_event))
#     print("Sending alert:", send_alert(alert))
#     print("Sending metric:", send_metric(metric))
    
#     # Cleanup
#     cleanup_producer()
#     print("Test completed")

"""
Kafka Producer with Circuit Breaker Pattern and Advanced Features

This module provides:
- Robust Kafka message publishing with circuit breaker protection
- Batch processing capabilities for high throughput
- Message compression and serialization
- Retry mechanisms with exponential backoff
- Metrics collection and monitoring
- Schema validation before publishing
- Correlation ID support for distributed tracing
"""

import json
import time
import uuid
import asyncio
from typing import Dict, List, Optional, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
import threading
from queue import Queue, Empty
import hashlib

import kafka as kafka_lib
KafkaProducer = kafka_lib.KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
import redis # This imports synchronous redis client
import redis.asyncio as aioredis # Use async Redis client if preferred for async operations
from prometheus_client import Counter, Histogram, Gauge

# Removed module-level get_settings() and TopicManager() instance
# from config.settings import get_settings
from utils.logger import get_logger, LogContext
# TopicManager needs to be imported, but its instance passed or created with settings
# from kafka_handlers.topics import TopicType, TopicManager, MessageSchemaValidator

# Type hinting for settings and TopicManager to avoid runtime import issues
if TYPE_CHECKING:
    from config.settings import Settings as AppSettings
    from kafka_handlers.topics import TopicManager as AppTopicManager
    from kafka_handlers.topics import TopicType, MessageSchemaValidator


# Initialize logger
logger = get_logger(__name__)

# Prometheus metrics
producer_messages_total = Counter(
    'kafka_producer_messages_total', 
    'Total messages sent to Kafka',
    ['topic', 'status']
)
producer_latency = Histogram(
    'kafka_producer_latency_seconds',
    'Time spent sending messages to Kafka',
    ['topic']
)
producer_batch_size = Histogram(
    'kafka_producer_batch_size',
    'Size of message batches sent to Kafka',
    ['topic']
)
circuit_breaker_state = Gauge(
    'kafka_producer_circuit_breaker_state',
    'Circuit breaker state (0=closed, 1=open, 2=half-open)',
    ['topic']
)


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = 0      # Normal operation
    OPEN = 1        # Circuit is open, failing fast
    HALF_OPEN = 2   # Testing if service is back


@dataclass
class ProducerConfig:
    """Configuration for Kafka producer"""
    bootstrap_servers: str
    client_id: str
    compression_type: str = 'gzip'
    batch_size: int = 16384
    linger_ms: int = 10
    buffer_memory: int = 33554432
    max_request_size: int = 1048576
    acks: str = 'all'
    retries: int = 3
    retry_backoff_ms: int = 100
    request_timeout_ms: int = 30000
    max_in_flight_requests_per_connection: int = 5
    # enable_idempotence: bool = True # Removed as it's not supported by kafka-python 2.0.2


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker"""
    failure_threshold: int = 5
    recovery_timeout: int = 60
    half_open_max_calls: int = 3
    timeout: int = 30


class CircuitBreaker:
    """Circuit breaker implementation for Kafka producer"""
    def __init__(self, config: CircuitBreakerConfig, topic: str):
        self.config = config
        self.topic = topic
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.half_open_calls = 0
        self.lock = threading.Lock()
        
        # Update metrics
        circuit_breaker_state.labels(topic=topic).set(self.state.value)
    
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        with self.lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time > self.config.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_calls = 0
                    logger.info(f"Circuit breaker for {self.topic} moved to HALF_OPEN")
                    circuit_breaker_state.labels(topic=self.topic).set(self.state.value)
                else:
                    raise Exception(f"Circuit breaker is OPEN for topic {self.topic}")
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_calls >= self.config.half_open_max_calls:
                    raise Exception(f"Circuit breaker HALF_OPEN limit reached for topic {self.topic}")
                self.half_open_calls += 1
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self):
        """Handle successful call"""
        with self.lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
                logger.info(f"Circuit breaker for {self.topic} moved to CLOSED")
            
            self.failure_count = 0
            circuit_breaker_state.labels(topic=self.topic).set(self.state.value)
    
    def _on_failure(self):
        """Handle failed call"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.config.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"Circuit breaker for {self.topic} moved to OPEN")
                circuit_breaker_state.labels(topic=self.topic).set(self.state.value)


class MessageBatch:
    """Batch of messages for efficient processing"""
    def __init__(self, max_size: int = 100, max_wait_time: int = 5):
        self.max_size = max_size
        self.max_wait_time = max_wait_time
        self.messages: List[Dict[str, Any]] = []
        self.created_at = time.time()
        self.lock = threading.Lock()
    
    def add_message(self, message: Dict[str, Any]) -> bool:
        """Add message to batch, return True if batch is full"""
        with self.lock:
            self.messages.append(message)
            return len(self.messages) >= self.max_size
    
    def is_ready(self) -> bool:
        """Check if batch is ready for processing"""
        with self.lock:
            return (len(self.messages) >= self.max_size or 
                   (len(self.messages) > 0 and time.time() - self.created_at > self.max_wait_time))
    
    def get_messages(self) -> List[Dict[str, Any]]:
        """Get all messages and clear batch"""
        with self.lock:
            messages = self.messages.copy()
            self.messages.clear()
            return messages


class KafkaProducerClient:
    """Enhanced Kafka producer with circuit breaker and advanced features."""
    
    def __init__(self, settings_obj: 'AppSettings', topic_manager_obj: 'AppTopicManager'): # MODIFIED: Accept settings and topic_manager
        self.settings = settings_obj
        self.topic_manager = topic_manager_obj # Store topic_manager instance
        self.config = self._get_default_config()
        self.producer: Optional[KafkaProducer] = None
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.message_batches: Dict[str, MessageBatch] = {}
        self.redis_client: Optional[redis.Redis] = None # Using synchronous redis here
        self.batch_processor_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.started = False
        
    async def start(self):
        """Initialize and start the Kafka producer."""
        if self.started:
            logger.warning("Kafka producer already started")
            return
        
        try:
            self._initialize_producer()
            self._initialize_redis()
            self._start_batch_processor()
            self.started = True
            logger.info("Kafka producer started successfully")
        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}", exc_info=True)
            raise
    
    async def shutdown(self):
        """Shutdown the Kafka producer gracefully."""
        if not self.started:
            logger.info("Kafka producer is not started, skipping shutdown.")
            return
        
        logger.info("Shutting down Kafka producer...")
        self.close() # Calls synchronous close
        self.started = False
        logger.info("Kafka producer shutdown completed")
    
    def _get_default_config(self) -> ProducerConfig:
        """Get default producer configuration from settings."""
        return ProducerConfig(
            bootstrap_servers=self.settings.kafka.bootstrap_servers,
            client_id=f"producer_{uuid.uuid4().hex[:8]}",
            compression_type=self.settings.kafka.compression_type,
            batch_size=self.settings.kafka.batch_size,
            linger_ms=self.settings.kafka.linger_ms,
            acks=self.settings.kafka.acks,
            retries=self.settings.kafka.retries,
            buffer_memory=self.settings.kafka.buffer_memory, # Added missing fields
            max_request_size=self.settings.kafka.max_request_size, # Added missing fields
            retry_backoff_ms=self.settings.kafka.retry_backoff_ms, # Added missing fields
            request_timeout_ms=self.settings.kafka.request_timeout_ms, # Added missing fields
            max_in_flight_requests_per_connection=self.settings.kafka.max_in_flight_requests_per_connection # Added missing fields
        )
    
    def _initialize_producer(self):
        """Initialize Kafka producer with configuration."""
        try:
            producer_config = {
                'bootstrap_servers': self.settings.kafka.bootstrap_servers,
                'client_id': self.config.client_id,
                'compression_type': self.settings.kafka.compression_type,
                'batch_size': self.settings.kafka.batch_size,
                'linger_ms': self.settings.kafka.linger_ms,
                'buffer_memory': self.settings.kafka.buffer_memory,
                'max_request_size': self.settings.kafka.max_request_size,
                'acks': self.settings.kafka.acks,
                'retries': self.settings.kafka.retries,
                'retry_backoff_ms': self.settings.kafka.retry_backoff_ms,
                'request_timeout_ms': self.settings.kafka.request_timeout_ms,
                'max_in_flight_requests_per_connection': self.settings.kafka.max_in_flight_requests_per_connection,
                'value_serializer': lambda v: json.dumps(v, default=str).encode('utf-8') if isinstance(v, (dict, list)) else str(v).encode('utf-8'), # Robust JSON/str serialization
                'key_serializer': lambda k: str(k).encode('utf-8') if k is not None else None
            }
            
            # Add security configuration if needed
            if self.settings.kafka.security_protocol != "PLAINTEXT":
                producer_config.update({
                    'security_protocol': self.settings.kafka.security_protocol,
                    'sasl_mechanism': self.settings.kafka.sasl_mechanism,
                    'sasl_plain_username': self.settings.kafka.sasl_username,
                    'sasl_plain_password': self.settings.kafka.sasl_password,
                })
            
            self.producer = KafkaProducer(**producer_config)
            logger.info("Kafka producer initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}", exc_info=True)
            raise # Re-raise to halt startup if critical
    
    def _initialize_redis(self):
        """Initialize Redis client for caching."""
        try:
            redis_enabled = self.settings.redis.enabled
            
            if redis_enabled:
                self.redis_client = redis.Redis( # Using synchronous redis client here
                    host=self.settings.redis.host,
                    port=self.settings.redis.port,
                    db=self.settings.redis.db,
                    password=self.settings.redis.password,
                    decode_responses=True,
                    socket_timeout=self.settings.redis.socket_timeout,
                    socket_connect_timeout=self.settings.redis.socket_connect_timeout,
                    retry_on_timeout=True,
                    max_connections=self.settings.redis.max_connections
                )
                self.redis_client.ping()
                logger.info("Redis client initialized successfully")
            else:
                logger.info("Redis is disabled in configuration for KafkaProducerClient.")
        except Exception as e:
            logger.warning(f"Failed to initialize Redis client for KafkaProducerClient: {e}", exc_info=True)
            self.redis_client = None
    
    def _start_batch_processor(self):
        """Start background thread for batch processing."""
        self.batch_processor_thread = threading.Thread(
            target=self._process_batches,
            daemon=True
        )
        self.batch_processor_thread.start()
        logger.info("Batch processor thread started")
    
    def _process_batches(self):
        """Background process for handling message batches."""
        # Import TopicType and MessageSchemaValidator here to avoid circular dependencies
        from kafka_handlers.topics import TopicType, MessageSchemaValidator
        
        while not self.stop_event.is_set():
            try:
                ready_topics = []
                
                for topic, batch in self.message_batches.items():
                    if batch.is_ready():
                        ready_topics.append(topic)
                
                for topic in ready_topics:
                    batch = self.message_batches[topic]
                    messages = batch.get_messages()
                    
                    if messages:
                        self._send_batch(topic, messages)
                        producer_batch_size.labels(topic=topic).observe(len(messages))
                
                time.sleep(0.1) # Small sleep to prevent busy-waiting
                
            except Exception as e:
                logger.error(f"Error in batch processor: {e}", exc_info=True)
                time.sleep(1) # Longer sleep on error
    
    def _send_batch(self, topic: str, messages: List[Dict[str, Any]]):
        """Send a batch of messages to Kafka."""
        try:
            if topic not in self.circuit_breakers:
                self.circuit_breakers[topic] = CircuitBreaker(
                    CircuitBreakerConfig(), topic
                )
            
            circuit_breaker = self.circuit_breakers[topic]
            
            def send_messages_sync():
                futures = []
                for message_payload in messages: # message is now message_payload from the batch
                    future = self.producer.send(
                        topic=topic,
                        value=message_payload['value'], # The actual message dictionary
                        key=message_payload.get('key'),
                        partition=message_payload.get('partition'),
                        timestamp_ms=message_payload.get('timestamp_ms')
                    )
                    futures.append(future)
                
                # Wait for all messages to be sent with a timeout
                for future in futures:
                    future.get(timeout=30) # Default to 30 seconds wait
                
                return len(futures)
            
            sent_count = circuit_breaker.call(send_messages_sync)
            
            producer_messages_total.labels(topic=topic, status='success').inc(sent_count)
            logger.debug(f"Sent batch of {sent_count} messages to {topic}")
            
        except Exception as e:
            producer_messages_total.labels(topic=topic, status='error').inc(len(messages))
            logger.error(f"Failed to send batch to {topic}: {e}", exc_info=True)
    
    def _get_circuit_breaker(self, topic: str) -> CircuitBreaker:
        """Get or create circuit breaker for topic."""
        if topic not in self.circuit_breakers:
            self.circuit_breakers[topic] = CircuitBreaker(
                CircuitBreakerConfig(), topic
            )
        return self.circuit_breakers[topic]
    
    def _cache_message(self, cache_key: str, message: Dict[str, Any], ttl: int = 300):
        """Cache message in Redis for deduplication."""
        if self.redis_client:
            try:
                # Use Redis's SET command with EX (expire) to set TTL
                # The `default=str` is important for serializing datetime objects in the message
                self.redis_client.setex(
                    cache_key, 
                    ttl, 
                    json.dumps(message, default=str)
                )
            except Exception as e:
                logger.warning(f"Failed to cache message {cache_key}: {e}", exc_info=True)
    
    def _is_duplicate_message(self, cache_key: str) -> bool:
        """Check if message is duplicate using Redis cache."""
        if self.redis_client:
            try:
                return self.redis_client.exists(cache_key) > 0 # exists returns 0 or 1
            except Exception as e:
                logger.warning(f"Failed to check duplicate for {cache_key}: {e}", exc_info=True)
        return False
    
    def _generate_message_key(self, topic: str, message: Dict[str, Any]) -> str:
        """Generate unique key for message deduplication."""
        # Use a stable serialization for hashing to ensure consistent keys
        content = json.dumps(message, sort_keys=True, default=str)
        hash_obj = hashlib.md5(content.encode())
        return f"msg:{topic}:{hash_obj.hexdigest()}"
    
    def _add_correlation_id(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Add correlation ID for distributed tracing."""
        if 'correlation_id' not in message:
            message['correlation_id'] = str(uuid.uuid4())
        return message
    
    def _validate_message(self, topic_type: 'TopicType', message: Dict[str, Any]) -> bool: # Added type hint
        """Validate message schema."""
        # Import MessageSchemaValidator here to avoid circular dependencies
        from kafka_handlers.topics import MessageSchemaValidator
        
        try:
            if topic_type == topic_type.DETECTION_EVENTS:
                return MessageSchemaValidator.validate_detection_event(message)
            elif topic_type == topic_type.ALERTS:
                return MessageSchemaValidator.validate_alert(message)
            elif topic_type == topic_type.METRICS:
                return MessageSchemaValidator.validate_metric(message)
            else:
                return True  # Skip validation for other message types
        except Exception as e:
            logger.error(f"Message validation failed for topic type {topic_type.value}: {e}", exc_info=True)
            return False
    
    def send_message(self, 
                    topic_type: 'TopicType', # Added type hint
                    message: Dict[str, Any],
                    key: Optional[str] = None,
                    partition: Optional[int] = None,
                    enable_deduplication: bool = True,
                    batch_mode: bool = False) -> bool:
        """
        Send a single message to Kafka topic.
        
        Args:
            topic_type: Type of topic to send to
            message: Message payload
            key: Message key for partitioning
            partition: Specific partition to send to
            enable_deduplication: Whether to check for duplicates
            batch_mode: Whether to use batch processing
        
        Returns:
            bool: True if message was sent successfully
        """
        if not self.started:
            logger.error("Kafka producer not started. Call start() first.")
            return False
            
        try:
            # Get topic name using the stored topic_manager
            topic_name = self.topic_manager.get_topic_name(topic_type)
            
            # Add correlation ID and metadata
            message = self._add_correlation_id(message)
            message['timestamp'] = datetime.utcnow().isoformat()
            message['producer_id'] = self.config.client_id
            
            # Validate message schema
            if not self._validate_message(topic_type, message):
                logger.error(f"Message validation failed for topic {topic_name}. Message: {message}")
                return False
            
            # Check for duplicates
            if enable_deduplication:
                cache_key = self._generate_message_key(topic_name, message)
                if self._is_duplicate_message(cache_key):
                    logger.info(f"Duplicate message detected for topic {topic_name}. Key: {cache_key}")
                    return True
                self._cache_message(cache_key, message, ttl=self.settings.redis.alerts_cache_ttl) # Use settings TTL
            
            # Prepare message for sending (value is the full dict, key is encoded later)
            message_to_send = {
                'value': message, # The actual dictionary message
                'key': key,
                'partition': partition,
                'timestamp_ms': int(time.time() * 1000)
            }

            if batch_mode:
                if topic_name not in self.message_batches:
                    self.message_batches[topic_name] = MessageBatch(
                        max_size=self.settings.kafka.batch_size, # Use Kafka batch size from settings
                        max_wait_time=self.settings.kafka.linger_ms / 1000 # Convert linger_ms to seconds
                    )
                
                self.message_batches[topic_name].add_message(message_to_send)
                return True
            else:
                return self._send_single_message(topic_name, message_to_send) # Pass the prepared dict
                
        except Exception as e:
            logger.error(f"Error sending message to {topic_type.value}: {e}", exc_info=True)
            return False
    
    def _send_single_message(self, topic: str, message_payload: Dict[str, Any]) -> bool: # Renamed message to message_payload
        """Send a single message immediately."""
        try:
            circuit_breaker = self._get_circuit_breaker(topic)
            
            def send_message_sync():
                start_time = time.time()
                
                future = self.producer.send(
                    topic=topic,
                    value=message_payload['value'], # Actual message dict
                    key=message_payload.get('key'),
                    partition=message_payload.get('partition'),
                    timestamp_ms=message_payload.get('timestamp_ms')
                )
                
                record_metadata = future.get(timeout=self.settings.kafka.request_timeout_ms / 1000) # Use settings for timeout
                
                latency = time.time() - start_time
                producer_latency.labels(topic=topic).observe(latency)
                
                return record_metadata
            
            record_metadata = circuit_breaker.call(send_message_sync)
            
            producer_messages_total.labels(topic=topic, status='success').inc()
            
            logger.debug(f"Message sent to {topic} at partition {record_metadata.partition}, offset {record_metadata.offset}")
            return True
            
        except Exception as e:
            producer_messages_total.labels(topic=topic, status='error').inc()
            logger.error(f"Failed to send single message to {topic}: {e}", exc_info=True)
            return False
    
    def send_detection_event(self, detection_data: Dict[str, Any]) -> bool:
        """Send detection event message."""
        # Import TopicType here to avoid circular dependencies
        from kafka_handlers.topics import TopicType
        return self.send_message(
            TopicType.DETECTION_EVENTS,
            detection_data,
            key=detection_data.get('camera_id'),
            batch_mode=True
        )
    
    def send_alert(self, alert_data: Dict[str, Any]) -> bool:
        """Send alert message."""
        # Import TopicType here to avoid circular dependencies
        from kafka_handlers.topics import TopicType
        return self.send_message(
            TopicType.ALERTS,
            alert_data,
            key=alert_data.get('camera_id'),
            batch_mode=False  # Alerts should be sent immediately
        )
    
    def send_metric(self, metric_data: Dict[str, Any]) -> bool:
        """Send metric message."""
        # Import TopicType here to avoid circular dependencies
        from kafka_handlers.topics import TopicType
        return self.send_message(
            TopicType.METRICS,
            metric_data,
            key=metric_data.get('service_name'),
            batch_mode=True
        )
    
    def flush(self, timeout: int = 30) -> bool:
        """Flush all pending messages."""
        try:
            # Process any remaining batches before flushing the producer itself
            for topic, batch in list(self.message_batches.items()): # Iterate over copy
                messages = batch.get_messages()
                if messages:
                    self._send_batch(topic, messages)
                    # Clear the batch from the dict if it's empty
                    if not batch.messages:
                        del self.message_batches[topic]

            # Then flush producer
            if self.producer:
                self.producer.flush(timeout=timeout)
            logger.info("Producer flushed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to flush producer: {e}", exc_info=True)
            return False
    
    async def health_check(self) -> Dict[str, Any]: # MODIFIED: Return Dict for consistency with other health checks
        """Health check for the producer."""
        if not self.started:
            return {'status': 'not_started', 'message': 'Kafka producer not initialized.'}
        
        try:
            # Check if producer is still functional by attempting to connect to bootstrap servers
            # The producer object itself may not expose a direct "is_connected" method
            # A common way is to check cluster metadata or try to send a dummy message.
            # For simplicity, we'll rely on the Kafka-Python internal connection status if available
            # or try a non-blocking poll.

            if self.producer is None:
                 return {'status': 'unhealthy', 'message': 'Producer object is None.'}

            # Check if the internal Kafka client is connected to bootstrap servers (rough check)
            # This is an internal detail, and might not be directly exposed.
            # A more robust check would involve trying to describe the cluster or list topics.
            # Using producer.poll(0) can check connectivity if there's no message to send.
            
            # Attempt to get cluster metadata to verify connection
            # This implicitly uses the producer's internal connection.
            cluster_metadata = self.producer.cluster().brokers()
            if not cluster_metadata:
                raise Exception("No brokers found in cluster metadata.")

            # Check circuit breaker states
            cb_states = {
                topic: cb.state.name for topic, cb in self.circuit_breakers.items()
            }
            overall_cb_status = 'CLOSED'
            if any(state != CircuitBreakerState.CLOSED.name for state in cb_states.values()):
                overall_cb_status = 'DEGRADED' if any(state == CircuitBreakerState.HALF_OPEN.name for state in cb_states.values()) else 'OPEN'

            return {
                'status': 'healthy',
                'message': 'Kafka producer is connected and operational.',
                'brokers_connected': len(cluster_metadata),
                'circuit_breakers': cb_states,
                'overall_circuit_breaker_status': overall_cb_status
            }

        except Exception as e:
            logger.error(f"Producer health check failed: {e}", exc_info=True)
            return {
                'status': 'unhealthy',
                'error': str(e),
                'message': 'Kafka producer connection or internal error.'
            }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics."""
        return {
            'started': self.started,
            'circuit_breakers': {
                topic: {
                    'state': cb.state.name,
                    'failure_count': cb.failure_count,
                    'half_open_calls': cb.half_open_calls
                }
                for topic, cb in self.circuit_breakers.items()
            },
            'active_batches': {
                topic: len(batch.messages)
                for topic, batch in self.message_batches.items()
            },
            'producer_config': {
                'client_id': self.config.client_id,
                'batch_size': self.config.batch_size,
                'linger_ms': self.config.linger_ms,
                'compression_type': self.config.compression_type,
                'acks': self.config.acks, # Include more config details
                'retries': self.config.retries
            }
        }
    
    def close(self):
        """Close producer and cleanup resources."""
        logger.info("Closing Kafka producer...")
        
        # Stop batch processor thread gracefully
        self.stop_event.set()
        if self.batch_processor_thread and self.batch_processor_thread.is_alive():
            self.batch_processor_thread.join(timeout=5) # Give it 5 seconds to finish
            if self.batch_processor_thread.is_alive():
                logger.warning("Batch processor thread did not terminate gracefully.")
        
        # Flush any remaining messages
        self.flush(timeout=5) # Give it 5 seconds to flush

        # Close Kafka producer
        if self.producer:
            self.producer.close(timeout=5) # Also add timeout for close
            logger.info("Kafka producer client closed.")
        
        # Close Redis client
        if self.redis_client:
            self.redis_client.close()
            logger.info("Redis client closed for KafkaProducerClient.")
        
        logger.info("Kafka producer closed completed.")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Global producer instance (will be set in main.py)
_producer_instance: Optional[KafkaProducerClient] = None
_producer_lock = threading.Lock()


def get_kafka_producer(settings: Optional['AppSettings'] = None, topic_manager: Optional['AppTopicManager'] = None) -> KafkaProducerClient: # MODIFIED: Accept settings and topic_manager
    """Get or create global Kafka producer instance (singleton pattern)."""
    global _producer_instance
    
    if _producer_instance is None:
        if settings is None or topic_manager is None:
            # Fallback for early calls, but ideally, settings and topic_manager should be passed.
            # This case means settings are not fully ready or passed in.
            from config.settings import get_settings as _get_global_settings
            from kafka_handlers.topics import get_topic_manager as _get_global_topic_manager
            _settings_fallback = _get_global_settings()
            _topic_manager_fallback = _get_global_topic_manager(_settings_fallback) # Pass settings to topic_manager
            logger.warning("get_kafka_producer called without settings or topic_manager. Using global fallbacks. Ensure proper initialization order.")
            settings = _settings_fallback
            topic_manager = _topic_manager_fallback

        with _producer_lock:
            if _producer_instance is None:
                _producer_instance = KafkaProducerClient(settings, topic_manager) # MODIFIED: Pass settings and topic_manager here
    
    return _producer_instance


def cleanup_producer():
    """Cleanup global producer instance."""
    global _producer_instance
    
    if _producer_instance:
        _producer_instance.close()
        _producer_instance = None


# Convenience functions for different message types
def send_detection_event(detection_data: Dict[str, Any]) -> bool:
    """Send detection event to Kafka."""
    # This will use the global producer instance, which should be initialized.
    producer = get_kafka_producer() 
    return producer.send_detection_event(detection_data)


def send_alert(alert_data: Dict[str, Any]) -> bool:
    """Send alert to Kafka."""
    producer = get_kafka_producer()
    return producer.send_alert(alert_data)


def send_metric(metric_data: Dict[str, Any]) -> bool:
    """Send metric to Kafka."""
    producer = get_kafka_producer()
    return producer.send_metric(metric_data)


if __name__ == "__main__":
    # This block is for testing producer.py directly.
    # It needs settings and topic_manager to be initialized.
    from dotenv import load_dotenv
    from config.settings import get_settings as _get_app_settings
    from kafka_handlers.topics import get_topic_manager as _get_app_topic_manager, TopicType

    print("--- Running KafkaProducerClient standalone test ---")
    
    load_dotenv() # Load .env for standalone test
    test_settings = _get_app_settings() # Get settings
    test_topic_manager = _get_app_topic_manager(test_settings) # Get topic manager, passing settings

    # Manually ensure topics exist for testing producer
    print("Ensuring Kafka topics exist for testing...")
    if not test_topic_manager.create_all_topics():
        print("WARNING: Failed to ensure all topics exist. Producer tests might fail.")
    else:
        print("Topics ready.")

    # Get producer instance (it will use the test_settings and test_topic_manager)
    producer_test_instance = get_kafka_producer(settings=test_settings, topic_manager=test_topic_manager)
    
    try:
        # Start the producer
        asyncio.run(producer_test_instance.start()) # start is async

        # Test detection event
        detection_event = {
            'detection_id': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat(),
            'camera_id': 'camera_001',
            'objects': [
                {
                    'class': 'person',
                    'confidence': 0.95,
                    'bbox': [100, 100, 200, 300],
                    'tracking_id': 'track_001'
                }
            ],
            'frame_id': 'frame_12345',
            'confidence': 0.95,
            'processing_time': 0.15
        }
        
        # Test alert
        alert = {
            'alert_id': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat(),
            'camera_id': 'camera_001',
            'alert_type': 'intrusion',
            'severity': 'high',
            'message': 'Person detected in restricted area',
            'correlation_id': str(uuid.uuid4())
        }
        
        # Test metric
        metric = {
            'metric_name': 'detection_latency',
            'value': 0.15,
            'timestamp': datetime.utcnow().isoformat(),
            'service_name': 'detection_service'
        }
        
        print("Sending detection event:", producer_test_instance.send_detection_event(detection_event))
        print("Sending alert:", producer_test_instance.send_alert(alert))
        print("Sending metric:", producer_test_instance.send_metric(metric))

        print("Flushing producer...")
        producer_test_instance.flush()
        print("Producer flushed.")

        print("Checking producer health:")
        health_status = asyncio.run(producer_test_instance.health_check()) # health_check is async
        print(f"Producer health: {health_status}")

    except Exception as e:
        print(f"An error occurred during producer test: {e}", file=sys.stderr)
    finally:
        # Cleanup
        asyncio.run(producer_test_instance.shutdown()) # shutdown is async
        print("Producer test completed")