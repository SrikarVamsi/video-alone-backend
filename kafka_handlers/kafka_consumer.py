# """
# Kafka Consumer with Advanced Resilience Features

# This module provides:
# - Robust Kafka message consumption with error handling
# - Automatic retry mechanisms with exponential backoff
# - Dead letter queue support for failed messages
# - Message deduplication using Redis
# - Graceful shutdown and reconnection handling
# - Metrics collection and health monitoring
# - Correlation ID tracking for distributed tracing
# - Batch processing capabilities
# """

# # import json
# # import time
# # import uuid
# # import asyncio
# # import signal
# # import threading
# # from typing import Dict, List, Optional, Any, Callable, Set
# # from dataclasses import dataclass
# # from datetime import datetime, timedelta
# # from enum import Enum
# # import hashlib
# # from queue import Queue, Empty
# # import traceback

# # from kafka import KafkaConsumer, TopicPartition
# # from kafka.errors import KafkaError, KafkaTimeoutError, CommitFailedError
# # from kafka.structs import ConsumerRecord
# # import redis
# # from prometheus_client import Counter, Histogram, Gauge, Summary

# # from config.settings import get_settings
# # from utils.logger import get_logger, LogContext
# # from kafka_handlers.topics import  TopicType, TopicManager, MessageSchemaValidator
# # from kafka_handlers.topics import TopicType, TopicManager, MessageSchemaValidator
# # from kafka_handlers.kafka_producer import get_kafka_producer

# """
# Kafka Consumer with Advanced Resilience Features
# """

# import json
# import time
# import uuid
# import asyncio
# import signal
# import threading
# from typing import Dict, List, Optional, Any, Callable, Set
# from dataclasses import dataclass
# from datetime import datetime, timedelta
# from enum import Enum
# import hashlib
# from queue import Queue, Empty
# import traceback

# # Import Kafka components based on your library
# import kafka as kafka_lib
# KafkaConsumer = kafka_lib.KafkaConsumer
# TopicPartition = kafka_lib.TopicPartition

# # Handle different kafka libraries
# try:
#     from kafka.errors import KafkaError, KafkaTimeoutError, CommitFailedError
# except ImportError:
#     # Fallback for different kafka library
#     from kafka.errors import KafkaError, KafkaTimeoutError
#     CommitFailedError = KafkaError

# # Remove ConsumerRecord import - it's returned by the consumer automatically
# # ConsumerRecord objects are namedtuples with: topic, partition, offset, key, value, timestamp

# import redis
# from prometheus_client import Counter, Histogram, Gauge, Summary

# from config.settings import get_settings
# from utils.logger import get_logger, LogContext
# from kafka_handlers.topics import TopicType, TopicManager, MessageSchemaValidator
# from kafka_handlers.kafka_producer import get_kafka_producer

# # Initialize components
# logger = get_logger(__name__)
# settings = get_settings()
# topic_manager = TopicManager()

# # Prometheus metrics
# consumer_messages_total = Counter(
#     'kafka_consumer_messages_total',
#     'Total messages consumed from Kafka',
#     ['topic', 'status', 'consumer_group']
# )
# consumer_lag = Gauge(
#     'kafka_consumer_lag',
#     'Consumer lag in messages',
#     ['topic', 'partition', 'consumer_group']
# )
# consumer_processing_time = Histogram(
#     'kafka_consumer_processing_time_seconds',
#     'Time spent processing messages',
#     ['topic', 'consumer_group']
# )
# consumer_errors_total = Counter(
#     'kafka_consumer_errors_total',
#     'Total consumer errors',
#     ['topic', 'error_type', 'consumer_group']
# )
# consumer_retries_total = Counter(
#     'kafka_consumer_retries_total',
#     'Total message processing retries',
#     ['topic', 'consumer_group']
# )


# class MessageStatus(Enum):
#     """Message processing status"""
#     PENDING = "pending"
#     PROCESSING = "processing"
#     COMPLETED = "completed"
#     FAILED = "failed"
#     RETRY = "retry"
#     DEAD_LETTER = "dead_letter"


# @dataclass
# class ConsumerConfig:
#     """Configuration for Kafka consumer"""
#     bootstrap_servers: str
#     group_id: str
#     client_id: str
#     auto_offset_reset: str = 'earliest'
#     enable_auto_commit: bool = False
#     max_poll_records: int = 500
#     max_poll_interval_ms: int = 300000
#     session_timeout_ms: int = 30000
#     heartbeat_interval_ms: int = 3000
#     fetch_min_bytes: int = 1
#     fetch_max_wait_ms: int = 500
#     max_partition_fetch_bytes: int = 1048576
#     isolation_level: str = 'read_committed'
#     consumer_timeout_ms: int = 1000


# @dataclass
# class RetryConfig:
#     """Configuration for retry mechanism"""
#     max_retries: int = 3
#     initial_delay: float = 1.0
#     max_delay: float = 60.0
#     backoff_multiplier: float = 2.0
#     jitter: bool = True


# @dataclass
# class ProcessingResult:
#     """Result of message processing"""
#     success: bool
#     message: str
#     retry_count: int = 0
#     error: Optional[Exception] = None
#     processing_time: float = 0.0
#     correlation_id: Optional[str] = None


# class MessageProcessor:
#     """Base class for message processors"""
    
#     def __init__(self, topic_type: TopicType):
#         self.topic_type = topic_type
#         self.logger = get_logger(f"{__name__}.{self.__class__.__name__}")
    
#     def process(self, message: Dict[str, Any], record: Any) -> ProcessingResult:
#         """Process a message - override in subclasses"""
#         raise NotImplementedError("Subclasses must implement process method")
    
#     def validate_message(self, message: Dict[str, Any]) -> bool:
#         """Validate message schema"""
#         try:
#             if self.topic_type == TopicType.DETECTION_EVENTS:
#                 return MessageSchemaValidator.validate_detection_event(message)
#             elif self.topic_type == TopicType.ALERTS:
#                 return MessageSchemaValidator.validate_alert(message)
#             elif self.topic_type == TopicType.METRICS:
#                 return MessageSchemaValidator.validate_metric(message)
#             else:
#                 return True
#         except Exception as e:
#             self.logger.error(f"Message validation failed: {e}")
#             return False


# class DeadLetterQueue:
#     """Dead letter queue for failed messages"""
    
#     def __init__(self, redis_client: Optional[redis.Redis] = None):
#         self.redis_client = redis_client
#         self.producer = get_kafka_producer()
    
#     def send_to_dlq(self, 
#                    original_topic: str, 
#                    message: Dict[str, Any], 
#                    error: Exception,
#                    retry_count: int):
#         """Send message to dead letter queue"""
#         try:
#             dlq_message = {
#                 'original_topic': original_topic,
#                 'original_message': message,
#                 'error': str(error),
#                 'error_type': type(error).__name__,
#                 'retry_count': retry_count,
#                 'timestamp': datetime.utcnow().isoformat(),
#                 'correlation_id': message.get('correlation_id', str(uuid.uuid4()))
#             }
            
#             # Send to Kafka DLQ topic
#             dlq_topic = f"{original_topic}_dlq"
#             success = self.producer.send_message(
#                 TopicType.SYSTEM_EVENTS,  # Use system events topic as DLQ
#                 dlq_message,
#                 key=f"dlq_{original_topic}",
#                 batch_mode=False
#             )
            
#             if success:
#                 logger.info(f"Message sent to DLQ: {dlq_topic}")
#             else:
#                 logger.error(f"Failed to send message to DLQ: {dlq_topic}")
            
#             # Also store in Redis for monitoring
#             if self.redis_client:
#                 self._store_in_redis(dlq_topic, dlq_message)
                
#         except Exception as e:
#             logger.error(f"Failed to send message to DLQ: {e}")
    
#     def _store_in_redis(self, dlq_topic: str, message: Dict[str, Any]):
#         """Store DLQ message in Redis for monitoring"""
#         try:
#             key = f"dlq:{dlq_topic}:{message['correlation_id']}"
#             self.redis_client.setex(
#                 key,
#                 86400,  # 24 hours TTL
#                 json.dumps(message, default=str)
#             )
            
#             # Add to DLQ list
#             list_key = f"dlq_list:{dlq_topic}"
#             self.redis_client.lpush(list_key, key)
#             self.redis_client.ltrim(list_key, 0, 1000)  # Keep only last 1000 entries
            
#         except Exception as e:
#             logger.warning(f"Failed to store DLQ message in Redis: {e}")


# class MessageRetryHandler:
#     """Handles message retry logic with exponential backoff"""
    
#     def __init__(self, config: RetryConfig):
#         self.config = config
#         self.retry_queues: Dict[str, Queue] = {}
#         self.retry_threads: Dict[str, threading.Thread] = {}
#         self.stop_event = threading.Event()
    
#     def should_retry(self, retry_count: int, error: Exception) -> bool:
#         """Determine if message should be retried"""
#         if retry_count >= self.config.max_retries:
#             return False
        
#         # Don't retry certain types of errors
#         non_retryable_errors = (ValueError, TypeError, KeyError)
#         if isinstance(error, non_retryable_errors):
#             return False
        
#         return True
    
#     def calculate_delay(self, retry_count: int) -> float:
#         """Calculate retry delay with exponential backoff"""
#         delay = self.config.initial_delay * (self.config.backoff_multiplier ** retry_count)
#         delay = min(delay, self.config.max_delay)
        
#         if self.config.jitter:
#             import random
#             delay = delay * (0.5 + random.random() * 0.5)
        
#         return delay
    
#     def schedule_retry(self, 
#                       topic: str,
#                       message: Dict[str, Any],
#                       record: Any,
#                       processor: MessageProcessor,
#                       retry_count: int,
#                       error: Exception):
#         """Schedule message for retry"""
#         if topic not in self.retry_queues:
#             self.retry_queues[topic] = Queue()
#             self._start_retry_thread(topic)
        
#         delay = self.calculate_delay(retry_count)
#         retry_time = time.time() + delay
        
#         retry_item = {
#             'retry_time': retry_time,
#             'message': message,
#             'record': record,
#             'processor': processor,
#             'retry_count': retry_count,
#             'error': error
#         }
        
#         self.retry_queues[topic].put(retry_item)
        
#         logger.info(f"Scheduled retry for message in {delay:.2f}s (attempt {retry_count + 1})")
    
#     def _start_retry_thread(self, topic: str):
#         """Start retry processing thread for topic"""
#         def retry_processor():
#             retry_queue = self.retry_queues[topic]
            
#             while not self.stop_event.is_set():
#                 try:
#                     retry_item = retry_queue.get(timeout=1)
                    
#                     # Wait for retry time
#                     wait_time = retry_item['retry_time'] - time.time()
#                     if wait_time > 0:
#                         time.sleep(wait_time)
                    
#                     # Process the message again
#                     processor = retry_item['processor']
#                     result = processor.process(retry_item['message'], retry_item['record'])
                    
#                     if result.success:
#                         logger.info(f"Message retry successful after {retry_item['retry_count']} attempts")
#                     else:
#                         logger.error(f"Message retry failed after {retry_item['retry_count']} attempts")
                    
#                     retry_queue.task_done()
                    
#                 except Empty:
#                     continue
#                 except Exception as e:
#                     logger.error(f"Error in retry processor: {e}")
        
#         thread = threading.Thread(target=retry_processor, daemon=True)
#         thread.start()
#         self.retry_threads[topic] = thread
#         logger.info(f"Started retry thread for topic: {topic}")
    
#     def stop(self):
#         """Stop all retry threads"""
#         self.stop_event.set()
#         for thread in self.retry_threads.values():
#             thread.join(timeout=5)


# class KafkaConsumerClient:
#     """Enhanced Kafka consumer with resilience features"""
    
#     def __init__(self, 
#                  topic_type: TopicType,
#                  processor: MessageProcessor,
#                  config: Optional[ConsumerConfig] = None):
#         self.topic_type = topic_type
#         self.processor = processor
#         self.config = config or self._get_default_config()
#         self.consumer = None
#         self.redis_client = None
#         self.dlq = None
#         self.retry_handler = None
#         self.processed_messages: Set[str] = set()
#         self.is_running = False
#         self.stop_event = threading.Event()
#         self.metrics_thread = None
        
#         self._initialize_consumer()
#         self._initialize_redis()
#         self._initialize_components()
    
#     def _get_default_config(self) -> ConsumerConfig:
#         """Get default consumer configuration"""
#         return ConsumerConfig(
#             bootstrap_servers=settings.kafka.bootstrap_servers,
#             group_id=f"{self.topic_type.value}_consumer_group",
#             client_id=f"consumer_{self.topic_type.value}_{uuid.uuid4().hex[:8]}",
#             auto_offset_reset=settings.kafka.auto_offset_reset,
#             enable_auto_commit=False,
#             max_poll_records=settings.kafka.max_poll_records,
#             consumer_timeout_ms=settings.kafka.consumer_timeout_ms
#         )
    
#     def _initialize_consumer(self):
#         """Initialize Kafka consumer with proper configuration"""
#         try:
#             consumer_config = {
#                 'bootstrap_servers': self.config.bootstrap_servers.split(','),
#                 'group_id': self.config.group_id,
#                 'client_id': self.config.client_id,
#                 'auto_offset_reset': self.config.auto_offset_reset,
#                 'enable_auto_commit': self.config.enable_auto_commit,
#                 'max_poll_records': self.config.max_poll_records,
#                 'max_poll_interval_ms': self.config.max_poll_interval_ms,
#                 'session_timeout_ms': self.config.session_timeout_ms,
#                 'heartbeat_interval_ms': self.config.heartbeat_interval_ms,
#                 'fetch_min_bytes': self.config.fetch_min_bytes,
#                 'fetch_max_wait_ms': self.config.fetch_max_wait_ms,
#                 'max_partition_fetch_bytes': self.config.max_partition_fetch_bytes,
#                 'isolation_level': self.config.isolation_level,
#                 'consumer_timeout_ms': self.config.consumer_timeout_ms,
#                 'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
#                 'key_deserializer': lambda x: x.decode('utf-8') if x else None,
#                 'security_protocol': 'PLAINTEXT',  # Update as needed
#             }
            
#             self.consumer = KafkaConsumer(**consumer_config)
#             logger.info(f"Kafka consumer initialized for topic: {self.topic_type.value}")
            
#         except Exception as e:
#             logger.error(f"Failed to initialize Kafka consumer: {e}")
#             raise
    
#     def _initialize_redis(self):
#         """Initialize Redis client for deduplication"""
#         try:
#             if hasattr(settings, 'redis') and settings.redis.enabled:
#                 self.redis_client = redis.Redis(
#                     host=settings.redis.host,
#                     port=settings.redis.port,
#                     db=settings.redis.db,
#                     decode_responses=True,
#                     socket_timeout=5,
#                     socket_connect_timeout=5,
#                     retry_on_timeout=True,
#                     health_check_interval=30
#                 )
#                 # Test connection
#                 self.redis_client.ping()
#                 logger.info("Redis client initialized successfully")
#         except Exception as e:
#             logger.warning(f"Failed to initialize Redis client: {e}")
#             self.redis_client = None
    
#     def _initialize_components(self):
#         """Initialize supporting components"""
#         self.dlq = DeadLetterQueue(self.redis_client)
#         self.retry_handler = MessageRetryHandler(RetryConfig())
#         self._start_metrics_collection()
    
#     def _start_metrics_collection(self):
#         """Start metrics collection thread"""
#         def collect_metrics():
#             while not self.stop_event.is_set():
#                 try:
#                     if self.consumer:
#                         self._collect_consumer_metrics()
#                     time.sleep(30)  # Collect metrics every 30 seconds
#                 except Exception as e:
#                     logger.error(f"Error collecting metrics: {e}")
        
#         self.metrics_thread = threading.Thread(target=collect_metrics, daemon=True)
#         self.metrics_thread.start()
    
#     def _collect_consumer_metrics(self):
#         """Collect consumer lag and other metrics"""
#         try:
#             for partition in self.consumer.assignment():
#                 committed = self.consumer.committed(partition)
#                 position = self.consumer.position(partition)
                
#                 if committed is not None and position is not None:
#                     lag = position - committed
#                     consumer_lag.labels(
#                         topic=partition.topic,
#                         partition=partition.partition,
#                         consumer_group=self.config.group_id
#                     ).set(lag)
#         except Exception as e:
#             logger.error(f"Error collecting consumer metrics: {e}")
    
#     def _is_duplicate_message(self, message: Dict[str, Any]) -> bool:
#         """Check if message is duplicate using Redis"""
#         if not self.redis_client:
#             return False
        
#         try:
#             correlation_id = message.get('correlation_id')
#             if not correlation_id:
#                 return False
            
#             key = f"processed:{self.topic_type.value}:{correlation_id}"
#             if self.redis_client.exists(key):
#                 return True
            
#             # Mark as processed with TTL
#             self.redis_client.setex(key, 3600, "1")  # 1 hour TTL
#             return False
            
#         except Exception as e:
#             logger.error(f"Error checking duplicate message: {e}")
#             return False
    
#     def _generate_correlation_id(self, record: Any) -> str:
#         """Generate correlation ID for message"""
#         data = f"{record.topic}:{record.partition}:{record.offset}:{record.timestamp}"
#         return hashlib.md5(data.encode()).hexdigest()
    
#     def _process_message(self, record: Any) -> ProcessingResult:
#         """Process a single message with error handling"""
#         start_time = time.time()
#         correlation_id = None
        
#         try:
#             # Parse message
#             message = record.value
#             correlation_id = message.get('correlation_id', self._generate_correlation_id(record))
            
#             # Add correlation ID to log context
#             with LogContext(correlation_id=correlation_id):
#                 # Check for duplicates
#                 if self._is_duplicate_message(message):
#                     logger.info(f"Duplicate message ignored: {correlation_id}")
#                     return ProcessingResult(
#                         success=True,
#                         message="Duplicate message ignored",
#                         correlation_id=correlation_id,
#                         processing_time=time.time() - start_time
#                     )
                
#                 # Validate message schema
#                 if not self.processor.validate_message(message):
#                     raise ValueError(f"Invalid message schema for topic {self.topic_type.value}")
                
#                 # Process message
#                 result = self.processor.process(message, record)
#                 result.correlation_id = correlation_id
#                 result.processing_time = time.time() - start_time
                
#                 # Update metrics
#                 status = "success" if result.success else "error"
#                 consumer_messages_total.labels(
#                     topic=record.topic,
#                     status=status,
#                     consumer_group=self.config.group_id
#                 ).inc()
                
#                 consumer_processing_time.labels(
#                     topic=record.topic,
#                     consumer_group=self.config.group_id
#                 ).observe(result.processing_time)
                
#                 return result
                
#         except Exception as e:
#             processing_time = time.time() - start_time
#             logger.error(f"Error processing message: {e}", exc_info=True)
            
#             # Update error metrics
#             consumer_errors_total.labels(
#                 topic=record.topic,
#                 error_type=type(e).__name__,
#                 consumer_group=self.config.group_id
#             ).inc()
            
#             return ProcessingResult(
#                 success=False,
#                 message=f"Processing failed: {str(e)}",
#                 error=e,
#                 correlation_id=correlation_id,
#                 processing_time=processing_time
#             )
    
#     def _handle_processing_result(self, result: ProcessingResult, record: Any):
#         """Handle the result of message processing"""
#         if result.success:
#             logger.info(f"Message processed successfully: {result.correlation_id}")
#             try:
#                 # Commit offset
#                 self.consumer.commit_async({
#                     TopicPartition(record.topic, record.partition): record.offset + 1
#                 })
#             except CommitFailedError as e:
#                 logger.error(f"Failed to commit offset: {e}")
#         else:
#             logger.error(f"Message processing failed: {result.message}")
            
#             # Handle retry logic
#             if self.retry_handler.should_retry(result.retry_count, result.error):
#                 consumer_retries_total.labels(
#                     topic=record.topic,
#                     consumer_group=self.config.group_id
#                 ).inc()
                
#                 self.retry_handler.schedule_retry(
#                     record.topic,
#                     record.value,
#                     record,
#                     self.processor,
#                     result.retry_count + 1,
#                     result.error
#                 )
#             else:
#                 # Send to dead letter queue
#                 self.dlq.send_to_dlq(
#                     record.topic,
#                     record.value,
#                     result.error,
#                     result.retry_count
#                 )
    
#     def subscribe(self, topics: List[str] = None):
#         """Subscribe to topics"""
#         try:
#             if topics is None:
#                 topics = [self.topic_type.value]
            
#             self.consumer.subscribe(topics)
#             logger.info(f"Subscribed to topics: {topics}")
            
#         except Exception as e:
#             logger.error(f"Failed to subscribe to topics: {e}")
#             raise
    
#     def start_consuming(self):
#         """Start consuming messages"""
#         if self.is_running:
#             logger.warning("Consumer is already running")
#             return
        
#         self.is_running = True
#         logger.info(f"Starting consumer for topic: {self.topic_type.value}")
        
#         # Subscribe to topic
#         self.subscribe()
        
#         # Set up signal handlers for graceful shutdown
#         def signal_handler(signum, frame):
#             logger.info(f"Received signal {signum}, shutting down gracefully...")
#             self.stop()
        
#         signal.signal(signal.SIGINT, signal_handler)
#         signal.signal(signal.SIGTERM, signal_handler)
        
#         try:
#             while self.is_running and not self.stop_event.is_set():
#                 try:
#                     # Poll for messages
#                     message_batch = self.consumer.poll(
#                         timeout_ms=1000,
#                         max_records=self.config.max_poll_records
#                     )
                    
#                     if not message_batch:
#                         continue
                    
#                     # Process messages
#                     for topic_partition, records in message_batch.items():
#                         for record in records:
#                             if self.stop_event.is_set():
#                                 break
                            
#                             result = self._process_message(record)
#                             self._handle_processing_result(result, record)
                    
#                 except KafkaTimeoutError:
#                     logger.debug("Kafka poll timeout, continuing...")
#                     continue
#                 except KafkaError as e:
#                     logger.error(f"Kafka error: {e}")
#                     time.sleep(5)  # Wait before retrying
#                 except Exception as e:
#                     logger.error(f"Unexpected error in consumer loop: {e}", exc_info=True)
#                     time.sleep(5)
        
#         except KeyboardInterrupt:
#             logger.info("Consumer interrupted by user")
#         finally:
#             self.stop()
    
#     def stop(self):
#         """Stop the consumer gracefully"""
#         if not self.is_running:
#             return
        
#         logger.info("Stopping consumer...")
#         self.is_running = False
#         self.stop_event.set()
        
#         # Stop retry handler
#         if self.retry_handler:
#             self.retry_handler.stop()
        
#         # Close consumer
#         if self.consumer:
#             try:
#                 self.consumer.close()
#                 logger.info("Kafka consumer closed")
#             except Exception as e:
#                 logger.error(f"Error closing consumer: {e}")
        
#         # Close Redis connection
#         if self.redis_client:
#             try:
#                 self.redis_client.close()
#                 logger.info("Redis connection closed")
#             except Exception as e:
#                 logger.error(f"Error closing Redis connection: {e}")
        
#         logger.info("Consumer stopped successfully")
    
#     def get_health_status(self) -> Dict[str, Any]:
#         """Get consumer health status"""
#         return {
#             'is_running': self.is_running,
#             'topic_type': self.topic_type.value,
#             'consumer_group': self.config.group_id,
#             'assignment': [str(tp) for tp in self.consumer.assignment()] if self.consumer else [],
#             'redis_connected': self.redis_client is not None and self._test_redis_connection(),
#             'last_poll_time': time.time(),
#             'processed_messages_count': len(self.processed_messages)
#         }
    
#     def _test_redis_connection(self) -> bool:
#         """Test Redis connection"""
#         try:
#             if self.redis_client:
#                 self.redis_client.ping()
#                 return True
#         except:
#             pass
#         return False


# # Specific message processors for different topic types
# class DetectionEventProcessor(MessageProcessor):
#     """Process detection events from Team A"""
    
#     def __init__(self):
#         super().__init__(TopicType.DETECTION_EVENTS)
    
#     def process(self, message: Dict[str, Any], record: Any) -> ProcessingResult:
#         """Process detection event message"""
#         try:
#             # Extract detection information
#             detection_type = message.get('detection_type')
#             camera_id = message.get('camera_id')
#             confidence = message.get('confidence', 0.0)
#             timestamp = message.get('timestamp')
            
#             self.logger.info(f"Processing detection: {detection_type} from camera {camera_id}")
            
#             # Here you would typically:
#             # 1. Store detection in database
#             # 2. Apply business rules
#             # 3. Generate alerts if needed
#             # 4. Update statistics
            
#             # For now, just log the detection
#             self.logger.info(f"Detection processed: {detection_type} (confidence: {confidence})")
            
#             return ProcessingResult(
#                 success=True,
#                 message=f"Detection event processed successfully: {detection_type}"
#             )
            
#         except Exception as e:
#             return ProcessingResult(
#                 success=False,
#                 message=f"Failed to process detection event: {str(e)}",
#                 error=e
#             )


# class AlertProcessor(MessageProcessor):
#     """Process alert messages"""
    
#     def __init__(self):
#         super().__init__(TopicType.ALERTS)
    
#     def process(self, message: Dict[str, Any], record: Any) -> ProcessingResult:
#         """Process alert message"""
#         try:
#             alert_type = message.get('alert_type')
#             camera_id = message.get('camera_id')
#             severity = message.get('severity', 'medium')
            
#             self.logger.info(f"Processing alert: {alert_type} from camera {camera_id}")
            
#             # Here you would typically:
#             # 1. Store alert in database
#             # 2. Send notifications
#             # 3. Update dashboards
#             # 4. Trigger workflows
            
#             return ProcessingResult(
#                 success=True,
#                 message=f"Alert processed successfully: {alert_type}"
#             )
            
#         except Exception as e:
#             return ProcessingResult(
#                 success=False,
#                 message=f"Failed to process alert: {str(e)}",
#                 error=e
#             )


# # Factory function to create consumers
# def create_consumer(topic_type: TopicType, 
#                    processor: Optional[MessageProcessor] = None,
#                    config: Optional[ConsumerConfig] = None) -> KafkaConsumerClient:
#     """Factory function to create configured consumers"""
    
#     if processor is None:
#         if topic_type == TopicType.DETECTION_EVENTS:
#             processor = DetectionEventProcessor()
#         elif topic_type == TopicType.ALERTS:
#             processor = AlertProcessor()
#         else:
#             raise ValueError(f"No default processor for topic type: {topic_type}")
    
#     return KafkaConsumerClient(topic_type, processor, config)


# # Async consumer for modern Python applications
# class AsyncKafkaConsumer:
#     """Async wrapper for Kafka consumer"""
    
#     def __init__(self, topic_type: TopicType, processor: MessageProcessor):
#         self.topic_type = topic_type
#         self.processor = processor
#         self.consumer_client = None
#         self.consumer_task = None
    
#     async def start(self):
#         """Start async consumer"""
#         self.consumer_client = create_consumer(self.topic_type, self.processor)
        
#         # Run consumer in thread pool
#         loop = asyncio.get_event_loop()
#         self.consumer_task = loop.run_in_executor(
#             None, self.consumer_client.start_consuming
#         )
    
#     async def stop(self):
#         """Stop async consumer"""
#         if self.consumer_client:
#             self.consumer_client.stop()
        
#         if self.consumer_task:
#             self.consumer_task.cancel()
#             try:
#                 await self.consumer_task
#             except asyncio.CancelledError:
#                 pass


# # Main execution
# if __name__ == "__main__":
#     # Example usage
#     try:
#         # Create consumer for detection events
#         consumer = create_consumer(TopicType.DETECTION_EVENTS)
        
#         # Start consuming
#         consumer.start_consuming()
        
#     except KeyboardInterrupt:
#         logger.info("Consumer stopped by user")
#     except Exception as e:
#         logger.error(f"Consumer failed: {e}", exc_info=True)
#     finally:
#         logger.info("Consumer shutdown complete")

# """
# Kafka Consumer with Advanced Resilience Features

# This module provides:
# - Robust Kafka message consumption with error handling
# - Automatic retry mechanisms with exponential backoff
# - Dead letter queue support for failed messages
# - Message deduplication using Redis
# - Graceful shutdown and reconnection handling
# - Metrics collection and health monitoring
# - Correlation ID tracking for distributed tracing
# - Batch processing capabilities
# """

# import json
# import time
# import uuid
# import asyncio
# import signal
# import threading
# from typing import Dict, List, Optional, Any, Callable, Set, TYPE_CHECKING
# from dataclasses import dataclass, field # Added field for dataclass defaults
# from datetime import datetime, timedelta
# from enum import Enum
# import hashlib
# from queue import Queue, Empty
# import traceback
# import sys # For critical error printing if logger isn't ready
# import redis
# from prometheus_client import Counter, Histogram, Gauge, Summary 

# # Import Kafka components based on your library
# import kafka as kafka_lib
# KafkaConsumer = kafka_lib.KafkaConsumer
# TopicPartition = kafka_lib.TopicPartition

# # Handle different kafka libraries for error types
# try:
#     from kafka.errors import KafkaError, KafkaTimeoutError, CommitFailedError
# except ImportError:
#     from kafka.errors import KafkaError, KafkaTimeoutError
#     # Define a fallback for CommitFailedError if it's not a distinct class
#     class CommitFailedError(KafkaError):
#         pass

# # Type hinting for settings and other managers to avoid runtime import issues
# if TYPE_CHECKING:
#     from config.settings import Settings as AppSettings
#     from kafka_handlers.topics import TopicType as AppTopicType # Renamed to avoid conflict
#     from kafka_handlers.topics import TopicManager as AppTopicManager
#     from kafka_handlers.topics import MessageSchemaValidator as AppMessageSchemaValidator
#     from kafka_handlers.kafka_producer import KafkaProducerClient as AppKafkaProducerClient
#     from alerts.alert_logic import AlertEngine as AppAlertEngine


# # Removed module-level get_settings() to prevent early loading issues
# # from config.settings import get_settings

# # Get logger (it will initialize with basic config if settings are not fully loaded yet)
# from utils.logger import get_logger, LogContext

# # Removed module-level get_settings() and topic_manager = TopicManager()
# # These dependencies will be passed into the consumer client.
# # from kafka_handlers.topics import TopicType, TopicManager, MessageSchemaValidator
# # from kafka_handlers.kafka_producer import get_kafka_producer


# # Initialize logger (it will get a basic logger first, then get reconfigured later by main.py)
# logger = get_logger(__name__)


# # Prometheus metrics
# consumer_messages_total = Counter(
#     'kafka_consumer_messages_total',
#     'Total messages consumed from Kafka',
#     ['topic', 'status', 'consumer_group']
# )
# consumer_lag = Gauge(
#     'kafka_consumer_lag',
#     'Consumer lag in messages',
#     ['topic', 'partition', 'consumer_group']
# )
# consumer_processing_time = Histogram(
#     'kafka_consumer_processing_time_seconds',
#     'Time spent processing messages',
#     ['topic', 'consumer_group']
# )
# consumer_errors_total = Counter(
#     'kafka_consumer_errors_total',
#     'Total consumer errors',
#     ['topic', 'error_type', 'consumer_group']
# )
# consumer_retries_total = Counter(
#     'kafka_consumer_retries_total',
#     'Total message processing retries',
#     ['topic', 'consumer_group']
# )


# class MessageStatus(Enum):
#     """Message processing status"""
#     PENDING = "pending"
#     PROCESSING = "processing"
#     COMPLETED = "completed"
#     FAILED = "failed"
#     RETRY = "retry"
#     DEAD_LETTER = "dead_letter"


# @dataclass
# class ConsumerConfig:
#     """Configuration for Kafka consumer"""
#     bootstrap_servers: str
#     group_id: str
#     client_id: str
#     auto_offset_reset: str = 'latest' # Changed default from 'earliest' to 'latest'
#     enable_auto_commit: bool = False
#     max_poll_records: int = 500
#     max_poll_interval_ms: int = 300000 # 5 minutes
#     session_timeout_ms: int = 30000 # 30 seconds
#     heartbeat_interval_ms: int = 3000 # 3 seconds
#     fetch_min_bytes: int = 1
#     fetch_max_wait_ms: int = 500
#     max_partition_fetch_bytes: int = 1048576 # 1 MB
#     isolation_level: str = 'read_uncommitted' # Changed from 'read_committed' if not using transactions
#     consumer_timeout_ms: int = 1000 # Consumer poll timeout
    
#     # Security protocol attributes (for direct KafkaConsumer init)
#     security_protocol: str = 'PLAINTEXT'
#     sasl_mechanism: Optional[str] = None
#     sasl_username: Optional[str] = None
#     sasl_password: Optional[str] = None


# @dataclass
# class RetryConfig:
#     """Configuration for retry mechanism"""
#     max_retries: int = 3
#     initial_delay: float = 1.0
#     max_delay: float = 60.0
#     backoff_multiplier: float = 2.0
#     jitter: bool = True


# @dataclass
# class ProcessingResult:
#     """Result of message processing"""
#     success: bool
#     message: str
#     retry_count: int = 0
#     error: Optional[Exception] = None
#     processing_time: float = 0.0
#     correlation_id: Optional[str] = None


# class MessageProcessor:
#     """Base class for message processors"""
#     def __init__(self, topic_type: 'AppTopicType', settings: 'AppSettings'): # MODIFIED: Accept settings
#         self.topic_type = topic_type
#         self.settings = settings # Store settings for access to global config
#         self.logger = get_logger(f"{__name__}.{self.__class__.__name__}")
    
#     def process(self, message: Dict[str, Any], record: Any) -> ProcessingResult:
#         """Process a message - override in subclasses"""
#         raise NotImplementedError("Subclasses must implement process method")
    
#     def validate_message(self, message: Dict[str, Any]) -> bool:
#         """Validate message schema using MessageSchemaValidator (imported locally)."""
#         # Import MessageSchemaValidator here to avoid circular dependency at module load
#         from kafka_handlers.topics import MessageSchemaValidator as AppMessageSchemaValidator
        
#         try:
#             if self.topic_type == AppMessageSchemaValidator.DETECTION_EVENTS: # Use fully qualified name
#                 return AppMessageSchemaValidator.validate_detection_event(message)
#             elif self.topic_type == AppMessageSchemaValidator.ALERTS:
#                 return AppMessageSchemaValidator.validate_alert(message)
#             elif self.topic_type == AppMessageSchemaValidator.METRICS:
#                 return AppMessageSchemaValidator.validate_metric(message)
#             else:
#                 return True
#         except Exception as e:
#             self.logger.error(f"Message validation failed: {e}", exc_info=True)
#             return False


# class DeadLetterQueue:
#     """Dead letter queue for failed messages."""
    
#     def __init__(self, redis_client: Optional[redis.Redis], producer_client: 'AppKafkaProducerClient', topic_manager_client: 'AppTopicManager'): # MODIFIED: Accept clients
#         self.redis_client = redis_client
#         self.producer = producer_client # Use injected producer client
#         self.topic_manager = topic_manager_client # Use injected topic manager
#         self.logger = get_logger(f"{__name__}.DeadLetterQueue")
    
#     def send_to_dlq(self, 
#                    original_topic_name: str, # Changed from original_topic for clarity
#                    message: Dict[str, Any], 
#                    error: Exception,
#                    retry_count: int):
#         """Send message to dead letter queue."""
#         # Import TopicType here to avoid circular dependency
#         from kafka_handlers.topics import TopicType as AppTopicType
        
#         try:
#             dlq_message = {
#                 'original_topic': original_topic_name,
#                 'original_message': message,
#                 'error': str(error),
#                 'error_type': type(error).__name__,
#                 'retry_count': retry_count,
#                 'timestamp': datetime.utcnow().isoformat(),
#                 'correlation_id': message.get('correlation_id', str(uuid.uuid4()))
#             }
            
#             # Send to Kafka DLQ topic (using topic manager for name consistency)
#             # Assuming a standard DLQ topic name convention, e.g., original_topic + "_dlq"
#             # Or you might have a generic DLQ topic like TopicType.SYSTEM_EVENTS
#             dlq_topic_type = AppTopicType.SYSTEM_EVENTS # Use a predefined SYSTEM_EVENTS for DLQ or create a new DLQ TopicType
#             dlq_topic_name = self.topic_manager.get_topic_name(dlq_topic_type) # Get full topic name
            
#             success = self.producer.send_message(
#                 dlq_topic_type, # Use the TopicType enum
#                 dlq_message,
#                 key=f"dlq_{original_topic_name}",
#                 batch_mode=False # DLQ messages should generally be sent immediately
#             )
            
#             if success:
#                 self.logger.info(f"Message sent to DLQ topic: {dlq_topic_name} (original: {original_topic_name})")
#             else:
#                 self.logger.error(f"Failed to send message to DLQ topic: {dlq_topic_name} (original: {original_topic_name})")
            
#             # Also store in Redis for monitoring
#             if self.redis_client:
#                 self._store_in_redis(dlq_topic_name, dlq_message)
                
#         except Exception as e:
#             self.logger.error(f"Critical error sending message to DLQ: {e}", exc_info=True)


#     def _store_in_redis(self, dlq_topic: str, message: Dict[str, Any]):
#         """Store DLQ message in Redis for monitoring."""
#         try:
#             # Ensure message is JSON serializable for Redis storage
#             json_message = json.dumps(message, default=str)
            
#             key = f"dlq:{dlq_topic}:{message['correlation_id']}"
#             # Use SETEX for automatic expiration
#             self.redis_client.setex(
#                 key,
#                 86400,  # 24 hours TTL, you might want to pull this from settings
#                 json_message
#             )
            
#             # Add to DLQ list (for easy retrieval of recent DLQ messages)
#             list_key = f"dlq_list:{dlq_topic}"
#             self.redis_client.lpush(list_key, key)
#             self.redis_client.ltrim(list_key, 0, 1000)  # Keep only last 1000 entries
            
#         except Exception as e:
#             self.logger.warning(f"Failed to store DLQ message in Redis: {e}", exc_info=True)


# class MessageRetryHandler:
#     """Handles message retry logic with exponential backoff."""
#     def __init__(self, config: RetryConfig, producer_client: 'AppKafkaProducerClient', dlq_client: 'DeadLetterQueue'): # MODIFIED: Accept producer and DLQ
#         self.config = config
#         self.producer = producer_client # Store producer for re-sending
#         self.dlq = dlq_client # Store DLQ for final handling
#         self.retry_queues: Dict[str, Queue] = {}
#         self.retry_threads: Dict[str, threading.Thread] = {}
#         self.stop_event = threading.Event()
#         self.logger = get_logger(f"{__name__}.MessageRetryHandler")
    
#     def should_retry(self, retry_count: int, error: Exception) -> bool:
#         """Determine if message should be retried."""
#         if retry_count >= self.config.max_retries:
#             self.logger.debug(f"Max retries ({self.config.max_retries}) reached for error type {type(error).__name__}.")
#             return False
        
#         # Don't retry certain types of errors (e.g., permanent data errors)
#         non_retryable_errors = (ValueError, TypeError, KeyError) # Add more as needed
#         if isinstance(error, non_retryable_errors):
#             self.logger.debug(f"Error type {type(error).__name__} is non-retryable.")
#             return False
        
#         return True
    
#     def calculate_delay(self, retry_count: int) -> float:
#         """Calculate retry delay with exponential backoff and jitter."""
#         delay = self.config.initial_delay * (self.config.backoff_multiplier ** retry_count)
#         delay = min(delay, self.config.max_delay)
        
#         if self.config.jitter:
#             import random
#             delay = delay * (0.5 + random.random() * 0.5) # Apply random jitter (50% to 100% of calculated delay)
        
#         return delay
    
#     def schedule_retry(self, 
#                       topic: str,
#                       message: Dict[str, Any],
#                       record: Any, # Original Kafka record for offset/topic info
#                       processor: MessageProcessor,
#                       retry_count: int,
#                       error: Exception):
#         """Schedule message for retry."""
#         if topic not in self.retry_queues:
#             self.retry_queues[topic] = Queue()
#             self._start_retry_thread(topic)
        
#         delay = self.calculate_delay(retry_count)
#         retry_time = time.time() + delay
        
#         retry_item = {
#             'retry_time': retry_time,
#             'message': message, # The original message payload
#             'record': record,   # The original Kafka ConsumerRecord
#             'processor': processor,
#             'retry_count': retry_count,
#             'error': error
#         }
        
#         self.retry_queues[topic].put(retry_item)
        
#         self.logger.info(f"Scheduled retry for message in topic '{topic}' in {delay:.2f}s (attempt {retry_count + 1})")
    
#     def _start_retry_thread(self, topic: str):
#         """Start retry processing thread for topic."""
#         # Use an outer function to capture `self` and `topic` for the thread target
#         def _retry_processor_target():
#             retry_queue = self.retry_queues[topic]
            
#             while not self.stop_event.is_set():
#                 try:
#                     retry_item = retry_queue.get(timeout=1) # Blocking with timeout
                    
#                     # Wait until retry time
#                     wait_time = retry_item['retry_time'] - time.time()
#                     if wait_time > 0:
#                         time.sleep(wait_time) # Sleep if necessary
                    
#                     # Process the message again
#                     processor = retry_item['processor']
#                     message = retry_item['message']
#                     record = retry_item['record']
#                     current_retry_count = retry_item['retry_count']
                    
#                     self.logger.info(f"Retrying message for topic '{topic}' (attempt {current_retry_count})...")
                    
#                     # NOTE: This retry mechanism processes messages in a separate thread,
#                     # which means it loses the original consumer's context (like committing offsets).
#                     # For a truly robust retry, consider re-publishing to a "retry topic" with delay,
#                     # or using a persistent retry queue that tracks offsets and ensures atomicity.
#                     # For this pattern, it's assumed processing in the retry thread is fire-and-forget
#                     # or reports back to a central state. For now, we'll call process directly.
                    
#                     result = processor.process(message, record) # Re-process message
                    
#                     if result.success:
#                         self.logger.info(f"Message retry successful for topic '{topic}' after {current_retry_count} attempts.")
#                         # No offset commit here as it's outside the main consumer loop.
#                         # This implies successful retry means no DLQ.
#                     else:
#                         self.logger.error(f"Message retry failed for topic '{topic}' after {current_retry_count} attempts: {result.message}")
#                         # If retry failed again, check if further retries are needed or go to DLQ
#                         if self.should_retry(current_retry_count, result.error):
#                             # Schedule another retry
#                             self.schedule_retry(
#                                 topic, message, record, processor, current_retry_count + 1, result.error
#                             )
#                         else:
#                             # Send to dead letter queue if no more retries
#                             self.dlq.send_to_dlq(record.topic, message, result.error, current_retry_count)
#                             self.logger.info(f"Message sent to DLQ after {current_retry_count} retries (topic: {topic}).")
                    
#                     retry_queue.task_done()
                    
#                 except Empty: # Queue.get() timed out, no items yet
#                     continue
#                 except Exception as e:
#                     self.logger.error(f"Unhandled error in retry processor thread for topic '{topic}': {e}", exc_info=True)
        
#         thread = threading.Thread(target=_retry_processor_target, daemon=True)
#         thread.start()
#         self.retry_threads[topic] = thread
#         self.logger.info(f"Started retry thread for topic: {topic}")
    
#     def stop(self):
#         """Stop all retry threads and wait for them to finish."""
#         self.stop_event.set()
#         for thread in self.retry_threads.values():
#             if thread.is_alive():
#                 thread.join(timeout=5) # Give threads a chance to finish
#                 if thread.is_alive():
#                     self.logger.warning(f"Retry thread for topic {thread.name} did not terminate gracefully.")
#         self.logger.info("Message retry handler stopped.")


# class KafkaConsumerClient:
#     """Enhanced Kafka consumer with resilience features."""
    
#     def __init__(self, 
#                  topic_type: 'AppTopicType',
#                  processor: MessageProcessor,
#                  settings: 'AppSettings', # MODIFIED: Accept settings object
#                  alert_engine_obj: 'AppAlertEngine' # MODIFIED: Accept alert_engine
#                  ):
#         self.topic_type = topic_type
#         self.processor = processor
#         self.settings = settings # Store settings reference
#         self.alert_engine = alert_engine_obj # Store alert_engine reference
        
#         # Initialize sub-components. These should also take settings/dependencies.
#         self._initialize_redis() # Redis client init
#         # Pass the created redis_client to DLQ and RetryHandler
        
#         # topic_manager is needed by DLQ, get it via global getter but pass settings.
#         # This will get the singleton initialized in main.py
#         from kafka_handlers.topics import get_topic_manager as _get_global_topic_manager
#         self.topic_manager = _get_global_topic_manager(settings=self.settings)

#         # Producer is needed by DLQ, get it via global getter but pass dependencies.
#         # This will get the singleton initialized in main.py
#         from kafka_handlers.kafka_producer import get_kafka_producer as _get_global_kafka_producer
#         self.producer = _get_global_kafka_producer(settings=self.settings, topic_manager=self.topic_manager)


#         self.dlq = DeadLetterQueue(self.redis_client, self.producer, self.topic_manager) # Pass producer and topic_manager
#         self.retry_handler = MessageRetryHandler(RetryConfig(), self.producer, self.dlq) # Pass producer and dlq
        
#         self.consumer: Optional[kafka_lib.KafkaConsumer] = None # Will be initialized in _initialize_consumer
#         self.is_running = False
#         self.stop_event = threading.Event()
#         self.metrics_thread: Optional[threading.Thread] = None
#         self.processed_messages: Set[str] = set() # Simple in-memory tracker (for logs, not deduplication)
        
#         self._initialize_consumer() # Initialize KafkaConsumer client itself
#         self._start_metrics_collection() # Start metrics thread

#         logger.info(f"KafkaConsumerClient initialized for topic: {self.topic_type.value}")
    
#     def _get_consumer_config_from_settings(self) -> ConsumerConfig:
#         """Helper to create ConsumerConfig from app settings."""
#         return ConsumerConfig(
#             bootstrap_servers=self.settings.kafka.bootstrap_servers,
#             group_id=self.settings.kafka.consumer_group_id, # Use global group_id from settings
#             client_id=f"consumer_{self.topic_type.value}_{uuid.uuid4().hex[:8]}",
#             auto_offset_reset=self.settings.kafka.auto_offset_reset,
#             enable_auto_commit=self.settings.kafka.enable_auto_commit,
#             max_poll_records=getattr(self.settings.kafka, 'max_poll_records', 500), # Use getattr for flexibility
#             max_poll_interval_ms=getattr(self.settings.kafka, 'max_poll_interval_ms', 300000),
#             session_timeout_ms=getattr(self.settings.kafka, 'session_timeout_ms', 30000),
#             heartbeat_interval_ms=getattr(self.settings.kafka, 'heartbeat_interval_ms', 3000),
#             fetch_min_bytes=getattr(self.settings.kafka, 'fetch_min_bytes', 1),
#             fetch_max_wait_ms=getattr(self.settings.kafka, 'fetch_max_wait_ms', 500),
#             max_partition_fetch_bytes=getattr(self.settings.kafka, 'max_partition_fetch_bytes', 1048576),
#             isolation_level=getattr(self.settings.kafka, 'isolation_level', 'read_uncommitted'),
#             consumer_timeout_ms=getattr(self.settings.kafka, 'consumer_timeout_ms', 1000),
#             security_protocol=getattr(self.settings.kafka, 'security_protocol', 'PLAINTEXT'),
#             sasl_mechanism=getattr(self.settings.kafka, 'sasl_mechanism', None),
#             sasl_username=getattr(self.settings.kafka, 'sasl_username', None),
#             sasl_password=getattr(self.settings.kafka, 'sasl_password', None)
#         )
    
#     def _initialize_consumer(self):
#         """Initialize Kafka consumer with proper configuration."""
#         try:
#             consumer_config_from_settings = self._get_consumer_config_from_settings()

#             consumer_params = {
#                 'bootstrap_servers': consumer_config_from_settings.bootstrap_servers.split(','),
#                 'group_id': consumer_config_from_settings.group_id,
#                 'client_id': consumer_config_from_settings.client_id,
#                 'auto_offset_reset': consumer_config_from_settings.auto_offset_reset,
#                 'enable_auto_commit': consumer_config_from_settings.enable_auto_commit,
#                 'max_poll_records': consumer_config_from_poll_settings.max_poll_records, # Corrected typo
#                 'max_poll_interval_ms': consumer_config_from_settings.max_poll_interval_ms,
#                 'session_timeout_ms': consumer_config_from_settings.session_timeout_ms,
#                 'heartbeat_interval_ms': consumer_config_from_settings.heartbeat_interval_ms,
#                 'fetch_min_bytes': consumer_config_from_settings.fetch_min_bytes,
#                 'fetch_max_wait_ms': consumer_config_from_settings.fetch_max_wait_ms,
#                 'max_partition_fetch_bytes': consumer_config_from_settings.max_partition_fetch_bytes,
#                 'isolation_level': consumer_config_from_settings.isolation_level,
#                 'consumer_timeout_ms': consumer_config_from_settings.consumer_timeout_ms,
#                 'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
#                 'key_deserializer': lambda x: x.decode('utf-8') if x else None,
#                 'security_protocol': consumer_config_from_settings.security_protocol,
#             }

#             # Add SASL configuration if protocol is not PLAINTEXT
#             if consumer_config_from_settings.security_protocol != "PLAINTEXT":
#                 consumer_params.update({
#                     'sasl_mechanism': consumer_config_from_settings.sasl_mechanism,
#                     'sasl_plain_username': consumer_config_from_settings.sasl_username,
#                     'sasl_plain_password': consumer_config_from_settings.sasl_password,
#                 })
            
#             self.consumer = KafkaConsumer(**consumer_params)
#             logger.info(f"Kafka consumer initialized for topic: {self.topic_manager.get_topic_name(self.topic_type)}") # Use topic_manager for full name
            
#         except Exception as e:
#             logger.error(f"Failed to initialize Kafka consumer: {e}", exc_info=True)
#             raise # Re-raise to halt startup if critical
    
#     def _initialize_redis(self):
#         """Initialize Redis client for deduplication/DLQ."""
#         try:
#             if self.settings.redis.enabled:
#                 self.redis_client = redis.Redis( # Using synchronous redis here
#                     host=self.settings.redis.host,
#                     port=self.settings.redis.port,
#                     db=self.settings.redis.db,
#                     password=self.settings.redis.password, # Pass password
#                     decode_responses=True,
#                     socket_timeout=self.settings.redis.socket_timeout, # Use settings
#                     socket_connect_timeout=self.settings.redis.socket_connect_timeout, # Use settings
#                     retry_on_timeout=True,
#                     health_check_interval=30 # Could be from settings if desired
#                 )
#                 self.redis_client.ping()
#                 logger.info("Redis client initialized successfully for KafkaConsumerClient.")
#             else:
#                 logger.info("Redis is disabled in settings for KafkaConsumerClient.")
#         except Exception as e:
#             logger.warning(f"Failed to initialize Redis client for KafkaConsumerClient: {e}", exc_info=True)
#             self.redis_client = None
    
#     def _start_metrics_collection(self):
#         """Start metrics collection thread."""
#         def collect_metrics_target():
#             while not self.stop_event.is_set():
#                 try:
#                     if self.consumer:
#                         self._collect_consumer_metrics()
#                     time.sleep(30)
#                 except Exception as e:
#                     logger.error(f"Error collecting consumer metrics: {e}", exc_info=True)
#                     time.sleep(5) # Shorter sleep on error
        
#         self.metrics_thread = threading.Thread(target=collect_metrics_target, daemon=True)
#         self.metrics_thread.start()
#         logger.info("Consumer metrics collection thread started.")
    
#     def _collect_consumer_metrics(self):
#         """Collect consumer lag and other metrics."""
#         try:
#             # Get assigned partitions for current consumer
#             assigned_partitions = list(self.consumer.assignment())
#             if not assigned_partitions:
#                 return # No partitions assigned yet
            
#             # Get end offsets (latest offsets) for all assigned topic-partitions
#             end_offsets = self.consumer.end_offsets(assigned_partitions)

#             for partition in assigned_partitions:
#                 committed_offset = self.consumer.committed(partition) # Last committed offset
#                 current_position = self.consumer.position(partition) # Current consumer position (next fetch)
                
#                 # High watermark is the end offset for the partition
#                 high_water_mark = end_offsets.get(partition)

#                 if high_water_mark is not None and committed_offset is not None and current_position is not None:
#                     lag_from_committed = high_water_mark - committed_offset
#                     lag_from_current_position = high_water_mark - current_position

#                     consumer_lag.labels(
#                         topic=partition.topic,
#                         partition=partition.partition,
#                         consumer_group=self.settings.kafka.consumer_group_id
#                     ).set(lag_from_current_position) # Report lag from current position

#                     logger.debug(f"Consumer Lag for {partition.topic}-{partition.partition}: "
#                                  f"Committed={committed_offset}, Current={current_position}, End={high_water_mark}, Lag={lag_from_current_position}")
#                 elif high_water_mark is None:
#                     logger.warning(f"Could not get end offset for {partition.topic}-{partition.partition}. Broker may be unavailable.")
#                 else:
#                     logger.warning(f"Could not get committed/current offset for {partition.topic}-{partition.partition}. Consumer not ready or no messages yet.")

#         except KafkaError as e:
#             logger.error(f"Kafka error during metrics collection: {e}", exc_info=True)
#             consumer_errors_total.labels(topic='_metrics', error_type='KafkaError', consumer_group=self.settings.kafka.consumer_group_id).inc()
#         except Exception as e:
#             logger.error(f"Unexpected error during consumer metrics collection: {e}", exc_info=True)
#             consumer_errors_total.labels(topic='_metrics', error_type='GenericError', consumer_group=self.settings.kafka.consumer_group_id).inc()
    
#     def _is_duplicate_message(self, message: Dict[str, Any]) -> bool:
#         """Check if message is duplicate using Redis (for deduplication at consumer level)."""
#         if not self.redis_client:
#             return False
        
#         try:
#             # Use a unique identifier from the message or generate one based on content
#             correlation_id = message.get('correlation_id')
#             if not correlation_id: # If no correlation ID, cannot deduplicate reliably
#                 return False
            
#             # Key should uniquely identify the message across restarts within TTL
#             key = f"processed_msg:{self.topic_type.value}:{correlation_id}"
            
#             # Check if key exists. If it does, it's a duplicate.
#             if self.redis_client.setnx(key, "1"): # SETNX returns 1 if key was set (not exists), 0 if key exists
#                 # Key did not exist, so it was set. Message is NOT a duplicate.
#                 self.redis_client.expire(key, self.settings.redis.alerts_cache_ttl) # Set TTL from settings
#                 return False
#             else:
#                 # Key already existed, so message IS a duplicate.
#                 return True
                
#         except Exception as e:
#             logger.error(f"Error checking duplicate message in Redis: {e}", exc_info=True)
#             # Fail open: if Redis fails, don't block messages as duplicates.
#             return False
    
#     def _generate_correlation_id(self, record: Any) -> str:
#         """Generate correlation ID for message if missing (for logging/tracing)."""
#         # A more robust correlation ID might combine producer_id and sequence number
#         # For now, a hash of record metadata is good enough as a fallback
#         data = f"{record.topic}:{record.partition}:{record.offset}:{record.timestamp}"
#         return hashlib.md5(data.encode()).hexdigest()
    
#     def _process_message(self, record: Any) -> ProcessingResult:
#         """Process a single message with error handling and deduplication."""
#         start_time = time.time()
#         correlation_id = None
        
#         try:
#             message_value = record.value # The deserialized message (dict or other)
#             # Ensure message_value is a dict, as expected by processors
#             if not isinstance(message_value, dict):
#                 raise TypeError(f"Message value is not a dictionary: {type(message_value)}")

#             # Extract/Generate correlation_id
#             correlation_id = message_value.get('correlation_id', self._generate_correlation_id(record))
            
#             with LogContext(correlation_id=correlation_id, service_name=self.settings.logging.service_name):
#                 # Deduplication check FIRST, before processing logic
#                 if self._is_duplicate_message(message_value):
#                     logger.info(f"Duplicate message ignored from topic '{record.topic}': {correlation_id}")
#                     consumer_messages_total.labels(
#                         topic=record.topic, status='duplicate', consumer_group=self.settings.kafka.consumer_group_id
#                     ).inc()
#                     return ProcessingResult(
#                         success=True,
#                         message="Duplicate message ignored",
#                         correlation_id=correlation_id,
#                         processing_time=time.time() - start_time
#                     )
                
#                 # Validate message schema (uses injected processor's method)
#                 if not self.processor.validate_message(message_value):
#                     raise ValueError(f"Invalid message schema for topic {self.topic_type.value}. Message: {message_value}")
                
#                 # Process message using the injected processor
#                 # The processor might generate alerts and use alert_engine
#                 processing_result_from_processor = self.processor.process(message_value, record)
                
#                 processing_time = time.time() - start_time
                
#                 # Update metrics based on final processing outcome
#                 status_label = "success" if processing_result_from_processor.success else "failed_final"
#                 consumer_messages_total.labels(
#                     topic=record.topic,
#                     status=status_label,
#                     consumer_group=self.settings.kafka.consumer_group_id
#                 ).inc()
#                 consumer_processing_time.labels(
#                     topic=record.topic,
#                     consumer_group=self.settings.kafka.consumer_group_id
#                 ).observe(processing_time)
                
#                 # Return result, adding correlation ID and time
#                 processing_result_from_processor.correlation_id = correlation_id
#                 processing_result_from_processor.processing_time = processing_time
                
#                 return processing_result_from_processor
                
#         except Exception as e:
#             processing_time = time.time() - start_time
#             logger.error(f"Error processing message from topic '{record.topic}', offset {record.offset}: {e}", exc_info=True)
            
#             consumer_errors_total.labels(
#                 topic=record.topic,
#                 error_type=type(e).__name__,
#                 consumer_group=self.settings.kafka.consumer_group_id
#             ).inc()
            
#             return ProcessingResult(
#                 success=False,
#                 message=f"Processing failed: {str(e)}",
#                 error=e,
#                 correlation_id=correlation_id,
#                 processing_time=processing_time
#             )
    
#     def _handle_processing_result(self, result: ProcessingResult, record: Any):
#         """Handle the result of message processing (commit, retry, DLQ)."""
#         if result.success:
#             logger.info(f"Message processed successfully from topic '{record.topic}': {result.correlation_id}")
#             try:
#                 # Commit offset (asynchronous commit is preferred for performance)
#                 # The next fetch will start from offset + 1.
#                 self.consumer.commit_async({
#                     TopicPartition(record.topic, record.partition): record.offset + 1
#                 })
#             except CommitFailedError as e:
#                 logger.error(f"Failed to commit offset for {record.topic}-{record.partition} at offset {record.offset}: {e}", exc_info=True)
#                 consumer_errors_total.labels(topic=record.topic, error_type='CommitFailedError', consumer_group=self.settings.kafka.consumer_group_id).inc()
#             except Exception as e:
#                 logger.error(f"Unexpected error during async commit for {record.topic}-{record.partition}: {e}", exc_info=True)
#                 consumer_errors_total.labels(topic=record.topic, error_type='UnexpectedCommitError', consumer_group=self.settings.kafka.consumer_group_id).inc()
#         else:
#             logger.error(f"Message processing failed for {record.topic}, offset {record.offset}: {result.message}")
            
#             # Handle retry logic
#             if self.retry_handler and self.retry_handler.should_retry(result.retry_count, result.error):
#                 consumer_retries_total.labels(
#                     topic=record.topic,
#                     consumer_group=self.settings.kafka.consumer_group_id
#                 ).inc()
                
#                 self.retry_handler.schedule_retry(
#                     record.topic, # Pass topic name string
#                     record.value, # Pass original message payload
#                     record,       # Pass original Kafka record
#                     self.processor, # Pass the processor instance
#                     result.retry_count + 1, # Increment retry count
#                     result.error
#                 )
#             else:
#                 # Send to dead letter queue if no more retries or non-retryable error
#                 if self.dlq:
#                     self.dlq.send_to_dlq(
#                         record.topic, # Pass topic name string
#                         record.value, # Pass original message payload
#                         result.error,
#                         result.retry_count
#                     )
#                     logger.info(f"Message sent to DLQ (topic: {record.topic}, offset: {record.offset})")
#                 else:
#                     logger.critical(f"DLQ not initialized! Message from {record.topic}, offset {record.offset} will be lost: {result.message}")

    
#     def subscribe(self, topics: Optional[List[str]] = None): # MODIFIED: Optional List[str]
#         """Subscribe to topics."""
#         # Get full topic name using TopicManager
#         # Import TopicType here to avoid circular dependency
#         from kafka_handlers.topics import TopicType as AppTopicType
        
#         try:
#             if topics is None:
#                 # Get the full topic name for the consumer's target topic type
#                 topic_to_subscribe = self.topic_manager.get_topic_name(self.topic_type)
#                 self.consumer.subscribe([topic_to_subscribe])
#                 logger.info(f"Subscribed to topic: {topic_to_subscribe}")
#             else:
#                 # If a list of raw topic names is provided, use them directly
#                 self.consumer.subscribe(topics)
#                 logger.info(f"Subscribed to custom topics: {topics}")
            
#         except Exception as e:
#             logger.error(f"Failed to subscribe to topics: {e}", exc_info=True)
#             raise # Re-raise to halt startup if critical
    
#     async def start(self): # MODIFIED: Made async for proper integration with FastAPI lifespan
#         """Start consuming messages."""
#         if self.is_running:
#             logger.warning("Consumer is already running.")
#             return
        
#         self.is_running = True
#         self.stop_event.clear() # Clear stop event
#         logger.info(f"Starting consumer for topic: {self.topic_type.value} (Group: {self.settings.kafka.consumer_group_id})")
        
#         # Subscribe to topic
#         self.subscribe()
        
#         # We run the synchronous poll loop in a separate thread so it doesn't block the asyncio event loop.
#         self._consumer_thread = threading.Thread(target=self._run_consumer_loop, daemon=True)
#         self._consumer_thread.start()
#         logger.info(f"Consumer loop started in background thread for topic: {self.topic_type.value}")
    
#     def _run_consumer_loop(self):
#         """Synchronous loop for polling Kafka messages."""
#         # Set up signal handlers for graceful shutdown (only in the main thread if possible,
#         # but for background threads, this is a fallback if signals are routed here)
#         # Avoid setting signals in background threads unless specifically required and managed.
#         # FastAPI's lifespan handles primary signal capture.

#         try:
#             while self.is_running and not self.stop_event.is_set():
#                 try:
#                     message_batch = self.consumer.poll(
#                         timeout_ms=self.settings.kafka.consumer_timeout_ms,
#                         max_records=self.settings.kafka.max_poll_records
#                     )
                    
#                     if not message_batch:
#                         logger.debug("Kafka poll timeout or no messages, continuing...")
#                         continue
                    
#                     for topic_partition, records in message_batch.items():
#                         for record in records:
#                             if self.stop_event.is_set():
#                                 logger.info(f"Stop event set, breaking from message processing loop for {topic_partition}.")
#                                 break # Exit loop if stop is requested
                            
#                             # Process message and handle result
#                             result = self._process_message(record)
#                             self._handle_processing_result(result, record)
                    
#                 except KafkaTimeoutError:
#                     logger.debug("Kafka poll timeout, no messages. Continuing consumer loop.")
#                     continue
#                 except CommitFailedError as e:
#                     logger.error(f"CommitFailedError in consumer loop: {e}. Rebalancing or transient network issue?", exc_info=True)
#                     consumer_errors_total.labels(topic='_consumer_loop', error_type='CommitFailedError', consumer_group=self.settings.kafka.consumer_group_id).inc()
#                     time.sleep(5)
#                 except KafkaError as e:
#                     logger.error(f"General Kafka error in consumer loop: {e}", exc_info=True)
#                     consumer_errors_total.labels(topic='_consumer_loop', error_type='KafkaError', consumer_group=self.settings.kafka.consumer_group_id).inc()
#                     time.sleep(5)
#                 except Exception as e:
#                     logger.critical(f"Unhandled critical error in consumer loop: {e}", exc_info=True)
#                     consumer_errors_total.labels(topic='_consumer_loop', error_type='CriticalError', consumer_group=self.settings.kafka.consumer_group_id).inc()
#                     time.sleep(10) # Longer sleep on critical error
        
#         except KeyboardInterrupt:
#             # This block might not be hit if signals are handled by main thread
#             logger.info("Consumer loop interrupted by keyboard (background thread).")
#         except Exception as e:
#             logger.critical(f"Consumer background thread terminated unexpectedly: {e}", exc_info=True)
#         finally:
#             self._cleanup_on_stop() # Perform cleanup operations


#     async def stop(self): # MODIFIED: Made async for proper integration with FastAPI lifespan
#         """Stop the consumer gracefully."""
#         if not self.is_running:
#             logger.info("Consumer is not running, skipping stop.")
#             return
        
#         logger.info(f"Stopping consumer for topic: {self.topic_type.value}...")
#         self.is_running = False
#         self.stop_event.set() # Signal all loops to stop
        
#         # Stop metrics thread
#         if self.metrics_thread and self.metrics_thread.is_alive():
#             self.metrics_thread.join(timeout=5)
#             if self.metrics_thread.is_alive():
#                 logger.warning("Metrics thread did not terminate gracefully.")

#         # Wait for the main consumer loop thread to finish
#         if hasattr(self, '_consumer_thread') and self._consumer_thread and self._consumer_thread.is_alive():
#             self._consumer_thread.join(timeout=10) # Give consumer loop up to 10 seconds to finish
#             if self._consumer_thread.is_alive():
#                 logger.warning(f"Consumer loop thread for {self.topic_type.value} did not terminate gracefully.")
        
#         self._cleanup_on_stop() # Call cleanup after threads join
#         logger.info(f"Consumer for topic '{self.topic_type.value}' stopped successfully.")

#     def _cleanup_on_stop(self):
#         """Internal method to perform resource cleanup when consumer stops."""
#         # Stop retry handler (if initialized)
#         if self.retry_handler:
#             self.retry_handler.stop()
        
#         # Close Kafka consumer
#         if self.consumer:
#             try:
#                 self.consumer.close()
#                 logger.info("Kafka consumer client closed.")
#             except Exception as e:
#                 logger.error(f"Error closing Kafka consumer client: {e}", exc_info=True)
        
#         # Close Redis client
#         if self.redis_client:
#             try:
#                 self.redis_client.close()
#                 logger.info("Redis connection closed for KafkaConsumerClient.")
#             except Exception as e:
#                 logger.error(f"Error closing Redis connection for KafkaConsumerClient: {e}", exc_info=True)
        
#         self.logger.info("KafkaConsumerClient resources cleaned up.")


#     async def health_check(self) -> Dict[str, Any]:
#         """Get consumer health status."""
#         if not self.is_running:
#             return {'status': 'not_running', 'message': 'Consumer is not active.'}
        
#         health_status = {'status': 'healthy', 'message': 'Consumer is running.'}

#         try:
#             # Check Kafka connection by listing topics (non-blocking)
#             topics_metadata = self.consumer.partitions_for_topic(self.topic_manager.get_topic_name(self.topic_type))
#             if topics_metadata is None:
#                 health_status['status'] = 'degraded'
#                 health_status['message'] = 'Could not get topic partitions, Kafka connection might be unstable.'
#                 logger.warning(f"Consumer health check: No topic partitions found for {self.topic_manager.get_topic_name(self.topic_type)}.")

#             # Check Redis connection
#             redis_ok = self._test_redis_connection()
#             if not redis_ok:
#                 health_status['status'] = 'degraded'
#                 health_status['redis_status'] = 'disconnected'
#                 health_status['message'] += ' Redis disconnected.'
#                 logger.warning("Consumer health check: Redis client is not connected.")
#             else:
#                 health_status['redis_status'] = 'connected'

#             # Check if consumer thread is alive
#             if not hasattr(self, '_consumer_thread') or not self._consumer_thread.is_alive():
#                 health_status['status'] = 'unhealthy'
#                 health_status['message'] = 'Consumer background thread is not running.'
#                 logger.critical("Consumer health check: Background thread is dead.")
            
#             # Check if metrics thread is alive
#             if self.metrics_thread and not self.metrics_thread.is_alive():
#                 health_status['status'] = 'degraded'
#                 health_status['message'] += ' Metrics collection thread is not running.'
#                 logger.warning("Consumer health check: Metrics collection thread is dead.")

#             # Check retry handler thread status
#             if self.retry_handler:
#                 for topic, thread in self.retry_handler.retry_threads.items():
#                     if not thread.is_alive():
#                         health_status['status'] = 'degraded'
#                         health_status['message'] += f' Retry thread for {topic} is not running.'
#                         logger.warning(f"Consumer health check: Retry thread for {topic} is dead.")

#             health_status.update({
#                 'topic_type': self.topic_type.value,
#                 'consumer_group': self.settings.kafka.consumer_group_id,
#                 'assignment': [str(tp) for tp in self.consumer.assignment()] if self.consumer else [],
#                 'background_thread_alive': hasattr(self, '_consumer_thread') and self._consumer_thread.is_alive(),
#                 'last_polled': time.time() # This would need to be updated in the loop
#             })

#         except Exception as e:
#             logger.error(f"Error during consumer health check: {e}", exc_info=True)
#             health_status = {'status': 'unhealthy', 'message': f'Exception during health check: {str(e)}'}
        
#         return health_status

#     def _test_redis_connection(self) -> bool:
#         """Test Redis connection."""
#         try:
#             if self.redis_client:
#                 self.redis_client.ping()
#                 return True
#         except Exception as e:
#             logger.debug(f"Redis ping failed: {e}")
#         return False


# # Specific message processors for different topic types
# class DetectionEventProcessor(MessageProcessor):
#     """Process detection events from Kafka."""
    
#     def __init__(self, settings: 'AppSettings', alert_engine: 'AppAlertEngine'):
#         # Pass topic type from kafka_handlers.topics.TopicType
#         from kafka_handlers.topics import TopicType as AppTopicType
#         super().__init__(AppTopicType.DETECTION_EVENTS, settings)
#         self.alert_engine = alert_engine # Store alert_engine reference
#         self.logger = get_logger(f"{__name__}.DetectionEventProcessor")

#     def process(self, message: Dict[str, Any], record: Any) -> ProcessingResult:
#         """Process detection event message."""
#         try:
#             detection_type = message.get('detection_type')
#             camera_id = message.get('camera_id')
#             confidence = message.get('confidence', 0.0)
            
#             self.logger.info(f"Processing detection: {detection_type} from camera {camera_id} (offset: {record.offset})")
            
#             # Here, use the alert_engine to decide if an alert should be raised
#             # The alert_engine.generate_alert expects a detection event dictionary
#             alert = self.alert_engine.generate_alert(message) # Pass the raw message dict

#             if alert:
#                 self.logger.info(f"Alert generated by AlertEngine for {detection_type} on camera {camera_id}: {alert['alert_type']}",
#                                  extra={'alert_id': alert.get('id')})
#                 # The alert_engine itself should handle sending the alert to Kafka/WebSocket
#                 # No need to send to Kafka from here, as alert_engine will do it.
#             else:
#                 self.logger.debug(f"No alert generated for detection: {detection_type} from camera {camera_id}.")
            
#             return ProcessingResult(
#                 success=True,
#                 message=f"Detection event processed successfully: {detection_type}"
#             )
            
#         except Exception as e:
#             self.logger.error(f"Failed to process detection event: {e}", exc_info=True)
#             return ProcessingResult(
#                 success=False,
#                 message=f"Failed to process detection event: {str(e)}",
#                 error=e
#             )


# class AlertProcessor(MessageProcessor):
#     """Process alert messages from Kafka."""
    
#     def __init__(self, settings: 'AppSettings'):
#         # Pass topic type from kafka_handlers.topics.TopicType
#         from kafka_handlers.topics import TopicType as AppTopicType
#         super().__init__(AppTopicType.ALERTS, settings)
#         self.logger = get_logger(f"{__name__}.AlertProcessor")

#     def process(self, message: Dict[str, Any], record: Any) -> ProcessingResult:
#         """Process alert message."""
#         try:
#             alert_type = message.get('alert_type')
#             camera_id = message.get('camera_id')
#             severity = message.get('severity', 'medium')
            
#             self.logger.info(f"Processing alert: {alert_type} from camera {camera_id} (offset: {record.offset})")
            
#             # Here you would typically:
#             # 1. Store alert in your AlertStore (already done by AlertEngine if it generated it)
#             # 2. Send notifications to frontend via WebSockets
#             #    (This would likely involve the WebSocketManager)
#             # 3. Update dashboards / trigger workflows
            
#             # Example: Send to WebSocket (assuming WebSocketManager is available via dependency or global access)
#             # This is illustrative; you'd need to properly inject/access WebSocketManager.
#             # from ws.websocket_manager import get_websocket_manager # if it's a global getter
#             # ws_manager = get_websocket_manager()
#             # if ws_manager:
#             #     asyncio.create_task(ws_manager.broadcast_to_room(f"alerts_camera_{camera_id}", message))
#             #     asyncio.create_task(ws_manager.broadcast_to_room("all_alerts", message))

#             self.logger.info(f"Alert processed for notification: {alert_type} (severity: {severity})")
            
#             return ProcessingResult(
#                 success=True,
#                 message=f"Alert processed successfully for notification: {alert_type}"
#             )
            
#         except Exception as e:
#             self.logger.error(f"Failed to process alert message: {e}", exc_info=True)
#             return ProcessingResult(
#                 success=False,
#                 message=f"Failed to process alert: {str(e)}",
#                 error=e
#             )


# # Factory function to create consumers (used by main.py)
# def create_consumer(topic_type: 'AppTopicType', 
#                    settings_obj: 'AppSettings', # MODIFIED: Accept settings
#                    alert_engine_obj: 'AppAlertEngine', # MODIFIED: Accept alert_engine
#                    processor: Optional[MessageProcessor] = None
#                    ) -> KafkaConsumerClient:
#     """Factory function to create configured consumers."""
    
#     if processor is None:
#         if topic_type.value == "detection_events": # Use .value for enum comparison
#             processor = DetectionEventProcessor(settings_obj, alert_engine_obj) # Pass settings and alert_engine
#         elif topic_type.value == "alerts":
#             processor = AlertProcessor(settings_obj) # Pass settings
#         else:
#             raise ValueError(f"No default processor for topic type: {topic_type.value}")
    
#     return KafkaConsumerClient(topic_type, processor, settings_obj, alert_engine_obj) # MODIFIED: Pass all required dependencies


# # AsyncKafkaConsumer for integration into main.py's lifecycle
# class AsyncKafkaConsumer:
#     """Async wrapper for Kafka consumer client."""
    
#     def __init__(self, topic_type: 'AppTopicType', processor: MessageProcessor, settings_obj: 'AppSettings', alert_engine_obj: 'AppAlertEngine'): # MODIFIED: Accept all dependencies
#         self.topic_type = topic_type
#         self.processor = processor
#         self.settings = settings_obj
#         self.alert_engine = alert_engine_obj
#         self.consumer_client: Optional[KafkaConsumerClient] = None
#         self.consumer_task: Optional[asyncio.Task] = None
    
#     async def start(self):
#         """Start async consumer."""
#         if self.consumer_client and self.consumer_client.is_running:
#             self.logger.warning("Attempted to start an already running consumer.")
#             return

#         self.consumer_client = KafkaConsumerClient(
#             self.topic_type, 
#             self.processor, 
#             self.settings, 
#             self.alert_engine # Pass alert_engine
#         )
        
#         # Start the consumer client's internal loop in the background
#         # It's a sync function, so run it in a thread pool executor.
#         loop = asyncio.get_event_loop()
#         self.consumer_task = loop.run_in_executor(
#             None, self.consumer_client._run_consumer_loop # Call the internal sync loop
#         )
#         self.logger.info(f"AsyncKafkaConsumer for {self.topic_type.value} started.")
#         # Subscribe is now called inside KafkaConsumerClient._initialize_consumer
#         # Or you can call self.consumer_client.subscribe() here if you want to explicitly control it.

#     async def shutdown(self):
#         """Stop async consumer."""
#         if self.consumer_client:
#             self.consumer_client.stop() # This is a synchronous stop method
#             # If start_consuming() was blocking, we need to ensure its thread stops.
#             # The _consumer_thread.join() in KafkaConsumerClient.stop() will handle waiting.
        
#         if self.consumer_task:
#             self.consumer_task.cancel()
#             try:
#                 # Await the task to ensure it finishes or is cancelled properly
#                 await self.consumer_task
#             except asyncio.CancelledError:
#                 self.logger.info(f"AsyncKafkaConsumer task for {self.topic_type.value} cancelled.")
#             except Exception as e:
#                 self.logger.error(f"Error while awaiting consumer task shutdown for {self.topic_type.value}: {e}", exc_info=True)
#         self.logger.info(f"AsyncKafkaConsumer for {self.topic_type.value} stopped.")


# # Main execution for standalone testing (if run directly)
# if __name__ == "__main__":
#     # This block is for testing consumer.py directly.
#     # It needs settings and alert_engine to be initialized.
#     from dotenv import load_dotenv
#     from config.settings import get_settings as _get_app_settings
#     from kafka_handlers.topics import TopicType as AppTopicType, get_topic_manager as _get_app_topic_manager
#     from kafka_handlers.kafka_producer import get_kafka_producer as _get_app_kafka_producer
#     from alerts.alert_logic import AlertEngine as AppAlertEngine, get_alert_generator as _get_app_alert_generator
#     from alerts.alert_store import get_alert_store as _get_app_alert_store

#     print("--- Running KafkaConsumerClient standalone test ---")
    
#     load_dotenv() # Load .env for standalone test
#     test_settings = _get_app_settings() # Get settings

#     # Initialize AlertStore and AlertEngine for the consumer test's AlertProcessor
#     print("Initializing Alert Store and Engine for consumer test...")
#     # These are async, so run them in an async context
#     async def _init_alert_system():
#         test_alert_store = await _get_app_alert_store()
#         test_alert_engine = AppAlertEngine(test_settings, test_alert_store)
#         await test_alert_engine.initialize()
#         return test_alert_engine
    
#     # Run async init in the current event loop
#     loop = asyncio.get_event_loop()
#     if loop.is_running():
#         # If already running (e.g., from an outer test suite), just create task
#         alert_engine_instance = loop.create_task(_init_alert_system())
#         # You'll need to await this task later or ensure it completes
#     else:
#         # Run it directly if no loop is running
#         alert_engine_instance = loop.run_until_complete(_init_alert_system())

#     test_topic_manager = _get_app_topic_manager(test_settings)
#     test_kafka_producer = _get_app_kafka_producer(test_settings, test_topic_manager)
#     # Ensure producer is started for DLQ functionality
#     asyncio.run(test_kafka_producer.start())

#     print("Ensuring Kafka topics exist for consumer testing...")
#     if not test_topic_manager.create_all_topics():
#         print("WARNING: Failed to ensure all topics exist. Consumer tests might fail.")
#     else:
#         print("Topics ready.")

#     detection_processor = DetectionEventProcessor(test_settings, alert_engine_instance) # Pass dependencies
#     alert_processor = AlertProcessor(test_settings) # Pass dependencies

#     async def run_consumer_test():
#         consumer_client = None
#         try:
#             print("\n--- Creating consumer for DETECTION_EVENTS ---")
#             consumer_client = create_consumer(
#                 AppTopicType.DETECTION_EVENTS,
#                 test_settings,
#                 alert_engine_instance, # Pass alert engine
#                 detection_processor
#             )
            
#             # Start consuming messages in the background
#             print("Starting consumer...")
#             await consumer_client.start()

#             # Give it some time to consume messages
#             print("Consumer running for 10 seconds. Send some dummy messages to 'video_analytics_development_detection_events' topic.")
#             await asyncio.sleep(10)
            
#             # For testing: send a dummy message to the topic to be consumed
#             dummy_detection = {
#                 'detection_id': str(uuid.uuid4()),
#                 'timestamp': datetime.utcnow().isoformat(),
#                 'camera_id': 'test_cam_007',
#                 'objects': [{'class': 'person', 'confidence': 0.9, 'bbox': [0,0,10,10], 'tracking_id': 't1'}],
#                 'frame_id': 'frame_test_001',
#                 'confidence': 0.9,
#                 'processing_time': 0.05
#             }
#             print("Sending a dummy detection event to Kafka for consumer to pick up...")
#             test_kafka_producer.send_detection_event(dummy_detection)
#             test_kafka_producer.flush() # Ensure it's sent

#             print("Waiting a bit more for message processing...")
#             await asyncio.sleep(5) # Wait for the dummy message to be processed

#             print("Consumer health status:", await consumer_client.health_check())

#         except Exception as e:
#             print(f"An error occurred during consumer test: {e}", file=sys.stderr)
#             traceback.print_exc()
#         finally:
#             if consumer_client:
#                 print("\nStopping consumer...")
#                 await consumer_client.stop()
#             print("Consumer shutdown complete.")
#             await alert_engine_instance.shutdown() # Shutdown alert engine
#             await test_kafka_producer.shutdown() # Shutdown producer

#     # Run the async test function
#     asyncio.run(run_consumer_test())

#     print("\n--- All KafkaConsumerClient standalone tests finished ---")

"""
Kafka Consumer with Advanced Resilience Features

This module provides:
- Robust Kafka message consumption with error handling
- Automatic retry mechanisms with exponential backoff
- Dead letter queue support for failed messages
- Message deduplication using Redis
- Graceful shutdown and reconnection handling
- Metrics collection and health monitoring
- Correlation ID tracking for distributed tracing
- Batch processing capabilities
"""

import json
import time
import uuid
import asyncio
import signal
import threading
from typing import Dict, List, Optional, Any, Callable, Set, TYPE_CHECKING
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import hashlib
from queue import Queue, Empty
import traceback
import sys
import redis
from prometheus_client import Counter, Histogram, Gauge, Summary 

# Import Kafka components based on your library
import kafka as kafka_lib
KafkaConsumer = kafka_lib.KafkaConsumer
TopicPartition = kafka_lib.TopicPartition

# Handle different kafka libraries for error types
try:
    from kafka.errors import KafkaError, KafkaTimeoutError, CommitFailedError
except ImportError:
    from kafka.errors import KafkaError, KafkaTimeoutError
    class CommitFailedError(KafkaError):
        pass

# Type hinting for settings and other managers to avoid runtime import issues
if TYPE_CHECKING:
    from config.settings import Settings as AppSettings
    from kafka_handlers.topics import TopicType as AppTopicType
    from kafka_handlers.topics import TopicManager as AppTopicManager
    from kafka_handlers.topics import MessageSchemaValidator as AppMessageSchemaValidator
    from kafka_handlers.kafka_producer import KafkaProducerClient as AppKafkaProducerClient
    from alerts.alert_logic import AlertEngine as AppAlertEngine


# Get logger
from utils.logger import get_logger, LogContext

logger = get_logger(__name__)


# Prometheus metrics
consumer_messages_total = Counter(
    'kafka_consumer_messages_total',
    'Total messages consumed from Kafka',
    ['topic', 'status', 'consumer_group']
)
consumer_lag = Gauge(
    'kafka_consumer_lag',
    'Consumer lag in messages',
    ['topic', 'partition', 'consumer_group']
)
consumer_processing_time = Histogram(
    'kafka_consumer_processing_time_seconds',
    'Time spent processing messages',
    ['topic', 'consumer_group']
)
consumer_errors_total = Counter(
    'kafka_consumer_errors_total',
    'Total consumer errors',
    ['topic', 'error_type', 'consumer_group']
)
consumer_retries_total = Counter(
    'kafka_consumer_retries_total',
    'Total message processing retries',
    ['topic', 'consumer_group']
)


class MessageStatus(Enum):
    """Message processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"


@dataclass
class ConsumerConfig:
    """Configuration for Kafka consumer"""
    bootstrap_servers: str
    group_id: str
    client_id: str
    auto_offset_reset: str = 'latest'
    enable_auto_commit: bool = False
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 3000
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    max_partition_fetch_bytes: int = 1048576
    # Removed isolation_level as it's not supported by kafka-python 2.0.2 consumer
    # isolation_level: str = 'read_uncommitted' 
    consumer_timeout_ms: int = 1000
    
    security_protocol: str = 'PLAINTEXT'
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None


@dataclass
class RetryConfig:
    """Configuration for retry mechanism"""
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True


@dataclass
class ProcessingResult:
    """Result of message processing"""
    success: bool
    message: str
    retry_count: int = 0
    error: Optional[Exception] = None
    processing_time: float = 0.0
    correlation_id: Optional[str] = None


class MessageProcessor:
    """Base class for message processors"""
    def __init__(self, topic_type: 'AppTopicType', settings: 'AppSettings'):
        self.topic_type = topic_type
        self.settings = settings
        self.logger = get_logger(f"{__name__}.{self.__class__.__name__}")
    
    def process(self, message: Dict[str, Any], record: Any) -> ProcessingResult:
        """Process a message - override in subclasses"""
        raise NotImplementedError("Subclasses must implement process method")
    
    def validate_message(self, message: Dict[str, Any]) -> bool:
        """Validate message schema using MessageSchemaValidator (imported locally)."""
        from kafka_handlers.topics import MessageSchemaValidator as AppMessageSchemaValidator
        
        try:
            if self.topic_type == AppMessageSchemaValidator.DETECTION_EVENTS:
                return AppMessageSchemaValidator.validate_detection_event(message)
            elif self.topic_type == AppMessageSchemaValidator.ALERTS:
                return AppMessageSchemaValidator.validate_alert(message)
            elif self.topic_type == AppMessageSchemaValidator.METRICS:
                return AppMessageSchemaValidator.validate_metric(message)
            else:
                return True
        except Exception as e:
            self.logger.error(f"Message validation failed: {e}", exc_info=True)
            return False


class DeadLetterQueue:
    """Dead letter queue for failed messages."""
    
    def __init__(self, redis_client: Optional[redis.Redis], producer_client: 'AppKafkaProducerClient', topic_manager_client: 'AppTopicManager'):
        self.redis_client = redis_client
        self.producer = producer_client
        self.topic_manager = topic_manager_client
        self.logger = get_logger(f"{__name__}.DeadLetterQueue")
    
    def send_to_dlq(self, 
                   original_topic_name: str, 
                   message: Dict[str, Any], 
                   error: Exception,
                   retry_count: int):
        """Send message to dead letter queue."""
        from kafka_handlers.topics import TopicType as AppTopicType
        
        try:
            dlq_message = {
                'original_topic': original_topic_name,
                'original_message': message,
                'error': str(error),
                'error_type': type(error).__name__,
                'retry_count': retry_count,
                'timestamp': datetime.utcnow().isoformat(),
                'correlation_id': message.get('correlation_id', str(uuid.uuid4()))
            }
            
            dlq_topic_type = AppTopicType.SYSTEM_EVENTS
            dlq_topic_name = self.topic_manager.get_topic_name(dlq_topic_type)
            
            success = self.producer.send_message(
                dlq_topic_type,
                dlq_message,
                key=f"dlq_{original_topic_name}",
                batch_mode=False
            )
            
            if success:
                self.logger.info(f"Message sent to DLQ topic: {dlq_topic_name} (original: {original_topic_name})")
            else:
                self.logger.error(f"Failed to send message to DLQ topic: {dlq_topic_name} (original: {original_topic_name})")
            
            if self.redis_client:
                self._store_in_redis(dlq_topic_name, dlq_message)
                
        except Exception as e:
            self.logger.error(f"Critical error sending message to DLQ: {e}", exc_info=True)


    def _store_in_redis(self, dlq_topic: str, message: Dict[str, Any]):
        """Store DLQ message in Redis for monitoring."""
        try:
            json_message = json.dumps(message, default=str)
            
            key = f"dlq:{dlq_topic}:{message['correlation_id']}"
            self.redis_client.setex(
                key,
                86400,
                json_message
            )
            
            list_key = f"dlq_list:{dlq_topic}"
            self.redis_client.lpush(list_key, key)
            self.redis_client.ltrim(list_key, 0, 1000)
            
        except Exception as e:
            self.logger.warning(f"Failed to store DLQ message in Redis: {e}", exc_info=True)


class MessageRetryHandler:
    """Handles message retry logic with exponential backoff."""
    def __init__(self, config: RetryConfig, producer_client: 'AppKafkaProducerClient', dlq_client: 'DeadLetterQueue'):
        self.config = config
        self.producer = producer_client
        self.dlq = dlq_client
        self.retry_queues: Dict[str, Queue] = {}
        self.retry_threads: Dict[str, threading.Thread] = {}
        self.stop_event = threading.Event()
        self.logger = get_logger(f"{__name__}.MessageRetryHandler")
    
    def should_retry(self, retry_count: int, error: Exception) -> bool:
        """Determine if message should be retried."""
        if retry_count >= self.config.max_retries:
            self.logger.debug(f"Max retries ({self.config.max_retries}) reached for error type {type(error).__name__}.")
            return False
        
        non_retryable_errors = (ValueError, TypeError, KeyError)
        if isinstance(error, non_retryable_errors):
            self.logger.debug(f"Error type {type(error).__name__} is non-retryable.")
            return False
        
        return True
    
    def calculate_delay(self, retry_count: int) -> float:
        """Calculate retry delay with exponential backoff and jitter."""
        delay = self.config.initial_delay * (self.config.backoff_multiplier ** retry_count)
        delay = min(delay, self.config.max_delay)
        
        if self.config.jitter:
            import random
            delay = delay * (0.5 + random.random() * 0.5)
        
        return delay
    
    def schedule_retry(self, 
                      topic: str,
                      message: Dict[str, Any],
                      record: Any,
                      processor: MessageProcessor,
                      retry_count: int,
                      error: Exception):
        """Schedule message for retry."""
        if topic not in self.retry_queues:
            self.retry_queues[topic] = Queue()
            self._start_retry_thread(topic)
        
        delay = self.calculate_delay(retry_count)
        retry_time = time.time() + delay
        
        retry_item = {
            'retry_time': retry_time,
            'message': message,
            'record': record,
            'processor': processor,
            'retry_count': retry_count,
            'error': error
        }
        
        self.retry_queues[topic].put(retry_item)
        
        self.logger.info(f"Scheduled retry for message in topic '{topic}' in {delay:.2f}s (attempt {retry_count + 1})")
    
    def _start_retry_thread(self, topic: str):
        """Start retry processing thread for topic."""
        def _retry_processor_target():
            retry_queue = self.retry_queues[topic]
            
            while not self.stop_event.is_set():
                try:
                    retry_item = retry_queue.get(timeout=1)
                    
                    wait_time = retry_item['retry_time'] - time.time()
                    if wait_time > 0:
                        time.sleep(wait_time)
                    
                    processor = retry_item['processor']
                    message = retry_item['message']
                    record = retry_item['record']
                    current_retry_count = retry_item['retry_count']
                    
                    self.logger.info(f"Retrying message for topic '{topic}' (attempt {current_retry_count})...")
                    
                    result = processor.process(message, record)
                    
                    if result.success:
                        self.logger.info(f"Message retry successful for topic '{topic}' after {current_retry_count} attempts.")
                    else:
                        self.logger.error(f"Message retry failed for topic '{topic}' after {current_retry_count} attempts: {result.message}")
                        if self.should_retry(current_retry_count, result.error):
                            self.schedule_retry(
                                topic, message, record, processor, current_retry_count + 1, result.error
                            )
                        else:
                            self.dlq.send_to_dlq(record.topic, message, result.error, current_retry_count)
                            self.logger.info(f"Message sent to DLQ after {current_retry_count} retries (topic: {topic}).")
                    
                    retry_queue.task_done()
                    
                except Empty:
                    continue
                except Exception as e:
                    self.logger.error(f"Unhandled error in retry processor thread for topic '{topic}': {e}", exc_info=True)
        
        thread = threading.Thread(target=_retry_processor_target, daemon=True)
        thread.start()
        self.retry_threads[topic] = thread
        self.logger.info(f"Started retry thread for topic: {topic}")
    
    def stop(self):
        """Stop all retry threads and wait for them to finish."""
        self.stop_event.set()
        for thread in self.retry_threads.values():
            if thread.is_alive():
                thread.join(timeout=5)
                if thread.is_alive():
                    self.logger.warning(f"Retry thread for topic {thread.name} did not terminate gracefully.")
        self.logger.info("Message retry handler stopped.")


class KafkaConsumerClient:
    """Enhanced Kafka consumer with resilience features."""
    
    def __init__(self, 
                 topic_type: 'AppTopicType',
                 processor: MessageProcessor,
                 settings: 'AppSettings',
                 alert_engine_obj: 'AppAlertEngine'
                 ):
        self.topic_type = topic_type
        self.processor = processor
        self.settings = settings
        self.alert_engine = alert_engine_obj
        
        self._initialize_redis()
        
        from kafka_handlers.topics import get_topic_manager as _get_global_topic_manager
        self.topic_manager = _get_global_topic_manager(settings=self.settings)

        from kafka_handlers.kafka_producer import get_kafka_producer as _get_global_kafka_producer
        self.producer = _get_global_kafka_producer(settings=self.settings, topic_manager=self.topic_manager)


        self.dlq = DeadLetterQueue(self.redis_client, self.producer, self.topic_manager)
        self.retry_handler = MessageRetryHandler(RetryConfig(), self.producer, self.dlq)
        
        self.consumer: Optional[kafka_lib.KafkaConsumer] = None
        self.is_running = False
        self.stop_event = threading.Event()
        self.metrics_thread: Optional[threading.Thread] = None
        self.processed_messages: Set[str] = set()
        
        self._initialize_consumer()
        self._start_metrics_collection()

        logger.info(f"KafkaConsumerClient initialized for topic: {self.topic_type.value}")
    
    def _get_consumer_config_from_settings(self) -> ConsumerConfig:
        """Helper to create ConsumerConfig from app settings."""
        return ConsumerConfig(
            bootstrap_servers=self.settings.kafka.bootstrap_servers,
            group_id=self.settings.kafka.consumer_group_id,
            client_id=f"consumer_{self.topic_type.value}_{uuid.uuid4().hex[:8]}",
            auto_offset_reset=self.settings.kafka.auto_offset_reset,
            enable_auto_commit=self.settings.kafka.enable_auto_commit,
            max_poll_records=getattr(self.settings.kafka, 'max_poll_records', 500),
            max_poll_interval_ms=getattr(self.settings.kafka, 'max_poll_interval_ms', 300000),
            session_timeout_ms=getattr(self.settings.kafka, 'session_timeout_ms', 30000),
            heartbeat_interval_ms=getattr(self.settings.kafka, 'heartbeat_interval_ms', 3000),
            fetch_min_bytes=getattr(self.settings.kafka, 'fetch_min_bytes', 1),
            fetch_max_wait_ms=getattr(self.settings.kafka, 'fetch_max_wait_ms', 500),
            max_partition_fetch_bytes=getattr(self.settings.kafka, 'max_partition_fetch_bytes', 1048576),
            # Removed isolation_level from here as it's not supported by kafka-python 2.0.2 consumer
            # isolation_level=getattr(self.settings.kafka, 'isolation_level', 'read_uncommitted'), 
            consumer_timeout_ms=getattr(self.settings.kafka, 'consumer_timeout_ms', 1000),
            security_protocol=getattr(self.settings.kafka, 'security_protocol', 'PLAINTEXT'),
            sasl_mechanism=getattr(self.settings.kafka, 'sasl_mechanism', None),
            sasl_username=getattr(self.settings.kafka, 'sasl_username', None),
            sasl_password=getattr(self.settings.kafka, 'sasl_password', None)
        )
    
    def _initialize_consumer(self):
        """Initialize Kafka consumer with proper configuration."""
        try:
            consumer_config_from_settings = self._get_consumer_config_from_settings()

            consumer_params = {
                'bootstrap_servers': consumer_config_from_settings.bootstrap_servers.split(','),
                'group_id': consumer_config_from_settings.group_id,
                'client_id': consumer_config_from_settings.client_id,
                'auto_offset_reset': consumer_config_from_settings.auto_offset_reset,
                'enable_auto_commit': consumer_config_from_settings.enable_auto_commit,
                'max_poll_records': consumer_config_from_settings.max_poll_records, # Corrected typo here, was consumer_config_from_poll_settings
                'max_poll_interval_ms': consumer_config_from_settings.max_poll_interval_ms,
                'session_timeout_ms': consumer_config_from_settings.session_timeout_ms,
                'heartbeat_interval_ms': consumer_config_from_settings.heartbeat_interval_ms,
                'fetch_min_bytes': consumer_config_from_settings.fetch_min_bytes,
                'fetch_max_wait_ms': consumer_config_from_settings.fetch_max_wait_ms,
                'max_partition_fetch_bytes': consumer_config_from_settings.max_partition_fetch_bytes,
                # Removed from here as well, since it's not in ConsumerConfig anymore
                # 'isolation_level': consumer_config_from_settings.isolation_level, 
                'consumer_timeout_ms': consumer_config_from_settings.consumer_timeout_ms,
                'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
                'key_deserializer': lambda x: x.decode('utf-8') if x else None,
                'security_protocol': consumer_config_from_settings.security_protocol,
            }

            if consumer_config_from_settings.security_protocol != "PLAINTEXT":
                consumer_params.update({
                    'sasl_mechanism': consumer_config_from_settings.sasl_mechanism,
                    'sasl_plain_username': consumer_config_from_settings.sasl_username,
                    'sasl_plain_password': consumer_config_from_settings.sasl_password,
                })
            
            self.consumer = KafkaConsumer(**consumer_params)
            logger.info(f"Kafka consumer initialized for topic: {self.topic_manager.get_topic_name(self.topic_type)}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}", exc_info=True)
            raise
    
    def _initialize_redis(self):
        """Initialize Redis client for deduplication/DLQ."""
        try:
            if self.settings.redis.enabled:
                self.redis_client = redis.Redis(
                    host=self.settings.redis.host,
                    port=self.settings.redis.port,
                    db=self.settings.redis.db,
                    password=self.settings.redis.password,
                    decode_responses=True,
                    socket_timeout=self.settings.redis.socket_timeout,
                    socket_connect_timeout=self.settings.redis.socket_connect_timeout,
                    retry_on_timeout=True,
                    health_check_interval=30
                )
                self.redis_client.ping()
                logger.info("Redis client initialized successfully for KafkaConsumerClient.")
            else:
                logger.info("Redis is disabled in settings for KafkaConsumerClient.")
        except Exception as e:
            logger.warning(f"Failed to initialize Redis client for KafkaConsumerClient: {e}", exc_info=True)
            self.redis_client = None
    
    def _start_metrics_collection(self):
        """Start metrics collection thread."""
        def collect_metrics_target():
            while not self.stop_event.is_set():
                try:
                    if self.consumer:
                        self._collect_consumer_metrics()
                    time.sleep(30)
                except Exception as e:
                    logger.error(f"Error collecting consumer metrics: {e}", exc_info=True)
                    time.sleep(5)
        
        self.metrics_thread = threading.Thread(target=collect_metrics_target, daemon=True)
        self.metrics_thread.start()
        logger.info("Consumer metrics collection thread started.")
    
    def _collect_consumer_metrics(self):
        """Collect consumer lag and other metrics."""
        try:
            assigned_partitions = list(self.consumer.assignment())
            if not assigned_partitions:
                return
            
            end_offsets = self.consumer.end_offsets(assigned_partitions)

            for partition in assigned_partitions:
                committed_offset = self.consumer.committed(partition)
                current_position = self.consumer.position(partition)
                
                high_water_mark = end_offsets.get(partition)

                if high_water_mark is not None and committed_offset is not None and current_position is not None:
                    lag_from_current_position = high_water_mark - current_position

                    consumer_lag.labels(
                        topic=partition.topic,
                        partition=partition.partition,
                        consumer_group=self.settings.kafka.consumer_group_id
                    ).set(lag_from_current_position)

                    logger.debug(f"Consumer Lag for {partition.topic}-{partition.partition}: "
                                 f"Committed={committed_offset}, Current={current_position}, End={high_water_mark}, Lag={lag_from_current_position}")
                elif high_water_mark is None:
                    logger.warning(f"Could not get end offset for {partition.topic}-{partition.partition}. Broker may be unavailable.")
                else:
                    logger.warning(f"Could not get committed/current offset for {partition.topic}-{partition.partition}. Consumer not ready or no messages yet.")

        except KafkaError as e:
            logger.error(f"Kafka error during metrics collection: {e}", exc_info=True)
            consumer_errors_total.labels(topic='_metrics', error_type='KafkaError', consumer_group=self.settings.kafka.consumer_group_id).inc()
        except Exception as e:
            logger.error(f"Unexpected error during consumer metrics collection: {e}", exc_info=True)
            consumer_errors_total.labels(topic='_metrics', error_type='GenericError', consumer_group=self.settings.kafka.consumer_group_id).inc()
    
    def _is_duplicate_message(self, message: Dict[str, Any]) -> bool:
        """Check if message is duplicate using Redis (for deduplication at consumer level)."""
        if not self.redis_client:
            return False
        
        try:
            correlation_id = message.get('correlation_id')
            if not correlation_id:
                return False
            
            key = f"processed_msg:{self.topic_type.value}:{correlation_id}"
            
            if self.redis_client.setnx(key, "1"):
                self.redis_client.expire(key, self.settings.redis.alerts_cache_ttl)
                return False
            else:
                return True
                
        except Exception as e:
            logger.error(f"Error checking duplicate message in Redis: {e}", exc_info=True)
            return False
    
    def _generate_correlation_id(self, record: Any) -> str:
        """Generate correlation ID for message if missing (for logging/tracing)."""
        data = f"{record.topic}:{record.partition}:{record.offset}:{record.timestamp}"
        return hashlib.md5(data.encode()).hexdigest()
    
    def _process_message(self, record: Any) -> ProcessingResult:
        """Process a single message with error handling and deduplication."""
        start_time = time.time()
        correlation_id = None
        
        try:
            message_value = record.value
            if not isinstance(message_value, dict):
                raise TypeError(f"Message value is not a dictionary: {type(message_value)}")

            correlation_id = message_value.get('correlation_id', self._generate_correlation_id(record))
            
            with LogContext(correlation_id=correlation_id, service_name=self.settings.logging.service_name):
                if self._is_duplicate_message(message_value):
                    logger.info(f"Duplicate message ignored from topic '{record.topic}': {correlation_id}")
                    consumer_messages_total.labels(
                        topic=record.topic, status='duplicate', consumer_group=self.settings.kafka.consumer_group_id
                    ).inc()
                    return ProcessingResult(
                        success=True,
                        message="Duplicate message ignored",
                        correlation_id=correlation_id,
                        processing_time=time.time() - start_time
                    )
                
                if not self.processor.validate_message(message_value):
                    raise ValueError(f"Invalid message schema for topic {self.topic_type.value}. Message: {message_value}")
                
                processing_result_from_processor = self.processor.process(message_value, record)
                
                processing_time = time.time() - start_time
                
                status_label = "success" if processing_result_from_processor.success else "failed_final"
                consumer_messages_total.labels(
                    topic=record.topic,
                    status=status_label,
                    consumer_group=self.settings.kafka.consumer_group_id
                ).inc()
                consumer_processing_time.labels(
                    topic=record.topic,
                    consumer_group=self.settings.kafka.consumer_group_id
                ).observe(processing_time)
                
                processing_result_from_processor.correlation_id = correlation_id
                processing_result_from_processor.processing_time = processing_time
                
                return processing_result_from_processor
                
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Error processing message from topic '{record.topic}', offset {record.offset}: {e}", exc_info=True)
            
            consumer_errors_total.labels(
                topic=record.topic,
                error_type=type(e).__name__,
                consumer_group=self.settings.kafka.consumer_group_id
            ).inc()
            
            return ProcessingResult(
                success=False,
                message=f"Processing failed: {str(e)}",
                error=e,
                correlation_id=correlation_id,
                processing_time=processing_time
            )
    
    def _handle_processing_result(self, result: ProcessingResult, record: Any):
        """Handle the result of message processing (commit, retry, DLQ)."""
        if result.success:
            logger.info(f"Message processed successfully from topic '{record.topic}': {result.correlation_id}")
            try:
                self.consumer.commit_async({
                    TopicPartition(record.topic, record.partition): record.offset + 1
                })
            except CommitFailedError as e:
                logger.error(f"Failed to commit offset for {record.topic}-{record.partition} at offset {record.offset}: {e}", exc_info=True)
                consumer_errors_total.labels(topic=record.topic, error_type='CommitFailedError', consumer_group=self.settings.kafka.consumer_group_id).inc()
            except Exception as e:
                logger.error(f"Unexpected error during async commit for {record.topic}-{record.partition}: {e}", exc_info=True)
                consumer_errors_total.labels(topic=record.topic, error_type='UnexpectedCommitError', consumer_group=self.settings.kafka.consumer_group_id).inc()
        else:
            logger.error(f"Message processing failed for {record.topic}, offset {record.offset}: {result.message}")
            
            if self.retry_handler and self.retry_handler.should_retry(result.retry_count, result.error):
                consumer_retries_total.labels(
                    topic=record.topic,
                    consumer_group=self.settings.kafka.consumer_group_id
                ).inc()
                
                self.retry_handler.schedule_retry(
                    record.topic,
                    record.value,
                    record,
                    self.processor,
                    result.retry_count + 1,
                    result.error
                )
            else:
                if self.dlq:
                    self.dlq.send_to_dlq(
                        record.topic,
                        record.value,
                        result.error,
                        result.retry_count
                    )
                    logger.info(f"Message sent to DLQ (topic: {record.topic}, offset: {record.offset})")
                else:
                    logger.critical(f"DLQ not initialized! Message from {record.topic}, offset {record.offset} will be lost: {result.message}")

    
    def subscribe(self, topics_list: Optional[List[str]] = None):
        """Subscribe to topics."""
        from kafka_handlers.topics import TopicType as AppTopicType
        
        try:
            if topics_list is None:
                topic_to_subscribe = self.topic_manager.get_topic_name(self.topic_type)
                self.consumer.subscribe([topic_to_subscribe])
                logger.info(f"Subscribed to topic: {topic_to_subscribe}")
            else:
                self.consumer.subscribe(topics_list)
                logger.info(f"Subscribed to custom topics: {topics_list}")
            
        except Exception as e:
            logger.error(f"Failed to subscribe to topics: {e}", exc_info=True)
            raise
    
    async def start(self):
        """Start consuming messages."""
        if self.is_running:
            logger.warning("Consumer is already running.")
            return
        
        self.is_running = True
        self.stop_event.clear()
        logger.info(f"Starting consumer for topic: {self.topic_type.value} (Group: {self.settings.kafka.consumer_group_id})")
        
        self.subscribe()
        
        self._consumer_thread = threading.Thread(target=self._run_consumer_loop, daemon=True)
        self._consumer_thread.start()
        logger.info(f"Consumer loop started in background thread for topic: {self.topic_type.value}")
    
    def _run_consumer_loop(self):
        """Synchronous loop for polling Kafka messages."""
        try:
            while self.is_running and not self.stop_event.is_set():
                try:
                    message_batch = self.consumer.poll(
                        timeout_ms=self.settings.kafka.consumer_timeout_ms,
                        max_records=self.settings.kafka.max_poll_records
                    )
                    
                    if not message_batch:
                        logger.debug("Kafka poll timeout or no messages, continuing...")
                        continue
                    
                    for topic_partition, records in message_batch.items():
                        for record in records:
                            if self.stop_event.is_set():
                                logger.info(f"Stop event set, breaking from message processing loop for {topic_partition}.")
                                break
                            
                            result = self._process_message(record)
                            self._handle_processing_result(result, record)
                    
                except KafkaTimeoutError:
                    logger.debug("Kafka poll timeout, no messages. Continuing consumer loop.")
                    continue
                except CommitFailedError as e:
                    logger.error(f"CommitFailedError in consumer loop: {e}. Rebalancing or transient network issue?", exc_info=True)
                    consumer_errors_total.labels(topic='_consumer_loop', error_type='CommitFailedError', consumer_group=self.settings.kafka.consumer_group_id).inc()
                    time.sleep(5)
                except KafkaError as e:
                    logger.error(f"General Kafka error in consumer loop: {e}", exc_info=True)
                    consumer_errors_total.labels(topic='_consumer_loop', error_type='KafkaError', consumer_group=self.settings.kafka.consumer_group_id).inc()
                    time.sleep(5)
                except Exception as e:
                    logger.critical(f"Unhandled critical error in consumer loop: {e}", exc_info=True)
                    consumer_errors_total.labels(topic='_consumer_loop', error_type='CriticalError', consumer_group=self.settings.kafka.consumer_group_id).inc()
                    time.sleep(10)
        
        except KeyboardInterrupt:
            logger.info("Consumer loop interrupted by user (background thread).")
        except Exception as e:
            logger.critical(f"Consumer background thread terminated unexpectedly: {e}", exc_info=True)
        finally:
            self._cleanup_on_stop()

    async def stop(self):
        """Stop the consumer gracefully."""
        if not self.is_running:
            logger.info("Consumer is not running, skipping stop.")
            return
        
        logger.info(f"Stopping consumer for topic: {self.topic_type.value}...")
        self.is_running = False
        self.stop_event.set()
        
        if self.metrics_thread and self.metrics_thread.is_alive():
            self.metrics_thread.join(timeout=5)
            if self.metrics_thread.is_alive():
                logger.warning("Metrics thread did not terminate gracefully.")

        if hasattr(self, '_consumer_thread') and self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=10)
            if self._consumer_thread.is_alive():
                logger.warning(f"Consumer loop thread for {self.topic_type.value} did not terminate gracefully.")
        
        self._cleanup_on_stop()
        logger.info(f"Consumer for topic '{self.topic_type.value}' stopped successfully.")

    def _cleanup_on_stop(self):
        """Internal method to perform resource cleanup when consumer stops."""
        if self.retry_handler:
            self.retry_handler.stop()
        
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer client closed.")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer client: {e}", exc_info=True)
        
        if self.redis_client:
            try:
                self.redis_client.close()
                logger.info("Redis connection closed for KafkaConsumerClient.")
            except Exception as e:
                logger.error(f"Error closing Redis connection for KafkaConsumerClient: {e}", exc_info=True)
        
        self.logger.info("KafkaConsumerClient resources cleaned up.")


    async def health_check(self) -> Dict[str, Any]:
        """Get consumer health status."""
        if not self.is_running:
            return {'status': 'not_running', 'message': 'Consumer is not active.'}
        
        health_status = {'status': 'healthy', 'message': 'Consumer is running.'}

        try:
            from kafka_handlers.topics import TopicType as AppTopicType
            consumer_topic_name = self.topic_manager.get_topic_name(self.topic_type)
            topics_metadata = self.consumer.partitions_for_topic(consumer_topic_name)
            if topics_metadata is None or not topics_metadata:
                health_status['status'] = 'degraded'
                health_status['message'] = 'Could not get topic partitions, Kafka connection might be unstable or topic missing.'
                logger.warning(f"Consumer health check: No topic partitions found for {consumer_topic_name}.")

            redis_ok = self._test_redis_connection()
            if not redis_ok:
                health_status['status'] = 'degraded'
                health_status['redis_status'] = 'disconnected'
                health_status['message'] += ' Redis disconnected.'
                logger.warning("Consumer health check: Redis client is not connected.")
            else:
                health_status['redis_status'] = 'connected'

            if not hasattr(self, '_consumer_thread') or not self._consumer_thread.is_alive():
                health_status['status'] = 'unhealthy'
                health_status['message'] = 'Consumer background thread is not running.'
                logger.critical("Consumer health check: Background thread is dead.")
            
            if self.metrics_thread and not self.metrics_thread.is_alive():
                health_status['status'] = 'degraded'
                health_status['message'] += ' Metrics collection thread is not running.'
                logger.warning("Consumer health check: Metrics collection thread is dead.")

            if self.retry_handler:
                for topic, thread in self.retry_handler.retry_threads.items():
                    if not thread.is_alive():
                        health_status['status'] = 'degraded'
                        health_status['message'] += f' Retry thread for {topic} is not running.'
                        logger.warning(f"Consumer health check: Retry thread for {topic} is dead.")

            health_status.update({
                'topic_type': self.topic_type.value,
                'consumer_group': self.settings.kafka.consumer_group_id,
                'assignment': [str(tp) for tp in self.consumer.assignment()] if self.consumer else [],
                'background_thread_alive': hasattr(self, '_consumer_thread') and self._consumer_thread.is_alive(),
            })

        except Exception as e:
            logger.error(f"Error during consumer health check: {e}", exc_info=True)
            health_status = {'status': 'unhealthy', 'message': f'Exception during health check: {str(e)}'}
        
        return health_status

    def _test_redis_connection(self) -> bool:
        """Test Redis connection."""
        try:
            if self.redis_client:
                self.redis_client.ping()
                return True
        except Exception as e:
            logger.debug(f"Redis ping failed: {e}")
        return False


# Specific message processors for different topic types
class DetectionEventProcessor(MessageProcessor):
    """Process detection events from Kafka."""
    
    def __init__(self, settings: 'AppSettings', alert_engine: 'AppAlertEngine'):
        from kafka_handlers.topics import TopicType as AppTopicType
        super().__init__(AppTopicType.DETECTION_EVENTS, settings)
        self.alert_engine = alert_engine
        self.logger = get_logger(f"{__name__}.DetectionEventProcessor")

    def process(self, message: Dict[str, Any], record: Any) -> ProcessingResult:
        """Process detection event message."""
        try:
            detection_type = message.get('detection_type')
            camera_id = message.get('camera_id')
            confidence = message.get('confidence', 0.0)
            
            self.logger.info(f"Processing detection: {detection_type} from camera {camera_id} (offset: {record.offset})")
            
            alert = self.alert_engine.generate_alert(message)

            if alert:
                self.logger.info(f"Alert generated by AlertEngine for {detection_type} on camera {camera_id}: {alert['alert_type']}",
                                 extra={'alert_id': alert.get('id')})
            else:
                self.logger.debug(f"No alert generated for detection: {detection_type} from camera {camera_id}.")
            
            return ProcessingResult(
                success=True,
                message=f"Detection event processed successfully: {detection_type}"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to process detection event: {e}", exc_info=True)
            return ProcessingResult(
                success=False,
                message=f"Failed to process detection event: {str(e)}",
                error=e
            )


class AlertProcessor(MessageProcessor):
    """Process alert messages from Kafka."""
    
    def __init__(self, settings: 'AppSettings'):
        from kafka_handlers.topics import TopicType as AppTopicType
        super().__init__(AppTopicType.ALERTS, settings)
        self.logger = get_logger(f"{__name__}.AlertProcessor")

    def process(self, message: Dict[str, Any], record: Any) -> ProcessingResult:
        """Process alert message."""
        try:
            alert_type = message.get('alert_type')
            camera_id = message.get('camera_id')
            severity = message.get('severity', 'medium')
            
            self.logger.info(f"Processing alert: {alert_type} from camera {camera_id} (offset: {record.offset})")
            
            self.logger.info(f"Alert processed for notification: {alert_type} (severity: {severity})")
            
            return ProcessingResult(
                success=True,
                message=f"Alert processed successfully for notification: {alert_type}"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to process alert message: {e}", exc_info=True)
            return ProcessingResult(
                success=False,
                message=f"Failed to process alert: {str(e)}",
                error=e
            )


# Factory function to create consumers (used by main.py)
def create_consumer(topic_type: 'AppTopicType', 
                   settings_obj: 'AppSettings',
                   alert_engine_obj: 'AppAlertEngine',
                   processor: Optional[MessageProcessor] = None
                   ) -> KafkaConsumerClient:
    """Factory function to create configured consumers."""
    from kafka_handlers.topics import TopicType as AppTopicType
    
    if processor is None:
        if topic_type == AppTopicType.DETECTION_EVENTS:
            processor = DetectionEventProcessor(settings_obj, alert_engine_obj)
        elif topic_type == AppTopicType.ALERTS:
            processor = AlertProcessor(settings_obj)
        else:
            raise ValueError(f"No default processor for topic type: {topic_type.value}")
    
    return KafkaConsumerClient(topic_type, processor, settings_obj, alert_engine_obj)


# AsyncKafkaConsumer for integration into main.py's lifecycle
class AsyncKafkaConsumer:
    """Async wrapper for Kafka consumer client."""
    
    def __init__(self, topic_type: 'AppTopicType', processor: MessageProcessor, settings_obj: 'AppSettings', alert_engine_obj: 'AppAlertEngine'):
        self.topic_type = topic_type
        self.processor = processor
        self.settings = settings_obj
        self.alert_engine = alert_engine_obj
        self.consumer_client: Optional[KafkaConsumerClient] = None
        self.consumer_task: Optional[asyncio.Task] = None
        self.logger = get_logger(f"{__name__}.AsyncKafkaConsumer")
    
    async def start(self):
        """Start async consumer."""
        if self.consumer_client and self.consumer_client.is_running:
            self.logger.warning("Attempted to start an already running consumer.")
            return

        self.consumer_client = KafkaConsumerClient(
            self.topic_type, 
            self.processor, 
            self.settings, 
            self.alert_engine
        )
        
        loop = asyncio.get_event_loop()
        self.consumer_task = loop.run_in_executor(
            None, self.consumer_client._run_consumer_loop
        )
        self.logger.info(f"AsyncKafkaConsumer for {self.topic_type.value} started.")

    async def shutdown(self):
        """Stop async consumer."""
        if self.consumer_client:
            self.consumer_client.stop()
        
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                self.logger.info(f"AsyncKafkaConsumer task for {self.topic_type.value} cancelled.")
            except Exception as e:
                self.logger.error(f"Error while awaiting consumer task shutdown for {self.topic_type.value}: {e}", exc_info=True)
        self.logger.info(f"AsyncKafkaConsumer for {self.topic_type.value} stopped.")


# Main execution for standalone testing (if run directly)
if __name__ == "__main__":
    from dotenv import load_dotenv
    from config.settings import get_settings as _get_app_settings
    from kafka_handlers.topics import TopicType as AppTopicType, get_topic_manager as _get_app_topic_manager
    from kafka_handlers.kafka_producer import get_kafka_producer as _get_app_kafka_producer
    from alerts.alert_logic import AlertEngine as AppAlertEngine, get_alert_generator as _get_app_alert_generator
    from alerts.alert_store import get_alert_store as _get_app_alert_store

    print("--- Running KafkaConsumerClient standalone test ---")
    
    load_dotenv()
    test_settings = _get_app_settings()

    async def _init_alert_system_for_test():
        test_alert_store = await _get_app_alert_store(test_settings)
        test_alert_engine = AppAlertEngine(test_settings, test_alert_store)
        await test_alert_engine.initialize()
        return test_alert_engine
    
    loop = asyncio.get_event_loop()
    if loop.is_running():
        alert_engine_instance = loop.create_task(_init_alert_system_for_test())
    else:
        alert_engine_instance = loop.run_until_complete(_init_alert_system_for_test())

    test_topic_manager = _get_app_topic_manager(test_settings)
    test_kafka_producer = _get_app_kafka_producer(test_settings, test_topic_manager)
    asyncio.run(test_kafka_producer.start())

    print("Ensuring Kafka topics exist for consumer testing...")
    if not test_topic_manager.create_all_topics():
        print("WARNING: Failed to ensure all topics exist. Consumer tests might fail.")
    else:
        print("Topics ready.")

    async def run_consumer_test_suite():
        detection_consumer_client = None
        alert_consumer_client = None
        try:
            print("\n--- Creating consumer for DETECTION_EVENTS ---")
            if isinstance(alert_engine_instance, asyncio.Task):
                resolved_alert_engine = await alert_engine_instance
            else:
                resolved_alert_engine = alert_engine_instance

            detection_processor = DetectionEventProcessor(test_settings, resolved_alert_engine)
            detection_consumer_client = create_consumer(
                AppTopicType.DETECTION_EVENTS,
                test_settings,
                resolved_alert_engine,
                detection_processor
            )
            
            print("Starting detection consumer...")
            await detection_consumer_client.start()

            print("\n--- Creating consumer for ALERTS ---")
            alert_processor = AlertProcessor(test_settings)
            alert_consumer_client = create_consumer(
                AppTopicType.ALERTS,
                test_settings,
                resolved_alert_engine,
                alert_processor
            )
            print("Starting alert consumer...")
            await alert_consumer_client.start()

            print("Consumers running for 15 seconds. Send some dummy messages.")
            print(f"Send detection to: {test_topic_manager.get_topic_name(AppTopicType.DETECTION_EVENTS)}")
            print(f"Send alerts to: {test_topic_manager.get_topic_name(AppTopicType.ALERTS)}")
            
            dummy_detection = {
                'detection_id': str(uuid.uuid4()),
                'timestamp': datetime.utcnow().isoformat(),
                'camera_id': 'test_cam_007',
                'objects': [{'class': 'person', 'confidence': 0.9, 'bbox': [0,0,10,10], 'tracking_id': 't1'}],
                'frame_id': 'frame_test_001',
                'confidence': 0.9,
                'processing_time': 0.05
            }
            print("Sending a dummy detection event to Kafka...")
            test_kafka_producer.send_detection_event(dummy_detection)
            test_kafka_producer.flush()

            await asyncio.sleep(5)

            dummy_alert = {
                'alert_id': str(uuid.uuid4()),
                'timestamp': datetime.utcnow().isoformat(),
                'camera_id': 'test_cam_008',
                'alert_type': 'test_alert',
                'severity': 'LOW',
                'message': 'This is a test alert message.',
                'correlation_id': str(uuid.uuid4())
            }
            print("Sending a dummy alert directly to Kafka...")
            test_kafka_producer.send_alert(dummy_alert)
            test_kafka_producer.flush()

            await asyncio.sleep(10)

            print("\nConsumer health statuses:")
            print("Detection Consumer Health:", await detection_consumer_client.health_check())
            print("Alert Consumer Health:", await alert_consumer_client.health_check())

        except Exception as e:
            print(f"An error occurred during consumer test suite: {e}", file=sys.stderr)
            traceback.print_exc()
        finally:
            if detection_consumer_client:
                print("Stopping detection consumer...")
                await detection_consumer_client.stop()
            if alert_consumer_client:
                print("Stopping alert consumer...")
                await alert_consumer_client.stop()
            
            print("Performing final shutdowns...")
            if isinstance(alert_engine_instance, asyncio.Task):
                 await (await alert_engine_instance).shutdown() # Await the task to get the object, then shutdown
            else:
                 await alert_engine_instance.shutdown()
            await test_kafka_producer.shutdown()
            print("All test shutdowns complete.")

    asyncio.run(run_consumer_test_suite())

    print("\n--- All KafkaConsumerClient standalone tests finished ---")