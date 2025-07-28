# """
# Kafka Topic Management System for Real-Time Analytics Pipeline

# This module provides:
# - Topic definitions with clear naming conventions
# - Dynamic topic creation with proper error handling
# - Topic validation and health checks
# - Schema definitions for message types
# - Topic migration and versioning support
# - Monitoring helpers for topic metrics
# """

# import json
# import time
# from enum import Enum
# from typing import Dict, List, Optional, Any
# from dataclasses import dataclass, asdict
# from datetime import datetime

# from kafka import KafkaProducer, KafkaConsumer
# from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
# from kafka.admin.config_resource import ConfigResource
# from kafka.admin.new_topic import NewTopic
# from kafka.errors import TopicAlreadyExistsError, KafkaError, KafkaTimeoutError
# from confluent_kafka.admin import AdminClient, NewTopic as ConfluentNewTopic
# from confluent_kafka import KafkaException

# from config.settings import get_settings
# from utils.logger import get_logger, log_kafka_event, LogContext

# # Initialize logger and settings
# logger = get_logger(__name__)
# settings = get_settings()


# class TopicType(str, Enum):
#     """Topic type enumeration"""
#     RAW_FRAMES = "raw_frames"
#     DETECTION_EVENTS = "detection_events"
#     ALERTS = "alerts"
#     METRICS = "metrics"
#     HEALTH_CHECKS = "health_checks"
#     AUDIT_LOGS = "audit_logs"
#     NOTIFICATIONS = "notifications"
#     SYSTEM_EVENTS = "system_events"


# class MessageSchema(str, Enum):
#     """Message schema types"""
#     JSON = "json"
#     AVRO = "avro"
#     PROTOBUF = "protobuf"
#     BINARY = "binary"


# @dataclass
# class TopicConfig:
#     """Topic configuration class"""
#     name: str
#     partitions: int
#     replication_factor: int
#     retention_ms: int
#     cleanup_policy: str
#     compression_type: str
#     max_message_bytes: int
#     schema_type: MessageSchema
#     description: str
    
#     # Advanced configuration
#     min_insync_replicas: int = 1
#     segment_ms: int = 604800000  # 7 days
#     segment_bytes: int = 1073741824  # 1GB
#     delete_retention_ms: int = 86400000  # 1 day
    
#     def to_kafka_config(self) -> Dict[str, str]:
#         """Convert to Kafka configuration dictionary"""
#         return {
#             'retention.ms': str(self.retention_ms),
#             'cleanup.policy': self.cleanup_policy,
#             'compression.type': self.compression_type,
#             'max.message.bytes': str(self.max_message_bytes),
#             'min.insync.replicas': str(self.min_insync_replicas),
#             'segment.ms': str(self.segment_ms),
#             'segment.bytes': str(self.segment_bytes),
#             'delete.retention.ms': str(self.delete_retention_ms),
#         }


# class TopicRegistry:
#     """Registry for all Kafka topics"""
    
#     def __init__(self):
#         self.topics: Dict[TopicType, TopicConfig] = {}
#         self._register_default_topics()
    
#     def _register_default_topics(self):
#         """Register default topics for the analytics pipeline"""
        
#         # Raw video frames topic
#         self.topics[TopicType.RAW_FRAMES] = TopicConfig(
#             name=TopicType.RAW_FRAMES.value,
#             partitions=12,  # High throughput for video frames
#             replication_factor=2,
#             retention_ms=3600000,  # 1 hour retention
#             cleanup_policy="delete",
#             compression_type="gzip",
#             max_message_bytes=10485760,  # 10MB for video frames
#             schema_type=MessageSchema.BINARY,
#             description="Raw video frames from cameras for processing"
#         )
        
#         # Detection events topic
#         self.topics[TopicType.DETECTION_EVENTS] = TopicConfig(
#             name=TopicType.DETECTION_EVENTS.value,
#             partitions=6,
#             replication_factor=2,
#             retention_ms=86400000,  # 24 hours
#             cleanup_policy="delete",
#             compression_type="gzip",
#             max_message_bytes=1048576,  # 1MB
#             schema_type=MessageSchema.JSON,
#             description="Object detection events from YOLO processing"
#         )
        
#         # Alerts topic
#         self.topics[TopicType.ALERTS] = TopicConfig(
#             name=TopicType.ALERTS.value,
#             partitions=3,
#             replication_factor=3,  # High availability for alerts
#             retention_ms=604800000,  # 7 days
#             cleanup_policy="delete",
#             compression_type="gzip",
#             max_message_bytes=1048576,  # 1MB
#             schema_type=MessageSchema.JSON,
#             description="Generated alerts from detection events"
#         )
        
#         # Metrics topic
#         self.topics[TopicType.METRICS] = TopicConfig(
#             name=TopicType.METRICS.value,
#             partitions=2,
#             replication_factor=2,
#             retention_ms=2592000000,  # 30 days
#             cleanup_policy="delete",
#             compression_type="gzip",
#             max_message_bytes=262144,  # 256KB
#             schema_type=MessageSchema.JSON,
#             description="System and application metrics"
#         )
        
#         # Health checks topic
#         self.topics[TopicType.HEALTH_CHECKS] = TopicConfig(
#             name=TopicType.HEALTH_CHECKS.value,
#             partitions=1,
#             replication_factor=2,
#             retention_ms=86400000,  # 24 hours
#             cleanup_policy="delete",
#             compression_type="gzip",
#             max_message_bytes=65536,  # 64KB
#             schema_type=MessageSchema.JSON,
#             description="Health check results from all services"
#         )
        
#         # Audit logs topic
#         self.topics[TopicType.AUDIT_LOGS] = TopicConfig(
#             name=TopicType.AUDIT_LOGS.value,
#             partitions=2,
#             replication_factor=3,
#             retention_ms=2592000000,  # 30 days
#             cleanup_policy="delete",
#             compression_type="gzip",
#             max_message_bytes=262144,  # 256KB
#             schema_type=MessageSchema.JSON,
#             description="Audit logs for security and compliance"
#         )
        
#         # Notifications topic
#         self.topics[TopicType.NOTIFICATIONS] = TopicConfig(
#             name=TopicType.NOTIFICATIONS.value,
#             partitions=2,
#             replication_factor=2,
#             retention_ms=86400000,  # 24 hours
#             cleanup_policy="delete",
#             compression_type="gzip",
#             max_message_bytes=262144,  # 256KB
#             schema_type=MessageSchema.JSON,
#             description="User notifications and system announcements"
#         )
        
#         # System events topic
#         self.topics[TopicType.SYSTEM_EVENTS] = TopicConfig(
#             name=TopicType.SYSTEM_EVENTS.value,
#             partitions=2,
#             replication_factor=2,
#             retention_ms=604800000,  # 7 days
#             cleanup_policy="delete",
#             compression_type="gzip",
#             max_message_bytes=262144,  # 256KB
#             schema_type=MessageSchema.JSON,
#             description="System lifecycle and operational events"
#         )
    
#     def get_topic_config(self, topic_type: TopicType) -> TopicConfig:
#         """Get topic configuration by type"""
#         return self.topics.get(topic_type)
    
#     def get_all_topics(self) -> Dict[TopicType, TopicConfig]:
#         """Get all registered topics"""
#         return self.topics.copy()
    
#     def register_topic(self, topic_type: TopicType, config: TopicConfig):
#         """Register a new topic"""
#         self.topics[topic_type] = config
#         logger.info(f"Registered topic: {topic_type.value}", extra={
#             'topic_name': config.name,
#             'partitions': config.partitions,
#             'replication_factor': config.replication_factor
#         })


# # Global topic registry
# topic_registry = TopicRegistry()


# class MessageSchemaValidator:
#     """Validates message schemas for different topic types"""
    
#     @staticmethod
#     def validate_detection_event(message: Dict[str, Any]) -> bool:
#         """Validate detection event message schema"""
#         required_fields = [
#             'detection_id', 'timestamp', 'camera_id', 'objects',
#             'frame_id', 'confidence', 'processing_time'
#         ]
        
#         if not all(field in message for field in required_fields):
#             return False
        
#         # Validate objects structure
#         if not isinstance(message['objects'], list):
#             return False
        
#         for obj in message['objects']:
#             obj_required = ['class', 'confidence', 'bbox', 'tracking_id']
#             if not all(field in obj for field in obj_required):
#                 return False
        
#         return True
    
#     @staticmethod
#     def validate_alert(message: Dict[str, Any]) -> bool:
#         """Validate alert message schema"""
#         required_fields = [
#             'alert_id', 'timestamp', 'camera_id', 'alert_type',
#             'severity', 'message', 'correlation_id'
#         ]
        
#         return all(field in message for field in required_fields)
    
#     @staticmethod
#     def validate_metric(message: Dict[str, Any]) -> bool:
#         """Validate metric message schema"""
#         required_fields = [
#             'metric_name', 'value', 'timestamp', 'service_name'
#         ]
        
#         return all(field in message for field in required_fields)


# class TopicManager:
#     """Manages Kafka topics lifecycle"""
    
#     def __init__(self):
#         self.admin_client = None
#         self.confluent_admin = None
#         self._initialize_admin_clients()
    
#     def _initialize_admin_clients(self):
#         """Initialize Kafka admin clients"""
#         try:
#             # Kafka-python admin client
#             self.admin_client = KafkaAdminClient(
#                 bootstrap_servers=settings.kafka.bootstrap_servers,
#                 client_id="topic_manager",
#                 request_timeout_ms=30000,
#                 connections_max_idle_ms=540000,
#             )
            
#             # Confluent admin client for advanced operations
#             conf = {
#                 'bootstrap.servers': settings.kafka.bootstrap_servers,
#                 'client.id': 'topic_manager_confluent'
#             }
            
#             if settings.kafka.security_protocol != "PLAINTEXT":
#                 conf.update({
#                     'security.protocol': settings.kafka.security_protocol,
#                     'sasl.mechanism': settings.kafka.sasl_mechanism,
#                     'sasl.username': settings.kafka.sasl_username,
#                     'sasl.password': settings.kafka.sasl_password,
#                 })
            
#             self.confluent_admin = AdminClient(conf)
            
#             logger.info("Kafka admin clients initialized")
            
#         except Exception as e:
#             logger.error(f"Failed to initialize Kafka admin clients: {e}")
#             raise
    
#     def get_topic_name(self, topic_type: TopicType) -> str:
#         """Get full topic name with environment prefix"""
#         return f"{settings.environment}_{topic_type.value}"
    
#     def create_topic(self, topic_type: TopicType, validate_only: bool = False) -> bool:
#         """Create a single topic"""
#         config = topic_registry.get_topic_config(topic_type)
#         if not config:
#             logger.error(f"No configuration found for topic: {topic_type.value}")
#             return False
        
#         topic_name = self.get_topic_name(topic_type)
        
#         try:
#             # Create NewTopic instance
#             new_topic = NewTopic(
#                 name=topic_name,
#                 num_partitions=config.partitions,
#                 replication_factor=config.replication_factor,
#                 topic_configs=config.to_kafka_config()
#             )
            
#             if validate_only:
#                 # Validate topic creation without actually creating
#                 result = self.admin_client.create_topics([new_topic], validate_only=True)
#                 for topic, future in result.items():
#                     future.result()  # Will raise exception if validation fails
#                 return True
            
#             # Create the topic
#             result = self.admin_client.create_topics([new_topic], timeout_ms=30000)
            
#             for topic, future in result.items():
#                 try:
#                     future.result()
#                     logger.info(f"Topic {topic_name} created successfully", extra={
#                         'topic': topic_name,
#                         'event_type': 'topic_created',
#                         'partitions': config.partitions,
#                         'replication_factor': config.replication_factor
#                     })
#                     return True
#                 except TopicAlreadyExistsError:
#                     logger.info(f"Topic {topic_name} already exists")
#                     return True
#                 except Exception as e:
#                     logger.error(f"Failed to create topic {topic_name}: {e}")
#                     return False
                    
#         except Exception as e:
#             logger.error(f"Error creating topic {topic_name}: {e}")
#             return False
    
#     def create_all_topics(self, validate_only: bool = False) -> bool:
#         """Create all registered topics"""
#         logger.info("Creating all Kafka topics...")
        
#         success_count = 0
#         total_count = len(topic_registry.topics)
        
#         for topic_type in topic_registry.topics:
#             if self.create_topic(topic_type, validate_only=validate_only):
#                 success_count += 1
        
#         success_rate = (success_count / total_count) * 100
#         logger.info(f"Topic creation completed: {success_count}/{total_count} ({success_rate:.1f}%)")
        
#         return success_count == total_count
    
#     def delete_topic(self, topic_type: TopicType) -> bool:
#         """Delete a topic"""
#         topic_name = self.get_topic_name(topic_type)
        
#         try:
#             result = self.admin_client.delete_topics([topic_name], timeout_ms=30000)
            
#             for topic, future in result.items():
#                 try:
#                     future.result()
#                     logger.info(f"Topic {topic_name} deleted successfully", extra={
#                         'topic': topic_name,
#                         'event_type': 'topic_deleted'
#                     })
#                     return True
#                 except Exception as e:
#                     logger.error(f"Failed to delete topic {topic_name}: {e}")
#                     return False
                    
#         except Exception as e:
#             logger.error(f"Error deleting topic {topic_name}: {e}")
#             return False
    
#     def describe_topic(self, topic_type: TopicType) -> Optional[Dict[str, Any]]:
#         """Describe a topic"""
#         topic_name = self.get_topic_name(topic_type)
        
#         try:
#             metadata = self.admin_client.describe_topics([topic_name])
#             topic_metadata = metadata[topic_name]
            
#             return {
#                 'name': topic_name,
#                 'partitions': len(topic_metadata.partitions),
#                 'replication_factor': len(topic_metadata.partitions[0].replicas) if topic_metadata.partitions else 0,
#                 'partition_details': [
#                     {
#                         'id': p.id,
#                         'leader': p.leader,
#                         'replicas': p.replicas,
#                         'isr': p.isr
#                     }
#                     for p in topic_metadata.partitions
#                 ]
#             }
            
#         except Exception as e:
#             logger.error(f"Error describing topic {topic_name}: {e}")
#             return None
    
#     def list_topics(self) -> List[str]:
#         """List all topics"""
#         try:
#             metadata = self.admin_client.list_topics(timeout_ms=10000)
#             return list(metadata.topics.keys())
#         except Exception as e:
#             logger.error(f"Error listing topics: {e}")
#             return []
    
#     def get_topic_metrics(self, topic_type: TopicType) -> Dict[str, Any]:
#         """Get topic metrics"""
#         topic_name = self.get_topic_name(topic_type)
        
#         try:
#             # Use a temporary consumer to get partition information
#             consumer = KafkaConsumer(
#                 bootstrap_servers=settings.kafka.bootstrap_servers,
#                 consumer_timeout_ms=1000,
#                 enable_auto_commit=False
#             )
            
#             partitions = consumer.partitions_for_topic(topic_name)
#             if not partitions:
#                 return {}
            
#             # Get high water marks for each partition
#             partition_info = {}
#             for partition in partitions:
#                 tp = TopicPartition(topic_name, partition)
#                 high_water_mark = consumer.get_partition_metadata(tp).high_water_mark
#                 partition_info[partition] = {
#                     'high_water_mark': high_water_mark,
#                     'partition_id': partition
#                 }
            
#             consumer.close()
            
#             return {
#                 'topic_name': topic_name,
#                 'partition_count': len(partitions),
#                 'partitions': partition_info
#             }
            
#         except Exception as e:
#             logger.error(f"Error getting metrics for topic {topic_name}: {e}")
#             return {}
    
#     def health_check(self) -> Dict[str, Any]:
#         """Perform health check on Kafka cluster"""
#         try:
#             start_time = time.time()
            
#             # Check if we can list topics
#             topics = self.list_topics()
            
#             # Check admin client connection
#             cluster_metadata = self.admin_client.describe_cluster()
            
#             end_time = time.time()
            
#             return {
#                 'status': 'healthy',
#                 'topic_count': len(topics),
#                 'response_time_ms': (end_time - start_time) * 1000,
#                 'cluster_id': cluster_metadata.cluster_id,
#                 'brokers': [
#                     {
#                         'id': broker.id,
#                         'host': broker.host,
#                         'port': broker.port
#                     }
#                     for broker in cluster_metadata.brokers
#                 ]
#             }
            
#         except Exception as e:
#             logger.error(f"Kafka health check failed: {e}")
#             return {
#                 'status': 'unhealthy',
#                 'error': str(e)
#             }
    
#     def cleanup_expired_topics(self) -> int:
#         """Clean up expired topics (for testing environments)"""
#         if settings.environment == 'production':
#             logger.warning("Topic cleanup disabled in production")
#             return 0
        
#         cleaned_count = 0
#         try:
#             topics = self.list_topics()
#             current_time = time.time()
            
#             for topic in topics:
#                 # Check if topic is old (based on creation time - simplified)
#                 # In a real implementation, you'd check actual topic metadata
#                 if topic.startswith(f"{settings.environment}_temp_"):
#                     # Delete temporary topics
#                     try:
#                         self.admin_client.delete_topics([topic])
#                         cleaned_count += 1
#                         logger.info(f"Cleaned up expired topic: {topic}")
#                     except Exception as e:
#                         logger.warning(f"Failed to cleanup topic {topic}: {e}")
            
#         except Exception as e:
#             logger.error(f"Error during topic cleanup: {e}")
        
#         return cleaned_count


# # Global topic manager instance
# topic_manager = TopicManager()


# def initialize_topics():
#     """Initialize all topics - called during application startup"""
#     logger.info("Initializing Kafka topics...")
    
#     try:
#         # Create all topics
#         success = topic_manager.create_all_topics()
        
#         if success:
#             logger.info("All topics initialized successfully")
#         else:
#             logger.error("Some topics failed to initialize")
            
#         return success
        
#     except Exception as e:
#         logger.error(f"Failed to initialize topics: {e}")
#         return False


# def get_topic_manager() -> TopicManager:
#     """Get the global topic manager instance"""
#     return topic_manager


# def get_topic_registry() -> TopicRegistry:
#     """Get the global topic registry instance"""
#     return topic_registry


# if __name__ == "__main__":
#     # Script to manage topics from command line
#     import sys
    
#     if len(sys.argv) < 2:
#         print("Usage: python topics.py [create|delete|list|describe] [topic_type]")
#         sys.exit(1)
    
#     action = sys.argv[1]
    
#     if action == "create":
#         if len(sys.argv) > 2:
#             topic_type = TopicType(sys.argv[2])
#             topic_manager.create_topic(topic_type)
#         else:
#             topic_manager.create_all_topics()
    
#     elif action == "delete":
#         if len(sys.argv) > 2:
#             topic_type = TopicType(sys.argv[2])
#             topic_manager.delete_topic(topic_type)
#         else:
#             print("Topic type required for delete")
    
#     elif action == "list":
#         topics = topic_manager.list_topics()
#         print(f"Topics ({len(topics)}):")
#         for topic in topics:
#             print(f"  - {topic}")
    
#     elif action == "describe":
#         if len(sys.argv) > 2:
#             topic_type = TopicType(sys.argv[2])
#             info = topic_manager.describe_topic(topic_type)
#             print(json.dumps(info, indent=2))
#         else:
#             print("Topic type required for describe")
    
#     elif action == "health":
#         health = topic_manager.health_check()
#         print(json.dumps(health, indent=2))
    
#     else:
#         print(f"Unknown action: {action}")
#         sys.exit(1)

"""
Kafka Topic Management System for Real-Time Analytics Pipeline

This module provides:
- Topic definitions with clear naming conventions
- Dynamic topic creation with proper error handling
- Topic validation and health checks
- Schema definitions for message types
- Topic migration and versioning support
- Monitoring helpers for topic metrics
"""

import json
import time
from enum import Enum
from typing import Dict, List, Optional, Any, TYPE_CHECKING # Added TYPE_CHECKING
from dataclasses import dataclass, asdict
from datetime import datetime

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka.admin.new_topic import NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError, KafkaTimeoutError
from kafka.structs import TopicPartition # ADDED: Correct import for TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic as ConfluentNewTopic
from confluent_kafka import KafkaException
import threading

# Removed module-level get_settings() to prevent early loading issues
# from config.settings import get_settings

from utils.logger import get_logger, log_kafka_event, LogContext

# Initialize logger. It should get a fallback if settings are not fully loaded yet.
logger = get_logger(__name__)

# Type hinting for settings to avoid runtime import issues
if TYPE_CHECKING:
    from config.settings import Settings as AppSettings


class TopicType(str, Enum):
    """Topic type enumeration"""
    RAW_FRAMES = "raw_frames"
    DETECTION_EVENTS = "detection_events"
    ALERTS = "alerts"
    METRICS = "metrics"
    HEALTH_CHECKS = "health_checks"
    AUDIT_LOGS = "audit_logs"
    NOTIFICATIONS = "notifications"
    SYSTEM_EVENTS = "system_events"


class MessageSchema(str, Enum):
    """Message schema types"""
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"
    BINARY = "binary"


@dataclass
class TopicConfig:
    """Topic configuration class"""
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    cleanup_policy: str
    compression_type: str
    max_message_bytes: int
    schema_type: MessageSchema
    description: str
    
    # Advanced configuration
    min_insync_replicas: int = 1
    segment_ms: int = 604800000  # 7 days
    segment_bytes: int = 1073741824  # 1GB
    delete_retention_ms: int = 86400000  # 1 day
    
    def to_kafka_config(self) -> Dict[str, str]:
        """Convert to Kafka configuration dictionary"""
        return {
            'retention.ms': str(self.retention_ms),
            'cleanup.policy': self.cleanup_policy,
            'compression.type': self.compression_type,
            'max.message.bytes': str(self.max_message_bytes),
            'min.insync.replicas': str(self.min_insync_replicas),
            'segment.ms': str(self.segment_ms),
            'segment.bytes': str(self.segment_bytes),
            'delete.retention.ms': str(self.delete_retention_ms),
        }


class TopicRegistry:
    """Registry for all Kafka topics"""
    
    def __init__(self):
        self.topics: Dict[TopicType, TopicConfig] = {}
        self._register_default_topics()
    
    def _register_default_topics(self):
        """Register default topics for the analytics pipeline"""
        
        # Raw video frames topic
        self.topics[TopicType.RAW_FRAMES] = TopicConfig(
            name=TopicType.RAW_FRAMES.value,
            partitions=12,  # High throughput for video frames
            replication_factor=2,
            retention_ms=3600000,  # 1 hour retention
            cleanup_policy="delete",
            compression_type="gzip",
            max_message_bytes=10485760,  # 10MB for video frames
            schema_type=MessageSchema.BINARY,
            description="Raw video frames from cameras for processing"
        )
        
        # Detection events topic
        self.topics[TopicType.DETECTION_EVENTS] = TopicConfig(
            name=TopicType.DETECTION_EVENTS.value,
            partitions=6,
            replication_factor=2,
            retention_ms=86400000,  # 24 hours
            cleanup_policy="delete",
            compression_type="gzip",
            max_message_bytes=1048576,  # 1MB
            schema_type=MessageSchema.JSON,
            description="Object detection events from YOLO processing"
        )
        
        # Alerts topic
        self.topics[TopicType.ALERTS] = TopicConfig(
            name=TopicType.ALERTS.value,
            partitions=3,
            replication_factor=3,  # High availability for alerts
            retention_ms=604800000,  # 7 days
            cleanup_policy="delete",
            compression_type="gzip",
            max_message_bytes=1048576,  # 1MB
            schema_type=MessageSchema.JSON,
            description="Generated alerts from detection events"
        )
        
        # Metrics topic
        self.topics[TopicType.METRICS] = TopicConfig(
            name=TopicType.METRICS.value,
            partitions=2,
            replication_factor=2,
            retention_ms=2592000000,  # 30 days
            cleanup_policy="delete",
            compression_type="gzip",
            max_message_bytes=262144,  # 256KB
            schema_type=MessageSchema.JSON,
            description="System and application metrics"
        )
        
        # Health checks topic
        self.topics[TopicType.HEALTH_CHECKS] = TopicConfig(
            name=TopicType.HEALTH_CHECKS.value,
            partitions=1,
            replication_factor=2,
            retention_ms=86400000,  # 24 hours
            cleanup_policy="delete",
            compression_type="gzip",
            max_message_bytes=65536,  # 64KB
            schema_type=MessageSchema.JSON,
            description="Health check results from all services"
        )
        
        # Audit logs topic
        self.topics[TopicType.AUDIT_LOGS] = TopicConfig(
            name=TopicType.AUDIT_LOGS.value,
            partitions=2,
            replication_factor=3,
            retention_ms=2592000000,  # 30 days
            cleanup_policy="delete",
            compression_type="gzip",
            max_message_bytes=262144,  # 256KB
            schema_type=MessageSchema.JSON,
            description="Audit logs for security and compliance"
        )
        
        # Notifications topic
        self.topics[TopicType.NOTIFICATIONS] = TopicConfig(
            name=TopicType.NOTIFICATIONS.value,
            partitions=2,
            replication_factor=2,
            retention_ms=86400000,  # 24 hours
            cleanup_policy="delete",
            compression_type="gzip",
            max_message_bytes=262144,  # 256KB
            schema_type=MessageSchema.JSON,
            description="User notifications and system announcements"
        )
        
        # System events topic
        self.topics[TopicType.SYSTEM_EVENTS] = TopicConfig(
            name=TopicType.SYSTEM_EVENTS.value,
            partitions=2,
            replication_factor=2,
            retention_ms=604800000,  # 7 days
            cleanup_policy="delete",
            compression_type="gzip",
            max_message_bytes=262144,  # 256KB
            schema_type=MessageSchema.JSON,
            description="System lifecycle and operational events"
        )
    
    def get_topic_config(self, topic_type: TopicType) -> TopicConfig:
        """Get topic configuration by type"""
        return self.topics.get(topic_type)
    
    def get_all_topics(self) -> Dict[TopicType, TopicConfig]:
        """Get all registered topics"""
        return self.topics.copy()
    
    def register_topic(self, topic_type: TopicType, config: TopicConfig):
        """Register a new topic"""
        self.topics[topic_type] = config
        logger.info(f"Registered topic: {topic_type.value}", extra={
            'topic_name': config.name,
            'partitions': config.partitions,
            'replication_factor': config.replication_factor
        })


# Global topic registry (initialized once)
topic_registry = TopicRegistry()


class MessageSchemaValidator:
    """Validates message schemas for different topic types"""
    
    @staticmethod
    def validate_detection_event(message: Dict[str, Any]) -> bool:
        """Validate detection event message schema"""
        required_fields = [
            'detection_id', 'timestamp', 'camera_id', 'objects',
            'frame_id', 'confidence', 'processing_time'
        ]
        
        if not all(field in message for field in required_fields):
            logger.warning(f"Detection event missing required fields: {set(required_fields) - set(message.keys())}")
            return False
        
        # Validate objects structure
        if not isinstance(message['objects'], list):
            logger.warning("Detection event 'objects' field is not a list.")
            return False
        
        for obj in message['objects']:
            obj_required = ['class', 'confidence', 'bbox', 'tracking_id']
            if not all(field in obj for field in obj_required):
                logger.warning(f"Detection event object missing required fields: {set(obj_required) - set(obj.keys())}")
                return False
        
        return True
    
    @staticmethod
    def validate_alert(message: Dict[str, Any]) -> bool:
        """Validate alert message schema"""
        required_fields = [
            'alert_id', 'timestamp', 'camera_id', 'alert_type',
            'severity', 'message', 'correlation_id'
        ]
        
        if not all(field in message for field in required_fields):
            logger.warning(f"Alert message missing required fields: {set(required_fields) - set(message.keys())}")
            return False
        return True
    
    @staticmethod
    def validate_metric(message: Dict[str, Any]) -> bool:
        """Validate metric message schema"""
        required_fields = [
            'metric_name', 'value', 'timestamp', 'service_name'
        ]
        
        if not all(field in message for field in required_fields):
            logger.warning(f"Metric message missing required fields: {set(required_fields) - set(message.keys())}")
            return False
        return True


class TopicManager:
    """Manages Kafka topics lifecycle."""
    
    def __init__(self, settings: 'AppSettings'): # MODIFIED: Accept settings object
        self.settings = settings # Store settings reference
        self.admin_client = None
        self.confluent_admin = None
        self._initialize_admin_clients()
    
    def _initialize_admin_clients(self):
        """Initialize Kafka admin clients using stored settings."""
        try:
            # Kafka-python admin client
            kafka_config_base = {
                'bootstrap_servers': self.settings.kafka.bootstrap_servers,
                'client_id': "topic_manager_kafka_python",
                'request_timeout_ms': 30000,
                'connections_max_idle_ms': 540000,
            }

            # Add security configuration if needed
            if self.settings.kafka.security_protocol != "PLAINTEXT":
                kafka_config_base.update({
                    'security_protocol': self.settings.kafka.security_protocol,
                    'sasl_mechanism': self.settings.kafka.sasl_mechanism,
                    'sasl_plain_username': self.settings.kafka.sasl_username,
                    'sasl_plain_password': self.settings.kafka.sasl_password,
                })

            self.admin_client = KafkaAdminClient(**kafka_config_base)
            
            # Confluent admin client for advanced operations (e.g., Acl, Config, Quotas)
            confluent_conf = {
                'bootstrap.servers': self.settings.kafka.bootstrap_servers,
                'client.id': 'topic_manager_confluent'
            }
            
            if self.settings.kafka.security_protocol != "PLAINTEXT":
                confluent_conf.update({
                    'security.protocol': self.settings.kafka.security_protocol,
                    'sasl.mechanism': self.settings.kafka.sasl_mechanism,
                    'sasl.username': self.settings.kafka.sasl_username,
                    'sasl.password': self.settings.kafka.sasl_password,
                })
            
            self.confluent_admin = AdminClient(confluent_conf)
            
            logger.info("Kafka admin clients initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka admin clients: {e}", exc_info=True)
            raise # Re-raise to halt startup if critical
    
    def get_topic_name(self, topic_type: TopicType) -> str:
        """Get full topic name with environment prefix using stored settings."""
        return f"{self.settings.kafka.topic_prefix}_{self.settings.environment.value}_{topic_type.value}" # MODIFIED: Use settings correctly
    
    def create_topic(self, topic_type: TopicType, validate_only: bool = False) -> bool:
        """Create a single topic"""
        config = topic_registry.get_topic_config(topic_type)
        if not config:
            logger.error(f"No configuration found for topic: {topic_type.value}")
            return False
        
        topic_name = self.get_topic_name(topic_type)
        
        try:
            # Create NewTopic instance
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=config.partitions,
                replication_factor=config.replication_factor,
                topic_configs=config.to_kafka_config()
            )
            
            if validate_only:
                result = self.admin_client.create_topics([new_topic], validate_only=True)
                for topic, future in result.items():
                    future.result()  # Will raise exception if validation fails
                logger.info(f"Topic {topic_name} validated successfully (not created).")
                return True
            
            # Create the topic
            result = self.admin_client.create_topics([new_topic], timeout_ms=30000)
            
            for topic, future in result.items():
                try:
                    future.result()
                    log_kafka_event(
                        topic=topic_name,
                        event_type='topic_created',
                        message=f"Topic {topic_name} created successfully",
                        partitions=config.partitions,
                        replication_factor=config.replication_factor
                    )
                    return True
                except TopicAlreadyExistsError:
                    logger.info(f"Topic {topic_name} already exists")
                    return True # Already exists is a success for creation intent
                except Exception as e:
                    logger.error(f"Failed to create topic {topic_name}: {e}", exc_info=True)
                    return False
                    
        except Exception as e:
            logger.error(f"Error creating topic {topic_name}: {e}", exc_info=True)
            return False
    
    def create_all_topics(self, validate_only: bool = False) -> bool:
        """Create all registered topics"""
        logger.info("Creating all Kafka topics...")
        
        success_count = 0
        total_count = len(topic_registry.topics)
        
        for topic_type in topic_registry.topics:
            if self.create_topic(topic_type, validate_only=validate_only):
                success_count += 1
        
        success_rate = (success_count / total_count) * 100 if total_count > 0 else 0
        logger.info(f"Topic creation completed: {success_count}/{total_count} ({success_rate:.1f}%)")
        
        return success_count == total_count
    
    def delete_topic(self, topic_type: TopicType) -> bool:
        """Delete a topic"""
        topic_name = self.get_topic_name(topic_type)
        
        try:
            result = self.admin_client.delete_topics([topic_name], timeout_ms=30000)
            
            for topic, future in result.items():
                try:
                    future.result()
                    log_kafka_event(
                        topic=topic_name,
                        event_type='topic_deleted',
                        message=f"Topic {topic_name} deleted successfully"
                    )
                    return True
                except Exception as e:
                    logger.error(f"Failed to delete topic {topic_name}: {e}", exc_info=True)
                    return False
                    
        except Exception as e:
            logger.error(f"Error deleting topic {topic_name}: {e}", exc_info=True)
            return False
    
    def describe_topic(self, topic_type: TopicType) -> Optional[Dict[str, Any]]:
        """Describe a topic"""
        topic_name = self.get_topic_name(topic_type)
        
        try:
            metadata = self.admin_client.describe_topics([topic_name])
            topic_metadata = metadata.topics[topic_name] # Access metadata by .topics
            
            return {
                'name': topic_name,
                'partitions': len(topic_metadata.partitions),
                'replication_factor': len(topic_metadata.partitions[0].replicas) if topic_metadata.partitions else 0,
                'partition_details': [
                    {
                        'id': p.id,
                        'leader': p.leader,
                        'replicas': p.replicas,
                        'isr': p.isr
                    }
                    for p in topic_metadata.partitions
                ]
            }
            
        except Exception as e:
            logger.error(f"Error describing topic {topic_name}: {e}", exc_info=True)
            return None
    
    def list_topics(self) -> List[str]:
        """List all topics"""
        try:
            # Use Confluent admin client for more robust topic listing
            # ListTopicsOptions has no timeout_ms parameter in kafka-python
            futures = self.confluent_admin.list_topics(timeout=10) # Confluent uses seconds
            metadata = futures.result() # Await the future directly here
            return list(metadata.topics.keys())
        except KafkaException as e:
            logger.error(f"Error listing topics with Confluent admin client: {e}", exc_info=True)
            return []
        except Exception as e:
            logger.error(f"Error listing topics: {e}", exc_info=True)
            return []
    
    def get_topic_metrics(self, topic_type: TopicType) -> Dict[str, Any]:
        """Get topic metrics"""
        topic_name = self.get_topic_name(topic_type)
        
        try:
            # Using kafka-python consumer is fine for basic metrics
            consumer = KafkaConsumer(
                bootstrap_servers=self.settings.kafka.bootstrap_servers,
                consumer_timeout_ms=1000,
                enable_auto_commit=False,
                client_id=f"metrics_consumer_{uuid.uuid4().hex[:8]}"
            )
            
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                consumer.close()
                return {}
            
            partition_info = {}
            for partition in partitions:
                tp = TopicPartition(topic_name, partition)
                
                # Assign topic-partition to consumer to get watermarks
                consumer.assign([tp])
                consumer.seek_to_beginning(tp) # Get beginning offset
                beginning_offset = consumer.position(tp)
                consumer.seek_to_end(tp) # Get end offset (high water mark)
                high_water_mark = consumer.position(tp)
                
                partition_info[partition] = {
                    'high_water_mark': high_water_mark,
                    'beginning_offset': beginning_offset,
                    'message_count': high_water_mark - beginning_offset,
                    'partition_id': partition
                }
            
            consumer.close()
            
            return {
                'topic_name': topic_name,
                'partition_count': len(partitions),
                'partitions': partition_info
            }
            
        except Exception as e:
            logger.error(f"Error getting metrics for topic {topic_name}: {e}", exc_info=True)
            return {}
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on Kafka cluster"""
        try:
            start_time = time.time()
            
            # Try to list topics to confirm broker connectivity
            topics = self.list_topics()
            
            # Check admin client connection via describe cluster
            cluster_metadata = self.admin_client.describe_cluster()
            
            end_time = time.time()
            
            return {
                'status': 'healthy',
                'topic_count': len(topics),
                'response_time_ms': (end_time - start_time) * 1000,
                'cluster_id': cluster_metadata.cluster_id,
                'brokers': [
                    {
                        'id': broker.id,
                        'host': broker.host,
                        'port': broker.port
                    }
                    for broker in cluster_metadata.brokers
                ]
            }
            
        except Exception as e:
            logger.error(f"Kafka health check failed: {e}", exc_info=True)
            return {
                'status': 'unhealthy',
                'error': str(e)
            }
    
    def cleanup_expired_topics(self) -> int:
        """Clean up expired topics (for testing environments)"""
        if self.settings.environment.value == 'production': # MODIFIED: Use settings object and its value
            logger.warning("Topic cleanup disabled in production environment.")
            return 0
        
        cleaned_count = 0
        try:
            topics = self.list_topics()
            
            for topic in topics:
                # Check if topic is a temporary topic based on naming convention
                # For more robust cleanup, you'd need to store/retrieve creation timestamps
                # or use specific tags/properties when creating temporary topics.
                # Example: look for topics prefixed with environment and 'temp'
                if topic.startswith(f"{self.settings.kafka.topic_prefix}_{self.settings.environment.value}_temp_"):
                    try:
                        self.admin_client.delete_topics([topic])
                        cleaned_count += 1
                        logger.info(f"Cleaned up expired temporary topic: {topic}")
                    except Exception as e:
                        logger.warning(f"Failed to cleanup topic {topic}: {e}")
            
        except Exception as e:
            logger.error(f"Error during topic cleanup: {e}", exc_info=True)
        
        return cleaned_count

    async def initialize(self):
        """Asynchronous initialization (e.g., connect, verify)."""
        logger.info("TopicManager: Initializing...")
        # No async setup needed here, as admin_clients are initialized in __init__
        # and health_check is synchronous. This method serves as a lifecycle hook.
        # You could add topic creation here if you want to ensure they exist on startup.
        # self.create_all_topics() # This would run it synchronously
        logger.info("TopicManager: Initialization complete.")

    async def shutdown(self):
        """Asynchronous shutdown (e.g., close connections)."""
        logger.info("TopicManager: Shutting down...")
        if self.admin_client:
            try:
                self.admin_client.close()
                logger.info("KafkaAdminClient closed.")
            except Exception as e:
                logger.error(f"Error closing KafkaAdminClient: {e}", exc_info=True)
        # Confluent AdminClient typically doesn't have a close method,
        # it cleans up on object destruction.
        logger.info("TopicManager: Shutdown complete.")


# Global topic manager instance (will be set in main.py)
_topic_manager_instance: Optional[TopicManager] = None
_topic_manager_lock = threading.Lock() # For singleton initialization


def get_topic_manager(settings: Optional['AppSettings'] = None) -> TopicManager: # MODIFIED: Accept settings
    """Get the global topic manager instance (singleton pattern)."""
    global _topic_manager_instance
    if _topic_manager_instance is None:
        if settings is None:
            # Fallback for early calls, but ideally, settings should be passed.
            # This case means settings are not fully ready or passed in.
            from config.settings import get_settings as _get_global_settings
            _settings_fallback = _get_global_settings()
            logger.warning("get_topic_manager called without settings. Using global settings fallback. Ensure proper initialization order.")
            settings = _settings_fallback

        with _topic_manager_lock:
            if _topic_manager_instance is None:
                _topic_manager_instance = TopicManager(settings) # MODIFIED: Pass settings here
                # Optionally create all topics here on first initialization
                # if not _topic_manager_instance.create_all_topics():
                #     logger.critical("Failed to create all necessary Kafka topics during TopicManager initialization!")
    return _topic_manager_instance


def get_topic_registry() -> TopicRegistry:
    """Get the global topic registry instance."""
    return topic_registry


if __name__ == "__main__":
    # This block is for testing `topics.py` directly, usually not run by `main.py`
    # Load .env and get settings for standalone testing
    from dotenv import load_dotenv
    from config.settings import get_settings as _get_app_settings

    load_dotenv()
    test_settings = _get_app_settings() # Get settings for standalone test
    
    # Initialize TopicManager for testing
    test_topic_manager = get_topic_manager(test_settings) # Pass settings to the singleton getter
    
    print("Running TopicManager tests...")
    
    # Create all topics (for testing, ensure Kafka is running)
    print("\n--- Creating all topics ---")
    if test_topic_manager.create_all_topics():
        print("✅ All topics created/verified successfully.")
    else:
        print("❌ Failed to create all topics.")
    
    # List topics
    print("\n--- Listing topics ---")
    topics = test_topic_manager.list_topics()
    print(f"Topics ({len(topics)}):")
    for topic in topics:
        print(f"  - {topic}")
        
    # Describe a topic
    print("\n--- Describing ALERTS topic ---")
    alert_topic_info = test_topic_manager.describe_topic(TopicType.ALERTS)
    print(json.dumps(alert_topic_info, indent=2))

    # Get topic metrics
    print("\n--- Getting ALERTS topic metrics ---")
    alert_metrics = test_topic_manager.get_topic_metrics(TopicType.ALERTS)
    print(json.dumps(alert_metrics, indent=2))
    
    # Health check
    print("\n--- Kafka Cluster Health Check ---")
    health = test_topic_manager.health_check()
    print(json.dumps(health, indent=2))

    # Delete a test topic
    print("\n--- Deleting a temporary topic (if exists) ---")
    # To test delete, create a specific temporary topic
    temp_topic_config = TopicConfig(
        name=f"{test_settings.kafka.topic_prefix}_{test_settings.environment.value}_temp_test",
        partitions=1,
        replication_factor=1,
        retention_ms=3600000,
        cleanup_policy="delete",
        compression_type="none",
        max_message_bytes=1024,
        schema_type=MessageSchema.JSON,
        description="Temporary topic for testing delete"
    )
    temp_topic_type = TopicType("temp_test") # Register a dummy TopicType for this test
    topic_registry.register_topic(temp_topic_type, temp_topic_config)

    if test_topic_manager.create_topic(temp_topic_type):
        print(f"Created temporary topic: {test_topic_manager.get_topic_name(temp_topic_type)}")
        if test_topic_manager.delete_topic(temp_topic_type):
            print(f"✅ Deleted temporary topic: {test_topic_manager.get_topic_name(temp_topic_type)}")
        else:
            print(f"❌ Failed to delete temporary topic: {test_topic_manager.get_topic_name(temp_topic_type)}")
    else:
        print("Could not create temporary topic to test delete.")

    # Cleanup expired topics (will only work in non-production)
    print("\n--- Cleaning up expired topics ---")
    cleaned_count = test_topic_manager.cleanup_expired_topics()
    print(f"Cleaned up {cleaned_count} expired topics.")

    print("\n--- All TopicManager tests finished ---")