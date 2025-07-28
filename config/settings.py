# """
# Configuration Management System for Real-Time Analytics Pipeline

# This module provides a comprehensive configuration management system that:
# - Uses environment variables with fallback defaults
# - Supports multiple environments (dev, staging, production)
# - Validates all settings using Pydantic
# - Supports hot-reloading for certain configurations
# - Provides type safety and comprehensive documentation
# """

# import os
# from enum import Enum
# from typing import Dict, List, Optional, Any
# from pydantic import BaseModel, Field, validator
# from pydantic_settings import BaseSettings


# class Environment(str, Enum):
#     """Supported deployment environments"""
#     DEVELOPMENT = "development"
#     STAGING = "staging"
#     PRODUCTION = "production"
#     TESTING = "testing"


# class LogLevel(str, Enum):
#     """Supported logging levels"""
#     DEBUG = "DEBUG"
#     INFO = "INFO"
#     WARNING = "WARNING"
#     ERROR = "ERROR"
#     CRITICAL = "CRITICAL"


# class AlertSeverity(str, Enum):
#     """Alert severity levels"""
#     LOW = "LOW"
#     MEDIUM = "MEDIUM"
#     HIGH = "HIGH"
#     CRITICAL = "CRITICAL"


# class LoggingConfig(BaseModel):
#     """Logging configuration"""
#     level: LogLevel = Field(default=LogLevel.INFO, description="Logging level")
#     format: str = Field(default="json", description="Log format (json/text)")
#     json_format: bool = Field(default=False, description="Use JSON format for logs")
#     file_enabled: bool = Field(default=True, description="Enable file logging")
#     file_path: str = Field(default="logs/app.log", description="Log file path")
#     max_file_size: int = Field(default=10 * 1024 * 1024, description="Maximum log file size in bytes")
#     backup_count: int = Field(default=5, description="Number of backup log files")
#     console_enabled: bool = Field(default=True, description="Enable console logging")
#     correlation_id_header: str = Field(default="X-Correlation-ID", description="HTTP header for correlation ID")
#     service_name: str = Field(default="video-analytics", description="Service name for structured logging")


# class KafkaConfig(BaseModel):
#     """Kafka configuration"""
#     bootstrap_servers: str = Field(default="localhost:9092", description="Comma-separated list of Kafka broker addresses")
#     topic_prefix: str = Field(default="video_analytics", description="Prefix for all Kafka topics")
#     detection_events_topic: str = Field(default="detection_events", description="Topic for object detection events")
#     alerts_topic: str = Field(default="alerts", description="Topic for generated alerts")
#     raw_frames_topic: str = Field(default="raw_frames", description="Topic for raw video frames")
#     consumer_group_id: str = Field(default="video-analytics-backend", description="Consumer group ID for Kafka consumers")
#     client_id: str = Field(default="video-analytics-backend", description="Kafka client ID")
#     auto_offset_reset: str = Field(default="latest", description="Auto offset reset policy (earliest/latest)")
#     enable_auto_commit: bool = Field(default=True, description="Enable automatic offset commits")
#     auto_commit_interval_ms: int = Field(default=1000, description="Auto commit interval in milliseconds")
#     compression_type: str = Field(default="gzip", description="Compression type for messages (gzip/snappy/lz4)")
#     batch_size: int = Field(default=16384, description="Batch size for producer")
#     linger_ms: int = Field(default=10, description="Linger time in milliseconds")
#     buffer_memory: int = Field(default=33554432, description="Total memory (in bytes) for buffering messages to be sent to Kafka")
#     max_request_size: int = Field(default=1048576, description="Maximum size of a request in bytes (default 1MB)")
#     request_timeout_ms: int = Field(default=30000, description="Request timeout in milliseconds (default 30 seconds)")
#     max_in_flight_requests_per_connection: int = Field(default=5, description="Maximum number of unacknowledged requests the client will send on a single connection")
#     # Removed enable_idempotence as it's not supported by kafka-python 2.0.2
#     acks: str = Field(default="all", description="Acknowledgment policy (0/1/all)")
#     retries: int = Field(default=3, description="Number of retries for failed messages")
#     retry_backoff_ms: int = Field(default=100, description="Backoff time between retries")
#     security_protocol: str = Field(default="PLAINTEXT", description="Security protocol (PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL)")
#     sasl_mechanism: Optional[str] = Field(default=None, description="SASL mechanism (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512)")
#     sasl_username: Optional[str] = Field(default=None, description="SASL username")
#     sasl_password: Optional[str] = Field(default=None, description="SASL password")


# class APIConfig(BaseModel):
#     """API configuration"""
#     host: str = Field(default="localhost", description="API server host")
#     port: int = Field(default=8000, description="API server port")
#     workers: int = Field(default=1, description="Number of API workers")
#     title: str = Field(default="Video Analytics API", description="API title")
#     description: str = Field(default="Real-time video analytics and alert system", description="API description")
#     version: str = Field(default="1.0.0", description="API version")


# class JWTConfig(BaseModel):
#     """JWT configuration"""
#     secret: str = Field(default="your-secret-key-here", description="JWT secret key for token signing")
#     algorithm: str = Field(default="HS256", description="JWT algorithm")
#     expiration_hours: int = Field(default=24, description="JWT expiration time in hours")
#     access_token_expire_minutes: int = Field(default=30, description="Access token expiration time in minutes")
#     refresh_token_expire_days: int = Field(default=7, description="Refresh token expiration time in days")


# class WebSocketConfig(BaseModel):
#     """WebSocket configuration"""
#     host: str = Field(default="localhost", description="WebSocket server host")
#     port: int = Field(default=8001, description="WebSocket server port")
#     max_connections: int = Field(default=1000, description="Maximum concurrent WebSocket connections")
#     ping_interval: int = Field(default=30, description="Ping interval in seconds")
#     ping_timeout: int = Field(default=10, description="Ping timeout in seconds")
#     max_message_size: int = Field(default=1024 * 1024, description="Maximum message size in bytes")
#     connection_timeout: int = Field(default=300, description="Connection timeout in seconds")
#     heartbeat_interval: int = Field(default=25, description="Heartbeat interval in seconds")


# class AlertConfig(BaseModel):
#     """Alert configuration"""
#     threshold_confidence: float = Field(default=0.7, description="Default confidence threshold for alerts")
#     person_detection_threshold: float = Field(default=0.8, description="Confidence threshold for person detection")
#     vehicle_detection_threshold: float = Field(default=0.75, description="Confidence threshold for vehicle detection")
#     cooldown_seconds: int = Field(default=30, description="Alert suppression window in seconds")
#     max_alerts_per_camera: int = Field(default=10, description="Maximum alerts per camera in suppression window")
#     critical_alert_threshold: int = Field(default=5, description="Number of high severity alerts before escalation")
#     escalation_window: int = Field(default=300, description="Escalation window in seconds")
#     enable_grouping: bool = Field(default=True, description="Enable alert grouping for similar events")
#     grouping_time_window: int = Field(default=60, description="Time window for alert grouping in seconds")
#     retention_days: int = Field(default=30, description="Alert retention period in days")
#     max_alerts_in_memory: int = Field(default=10000, description="Maximum alerts to keep in memory")


# class RedisConfig(BaseModel):
#     """Redis configuration"""
#     host: str = Field(default="localhost", description="Redis host")
#     port: int = Field(default=6379, description="Redis port")
#     db: int = Field(default=0, description="Redis database number")
#     password: Optional[str] = Field(default=None, description="Redis password")
#     enabled: bool = Field(default=True, description="Enable Redis caching")  # ADD THIS LINE
#     socket_timeout: int = Field(default=5, description="Socket timeout in seconds")
#     socket_connect_timeout: int = Field(default=5, description="Socket connect timeout in seconds")
#     max_connections: int = Field(default=100, description="Maximum connections in pool")
#     default_ttl: int = Field(default=3600, description="Default cache TTL in seconds")
#     alerts_cache_ttl: int = Field(default=300, description="Alerts cache TTL in seconds")


# class Settings(BaseSettings):
#     """Application settings with environment variable support."""
    
#     # Environment Configuration
#     environment: Environment = Field(
#         default=Environment.DEVELOPMENT,
#         description="Application environment"
#     )
    
#     debug: bool = Field(
#         default=False,
#         description="Enable debug mode"
#     )
    
#     # Nested configurations
#     kafka: KafkaConfig = Field(default_factory=KafkaConfig)
#     api: APIConfig = Field(default_factory=APIConfig)
#     jwt: JWTConfig = Field(default_factory=JWTConfig)
#     websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
#     alerts: AlertConfig = Field(default_factory=AlertConfig)
#     logging: LoggingConfig = Field(default_factory=LoggingConfig)
#     redis: RedisConfig = Field(default_factory=RedisConfig)
    
#     # Application Metadata
#     app_name: str = Field(
#         default="Video Analytics Pipeline",
#         env="APP_NAME",
#         description="Application name"
#     )
    
#     app_version: str = Field(
#         default="1.0.0",
#         env="APP_VERSION",
#         description="Application version"
#     )
    
#     # CORS Configuration
#     allowed_origins: str = Field(
#         default="*",
#         env="ALLOWED_ORIGINS",
#         description="Allowed CORS origins (comma-separated)"
#     )
    
#     allowed_hosts: str = Field(
#         default="*",
#         env="ALLOWED_HOSTS",
#         description="Allowed hosts (comma-separated)"
#     )
    
#     cors_methods: str = Field(
#         default="GET,POST,PUT,DELETE",
#         env="CORS_METHODS",
#         description="Allowed CORS methods (comma-separated)"
#     )
    
#     # Rate Limiting
#     rate_limit_requests: int = Field(
#         default=100,
#         env="RATE_LIMIT_REQUESTS",
#         description="Rate limit: requests per minute"
#     )
    
#     rate_limit_window: int = Field(
#         default=60,
#         env="RATE_LIMIT_WINDOW",
#         description="Rate limit window in seconds"
#     )
    
#     # Hot Reload Configuration
#     hot_reload_enabled: bool = Field(
#         default=True,
#         env="HOT_RELOAD_ENABLED",
#         description="Enable hot reload for development"
#     )
    
#     config_check_interval: int = Field(
#         default=60,
#         env="CONFIG_CHECK_INTERVAL",
#         description="Configuration check interval in seconds"
#     )
    
#     # Circuit Breaker Configuration
#     circuit_failure_threshold: int = Field(
#         default=5,
#         env="CIRCUIT_FAILURE_THRESHOLD",
#         description="Number of failures before opening circuit"
#     )
    
#     circuit_recovery_timeout: int = Field(
#         default=60,
#         env="CIRCUIT_RECOVERY_TIMEOUT",
#         description="Recovery timeout in seconds"
#     )
    
#     # Monitoring Configuration
#     monitoring_prometheus_enabled: bool = Field(
#         default=True,
#         env="MONITORING_PROMETHEUS_ENABLED",
#         description="Enable Prometheus metrics"
#     )
    
#     monitoring_prometheus_port: int = Field(
#         default=8002,
#         env="MONITORING_PROMETHEUS_PORT",
#         description="Prometheus metrics port"
#     )
    
#     monitoring_tracing_enabled: bool = Field(
#         default=True,
#         env="MONITORING_TRACING_ENABLED",
#         description="Enable distributed tracing"
#     )
    
#     monitoring_jaeger_endpoint: str = Field(
#         default="http://localhost:14268/api/traces",
#         env="MONITORING_JAEGER_ENDPOINT",
#         description="Jaeger endpoint for traces"
#     )
    
#     monitoring_health_check_interval: int = Field(
#         default=30,
#         env="MONITORING_HEALTH_CHECK_INTERVAL",
#         description="Health check interval in seconds"
#     )
    
#     monitoring_metrics_collection_interval: int = Field(
#         default=10,
#         env="MONITORING_METRICS_COLLECTION_INTERVAL",
#         description="Metrics collection interval in seconds"
#     )
    
#     @validator('environment')
#     def validate_environment(cls, v):
#         """Validate environment configuration"""
#         if v == Environment.PRODUCTION:
#             # Additional validation for production
#             pass
#         return v
    
#     @validator('jwt')
#     def validate_jwt_secret(cls, v, values):
#         """Validate JWT secret in production"""
#         if values.get('environment') == Environment.PRODUCTION:
#             if v.secret == "your-secret-key-here":
#                 raise ValueError("JWT secret must be changed in production")
#         return v
    
#     @validator('kafka')
#     def validate_kafka_security(cls, v, values):
#         """Validate Kafka security in production"""
#         if values.get('environment') == Environment.PRODUCTION:
#             if v.security_protocol == "PLAINTEXT":
#                 raise ValueError("Production environment requires secure Kafka connection")
#         return v
    
#     def get_allowed_origins(self) -> List[str]:
#         """Get allowed origins as list"""
#         if self.allowed_origins == "*":
#             return ["*"]
#         return [origin.strip() for origin in self.allowed_origins.split(",")]
    
#     def get_allowed_hosts(self) -> List[str]:
#         """Get allowed hosts as list"""
#         if self.allowed_hosts == "*":
#             return ["*"]
#         return [host.strip() for host in self.allowed_hosts.split(",")]
    
#     def get_cors_methods(self) -> List[str]:
#         """Get CORS methods as list"""
#         return [method.strip() for method in self.cors_methods.split(",")]
    
#     def get_kafka_config(self) -> Dict[str, Any]:
#         """Get Kafka configuration as dictionary"""
#         config = {
#             'bootstrap_servers': self.kafka.bootstrap_servers,
#             'auto_offset_reset': self.kafka.auto_offset_reset,
#             'enable_auto_commit': self.kafka.enable_auto_commit,
#             'group_id': self.kafka.consumer_group_id,
#             'security_protocol': self.kafka.security_protocol,
#         }
        
#         if self.kafka.sasl_mechanism:
#             config.update({
#                 'sasl_mechanism': self.kafka.sasl_mechanism,
#                 'sasl_plain_username': self.kafka.sasl_username,
#                 'sasl_plain_password': self.kafka.sasl_password,
#             })
        
#         return config
    
#     def get_producer_config(self) -> Dict[str, Any]:
#         """Get Kafka producer configuration"""
#         config = {
#             'bootstrap_servers': self.kafka.bootstrap_servers,
#             'compression_type': self.kafka.compression_type,
#             'batch_size': self.kafka.batch_size,
#             'linger_ms': self.kafka.linger_ms,
#             'acks': self.kafka.acks,
#             'retries': self.kafka.retries,
#             'retry_backoff_ms': self.kafka.retry_backoff_ms,
#             # Removed enable_idempotence since it's not supported by kafka-python 2.0.2
#             # Note: With acks='all', you still get strong durability guarantees
#         }
        
#         if self.kafka.security_protocol != "PLAINTEXT":
#             config['security_protocol'] = self.kafka.security_protocol
            
#         if self.kafka.sasl_mechanism:
#             config.update({
#                 'sasl_mechanism': self.kafka.sasl_mechanism,
#                 'sasl_plain_username': self.kafka.sasl_username,
#                 'sasl_plain_password': self.kafka.sasl_password,
#             })
        
#         return config
    
#     def get_topic_name(self, topic: str) -> str:
#         """Get full topic name with environment prefix"""
#         return f"{self.kafka.topic_prefix}_{self.environment.value}_{topic}"
    
#     def is_production(self) -> bool:
#         """Check if running in production environment"""
#         return self.environment == Environment.PRODUCTION
    
#     def is_development(self) -> bool:
#         """Check if running in development environment"""
#         return self.environment == Environment.DEVELOPMENT
    
#     @property
#     def bootstrap_servers(self):
#         return self.kafka_bootstrap_servers
    
#     def model_post_init(self, __context):
#         """Post-initialization setup to handle environment variable mapping"""
#         # Map environment variables to nested configs
#         env_mappings = {
#             # Kafka mappings (removed enable_idempotence mapping)
#             'KAFKA_BOOTSTRAP_SERVERS': ('kafka', 'bootstrap_servers'),
#             'KAFKA_TOPIC_PREFIX': ('kafka', 'topic_prefix'),
#             'KAFKA_DETECTION_EVENTS_TOPIC': ('kafka', 'detection_events_topic'),
#             'KAFKA_ALERTS_TOPIC': ('kafka', 'alerts_topic'),
#             'KAFKA_RAW_FRAMES_TOPIC': ('kafka', 'raw_frames_topic'),
#             'KAFKA_CONSUMER_GROUP_ID': ('kafka', 'consumer_group_id'),
#             'KAFKA_CLIENT_ID': ('kafka', 'client_id'),
#             'KAFKA_AUTO_OFFSET_RESET': ('kafka', 'auto_offset_reset'),
#             'KAFKA_ENABLE_AUTO_COMMIT': ('kafka', 'enable_auto_commit'),
#             'KAFKA_AUTO_COMMIT_INTERVAL_MS': ('kafka', 'auto_commit_interval_ms'),
#             'KAFKA_COMPRESSION_TYPE': ('kafka', 'compression_type'),
#             'KAFKA_BATCH_SIZE': ('kafka', 'batch_size'),
#             'KAFKA_LINGER_MS': ('kafka', 'linger_ms'),
#             'KAFKA_BUFFER_MEMORY': ('kafka', 'buffer_memory'),
#             'KAFKA_MAX_REQUEST_SIZE': ('kafka', 'max_request_size'),
#             'KAFKA_REQUEST_TIMEOUT_MS': ('kafka', 'request_timeout_ms'),
#             'KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION': ('kafka', 'max_in_flight_requests_per_connection'),
#             'KAFKA_ACKS': ('kafka', 'acks'),
#             'KAFKA_RETRIES': ('kafka', 'retries'),
#             'KAFKA_RETRY_BACKOFF_MS': ('kafka', 'retry_backoff_ms'),
#             'KAFKA_SECURITY_PROTOCOL': ('kafka', 'security_protocol'),
#             'KAFKA_SASL_MECHANISM': ('kafka', 'sasl_mechanism'),
#             'KAFKA_SASL_USERNAME': ('kafka', 'sasl_username'),
#             'KAFKA_SASL_PASSWORD': ('kafka', 'sasl_password'),
            
#             # API mappings
#             'API_HOST': ('api', 'host'),
#             'API_PORT': ('api', 'port'),
#             'API_WORKERS': ('api', 'workers'),
#             'API_TITLE': ('api', 'title'),
#             'API_DESCRIPTION': ('api', 'description'),
#             'API_VERSION': ('api', 'version'),
            
#             # JWT mappings
#             'JWT_SECRET': ('jwt', 'secret'),
#             'JWT_ALGORITHM': ('jwt', 'algorithm'),
#             'JWT_EXPIRATION_HOURS': ('jwt', 'expiration_hours'),
#             'JWT_ACCESS_TOKEN_EXPIRE_MINUTES': ('jwt', 'access_token_expire_minutes'),
#             'JWT_REFRESH_TOKEN_EXPIRE_DAYS': ('jwt', 'refresh_token_expire_days'),
            
#             # WebSocket mappings
#             'WS_HOST': ('websocket', 'host'),
#             'WS_PORT': ('websocket', 'port'),
#             'WS_MAX_CONNECTIONS': ('websocket', 'max_connections'),
#             'WS_PING_INTERVAL': ('websocket', 'ping_interval'),
#             'WS_PING_TIMEOUT': ('websocket', 'ping_timeout'),
#             'WS_MAX_MESSAGE_SIZE': ('websocket', 'max_message_size'),
#             'WS_CONNECTION_TIMEOUT': ('websocket', 'connection_timeout'),
#             'WS_HEARTBEAT_INTERVAL': ('websocket', 'heartbeat_interval'),
            
#             # Alert mappings
#             'ALERT_THRESHOLD_CONFIDENCE': ('alerts', 'threshold_confidence'),
#             'ALERT_PERSON_DETECTION_THRESHOLD': ('alerts', 'person_detection_threshold'),
#             'ALERT_VEHICLE_DETECTION_THRESHOLD': ('alerts', 'vehicle_detection_threshold'),
#             'ALERT_COOLDOWN_SECONDS': ('alerts', 'cooldown_seconds'),
#             'ALERT_MAX_ALERTS_PER_CAMERA': ('alerts', 'max_alerts_per_camera'),
#             'ALERT_CRITICAL_ALERT_THRESHOLD': ('alerts', 'critical_alert_threshold'),
#             'ALERT_ESCALATION_WINDOW': ('alerts', 'escalation_window'),
#             'ALERT_ENABLE_GROUPING': ('alerts', 'enable_grouping'),
#             'ALERT_GROUPING_TIME_WINDOW': ('alerts', 'grouping_time_window'),
#             'ALERT_RETENTION_DAYS': ('alerts', 'retention_days'),
#             'ALERT_MAX_ALERTS_IN_MEMORY': ('alerts', 'max_alerts_in_memory'),
            
#             # Logging mappings
#             'LOG_LEVEL': ('logging', 'level'),
#             'LOG_FORMAT': ('logging', 'format'),
#             'LOG_JSON_FORMAT': ('logging', 'json_format'),
#             'LOG_FILE_ENABLED': ('logging', 'file_enabled'),
#             'LOG_FILE_PATH': ('logging', 'file_path'),
#             'LOG_MAX_FILE_SIZE': ('logging', 'max_file_size'),
#             'LOG_BACKUP_COUNT': ('logging', 'backup_count'),
#             'LOG_CONSOLE_ENABLED': ('logging', 'console_enabled'),
#             'LOG_CORRELATION_ID_HEADER': ('logging', 'correlation_id_header'),
#             'LOG_SERVICE_NAME': ('logging', 'service_name'),
            
#             # Redis mappings
#             'REDIS_HOST': ('redis', 'host'),
#             'REDIS_PORT': ('redis', 'port'),
#             'REDIS_DB': ('redis', 'db'),
#             'REDIS_PASSWORD': ('redis', 'password'),
#             'REDIS_SOCKET_TIMEOUT': ('redis', 'socket_timeout'),
#             'REDIS_SOCKET_CONNECT_TIMEOUT': ('redis', 'socket_connect_timeout'),
#             'REDIS_MAX_CONNECTIONS': ('redis', 'max_connections'),
#             'REDIS_DEFAULT_TTL': ('redis', 'default_ttl'),
#             'REDIS_ALERTS_CACHE_TTL': ('redis', 'alerts_cache_ttl'),
#         }
        
#         # Apply environment variable overrides
#         for env_var, (config_section, field_name) in env_mappings.items():
#             env_value = os.getenv(env_var)
#             if env_value is not None:
#                 config_obj = getattr(self, config_section)
#                 field_info = config_obj.model_fields.get(field_name)
#                 if field_info:
#                     # Convert string to appropriate type
#                     if field_info.annotation == bool:
#                         env_value = env_value.lower() in ('true', '1', 'yes', 'on')
#                     elif field_info.annotation == int:
#                         env_value = int(env_value)
#                     elif field_info.annotation == float:
#                         env_value = float(env_value)
#                     elif hasattr(field_info.annotation, '__origin__') and field_info.annotation.__origin__ is type(None):
#                         # Handle Optional types
#                         if env_value.lower() in ('none', 'null', ''):
#                             env_value = None
                    
#                     setattr(config_obj, field_name, env_value)

#     class Config:
#         env_file = ".env"
#         env_file_encoding = "utf-8"
#         case_sensitive = False
#         # Allow extra fields to prevent validation errors
#         extra = "allow"


# # Global settings instance
# _settings: Optional[Settings] = None


# def get_settings() -> Settings:
#     """Get application settings (singleton pattern)."""
#     global _settings
#     if _settings is None:
#         _settings = Settings()
#     return _settings


# def reload_settings():
#     """Reload settings from environment."""
#     global _settings
#     _settings = None
#     return get_settings()


# # Configuration validation
# def validate_configuration():
#     """Validate all configuration settings"""
#     try:
#         settings = get_settings()
        
#         # Validate critical configurations
#         if settings.is_production():
#             if settings.jwt.secret == "your-secret-key-here":
#                 raise ValueError("JWT secret key must be changed in production")
            
#             if settings.kafka.security_protocol == "PLAINTEXT":
#                 raise ValueError("Kafka security protocol must be configured for production")
        
#         # Validate port conflicts
#         ports = [
#             settings.api.port,
#             settings.websocket.port,
#             settings.monitoring_prometheus_port
#         ]
        
#         if len(ports) != len(set(ports)):
#             raise ValueError("Port conflicts detected in configuration")
        
#         return True
        
#     except Exception as e:
#         print(f"Configuration validation failed: {e}")
#         return False


# if __name__ == "__main__":
#     # Test configuration loading
#     if validate_configuration():
#         print("✅ Configuration validation passed")
#         settings = get_settings()
#         print(f"Environment: {settings.environment}")
#         print(f"Kafka Topics: {settings.get_topic_name('alerts')}")
#         print(f"API Port: {settings.api.port}")
#         print(f"WebSocket Port: {settings.websocket.port}")
#         print(f"Log Level: {settings.logging.level}")
#     else:
#         print("❌ Configuration validation failed")

# """
# Configuration Management System for Real-Time Analytics Pipeline

# This module provides a comprehensive configuration management system that:
# - Uses environment variables with fallback defaults
# - Supports multiple environments (dev, staging, production)
# - Validates all settings using Pydantic
# - Supports hot-reloading for certain configurations
# - Provides type safety and comprehensive documentation
# """

# import os
# from enum import Enum
# from typing import Dict, List, Optional, Any, Union
# from pydantic import BaseModel, Field, validator
# from pydantic_settings import BaseSettings

# from dotenv import load_dotenv # ADDED: Import load_dotenv

# load_dotenv() # ADDED: Load .env file immediately when settings.py is imported


# class Environment(str, Enum):
#     """Supported deployment environments"""
#     DEVELOPMENT = "development"
#     STAGING = "staging"
#     PRODUCTION = "production"
#     TESTING = "testing"


# class LogLevel(str, Enum):
#     """Supported logging levels"""
#     DEBUG = "DEBUG"
#     INFO = "INFO"
#     WARNING = "WARNING"
#     ERROR = "ERROR"
#     CRITICAL = "CRITICAL"


# class AlertSeverity(str, Enum):
#     """Alert severity levels"""
#     LOW = "LOW"
#     MEDIUM = "MEDIUM"
#     HIGH = "HIGH"
#     CRITICAL = "CRITICAL"


# class LoggingConfig(BaseModel):
#     """Logging configuration"""
#     level: LogLevel = Field(default=LogLevel.INFO, description="Logging level")
#     format: str = Field(default="json", description="Log format (json/text)")
#     json_format: bool = Field(default=False, description="Use JSON format for logs")
#     file_enabled: bool = Field(default=True, description="Enable file logging")
#     file_path: str = Field(default="logs/app.log", description="Log file path")
#     max_file_size: int = Field(default=10 * 1024 * 1024, description="Maximum log file size in bytes")
#     backup_count: int = Field(default=5, description="Number of backup log files")
#     console_enabled: bool = Field(default=True, description="Enable console logging")
#     correlation_id_header: str = Field(default="X-Correlation-ID", description="HTTP header for correlation ID")
#     service_name: str = Field(default="video-analytics", description="Service name for structured logging")


# class KafkaConfig(BaseModel):
#     """Kafka configuration"""
#     bootstrap_servers: str = Field(default="localhost:9092", description="Comma-separated list of Kafka broker addresses")
#     topic_prefix: str = Field(default="video_analytics", description="Prefix for all Kafka topics")
#     detection_events_topic: str = Field(default="detection_events", description="Topic for object detection events")
#     alerts_topic: str = Field(default="alerts", description="Topic for generated alerts")
#     raw_frames_topic: str = Field(default="raw_frames", description="Topic for raw video frames")
#     consumer_group_id: str = Field(default="video-analytics-backend", description="Consumer group ID for Kafka consumers")
#     client_id: str = Field(default="video-analytics-backend", description="Kafka client ID")
#     auto_offset_reset: str = Field(default="latest", description="Auto offset reset policy (earliest/latest)")
#     enable_auto_commit: bool = Field(default=True, description="Enable automatic offset commits")
#     auto_commit_interval_ms: int = Field(default=1000, description="Auto commit interval in milliseconds")
#     compression_type: str = Field(default="gzip", description="Compression type for messages (gzip/snappy/lz4)")
#     batch_size: int = Field(default=16384, description="Batch size for producer")
#     linger_ms: int = Field(default=10, description="Linger time in milliseconds")
#     buffer_memory: int = Field(default=33554432, description="Total memory (in bytes) for buffering messages to be sent to Kafka")
#     max_request_size: int = Field(default=1048576, description="Maximum size of a request in bytes (default 1MB)")
#     request_timeout_ms: int = Field(default=30000, description="Request timeout in milliseconds (default 30 seconds)")
#     max_in_flight_requests_per_connection: int = Field(default=5, description="Maximum number of unacknowledged requests the client will send on a single connection")
#     acks: str = Field(default="all", description="Acknowledgment policy (0/1/all)")
#     retries: int = Field(default=3, description="Number of retries for failed messages")
#     retry_backoff_ms: int = Field(default=100, description="Backoff time between retries")
#     security_protocol: str = Field(default="PLAINTEXT", description="Security protocol (PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL)")
#     sasl_mechanism: Optional[str] = Field(default=None, description="SASL mechanism (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512)")
#     sasl_username: Optional[str] = Field(default=None, description="SASL username")
#     sasl_password: Optional[str] = Field(default=None, description="SASL password")


# class APIConfig(BaseModel):
#     """API configuration"""
#     host: str = Field(default="localhost", description="API server host")
#     port: int = Field(default=8000, description="API server port")
#     workers: int = Field(default=1, description="Number of API workers")
#     title: str = Field(default="Video Analytics API", description="API title")
#     description: str = Field(default="Real-time video analytics and alert system", description="API description")
#     version: str = Field(default="1.0.0", description="API version")
#     database_url: str = Field(default="sqlite:///./auth.db", description="Database URL for authentication and other API data")
#     api_key_prefix: str = Field(default="ak_", description="Prefix for API keys")


# class JWTConfig(BaseModel):
#     """JWT configuration"""
#     # *** THIS IS THE KEY CHANGE ***
#     # Make 'secret' a required field and tell Pydantic-settings to look for JWT_SECRET in environment
#     secret: str = Field(..., env="JWT_SECRET", description="JWT secret key for token signing (REQUIRED from env: JWT_SECRET)")
#     algorithm: str = Field(default="HS256", description="JWT algorithm")
#     expiration_hours: int = Field(default=24, description="JWT expiration time in hours")
#     access_token_expire_minutes: int = Field(default=30, description="Access token expiration time in minutes")
#     refresh_token_expire_days: int = Field(default=7, description="Refresh token expiration time in days")



# class WebSocketConfig(BaseModel):
#     """WebSocket configuration"""
#     host: str = Field(default="localhost", description="WebSocket server host")
#     port: int = Field(default=8001, description="WebSocket server port")
#     max_connections: int = Field(default=1000, description="Maximum concurrent WebSocket connections")
#     ping_interval: int = Field(default=30, description="Ping interval in seconds")
#     ping_timeout: int = Field(default=10, description="Ping timeout in seconds")
#     max_message_size: int = Field(default=1024 * 1024, description="Maximum message size in bytes")
#     connection_timeout: int = Field(default=300, description="Connection timeout in seconds")
#     heartbeat_interval: int = Field(default=25, description="Heartbeat interval in seconds")
#     enable_persistence: bool = Field(default=False, description="Enable message persistence for WebSockets (requires PostgreSQL)")
#     postgres_url: str = Field(default="postgresql+asyncpg://user:pass@localhost/db", description="PostgreSQL URL for WebSocket message persistence")


# class AlertConfig(BaseModel):
#     """Alert configuration"""
#     threshold_confidence: float = Field(default=0.7, description="Default confidence threshold for alerts")
#     person_detection_threshold: float = Field(default=0.8, description="Confidence threshold for person detection")
#     vehicle_detection_threshold: float = Field(default=0.75, description="Confidence threshold for vehicle detection")
#     cooldown_seconds: int = Field(default=30, description="Alert suppression window in seconds")
#     max_alerts_per_camera: int = Field(default=10, description="Maximum alerts per camera in suppression window")
#     critical_alert_threshold: int = Field(default=5, description="Number of high severity alerts before escalation")
#     escalation_window: int = Field(default=300, description="Escalation window in seconds")
#     enable_grouping: bool = Field(default=True, description="Enable alert grouping for similar events")
#     grouping_time_window: int = Field(default=60, description="Time window for alert grouping in seconds")
#     retention_days: int = Field(default=30, description="Alert retention period in days")
#     max_alerts_in_memory: int = Field(default=10000, description="Maximum alerts to keep in memory")


# class RedisConfig(BaseModel):
#     """Redis configuration"""
#     host: str = Field(default="localhost", description="Redis host")
#     port: int = Field(default=6379, description="Redis port")
#     db: int = Field(default=0, description="Redis database number")
#     password: Optional[str] = Field(default=None, description="Redis password")
#     enabled: bool = Field(default=True, description="Enable Redis caching")
#     socket_timeout: int = Field(default=5, description="Socket timeout in seconds")
#     socket_connect_timeout: int = Field(default=5, description="Socket connect timeout in seconds")
#     max_connections: int = Field(default=100, description="Maximum connections in pool")
#     default_ttl: int = Field(default=3600, description="Default cache TTL in seconds")
#     alerts_cache_ttl: int = Field(default=300, description="Alerts cache TTL in seconds")


# class Settings(BaseSettings):
#     """Application settings with environment variable support."""
    
#     # Environment Configuration
#     environment: Environment = Field(
#         default=Environment.DEVELOPMENT,
#         description="Application environment"
#     )
    
#     debug: bool = Field(
#         default=False,
#         description="Enable debug mode"
#     )
    
#     # Nested configurations
#     kafka: KafkaConfig = Field(default_factory=KafkaConfig)
#     api: APIConfig = Field(default_factory=APIConfig)
#     jwt: JWTConfig = Field(default_factory=JWTConfig) 
#     websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
#     alerts: AlertConfig = Field(default_factory=AlertConfig)
#     logging: LoggingConfig = Field(default_factory=LoggingConfig)
#     redis: RedisConfig = Field(default_factory=RedisConfig)
    
#     # Application Metadata
#     app_name: str = Field(
#         default="Video Analytics Pipeline",
#         env="APP_NAME",
#         description="Application name"
#     )
    
#     app_version: str = Field(
#         default="1.0.0",
#         env="APP_VERSION",
#         description="Application version"
#     )
    
#     # CORS Configuration
#     allowed_origins: str = Field(
#         default="*",
#         env="ALLOWED_ORIGINS",
#         description="Allowed CORS origins (comma-separated)"
#     )
    
#     allowed_hosts: str = Field(
#         default="*",
#         env="ALLOWED_HOSTS",
#         description="Allowed hosts (comma-separated)"
#     )
    
#     cors_methods: str = Field(
#         default="GET,POST,PUT,DELETE",
#         env="CORS_METHODS",
#         description="Allowed CORS methods (comma-separated)"
#     )
    
#     # Rate Limiting
#     rate_limit_requests: int = Field(
#         default=100,
#         env="RATE_LIMIT_REQUESTS",
#         description="Rate limit: requests per minute"
#     )
    
#     rate_limit_window: int = Field(
#         default=60,
#         env="RATE_LIMIT_WINDOW",
#         description="Rate limit window in seconds"
#     )
    
#     # Hot Reload Configuration
#     hot_reload_enabled: bool = Field(
#         default=True,
#         env="HOT_RELOAD_ENABLED",
#         description="Enable hot reload for development"
#     )
    
#     config_check_interval: int = Field(
#         default=60,
#         env="CONFIG_CHECK_INTERVAL",
#         description="Configuration check interval in seconds"
#     )
    
#     # Circuit Breaker Configuration
#     circuit_failure_threshold: int = Field(
#         default=5,
#         env="CIRCUIT_FAILURE_THRESHOLD",
#         description="Number of failures before opening circuit"
#     )
    
#     circuit_recovery_timeout: int = Field(
#         default=60,
#         env="CIRCUIT_RECOVERY_TIMEOUT",
#         description="Recovery timeout in seconds"
#     )
    
#     # Monitoring Configuration
#     monitoring_prometheus_enabled: bool = Field(
#         default=True,
#         env="MONITORING_PROMETHEUS_ENABLED",
#         description="Enable Prometheus metrics"
#     )
    
#     monitoring_prometheus_port: int = Field(
#         default=8002,
#         env="MONITORING_PROMETHEUS_PORT",
#         description="Prometheus metrics port"
#     )
    
#     monitoring_tracing_enabled: bool = Field(
#         default=True,
#         env="MONITORING_TRACING_ENABLED",
#         description="Enable distributed tracing"
#     )
    
#     monitoring_jaeger_endpoint: str = Field(
#         default="http://localhost:14268/api/traces",
#         env="MONITORING_JAEGER_ENDPOINT",
#         description="Jaeger endpoint for traces"
#     )
    
#     monitoring_health_check_interval: int = Field(
#         default=30,
#         env="MONITORING_HEALTH_CHECK_INTERVAL",
#         description="Health check interval in seconds"
#     )
    
#     monitoring_metrics_collection_interval: int = Field(
#         default=10,
#         env="MONITORING_METRICS_COLLECTION_INTERVAL",
#         description="Metrics collection interval in seconds"
#     )
    
#     @validator('environment')
#     def validate_environment(cls, v):
#         """Validate environment configuration"""
#         if v == Environment.PRODUCTION:
#             # Additional validation for production
#             pass
#         return v
    
#     @validator('jwt')
#     def validate_jwt_secret(cls, v, values):
#         """Validate JWT secret in production"""
#         if values.get('environment') == Environment.PRODUCTION:
#             if v.secret == "your-secret-key-here": # Ensure this is changed from default
#                 raise ValueError("JWT secret must be changed in production")
#         return v
    
#     @validator('kafka')
#     def validate_kafka_security(cls, v, values):
#         """Validate Kafka security in production"""
#         if values.get('environment') == Environment.PRODUCTION:
#             if v.security_protocol == "PLAINTEXT":
#                 raise ValueError("Production environment requires secure Kafka connection")
#         return v
    
#     def get_allowed_origins(self) -> List[str]:
#         """Get allowed origins as list"""
#         if self.allowed_origins == "*":
#             return ["*"]
#         return [origin.strip() for origin in self.allowed_origins.split(",")]
    
#     def get_allowed_hosts(self) -> List[str]:
#         """Get allowed hosts as list"""
#         if self.allowed_hosts == "*":
#             return ["*"]
#         return [host.strip() for host in self.allowed_hosts.split(",")]
    
#     def get_cors_methods(self) -> List[str]:
#         """Get CORS methods as list"""
#         return [method.strip() for method in self.cors_methods.split(",")]
    
#     def get_kafka_config(self) -> Dict[str, Any]:
#         """Get Kafka configuration as dictionary"""
#         config = {
#             'bootstrap_servers': self.kafka.bootstrap_servers,
#             'auto_offset_reset': self.kafka.auto_offset_reset,
#             'enable_auto_commit': self.kafka.enable_auto_commit,
#             'group_id': self.kafka.consumer_group_id,
#             'security_protocol': self.kafka.security_protocol,
#         }
        
#         if self.kafka.sasl_mechanism:
#             config.update({
#                 'sasl_mechanism': self.kafka.sasl_mechanism,
#                 'sasl_plain_username': self.kafka.sasl_username,
#                 'sasl_plain_password': self.kafka.sasl_password,
#             })
        
#         return config
    
#     def get_producer_config(self) -> Dict[str, Any]:
#         """Get Kafka producer configuration"""
#         config = {
#             'bootstrap_servers': self.kafka.bootstrap_servers,
#             'compression_type': self.kafka.compression_type,
#             'batch_size': self.kafka.batch_size,
#             'linger_ms': self.kafka.linger_ms,
#             'acks': self.kafka.acks,
#             'retries': self.kafka.retries,
#             'retry_backoff_ms': self.kafka.retry_backoff_ms,
#         }
        
#         if self.kafka.security_protocol != "PLAINTEXT":
#             config['security_protocol'] = self.kafka.security_protocol
            
#         if self.kafka.sasl_mechanism:
#             config.update({
#                 'sasl_mechanism': self.kafka.sasl_mechanism,
#                 'sasl_plain_username': self.kafka.sasl_username,
#                 'sasl_plain_password': self.kafka.sasl_password,
#             })
        
#         return config
    
#     def get_topic_name(self, topic: str) -> str:
#         """Get full topic name with environment prefix"""
#         return f"{self.kafka.topic_prefix}_{self.environment.value}_{topic}"
    
#     def is_production(self) -> bool:
#         """Check if running in production environment"""
#         return self.environment == Environment.PRODUCTION
    
#     def is_development(self) -> bool:
#         """Check if running in development environment"""
#         return self.environment == Environment.DEVELOPMENT
    
#     @property
#     def bootstrap_servers(self):
#         return self.kafka.bootstrap_servers
    
#     def model_post_init(self, __context: Any) -> None:
#         """
#         Post-initialization hook to apply environment variables to nested Pydantic models.
#         This handles cases where `env=` is not directly used on fields within nested `BaseModel`s,
#         or for complex type conversions.
#         """
#         # This mapping assumes environment variable names generally correspond to nested paths
#         # or are explicitly defined for specific fields within sub-models.
#         env_mappings = {
#             # Kafka mappings
#             'KAFKA_BOOTSTRAP_SERVERS': ('kafka', 'bootstrap_servers'),
#             'KAFKA_TOPIC_PREFIX': ('kafka', 'topic_prefix'),
#             'KAFKA_DETECTION_EVENTS_TOPIC': ('kafka', 'detection_events_topic'),
#             'KAFKA_ALERTS_TOPIC': ('kafka', 'alerts_topic'),
#             'KAFKA_RAW_FRAMES_TOPIC': ('kafka', 'raw_frames_topic'),
#             'KAFKA_CONSUMER_GROUP_ID': ('kafka', 'consumer_group_id'),
#             'KAFKA_CLIENT_ID': ('kafka', 'client_id'),
#             'KAFKA_AUTO_OFFSET_RESET': ('kafka', 'auto_offset_reset'),
#             'KAFKA_ENABLE_AUTO_COMMIT': ('kafka', 'enable_auto_commit'),
#             'KAFKA_AUTO_COMMIT_INTERVAL_MS': ('kafka', 'auto_commit_interval_ms'),
#             'KAFKA_COMPRESSION_TYPE': ('kafka', 'compression_type'),
#             'KAFKA_BATCH_SIZE': ('kafka', 'batch_size'),
#             'KAFKA_LINGER_MS': ('kafka', 'linger_ms'),
#             'KAFKA_BUFFER_MEMORY': ('kafka', 'buffer_memory'),
#             'KAFKA_MAX_REQUEST_SIZE': ('kafka', 'max_request_size'),
#             'KAFKA_REQUEST_TIMEOUT_MS': ('kafka', 'request_timeout_ms'),
#             'KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION': ('kafka', 'max_in_flight_requests_per_connection'),
#             'KAFKA_ACKS': ('kafka', 'acks'),
#             'KAFKA_RETRIES': ('kafka', 'retries'),
#             'KAFKA_RETRY_BACKOFF_MS': ('kafka', 'retry_backoff_ms'),
#             'KAFKA_SECURITY_PROTOCOL': ('kafka', 'security_protocol'),
#             'KAFKA_SASL_MECHANISM': ('kafka', 'sasl_mechanism'),
#             'KAFKA_SASL_USERNAME': ('kafka', 'sasl_username'),
#             'KAFKA_SASL_PASSWORD': ('kafka', 'sasl_password'),
            
#             # API mappings
#             'API_HOST': ('api', 'host'),
#             'API_PORT': ('api', 'port'),
#             'API_WORKERS': ('api', 'workers'),
#             'API_TITLE': ('api', 'title'),
#             'API_DESCRIPTION': ('api', 'description'),
#             'API_VERSION': ('api', 'version'),
#             'API_DATABASE_URL': ('api', 'database_url'),
#             'API_API_KEY_PREFIX': ('api', 'api_key_prefix'),
            
#             # JWT mappings - JWT_SECRET is handled directly by env= on field definition
#             # but other JWT related fields that don't have 'env=' need manual mapping
#             'JWT_ALGORITHM': ('jwt', 'algorithm'),
#             'JWT_EXPIRATION_HOURS': ('jwt', 'expiration_hours'),
#             'JWT_ACCESS_TOKEN_EXPIRE_MINUTES': ('jwt', 'access_token_expire_minutes'),
#             'JWT_REFRESH_TOKEN_EXPIRE_DAYS': ('jwt', 'refresh_token_expire_days'),
            
#             # WebSocket mappings
#             'WS_HOST': ('websocket', 'host'),
#             'WS_PORT': ('websocket', 'port'),
#             'WS_MAX_CONNECTIONS': ('websocket', 'max_connections'),
#             'WS_PING_INTERVAL': ('websocket', 'ping_interval'),
#             'WS_PING_TIMEOUT': ('websocket', 'ping_timeout'),
#             'WS_MAX_MESSAGE_SIZE': ('websocket', 'max_message_size'),
#             'WS_CONNECTION_TIMEOUT': ('websocket', 'connection_timeout'),
#             'WS_HEARTBEAT_INTERVAL': ('websocket', 'heartbeat_interval'),
#             'WS_ENABLE_PERSISTENCE': ('websocket', 'enable_persistence'),
#             'WS_POSTGRES_URL': ('websocket', 'postgres_url'),
            
#             # Alert mappings
#             'ALERT_THRESHOLD_CONFIDENCE': ('alerts', 'threshold_confidence'),
#             'ALERT_PERSON_DETECTION_THRESHOLD': ('alerts', 'person_detection_threshold'),
#             'ALERT_VEHICLE_DETECTION_THRESHOLD': ('alerts', 'vehicle_detection_threshold'),
#             'ALERT_COOLDOWN_SECONDS': ('alerts', 'cooldown_seconds'),
#             'ALERT_MAX_ALERTS_PER_CAMERA': ('alerts', 'max_alerts_per_camera'),
#             'ALERT_CRITICAL_ALERT_THRESHOLD': ('alerts', 'critical_alert_threshold'),
#             'ALERT_ESCALATION_WINDOW': ('alerts', 'escalation_window'),
#             'ALERT_ENABLE_GROUPING': ('alerts', 'enable_grouping'),
#             'ALERT_GROUPING_TIME_WINDOW': ('alerts', 'grouping_time_window'),
#             'ALERT_RETENTION_DAYS': ('alerts', 'retention_days'),
#             'ALERT_MAX_ALERTS_IN_MEMORY': ('alerts', 'max_alerts_in_memory'),
            
#             # Logging mappings
#             'LOG_LEVEL': ('logging', 'level'),
#             'LOG_FORMAT': ('logging', 'format'),
#             'LOG_JSON_FORMAT': ('logging', 'json_format'),
#             'LOG_FILE_ENABLED': ('logging', 'file_enabled'),
#             'LOG_FILE_PATH': ('logging', 'file_path'),
#             'LOG_MAX_FILE_SIZE': ('logging', 'max_file_size'),
#             'LOG_BACKUP_COUNT': ('logging', 'backup_count'),
#             'LOG_CONSOLE_ENABLED': ('logging', 'console_enabled'),
#             'LOG_CORRELATION_ID_HEADER': ('logging', 'correlation_id_header'),
#             'LOG_SERVICE_NAME': ('logging', 'service_name'),
            
#             # Redis mappings
#             'REDIS_HOST': ('redis', 'host'),
#             'REDIS_PORT': ('redis', 'port'),
#             'REDIS_DB': ('redis', 'db'),
#             'REDIS_PASSWORD': ('redis', 'password'),
#             'REDIS_ENABLED': ('redis', 'enabled'), # Ensure this is mapped correctly from .env
#             'REDIS_SOCKET_TIMEOUT': ('redis', 'socket_timeout'),
#             'REDIS_SOCKET_CONNECT_TIMEOUT': ('redis', 'socket_connect_timeout'),
#             'REDIS_MAX_CONNECTIONS': ('redis', 'max_connections'),
#             'REDIS_DEFAULT_TTL': ('redis', 'default_ttl'),
#             'REDIS_ALERTS_CACHE_TTL': ('redis', 'alerts_cache_ttl'),
#         }
        
#         # Apply environment variable overrides
#         for env_var, (config_section, field_name) in env_mappings.items():
#             env_value = os.getenv(env_var)
#             if env_value is not None:
#                 # Get the nested Pydantic model instance
#                 config_obj = getattr(self, config_section)
                
#                 # Check if the field exists on the nested model
#                 if hasattr(config_obj, field_name):
#                     # Get the Pydantic model_fields for type conversion (Pydantic v2+)
#                     field_info = config_obj.model_fields.get(field_name) 
                    
#                     if field_info:
#                         # Convert string to appropriate type based on annotation
#                         target_type = field_info.annotation
                        
#                         # Handle Union types (like Optional) by checking its args
#                         if hasattr(target_type, '__origin__') and target_type.__origin__ is Union:
#                             for arg_type in target_type.__args__:
#                                 if arg_type is not type(None): # Skip NoneType in Optional
#                                     try:
#                                         # Attempt conversion to the actual type
#                                         if arg_type == bool:
#                                             converted_value = env_value.lower() in ('true', '1', 'yes', 'on')
#                                         elif arg_type == int:
#                                             converted_value = int(env_value)
#                                         elif arg_type == float:
#                                             converted_value = float(env_value)
#                                         else: # For other types like str, Enum etc.
#                                             converted_value = arg_type(env_value)
                                        
#                                         setattr(config_obj, field_name, converted_value)
#                                         break # Conversion successful, move to next env_var
#                                     except (ValueError, TypeError):
#                                         pass # Conversion failed, try next type in Union or fallback
#                             else: # If loop completes without break, no non-None type converted successfully
#                                 if env_value.lower() in ('none', 'null', ''):
#                                     setattr(config_obj, field_name, None)
#                                 else:
#                                     setattr(config_obj, field_name, env_value) # Keep as string if no conversion
#                         else: # Handle non-Union types directly
#                             try:
#                                 if target_type == bool:
#                                     converted_value = env_value.lower() in ('true', '1', 'yes', 'on')
#                                 elif target_type == int:
#                                     converted_value = int(env_value)
#                                 elif target_type == float:
#                                     converted_value = float(env_value)
#                                 else: # For str, Enum etc.
#                                     converted_value = target_type(env_value)
#                                 setattr(config_obj, field_name, converted_value)
#                             except (ValueError, TypeError) as e:
#                                 print(f"Warning: Could not convert environment variable '{env_var}' value '{env_value}' to expected type '{target_type}'. Error: {e}")
#                                 setattr(config_obj, field_name, env_value) # Assign raw value as fallback
#                     else:
#                         # Fallback for fields not explicitly in model_fields (e.g., if 'extra' is 'allow')
#                         setattr(config_obj, field_name, env_value)
#                 else:
#                     # Log a warning if an environment variable maps to a field that doesn't exist
#                     print(f"Warning: Environment variable '{env_var}' maps to non-existent field '{field_name}' in section '{config_section}'.")


#     class Config:
#         env_file = ".env"
#         env_file_encoding = "utf-8"
#         case_sensitive = False
#         extra = "allow" # Allows extra fields if present in input but not defined in schema


# # Global settings instance
# _settings: Optional[Settings] = None


# def get_settings() -> Settings:
#     """Get application settings (singleton pattern)."""
#     global _settings
#     if _settings is None:
#         _settings = Settings()
#     return _settings


# def reload_settings():
#     """Reload settings from environment."""
#     global _settings
#     _settings = None
#     return get_settings()


# # Configuration validation
# def validate_configuration():
#     """Validate all configuration settings"""
#     try:
#         settings = get_settings()
        
#         # Validate critical configurations
#         if settings.is_production():
#             if settings.jwt.secret == "your-secret-key-here":
#                 raise ValueError("JWT secret key must be changed in production")
            
#             if settings.kafka.security_protocol == "PLAINTEXT":
#                 raise ValueError("Kafka security protocol must be configured for production")
        
#         # Validate port conflicts
#         ports = [
#             settings.api.port,
#             settings.websocket.port,
#             settings.monitoring_prometheus_port
#         ]
        
#         if len(ports) != len(set(ports)):
#             raise ValueError("Port conflicts detected in configuration")
        
#         return True
        
#     except Exception as e:
#         print(f"Configuration validation failed: {e}")
#         return False


# if __name__ == "__main__":
#     # This block is for testing settings.py directly
#     # `load_dotenv()` is already at the very top of this file.
#     if validate_configuration():
#         print("✅ Configuration validation passed")
#         settings = get_settings()
#         print(f"Environment: {settings.environment}")
#         print(f"Kafka Topics: {settings.get_topic_name('alerts')}")
#         print(f"API Port: {settings.api.port}")
#         print(f"WebSocket Port: {settings.websocket.port}")
#         print(f"Log Level: {settings.logging.level}")
#         print(f"JWT Secret (first 5 chars): {settings.jwt.secret[:5]}...") # Display first few chars for check
#         print(f"Auth DB URL: {settings.api.database_url}")
#         print(f"WS Persistence Enabled: {settings.websocket.enable_persistence}")
#         if settings.websocket.enable_persistence:
#             print(f"WS Postgres URL: {settings.websocket.postgres_url}")
#     else:
#         print("❌ Configuration validation failed")

"""
Configuration Management System for Real-Time Analytics Pipeline

This module provides a comprehensive configuration management system that:
- Uses environment variables with fallback defaults
- Supports multiple environments (dev, staging, production)
- Validates all settings using Pydantic
- Supports hot-reloading for certain configurations
- Provides type safety and comprehensive documentation
"""

import os
from enum import Enum
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings

from dotenv import load_dotenv

load_dotenv()


class Environment(str, Enum):
    """Supported deployment environments"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"


class LogLevel(str, Enum):
    """Supported logging levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AlertSeverity(str, Enum):
    """Alert severity levels"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class LoggingConfig(BaseModel):
    """Logging configuration"""
    level: LogLevel = Field(default=LogLevel.INFO, description="Logging level")
    format: str = Field(default="json", description="Log format (json/text)")
    json_format: bool = Field(default=False, description="Use JSON format for logs")
    file_enabled: bool = Field(default=True, description="Enable file logging")
    file_path: str = Field(default="logs/app.log", description="Log file path")
    max_file_size: int = Field(default=10 * 1024 * 1024, description="Maximum log file size in bytes")
    backup_count: int = Field(default=5, description="Number of backup log files")
    console_enabled: bool = Field(default=True, description="Enable console logging")
    correlation_id_header: str = Field(default="X-Correlation-ID", description="HTTP header for correlation ID")
    service_name: str = Field(default="video-analytics", description="Service name for structured logging")


class KafkaConfig(BaseModel):
    """Kafka configuration"""
    bootstrap_servers: str = Field(default="localhost:9092", description="Comma-separated list of Kafka broker addresses")
    topic_prefix: str = Field(default="video_analytics", description="Prefix for all Kafka topics")
    detection_events_topic: str = Field(default="detection_events", description="Topic for object detection events")
    alerts_topic: str = Field(default="alerts", description="Topic for generated alerts")
    raw_frames_topic: str = Field(default="raw_frames", description="Topic for raw video frames")
    consumer_group_id: str = Field(default="video-analytics-backend", description="Consumer group ID for Kafka consumers")
    client_id: str = Field(default="video-analytics-backend", description="Kafka client ID")
    auto_offset_reset: str = Field(default="latest", description="Auto offset reset policy (earliest/latest)")
    enable_auto_commit: bool = Field(default=True, description="Enable automatic offset commits")
    auto_commit_interval_ms: int = Field(default=1000, description="Auto commit interval in milliseconds")
    compression_type: str = Field(default="gzip", description="Compression type for messages (gzip/snappy/lz4)")
    batch_size: int = Field(default=16384, description="Batch size for producer")
    linger_ms: int = Field(default=10, description="Linger time in milliseconds")
    buffer_memory: int = Field(default=33554432, description="Total memory (in bytes) for buffering messages to be sent to Kafka")
    max_request_size: int = Field(default=1048576, description="Maximum size of a request in bytes (default 1MB)")
    request_timeout_ms: int = Field(default=30000, description="Request timeout in milliseconds (default 30 seconds)")
    max_in_flight_requests_per_connection: int = Field(default=5, description="Maximum number of unacknowledged requests the client will send on a single connection")
    acks: str = Field(default="all", description="Acknowledgment policy (0/1/all)")
    retries: int = Field(default=3, description="Number of retries for failed messages")
    retry_backoff_ms: int = Field(default=100, description="Backoff time between retries")
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol (PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL)")
    sasl_mechanism: Optional[str] = Field(default=None, description="SASL mechanism (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512)")
    sasl_username: Optional[str] = Field(default=None, description="SASL username")
    sasl_password: Optional[str] = Field(default=None, description="SASL password")

    # ADDED Kafka Consumer specific configurations
    consumer_timeout_ms: int = Field(default=1000, description="Kafka consumer poll timeout in milliseconds")
    max_poll_records: int = Field(default=500, description="Maximum number of records returned in a single poll")
    max_poll_interval_ms: int = Field(default=300000, description="Maximum time between poll calls before consumer is considered dead")
    session_timeout_ms: int = Field(default=30000, description="Consumer group session timeout in milliseconds")
    heartbeat_interval_ms: int = Field(default=3000, description="Expected time between consumer heartbeats to consumer coordinator")
    fetch_min_bytes: int = Field(default=1, description="Minimum bytes of data the consumer should return for a fetch request")
    fetch_max_wait_ms: int = Field(default=500, description="Maximum time in ms the consumer will wait for fetch_min_bytes to be available")
    max_partition_fetch_bytes: int = Field(default=1048576, description="Maximum amount of data per partition the server will return")


class APIConfig(BaseModel):
    """API configuration"""
    host: str = Field(default="localhost", description="API server host")
    port: int = Field(default=8000, description="API server port")
    workers: int = Field(default=1, description="Number of API workers")
    title: str = Field(default="Video Analytics API", description="API title")
    description: str = Field(default="Real-time video analytics and alert system", description="API description")
    version: str = Field(default="1.0.0", description="API version")
    database_url: str = Field(default="sqlite:///./auth.db", description="Database URL for authentication and other API data")
    api_key_prefix: str = Field(default="ak_", description="Prefix for API keys")


class JWTConfig(BaseModel):
    """JWT configuration"""
    secret: str = Field(
        default_factory=lambda: os.getenv("JWT_SECRET") or "your-insecure-default-secret-key-for-dev-only",
        description="JWT secret key for token signing. Fetched from JWT_SECRET env var."
    )
    algorithm: str = Field(default="HS256", description="JWT algorithm")
    expiration_hours: int = Field(default=24, description="JWT expiration time in hours")
    access_token_expire_minutes: int = Field(default=30, description="Access token expiration time in minutes")
    refresh_token_expire_days: int = Field(default=7, description="Refresh token expiration time in days")


class WebSocketConfig(BaseModel):
    """WebSocket configuration"""
    host: str = Field(default="localhost", description="WebSocket server host")
    port: int = Field(default=8001, description="WebSocket server port")
    max_connections: int = Field(default=1000, description="Maximum concurrent WebSocket connections")
    ping_interval: int = Field(default=30, description="Ping interval in seconds")
    ping_timeout: int = Field(default=10, description="Ping timeout in seconds")
    max_message_size: int = Field(default=1024 * 1024, description="Maximum message size in bytes")
    connection_timeout: int = Field(default=300, description="Connection timeout in seconds")
    heartbeat_interval: int = Field(default=25, description="Heartbeat interval in seconds")
    enable_persistence: bool = Field(default=False, description="Enable message persistence for WebSockets (requires PostgreSQL)")
    postgres_url: str = Field(default="postgresql+asyncpg://user:pass@localhost/db", description="PostgreSQL URL for WebSocket message persistence")
    cleanup_interval: int = Field(default=60, description="Interval in seconds for cleaning up stale WebSocket connections.") # ADDED: cleanup_interval


class AlertConfig(BaseModel):
    """Alert configuration"""
    threshold_confidence: float = Field(default=0.7, description="Default confidence threshold for alerts")
    person_detection_threshold: float = Field(default=0.8, description="Confidence threshold for person detection")
    vehicle_detection_threshold: float = Field(default=0.75, description="Confidence threshold for vehicle detection")
    cooldown_seconds: int = Field(default=30, description="Alert suppression window in seconds")
    max_alerts_per_camera: int = Field(default=10, description="Maximum alerts per camera in suppression window")
    critical_alert_threshold: int = Field(default=5, description="Number of high severity alerts before escalation")
    escalation_window: int = Field(default=300, description="Escalation window in seconds")
    enable_grouping: bool = Field(default=True, description="Enable alert grouping for similar events")
    grouping_time_window: int = Field(default=60, description="Time window for alert grouping in seconds")
    retention_days: int = Field(default=30, description="Alert retention period in days")
    max_alerts_in_memory: int = Field(default=10000, description="Maximum alerts to keep in memory")


class RedisConfig(BaseModel):
    """Redis configuration"""
    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    db: int = Field(default=0, description="Redis database number")
    password: Optional[str] = Field(default=None, description="Redis password")
    enabled: bool = Field(default=True, description="Enable Redis caching")
    socket_timeout: int = Field(default=5, description="Socket timeout in seconds")
    socket_connect_timeout: int = Field(default=5, description="Socket connect timeout in seconds")
    max_connections: int = Field(default=100, description="Maximum connections in pool")
    default_ttl: int = Field(default=3600, description="Default cache TTL in seconds")
    alerts_cache_ttl: int = Field(default=300, description="Alerts cache TTL in seconds")


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    # Environment Configuration
    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        description="Application environment"
    )
    
    debug: bool = Field(
        default=False,
        description="Enable debug mode"
    )
    
    # Nested configurations
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    jwt: JWTConfig = JWTConfig()
    websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
    alerts: AlertConfig = Field(default_factory=AlertConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    
    # Application Metadata
    app_name: str = Field(
        default="Video Analytics Pipeline",
        env="APP_NAME",
        description="Application name"
    )
    
    app_version: str = Field(
        default="1.0.0",
        env="APP_VERSION",
        description="Application version"
    )
    
    # CORS Configuration
    allowed_origins: str = Field(
        default="*",
        env="ALLOWED_ORIGINS",
        description="Allowed CORS origins (comma-separated)"
    )
    
    allowed_hosts: str = Field(
        default="*",
        env="ALLOWED_HOSTS",
        description="Allowed hosts (comma-separated)"
    )
    
    cors_methods: str = Field(
        default="GET,POST,PUT,DELETE",
        env="CORS_METHODS",
        description="Allowed CORS methods (comma-separated)"
    )
    
    # Rate Limiting
    rate_limit_requests: int = Field(
        default=100,
        env="RATE_LIMIT_REQUESTS",
        description="Rate limit: requests per minute"
    )
    
    rate_limit_window: int = Field(
        default=60,
        env="RATE_LIMIT_WINDOW",
        description="Rate limit window in seconds"
    )
    
    # Hot Reload Configuration
    hot_reload_enabled: bool = Field(
        default=True,
        env="HOT_RELOAD_ENABLED",
        description="Enable hot reload for development"
    )
    
    config_check_interval: int = Field(
        default=60,
        env="CONFIG_CHECK_INTERVAL",
        description="Configuration check interval in seconds"
    )
    
    # Circuit Breaker Configuration
    circuit_failure_threshold: int = Field(
        default=5,
        env="CIRCUIT_FAILURE_THRESHOLD",
        description="Number of failures before opening circuit"
    )
    
    circuit_recovery_timeout: int = Field(
        default=60,
        env="CIRCUIT_RECOVERY_TIMEOUT",
        description="Recovery timeout in seconds"
    )
    
    # Monitoring Configuration
    monitoring_prometheus_enabled: bool = Field(
        default=True,
        env="MONITORING_PROMETHEUS_ENABLED",
        description="Enable Prometheus metrics"
    )
    
    monitoring_prometheus_port: int = Field(
        default=8002,
        env="MONITORING_PROMETHEUS_PORT",
        description="Prometheus metrics port"
    )
    
    monitoring_tracing_enabled: bool = Field(
        default=True,
        env="MONITORING_TRACING_ENABLED",
        description="Enable distributed tracing"
    )
    
    monitoring_jaeger_endpoint: str = Field(
        default="http://localhost:14268/api/traces",
        env="MONITORING_JAEGER_ENDPOINT",
        description="Jaeger endpoint for traces"
    )
    
    monitoring_health_check_interval: int = Field(
        default=30,
        env="MONITORING_HEALTH_CHECK_INTERVAL",
        description="Health check interval in seconds"
    )
    
    monitoring_metrics_collection_interval: int = Field(
        default=10,
        env="MONITORING_METRICS_COLLECTION_INTERVAL",
        description="Metrics collection interval in seconds"
    )
    
    @validator('environment')
    def validate_environment(cls, v):
        """Validate environment configuration"""
        if v == Environment.PRODUCTION:
            pass
        return v
    
    @validator('jwt')
    def validate_jwt_secret(cls, v, values):
        """Validate JWT secret in production"""
        if values.get('environment') == Environment.PRODUCTION:
            if v.secret == "your-insecure-default-secret-key-for-dev-only" or len(v.secret) < 32:
                raise ValueError("JWT secret must be a strong, unique value in production")
        return v
    
    @validator('kafka')
    def validate_kafka_security(cls, v, values):
        """Validate Kafka security in production"""
        if values.get('environment') == Environment.PRODUCTION:
            if v.security_protocol == "PLAINTEXT":
                raise ValueError("Production environment requires secure Kafka connection")
        return v
    
    def get_allowed_origins(self) -> List[str]:
        """Get allowed origins as list"""
        if self.allowed_origins == "*":
            return ["*"]
        return [origin.strip() for origin in self.allowed_origins.split(",")]
    
    def get_allowed_hosts(self) -> List[str]:
        """Get allowed hosts as list"""
        if self.allowed_hosts == "*":
            return ["*"]
        return [host.strip() for host in self.allowed_hosts.split(",")]
    
    def get_cors_methods(self) -> List[str]:
        """Get CORS methods as list"""
        return [method.strip() for method in self.cors_methods.split(",")]
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka configuration as dictionary"""
        config = {
            'bootstrap_servers': self.kafka.bootstrap_servers,
            'auto_offset_reset': self.kafka.auto_offset_reset,
            'enable_auto_commit': self.kafka.enable_auto_commit,
            'group_id': self.kafka.consumer_group_id,
            'security_protocol': self.kafka.security_protocol,
        }
        
        if self.kafka.sasl_mechanism:
            config.update({
                'sasl_mechanism': self.kafka.sasl_mechanism,
                'sasl_plain_username': self.kafka.sasl_username,
                'sasl_plain_password': self.kafka.sasl_password,
            })
        
        return config
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Get Kafka producer configuration"""
        config = {
            'bootstrap_servers': self.kafka.bootstrap_servers,
            'compression_type': self.kafka.compression_type,
            'batch_size': self.kafka.batch_size,
            'linger_ms': self.kafka.linger_ms,
            'acks': self.kafka.acks,
            'retries': self.kafka.retries,
            'retry_backoff_ms': self.kafka.retry_backoff_ms,
        }
        
        if self.kafka.security_protocol != "PLAINTEXT":
            config['security_protocol'] = self.kafka.security_protocol
            
        if self.kafka.sasl_mechanism:
            config.update({
                'sasl_mechanism': self.kafka.sasl_mechanism,
                'sasl_plain_username': self.kafka.sasl_username,
                'sasl_plain_password': self.kafka.sasl_password,
            })
        
        return config
    
    def get_topic_name(self, topic: str) -> str:
        """Get full topic name with environment prefix"""
        return f"{self.kafka.topic_prefix}_{self.environment.value}_{topic}"
    
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.environment == Environment.PRODUCTION
    
    def is_development(self) -> bool:
        """Check if running in development environment"""
        return self.environment == Environment.DEVELOPMENT
    
    @property
    def bootstrap_servers(self):
        return self.kafka.bootstrap_servers
    
    def model_post_init(self, __context: Any) -> None:
        """
        Post-initialization hook to apply environment variables to nested Pydantic models.
        This handles cases where `env=` is not directly used on fields within nested `BaseModel`s,
        or for complex type conversions.
        """
        env_mappings = {
            'KAFKA_BOOTSTRAP_SERVERS': ('kafka', 'bootstrap_servers'),
            'KAFKA_TOPIC_PREFIX': ('kafka', 'topic_prefix'),
            'KAFKA_DETECTION_EVENTS_TOPIC': ('kafka', 'detection_events_topic'),
            'KAFKA_ALERTS_TOPIC': ('kafka', 'alerts_topic'),
            'KAFKA_RAW_FRAMES_TOPIC': ('kafka', 'raw_frames_topic'),
            'KAFKA_CONSUMER_GROUP_ID': ('kafka', 'consumer_group_id'),
            'KAFKA_CLIENT_ID': ('kafka', 'client_id'),
            'KAFKA_AUTO_OFFSET_RESET': ('kafka', 'auto_offset_reset'),
            'KAFKA_ENABLE_AUTO_COMMIT': ('kafka', 'enable_auto_commit'),
            'KAFKA_AUTO_COMMIT_INTERVAL_MS': ('kafka', 'auto_commit_interval_ms'),
            'KAFKA_COMPRESSION_TYPE': ('kafka', 'compression_type'),
            'KAFKA_BATCH_SIZE': ('kafka', 'batch_size'),
            'KAFKA_LINGER_MS': ('kafka', 'linger_ms'),
            'KAFKA_BUFFER_MEMORY': ('kafka', 'buffer_memory'),
            'KAFKA_MAX_REQUEST_SIZE': ('kafka', 'max_request_size'),
            'KAFKA_REQUEST_TIMEOUT_MS': ('kafka', 'request_timeout_ms'),
            'KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION': ('kafka', 'max_in_flight_requests_per_connection'),
            'KAFKA_ACKS': ('kafka', 'acks'),
            'KAFKA_RETRIES': ('kafka', 'retries'),
            'KAFKA_RETRY_BACKOFF_MS': ('kafka', 'retry_backoff_ms'),
            'KAFKA_SECURITY_PROTOCOL': ('kafka', 'security_protocol'),
            'KAFKA_SASL_MECHANISM': ('kafka', 'sasl_mechanism'),
            'KAFKA_SASL_USERNAME': ('kafka', 'sasl_username'),
            'KAFKA_SASL_PASSWORD': ('kafka', 'sasl_password'),
            
            # Kafka Consumer specific mappings (added these)
            'KAFKA_CONSUMER_TIMEOUT_MS': ('kafka', 'consumer_timeout_ms'),
            'KAFKA_MAX_POLL_RECORDS': ('kafka', 'max_poll_records'),
            'KAFKA_MAX_POLL_INTERVAL_MS': ('kafka', 'max_poll_interval_ms'),
            'KAFKA_SESSION_TIMEOUT_MS': ('kafka', 'session_timeout_ms'),
            'KAFKA_HEARTBEAT_INTERVAL_MS': ('kafka', 'heartbeat_interval_ms'),
            'KAFKA_FETCH_MIN_BYTES': ('kafka', 'fetch_min_bytes'),
            'KAFKA_FETCH_MAX_WAIT_MS': ('kafka', 'fetch_max_wait_ms'),
            'KAFKA_MAX_PARTITION_FETCH_BYTES': ('kafka', 'max_partition_fetch_bytes'),

            # API mappings
            'API_HOST': ('api', 'host'),
            'API_PORT': ('api', 'port'),
            'API_WORKERS': ('api', 'workers'),
            'API_TITLE': ('api', 'title'),
            'API_DESCRIPTION': ('api', 'description'),
            'API_VERSION': ('api', 'version'),
            'API_DATABASE_URL': ('api', 'database_url'),
            'API_API_KEY_PREFIX': ('api', 'api_key_prefix'),
            
            # JWT mappings
            'JWT_ALGORITHM': ('jwt', 'algorithm'),
            'JWT_EXPIRATION_HOURS': ('jwt', 'expiration_hours'),
            'JWT_ACCESS_TOKEN_EXPIRE_MINUTES': ('jwt', 'access_token_expire_minutes'),
            'JWT_REFRESH_TOKEN_EXPIRE_DAYS': ('jwt', 'refresh_token_expire_days'),
            
            # WebSocket mappings
            'WS_HOST': ('websocket', 'host'),
            'WS_PORT': ('websocket', 'port'),
            'WS_MAX_CONNECTIONS': ('websocket', 'max_connections'),
            'WS_PING_INTERVAL': ('websocket', 'ping_interval'),
            'WS_PING_TIMEOUT': ('websocket', 'ping_timeout'),
            'WS_MAX_MESSAGE_SIZE': ('websocket', 'max_message_size'),
            'WS_CONNECTION_TIMEOUT': ('websocket', 'connection_timeout'),
            'WS_HEARTBEAT_INTERVAL': ('websocket', 'heartbeat_interval'),
            'WS_ENABLE_PERSISTENCE': ('websocket', 'enable_persistence'),
            'WS_POSTGRES_URL': ('websocket', 'postgres_url'),
            'WS_CLEANUP_INTERVAL': ('websocket', 'cleanup_interval'), # ADDED: WebSocket cleanup interval mapping
            
            # Alert mappings
            'ALERT_THRESHOLD_CONFIDENCE': ('alerts', 'threshold_confidence'),
            'ALERT_PERSON_DETECTION_THRESHOLD': ('alerts', 'person_detection_threshold'),
            'ALERT_VEHICLE_DETECTION_THRESHOLD': ('alerts', 'vehicle_detection_threshold'),
            'ALERT_COOLDOWN_SECONDS': ('alerts', 'cooldown_seconds'),
            'ALERT_MAX_ALERTS_PER_CAMERA': ('alerts', 'max_alerts_per_camera'),
            'ALERT_CRITICAL_ALERT_THRESHOLD': ('alerts', 'critical_alert_threshold'),
            'ALERT_ESCALATION_WINDOW': ('alerts', 'escalation_window'),
            'ALERT_ENABLE_GROUPING': ('alerts', 'enable_grouping'),
            'ALERT_GROUPING_TIME_WINDOW': ('alerts', 'grouping_time_window'),
            'ALERT_RETENTION_DAYS': ('alerts', 'retention_days'),
            'ALERT_MAX_ALERTS_IN_MEMORY': ('alerts', 'max_alerts_in_memory'),
            
            # Logging mappings
            'LOG_LEVEL': ('logging', 'level'),
            'LOG_FORMAT': ('logging', 'format'),
            'LOG_JSON_FORMAT': ('logging', 'json_format'),
            'LOG_FILE_ENABLED': ('logging', 'file_enabled'),
            'LOG_FILE_PATH': ('logging', 'file_path'),
            'LOG_MAX_FILE_SIZE': ('logging', 'max_file_size'),
            'LOG_BACKUP_COUNT': ('logging', 'backup_count'),
            'LOG_CONSOLE_ENABLED': ('logging', 'console_enabled'),
            'LOG_CORRELATION_ID_HEADER': ('logging', 'correlation_id_header'),
            'LOG_SERVICE_NAME': ('logging', 'service_name'),
            
            # Redis mappings
            'REDIS_HOST': ('redis', 'host'),
            'REDIS_PORT': ('redis', 'port'),
            'REDIS_DB': ('redis', 'db'),
            'REDIS_PASSWORD': ('redis', 'password'),
            'REDIS_ENABLED': ('redis', 'enabled'),
            'REDIS_SOCKET_TIMEOUT': ('redis', 'socket_timeout'),
            'REDIS_SOCKET_CONNECT_TIMEOUT': ('redis', 'socket_connect_timeout'),
            'REDIS_MAX_CONNECTIONS': ('redis', 'max_connections'),
            'REDIS_DEFAULT_TTL': ('redis', 'default_ttl'),
            'REDIS_ALERTS_CACHE_TTL': ('redis', 'alerts_cache_ttl'),
        }
        
        for env_var, (config_section, field_name) in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                config_obj = getattr(self, config_section)
                
                if hasattr(config_obj, field_name):
                    field_info = config_obj.model_fields.get(field_name) 
                    
                    if field_info:
                        target_type = field_info.annotation
                        
                        if hasattr(target_type, '__origin__') and target_type.__origin__ is Union:
                            for arg_type in target_type.__args__:
                                if arg_type is not type(None):
                                    try:
                                        if arg_type == bool:
                                            converted_value = env_value.lower() in ('true', '1', 'yes', 'on')
                                        elif arg_type == int:
                                            converted_value = int(env_value)
                                        elif arg_type == float:
                                            converted_value = float(env_value)
                                        else:
                                            converted_value = arg_type(env_value)
                                        
                                        setattr(config_obj, field_name, converted_value)
                                        break
                                    except (ValueError, TypeError):
                                        pass
                            else:
                                if env_value.lower() in ('none', 'null', ''):
                                    setattr(config_obj, field_name, None)
                                else:
                                    setattr(config_obj, field_name, env_value)
                        else:
                            try:
                                if target_type == bool:
                                    converted_value = env_value.lower() in ('true', '1', 'yes', 'on')
                                elif target_type == int:
                                    converted_value = int(env_value)
                                elif target_type == float:
                                    converted_value = float(env_value)
                                else:
                                    converted_value = target_type(env_value)
                                setattr(config_obj, field_name, converted_value)
                            except (ValueError, TypeError) as e:
                                print(f"Warning: Could not convert environment variable '{env_var}' value '{env_value}' to expected type '{target_type}'. Error: {e}")
                                setattr(config_obj, field_name, env_value)
                    else:
                        setattr(config_obj, field_name, env_value)
                else:
                    print(f"Warning: Environment variable '{env_var}' maps to non-existent field '{field_name}' in section '{config_section}'.")


    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get application settings (singleton pattern)."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings():
    """Reload settings from environment."""
    global _settings
    _settings = None
    return get_settings()


# Configuration validation
def validate_configuration():
    """Validate all configuration settings"""
    try:
        settings = get_settings()
        
        if settings.is_production():
            pass
            
            if settings.kafka.security_protocol == "PLAINTEXT":
                raise ValueError("Kafka security protocol must be configured for production")
        
        ports = [
            settings.api.port,
            settings.websocket.port,
            settings.monitoring_prometheus_port
        ]
        
        if len(ports) != len(set(ports)):
            raise ValueError("Port conflicts detected in configuration")
        
        return True
        
    except Exception as e:
        print(f"Configuration validation failed: {e}")
        return False


if __name__ == "__main__":
    if validate_configuration():
        print("✅ Configuration validation passed")
        settings = get_settings()
        print(f"Environment: {settings.environment}")
        print(f"Kafka Topics: {settings.get_topic_name('alerts')}")
        print(f"API Port: {settings.api.port}")
        print(f"WebSocket Port: {settings.websocket.port}")
        print(f"Log Level: {settings.logging.level}")
        print(f"JWT Secret (first 5 chars): {settings.jwt.secret[:5]}...") 
        print(f"Auth DB URL: {settings.api.database_url}")
        print(f"WS Persistence Enabled: {settings.websocket.enable_persistence}")
        if settings.websocket.enable_persistence:
            print(f"WS Postgres URL: {settings.websocket.postgres_url}")
    else:
        print("❌ Configuration validation failed")