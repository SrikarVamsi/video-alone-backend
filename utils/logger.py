# """
# Sophisticated Logging Utility for Distributed Real-Time Analytics System

# This module provides:
# - Structured logging with JSON format for production
# - Contextual information (correlation IDs, service names)
# - Performance logging decorators
# - Thread-safe logging with minimal performance impact
# - Log rotation and file management
# - Distributed tracing support
# """

# import json
# import logging
# import logging.handlers
# import os
# import sys
# import time
# import uuid
# from contextvars import ContextVar
# from functools import wraps
# from typing import Any, Dict, Optional, Callable
# from datetime import datetime
# import threading
# from pathlib import Path

# try:
#     import structlog
#     import colorlog
#     from opentelemetry import trace
#     from opentelemetry.trace import Status, StatusCode
#     OPTIONAL_DEPS_AVAILABLE = True
# except ImportError:
#     OPTIONAL_DEPS_AVAILABLE = False

# from config.settings import get_settings

# # Context variables for distributed tracing
# correlation_id_var: ContextVar[str] = ContextVar('correlation_id', default='')
# user_id_var: ContextVar[str] = ContextVar('user_id', default='')
# service_name_var: ContextVar[str] = ContextVar('service_name', default='')

# # Thread-local storage for logger instances
# _thread_local = threading.local()

# # Global configuration
# settings = get_settings()


# class CorrelationIdFilter(logging.Filter):
#     """Filter to add correlation ID to log records"""
    
#     def filter(self, record):
#         record.correlation_id = correlation_id_var.get() or str(uuid.uuid4())
#         record.user_id = user_id_var.get() or 'anonymous'
#         record.service_name = service_name_var.get() or getattr(settings.logging, 'service_name', 'analytics-pipeline')
#         return True


# class JSONFormatter(logging.Formatter):
#     """JSON formatter for structured logging"""
    
#     def __init__(self, service_name: str):
#         super().__init__()
#         self.service_name = service_name
    
#     def format(self, record):
#         """Format log record as JSON"""
#         log_entry = {
#             'timestamp': datetime.utcnow().isoformat(),
#             'level': record.levelname,
#             'service': self.service_name,
#             'logger': record.name,
#             'message': record.getMessage(),
#             'correlation_id': getattr(record, 'correlation_id', ''),
#             'user_id': getattr(record, 'user_id', ''),
#             'module': record.module,
#             'function': record.funcName,
#             'line': record.lineno,
#             'thread': record.thread,
#             'process': record.process,
#         }
        
#         # Add exception information if present
#         if record.exc_info:
#             log_entry['exception'] = {
#                 'type': record.exc_info[0].__name__,
#                 'message': str(record.exc_info[1]),
#                 'traceback': self.formatException(record.exc_info)
#             }
        
#         # Add extra fields
#         for key, value in record.__dict__.items():
#             if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
#                           'filename', 'module', 'lineno', 'funcName', 'created', 
#                           'msecs', 'relativeCreated', 'thread', 'threadName', 
#                           'processName', 'process', 'message', 'exc_info', 'exc_text', 
#                           'stack_info', 'correlation_id', 'user_id', 'service_name']:
#                 log_entry[key] = value
        
#         return json.dumps(log_entry)


# class ColoredFormatter(logging.Formatter):
#     """Colored formatter for console output (fallback if colorlog not available)"""
    
#     def __init__(self):
#         if OPTIONAL_DEPS_AVAILABLE:
#             # Use colorlog if available
#             import colorlog
#             super().__init__()
#             self.formatter = colorlog.ColoredFormatter(
#                 fmt='%(log_color)s%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
#                 datefmt='%Y-%m-%d %H:%M:%S',
#                 log_colors={
#                     'DEBUG': 'cyan',
#                     'INFO': 'green',
#                     'WARNING': 'yellow',
#                     'ERROR': 'red',
#                     'CRITICAL': 'red,bg_white',
#                 }
#             )
#         else:
#             # Fallback to basic formatting
#             super().__init__(
#                 fmt='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
#                 datefmt='%Y-%m-%d %H:%M:%S'
#             )
    
#     def format(self, record):
#         if OPTIONAL_DEPS_AVAILABLE:
#             return self.formatter.format(record)
#         else:
#             return super().format(record)


# class PerformanceLogger:
#     """Performance logging utility with timing and metrics"""
    
#     def __init__(self, logger: logging.Logger):
#         self.logger = logger
#         if OPTIONAL_DEPS_AVAILABLE:
#             self.tracer = trace.get_tracer(__name__)
#         else:
#             self.tracer = None
    
#     def log_performance(self, operation: str, duration: float, **kwargs):
#         """Log performance metrics"""
#         self.logger.info(
#             f"Performance: {operation} completed in {duration:.3f}s",
#             extra={
#                 'operation': operation,
#                 'duration': duration,
#                 'metrics': kwargs
#             }
#         )
    
#     def time_function(self, func_name: str = None):
#         """Decorator to time function execution"""
#         def decorator(func):
#             @wraps(func)
#             def wrapper(*args, **kwargs):
#                 start_time = time.time()
#                 operation_name = func_name or f"{func.__module__}.{func.__name__}"
                
#                 # Create tracing span if OpenTelemetry is available
#                 if self.tracer:
#                     with self.tracer.start_as_current_span(operation_name) as span:
#                         try:
#                             result = func(*args, **kwargs)
#                             if OPTIONAL_DEPS_AVAILABLE:
#                                 span.set_status(Status(StatusCode.OK))
#                             return result
#                         except Exception as e:
#                             if OPTIONAL_DEPS_AVAILABLE:
#                                 span.set_status(Status(StatusCode.ERROR, str(e)))
#                                 span.record_exception(e)
#                             raise
#                         finally:
#                             duration = time.time() - start_time
#                             self.log_performance(operation_name, duration)
#                             if OPTIONAL_DEPS_AVAILABLE:
#                                 span.set_attribute("duration", duration)
#                 else:
#                     # Fallback without tracing
#                     try:
#                         result = func(*args, **kwargs)
#                         return result
#                     except Exception as e:
#                         raise
#                     finally:
#                         duration = time.time() - start_time
#                         self.log_performance(operation_name, duration)
            
#             return wrapper
#         return decorator
    
#     def time_async_function(self, func_name: str = None):
#         """Decorator to time async function execution"""
#         def decorator(func):
#             @wraps(func)
#             async def wrapper(*args, **kwargs):
#                 start_time = time.time()
#                 operation_name = func_name or f"{func.__module__}.{func.__name__}"
                
#                 if self.tracer:
#                     with self.tracer.start_as_current_span(operation_name) as span:
#                         try:
#                             result = await func(*args, **kwargs)
#                             if OPTIONAL_DEPS_AVAILABLE:
#                                 span.set_status(Status(StatusCode.OK))
#                             return result
#                         except Exception as e:
#                             if OPTIONAL_DEPS_AVAILABLE:
#                                 span.set_status(Status(StatusCode.ERROR, str(e)))
#                                 span.record_exception(e)
#                             raise
#                         finally:
#                             duration = time.time() - start_time
#                             self.log_performance(operation_name, duration)
#                             if OPTIONAL_DEPS_AVAILABLE:
#                                 span.set_attribute("duration", duration)
#                 else:
#                     # Fallback without tracing
#                     try:
#                         result = await func(*args, **kwargs)
#                         return result
#                     except Exception as e:
#                         raise
#                     finally:
#                         duration = time.time() - start_time
#                         self.log_performance(operation_name, duration)
            
#             return wrapper
#         return decorator


# class LoggerManager:
#     """Centralized logger management"""
    
#     def __init__(self):
#         self.loggers: Dict[str, logging.Logger] = {}
#         self.performance_loggers: Dict[str, PerformanceLogger] = {}
#         self._setup_root_logger()
    
#     # def _setup_root_logger(self):
#     #     """Setup root logger configuration"""
#     #     root_logger = logging.getLogger()
        
#     #     # Handle both enum and string level values
#     #     if hasattr(settings.logging.level, 'value'):
#     #         log_level = getattr(logging, settings.logging.level.value.upper())
#     #     else:
#     #         log_level = getattr(logging, str(settings.logging.level).upper())
        
#     #     root_logger.setLevel(log_level)
        
#     #     # Clear any existing handlers
#     #     root_logger.handlers.clear()
        
#     #     # Add correlation ID filter
#     #     correlation_filter = CorrelationIdFilter()
        
#     #     # Console handler
#     #     if getattr(settings.logging, 'console_enabled', True):
#     #         console_handler = logging.StreamHandler(sys.stdout)
#     #         console_handler.setLevel(log_level)
#     #         console_handler.addFilter(correlation_filter)
            
#     #         if getattr(settings.logging, 'json_format', False):
#     #             console_handler.setFormatter(JSONFormatter(getattr(settings.logging, 'service_name', 'analytics-pipeline')))
#     #         else:
#     #             console_handler.setFormatter(ColoredFormatter())
            
#     #         root_logger.addHandler(console_handler)
        
#     #     # File handler
#     #     if getattr(settings.logging, 'file_enabled', False):
#     #         # Create log directory if it doesn't exist
#     #         file_path = getattr(settings.logging, 'file_path', 'logs/app.log')
#     #         log_dir = Path(file_path).parent
#     #         log_dir.mkdir(parents=True, exist_ok=True)
            
#     #         file_handler = logging.handlers.RotatingFileHandler(
#     #             filename=file_path,
#     #             maxBytes=getattr(settings.logging, 'max_file_size', 10*1024*1024),
#     #             backupCount=getattr(settings.logging, 'backup_count', 5)
#     #         )
#     #         file_handler.setLevel(log_level)
#     #         file_handler.addFilter(correlation_filter)
#     #         file_handler.setFormatter(JSONFormatter(getattr(settings.logging, 'service_name', 'analytics-pipeline')))
            
#     #         root_logger.addHandler(file_handler)

#     def _setup_root_logger(self):
#         """Setup root logger configuration"""
#     root_logger = logging.getLogger()
    
#     # Handle both enum and string level values more robustly
#     try:
#         if hasattr(settings.logging, 'level'):
#             level_value = settings.logging.level
            
#             # Handle enum-like objects
#             if hasattr(level_value, 'value'):
#                 log_level_str = str(level_value.value).upper()
#             elif hasattr(level_value, 'name'):
#                 log_level_str = str(level_value.name).upper()
#             else:
#                 log_level_str = str(level_value).upper()
#         else:
#             log_level_str = 'INFO'  # Default fallback
        
#         # Validate the log level
#         if log_level_str not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
#             log_level_str = 'INFO'
        
#         log_level = getattr(logging, log_level_str)
#     except (AttributeError, ValueError) as e:
#         # Fallback to INFO if there's any issue getting the log level
#         log_level = logging.INFO
#         log_level_str = 'INFO'
    
#     root_logger.setLevel(log_level)
    
#     # Clear any existing handlers
#     root_logger.handlers.clear()
    
#     # Add correlation ID filter
#     correlation_filter = CorrelationIdFilter()
    
#     # Console handler
#     if getattr(settings.logging, 'console_enabled', True):
#         console_handler = logging.StreamHandler(sys.stdout)
#         console_handler.setLevel(log_level)
#         console_handler.addFilter(correlation_filter)
        
#         if getattr(settings.logging, 'json_format', False):
#             console_handler.setFormatter(JSONFormatter(getattr(settings.logging, 'service_name', 'analytics-pipeline')))
#         else:
#             console_handler.setFormatter(ColoredFormatter())
        
#         root_logger.addHandler(console_handler)
    
#     # File handler
#     if getattr(settings.logging, 'file_enabled', False):
#         try:
#             # Create log directory if it doesn't exist
#             file_path = getattr(settings.logging, 'file_path', 'logs/app.log')
#             log_dir = Path(file_path).parent
#             log_dir.mkdir(parents=True, exist_ok=True)
            
#             file_handler = logging.handlers.RotatingFileHandler(
#                 filename=file_path,
#                 maxBytes=getattr(settings.logging, 'max_file_size', 10*1024*1024),
#                 backupCount=getattr(settings.logging, 'backup_count', 5)
#             )
#             file_handler.setLevel(log_level)
#             file_handler.addFilter(correlation_filter)
#             file_handler.setFormatter(JSONFormatter(getattr(settings.logging, 'service_name', 'analytics-pipeline')))
            
#             root_logger.addHandler(file_handler)
#         except Exception as e:
#             # If file handler setup fails, continue with console logging
#             print(f"Warning: Could not setup file logging: {e}")
    
#     def get_logger(self, name: str) -> logging.Logger:
#         """Get or create a logger with the given name"""
#         if name not in self.loggers:
#             logger = logging.getLogger(name)
#             self.loggers[name] = logger
#         return self.loggers[name]
    
#     def get_performance_logger(self, name: str) -> PerformanceLogger:
#         """Get or create a performance logger"""
#         if name not in self.performance_loggers:
#             logger = self.get_logger(name)
#             self.performance_loggers[name] = PerformanceLogger(logger)
#         return self.performance_loggers[name]


# # Global logger manager instance
# logger_manager = LoggerManager()


# # def setup_logging(level: str = None, format_type: str = None):
# #     """Setup logging configuration - compatibility function for main.py"""
# #     global logger_manager
    
# #     # Update settings if provided
# #     if level:
# #         if hasattr(settings.logging, 'level'):
# #             if hasattr(settings.logging.level, 'value'):
# #                 settings.logging.level.value = level.upper()
# #             else:
# #                 settings.logging.level = level.upper()
# #         else:
# #             # If settings doesn't have logging.level, set a default
# #             settings.logging.level = level.upper()
    
# #     if format_type:
# #         if hasattr(settings.logging, 'json_format'):
# #             settings.logging.json_format = format_type.lower() == 'json'
# #         else:
# #             settings.logging.json_format = format_type.lower() == 'json'
    
# #     # Reinitialize logger manager with new settings
# #     logger_manager = LoggerManager()
    
# #     logger = get_logger(__name__)
# #     logger.info("Logging reconfigured", extra={
# #         'log_level': settings.logging.level.value if hasattr(settings.logging.level, 'value') else str(settings.logging.level),
# #         'json_format': getattr(settings.logging, 'json_format', False),
# #         'file_enabled': getattr(settings.logging, 'file_enabled', False),
# #         'console_enabled': getattr(settings.logging, 'console_enabled', True)
# #     })
# def setup_logging(level: str = None, format_type: str = None):
#     """Setup logging configuration - compatibility function for main.py"""
#     global logger_manager
    
#     # Don't try to modify settings directly - just use the provided values
#     # The settings object may contain immutable enum values
    
#     # If level is provided, we'll use it in the logger setup
#     # If not, we'll use the existing settings value
#     if level:
#         log_level_str = level.upper()
#     else:
#         # Extract level from settings, handling both enum and string cases
#         if hasattr(settings.logging, 'level'):
#             if hasattr(settings.logging.level, 'value'):
#                 log_level_str = settings.logging.level.value.upper()
#             else:
#                 log_level_str = str(settings.logging.level).upper()
#         else:
#             log_level_str = 'INFO'  # Default fallback
    
#     # Handle format type
#     if format_type:
#         json_format = format_type.lower() == 'json'
#     else:
#         json_format = getattr(settings.logging, 'json_format', False)
    
#     # Create a temporary settings-like object for the logger manager
#     # This avoids modifying the original settings
#     class TempLoggingSettings:
#         def __init__(self, level_str, json_fmt):
#             self.level = level_str
#             self.json_format = json_fmt
#             self.service_name = getattr(settings.logging, 'service_name', 'analytics-pipeline')
#             self.console_enabled = getattr(settings.logging, 'console_enabled', True)
#             self.file_enabled = getattr(settings.logging, 'file_enabled', False)
#             self.file_path = getattr(settings.logging, 'file_path', 'logs/app.log')
#             self.max_file_size = getattr(settings.logging, 'max_file_size', 10*1024*1024)
#             self.backup_count = getattr(settings.logging, 'backup_count', 5)
    
#     # Temporarily replace the settings for logger initialization
#     original_logging_settings = settings.logging
#     settings.logging = TempLoggingSettings(log_level_str, json_format)
    
#     try:
#         # Reinitialize logger manager with new settings
#         logger_manager = LoggerManager()
        
#         logger = get_logger(__name__)
#         logger.info("Logging reconfigured", extra={
#             'log_level': log_level_str,
#             'json_format': json_format,
#             'file_enabled': getattr(settings.logging, 'file_enabled', False),
#             'console_enabled': getattr(settings.logging, 'console_enabled', True)
#         })
#     finally:
#         # Restore original settings
#         settings.logging = original_logging_settings

# def get_logger(name: str = None) -> logging.Logger:
#     """Get logger instance for the calling module"""
#     if name is None:
#         # Get the caller's module name
#         import inspect
#         frame = inspect.currentframe().f_back
#         name = frame.f_globals.get('__name__', 'unknown')
    
#     return logger_manager.get_logger(name)


# def get_performance_logger(name: str = None) -> PerformanceLogger:
#     """Get performance logger instance"""
#     if name is None:
#         import inspect
#         frame = inspect.currentframe().f_back
#         name = frame.f_globals.get('__name__', 'unknown')
    
#     return logger_manager.get_performance_logger(name)


# def set_correlation_id(correlation_id: str):
#     """Set correlation ID for current context"""
#     correlation_id_var.set(correlation_id)


# def get_correlation_id() -> str:
#     """Get current correlation ID"""
#     return correlation_id_var.get()


# def set_user_id(user_id: str):
#     """Set user ID for current context"""
#     user_id_var.set(user_id)


# def get_user_id() -> str:
#     """Get current user ID"""
#     return user_id_var.get()


# def set_service_name(service_name: str):
#     """Set service name for current context"""
#     service_name_var.set(service_name)


# def get_service_name() -> str:
#     """Get current service name"""
#     return service_name_var.get()


# def generate_correlation_id() -> str:
#     """Generate a new correlation ID"""
#     return str(uuid.uuid4())


# class LogContext:
#     """Context manager for setting log context"""
    
#     def __init__(self, correlation_id: str = None, user_id: str = None, service_name: str = None):
#         self.correlation_id = correlation_id or generate_correlation_id()
#         self.user_id = user_id
#         self.service_name = service_name
#         self.old_correlation_id = None
#         self.old_user_id = None
#         self.old_service_name = None
    
#     def __enter__(self):
#         self.old_correlation_id = correlation_id_var.get()
#         self.old_user_id = user_id_var.get()
#         self.old_service_name = service_name_var.get()
        
#         correlation_id_var.set(self.correlation_id)
#         if self.user_id:
#             user_id_var.set(self.user_id)
#         if self.service_name:
#             service_name_var.set(self.service_name)
        
#         return self
    
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         correlation_id_var.set(self.old_correlation_id)
#         user_id_var.set(self.old_user_id)
#         service_name_var.set(self.old_service_name)


# # Convenience decorators
# def log_function_call(logger: logging.Logger = None, level: str = 'INFO'):
#     """Decorator to log function calls"""
#     def decorator(func):
#         nonlocal logger
#         if logger is None:
#             logger = get_logger(func.__module__)
        
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             log_level = getattr(logging, level.upper())
#             logger.log(log_level, f"Calling {func.__name__}", extra={
#                 'function': func.__name__,
#                 'args_count': len(args),
#                 'kwargs_count': len(kwargs)
#             })
            
#             try:
#                 result = func(*args, **kwargs)
#                 logger.log(log_level, f"Completed {func.__name__}", extra={
#                     'function': func.__name__,
#                     'success': True
#                 })
#                 return result
#             except Exception as e:
#                 logger.error(f"Failed {func.__name__}: {str(e)}", extra={
#                     'function': func.__name__,
#                     'error': str(e),
#                     'exception_type': type(e).__name__
#                 })
#                 raise
        
#         return wrapper
#     return decorator


# def log_async_function_call(logger: logging.Logger = None, level: str = 'INFO'):
#     """Decorator to log async function calls"""
#     def decorator(func):
#         nonlocal logger
#         if logger is None:
#             logger = get_logger(func.__module__)
        
#         @wraps(func)
#         async def wrapper(*args, **kwargs):
#             log_level = getattr(logging, level.upper())
#             logger.log(log_level, f"Calling async {func.__name__}", extra={
#                 'function': func.__name__,
#                 'args_count': len(args),
#                 'kwargs_count': len(kwargs)
#             })
            
#             try:
#                 result = await func(*args, **kwargs)
#                 logger.log(log_level, f"Completed async {func.__name__}", extra={
#                     'function': func.__name__,
#                     'success': True
#                 })
#                 return result
#             except Exception as e:
#                 logger.error(f"Failed async {func.__name__}: {str(e)}", extra={
#                     'function': func.__name__,
#                     'error': str(e),
#                     'exception_type': type(e).__name__
#                 })
#                 raise
        
#         return wrapper
#     return decorator


# def log_exception(logger: logging.Logger = None, reraise: bool = True):
#     """Decorator to log exceptions"""
#     def decorator(func):
#         nonlocal logger
#         if logger is None:
#             logger = get_logger(func.__module__)
        
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             try:
#                 return func(*args, **kwargs)
#             except Exception as e:
#                 logger.exception(f"Exception in {func.__name__}: {str(e)}", extra={
#                     'function': func.__name__,
#                     'exception_type': type(e).__name__,
#                     'args_count': len(args),
#                     'kwargs_count': len(kwargs)
#                 })
#                 if reraise:
#                     raise
#                 return None
        
#         return wrapper
#     return decorator


# # Structured logging helpers
# def log_event(event_type: str, message: str, logger: logging.Logger = None, **kwargs):
#     """Log a structured event"""
#     if logger is None:
#         logger = get_logger()
    
#     logger.info(message, extra={
#         'event_type': event_type,
#         'event_data': kwargs
#     })


# def log_metric(metric_name: str, value: float, unit: str = None, logger: logging.Logger = None, **tags):
#     """Log a metric"""
#     if logger is None:
#         logger = get_logger()
    
#     logger.info(f"Metric: {metric_name} = {value}", extra={
#         'metric_name': metric_name,
#         'metric_value': value,
#         'metric_unit': unit,
#         'metric_tags': tags
#     })


# def log_alert(alert_type: str, message: str, severity: str, logger: logging.Logger = None, **kwargs):
#     """Log an alert"""
#     if logger is None:
#         logger = get_logger()
    
#     log_level = logging.WARNING if severity.upper() in ['LOW', 'MEDIUM'] else logging.ERROR
#     logger.log(log_level, f"Alert: {message}", extra={
#         'alert_type': alert_type,
#         'alert_severity': severity,
#         'alert_data': kwargs
#     })


# def log_kafka_event(topic: str, event_type: str, message: str, logger: logging.Logger = None, **kwargs):
#     """Log Kafka-related events"""
#     if logger is None:
#         logger = get_logger()
    
#     logger.info(f"Kafka {event_type}: {message}", extra={
#         'kafka_topic': topic,
#         'kafka_event_type': event_type,
#         'kafka_data': kwargs
#     })


# def log_websocket_event(event_type: str, client_id: str, message: str, logger: logging.Logger = None, **kwargs):
#     """Log WebSocket events"""
#     if logger is None:
#         logger = get_logger()
    
#     logger.info(f"WebSocket {event_type}: {message}", extra={
#         'websocket_event_type': event_type,
#         'websocket_client_id': client_id,
#         'websocket_data': kwargs
#     })


# def log_api_request(method: str, path: str, status_code: int, duration: float, logger: logging.Logger = None, **kwargs):
#     """Log API requests"""
#     if logger is None:
#         logger = get_logger()
    
#     logger.info(f"API {method} {path} {status_code} ({duration:.3f}s)", extra={
#         'api_method': method,
#         'api_path': path,
#         'api_status_code': status_code,
#         'api_duration': duration,
#         'api_data': kwargs
#     })


# # Health check logging
# def log_health_check(component: str, status: str, details: Dict[str, Any] = None, logger: logging.Logger = None):
#     """Log health check results"""
#     if logger is None:
#         logger = get_logger()
    
#     log_level = logging.INFO if status == 'healthy' else logging.WARNING
#     logger.log(log_level, f"Health check: {component} is {status}", extra={
#         'health_component': component,
#         'health_status': status,
#         'health_details': details or {}
#     })


# # Error aggregation helpers
# class ErrorAggregator:
#     """Aggregate and report errors"""
    
#     def __init__(self, logger: logging.Logger = None):
#         self.logger = logger or get_logger()
#         self.error_counts = {}
#         self.lock = threading.Lock()
    
#     def record_error(self, error_type: str, error_message: str, **kwargs):
#         """Record an error occurrence"""
#         with self.lock:
#             key = f"{error_type}:{error_message}"
#             if key not in self.error_counts:
#                 self.error_counts[key] = {
#                     'count': 0,
#                     'first_seen': datetime.utcnow(),
#                     'last_seen': datetime.utcnow(),
#                     'details': kwargs
#                 }
            
#             self.error_counts[key]['count'] += 1
#             self.error_counts[key]['last_seen'] = datetime.utcnow()
    
#     def get_error_summary(self) -> Dict[str, Any]:
#         """Get error summary"""
#         with self.lock:
#             return dict(self.error_counts)
    
#     def log_error_summary(self):
#         """Log error summary"""
#         summary = self.get_error_summary()
#         if summary:
#             self.logger.warning("Error summary", extra={
#                 'error_summary': summary,
#                 'total_error_types': len(summary)
#             })


# # Global error aggregator
# error_aggregator = ErrorAggregator()


# # Utility functions for common logging patterns
# def setup_request_logging(correlation_id: str = None, user_id: str = None):
#     """Setup logging context for a request"""
#     if correlation_id is None:
#         correlation_id = generate_correlation_id()
    
#     set_correlation_id(correlation_id)
#     if user_id:
#         set_user_id(user_id)
    
#     return correlation_id


# def cleanup_request_logging():
#     """Cleanup logging context after request"""
#     correlation_id_var.set('')
#     user_id_var.set('')


# # Performance monitoring helpers
# class PerformanceMonitor:
#     """Monitor and log performance metrics"""
    
#     def __init__(self, logger: logging.Logger = None):
#         self.logger = logger or get_logger()
#         self.metrics = {}
#         self.lock = threading.Lock()
    
#     def record_metric(self, name: str, value: float, unit: str = 'ms', tags: Dict[str, str] = None):
#         """Record a performance metric"""
#         with self.lock:
#             if name not in self.metrics:
#                 self.metrics[name] = []
            
#             self.metrics[name].append({
#                 'value': value,
#                 'unit': unit,
#                 'timestamp': time.time(),
#                 'tags': tags or {}
#             })
    
#     def get_metrics_summary(self) -> Dict[str, Dict[str, float]]:
#         """Get summary statistics for all metrics"""
#         with self.lock:
#             summary = {}
#             for name, values in self.metrics.items():
#                 if values:
#                     vals = [v['value'] for v in values]
#                     summary[name] = {
#                         'count': len(vals),
#                         'min': min(vals),
#                         'max': max(vals),
#                         'avg': sum(vals) / len(vals),
#                         'last': vals[-1]
#                     }
#             return summary
    
#     def log_metrics_summary(self):
#         """Log performance metrics summary"""
#         summary = self.get_metrics_summary()
#         if summary:
#             self.logger.info("Performance metrics summary", extra={
#                 'performance_metrics': summary
#             })
    
#     def clear_metrics(self):
#         """Clear all stored metrics"""
#         with self.lock:
#             self.metrics.clear()


# # Global performance monitor
# performance_monitor = PerformanceMonitor()


# # Database operation logging
# def log_database_operation(operation: str, table: str, duration: float, rows_affected: int = None, 
#                           logger: logging.Logger = None, **kwargs):
#     """Log database operations"""
#     if logger is None:
#         logger = get_logger()
    
#     logger.info(f"Database {operation} on {table} ({duration:.3f}s)", extra={
#         'db_operation': operation,
#         'db_table': table,
#         'db_duration': duration,
#         'db_rows_affected': rows_affected,
#         'db_extra': kwargs
#     })


# # Cache operation logging
# def log_cache_operation(operation: str, key: str, hit: bool = None, duration: float = None,
#                        logger: logging.Logger = None, **kwargs):
#     """Log cache operations"""
#     if logger is None:
#         logger = get_logger()
    
#     logger.info(f"Cache {operation} for key {key}", extra={
#         'cache_operation': operation,
#         'cache_key': key,
#         'cache_hit': hit,
#         'cache_duration': duration,
#         'cache_extra': kwargs
#     })


# # Network operation logging
# def log_network_operation(operation: str, endpoint: str, status: int = None, 
#                          duration: float = None, logger: logging.Logger = None, **kwargs):
#     """Log network operations"""
#     if logger is None:
#         logger = get_logger()
    
#     logger.info(f"Network {operation} to {endpoint}", extra={
#         'network_operation': operation,
#         'network_endpoint': endpoint,
#         'network_status': status,
#         'network_duration': duration,
#         'network_extra': kwargs
#     })


# # Security event logging
# def log_security_event(event_type: str, severity: str, message: str, user_id: str = None,
#                       ip_address: str = None, logger: logging.Logger = None, **kwargs):
#     """Log security events"""
#     if logger is None:
#         logger = get_logger()
    
#     log_level = logging.WARNING if severity.upper() in ['LOW', 'MEDIUM'] else logging.ERROR
#     logger.log(log_level, f"Security event: {message}", extra={
#         'security_event_type': event_type,
#         'security_severity': severity,
#         'security_user_id': user_id,
#         'security_ip_address': ip_address,
#         'security_data': kwargs
#     })


# # Business logic logging
# def log_business_event(event_type: str, entity_type: str, entity_id: str, 
#                       action: str, logger: logging.Logger = None, **kwargs):
#     """Log business logic events"""
#     if logger is None:
#         logger = get_logger()
    
#     logger.info(f"Business event: {action} on {entity_type} {entity_id}", extra={
#         'business_event_type': event_type,
#         'business_entity_type': entity_type,
#         'business_entity_id': entity_id,
#         'business_action': action,
#         'business_data': kwargs
#     })


# # Audit logging
# def log_audit_event(action: str, resource: str, user_id: str, result: str,
#                    logger: logging.Logger = None, **kwargs):
#     """Log audit events"""
#     if logger is None:
#         logger = get_logger()
    
#     logger.info(f"Audit: {user_id} {action} {resource} - {result}", extra={
#         'audit_action': action,
#         'audit_resource': resource,
#         'audit_user_id': user_id,
#         'audit_result': result,
#         'audit_timestamp': datetime.utcnow().isoformat(),
#         'audit_data': kwargs
#     })


# # Configuration change logging
# def log_config_change(component: str, setting: str, old_value: str, new_value: str,
#                      changed_by: str = None, logger: logging.Logger = None, **kwargs):
#     """Log configuration changes"""
#     if logger is None:
#         logger = get_logger()
    
#     logger.warning(f"Configuration change: {component}.{setting} changed from {old_value} to {new_value}", extra={
#         'config_component': component,
#         'config_setting': setting,
#         'config_old_value': old_value,
#         'config_new_value': new_value,
#         'config_changed_by': changed_by,
#         'config_data': kwargs
#     })


# # System resource logging
# def log_system_resource(resource_type: str, usage: float, threshold: float = None,
#                        logger: logging.Logger = None, **kwargs):
#     """Log system resource usage"""
#     if logger is None:
#         logger = get_logger()
    
#     level = logging.WARNING if threshold and usage > threshold else logging.INFO
#     logger.log(level, f"System resource: {resource_type} usage at {usage}", extra={
#         'system_resource_type': resource_type,
#         'system_resource_usage': usage,
#         'system_resource_threshold': threshold,
#         'system_resource_data': kwargs
#     })


# # Async context manager for logging
# class AsyncLogContext:
#     """Async context manager for logging context"""
    
#     def __init__(self, correlation_id: str = None, user_id: str = None, 
#                  service_name: str = None, logger: logging.Logger = None):
#         self.correlation_id = correlation_id or generate_correlation_id()
#         self.user_id = user_id
#         self.service_name = service_name
#         self.logger = logger or get_logger()
#         self.start_time = None
#         self.old_correlation_id = None
#         self.old_user_id = None
#         self.old_service_name = None
    
#     async def __aenter__(self):
#         self.start_time = time.time()
#         self.old_correlation_id = correlation_id_var.get()
#         self.old_user_id = user_id_var.get()
#         self.old_service_name = service_name_var.get()
        
#         correlation_id_var.set(self.correlation_id)
#         if self.user_id:
#             user_id_var.set(self.user_id)
#         if self.service_name:
#             service_name_var.set(self.service_name)
        
#         self.logger.debug("Async context started", extra={
#             'context_correlation_id': self.correlation_id,
#             'context_user_id': self.user_id,
#             'context_service_name': self.service_name
#         })
        
#         return self
    
#     async def __aexit__(self, exc_type, exc_val, exc_tb):
#         duration = time.time() - self.start_time
        
#         # Reset context
#         correlation_id_var.set(self.old_correlation_id)
#         user_id_var.set(self.old_user_id)
#         service_name_var.set(self.old_service_name)
        
#         if exc_type:
#             self.logger.error(f"Async context failed after {duration:.3f}s", extra={
#                 'context_duration': duration,
#                 'context_error': str(exc_val),
#                 'context_exception_type': exc_type.__name__
#             })
#         else:
#             self.logger.debug(f"Async context completed in {duration:.3f}s", extra={
#                 'context_duration': duration
#             })


# # Log rotation and cleanup utilities
# class LogRotationManager:
#     """Manage log rotation and cleanup"""
    
#     def __init__(self, log_dir: str, max_age_days: int = 30, max_size_mb: int = 100):
#         self.log_dir = Path(log_dir)
#         self.max_age_days = max_age_days
#         self.max_size_mb = max_size_mb
#         self.logger = get_logger(__name__)
    
#     def cleanup_old_logs(self):
#         """Remove old log files"""
#         try:
#             cutoff_time = time.time() - (self.max_age_days * 24 * 60 * 60)
            
#             for log_file in self.log_dir.glob("*.log*"):
#                 if log_file.stat().st_mtime < cutoff_time:
#                     log_file.unlink()
#                     self.logger.info(f"Removed old log file: {log_file}")
        
#         except Exception as e:
#             self.logger.error(f"Error cleaning up old logs: {e}")
    
#     def check_log_sizes(self):
#         """Check and report log file sizes"""
#         try:
#             total_size = 0
#             large_files = []
            
#             for log_file in self.log_dir.glob("*.log*"):
#                 size_mb = log_file.stat().st_size / (1024 * 1024)
#                 total_size += size_mb
                
#                 if size_mb > self.max_size_mb:
#                     large_files.append((log_file, size_mb))
            
#             if large_files:
#                 self.logger.warning(f"Large log files detected", extra={
#                     'large_files': [(str(f), s) for f, s in large_files],
#                     'total_size_mb': total_size
#                 })
            
#             return total_size, large_files
        
#         except Exception as e:
#             self.logger.error(f"Error checking log sizes: {e}")
#             return 0, []


# # Export all public functions and classes
# __all__ = [
#     'setup_logging',
#     'get_logger',
#     'get_performance_logger',
#     'set_correlation_id',
#     'get_correlation_id',
#     'set_user_id',
#     'get_user_id',
#     'set_service_name',
#     'get_service_name',
#     'generate_correlation_id',
#     'LogContext',
#     'AsyncLogContext',
#     'log_function_call',
#     'log_async_function_call',
#     'log_exception',
#     'log_event',
#     'log_metric',
#     'log_alert',
#     'log_kafka_event',
#     'log_websocket_event',
#     'log_api_request',
#     'log_health_check',
#     'log_database_operation',
#     'log_cache_operation',
#     'log_network_operation',
#     'log_security_event',
#     'log_business_event',
#     'log_audit_event',
#     'log_config_change',
#     'log_system_resource',
#     'setup_request_logging',
#     'cleanup_request_logging',
#     'ErrorAggregator',
#     'PerformanceMonitor',
#     'LogRotationManager',
#     'performance_monitor',
#     'error_aggregator'
# ]

"""
Sophisticated Logging Utility for Distributed Real-Time Analytics System

This module provides:
- Structured logging with JSON format for production
- Contextual information (correlation IDs, service names)
- Performance logging decorators
- Thread-safe logging with minimal performance impact
- Log rotation and file management
- Distributed tracing support
"""

import json
import logging
import logging.handlers
import os
import sys
import time
import uuid
from contextvars import ContextVar
from functools import wraps
from typing import Any, Dict, Optional, Callable, TYPE_CHECKING # Added TYPE_CHECKING
from datetime import datetime
import threading
from pathlib import Path

# Use TYPE_CHECKING to avoid circular imports during runtime, but allow type hints for IDEs
if TYPE_CHECKING:
    from config.settings import Settings as AppSettings # Only for type hinting, not actual import at runtime

try:
    import structlog
    import colorlog
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    OPTIONAL_DEPS_AVAILABLE = True
except ImportError:
    OPTIONAL_DEPS_AVAILABLE = False
    # Fallback/warning for missing dependencies
    # print("Warning: Optional logging dependencies (structlog, colorlog, opentelemetry) not found. Some features will be unavailable.", file=sys.stderr)


# REMOVED: settings = get_settings() # This line is the root cause of the circular import / early loading issue

# Context variables for distributed tracing
correlation_id_var: ContextVar[str] = ContextVar('correlation_id', default='')
user_id_var: ContextVar[str] = ContextVar('user_id', default='')
service_name_var: ContextVar[str] = ContextVar('service_name', default='')

# Global logger manager instance (will be initialized upon first access or via setup_logging)
_logger_manager_instance: Optional['LoggerManager'] = None
_logger_manager_lock = threading.Lock()


class CorrelationIdFilter(logging.Filter):
    """Filter to add correlation ID, user ID, and service name to log records"""
    def __init__(self, service_name_default: str = 'analytics-pipeline'):
        super().__init__()
        self.service_name_default = service_name_default

    def filter(self, record):
        record.correlation_id = correlation_id_var.get() or str(uuid.uuid4())
        record.user_id = user_id_var.get() or 'anonymous'
        record.service_name = service_name_var.get() or self.service_name_default
        return True


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name
    
    def format(self, record):
        """Format log record as JSON"""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'service': self.service_name,
            'logger': record.name,
            'message': record.getMessage(),
            'correlation_id': getattr(record, 'correlation_id', ''),
            'user_id': getattr(record, 'user_id', ''),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'thread': record.thread,
            'process': record.process,
        }
        
        # Add exception information if present
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': self.formatException(record.exc_info)
            }
        
        # Add extra fields (ensure they are safe for JSON serialization)
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'lineno', 'funcName', 'created', 
                          'msecs', 'relativeCreated', 'thread', 'threadName', 
                          'processName', 'process', 'message', 'exc_info', 'exc_text', 
                          'stack_info', 'correlation_id', 'user_id', 'service_name', # Standard record attributes
                          '_stack_info', 'task', '_cached_format_exception', # Internal/private
                          ]:
                try:
                    # Attempt to serialize to ensure it's JSON-compatible
                    json.dumps(value) 
                    log_entry[key] = value
                except (TypeError, ValueError):
                    # Fallback for non-serializable objects (e.g., convert to string representation)
                    log_entry[key] = str(value)
        
        return json.dumps(log_entry)


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output (fallback if colorlog not available)"""
    def __init__(self, service_name: str):
        self.service_name = service_name # Store service name for consistency
        if OPTIONAL_DEPS_AVAILABLE:
            import colorlog
            self.formatter = colorlog.ColoredFormatter(
                fmt='%(log_color)s%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'red,bg_white',
                }
            )
        else:
            super().__init__(
                fmt='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
    
    def format(self, record):
        if OPTIONAL_DEPS_AVAILABLE:
            return self.formatter.format(record)
        else:
            return super().format(record)


class PerformanceLogger:
    """Performance logging utility with timing and metrics"""
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        if OPTIONAL_DEPS_AVAILABLE:
            self.tracer = trace.get_tracer(__name__)
        else:
            self.tracer = None
    
    def log_performance(self, operation: str, duration: float, **kwargs):
        """Log performance metrics"""
        self.logger.info(
            f"Performance: {operation} completed in {duration:.3f}s",
            extra={
                'operation': operation,
                'duration': duration,
                'metrics': kwargs
            }
        )
    
    def time_function(self, func_name: str = None):
        """Decorator to time function execution (sync)"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                operation_name = func_name or f"{func.__module__}.{func.__name__}"
                
                if self.tracer:
                    with self.tracer.start_as_current_span(operation_name) as span:
                        try:
                            result = func(*args, **kwargs)
                            if OPTIONAL_DEPS_AVAILABLE:
                                span.set_status(Status(StatusCode.OK))
                            return result
                        except Exception as e:
                            if OPTIONAL_DEPS_AVAILABLE:
                                span.set_status(Status(StatusCode.ERROR, str(e)))
                                span.record_exception(e)
                            raise
                        finally:
                            duration = time.time() - start_time
                            self.log_performance(operation_name, duration)
                            if OPTIONAL_DEPS_AVAILABLE:
                                span.set_attribute("duration", duration)
                else:
                    try:
                        result = func(*args, **kwargs)
                        return result
                    except Exception as e:
                        raise
                    finally:
                        duration = time.time() - start_time
                        self.log_performance(operation_name, duration)
            return wrapper
        return decorator
    
    def time_async_function(self, func_name: str = None):
        """Decorator to time async function execution"""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                start_time = time.time()
                operation_name = func_name or f"{func.__module__}.{func.__name__}"
                
                if self.tracer:
                    with self.tracer.start_as_current_span(operation_name) as span:
                        try:
                            result = await func(*args, **kwargs)
                            if OPTIONAL_DEPS_AVAILABLE:
                                span.set_status(Status(StatusCode.OK))
                            return result
                        except Exception as e:
                            if OPTIONAL_DEPS_AVAILABLE:
                                span.set_status(Status(StatusCode.ERROR, str(e)))
                                span.record_exception(e)
                            raise
                        finally:
                            duration = time.time() - start_time
                            self.log_performance(operation_name, duration)
                            if OPTIONAL_DEPS_AVAILABLE:
                                span.set_attribute("duration", duration)
                else:
                    try:
                        result = await func(*args, **kwargs)
                        return result
                    except Exception as e:
                        raise
                    finally:
                        duration = time.time() - start_time
                        self.log_performance(operation_name, duration)
            return wrapper
        return decorator


class LoggerManager:
    """Centralized logger management for the application."""
    
    _initialized_with_settings = False # Flag to track if setup_logging has been called with settings

    def __init__(self):
        self.loggers: Dict[str, logging.Logger] = {}
        self.performance_loggers: Dict[str, PerformanceLogger] = {}
        self._root_logger_configured = False # Internal flag for root logger setup

    def configure_root_logger(self, log_config: Any): # log_config will be a settings.LoggingConfig object
        """
        Configures the root Python logger based on the provided log_config.
        This method should be called only once during application startup.
        """
        if self._root_logger_configured:
            # logging.getLogger(__name__).warning("Root logger already configured. Skipping re-configuration.")
            return # Prevent multiple setups unless explicitly cleared

        root_logger = logging.getLogger()
        
        # Determine log level
        log_level_str = 'INFO'
        if hasattr(log_config, 'level'):
            level_val = log_config.level
            if hasattr(level_val, 'value'): # Handle Enum members
                log_level_str = level_val.value.upper()
            else: # Handle direct string values
                log_level_str = str(level_val).upper()
        
        log_level = getattr(logging, log_level_str, logging.INFO) # Default to INFO if invalid
        root_logger.setLevel(log_level)
        
        # Clear any existing handlers to prevent duplicates on re-configuration
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        
        # Determine service name for structured logs
        service_name = getattr(log_config, 'service_name', 'analytics-pipeline')
        
        # Add correlation ID filter (initialized with default service name)
        correlation_filter = CorrelationIdFilter(service_name_default=service_name)
        
        # Console handler
        if getattr(log_config, 'console_enabled', True):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(log_level)
            console_handler.addFilter(correlation_filter)
            
            if getattr(log_config, 'json_format', False):
                console_handler.setFormatter(JSONFormatter(service_name=service_name))
            else:
                console_handler.setFormatter(ColoredFormatter(service_name=service_name)) # Pass service_name
            
            root_logger.addHandler(console_handler)
        
        # File handler
        if getattr(log_config, 'file_enabled', False):
            try:
                file_path = getattr(log_config, 'file_path', 'logs/app.log')
                log_dir = Path(file_path).parent
                log_dir.mkdir(parents=True, exist_ok=True)
                
                file_handler = logging.handlers.RotatingFileHandler(
                    filename=file_path,
                    maxBytes=getattr(log_config, 'max_file_size', 10*1024*1024),
                    backupCount=getattr(log_config, 'backup_count', 5)
                )
                file_handler.setLevel(log_level)
                file_handler.addFilter(correlation_filter)
                file_handler.setFormatter(JSONFormatter(service_name=service_name))
                
                root_logger.addHandler(file_handler)
            except Exception as e:
                # If file handler setup fails, log to console (if enabled) and continue
                if getattr(log_config, 'console_enabled', True):
                    root_logger.critical(f"Failed to set up file logging: {e}", exc_info=True)
                else:
                    print(f"CRITICAL: Could not set up file logging: {e}", file=sys.stderr)
        
        # Ensure all existing loggers propagate to the root after setup
        for logger_name in logging.Logger.manager.loggerDict:
            if isinstance(logging.Logger.manager.loggerDict[logger_name], logging.Logger):
                logging.Logger.manager.loggerDict[logger_name].propagate = True

        self._root_logger_configured = True
        # Set initial context for the main logger instance to be used by get_logger()
        # This prevents the initial fallback get_logger from being used always.
        set_service_name(service_name)
        # We cannot get a logger here without potential infinite recursion if get_logger calls _get_logger_manager
        # So we trust the root logger is configured.

    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """Get or create a logger with the given name. Uses a fallback if not configured."""
        if name is None:
            # Automatically determine caller's module name if not provided
            import inspect
            frame = inspect.currentframe()
            # Traverse up until we're outside this module's functions
            while frame and frame.f_globals.get('__name__', '').startswith('utils.logger'):
                frame = frame.f_back
            name = frame.f_globals.get('__name__', 'root') if frame else 'root'

        # If root logger is not yet configured, provide a basic logger as a fallback
        if not self._root_logger_configured:
            # Configure a basic logger if it's the very first call before main app setup
            # This handles cases where some modules log before setup_logging is called
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            return logging.getLogger(name)

        if name not in self.loggers:
            logger_instance = logging.getLogger(name)
            self.loggers[name] = logger_instance
        return self.loggers[name]
    
    def get_performance_logger(self, name: str) -> 'PerformanceLogger': # Added type hint for clarity
        """Get or create a performance logger"""
        if name not in self.performance_loggers:
            logger = self.get_logger(name)
            self.performance_loggers[name] = PerformanceLogger(logger)
        return self.performance_loggers[name]


def _get_logger_manager() -> 'LoggerManager':
    """Singleton pattern for LoggerManager."""
    global _logger_manager_instance
    if _logger_manager_instance is None:
        with _logger_manager_lock:
            if _logger_manager_instance is None:
                _logger_manager_instance = LoggerManager()
    return _logger_manager_instance


def setup_logging(log_config: Any): # Changed signature to accept the full log_config object from settings
    """
    Configures the global logging system based on provided settings.LoggingConfig object.
    This function should be called once during application startup.
    """
    manager = _get_logger_manager()
    manager.configure_root_logger(log_config) # Pass the log_config object
    logging.getLogger(__name__).info("Logging system configured successfully.")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get logger instance for the calling module"""
    manager = _get_logger_manager()
    return manager.get_logger(name)


def get_performance_logger(name: Optional[str] = None) -> 'PerformanceLogger':
    """Get performance logger instance"""
    manager = _get_logger_manager()
    return manager.get_performance_logger(name)


def set_correlation_id(correlation_id: str):
    """Set correlation ID for current context"""
    correlation_id_var.set(correlation_id)


def get_correlation_id() -> str:
    """Get current correlation ID"""
    return correlation_id_var.get()


def set_user_id(user_id: str):
    """Set user ID for current context"""
    user_id_var.set(user_id)


def get_user_id() -> str:
    """Get current user ID"""
    return user_id_var.get()


def set_service_name(service_name: str):
    """Set service name for current context"""
    service_name_var.set(service_name)


def get_service_name() -> str:
    """Get current service name"""
    return service_name_var.get()


def generate_correlation_id() -> str:
    """Generate a new correlation ID"""
    return str(uuid.uuid4())


class LogContext:
    """Context manager for setting log context"""
    def __init__(self, correlation_id: str = None, user_id: str = None, service_name: str = None):
        self.correlation_id = correlation_id or generate_correlation_id()
        self.user_id = user_id
        self.service_name = service_name
        self.old_correlation_id = None
        self.old_user_id = None
        self.old_service_name = None
    
    def __enter__(self):
        self.old_correlation_id = correlation_id_var.get()
        self.old_user_id = user_id_var.get()
        self.old_service_name = service_name_var.get()
        
        correlation_id_var.set(self.correlation_id)
        if self.user_id is not None: # Use is not None to allow empty string
            user_id_var.set(self.user_id)
        if self.service_name is not None:
            service_name_var.set(self.service_name)
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        correlation_id_var.set(self.old_correlation_id)
        user_id_var.set(self.old_user_id)
        service_name_var.set(self.old_service_name)


# Convenience decorators
def log_function_call(logger: logging.Logger = None, level: str = 'INFO'):
    """Decorator to log function calls"""
    def decorator(func):
        _logger_to_use = logger if logger is not None else get_logger(func.__module__)
        _log_level_numeric = getattr(logging, level.upper())

        @wraps(func)
        def wrapper(*args, **kwargs):
            _logger_to_use.log(_log_level_numeric, f"Calling {func.__name__}", extra={
                'function': func.__name__,
                'args_count': len(args),
                'kwargs_count': len(kwargs)
            })
            
            try:
                result = func(*args, **kwargs)
                _logger_to_use.log(_log_level_numeric, f"Completed {func.__name__}", extra={
                    'function': func.__name__,
                    'success': True
                })
                return result
            except Exception as e:
                _logger_to_use.error(f"Failed {func.__name__}: {str(e)}", exc_info=True, extra={ # Added exc_info
                    'function': func.__name__,
                    'error': str(e),
                    'exception_type': type(e).__name__
                })
                raise
        return wrapper
    return decorator


def log_async_function_call(logger: logging.Logger = None, level: str = 'INFO'):
    """Decorator to log async function calls"""
    def decorator(func):
        _logger_to_use = logger if logger is not None else get_logger(func.__module__)
        _log_level_numeric = getattr(logging, level.upper())

        @wraps(func)
        async def wrapper(*args, **kwargs):
            _logger_to_use.log(_log_level_numeric, f"Calling async {func.__name__}", extra={
                'function': func.__name__,
                'args_count': len(args),
                'kwargs_count': len(kwargs)
            })
            
            try:
                result = await func(*args, **kwargs)
                _logger_to_use.log(_log_level_numeric, f"Completed async {func.__name__}", extra={
                    'function': func.__name__,
                    'success': True
                })
                return result
            except Exception as e:
                _logger_to_use.error(f"Failed async {func.__name__}: {str(e)}", exc_info=True, extra={ # Added exc_info
                    'function': func.__name__,
                    'error': str(e),
                    'exception_type': type(e).__name__
                })
                raise
        return wrapper
    return decorator


def log_exception(logger: logging.Logger = None, reraise: bool = True):
    """Decorator to log exceptions"""
    def decorator(func):
        _logger_to_use = logger if logger is not None else get_logger(func.__module__)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                _logger_to_use.exception(f"Exception in {func.__name__}: {str(e)}", extra={
                    'function': func.__name__,
                    'exception_type': type(e).__name__,
                    'args_count': len(args),
                    'kwargs_count': len(kwargs)
                })
                if reraise:
                    raise
                return None
        return wrapper
    return decorator


# Structured logging helpers
def log_event(event_type: str, message: str, logger: logging.Logger = None, **kwargs):
    """Log a structured event"""
    _logger_to_use = logger if logger is not None else get_logger()
    _logger_to_use.info(message, extra={
        'event_type': event_type,
        'event_data': kwargs
    })


def log_metric(metric_name: str, value: float, unit: str = None, logger: logging.Logger = None, **tags):
    """Log a metric"""
    _logger_to_use = logger if logger is not None else get_logger()
    _logger_to_use.info(f"Metric: {metric_name} = {value}", extra={
        'metric_name': metric_name,
        'metric_value': value,
        'metric_unit': unit,
        'metric_tags': tags
    })


def log_alert(alert_type: str, message: str, severity: str, logger: logging.Logger = None, **kwargs):
    """Log an alert"""
    _logger_to_use = logger if logger is not None else get_logger()
    
    log_level = logging.WARNING if severity.upper() in ['LOW', 'MEDIUM'] else logging.ERROR
    _logger_to_use.log(log_level, f"Alert: {message}", extra={
        'alert_type': alert_type,
        'alert_severity': severity,
        'alert_data': kwargs
    })


def log_kafka_event(topic: str, event_type: str, message: str, logger: logging.Logger = None, **kwargs):
    """Log Kafka-related events"""
    _logger_to_use = logger if logger is not None else get_logger()
    _logger_to_use.info(f"Kafka {event_type}: {message}", extra={
        'kafka_topic': topic,
        'kafka_event_type': event_type,
        'kafka_data': kwargs
    })


def log_websocket_event(event_type: str, client_id: str, message: str, logger: logging.Logger = None, **kwargs):
    """Log WebSocket events"""
    _logger_to_use = logger if logger is not None else get_logger()
    _logger_to_use.info(f"WebSocket {event_type}: {message}", extra={
        'websocket_event_type': event_type,
        'websocket_client_id': client_id,
        'websocket_data': kwargs
    })


def log_api_request(method: str, path: str, status_code: int, duration: float, logger: logging.Logger = None, **kwargs):
    """Log API requests"""
    _logger_to_use = logger if logger is not None else get_logger()
    _logger_to_use.info(f"API {method} {path} {status_code} ({duration:.3f}s)", extra={
        'api_method': method,
        'api_path': path,
        'api_status_code': status_code,
        'api_duration': duration,
        'api_data': kwargs
    })


# Health check logging
def log_health_check(component: str, status: str, details: Dict[str, Any] = None, logger: logging.Logger = None):
    """Log health check results"""
    _logger_to_use = logger if logger is not None else get_logger()
    
    log_level = logging.INFO if status == 'healthy' else logging.WARNING
    _logger_to_use.log(log_level, f"Health check: {component} is {status}", extra={
        'health_component': component,
        'health_status': status,
        'health_details': details or {}
    })


# Error aggregation helpers
class ErrorAggregator:
    """Aggregate and report errors"""
    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or get_logger(__name__)
        self.error_counts = {}
        self.lock = threading.Lock()
    
    def record_error(self, error_type: str, error_message: str, **kwargs):
        """Record an error occurrence"""
        with self.lock:
            key = f"{error_type}:{error_message}"
            if key not in self.error_counts:
                self.error_counts[key] = {
                    'count': 0,
                    'first_seen': datetime.utcnow(),
                    'last_seen': datetime.utcnow(),
                    'details': kwargs
                }
            
            self.error_counts[key]['count'] += 1
            self.error_counts[key]['last_seen'] = datetime.utcnow()
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary"""
        with self.lock:
            return dict(self.error_counts)
    
    def log_error_summary(self):
        """Log error summary"""
        summary = self.get_error_summary()
        if summary:
            self.logger.warning("Error summary", extra={
                'error_summary': summary,
                'total_error_types': len(summary)
            })


# Global error aggregator (initialized with default logger, will update after setup_logging)
error_aggregator = ErrorAggregator()


# Utility functions for common logging patterns
def setup_request_logging(correlation_id: str = None, user_id: str = None):
    """Setup logging context for a request"""
    if correlation_id is None:
        correlation_id = generate_correlation_id()
    
    set_correlation_id(correlation_id)
    if user_id:
        set_user_id(user_id)
    
    return correlation_id


def cleanup_request_logging():
    """Cleanup logging context after request"""
    correlation_id_var.set('')
    user_id_var.set('')


# Performance monitoring helpers
class PerformanceMonitor:
    """Monitor and log performance metrics"""
    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or get_logger(__name__)
        self.metrics = {}
        self.lock = threading.Lock()
    
    def record_metric(self, name: str, value: float, unit: str = 'ms', tags: Dict[str, str] = None):
        """Record a performance metric"""
        with self.lock:
            if name not in self.metrics:
                self.metrics[name] = []
            
            self.metrics[name].append({
                'value': value,
                'unit': unit,
                'timestamp': time.time(),
                'tags': tags or {}
            })
    
    def get_metrics_summary(self) -> Dict[str, Dict[str, float]]:
        """Get summary statistics for all metrics"""
        with self.lock:
            summary = {}
            for name, values in self.metrics.items():
                if values:
                    vals = [v['value'] for v in values]
                    summary[name] = {
                        'count': len(vals),
                        'min': min(vals),
                        'max': max(vals),
                        'avg': sum(vals) / len(vals),
                        'last': vals[-1]
                    }
            return summary
    
    def log_metrics_summary(self):
        """Log performance metrics summary"""
        summary = self.get_metrics_summary()
        if summary:
            self.logger.info("Performance metrics summary", extra={
                'performance_metrics': summary
            })
    
    def clear_metrics(self):
        """Clear all stored metrics"""
        with self.lock:
            self.metrics.clear()


# Global performance monitor (initialized with default logger, will update after setup_logging)
performance_monitor = PerformanceMonitor()


# Database operation logging
def log_database_operation(operation: str, table: str, duration: float, rows_affected: int = None, 
                          logger: logging.Logger = None, **kwargs):
    """Log database operations"""
    _logger_to_use = logger if logger is not None else get_logger()
    _logger_to_use.info(f"Database {operation} on {table} ({duration:.3f}s)", extra={
        'db_operation': operation,
        'db_table': table,
        'db_duration': duration,
        'db_rows_affected': rows_affected,
        'db_extra': kwargs
    })


# Cache operation logging
def log_cache_operation(operation: str, key: str, hit: bool = None, duration: float = None,
                       logger: logging.Logger = None, **kwargs):
    """Log cache operations"""
    _logger_to_use = logger if logger is not None else get_logger()
    _logger_to_use.info(f"Cache {operation} for key {key}", extra={
        'cache_operation': operation,
        'cache_key': key,
        'cache_hit': hit,
        'cache_duration': duration,
        'cache_extra': kwargs
    })


# Network operation logging
def log_network_operation(operation: str, endpoint: str, status: int = None, 
                         duration: float = None, logger: logging.Logger = None, **kwargs):
    """Log network operations"""
    _logger_to_use = logger if logger is not None else get_logger()
    _logger_to_use.info(f"Network {operation} to {endpoint}", extra={
        'network_operation': operation,
        'network_endpoint': endpoint,
        'network_status': status,
        'network_duration': duration,
        'network_extra': kwargs
    })


# Security event logging
def log_security_event(event_type: str, severity: str, message: str, user_id: str = None,
                      ip_address: str = None, logger: logging.Logger = None, **kwargs):
    """Log security events"""
    _logger_to_use = logger if logger is not None else get_logger()
    
    log_level = logging.WARNING if severity.upper() in ['LOW', 'MEDIUM'] else logging.ERROR
    _logger_to_use.log(log_level, f"Security event: {message}", extra={
        'security_event_type': event_type,
        'security_severity': severity,
        'security_user_id': user_id,
        'security_ip_address': ip_address,
        'security_data': kwargs
    })


# Business logic logging
def log_business_event(event_type: str, entity_type: str, entity_id: str, 
                      action: str, logger: logging.Logger = None, **kwargs):
    """Log business logic events"""
    _logger_to_use = logger if logger is not None else get_logger()
    _logger_to_use.info(f"Business event: {action} on {entity_type} {entity_id}", extra={
        'business_event_type': event_type,
        'business_entity_type': entity_type,
        'business_entity_id': entity_id,
        'business_action': action,
        'business_data': kwargs
    })


# Audit logging
def log_audit_event(action: str, resource: str, user_id: str, result: str,
                   logger: logging.Logger = None, **kwargs):
    """Log audit events"""
    _logger_to_use = logger if logger is not None else get_logger()
    _logger_to_use.info(f"Audit: {user_id} {action} {resource} - {result}", extra={
        'audit_action': action,
        'audit_resource': resource,
        'audit_user_id': user_id,
        'audit_result': result,
        'audit_timestamp': datetime.utcnow().isoformat(),
        'audit_data': kwargs
    })


# Configuration change logging
def log_config_change(component: str, setting: str, old_value: str, new_value: str,
                     changed_by: str = None, logger: logging.Logger = None, **kwargs):
    """Log configuration changes"""
    _logger_to_use = logger if logger is not None else get_logger()
    _logger_to_use.warning(f"Configuration change: {component}.{setting} changed from {old_value} to {new_value}", extra={
        'config_component': component,
        'config_setting': setting,
        'config_old_value': old_value,
        'config_new_value': new_value,
        'config_changed_by': changed_by,
        'config_data': kwargs
    })


# System resource logging
def log_system_resource(resource_type: str, usage: float, threshold: float = None,
                       logger: logging.Logger = None, **kwargs):
    """Log system resource usage"""
    _logger_to_use = logger if logger is not None else get_logger()
    
    level = logging.WARNING if threshold and usage > threshold else logging.INFO
    _logger_to_use.log(level, f"System resource: {resource_type} usage at {usage}", extra={
        'system_resource_type': resource_type,
        'system_resource_usage': usage,
        'system_resource_threshold': threshold,
        'system_resource_data': kwargs
    })


# Async context manager for logging
class AsyncLogContext:
    """Async context manager for logging context"""
    def __init__(self, correlation_id: str = None, user_id: str = None, 
                 service_name: str = None, logger: logging.Logger = None):
        self.correlation_id = correlation_id or generate_correlation_id()
        self.user_id = user_id
        self.service_name = service_name
        self.logger = logger or get_logger() # Use get_logger() which now handles fallback
        self.start_time = None
        self.old_correlation_id = None
        self.old_user_id = None
        self.old_service_name = None
    
    async def __aenter__(self):
        self.start_time = time.time()
        self.old_correlation_id = correlation_id_var.get()
        self.old_user_id = user_id_var.get()
        self.old_service_name = service_name_var.get()
        
        correlation_id_var.set(self.correlation_id)
        if self.user_id is not None:
            user_id_var.set(self.user_id)
        if self.service_name is not None:
            service_name_var.set(self.service_name)
        
        self.logger.debug("Async context started", extra={
            'context_correlation_id': self.correlation_id,
            'context_user_id': self.user_id,
            'context_service_name': self.service_name
        })
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        
        # Reset context
        correlation_id_var.set(self.old_correlation_id)
        user_id_var.set(self.old_user_id)
        service_name_var.set(self.old_old_service_name) # Corrected typo here
        
        if exc_type:
            self.logger.error(f"Async context failed after {duration:.3f}s", exc_info=True, extra={
                'context_duration': duration,
                'context_error': str(exc_val),
                'context_exception_type': exc_type.__name__
            })
        else:
            self.logger.debug(f"Async context completed in {duration:.3f}s", extra={
                'context_duration': duration
            })


# Log rotation and cleanup utilities
class LogRotationManager:
    """Manage log rotation and cleanup"""
    def __init__(self, log_dir: str, max_age_days: int = 30, max_size_mb: int = 100):
        self.log_dir = Path(log_dir)
        self.max_age_days = max_age_days
        self.max_size_mb = max_size_mb
        self.logger = get_logger(__name__)
    
    def cleanup_old_logs(self):
        """Remove old log files"""
        try:
            cutoff_time = time.time() - (self.max_age_days * 24 * 60 * 60)
            
            for log_file in self.log_dir.glob("*.log*"):
                if log_file.stat().st_mtime < cutoff_time:
                    log_file.unlink()
                    self.logger.info(f"Removed old log file: {log_file}")
        except Exception as e:
            self.logger.error(f"Error cleaning up old logs: {e}", exc_info=True)
    
    def check_log_sizes(self):
        """Check and report log file sizes"""
        try:
            total_size = 0
            large_files = []
            
            for log_file in self.log_dir.glob("*.log*"):
                size_mb = log_file.stat().st_size / (1024 * 1024)
                total_size += size_mb
                
                if size_mb > self.max_size_mb:
                    large_files.append((log_file, size_mb))
            
            if large_files:
                self.logger.warning(f"Large log files detected", extra={
                    'large_files': [(str(f), s) for f, s in large_files],
                    'total_size_mb': total_size
                })
            
            return total_size, large_files
        except Exception as e:
            self.logger.error(f"Error checking log sizes: {e}", exc_info=True)
            return 0, []


# Global error aggregator (initialized with default logger, will update after setup_logging)
# It's better to make these global objects use the get_logger() function to ensure they get
# a properly configured logger *after* setup_logging has run.
# They will initially get the basic fallback logger, then switch when root logger is configured.
error_aggregator = ErrorAggregator(logger=get_logger(__name__ + '.ErrorAggregator'))
performance_monitor = PerformanceMonitor(logger=get_logger(__name__ + '.PerformanceMonitor'))


# Export all public functions and classes
__all__ = [
    'setup_logging',
    'get_logger',
    'get_performance_logger',
    'set_correlation_id',
    'get_correlation_id',
    'set_user_id',
    'get_user_id',
    'set_service_name',
    'get_service_name',
    'generate_correlation_id',
    'LogContext',
    'AsyncLogContext',
    'log_function_call',
    'log_async_function_call',
    'log_exception',
    'log_event',
    'log_metric',
    'log_alert',
    'log_kafka_event',
    'log_websocket_event',
    'log_api_request',
    'log_health_check',
    'log_database_operation',
    'log_cache_operation',
    'log_network_operation',
    'log_security_event',
    'log_business_event',
    'log_audit_event',
    'log_config_change',
    'log_system_resource',
    'setup_request_logging',
    'cleanup_request_logging',
    'ErrorAggregator',
    'PerformanceMonitor',
    'LogRotationManager',
    'performance_monitor',
    'error_aggregator'
]

# This block is for testing `logger.py` directly, not usually run by `main.py`
if __name__ == "__main__":
    # In direct testing, ensure env is loaded if needed for settings indirectly.
    # But for this file, we want to test its standalone functionality.
    # No direct call to get_settings() here to avoid circular dependency.
    
    # Minimal example of how settings would be passed
    class MockLoggingSettings:
        def __init__(self):
            self.level = 'INFO'
            self.json_format = False
            self.service_name = 'test-service'
            self.console_enabled = True
            self.file_enabled = False
            self.file_path = 'logs/test_app.log'
            self.max_file_size = 10 * 1024 # Small size for testing rotation
            self.backup_count = 2

    # Initialize a basic logger before setup_logging to see fallback
    print("--- Testing initial fallback logger ---")
    initial_logger = logging.getLogger("initial.logger")
    initial_logger.info("This is an initial message before full logging setup.")
    initial_logger.warning("This warning might not have full formatting.")

    print("\n--- Testing basic text logging ---")
    mock_settings = MockLoggingSettings()
    setup_logging(log_config=mock_settings) # Pass mock settings
    
    logger_test_1 = get_logger("test.module.1")
    logger_test_1.info("This is an info message after setup.")
    logger_test_1.warning("This is a warning message.")
    logger_test_1.error("This is an error message with some data.", extra_data={'code': 123, 'details': 'something went wrong'})

    print("\n--- Testing JSON logging ---")
    mock_settings.json_format = True
    mock_settings.level = 'DEBUG'
    setup_logging(log_config=mock_settings) # Reconfigure to JSON
    
    logger_test_2 = get_logger("test.module.2")
    logger_test_2.debug("A debug message in JSON.")
    
    # Using LogContext
    with LogContext(correlation_id="abc-123", user_id="user-456", service_name="my_service"):
        logger_test_2.info("An info message with context.")
    logger_test_2.error("An error message without context.")

    print("\n--- Testing timed logging ---")
    mock_settings.json_format = False # Reset to text format for cleaner timed output
    mock_settings.level = 'INFO'
    setup_logging(log_config=mock_settings) 
    
    logger_test_3 = get_logger("test.module.3")
    perf_logger_test = get_performance_logger("test.perf")

    @perf_logger_test.time_function()
    def my_sync_function():
        time.sleep(0.1)
        return "Sync done"

    @perf_logger_test.time_async_function()
    async def my_async_function():
        await asyncio.sleep(0.05)
        return "Async done"

    my_sync_function()
    asyncio.run(my_async_function())
    
    print("\n--- Testing file logging and rotation (check logs/test_app.log and backups) ---")
    mock_settings.file_enabled = True
    mock_settings.console_enabled = False # Only file output
    mock_settings.file_path = 'logs/test_app.log'
    mock_settings.max_file_size = 1024 # Very small size to force rotation
    mock_settings.backup_count = 2
    setup_logging(log_config=mock_settings)
    
    logger_test_4 = get_logger("test.file.logger")
    for i in range(10): # Write enough to force rotation
        logger_test_4.info(f"This message should go to logs/test_app.log only. Message {i}")
    
    # Test error aggregator
    error_aggregator.record_error("TEST_ERROR", "Something broke", component="test", code=500)
    error_aggregator.log_error_summary()

    # Test rotation manager (manual trigger for testing)
    # Ensure logs directory exists for this test
    Path('logs').mkdir(parents=True, exist_ok=True)
    rotation_manager = LogRotationManager('logs', max_age_days=0, max_size_mb=0.0001) # Set extreme limits for immediate cleanup
    rotation_manager.cleanup_old_logs()
    rotation_manager.check_log_sizes()
    
    print("\n--- Done testing utils/logger.py ---")