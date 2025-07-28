# #!/usr/bin/env python3
# """
# Main application orchestrator for the real-time analytics pipeline.
# Manages service lifecycle, dependency injection, and graceful shutdown.
# """

# import asyncio
# import signal
# import sys
# import time
# import uuid
# from contextlib import asynccontextmanager
# from typing import Dict, List, Optional

# import uvicorn
# from fastapi import FastAPI, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.middleware.trustedhost import TrustedHostMiddleware
# from fastapi.responses import JSONResponse

# # Import all components
# from config.settings import Settings, get_settings
# from utils.logger import get_logger, setup_logging
# from kafka_handlers.kafka_producer import KafkaProducerClient
# from kafka_handlers.kafka_consumer import KafkaConsumer
# from kafka_handlers.topics import TopicManager
# from alerts.alert_store import AlertStore
# from alerts.alert_logic import AlertEngine
# from ws.websocket_manager import WebSocketManager
# from ws.ws_server import setup_websocket_routes
# from api.api_router import setup_api_routes
# from api.auth import AuthManager

# # Global components registry
# class ServiceRegistry:
#     """Global service registry for dependency injection."""
    
#     def __init__(self):
#         self.services: Dict[str, any] = {}
#         self.startup_order: List[str] = [
#             'settings',
#             'logger',
#             'topic_manager',
#             'kafka_producer',
#             'alert_store',
#             'alert_engine',
#             'websocket_manager',
#             'auth_manager',
#             'kafka_consumer'
#         ]
#         self.shutdown_order: List[str] = list(reversed(self.startup_order))
    
#     def register(self, name: str, service: any):
#         """Register a service."""
#         self.services[name] = service
    
#     def get(self, name: str) -> any:
#         """Get a service by name."""
#         return self.services.get(name)
    
#     def get_all(self) -> Dict[str, any]:
#         """Get all registered services."""
#         return self.services.copy()

# # Global registry instance
# registry = ServiceRegistry()

# class HealthChecker:
#     """Health check manager for all services."""
    
#     def __init__(self, registry: ServiceRegistry):
#         self.registry = registry
#         self.logger = get_logger(__name__)
    
#     async def check_service_health(self, service_name: str) -> Dict[str, any]:
#         """Check health of a specific service."""
#         service = self.registry.get(service_name)
#         if not service:
#             return {
#                 'status': 'unknown',
#                 'message': f'Service {service_name} not found'
#             }
        
#         try:
#             # Check if service has a health_check method
#             if hasattr(service, 'health_check'):
#                 health = await service.health_check()
#                 return {
#                     'status': 'healthy' if health else 'unhealthy',
#                     'service': service_name,
#                     'timestamp': time.time()
#                 }
#             else:
#                 return {
#                     'status': 'healthy',
#                     'service': service_name,
#                     'message': 'No health check method available',
#                     'timestamp': time.time()
#                 }
#         except Exception as e:
#             self.logger.error(f"Health check failed for {service_name}: {e}")
#             return {
#                 'status': 'unhealthy',
#                 'service': service_name,
#                 'error': str(e),
#                 'timestamp': time.time()
#             }
    
#     async def check_all_services(self) -> Dict[str, any]:
#         """Check health of all registered services."""
#         results = {}
#         overall_status = 'healthy'
        
#         for service_name in self.registry.services:
#             health = await self.check_service_health(service_name)
#             results[service_name] = health
            
#             if health['status'] != 'healthy':
#                 overall_status = 'degraded'
        
#         return {
#             'overall_status': overall_status,
#             'services': results,
#             'timestamp': time.time()
#         }

# class ApplicationLifecycle:
#     """Manages application lifecycle events."""
    
#     def __init__(self):
#         self.logger = get_logger(__name__)
#         self.startup_complete = False
#         self.shutdown_initiated = False
#         self.correlation_id = str(uuid.uuid4())
    
#     async def startup_sequence(self):
#         """Execute startup sequence for all services."""
#         self.logger.info(f"Starting application startup sequence (correlation_id: {self.correlation_id})")
        
#         try:
#             # Initialize settings
#             settings = get_settings()
#             registry.register('settings', settings)
#             self.logger.info("Settings initialized")
            
#             # Setup logging
#             setup_logging(settings.logging.level, settings.logging.format)
#             logger = get_logger(__name__)
#             registry.register('logger', logger)
#             self.logger.info("Logging configured")
            
#             # Initialize Kafka topic manager
#             topic_manager = TopicManager()
#             registry.register('topic_manager', topic_manager)
#             self.logger.info("Kafka topic manager initialized")
            
#             # Initialize Kafka producer
#             kafka_producer = KafkaProducerClient(settings)
#             await kafka_producer.start()
#             registry.register('kafka_producer', kafka_producer)
#             self.logger.info("Kafka producer started")
            
#             # Initialize alert store
#             alert_store = AlertStore(settings)
#             await alert_store.initialize()
#             registry.register('alert_store', alert_store)
#             self.logger.info("Alert store initialized")
            
#             # Initialize alert engine
#             alert_engine = AlertEngine(settings, alert_store)
#             await alert_engine.initialize()
#             registry.register('alert_engine', alert_engine)
#             self.logger.info("Alert engine initialized")
            
#             # Initialize WebSocket manager
#             websocket_manager = WebSocketManager(settings)
#             await websocket_manager.startup()
#             registry.register('websocket_manager', websocket_manager)
#             self.logger.info("WebSocket manager initialized")
            
#             # Initialize auth manager
#             auth_manager = AuthManager(settings)
#             await auth_manager.initialize()
#             registry.register('auth_manager', auth_manager)
#             self.logger.info("Auth manager initialized")
            
#             # Initialize Kafka consumer (last, as it starts processing)
#             kafka_consumer = KafkaConsumer(settings, alert_engine)
#             await kafka_consumer.start()
#             registry.register('kafka_consumer', kafka_consumer)
#             self.logger.info("Kafka consumer started")
            
#             self.startup_complete = True
#             self.logger.info("Application startup completed successfully")
            
#         except Exception as e:
#             self.logger.error(f"Startup failed: {e}")
#             await self.shutdown_sequence()
#             raise
    
#     async def shutdown_sequence(self):
#         """Execute graceful shutdown sequence."""
#         if self.shutdown_initiated:
#             return
        
#         self.shutdown_initiated = True
#         self.logger.info("Initiating graceful shutdown sequence")
        
#         # Shutdown services in reverse order
#         for service_name in registry.shutdown_order:
#             service = registry.get(service_name)
#             if service and hasattr(service, 'shutdown'):
#                 try:
#                     self.logger.info(f"Shutting down {service_name}")
#                     await service.shutdown()
#                 except Exception as e:
#                     self.logger.error(f"Error shutting down {service_name}: {e}")
        
#         self.logger.info("Graceful shutdown completed")
    
#     async def health_check(self) -> Dict[str, any]:
#         """Application-level health check."""
#         if not self.startup_complete:
#             return {
#                 'status': 'starting',
#                 'message': 'Application is still starting up'
#             }
        
#         if self.shutdown_initiated:
#             return {
#                 'status': 'shutting_down',
#                 'message': 'Application is shutting down'
#             }
        
#         health_checker = HealthChecker(registry)
#         return await health_checker.check_all_services()

# # Global lifecycle manager
# lifecycle = ApplicationLifecycle()

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     """FastAPI lifespan context manager."""
#     # Startup
#     await lifecycle.startup_sequence()
    
#     # Setup signal handlers
#     def signal_handler(signum, frame):
#         asyncio.create_task(lifecycle.shutdown_sequence())
    
#     signal.signal(signal.SIGINT, signal_handler)
#     signal.signal(signal.SIGTERM, signal_handler)
    
#     yield
    
#     # Shutdown
#     await lifecycle.shutdown_sequence()

# def create_app() -> FastAPI:
#     """Create and configure the FastAPI application."""
#     settings = get_settings()
    
#     app = FastAPI(
#         title=settings.api.title,
#         description=settings.api.description,
#         version=settings.api.version,
#         lifespan=lifespan,
#         docs_url="/docs" if settings.environment != "production" else None,
#         redoc_url="/redoc" if settings.environment != "production" else None,
#     )
    
#     # Add middleware
#     app.add_middleware(
#         CORSMiddleware,
#         allow_origins=settings.get_allowed_origins(),
#         allow_credentials=True,
#         allow_methods=settings.get_cors_methods(),
#         allow_headers=["*"],
#     )
    
#     app.add_middleware(
#         TrustedHostMiddleware,
#         allowed_hosts=settings.get_allowed_hosts()
#     )
    
#     # Add global exception handler
#     @app.exception_handler(Exception)
#     async def global_exception_handler(request, exc):
#         logger = get_logger(__name__)
#         logger.error(f"Unhandled exception: {exc}", exc_info=True)
#         return JSONResponse(
#             status_code=500,
#             content={"detail": "Internal server error"}
#         )
    
#     # Health check endpoint
#     @app.get("/health")
#     async def health_check():
#         """Application health check endpoint."""
#         return await lifecycle.health_check()
    
#     @app.get("/health/ready")
#     async def readiness_check():
#         """Readiness check for Kubernetes."""
#         if lifecycle.startup_complete and not lifecycle.shutdown_initiated:
#             return {"status": "ready"}
#         raise HTTPException(status_code=503, detail="Service not ready")
    
#     @app.get("/health/live")
#     async def liveness_check():
#         """Liveness check for Kubernetes."""
#         if not lifecycle.shutdown_initiated:
#             return {"status": "alive"}
#         raise HTTPException(status_code=503, detail="Service not alive")
    
#     # Setup routes
#     setup_api_routes(app, registry)
#     setup_websocket_routes(app, registry)
    
#     return app

# def main():
#     """Main entry point."""
#     logger = None  # Initialize logger variable
    
#     try:
#         settings = get_settings()
        
#         # Setup basic logging before full initialization
#         setup_logging(settings.logging.level if hasattr(settings.logging, 'level') else 'INFO', 
#                      getattr(settings.logging, 'format', 'colored'))
#         logger = get_logger(__name__)
        
#         logger.info(f"Starting {settings.app_name}")
#         logger.info(f"Environment: {settings.environment}")
#         logger.info(f"API Host: {settings.api.host}:{settings.api.port}")
#         logger.info(f"WebSocket Host: {settings.websocket.host}:{settings.websocket.port}")
        
#         # Create application
#         app = create_app()
        
#         # Configure uvicorn
#         uvicorn_config = {
#             "host": settings.api.host,
#             "port": settings.api.port,
#             "log_level": (settings.logging.level.value if hasattr(settings.logging.level, 'value') 
#                          else str(settings.logging.level)).lower(),
#             "access_log": settings.environment != "production",
#             "reload": settings.hot_reload_enabled and settings.is_development(),
#             "workers": 1,  # Use 1 worker for proper lifecycle management
#         }
        
#         if settings.is_production():
#             uvicorn_config.update({
#                 "workers": settings.api.workers,
#                 "loop": "uvloop",
#                 "http": "httptools",
#             })
        
#         # Start server
#         uvicorn.run(app, **uvicorn_config)
        
#     except KeyboardInterrupt:
#         if logger:
#             logger.info("Received keyboard interrupt, shutting down...")
#         else:
#             print("Received keyboard interrupt, shutting down...")
#     except Exception as e:
#         if logger:
#             logger.error(f"Application failed to start: {e}")
#         else:
#             print(f"Application failed to start: {e}")
#         sys.exit(1)
# if __name__ == "__main__":
#     main()
    
#!/usr/bin/env python3
#!/usr/bin/env python3
# main.py

#!/usr/bin/env python3
# """
# Main application orchestrator for the real-time analytics pipeline.
# Manages service lifecycle, dependency injection, and graceful shutdown.
# """

# from dotenv import load_dotenv # MOVED: Load .env at the absolute top of the main entry point
# load_dotenv()                 # MOVED: This ensures env vars are available before any other module imports settings

# import asyncio
# import signal
# import sys
# import time
# import uuid
# from contextlib import asynccontextmanager
# from typing import Dict, List, Optional

# import uvicorn
# from fastapi import FastAPI, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.middleware.trustedhost import TrustedHostMiddleware
# from fastapi.responses import JSONResponse

# # Import all components (now that dotenv is loaded)
# from config.settings import Settings, get_settings
# from utils.logger import get_logger, setup_logging
# from kafka_handlers.kafka_producer import KafkaProducerClient
# from kafka_handlers.kafka_consumer import KafkaConsumer
# from kafka_handlers.topics import TopicManager
# from alerts.alert_store import AlertStore
# from alerts.alert_logic import AlertEngine
# from ws.websocket_manager import WebSocketManager
# from ws.ws_server import setup_websocket_routes
# from api.api_router import setup_api_routes
# from api.auth import AuthManager, get_auth_manager_instance


# # Global components registry
# class ServiceRegistry:
#     """Global service registry for dependency injection."""
    
#     def __init__(self):
#         self.services: Dict[str, any] = {}
#         self.startup_order: List[str] = [
#             'settings',
#             'logger',
#             'topic_manager',
#             'kafka_producer',
#             'alert_store',
#             'alert_engine',
#             'auth_manager',       # Moved before websocket_manager to ensure auth is ready
#             'websocket_manager',
#             'kafka_consumer'
#         ]
#         self.shutdown_order: List[str] = list(reversed(self.startup_order))
    
#     def register(self, name: str, service: any):
#         """Register a service."""
#         self.services[name] = service
    
#     def get(self, name: str) -> any:
#         """Get a service by name."""
#         return self.services.get(name)
    
#     def get_all(self) -> Dict[str, any]:
#         """Get all registered services."""
#         return self.services.copy()

# # Global registry instance
# registry = ServiceRegistry()

# class HealthChecker:
#     """Health check manager for all services."""
    
#     def __init__(self, registry: ServiceRegistry):
#         self.registry = registry
#         self.logger = get_logger(__name__)
    
#     async def check_service_health(self, service_name: str) -> Dict[str, any]:
#         """Check health of a specific service."""
#         service = self.registry.get(service_name)
#         if not service:
#             return {
#                 'status': 'unknown',
#                 'message': f'Service {service_name} not found'
#             }
        
#         try:
#             if hasattr(service, 'health_check'):
#                 health = await service.health_check()
#                 return {
#                     'status': 'healthy' if health else 'unhealthy',
#                     'service': service_name,
#                     'timestamp': time.time(),
#                     'details': health if isinstance(health, dict) else None
#                 }
#             else:
#                 return {
#                     'status': 'healthy',
#                     'service': service_name,
#                     'message': 'No specific health check method available',
#                     'timestamp': time.time()
#                 }
#         except Exception as e:
#             self.logger.error(f"Health check failed for {service_name}: {e}", exc_info=True)
#             return {
#                 'status': 'unhealthy',
#                 'service': service_name,
#                 'error': str(e),
#                 'timestamp': time.time()
#             }
    
#     async def check_all_services(self) -> Dict[str, any]:
#         """Check health of all registered services."""
#         results = {}
#         overall_status = 'healthy'
        
#         services_to_check = registry.startup_order
        
#         for service_name in services_to_check:
#             if service_name not in self.registry.services:
#                 results[service_name] = {'status': 'not_initialized', 'message': 'Service not registered yet'}
#                 overall_status = 'degraded'
#                 continue

#             health = await self.check_service_health(service_name)
#             results[service_name] = health
            
#             if health['status'] != 'healthy':
#                 overall_status = 'degraded'
        
#         return {
#             'overall_status': overall_status,
#             'services': results,
#             'timestamp': time.time()
#         }

# class ApplicationLifecycle:
#     """Manages application lifecycle events."""
    
#     def __init__(self):
#         self.logger = get_logger(__name__)
#         self.startup_complete = False
#         self.shutdown_initiated = False
#         self.correlation_id = str(uuid.uuid4())
    
#     async def startup_sequence(self):
#         """Execute startup sequence for all services."""
#         self.logger.info(f"Starting application startup sequence (correlation_id: {self.correlation_id})")
        
#         try:
#             settings = get_settings()
#             registry.register('settings', settings)
#             self.logger.info("Settings initialized")
            
#             setup_logging(settings.logging.level, settings.logging.format)
#             logger = get_logger(__name__) 
#             registry.register('logger', logger)
#             self.logger.info("Logging configured")
            
#             topic_manager = TopicManager()
#             registry.register('topic_manager', topic_manager)
#             self.logger.info("Kafka topic manager initialized")
            
#             kafka_producer = KafkaProducerClient(settings)
#             await kafka_producer.start()
#             registry.register('kafka_producer', kafka_producer)
#             self.logger.info("Kafka producer started")
            
#             alert_store = AlertStore(settings)
#             await alert_store.initialize()
#             registry.register('alert_store', alert_store)
#             self.logger.info("Alert store initialized")
            
#             alert_engine = AlertEngine(settings, alert_store)
#             await alert_engine.initialize()
#             registry.register('alert_engine', alert_engine)
#             self.logger.info("Alert engine initialized")
            
#             global _auth_manager_instance
#             _auth_manager_instance = AuthManager(settings)
#             await _auth_manager_instance.initialize()
#             registry.register('auth_manager', _auth_manager_instance)
#             self.logger.info("Auth manager initialized")

#             websocket_manager = WebSocketManager(settings)
#             await websocket_manager.startup()
#             registry.register('websocket_manager', websocket_manager)
#             self.logger.info("WebSocket manager started")
            
#             kafka_consumer = KafkaConsumer(settings, alert_engine)
#             await kafka_consumer.start()
#             registry.register('kafka_consumer', kafka_consumer)
#             self.logger.info("Kafka consumer started")
            
#             self.startup_complete = True
#             self.logger.info("Application startup completed successfully")
            
#         except Exception as e:
#             self.logger.critical(f"Startup failed: {e}", exc_info=True)
#             await self.shutdown_sequence()
#             raise
    
#     async def shutdown_sequence(self):
#         """Execute graceful shutdown sequence."""
#         if self.shutdown_initiated:
#             return
        
#         self.shutdown_initiated = True
#         self.logger.info("Initiating graceful shutdown sequence")
        
#         for service_name in registry.shutdown_order:
#             service = registry.get(service_name)
#             if service:
#                 if hasattr(service, 'shutdown'):
#                     try:
#                         self.logger.info(f"Shutting down {service_name}")
#                         await service.shutdown()
#                     except Exception as e:
#                         self.logger.error(f"Error shutting down {service_name}: {e}", exc_info=True)
#                 else:
#                     self.logger.debug(f"Service {service_name} has no 'shutdown' method.")
#             else:
#                 self.logger.debug(f"Service {service_name} not found in registry during shutdown, skipping.")
        
#         self.logger.info("Graceful shutdown completed")
    
#     async def health_check(self) -> Dict[str, Any]:
#         """Application-level health check."""
#         if not self.startup_complete:
#             return {
#                 'status': 'starting',
#                 'message': 'Application is still starting up',
#                 'timestamp': time.time()
#             }
        
#         if self.shutdown_initiated:
#             return {
#                 'status': 'shutting_down',
#                 'message': 'Application is shutting down',
#                 'timestamp': time.time()
#             }
        
#         health_checker = HealthChecker(registry)
#         return await health_checker.check_all_services()

# # Global lifecycle manager
# lifecycle = ApplicationLifecycle()

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     """FastAPI lifespan context manager."""
#     try:
#         await lifecycle.startup_sequence()
#     except Exception:
#         sys.exit(1)
    
#     def signal_handler(signum, frame):
#         asyncio.create_task(lifecycle.shutdown_sequence())
#         asyncio.get_event_loop().call_later(2, sys.exit, 0)

#     if not lifecycle.get_settings().hot_reload_enabled or not lifecycle.get_settings().is_development():
#         signal.signal(signal.SIGINT, signal_handler)
#         signal.signal(signal.SIGTERM, signal_handler)
#     else:
#         lifecycle.logger.info("Skipping signal handler setup in development/hot-reload mode.")
    
#     yield
    
#     await lifecycle.shutdown_sequence()

# def create_app() -> FastAPI:
#     """Create and configure the FastAPI application."""
#     settings = get_settings()
    
#     app = FastAPI(
#         title=settings.api.title,
#         description=settings.api.description,
#         version=settings.api.version,
#         lifespan=lifespan,
#         docs_url="/docs" if settings.environment != Environment.PRODUCTION else None,
#         redoc_url="/redoc" if settings.environment != Environment.PRODUCTION else None,
#     )
    
#     app.add_middleware(
#         CORSMiddleware,
#         allow_origins=settings.get_allowed_origins(),
#         allow_credentials=True,
#         allow_methods=settings.get_cors_methods(),
#         allow_headers=["*"],
#     )
    
#     app.add_middleware(
#         TrustedHostMiddleware,
#         allowed_hosts=settings.get_allowed_hosts()
#     )
    
#     @app.exception_handler(Exception)
#     async def global_exception_handler(request: Request, exc: Exception):
#         logger = get_logger(__name__)
#         logger.error(f"Unhandled exception during request: {request.url} - {exc}", exc_info=True)
#         return JSONResponse(
#             status_code=500,
#             content={"detail": "Internal server error"}
#         )
    
#     @app.get("/health", summary="Get overall application health")
#     async def get_app_health():
#         return await lifecycle.health_check()
    
#     @app.get("/health/ready", summary="Readiness probe for deployment")
#     async def readiness_check():
#         if lifecycle.startup_complete and not lifecycle.shutdown_initiated:
#             health_status = await lifecycle.health_check()
#             if health_status['overall_status'] == 'healthy':
#                 return {"status": "ready"}
#             raise HTTPException(status_code=503, detail=f"Service not fully healthy: {health_status['overall_status']}")
#         raise HTTPException(status_code=503, detail="Service not ready: Startup incomplete or shutdown initiated.")
    
#     @app.get("/health/live", summary="Liveness probe for process monitoring")
#     async def liveness_check():
#         if not lifecycle.shutdown_initiated:
#             return {"status": "alive"}
#         raise HTTPException(status_code=503, detail="Service not alive: Shutdown initiated.")
    
#     setup_api_routes(app, registry)
#     setup_websocket_routes(app, registry)
    
#     return app

# def main():
#     """Main entry point."""
#     logger = None
    
#     try:
#         # load_dotenv() is now at the very top of main.py, so it's guaranteed to run first.
#         # Removed redundant call here.

#         settings = get_settings()
        
#         log_level_str = (settings.logging.level.value if isinstance(settings.logging.level, Enum)
#                          else str(settings.logging.level)).upper()
#         setup_logging(log_level_str, getattr(settings.logging, 'format', 'colored'))
#         logger = get_logger(__name__)
        
#         logger.info(f"Starting {settings.app_name} (Version: {settings.app_version})")
#         logger.info(f"Environment: {settings.environment}")
#         logger.info(f"API Host: {settings.api.host}:{settings.api.port}")
#         logger.info(f"WebSocket Host: {settings.websocket.host}:{settings.websocket.port}")
#         logger.info(f"Redis Host: {settings.redis.host}:{settings.redis.port} (Enabled: {settings.redis.enabled})")
#         logger.info(f"Auth DB URL: {settings.api.database_url}")
#         logger.info(f"WS Persistence Enabled: {settings.websocket.enable_persistence}")

#         app = create_app()
        
#         uvicorn_config = {
#             "app": app,
#             "host": settings.api.host,
#             "port": settings.api.port,
#             "log_level": log_level_str.lower(),
#             "access_log": settings.environment != Environment.PRODUCTION,
#             "reload": settings.hot_reload_enabled and settings.is_development(),
#             "workers": 1,
#         }
        
#         if not (settings.is_development() and settings.hot_reload_enabled):
#             uvicorn_config["workers"] = settings.api.workers

#         if settings.is_production():
#             uvicorn_config.update({
#                 "loop": "uvloop",
#                 "http": "httptools",
#             })
        
#         uvicorn.run(**uvicorn_config)
        
#     except KeyboardInterrupt:
#         if logger:
#             logger.info("Received keyboard interrupt, initiating graceful shutdown...")
#         else:
#             print("Received keyboard interrupt, initiating graceful shutdown...")
#     except Exception as e:
#         if logger:
#             logger.critical(f"Application failed to start critically: {e}", exc_info=True)
#         else:
#             print(f"Application failed to start critically: {e}")
#         sys.exit(1)

# if __name__ == "__main__":
#     main()

#!/usr/bin/env python3
#!/usr/bin/env python3
#!/usr/bin/env python3
#!/usr/bin/env python3
#!/usr/bin/env python3
#!/usr/bin/env python3
#!/usr/bin/env python3
#!/usr/bin/env python3
"""
Main application orchestrator for the real-time analytics pipeline.
Manages service lifecycle, dependency injection, and graceful shutdown.
"""

from dotenv import load_dotenv
load_dotenv()

import asyncio
import signal
import sys
import time
import uuid
from contextlib import asynccontextmanager
from typing import Dict, List, Optional, Any

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse

from enum import Enum

# Import all components (now that dotenv is loaded)
from config.settings import Settings, get_settings, Environment
from utils.logger import get_logger, setup_logging
from kafka_handlers.kafka_producer import KafkaProducerClient
from kafka_handlers.kafka_consumer import AsyncKafkaConsumer, create_consumer # Make sure AsyncKafkaConsumer and create_consumer are imported
from kafka_handlers.topics import TopicManager, TopicType # Make sure TopicType is imported
from alerts.alert_store import AlertStore
from alerts.alert_logic import AlertEngine
from ws.websocket_manager import WebSocketManager # Ensure WebSocketManager is imported
from ws.ws_server import setup_websocket_routes
from api.api_router import setup_api_routes # Ensure setup_api_routes is imported
from api.auth import AuthManager, _auth_manager_instance


# Global components registry
class ServiceRegistry:
    """Global service registry for dependency injection."""
    
    def __init__(self):
        self.services: Dict[str, Any] = {}
        self.startup_order: List[str] = [
            'settings',
            'logger',
            'topic_manager',
            'kafka_producer',
            'alert_store',
            'alert_engine',
            'auth_manager',
            'websocket_manager',
            'kafka_consumer_detection_events' # Specific consumer name, or general if only one
        ]
        # Dynamically adjust shutdown order based on actual startup order
        self.shutdown_order: List[str] = list(reversed(self.startup_order))
    
    def register(self, name: str, service: Any):
        """Register a service."""
        self.services[name] = service
    
    def get(self, name: str) -> Any:
        """Get a service by name."""
        return self.services.get(name)
    
    def get_all(self) -> Dict[str, Any]:
        """Get all registered services."""
        return self.services.copy()

# Global registry instance
registry = ServiceRegistry()

class HealthChecker:
    """Health check manager for all services."""
    
    def __init__(self, registry: ServiceRegistry):
        self.registry = registry
        self.logger = get_logger(__name__)
    
    async def check_service_health(self, service_name: str) -> Dict[str, Any]:
        """Check health of a specific service."""
        service = self.registry.get(service_name)
        if not service:
            return {
                'status': 'unknown',
                'message': f'Service {service_name} not found'
            }
        
        try:
            if hasattr(service, 'health_check'):
                # Handle both async and sync health_check methods
                if asyncio.iscoroutinefunction(service.health_check):
                    health = await service.health_check()
                else:
                    health = service.health_check()
                
                return {
                    'status': 'healthy' if (isinstance(health, dict) and health.get('status') == 'healthy') or health is True else 'unhealthy',
                    'service': service_name,
                    'timestamp': time.time(),
                    'details': health if isinstance(health, dict) else None
                }
            else:
                return {
                    'status': 'healthy',
                    'service': service_name,
                    'message': 'No specific health check method available',
                    'timestamp': time.time()
                }
        except Exception as e:
            self.logger.error(f"Health check failed for {service_name}: {e}", exc_info=True)
            return {
                'status': 'unhealthy',
                'service': service_name,
                'error': str(e),
                'timestamp': time.time()
            }
    
    async def check_all_services(self) -> Dict[str, Any]:
        """Check health of all registered services."""
        results = {}
        overall_status = 'healthy'
        
        services_to_check = registry.startup_order
        
        for service_name in services_to_check:
            if service_name not in self.registry.services:
                results[service_name] = {'status': 'not_initialized', 'message': 'Service not registered yet'}
                overall_status = 'degraded'
                continue

            health = await self.check_service_health(service_name)
            results[service_name] = health
            
            if health['status'] != 'healthy':
                overall_status = 'degraded'
        
        return {
            'overall_status': overall_status,
            'services': results,
            'timestamp': time.time()
        }

class ApplicationLifecycle:
    """Manages application lifecycle events."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.startup_complete = False
        self.shutdown_initiated = False
        self.correlation_id = str(uuid.uuid4())
    
    async def startup_sequence(self):
        """Execute startup sequence for all services."""
        self.logger.info(f"Starting application startup sequence (correlation_id: {self.correlation_id})")
        
        try:
            settings = get_settings()
            registry.register('settings', settings)
            self.logger.info("Settings initialized")
            
            setup_logging(settings.logging)
            logger = get_logger(__name__) 
            registry.register('logger', logger)
            self.logger.info("Logging configured")
            
            # Initialize TopicManager
            topic_manager = TopicManager(settings) 
            registry.register('topic_manager', topic_manager)
            await topic_manager.initialize() 
            self.logger.info("Kafka topic manager initialized")
            
            # Initialize Kafka Producer
            kafka_producer = KafkaProducerClient(settings, topic_manager) 
            await kafka_producer.start()
            registry.register('kafka_producer', kafka_producer)
            self.logger.info("Kafka producer started")
            
            # Initialize Alert Store
            alert_store = AlertStore(settings)
            await alert_store.initialize()
            registry.register('alert_store', alert_store)
            self.logger.info("Alert store initialized")
            
            # Initialize Alert Engine (depends on settings and alert_store)
            alert_engine = AlertEngine(settings, alert_store)
            await alert_engine.initialize()
            registry.register('alert_engine', alert_engine)
            self.logger.info("Alert engine initialized")
            
            # Initialize Auth Manager (depends on settings)
            # Importing here to ensure the global _auth_manager_instance is properly set before other modules might try to access it
            from api.auth import AuthManager, _auth_manager_instance # Corrected import statement to directly reference the global
            global _auth_manager_instance # Declare intent to modify global variable
            _auth_manager_instance = AuthManager(settings)
            await _auth_manager_instance.initialize()
            registry.register('auth_manager', _auth_manager_instance)
            self.logger.info("Auth manager initialized")

            # Initialize WebSocket Manager (depends on settings)
            websocket_manager = WebSocketManager(settings)
            await websocket_manager.startup()
            registry.register('websocket_manager', websocket_manager)
            self.logger.info("WebSocket manager started")
            
            # Initialize Kafka Consumer (depends on settings and alert_engine)
            kafka_consumer_detection_events = create_consumer(
                TopicType.DETECTION_EVENTS,
                settings, 
                alert_engine,
                processor=None 
            )
            
            registry.register('kafka_consumer_detection_events', kafka_consumer_detection_events)
            
            # Start the consumer
            await kafka_consumer_detection_events.start()
            
            self.logger.info("Kafka consumer started") # Log that *a* consumer is started
            
            self.startup_complete = True
            self.logger.info("Application startup completed successfully")
            
        except Exception as e:
            self.logger.critical(f"Startup failed: {e}", exc_info=True)
            await self.shutdown_sequence()
            raise # Re-raise the exception to stop the application
    
    async def shutdown_sequence(self):
        """Execute graceful shutdown sequence."""
        if self.shutdown_initiated:
            return
        
        self.shutdown_initiated = True
        self.logger.info("Initiating graceful shutdown sequence")
        
        # Shut down services in reverse order of their startup
        # We use a copy of startup_order and reverse it to ensure dependencies are shut down last
        for service_name in reversed(registry.startup_order): # Iterate in reverse of startup order
            service = registry.get(service_name)
            if service:
                if hasattr(service, 'shutdown'):
                    try:
                        self.logger.info(f"Shutting down {service_name}")
                        if asyncio.iscoroutinefunction(service.shutdown):
                            await service.shutdown()
                        else:
                            service.shutdown()
                    except Exception as e:
                        self.logger.error(f"Error shutting down {service_name}: {e}", exc_info=True)
                else:
                    self.logger.debug(f"Service {service_name} has no 'shutdown' method.")
            else:
                self.logger.debug(f"Service {service_name} not found in registry during shutdown, skipping.")
        
        self.logger.info("Graceful shutdown completed")
    
    async def health_check(self) -> Dict[str, Any]:
        """Application-level health check."""
        if not self.startup_complete:
            return {
                'status': 'starting',
                'message': 'Application is still starting up',
                'timestamp': time.time()
            }
        
        if self.shutdown_initiated:
            return {
                'status': 'shutting_down',
                'message': 'Application is shutting down',
                'timestamp': time.time()
            }
        
        health_checker = HealthChecker(registry)
        return await health_checker.check_all_services()

# Global lifecycle manager
lifecycle = ApplicationLifecycle()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context manager."""
    try:
        await lifecycle.startup_sequence()
    except Exception:
        sys.exit(1)
    
    def signal_handler(signum, frame):
        asyncio.create_task(lifecycle.shutdown_sequence())
        asyncio.get_event_loop().call_later(2, sys.exit, 0)

    # CRITICAL FIX HERE: Use global get_settings() function, not a method on lifecycle instance
    current_settings = get_settings()
    if not (current_settings.hot_reload_enabled and current_settings.is_development()):
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    else:
        lifecycle.logger.info("Skipping signal handler setup in development/hot-reload mode.")
    
    yield
    
    await lifecycle.shutdown_sequence()

def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()
    
    app = FastAPI(
        title=settings.api.title,
        description=settings.api.description,
        version=settings.api.version,
        lifespan=lifespan,
        docs_url="/docs" if settings.environment != Environment.PRODUCTION else None,
        redoc_url="/redoc" if settings.environment != Environment.PRODUCTION else None,
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.get_allowed_origins(),
        allow_credentials=True,
        allow_methods=settings.get_cors_methods(),
        allow_headers=["*"],
    )
    
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=settings.get_allowed_hosts()
    )
    
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger = get_logger(__name__)
        logger.error(f"Unhandled exception during request: {request.url} - {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )
    
    @app.get("/health", summary="Get overall application health")
    async def get_app_health():
        return await lifecycle.health_check()
    
    @app.get("/health/ready", summary="Readiness probe for deployment")
    async def readiness_check():
        if lifecycle.startup_complete and not lifecycle.shutdown_initiated:
            health_status = await lifecycle.health_check()
            if health_status['overall_status'] == 'healthy':
                return {"status": "ready"}
            raise HTTPException(status_code=503, detail=f"Service not fully healthy: {health_status['overall_status']}")
        raise HTTPException(status_code=503, detail="Service not ready: Startup incomplete or shutdown initiated.")
    
    @app.get("/health/live", summary="Liveness probe for process monitoring")
    async def liveness_check():
        if not lifecycle.shutdown_initiated:
            return {"status": "alive"}
        raise HTTPException(status_code=503, detail="Service not alive: Shutdown initiated.")
    
    setup_api_routes(app, registry)
    setup_websocket_routes(app, registry)
    
    return app

def main():
    """Main entry point."""
    logger = None
    
    try:
        settings = get_settings()
        
        setup_logging(settings.logging)
        logger = get_logger(__name__)
        
        logger.info(f"Starting {settings.app_name} (Version: {settings.app_version})")
        logger.info(f"Environment: {settings.environment}")
        logger.info(f"API Host: {settings.api.host}:{settings.api.port}")
        logger.info(f"WebSocket Host: {settings.websocket.host}:{settings.websocket.port}")
        logger.info(f"Redis Host: {settings.redis.host}:{settings.redis.port} (Enabled: {settings.redis.enabled})")
        logger.info(f"Auth DB URL: {settings.api.database_url}")
        logger.info(f"WS Persistence Enabled: {settings.websocket.enable_persistence}")

        app = create_app()
        
        uvicorn_config = {
            "app": app,
            "host": settings.api.host,
            "port": settings.api.port,
            "log_level": (settings.logging.level.value if isinstance(settings.logging.level, Enum)
                         else str(settings.logging.level)).lower(),
            "access_log": settings.environment != Environment.PRODUCTION,
            "reload": settings.hot_reload_enabled and settings.is_development(),
            "workers": 1,
        }
        
        if not (settings.is_development() and settings.hot_reload_enabled):
            uvicorn_config["workers"] = settings.api.workers

        if settings.is_production():
            uvicorn_config.update({
                "loop": "uvloop",
                "http": "httptools",
            })
        
        uvicorn.run(**uvicorn_config)
        
    except KeyboardInterrupt:
        if logger:
            logger.info("Received keyboard interrupt, initiating graceful shutdown...")
        else:
            print("Received keyboard interrupt, initiating graceful shutdown...")
    except Exception as e:
        if logger:
            logger.critical(f"Application failed to start critically: {e}", exc_info=True)
        else:
            print(f"Application failed to start critically: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()