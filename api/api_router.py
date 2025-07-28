#!/usr/bin/env python3
"""
API Router module for the real-time analytics pipeline.
Provides REST API endpoints for alerts, filters, and configurations.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

from fastapi import (
    APIRouter, BackgroundTasks, Depends, FastAPI, HTTPException, 
    Query, Request, Response, status
)
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from utils.logger import get_logger

# Configure logging
logger = get_logger(__name__)

# Rate limiting setup
limiter = Limiter(key_func=get_remote_address)

# Security
security = HTTPBearer()

# API Versioning
API_V1_PREFIX = "/api/v1"

# ====================
# Pydantic Models
# ====================

class AlertSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AlertStatus(str, Enum):
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"

class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"

# Base Models
class BaseResponse(BaseModel):
    success: bool = True
    message: str = "Operation completed successfully"
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class PaginationParams(BaseModel):
    page: int = Field(1, ge=1, description="Page number")
    limit: int = Field(10, ge=1, le=100, description="Items per page")
    sort_by: str = Field("created_at", description="Sort field")
    sort_order: SortOrder = Field(SortOrder.DESC, description="Sort order")

class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    limit: int
    pages: int
    has_next: bool
    has_prev: bool

# Alert Models
class AlertBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    severity: AlertSeverity
    threshold: float = Field(..., description="Alert threshold value")
    metric_name: str = Field(..., min_length=1, max_length=255)
    condition: str = Field(..., pattern=r'^(>|<|>=|<=|==|!=)$')
    enabled: bool = True
    tags: List[str] = Field(default_factory=list)

class AlertCreate(AlertBase):
    pass

class AlertUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    severity: Optional[AlertSeverity] = None
    threshold: Optional[float] = None
    metric_name: Optional[str] = Field(None, min_length=1, max_length=255)
    condition: Optional[str] = Field(None, pattern=r'^(>|<|>=|<=|==|!=)$')
    enabled: Optional[bool] = None
    tags: Optional[List[str]] = None

class AlertResponse(AlertBase):
    id: UUID
    status: AlertStatus
    created_at: datetime
    updated_at: datetime
    last_triggered: Optional[datetime] = None
    trigger_count: int = 0

    class Config:
        from_attributes = True

# Bulk Operations
class BulkAlertCreate(BaseModel):
    alerts: List[AlertCreate]

class BulkAlertUpdate(BaseModel):
    alert_ids: List[UUID]
    updates: AlertUpdate

class BulkResponse(BaseModel):
    success_count: int
    failed_count: int
    errors: List[str] = Field(default_factory=list)

# Health Check Models
class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"

class HealthCheck(BaseModel):
    status: HealthStatus
    timestamp: datetime
    version: str
    uptime: float
    checks: Dict[str, Any]

# ====================
# In-Memory Database (for demo purposes)
# ====================

class InMemoryDatabase:
    def __init__(self):
        self.alerts: Dict[UUID, AlertResponse] = {}
        self.start_time = time.time()

    def get_uptime(self) -> float:
        return time.time() - self.start_time

# Global database instance
db = InMemoryDatabase()

# ====================
# Dependencies
# ====================

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Mock authentication - replace with actual authentication logic"""
    try:
        if not credentials or credentials.scheme != "Bearer":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Mock user validation - replace with actual auth logic
        if credentials.credentials == "invalid_token":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        return {"user_id": "mock_user", "username": "test_user"}
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed",
            headers={"WWW-Authenticate": "Bearer"},
        )

def get_pagination_params(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    sort_by: str = Query("created_at", description="Sort field"),
    sort_order: SortOrder = Query(SortOrder.DESC, description="Sort order")
) -> PaginationParams:
    return PaginationParams(
        page=page,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order
    )

# ====================
# Utility Functions
# ====================

def create_paginated_response(
    items: List[Any],
    total: int,
    pagination: PaginationParams
) -> PaginatedResponse:
    pages = (total + pagination.limit - 1) // pagination.limit
    return PaginatedResponse(
        items=items,
        total=total,
        page=pagination.page,
        limit=pagination.limit,
        pages=pages,
        has_next=pagination.page < pages,
        has_prev=pagination.page > 1
    )

def apply_pagination_and_sorting(
    items: List[Any],
    pagination: PaginationParams
) -> List[Any]:
    """Apply sorting and pagination to items"""
    try:
        # Sort items
        if pagination.sort_order == SortOrder.DESC:
            items.sort(key=lambda x: getattr(x, pagination.sort_by, ""), reverse=True)
        else:
            items.sort(key=lambda x: getattr(x, pagination.sort_by, ""))
        
        # Apply pagination
        start = (pagination.page - 1) * pagination.limit
        end = start + pagination.limit
        return items[start:end]
    except Exception as e:
        logger.error(f"Error in pagination/sorting: {str(e)}")
        return items

# ====================
# API Route Handlers
# ====================

def create_alert_routes(registry) -> APIRouter:
    """Create alert-related routes"""
    router = APIRouter(prefix="/alerts", tags=["alerts"])
    
    @router.post("", response_model=AlertResponse)
    @limiter.limit("30/minute")
    async def create_alert(
        request: Request,
        alert: AlertCreate,
        current_user: dict = Depends(get_current_user)
    ):
        """Create a new alert"""
        try:
            alert_id = uuid4()
            alert_response = AlertResponse(
                id=alert_id,
                status=AlertStatus.ACTIVE,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                **alert.dict()
            )
            db.alerts[alert_id] = alert_response
            logger.info(f"Created alert {alert_id} by user {current_user['username']}")
            return alert_response
        except Exception as e:
            logger.error(f"Error creating alert: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create alert"
            )

    @router.get("", response_model=PaginatedResponse)
    @limiter.limit("100/minute")
    async def list_alerts(
        request: Request,
        pagination: PaginationParams = Depends(get_pagination_params),
        severity: Optional[AlertSeverity] = Query(None, description="Filter by severity"),
        status_filter: Optional[AlertStatus] = Query(None, alias="status", description="Filter by status"),
        enabled: Optional[bool] = Query(None, description="Filter by enabled status"),
        current_user: dict = Depends(get_current_user)
    ):
        """List alerts with pagination and filtering"""
        try:
            alerts = list(db.alerts.values())
            
            # Apply filters
            if severity:
                alerts = [a for a in alerts if a.severity == severity]
            if status_filter:
                alerts = [a for a in alerts if a.status == status_filter]
            if enabled is not None:
                alerts = [a for a in alerts if a.enabled == enabled]
            
            total = len(alerts)
            paginated_alerts = apply_pagination_and_sorting(alerts, pagination)
            
            return create_paginated_response(paginated_alerts, total, pagination)
        except Exception as e:
            logger.error(f"Error listing alerts: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to list alerts"
            )

    @router.get("/{alert_id}", response_model=AlertResponse)
    @limiter.limit("200/minute")
    async def get_alert(
        request: Request,
        alert_id: UUID,
        current_user: dict = Depends(get_current_user)
    ):
        """Get alert by ID"""
        if alert_id not in db.alerts:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Alert not found"
            )
        return db.alerts[alert_id]

    @router.put("/{alert_id}", response_model=AlertResponse)
    @limiter.limit("30/minute")
    async def update_alert(
        request: Request,
        alert_id: UUID,
        alert_update: AlertUpdate,
        current_user: dict = Depends(get_current_user)
    ):
        """Update an existing alert"""
        if alert_id not in db.alerts:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Alert not found"
            )
        
        try:
            existing_alert = db.alerts[alert_id]
            update_data = alert_update.dict(exclude_unset=True)
            
            for field, value in update_data.items():
                setattr(existing_alert, field, value)
            
            existing_alert.updated_at = datetime.utcnow()
            logger.info(f"Updated alert {alert_id} by user {current_user['username']}")
            return existing_alert
        except Exception as e:
            logger.error(f"Error updating alert: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update alert"
            )

    @router.delete("/{alert_id}")
    @limiter.limit("20/minute")
    async def delete_alert(
        request: Request,
        alert_id: UUID,
        current_user: dict = Depends(get_current_user)
    ):
        """Delete an alert"""
        if alert_id not in db.alerts:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Alert not found"
            )
        
        try:
            del db.alerts[alert_id]
            logger.info(f"Deleted alert {alert_id} by user {current_user['username']}")
            return {"message": "Alert deleted successfully"}
        except Exception as e:
            logger.error(f"Error deleting alert: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to delete alert"
            )

    @router.post("/bulk", response_model=BulkResponse)
    @limiter.limit("10/minute")
    async def bulk_create_alerts(
        request: Request,
        bulk_data: BulkAlertCreate,
        current_user: dict = Depends(get_current_user)
    ):
        """Create multiple alerts in bulk"""
        try:
            success_count = 0
            errors = []
            
            for i, alert_data in enumerate(bulk_data.alerts):
                try:
                    alert_id = uuid4()
                    alert_response = AlertResponse(
                        id=alert_id,
                        status=AlertStatus.ACTIVE,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                        **alert_data.dict()
                    )
                    db.alerts[alert_id] = alert_response
                    success_count += 1
                except Exception as e:
                    errors.append(f"Alert {i}: {str(e)}")
            
            logger.info(f"Bulk created {success_count} alerts by user {current_user['username']}")
            return BulkResponse(
                success_count=success_count,
                failed_count=len(errors),
                errors=errors
            )
        except Exception as e:
            logger.error(f"Error in bulk create alerts: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to bulk create alerts"
            )

    return router

def create_health_routes(registry) -> APIRouter:
    """Create health check routes"""
    router = APIRouter(prefix="/health", tags=["health"])
    
    @router.get("", response_model=HealthCheck)
    async def health_check(request: Request):
        """Comprehensive health check endpoint"""
        try:
            # Get services from registry
            services = registry.get_all()
            
            # Check service health
            service_checks = {}
            overall_status = HealthStatus.HEALTHY
            
            for service_name, service in services.items():
                try:
                    if hasattr(service, 'health_check'):
                        health = await service.health_check()
                        service_checks[service_name] = "healthy" if health else "unhealthy"
                        if not health:
                            overall_status = HealthStatus.DEGRADED
                    else:
                        service_checks[service_name] = "healthy"
                except Exception as e:
                    service_checks[service_name] = f"unhealthy: {str(e)}"
                    overall_status = HealthStatus.UNHEALTHY
            
            return HealthCheck(
                status=overall_status,
                timestamp=datetime.utcnow(),
                version="1.0.0",
                uptime=db.get_uptime(),
                checks={
                    "services": service_checks,
                    "alerts_count": len(db.alerts),
                    "database": "healthy"
                }
            )
        except Exception as e:
            logger.error(f"Error in health check: {str(e)}")
            return HealthCheck(
                status=HealthStatus.UNHEALTHY,
                timestamp=datetime.utcnow(),
                version="1.0.0",
                uptime=db.get_uptime(),
                checks={"error": str(e)}
            )

    @router.get("/metrics")
    @limiter.limit("100/minute")
    async def get_metrics(request: Request):
        """Get system metrics"""
        try:
            return {
                "system": {
                    "uptime": db.get_uptime()
                },
                "application": {
                    "alerts_count": len(db.alerts),
                    "active_alerts": len([a for a in db.alerts.values() if a.status == AlertStatus.ACTIVE])
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting metrics: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to get metrics"
            )

    return router

# ====================
# Main Setup Function
# ====================

def setup_api_routes(app: FastAPI, registry):
    """Setup all API routes on the FastAPI app"""
    try:
        # Add rate limiting to app
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        
        # Create main API router
        api_router = APIRouter(prefix=API_V1_PREFIX)
        
        # Add all route groups
        alert_routes = create_alert_routes(registry)
        health_routes = create_health_routes(registry)
        
        # Include routes in main router
        api_router.include_router(alert_routes)
        api_router.include_router(health_routes)
        
        # Add the main router to the app
        app.include_router(api_router)
        
        # Add root endpoint
        @app.get("/")
        async def root():
            """Root endpoint with API information"""
            return {
                "message": "Analytics System API",
                "version": "1.0.0",
                "docs": "/docs",
                "redoc": "/redoc",
                "health": f"{API_V1_PREFIX}/health",
                "metrics": f"{API_V1_PREFIX}/health/metrics"
            }
        
        # Add global exception handler
        @app.exception_handler(Exception)
        async def global_exception_handler(request: Request, exc: Exception):
            """Global exception handler"""
            logger.error(f"Unhandled exception: {str(exc)}")
            return JSONResponse(
                status_code=500,
                content={"detail": "Internal server error", "error": str(exc)}
            )
        
        # Add request logging middleware
        @app.middleware("http")
        async def log_requests(request: Request, call_next):
            """Log all requests"""
            start_time = time.time()
            
            # Log request
            logger.info(f"Request: {request.method} {request.url}")
            
            # Process request
            response = await call_next(request)
            
            # Log response
            process_time = time.time() - start_time
            logger.info(f"Response: {response.status_code} ({process_time:.3f}s)")
            
            # Add process time header
            response.headers["X-Process-Time"] = str(process_time)
            
            return response
        
        # Initialize sample data
        sample_alert = AlertResponse(
            id=uuid4(),
            name="Sample Alert",
            description="This is a sample alert for demonstration",
            severity=AlertSeverity.MEDIUM,
            threshold=80.0,
            metric_name="cpu_usage",
            condition=">",
            enabled=True,
            status=AlertStatus.ACTIVE,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            tags=["sample", "demo"]
        )
        db.alerts[sample_alert.id] = sample_alert
        
        logger.info("API routes setup completed successfully")
        
    except Exception as e:
        logger.error(f"Error setting up API routes: {str(e)}")
        raise
# ====================
# API Documentation Examples
# ====================

"""
API Usage Examples:

1. Create Alert:
   POST /api/v1/alerts
   {
     "name": "High CPU Usage",
     "description": "Alert when CPU usage exceeds threshold",
     "severity": "high",
     "threshold": 85.0,
     "metric_name": "cpu_usage",
     "condition": ">",
     "enabled": true,
     "tags": ["cpu", "performance"]
   }

2. List Alerts with Filtering:
   GET /api/v1/alerts?page=1&limit=10&severity=high&enabled=true

3. Bulk Create Alerts:
   POST /api/v1/alerts/bulk
   {
     "alerts": [
       {
         "name": "Alert 1",
         "severity": "medium",
         "threshold": 70.0,
         "metric_name": "memory_usage",
         "condition": ">"
       },
       {
         "name": "Alert 2",
         "severity": "high",
         "threshold": 90.0,
         "metric_name": "disk_usage",
         "condition": ">"
       }
     ]
   }

4. Health Check:
   GET /api/v1/health

5. File Upload:
   POST /api/v1/files/upload
   Content-Type: multipart/form-data
   file: [file data]
   description: "Configuration file"

6. Export Alerts:
   GET /api/v1/export/alerts?format=json
   GET /api/v1/export/alerts?format=csv

Rate Limiting:
- Most endpoints: 100 requests/minute
- Create/Update operations: 30 requests/minute
- Bulk operations: 10 requests/minute
- File operations: 10 requests/minute

Authentication:
- All endpoints require Bearer token authentication
- Include Authorization header: "Bearer <token>"

Error Responses:
- 400: Bad Request - Invalid input data
- 401: Unauthorized - Missing or invalid authentication
- 403: Forbidden - Insufficient permissions
- 404: Not Found - Resource not found
- 422: Validation Error - Invalid request data
- 429: Too Many Requests - Rate limit exceeded
- 500: Internal Server Error - Server error

Response Format:
- All responses include proper HTTP status codes
- Error responses include detailed error messages
- Paginated responses include metadata (total, pages, etc.)
- Timestamps are in ISO 8601 format
"""