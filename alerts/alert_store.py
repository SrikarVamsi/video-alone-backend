"""
Alert Storage System with Redis Caching and Circuit Breaker Pattern

This module provides:
- In-memory alert storage with Redis backup
- Circuit breaker pattern for Redis operations
- Alert filtering and pagination
- Alert aggregation and analytics
- TTL-based alert expiration
- Distributed caching across multiple instances
- Metrics collection for storage operations
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
from enum import Enum
import redis.asyncio as redis
from redis.exceptions import ConnectionError, TimeoutError
import logging
from contextlib import asynccontextmanager

from utils.logger import get_logger
from config.settings import get_settings
from alerts.alert_schema import Alert, AlertType, AlertSeverity


class StorageState(Enum):
    """Storage system state for circuit breaker pattern"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"


@dataclass
class AlertFilter:
    """Alert filtering parameters"""
    camera_id: Optional[str] = None
    alert_type: Optional[AlertType] = None
    severity: Optional[AlertSeverity] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = 100
    offset: int = 0


@dataclass
class AlertStats:
    """Alert statistics and metrics"""
    total_alerts: int
    alerts_by_type: Dict[str, int]
    alerts_by_severity: Dict[str, int]
    alerts_by_camera: Dict[str, int]
    recent_alerts_count: int
    avg_response_time: float


class CircuitBreaker:
    """Circuit breaker for Redis operations"""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = StorageState.HEALTHY
        
    def is_open(self) -> bool:
        """Check if circuit breaker is open"""
        if self.state == StorageState.FAILED:
            if time.time() - self.last_failure_time > self.timeout:
                self.state = StorageState.DEGRADED
                return False
            return True
        return False
    
    def record_success(self):
        """Record successful operation"""
        self.failure_count = 0
        self.state = StorageState.HEALTHY
    
    def record_failure(self):
        """Record failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = StorageState.FAILED


class AlertStore:
    """
    Advanced alert storage system with Redis caching and circuit breaker pattern
    """
    
    def __init__(self, redis_url: str = None, max_memory_alerts: int = 10000):
        self.settings = get_settings()
        self.logger = get_logger(__name__)
        
        # In-memory storage (primary)
        self.alerts: deque = deque(maxlen=max_memory_alerts)
        self.alert_index: Dict[str, Alert] = {}  # Fast lookup by ID
        self.camera_alerts: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.type_alerts: Dict[AlertType, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # Redis configuration
        self.redis_url = redis_url or self.settings.redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.circuit_breaker = CircuitBreaker()
        
        # Metrics
        self.metrics = {
            'total_stored': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'redis_errors': 0,
            'operation_times': deque(maxlen=1000)
        }
        
        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._sync_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize the alert store"""
        try:
            self.logger.info("Initializing Alert Store...")
            
            # Initialize Redis connection
            if self.redis_url:
                await self._init_redis()
            
            # Start background tasks
            self._cleanup_task = asyncio.create_task(self._cleanup_expired_alerts())
            self._sync_task = asyncio.create_task(self._sync_with_redis())
            
            self.logger.info("Alert Store initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Alert Store: {e}")
            raise
    
    async def _init_redis(self):
        """Initialize Redis connection with circuit breaker"""
        try:
            self.redis_client = redis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            # Test connection
            await self.redis_client.ping()
            self.circuit_breaker.record_success()
            self.logger.info("Redis connection established")
            
        except Exception as e:
            self.circuit_breaker.record_failure()
            self.logger.warning(f"Redis connection failed: {e}")
            self.redis_client = None
    
    async def store_alert(self, alert: Alert) -> bool:
        """
        Store an alert in memory and Redis
        
        Args:
            alert: Alert object to store
            
        Returns:
            bool: True if stored successfully
        """
        start_time = time.time()
        
        try:
            # Store in memory (primary storage)
            self.alerts.append(alert)
            self.alert_index[alert.id] = alert
            self.camera_alerts[alert.camera_id].append(alert)
            self.type_alerts[alert.alert_type].append(alert)
            
            # Store in Redis (backup/cache)
            await self._store_in_redis(alert)
            
            # Update metrics
            self.metrics['total_stored'] += 1
            self.metrics['operation_times'].append(time.time() - start_time)
            
            self.logger.debug(f"Alert stored: {alert.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store alert {alert.id}: {e}")
            return False
    
    async def _store_in_redis(self, alert: Alert):
        """Store alert in Redis with circuit breaker protection"""
        if not self.redis_client or self.circuit_breaker.is_open():
            return
        
        try:
            # Store alert data
            alert_data = json.dumps(asdict(alert), default=str)
            await self.redis_client.setex(
                f"alert:{alert.id}",
                3600,  # 1 hour TTL
                alert_data
            )
            
            # Add to sorted sets for querying
            timestamp = alert.timestamp.timestamp()
            await self.redis_client.zadd(
                "alerts:by_time",
                {alert.id: timestamp}
            )
            await self.redis_client.zadd(
                f"alerts:camera:{alert.camera_id}",
                {alert.id: timestamp}
            )
            await self.redis_client.zadd(
                f"alerts:type:{alert.alert_type.value}",
                {alert.id: timestamp}
            )
            
            self.circuit_breaker.record_success()
            
        except (ConnectionError, TimeoutError) as e:
            self.circuit_breaker.record_failure()
            self.metrics['redis_errors'] += 1
            self.logger.warning(f"Redis operation failed: {e}")
    
    async def get_alert(self, alert_id: str) -> Optional[Alert]:
        """
        Retrieve alert by ID
        
        Args:
            alert_id: Alert identifier
            
        Returns:
            Alert object or None if not found
        """
        start_time = time.time()
        
        try:
            # Check memory first
            if alert_id in self.alert_index:
                self.metrics['cache_hits'] += 1
                return self.alert_index[alert_id]
            
            # Check Redis
            alert = await self._get_from_redis(alert_id)
            if alert:
                self.metrics['cache_hits'] += 1
                return alert
            
            self.metrics['cache_misses'] += 1
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get alert {alert_id}: {e}")
            return None
        finally:
            self.metrics['operation_times'].append(time.time() - start_time)
    
    async def _get_from_redis(self, alert_id: str) -> Optional[Alert]:
        """Get alert from Redis with circuit breaker protection"""
        if not self.redis_client or self.circuit_breaker.is_open():
            return None
        
        try:
            alert_data = await self.redis_client.get(f"alert:{alert_id}")
            if alert_data:
                data = json.loads(alert_data)
                return Alert(**data)
            return None
            
        except Exception as e:
            self.circuit_breaker.record_failure()
            self.logger.warning(f"Redis get failed: {e}")
            return None
    
    async def get_alerts(self, filter_params: AlertFilter) -> Tuple[List[Alert], int]:
        """
        Get filtered alerts with pagination
        
        Args:
            filter_params: Filtering parameters
            
        Returns:
            Tuple of (alerts list, total count)
        """
        try:
            # Filter alerts from memory
            filtered_alerts = []
            
            for alert in reversed(self.alerts):  # Most recent first
                if self._matches_filter(alert, filter_params):
                    filtered_alerts.append(alert)
            
            # Apply pagination
            total_count = len(filtered_alerts)
            start_idx = filter_params.offset
            end_idx = start_idx + filter_params.limit
            
            paginated_alerts = filtered_alerts[start_idx:end_idx]
            
            self.logger.debug(f"Retrieved {len(paginated_alerts)} alerts (total: {total_count})")
            return paginated_alerts, total_count
            
        except Exception as e:
            self.logger.error(f"Failed to get alerts: {e}")
            return [], 0
    
    def _matches_filter(self, alert: Alert, filter_params: AlertFilter) -> bool:
        """Check if alert matches filter criteria"""
        if filter_params.camera_id and alert.camera_id != filter_params.camera_id:
            return False
        
        if filter_params.alert_type and alert.alert_type != filter_params.alert_type:
            return False
        
        if filter_params.severity and alert.severity != filter_params.severity:
            return False
        
        if filter_params.start_time and alert.timestamp < filter_params.start_time:
            return False
        
        if filter_params.end_time and alert.timestamp > filter_params.end_time:
            return False
        
        return True
    
    async def get_alert_stats(self, hours: int = 24) -> AlertStats:
        """
        Get alert statistics for the specified time period
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            AlertStats object with metrics
        """
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            recent_alerts = [
                alert for alert in self.alerts 
                if alert.timestamp >= cutoff_time
            ]
            
            # Calculate statistics
            alerts_by_type = defaultdict(int)
            alerts_by_severity = defaultdict(int)
            alerts_by_camera = defaultdict(int)
            
            for alert in recent_alerts:
                alerts_by_type[alert.alert_type.value] += 1
                alerts_by_severity[alert.severity.value] += 1
                alerts_by_camera[alert.camera_id] += 1
            
            # Calculate average response time
            avg_response_time = 0
            if self.metrics['operation_times']:
                avg_response_time = sum(self.metrics['operation_times']) / len(self.metrics['operation_times'])
            
            return AlertStats(
                total_alerts=len(self.alerts),
                alerts_by_type=dict(alerts_by_type),
                alerts_by_severity=dict(alerts_by_severity),
                alerts_by_camera=dict(alerts_by_camera),
                recent_alerts_count=len(recent_alerts),
                avg_response_time=avg_response_time
            )
            
        except Exception as e:
            self.logger.error(f"Failed to get alert stats: {e}")
            return AlertStats(0, {}, {}, {}, 0, 0)
    
    async def delete_alert(self, alert_id: str) -> bool:
        """
        Delete an alert from storage
        
        Args:
            alert_id: Alert identifier
            
        Returns:
            bool: True if deleted successfully
        """
        try:
            # Remove from memory
            if alert_id in self.alert_index:
                alert = self.alert_index.pop(alert_id)
                
                # Remove from collections (expensive but necessary)
                if alert in self.alerts:
                    self.alerts.remove(alert)
                if alert in self.camera_alerts[alert.camera_id]:
                    self.camera_alerts[alert.camera_id].remove(alert)
                if alert in self.type_alerts[alert.alert_type]:
                    self.type_alerts[alert.alert_type].remove(alert)
            
            # Remove from Redis
            await self._delete_from_redis(alert_id)
            
            self.logger.debug(f"Alert deleted: {alert_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete alert {alert_id}: {e}")
            return False
    
    async def _delete_from_redis(self, alert_id: str):
        """Delete alert from Redis"""
        if not self.redis_client or self.circuit_breaker.is_open():
            return
        
        try:
            # Get alert data first to clean up sorted sets
            alert_data = await self.redis_client.get(f"alert:{alert_id}")
            if alert_data:
                data = json.loads(alert_data)
                
                # Remove from sorted sets
                await self.redis_client.zrem("alerts:by_time", alert_id)
                await self.redis_client.zrem(f"alerts:camera:{data['camera_id']}", alert_id)
                await self.redis_client.zrem(f"alerts:type:{data['alert_type']}", alert_id)
            
            # Remove main alert data
            await self.redis_client.delete(f"alert:{alert_id}")
            
        except Exception as e:
            self.logger.warning(f"Redis delete failed: {e}")
    
    async def _cleanup_expired_alerts(self):
        """Background task to cleanup expired alerts"""
        while True:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes
                
                cutoff_time = datetime.now() - timedelta(hours=24)
                expired_alerts = [
                    alert for alert in self.alerts 
                    if alert.timestamp < cutoff_time
                ]
                
                for alert in expired_alerts:
                    await self.delete_alert(alert.id)
                
                if expired_alerts:
                    self.logger.info(f"Cleaned up {len(expired_alerts)} expired alerts")
                    
            except Exception as e:
                self.logger.error(f"Cleanup task error: {e}")
                await asyncio.sleep(60)  # Wait before retrying
    
    async def _sync_with_redis(self):
        """Background task to sync with Redis"""
        while True:
            try:
                await asyncio.sleep(60)  # Run every minute
                
                if self.redis_client and not self.circuit_breaker.is_open():
                    # Sync recent alerts to Redis
                    recent_alerts = [
                        alert for alert in self.alerts 
                        if alert.timestamp >= datetime.now() - timedelta(minutes=5)
                    ]
                    
                    for alert in recent_alerts:
                        await self._store_in_redis(alert)
                    
            except Exception as e:
                self.logger.error(f"Redis sync task error: {e}")
                await asyncio.sleep(60)
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get storage system health status"""
        return {
            'memory_alerts_count': len(self.alerts),
            'redis_connected': self.redis_client is not None,
            'circuit_breaker_state': self.circuit_breaker.state.value,
            'metrics': self.metrics.copy(),
            'redis_errors': self.metrics['redis_errors']
        }
    
    async def close(self):
        """Cleanup resources"""
        try:
            # Cancel background tasks
            if self._cleanup_task:
                self._cleanup_task.cancel()
            if self._sync_task:
                self._sync_task.cancel()
            
            # Close Redis connection
            if self.redis_client:
                await self.redis_client.close()
            
            self.logger.info("Alert Store closed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


# Global alert store instance
_alert_store: Optional[AlertStore] = None


async def get_alert_store() -> AlertStore:
    """Get or create global alert store instance"""
    global _alert_store
    
    if _alert_store is None:
        _alert_store = AlertStore()
        await _alert_store.initialize()
    
    return _alert_store


@asynccontextmanager
async def alert_store_context():
    """Context manager for alert store"""
    store = await get_alert_store()
    try:
        yield store
    finally:
        pass  # Keep store alive for application lifecycle


# Additional utility functions for alert store operations

async def create_alert_indices():
    """Create database indices for better query performance"""
    store = await get_alert_store()
    
    if store.redis_client:
        try:
            # Create indices for common query patterns
            await store.redis_client.execute_command(
                "FT.CREATE", "alerts_idx", "ON", "HASH", "PREFIX", "1", "alert:",
                "SCHEMA", "camera_id", "TAG", "alert_type", "TAG", "severity", "TAG",
                "timestamp", "NUMERIC", "SORTABLE"
            )
            store.logger.info("Redis search indices created")
        except Exception as e:
            store.logger.warning(f"Could not create search indices: {e}")


async def bulk_insert_alerts(alerts: List[Alert]) -> int:
    """
    Bulk insert multiple alerts efficiently
    
    Args:
        alerts: List of Alert objects to insert
        
    Returns:
        Number of successfully inserted alerts
    """
    store = await get_alert_store()
    success_count = 0
    
    for alert in alerts:
        if await store.store_alert(alert):
            success_count += 1
    
    return success_count


async def get_alert_trends(days: int = 7) -> Dict[str, Any]:
    """
    Get alert trends over time
    
    Args:
        days: Number of days to analyze
        
    Returns:
        Dictionary with trend data
    """
    store = await get_alert_store()
    
    # Create daily buckets
    daily_counts = defaultdict(int)
    daily_types = defaultdict(lambda: defaultdict(int))
    
    cutoff_time = datetime.now() - timedelta(days=days)
    
    for alert in store.alerts:
        if alert.timestamp >= cutoff_time:
            day_key = alert.timestamp.strftime('%Y-%m-%d')
            daily_counts[day_key] += 1
            daily_types[day_key][alert.alert_type.value] += 1
    
    return {
        'daily_counts': dict(daily_counts),
        'daily_types': dict(daily_types)
    }


async def cleanup_old_alerts(days: int = 30) -> int:
    """
    Clean up alerts older than specified days
    
    Args:
        days: Number of days to keep alerts
        
    Returns:
        Number of alerts cleaned up
    """
    store = await get_alert_store()
    cutoff_time = datetime.now() - timedelta(days=days)
    
    old_alerts = [
        alert for alert in store.alerts 
        if alert.timestamp < cutoff_time
    ]
    
    cleanup_count = 0
    for alert in old_alerts:
        if await store.delete_alert(alert.id):
            cleanup_count += 1
    
    return cleanup_count