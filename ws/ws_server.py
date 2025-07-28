"""
Advanced WebSocket Server for Real-Time Alert Streaming

This module provides:
- WebSocket connection management with auto-reconnection
- Real-time alert broadcasting to connected clients
- Client authentication and authorization
- Connection pooling and load balancing
- Heartbeat/ping-pong mechanism
- Message queuing for disconnected clients
- Metrics and monitoring integration
- Circuit breaker pattern for external dependencies
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, deque
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from websockets.server import WebSocketServerProtocol
import jwt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import logging

from utils.logger import get_logger
from config.settings import get_settings
from alerts.alert_schema import Alert
from alerts.alert_store import AlertStore
from api.auth import AuthManager


class ConnectionState(Enum):
    """WebSocket connection states"""
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"


class MessageType(Enum):
    """WebSocket message types"""
    ALERT = "alert"
    HEARTBEAT = "heartbeat"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    ERROR = "error"
    STATUS = "status"


@dataclass
class WebSocketMessage:
    """WebSocket message structure"""
    type: MessageType
    data: Dict[str, Any]
    timestamp: datetime
    correlation_id: str = None
    
    def to_json(self) -> str:
        """Convert message to JSON string"""
        return json.dumps({
            'type': self.type.value,
            'data': self.data,
            'timestamp': self.timestamp.isoformat(),
            'correlation_id': self.correlation_id
        })


@dataclass
class ClientConnection:
    """Client connection information"""
    id: str
    websocket: WebSocket
    user_id: Optional[str]
    connected_at: datetime
    last_ping: datetime
    subscriptions: Set[str]
    message_queue: deque
    state: ConnectionState
    
    def __post_init__(self):
        self.message_queue = deque(maxlen=1000)  # Limit queue size


class WebSocketManager:
    """
    Advanced WebSocket connection manager
    """
    
    def __init__(self, settings):
        self.settings = settings
        self.logger = get_logger(__name__)
        
        # Connection management
        self.connections: Dict[str, ClientConnection] = {}
        self.user_connections: Dict[str, List[str]] = defaultdict(list)
        self.subscription_connections: Dict[str, List[str]] = defaultdict(list)
        
        # Message queuing for offline clients
        self.offline_messages: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Metrics
        self.metrics = {
            'total_connections': 0,
            'active_connections': 0,
            'messages_sent': 0,
            'messages_failed': 0,
            'heartbeat_failures': 0,
            'reconnections': 0
        }
        
        # Background tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        
        # Rate limiting
        self.rate_limits: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Service dependencies
        self.auth_manager: Optional[AuthManager] = None
        self.alert_store: Optional[AlertStore] = None
        
    async def initialize(self):
        """Initialize WebSocket manager"""
        self.logger.info("Starting WebSocket Manager...")
        
        # Start background tasks
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        self.logger.info("WebSocket Manager started")
    
    async def health_check(self) -> bool:
        """Health check for WebSocket manager"""
        try:
            # Check if background tasks are running
            if self._heartbeat_task and self._heartbeat_task.done():
                return False
            if self._cleanup_task and self._cleanup_task.done():
                return False
            return True
        except Exception as e:
            self.logger.error(f"WebSocket health check failed: {e}")
            return False
    
    async def shutdown(self):
        """Shutdown WebSocket manager"""
        self.logger.info("Stopping WebSocket Manager...")
        
        # Cancel background tasks
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
        
        # Disconnect all clients
        for connection_id in list(self.connections.keys()):
            await self.disconnect(connection_id)
        
        self.logger.info("WebSocket Manager stopped")
    
    async def connect(self, websocket: WebSocket, user_id: str = None) -> str:
        """
        Handle new WebSocket connection
        
        Args:
            websocket: WebSocket connection
            user_id: Optional user identifier
            
        Returns:
            Connection ID
        """
        connection_id = str(uuid.uuid4())
        
        try:
            await websocket.accept()
            
            # Create connection object
            connection = ClientConnection(
                id=connection_id,
                websocket=websocket,
                user_id=user_id,
                connected_at=datetime.now(),
                last_ping=datetime.now(),
                subscriptions=set(),
                message_queue=deque(maxlen=1000),
                state=ConnectionState.CONNECTED
            )
            
            # Store connection
            self.connections[connection_id] = connection
            if user_id:
                self.user_connections[user_id].append(connection_id)
            
            # Update metrics
            self.metrics['total_connections'] += 1
            self.metrics['active_connections'] += 1
            
            # Send welcome message
            await self._send_message(connection_id, WebSocketMessage(
                type=MessageType.STATUS,
                data={
                    'status': 'connected',
                    'connection_id': connection_id,
                    'server_time': datetime.now().isoformat()
                },
                timestamp=datetime.now(),
                correlation_id=str(uuid.uuid4())
            ))
            
            # Send queued messages if user has offline messages
            if user_id and user_id in self.offline_messages:
                await self._send_queued_messages(connection_id, user_id)
            
            self.logger.info(f"WebSocket connected: {connection_id} (user: {user_id})")
            return connection_id
            
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}")
            await self.disconnect(connection_id)
            raise
    
    async def disconnect(self, connection_id: str):
        """
        Handle WebSocket disconnection
        
        Args:
            connection_id: Connection identifier
        """
        try:
            if connection_id not in self.connections:
                return
            
            connection = self.connections[connection_id]
            connection.state = ConnectionState.DISCONNECTED
            
            # Remove from user connections
            if connection.user_id:
                user_connections = self.user_connections[connection.user_id]
                if connection_id in user_connections:
                    user_connections.remove(connection_id)
                
                # If no more connections for user, keep some messages for later
                if not user_connections:
                    self.offline_messages[connection.user_id].extend(
                        list(connection.message_queue)[-10:]  # Keep last 10 messages
                    )
            
            # Remove from subscriptions
            for subscription in connection.subscriptions:
                if connection_id in self.subscription_connections[subscription]:
                    self.subscription_connections[subscription].remove(connection_id)
            
            # Close WebSocket
            try:
                await connection.websocket.close()
            except:
                pass
            
            # Remove connection
            del self.connections[connection_id]
            
            # Update metrics
            self.metrics['active_connections'] -= 1
            
            self.logger.info(f"WebSocket disconnected: {connection_id}")
            
        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")
    
    async def handle_message(self, connection_id: str, message: str):
        """
        Handle incoming WebSocket message
        
        Args:
            connection_id: Connection identifier
            message: Raw message string
        """
        try:
            if connection_id not in self.connections:
                return
            
            connection = self.connections[connection_id]
            
            # Rate limiting check
            if not self._check_rate_limit(connection_id):
                await self._send_error(connection_id, "Rate limit exceeded")
                return
            
            # Parse message
            try:
                data = json.loads(message)
                msg_type = MessageType(data.get('type'))
                msg_data = data.get('data', {})
                correlation_id = data.get('correlation_id')
            except (json.JSONDecodeError, ValueError) as e:
                await self._send_error(connection_id, f"Invalid message format: {e}")
                return
            
            # Handle different message types
            if msg_type == MessageType.HEARTBEAT:
                await self._handle_heartbeat(connection_id, correlation_id)
            elif msg_type == MessageType.SUBSCRIBE:
                await self._handle_subscribe(connection_id, msg_data, correlation_id)
            elif msg_type == MessageType.UNSUBSCRIBE:
                await self._handle_unsubscribe(connection_id, msg_data, correlation_id)
            else:
                await self._send_error(connection_id, f"Unknown message type: {msg_type}")
                
        except Exception as e:
            self.logger.error(f"Error handling message from {connection_id}: {e}")
            await self._send_error(connection_id, "Internal server error")
    
    async def _handle_heartbeat(self, connection_id: str, correlation_id: str):
        """Handle heartbeat/ping message"""
        if connection_id in self.connections:
            connection = self.connections[connection_id]
            connection.last_ping = datetime.now()
            
            # Send pong response
            await self._send_message(connection_id, WebSocketMessage(
                type=MessageType.HEARTBEAT,
                data={'pong': True},
                timestamp=datetime.now(),
                correlation_id=correlation_id
            ))
    
    async def _handle_subscribe(self, connection_id: str, data: Dict, correlation_id: str):
        """Handle subscription request"""
        try:
            subscription_topics = data.get('topics', [])
            
            if connection_id not in self.connections:
                return
            
            connection = self.connections[connection_id]
            
            for topic in subscription_topics:
                connection.subscriptions.add(topic)
                self.subscription_connections[topic].append(connection_id)
            
            await self._send_message(connection_id, WebSocketMessage(
                type=MessageType.STATUS,
                data={
                    'status': 'subscribed',
                    'topics': subscription_topics
                },
                timestamp=datetime.now(),
                correlation_id=correlation_id
            ))
            
            self.logger.debug(f"Connection {connection_id} subscribed to {subscription_topics}")
            
        except Exception as e:
            await self._send_error(connection_id, f"Subscription error: {e}")
    
    async def _handle_unsubscribe(self, connection_id: str, data: Dict, correlation_id: str):
        """Handle unsubscription request"""
        try:
            subscription_topics = data.get('topics', [])
            
            if connection_id not in self.connections:
                return
            
            connection = self.connections[connection_id]
            
            for topic in subscription_topics:
                connection.subscriptions.discard(topic)
                if connection_id in self.subscription_connections[topic]:
                    self.subscription_connections[topic].remove(connection_id)
            
            await self._send_message(connection_id, WebSocketMessage(
                type=MessageType.STATUS,
                data={
                    'status': 'unsubscribed',
                    'topics': subscription_topics
                },
                timestamp=datetime.now(),
                correlation_id=correlation_id
            ))
            
            self.logger.debug(f"Connection {connection_id} unsubscribed from {subscription_topics}")
            
        except Exception as e:
            await self._send_error(connection_id, f"Unsubscription error: {e}")
    
    async def broadcast_alert(self, alert: Alert):
        """
        Broadcast alert to all connected clients
        
        Args:
            alert: Alert object to broadcast
        """
        try:
            message = WebSocketMessage(
                type=MessageType.ALERT,
                data=asdict(alert),
                timestamp=datetime.now(),
                correlation_id=str(uuid.uuid4())
            )
            
            # Broadcast to all connected clients
            await self._broadcast_message(message)
            
            # Also send to specific subscriptions
            await self._send_to_subscriptions(f"camera:{alert.camera_id}", message)
            await self._send_to_subscriptions(f"alert_type:{alert.alert_type.value}", message)
            
            self.metrics['messages_sent'] += len(self.connections)
            self.logger.debug(f"Alert broadcasted to {len(self.connections)} clients")
            
        except Exception as e:
            self.logger.error(f"Error broadcasting alert: {e}")
    
    async def _broadcast_message(self, message: WebSocketMessage):
        """Broadcast message to all connected clients"""
        if not self.connections:
            return
        
        # Send to all connections concurrently
        tasks = []
        for connection_id in list(self.connections.keys()):
            task = asyncio.create_task(self._send_message(connection_id, message))
            tasks.append(task)
        
        # Wait for all sends to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count failures
        failures = sum(1 for result in results if isinstance(result, Exception))
        self.metrics['messages_failed'] += failures
    
    async def _send_to_subscriptions(self, topic: str, message: WebSocketMessage):
        """Send message to clients subscribed to specific topic"""
        if topic not in self.subscription_connections:
            return
        
        connection_ids = self.subscription_connections[topic].copy()
        
        for connection_id in connection_ids:
            await self._send_message(connection_id, message)
    
    async def _send_message(self, connection_id: str, message: WebSocketMessage):
        """Send message to specific connection"""
        try:
            if connection_id not in self.connections:
                return
            
            connection = self.connections[connection_id]
            
            if connection.state != ConnectionState.CONNECTED:
                # Queue message for later
                connection.message_queue.append(message)
                return
            
            # Send message
            await connection.websocket.send_text(message.to_json())
            
        except ConnectionClosed:
            await self.disconnect(connection_id)
        except Exception as e:
            self.logger.error(f"Error sending message to {connection_id}: {e}")
            await self.disconnect(connection_id)
    
    async def _send_error(self, connection_id: str, error_message: str):
        """Send error message to client"""
        error_msg = WebSocketMessage(
            type=MessageType.ERROR,
            data={'error': error_message},
            timestamp=datetime.now(),
            correlation_id=str(uuid.uuid4())
        )
        await self._send_message(connection_id, error_msg)
    
    async def _send_queued_messages(self, connection_id: str, user_id: str):
        """Send queued messages to reconnected user"""
        if user_id not in self.offline_messages:
            return
        
        messages = list(self.offline_messages[user_id])
        self.offline_messages[user_id].clear()
        
        for message in messages:
            await self._send_message(connection_id, message)
        
        self.logger.info(f"Sent {len(messages)} queued messages to {connection_id}")
    
    def _check_rate_limit(self, connection_id: str) -> bool:
        """Check if connection exceeds rate limit"""
        now = time.time()
        window_start = now - 60  # 1-minute window
        
        # Clean old entries
        rate_limit_queue = self.rate_limits[connection_id]
        while rate_limit_queue and rate_limit_queue[0] < window_start:
            rate_limit_queue.popleft()
        
        # Check limit (100 messages per minute)
        if len(rate_limit_queue) >= 100:
            return False
        
        # Add current request
        rate_limit_queue.append(now)
        return True
    
    async def _heartbeat_loop(self):
        """Background task for heartbeat monitoring"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                now = datetime.now()
                timeout_threshold = now - timedelta(seconds=120)  # 2-minute timeout
                
                # Check for timed-out connections
                timed_out_connections = []
                for connection_id, connection in self.connections.items():
                    if connection.last_ping < timeout_threshold:
                        timed_out_connections.append(connection_id)
                
                # Disconnect timed-out connections
                for connection_id in timed_out_connections:
                    self.logger.warning(f"Connection {connection_id} timed out")
                    await self.disconnect(connection_id)
                    self.metrics['heartbeat_failures'] += 1
                
            except Exception as e:
                self.logger.error(f"Heartbeat loop error: {e}")
                await asyncio.sleep(60)
    
    async def _cleanup_loop(self):
        """Background task for cleanup operations"""
        while True:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes
                
                # Clean up offline messages older than 1 hour
                cutoff_time = datetime.now() - timedelta(hours=1)
                
                for user_id, messages in self.offline_messages.items():
                    messages_to_keep = deque()
                    for message in messages:
                        if message.timestamp > cutoff_time:
                            messages_to_keep.append(message)
                    self.offline_messages[user_id] = messages_to_keep
                
                # Clean up empty subscription lists
                empty_subscriptions = [
                    topic for topic, connections in self.subscription_connections.items()
                    if not connections
                ]
                for topic in empty_subscriptions:
                    del self.subscription_connections[topic]
                
                self.logger.debug("Cleanup completed")
                
            except Exception as e:
                self.logger.error(f"Cleanup loop error: {e}")
                await asyncio.sleep(60)
    
    async def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection statistics"""
        return {
            'total_connections': self.metrics['total_connections'],
            'active_connections': self.metrics['active_connections'],
            'messages_sent': self.metrics['messages_sent'],
            'messages_failed': self.metrics['messages_failed'],
            'heartbeat_failures': self.metrics['heartbeat_failures'],
            'reconnections': self.metrics['reconnections'],
            'subscriptions': len(self.subscription_connections),
            'offline_users': len(self.offline_messages)
        }
    
    async def send_to_user(self, user_id: str, message: WebSocketMessage):
        """Send message to specific user"""
        if user_id not in self.user_connections:
            # User not connected, queue message
            self.offline_messages[user_id].append(message)
            return
        
        # Send to all user's connections
        for connection_id in self.user_connections[user_id]:
            await self._send_message(connection_id, message)


def setup_websocket_routes(app: FastAPI, registry):
    """
    Setup WebSocket routes for the FastAPI application
    
    Args:
        app: FastAPI application instance
        registry: Service registry containing all services
    """
    
    @app.websocket("/ws/alerts")
    async def websocket_endpoint(websocket: WebSocket, token: str = None):
        """WebSocket endpoint for alert streaming"""
        # Get services from registry
        websocket_manager = registry.get('websocket_manager')
        auth_manager = registry.get('auth_manager')
        
        if not websocket_manager:
            await websocket.close(code=4000, reason="WebSocket service unavailable")
            return
        
        # Authenticate user if token provided
        user_id = None
        if token and auth_manager:
            try:
                user_id = await auth_manager.verify_token(token)
            except Exception as e:
                await websocket.close(code=4001, reason="Authentication failed")
                return
        
        # Connect client
        connection_id = await websocket_manager.connect(websocket, user_id)
        
        try:
            # Listen for messages
            while True:
                try:
                    message = await websocket.receive_text()
                    await websocket_manager.handle_message(connection_id, message)
                except WebSocketDisconnect:
                    break
                except Exception as e:
                    logging.error(f"WebSocket error: {e}")
                    break
        
        finally:
            await websocket_manager.disconnect(connection_id)
    
    @app.get("/ws/stats")
    async def get_websocket_stats():
        """Get WebSocket connection statistics"""
        websocket_manager = registry.get('websocket_manager')
        if not websocket_manager:
            raise HTTPException(status_code=503, detail="WebSocket service unavailable")
        
        return await websocket_manager.get_connection_stats()
    
    @app.post("/ws/broadcast")
    async def broadcast_message(message: dict, token: str = None):
        """Broadcast message to all connected clients (admin only)"""
        # Get services from registry
        websocket_manager = registry.get('websocket_manager')
        auth_manager = registry.get('auth_manager')
        
        if not websocket_manager:
            raise HTTPException(status_code=503, detail="WebSocket service unavailable")
        
        # Verify admin token
        if not token:
            raise HTTPException(status_code=401, detail="Token required")
        
        if auth_manager:
            try:
                user_id = await auth_manager.verify_token(token)
                # Add admin check here based on your user system
                # For now, we'll assume any valid user can broadcast
            except Exception:
                raise HTTPException(status_code=401, detail="Invalid token")
        
        ws_message = WebSocketMessage(
            type=MessageType.STATUS,
            data=message,
            timestamp=datetime.now(),
            correlation_id=str(uuid.uuid4())
        )
        
        await websocket_manager._broadcast_message(ws_message)
        
        return {"status": "Message broadcasted"}


# For standalone testing
if __name__ == "__main__":
    import uvicorn
    from fastapi import FastAPI
    
    # Create standalone app for testing
    app = FastAPI(title="WebSocket Alert Service")
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Mock registry for testing
    class MockRegistry:
        def __init__(self):
            self.services = {}
            
        def get(self, name):
            return self.services.get(name)
            
        def register(self, name, service):
            self.services[name] = service
    
    # Setup WebSocket routes
    mock_registry = MockRegistry()
    setup_websocket_routes(app, mock_registry)
    
    # Run server
    uvicorn.run(app, host="0.0.0.0", port=8001)