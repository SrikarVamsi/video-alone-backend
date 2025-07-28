# import asyncio
# import json
# import logging
# import time
# import uuid
# from collections import defaultdict, deque
# from contextlib import asynccontextmanager
# from dataclasses import dataclass, field
# from enum import Enum
# from typing import Dict, List, Optional, Set, Callable, Any, Union
# from datetime import datetime, timedelta
# import weakref
# import redis.asyncio as redis
# from fastapi import FastAPI, WebSocket, HTTPException, Depends, status
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# from fastapi.middleware.cors import CORSMiddleware
# import jwt
# from pydantic import BaseModel, ValidationError
# import aiohttp
# from prometheus_client import Counter, Histogram, Gauge, start_http_server
# import asyncpg
# from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy import text
# from dotenv import load_dotenv
# import os


# load_dotenv()


# # Configuration
# @dataclass
# class WebSocketConfig:
#     max_connections_per_user: int = 5
#     max_total_connections: int = 10000
#     ping_interval: int = 30
#     ping_timeout: int = 10
#     message_queue_size: int = 1000
#     rate_limit_messages: int = 100
#     rate_limit_window: int = 60
#     jwt_secret: str = "your-secret-key"
#     jwt_algorithm: str = "HS256"
#     redis_url: str = "redis://localhost:6379"
#     postgres_url: str = "postgresql+asyncpg://user:pass@localhost/db"
#     enable_persistence: bool = True
#     max_room_size: int = 1000
#     cleanup_interval: int = 300  # 5 minutes


# # Models
# class MessageType(str, Enum):
#     ALERT = "alert"
#     ANALYTICS = "analytics"
#     SYSTEM = "system"
#     HEARTBEAT = "heartbeat"
#     SUBSCRIPTION = "subscription"
#     ERROR = "error"


# @dataclass
# class User:
#     id: str
#     username: str
#     roles: List[str]
#     permissions: Set[str]
    
#     def has_permission(self, permission: str) -> bool:
#         return permission in self.permissions


# @dataclass
# class Room:
#     id: str
#     name: str
#     type: str  # camera, alert_type, etc.
#     permissions: Set[str]
#     max_size: int = 1000
#     created_at: datetime = field(default_factory=datetime.utcnow)


# @dataclass
# class Connection:
#     id: str
#     websocket: WebSocket
#     user: User
#     rooms: Set[str]
#     subscriptions: Set[str]
#     last_ping: float
#     last_pong: float
#     message_queue: deque
#     rate_limit_count: int
#     rate_limit_reset: float
#     created_at: datetime
#     is_authenticated: bool = False
#     protocol_version: str = "1.0"
    
#     def __post_init__(self):
#         self.message_queue = deque(maxlen=1000)
#         self.rate_limit_reset = time.time() + 60


# class Message(BaseModel):
#     id: str = field(default_factory=lambda: str(uuid.uuid4()))
#     type: MessageType
#     room: Optional[str] = None
#     data: Dict[str, Any]
#     timestamp: datetime = field(default_factory=datetime.utcnow)
#     user_id: Optional[str] = None
#     persistent: bool = False
#     priority: int = 0  # 0 = normal, 1 = high, 2 = critical


# class Subscription(BaseModel):
#     rooms: List[str] = []
#     message_types: List[MessageType] = []
#     filters: Dict[str, Any] = {}


# # Metrics
# connection_count = Gauge('websocket_connections_total', 'Total WebSocket connections')
# message_count = Counter('websocket_messages_total', 'Total messages sent', ['type', 'room'])
# connection_duration = Histogram('websocket_connection_duration_seconds', 'Connection duration')
# message_latency = Histogram('websocket_message_latency_seconds', 'Message processing latency')
# error_count = Counter('websocket_errors_total', 'Total errors', ['type'])


# # Authentication
# class JWTAuth:
#     def __init__(self, secret: str, algorithm: str = "HS256"):
#         self.secret = secret
#         self.algorithm = algorithm
#         self.security = HTTPBearer()
    
#     async def authenticate(self, credentials: HTTPAuthorizationCredentials) -> User:
#         try:
#             payload = jwt.decode(credentials.credentials, self.secret, algorithms=[self.algorithm])
#             user_id = payload.get("user_id")
#             if not user_id:
#                 raise HTTPException(status_code=401, detail="Invalid token")
            
#             # In production, fetch user from database
#             user = User(
#                 id=user_id,
#                 username=payload.get("username", ""),
#                 roles=payload.get("roles", []),
#                 permissions=set(payload.get("permissions", []))
#             )
#             return user
#         except jwt.InvalidTokenError:
#             raise HTTPException(status_code=401, detail="Invalid token")


# # Rate Limiter
# class RateLimiter:
#     def __init__(self, redis_client: redis.Redis):
#         self.redis = redis_client
    
#     async def is_allowed(self, user_id: str, limit: int = 100, window: int = 60) -> bool:
#         key = f"rate_limit:{user_id}"
#         current_time = int(time.time())
#         window_start = current_time - window
        
#         async with self.redis.pipeline() as pipe:
#             pipe.zremrangebyscore(key, 0, window_start)
#             pipe.zcard(key)
#             pipe.zadd(key, {str(current_time): current_time})
#             pipe.expire(key, window)
#             results = await pipe.execute()
        
#         return results[1] < limit


# # Message Queue
# class MessageQueue:
#     def __init__(self, redis_client: redis.Redis):
#         self.redis = redis_client
    
#     async def enqueue(self, user_id: str, message: Message):
#         key = f"msg_queue:{user_id}"
#         await self.redis.lpush(key, message.model_dump_json())
#         await self.redis.expire(key, 3600)  # 1 hour TTL
    
#     async def dequeue(self, user_id: str) -> Optional[Message]:
#         key = f"msg_queue:{user_id}"
#         data = await self.redis.rpop(key)
#         if data:
#             return Message.model_validate_json(data)
#         return None
    
#     async def get_queued_messages(self, user_id: str) -> List[Message]:
#         key = f"msg_queue:{user_id}"
#         messages = await self.redis.lrange(key, 0, -1)
#         await self.redis.delete(key)
#         return [Message.model_validate_json(msg) for msg in messages]


# # Message Persistence
# class MessagePersistence:
#     def __init__(self, db_engine):
#         self.engine = db_engine
#         self.session_factory = sessionmaker(db_engine, class_=AsyncSession)
    
#     async def store_message(self, message: Message):
#         if not message.persistent:
#             return
        
#         async with self.session_factory() as session:
#             await session.execute(
#                 text("""
#                     INSERT INTO persistent_messages (id, type, room, data, user_id, timestamp, priority)
#                     VALUES (:id, :type, :room, :data, :user_id, :timestamp, :priority)
#                 """),
#                 {
#                     "id": message.id,
#                     "type": message.type,
#                     "room": message.room,
#                     "data": json.dumps(message.data),
#                     "user_id": message.user_id,
#                     "timestamp": message.timestamp,
#                     "priority": message.priority
#                 }
#             )
#             await session.commit()
    
#     async def get_missed_messages(self, user_id: str, since: datetime) -> List[Message]:
#         async with self.session_factory() as session:
#             result = await session.execute(
#                 text("""
#                     SELECT id, type, room, data, user_id, timestamp, priority
#                     FROM persistent_messages
#                     WHERE user_id = :user_id AND timestamp > :since
#                     ORDER BY timestamp ASC
#                 """),
#                 {"user_id": user_id, "since": since}
#             )
            
#             messages = []
#             for row in result:
#                 messages.append(Message(
#                     id=row.id,
#                     type=MessageType(row.type),
#                     room=row.room,
#                     data=json.loads(row.data),
#                     user_id=row.user_id,
#                     timestamp=row.timestamp,
#                     priority=row.priority,
#                     persistent=True
#                 ))
#             return messages


# # Room Manager
# class RoomManager:
#     def __init__(self, redis_client: redis.Redis):
#         self.redis = redis_client
#         self.rooms: Dict[str, Room] = {}
#         self.room_connections: Dict[str, Set[str]] = defaultdict(set)
    
#     async def create_room(self, room: Room):
#         self.rooms[room.id] = room
#         await self.redis.hset(f"room:{room.id}", mapping={
#             "name": room.name,
#             "type": room.type,
#             "permissions": json.dumps(list(room.permissions)),
#             "max_size": room.max_size,
#             "created_at": room.created_at.isoformat()
#         })
    
#     async def join_room(self, room_id: str, connection_id: str, user: User) -> bool:
#         room = await self.get_room(room_id)
#         if not room:
#             return False
        
#         # Check permissions
#         if room.permissions and not any(user.has_permission(perm) for perm in room.permissions):
#             return False
        
#         # Check room size
#         if len(self.room_connections[room_id]) >= room.max_size:
#             return False
        
#         self.room_connections[room_id].add(connection_id)
#         await self.redis.sadd(f"room_members:{room_id}", connection_id)
#         return True
    
#     async def leave_room(self, room_id: str, connection_id: str):
#         self.room_connections[room_id].discard(connection_id)
#         await self.redis.srem(f"room_members:{room_id}", connection_id)
    
#     async def get_room(self, room_id: str) -> Optional[Room]:
#         if room_id in self.rooms:
#             return self.rooms[room_id]
        
#         data = await self.redis.hgetall(f"room:{room_id}")
#         if data:
#             room = Room(
#                 id=room_id,
#                 name=data["name"],
#                 type=data["type"],
#                 permissions=set(json.loads(data["permissions"])),
#                 max_size=int(data["max_size"]),
#                 created_at=datetime.fromisoformat(data["created_at"])
#             )
#             self.rooms[room_id] = room
#             return room
#         return None
    
#     async def get_room_members(self, room_id: str) -> Set[str]:
#         return await self.redis.smembers(f"room_members:{room_id}")


# # Main WebSocket Manager
# class WebSocketManager:
#     def __init__(self, config: WebSocketConfig):
#         self.config = config
#         self.connections: Dict[str, Connection] = {}
#         self.user_connections: Dict[str, Set[str]] = defaultdict(set)
#         self.connection_count_gauge = connection_count
        
#         # Initialize components
#         self.redis = redis.from_url(config.redis_url)
#         self.db_engine = create_async_engine(config.postgres_url)
#         self.auth = JWTAuth(config.jwt_secret, config.jwt_algorithm)
#         self.rate_limiter = RateLimiter(self.redis)
#         self.message_queue = MessageQueue(self.redis)
#         self.message_persistence = MessagePersistence(self.db_engine)
#         self.room_manager = RoomManager(self.redis)
        
#         # Background tasks
#         self.ping_task: Optional[asyncio.Task] = None
#         self.cleanup_task: Optional[asyncio.Task] = None
        
#         # Message filters
#         self.message_filters: Dict[str, Callable] = {}
        
#         # Shutdown flag
#         self.shutdown_flag = False
    
#     async def startup(self):
#         """Initialize the WebSocket manager"""
#         # Start background tasks
#         self.ping_task = asyncio.create_task(self._ping_connections())
#         self.cleanup_task = asyncio.create_task(self._cleanup_connections())
        
#         # Start metrics server
#         start_http_server(8000)
        
#         logging.info("WebSocket Manager started")
    
#     async def shutdown(self):
#         """Gracefully shutdown the WebSocket manager"""
#         self.shutdown_flag = True
        
#         # Cancel background tasks
#         if self.ping_task:
#             self.ping_task.cancel()
#         if self.cleanup_task:
#             self.cleanup_task.cancel()
        
#         # Close all connections
#         for connection in list(self.connections.values()):
#             await self.disconnect(connection.id)
        
#         # Close Redis connection
#         await self.redis.close()
        
#         logging.info("WebSocket Manager shut down")
    
#     async def connect(self, websocket: WebSocket, user: User, protocol_version: str = "1.0") -> str:
#         """Establish a new WebSocket connection"""
#         # Check connection limits
#         if len(self.connections) >= self.config.max_total_connections:
#             raise HTTPException(status_code=503, detail="Server at capacity")
        
#         if len(self.user_connections[user.id]) >= self.config.max_connections_per_user:
#             raise HTTPException(status_code=429, detail="Too many connections for user")
        
#         # Create connection
#         connection_id = str(uuid.uuid4())
#         connection = Connection(
#             id=connection_id,
#             websocket=websocket,
#             user=user,
#             rooms=set(),
#             subscriptions=set(),
#             last_ping=time.time(),
#             last_pong=time.time(),
#             message_queue=deque(maxlen=self.config.message_queue_size),
#             rate_limit_count=0,
#             rate_limit_reset=time.time() + self.config.rate_limit_window,
#             created_at=datetime.utcnow(),
#             is_authenticated=True,
#             protocol_version=protocol_version
#         )
        
#         # Store connection
#         self.connections[connection_id] = connection
#         self.user_connections[user.id].add(connection_id)
        
#         # Update metrics
#         self.connection_count_gauge.set(len(self.connections))
        
#         # Send queued messages
#         queued_messages = await self.message_queue.get_queued_messages(user.id)
#         for message in queued_messages:
#             await self._send_message_to_connection(connection, message)
        
#         # Send missed persistent messages
#         if self.config.enable_persistence:
#             missed_messages = await self.message_persistence.get_missed_messages(
#                 user.id, datetime.utcnow() - timedelta(hours=24)
#             )
#             for message in missed_messages:
#                 await self._send_message_to_connection(connection, message)
        
#         logging.info(f"Connection {connection_id} established for user {user.id}")
#         return connection_id
    
#     async def disconnect(self, connection_id: str):
#         """Disconnect a WebSocket connection"""
#         if connection_id not in self.connections:
#             return
        
#         connection = self.connections[connection_id]
        
#         # Leave all rooms
#         for room_id in list(connection.rooms):
#             await self.room_manager.leave_room(room_id, connection_id)
        
#         # Remove from tracking
#         self.user_connections[connection.user.id].discard(connection_id)
#         del self.connections[connection_id]
        
#         # Update metrics
#         self.connection_count_gauge.set(len(self.connections))
#         connection_duration.observe(
#             (datetime.utcnow() - connection.created_at).total_seconds()
#         )
        
#         # Close WebSocket
#         try:
#             await connection.websocket.close()
#         except:
#             pass
        
#         logging.info(f"Connection {connection_id} disconnected")
    
#     async def handle_message(self, connection_id: str, message: dict):
#         """Handle incoming WebSocket message"""
#         start_time = time.time()
        
#         try:
#             connection = self.connections.get(connection_id)
#             if not connection:
#                 return
            
#             # Rate limiting
#             if not await self._check_rate_limit(connection):
#                 await self._send_error(connection, "Rate limit exceeded")
#                 return
            
#             # Process message based on type
#             msg_type = message.get("type")
            
#             if msg_type == "subscribe":
#                 await self._handle_subscription(connection, message)
#             elif msg_type == "join_room":
#                 await self._handle_join_room(connection, message)
#             elif msg_type == "leave_room":
#                 await self._handle_leave_room(connection, message)
#             elif msg_type == "ping":
#                 await self._handle_ping(connection)
#             elif msg_type == "broadcast":
#                 await self._handle_broadcast(connection, message)
#             else:
#                 await self._send_error(connection, f"Unknown message type: {msg_type}")
        
#         except Exception as e:
#             error_count.labels(type="message_handling").inc()
#             logging.error(f"Error handling message: {e}")
        
#         finally:
#             message_latency.observe(time.time() - start_time)
    
#     async def broadcast_to_room(self, room_id: str, message: Message):
#         """Broadcast message to all connections in a room"""
#         # Store persistent message
#         if message.persistent:
#             await self.message_persistence.store_message(message)
        
#         # Get room members
#         member_ids = await self.room_manager.get_room_members(room_id)
        
#         # Send to connected members
#         for connection_id in member_ids:
#             if connection_id in self.connections:
#                 connection = self.connections[connection_id]
#                 if await self._should_send_message(connection, message):
#                     await self._send_message_to_connection(connection, message)
#             else:
#                 # Queue for disconnected users
#                 # In production, you'd need to map connection_id to user_id
#                 pass
        
#         message_count.labels(type=message.type, room=room_id).inc()
    
#     async def broadcast_to_user(self, user_id: str, message: Message):
#         """Broadcast message to all connections of a user"""
#         connection_ids = self.user_connections.get(user_id, set())
        
#         if not connection_ids:
#             # User not connected, queue message
#             await self.message_queue.enqueue(user_id, message)
#             return
        
#         for connection_id in connection_ids:
#             if connection_id in self.connections:
#                 connection = self.connections[connection_id]
#                 if await self._should_send_message(connection, message):
#                     await self._send_message_to_connection(connection, message)
    
#     async def _handle_subscription(self, connection: Connection, message: dict):
#         """Handle subscription message"""
#         try:
#             subscription = Subscription.model_validate(message.get("data", {}))
            
#             # Update subscriptions
#             connection.subscriptions.clear()
#             connection.subscriptions.update(subscription.message_types)
            
#             # Join rooms
#             for room_id in subscription.rooms:
#                 await self.room_manager.join_room(room_id, connection.id, connection.user)
#                 connection.rooms.add(room_id)
            
#             await self._send_message_to_connection(connection, Message(
#                 type=MessageType.SYSTEM,
#                 data={"message": "Subscription updated"}
#             ))
        
#         except ValidationError as e:
#             await self._send_error(connection, f"Invalid subscription: {e}")
    
#     async def _handle_join_room(self, connection: Connection, message: dict):
#         """Handle join room message"""
#         room_id = message.get("room_id")
#         if not room_id:
#             await self._send_error(connection, "Missing room_id")
#             return
        
#         if await self.room_manager.join_room(room_id, connection.id, connection.user):
#             connection.rooms.add(room_id)
#             await self._send_message_to_connection(connection, Message(
#                 type=MessageType.SYSTEM,
#                 data={"message": f"Joined room {room_id}"}
#             ))
#         else:
#             await self._send_error(connection, f"Failed to join room {room_id}")
    
#     async def _handle_leave_room(self, connection: Connection, message: dict):
#         """Handle leave room message"""
#         room_id = message.get("room_id")
#         if not room_id:
#             await self._send_error(connection, "Missing room_id")
#             return
        
#         await self.room_manager.leave_room(room_id, connection.id)
#         connection.rooms.discard(room_id)
        
#         await self._send_message_to_connection(connection, Message(
#             type=MessageType.SYSTEM,
#             data={"message": f"Left room {room_id}"}
#         ))
    
#     async def _handle_ping(self, connection: Connection):
#         """Handle ping message"""
#         connection.last_ping = time.time()
#         await self._send_message_to_connection(connection, Message(
#             type=MessageType.HEARTBEAT,
#             data={"type": "pong", "timestamp": time.time()}
#         ))
    
#     async def _handle_broadcast(self, connection: Connection, message: dict):
#         """Handle broadcast message"""
#         if not connection.user.has_permission("broadcast"):
#             await self._send_error(connection, "Insufficient permissions")
#             return
        
#         try:
#             broadcast_msg = Message.model_validate(message.get("data", {}))
#             broadcast_msg.user_id = connection.user.id
            
#             if broadcast_msg.room:
#                 await self.broadcast_to_room(broadcast_msg.room, broadcast_msg)
#             else:
#                 await self._send_error(connection, "Room required for broadcast")
        
#         except ValidationError as e:
#             await self._send_error(connection, f"Invalid broadcast message: {e}")
    
#     async def _send_message_to_connection(self, connection: Connection, message: Message):
#         """Send message to a specific connection"""
#         try:
#             await connection.websocket.send_text(message.model_dump_json())
#         except Exception as e:
#             error_count.labels(type="send_message").inc()
#             logging.error(f"Failed to send message to {connection.id}: {e}")
#             await self.disconnect(connection.id)
    
#     async def _send_error(self, connection: Connection, error_message: str):
#         """Send error message to connection"""
#         error_msg = Message(
#             type=MessageType.ERROR,
#             data={"error": error_message}
#         )
#         await self._send_message_to_connection(connection, error_msg)
    
#     async def _should_send_message(self, connection: Connection, message: Message) -> bool:
#         """Check if message should be sent to connection"""
#         # Check subscription filters
#         if connection.subscriptions and message.type not in connection.subscriptions:
#             return False
        
#         # Apply custom filters
#         for filter_name, filter_func in self.message_filters.items():
#             if not filter_func(connection, message):
#                 return False
        
#         return True
    
#     async def _check_rate_limit(self, connection: Connection) -> bool:
#         """Check if connection is within rate limits"""
#         current_time = time.time()
        
#         if current_time > connection.rate_limit_reset:
#             connection.rate_limit_count = 0
#             connection.rate_limit_reset = current_time + self.config.rate_limit_window
        
#         if connection.rate_limit_count >= self.config.rate_limit_messages:
#             return False
        
#         connection.rate_limit_count += 1
#         return True
    
#     async def _ping_connections(self):
#         """Background task to ping connections"""
#         while not self.shutdown_flag:
#             try:
#                 current_time = time.time()
                
#                 for connection in list(self.connections.values()):
#                     # Send ping if needed
#                     if current_time - connection.last_ping > self.config.ping_interval:
#                         ping_msg = Message(
#                             type=MessageType.HEARTBEAT,
#                             data={"type": "ping", "timestamp": current_time}
#                         )
#                         await self._send_message_to_connection(connection, ping_msg)
#                         connection.last_ping = current_time
                    
#                     # Check for stale connections
#                     if current_time - connection.last_pong > self.config.ping_timeout:
#                         logging.warning(f"Connection {connection.id} timed out")
#                         await self.disconnect(connection.id)
                
#                 await asyncio.sleep(self.config.ping_interval)
            
#             except Exception as e:
#                 error_count.labels(type="ping_task").inc()
#                 logging.error(f"Error in ping task: {e}")
#                 await asyncio.sleep(5)
    
#     async def _cleanup_connections(self):
#         """Background task to cleanup stale connections"""
#         while not self.shutdown_flag:
#             try:
#                 # Clean up empty user connection sets
#                 empty_users = [
#                     user_id for user_id, connections in self.user_connections.items()
#                     if not connections
#                 ]
#                 for user_id in empty_users:
#                     del self.user_connections[user_id]
                
#                 # Clean up empty room connections
#                 for room_id in list(self.room_manager.room_connections.keys()):
#                     if not self.room_manager.room_connections[room_id]:
#                         del self.room_manager.room_connections[room_id]
                
#                 await asyncio.sleep(self.config.cleanup_interval)
            
#             except Exception as e:
#                 error_count.labels(type="cleanup_task").inc()
#                 logging.error(f"Error in cleanup task: {e}")
#                 await asyncio.sleep(60)
    
#     def add_message_filter(self, name: str, filter_func: Callable[[Connection, Message], bool]):
#         """Add a custom message filter"""
#         self.message_filters[name] = filter_func
    
#     def remove_message_filter(self, name: str):
#         """Remove a custom message filter"""
#         self.message_filters.pop(name, None)
    
#     def get_connection_stats(self) -> Dict[str, Any]:
#         """Get connection statistics"""
#         return {
#             "total_connections": len(self.connections),
#             "connections_by_user": {
#                 user_id: len(connections)
#                 for user_id, connections in self.user_connections.items()
#             },
#             "rooms": len(self.room_manager.rooms),
#             "room_connections": {
#                 room_id: len(connections)
#                 for room_id, connections in self.room_manager.room_connections.items()
#             }
#         }


# # FastAPI Integration
# def create_websocket_app(config: WebSocketConfig) -> FastAPI:
#     """Create FastAPI app with WebSocket support"""
#     app = FastAPI(title="WebSocket Analytics API")
    
#     # Add CORS middleware
#     app.add_middleware(
#         CORSMiddleware,
#         allow_origins=["*"],
#         allow_credentials=True,
#         allow_methods=["*"],
#         allow_headers=["*"],
#     )
    
#     # Initialize WebSocket manager
#     ws_manager = WebSocketManager(config)
    
#     @app.on_event("startup")
#     async def startup_event():
#         await ws_manager.startup()
    
#     @app.on_event("shutdown")
#     async def shutdown_event():
#         await ws_manager.shutdown()
    
#     @app.websocket("/ws")
#     async def websocket_endpoint(websocket: WebSocket, token: str):
#         await websocket.accept()
        
#         try:
#             # Authenticate user
#             credentials = HTTPAuthorizationCredentials(
#                 scheme="Bearer",
#                 credentials=token
#             )
#             user = await ws_manager.auth.authenticate(credentials)
            
#             # Establish connection
#             connection_id = await ws_manager.connect(websocket, user)
            
#             # Handle messages
#             while True:
#                 try:
#                     data = await websocket.receive_text()
#                     message = json.loads(data)
#                     await ws_manager.handle_message(connection_id, message)
#                 except Exception as e:
#                     logging.error(f"Error receiving message: {e}")
#                     break
        
#         except Exception as e:
#             logging.error(f"WebSocket error: {e}")
#         finally:
#             if 'connection_id' in locals():
#                 await ws_manager.disconnect(connection_id)
    
#     @app.get("/stats")
#     async def get_stats():
#         return ws_manager.get_connection_stats()
    
#     @app.post("/broadcast")
#     async def broadcast_message(
#         message: Message,
#         user: User = Depends(ws_manager.auth.authenticate)
#     ):
#         if not user.has_permission("broadcast"):
#             raise HTTPException(status_code=403, detail="Insufficient permissions")
        
#         if message.room:
#             await ws_manager.broadcast_to_room(message.room, message)
#         else:
#             raise HTTPException(status_code=400, detail="Room required")
        
#         return {"status": "sent"}
    
#     return app


# # Usage Example
# if __name__ == "__main__":
#     import uvicorn
    
#     # Configure logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
    
#     # Create configuration
#     config = WebSocketConfig(
#         max_connections_per_user=5,
#         max_total_connections=10000,
#         ping_interval=30,
#         jwt_secret=os.getenv("JWT_SECRET"),
#         redis_url="redis://localhost:6379",
#         postgres_url="postgresql+asyncpg://user:password@localhost/analytics"
#     )
    

    
    
#     # Create app
#     app = create_websocket_app(config)
    
#     # Run server
#     uvicorn.run(
#         app,
#         host="0.0.0.0",
#         port=8080,
#         log_level="info"
#     )

import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Callable, Any, Union
from datetime import datetime, timedelta
import weakref
import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
import jwt
from pydantic import BaseModel, ValidationError
import aiohttp
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from dotenv import load_dotenv
import os

# Import the global Settings from your config module
from config.settings import Settings as AppSettings


load_dotenv()


# Removed the local WebSocketConfig dataclass as it's not used for the main application settings.
# The 'config' object passed to WebSocketManager will be an instance of AppSettings (from config.settings).


# Models
class MessageType(str, Enum):
    ALERT = "alert"
    ANALYTICS = "analytics"
    SYSTEM = "system"
    HEARTBEAT = "heartbeat"
    SUBSCRIPTION = "subscription"
    ERROR = "error"


@dataclass
class User:
    id: str
    username: str
    roles: List[str]
    permissions: Set[str]
    
    def has_permission(self, permission: str) -> bool:
        return permission in self.permissions


@dataclass
class Room:
    id: str
    name: str
    type: str  # camera, alert_type, etc.
    permissions: Set[str]
    max_size: int = 1000
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Connection:
    id: str
    websocket: WebSocket
    user: User
    rooms: Set[str]
    subscriptions: Set[str]
    last_ping: float
    last_pong: float
    message_queue: deque
    rate_limit_count: int
    rate_limit_reset: float
    created_at: datetime
    is_authenticated: bool = False
    protocol_version: str = "1.0"
    
    def __post_init__(self):
        self.message_queue = deque(maxlen=1000)
        self.rate_limit_reset = time.time() + 60


class Message(BaseModel):
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: MessageType
    room: Optional[str] = None
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    user_id: Optional[str] = None
    persistent: bool = False
    priority: int = 0  # 0 = normal, 1 = high, 2 = critical


class Subscription(BaseModel):
    rooms: List[str] = []
    message_types: List[MessageType] = []
    filters: Dict[str, Any] = {}


# Metrics
connection_count = Gauge('websocket_connections_total', 'Total WebSocket connections')
message_count = Counter('websocket_messages_total', 'Total messages sent', ['type', 'room'])
connection_duration = Histogram('websocket_connection_duration_seconds', 'Connection duration')
message_latency = Histogram('websocket_message_latency_seconds', 'Message processing latency')
error_count = Counter('websocket_errors_total', 'Total errors', ['type'])


# Authentication
class JWTAuth:
    def __init__(self, secret: str, algorithm: str = "HS256"):
        self.secret = secret
        self.algorithm = algorithm
        self.security = HTTPBearer()
    
    async def authenticate(self, credentials: HTTPAuthorizationCredentials) -> User:
        try:
            payload = jwt.decode(credentials.credentials, self.secret, algorithms=[self.algorithm])
            user_id = payload.get("user_id")
            if not user_id:
                raise HTTPException(status_code=401, detail="Invalid token")
            
            # In production, fetch user from database
            user = User(
                id=user_id,
                username=payload.get("username", ""),
                roles=payload.get("roles", []),
                permissions=set(payload.get("permissions", []))
            )
            return user
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")


# Rate Limiter
class RateLimiter:
    def __init__(self, redis_client: Optional[redis.Redis]): # Added Optional for graceful handling
        self.redis = redis_client
        if not self.redis:
            logging.warning("RateLimiter initialized without Redis client. Rate limiting will not be enforced.")
    
    async def is_allowed(self, user_id: str, limit: int = 100, window: int = 60) -> bool:
        if not self.redis:
            return True # Allow all if Redis is not available
        
        key = f"rate_limit:{user_id}"
        current_time = int(time.time())
        window_start = current_time - window
        
        try:
            async with self.redis.pipeline() as pipe:
                pipe.zremrangebyscore(key, 0, window_start)
                pipe.zcard(key)
                pipe.zadd(key, {str(current_time): current_time})
                pipe.expire(key, window)
                results = await pipe.execute()
            
            return results[1] < limit
        except Exception as e:
            logging.error(f"RateLimiter Redis operation failed for user {user_id}: {e}")
            error_count.labels(type="redis_ratelimit").inc()
            return True # Fail open: if Redis fails, don't block requests


# Message Queue
class MessageQueue:
    def __init__(self, redis_client: Optional[redis.Redis]): # Added Optional
        self.redis = redis_client
        if not self.redis:
            logging.warning("MessageQueue initialized without Redis client. Message queuing will not be available.")
    
    async def enqueue(self, user_id: str, message: Message):
        if not self.redis:
            return
        try:
            key = f"msg_queue:{user_id}"
            await self.redis.lpush(key, message.model_dump_json())
            await self.redis.expire(key, 3600)  # 1 hour TTL
        except Exception as e:
            logging.error(f"Failed to enqueue message for user {user_id}: {e}")
            error_count.labels(type="redis_message_queue").inc()
    
    async def dequeue(self, user_id: str) -> Optional[Message]:
        if not self.redis:
            return None
        try:
            key = f"msg_queue:{user_id}"
            data = await self.redis.rpop(key)
            if data:
                return Message.model_validate_json(data)
            return None
        except Exception as e:
            logging.error(f"Failed to dequeue message for user {user_id}: {e}")
            error_count.labels(type="redis_message_queue").inc()
            return None
    
    async def get_queued_messages(self, user_id: str) -> List[Message]:
        if not self.redis:
            return []
        try:
            key = f"msg_queue:{user_id}"
            messages = await self.redis.lrange(key, 0, -1)
            await self.redis.delete(key) # Clear after reading
            return [Message.model_validate_json(msg) for msg in messages]
        except Exception as e:
            logging.error(f"Failed to get queued messages for user {user_id}: {e}")
            error_count.labels(type="redis_message_queue").inc()
            return []


# Message Persistence
class MessagePersistence:
    def __init__(self, db_engine: Optional[Any]): # Added Optional for graceful handling
        self.engine = db_engine
        if self.engine:
            self.session_factory = sessionmaker(db_engine, class_=AsyncSession)
        else:
            logging.warning("MessagePersistence initialized without DB engine. Message persistence will not be available.")
    
    async def store_message(self, message: Message):
        if not message.persistent or not self.engine:
            return
        
        try:
            async with self.session_factory() as session:
                await session.execute(
                    text("""
                        INSERT INTO persistent_messages (id, type, room, data, user_id, timestamp, priority)
                        VALUES (:id, :type, :room, :data, :user_id, :timestamp, :priority)
                    """),
                    {
                        "id": message.id,
                        "type": message.type.value, # Use .value for Enum
                        "room": message.room,
                        "data": json.dumps(message.data),
                        "user_id": message.user_id,
                        "timestamp": message.timestamp,
                        "priority": message.priority
                    }
                )
                await session.commit()
        except Exception as e:
            logging.error(f"Failed to store persistent message {message.id}: {e}")
            error_count.labels(type="db_persistence").inc()
    
    async def get_missed_messages(self, user_id: str, since: datetime) -> List[Message]:
        if not self.engine:
            return []
        try:
            async with self.session_factory() as session:
                result = await session.execute(
                    text("""
                        SELECT id, type, room, data, user_id, timestamp, priority
                        FROM persistent_messages
                        WHERE user_id = :user_id AND timestamp > :since
                        ORDER BY timestamp ASC
                    """),
                    {"user_id": user_id, "since": since}
                )
                
                messages = []
                for row in result:
                    messages.append(Message(
                        id=row.id,
                        type=MessageType(row.type),
                        room=row.room,
                        data=json.loads(row.data),
                        user_id=row.user_id,
                        timestamp=row.timestamp,
                        priority=row.priority,
                        persistent=True
                    ))
                return messages
        except Exception as e:
            logging.error(f"Failed to get missed messages for user {user_id}: {e}")
            error_count.labels(type="db_persistence").inc()
            return []


# Room Manager
class RoomManager:
    def __init__(self, redis_client: Optional[redis.Redis]): # Added Optional
        self.redis = redis_client
        self.rooms: Dict[str, Room] = {} # Local cache for room definitions
        self.room_connections: Dict[str, Set[str]] = defaultdict(set) # In-memory mapping for live connections
        if not self.redis:
            logging.warning("RoomManager initialized without Redis client. Room persistence will be limited.")

    async def create_room(self, room: Room):
        self.rooms[room.id] = room
        if self.redis:
            try:
                await self.redis.hset(f"room:{room.id}", mapping={
                    "name": room.name,
                    "type": room.type,
                    "permissions": json.dumps(list(room.permissions)),
                    "max_size": room.max_size,
                    "created_at": room.created_at.isoformat()
                })
            except Exception as e:
                logging.error(f"Redis operation failed for create_room {room.id}: {e}")
                error_count.labels(type="redis_room_manager").inc()
    
    async def join_room(self, room_id: str, connection_id: str, user: User) -> bool:
        room = await self.get_room(room_id)
        if not room:
            logging.warning(f"Attempted to join non-existent room: {room_id}")
            return False
        
        # Check permissions
        if room.permissions and not any(user.has_permission(perm) for perm in room.permissions):
            logging.warning(f"User {user.id} lacks permission to join room {room_id}")
            return False
        
        # Check room size (using in-memory count for live connections)
        if len(self.room_connections[room_id]) >= room.max_size:
            logging.warning(f"Room {room_id} is full. Max size: {room.max_size}")
            return False
        
        self.room_connections[room_id].add(connection_id)
        if self.redis:
            try:
                await self.redis.sadd(f"room_members:{room_id}", connection_id)
            except Exception as e:
                logging.error(f"Redis operation failed for join_room {room_id}: {e}")
                error_count.labels(type="redis_room_manager").inc()
        return True
    
    async def leave_room(self, room_id: str, connection_id: str):
        self.room_connections[room_id].discard(connection_id)
        if self.redis:
            try:
                await self.redis.srem(f"room_members:{room_id}", connection_id)
            except Exception as e:
                logging.error(f"Redis operation failed for leave_room {room_id}: {e}")
                error_count.labels(type="redis_room_manager").inc()

    async def get_room(self, room_id: str) -> Optional[Room]:
        if room_id in self.rooms:
            return self.rooms[room_id]
        
        if self.redis:
            try:
                data = await self.redis.hgetall(f"room:{room_id}")
                if data:
                    room = Room(
                        id=room_id,
                        name=data["name"],
                        type=data["type"],
                        permissions=set(json.loads(data["permissions"])),
                        max_size=int(data["max_size"]),
                        created_at=datetime.fromisoformat(data["created_at"])
                    )
                    self.rooms[room_id] = room # Cache locally
                    return room
            except Exception as e:
                logging.error(f"Redis operation failed for get_room {room_id}: {e}")
                error_count.labels(type="redis_room_manager").inc()
        return None
    
    async def get_room_members(self, room_id: str) -> Set[str]:
        # Prioritize in-memory for currently connected members
        if room_id in self.room_connections:
            return self.room_connections[room_id]
        
        if self.redis:
            try:
                # If not in memory (e.g., after restart, or connections managed elsewhere)
                # fetch from Redis, but consider if this should be the primary source for live members.
                # For a live connection manager, in-memory `self.room_connections` should be definitive.
                # This part needs careful design depending on distributed vs. single instance.
                # For now, it will fetch from Redis if not in local cache (e.g., a room was created externally)
                members = await self.redis.smembers(f"room_members:{room_id}")
                # Update local cache for consistency
                if members:
                    self.room_connections[room_id].update(members)
                return members
            except Exception as e:
                logging.error(f"Redis operation failed for get_room_members {room_id}: {e}")
                error_count.labels(type="redis_room_manager").inc()
        return set() # Return empty set if no Redis or in-memory data


# Main WebSocket Manager
class WebSocketManager:
    def __init__(self, app_settings: AppSettings): # Type hint changed to AppSettings
        self.app_settings = app_settings
        self.config = app_settings.websocket # Access the nested websocket config for local use
        
        self.connections: Dict[str, Connection] = {}
        self.user_connections: Dict[str, Set[str]] = defaultdict(set)
        self.connection_count_gauge = connection_count
        
        # Initialize Redis
        self.redis: Optional[redis.Redis] = None
        if self.app_settings.redis.enabled:
            redis_password_part = f":{self.app_settings.redis.password}" if self.app_settings.redis.password else ""
            redis_url_str = f"redis://{redis_password_part}@{self.app_settings.redis.host}:{self.app_settings.redis.port}/{self.app_settings.redis.db}"
            try:
                self.redis = redis.from_url(
                    redis_url_str,
                    socket_timeout=self.app_settings.redis.socket_timeout,
                    socket_connect_timeout=self.app_settings.redis.socket_connect_timeout,
                    retry_on_timeout=True,
                    health_check_interval=30
                )
                logging.info(f"WebSocketManager: Connected to Redis at {redis_url_str}")
            except Exception as e:
                logging.warning(f"WebSocketManager: Could not connect to Redis at {redis_url_str}. WebSocket features requiring Redis might be limited. Error: {e}")
                error_count.labels(type="redis_connection").inc()
                self.redis = None # Set to None if connection fails
        else:
            logging.info("WebSocketManager: Redis is disabled in settings. Running without Redis support.")
            self.redis = None # Explicitly set to None if disabled

        # Initialize DB Engine
        self.db_engine: Optional[Any] = None
        # Now self.config.enable_persistence will exist
        if self.config.enable_persistence:
            try:
                # Use self.config.postgres_url now that it's defined in WebSocketConfig
                self.db_engine = create_async_engine(self.config.postgres_url) 
                logging.info(f"WebSocketManager: Connected to PostgreSQL at {self.config.postgres_url}")
            except Exception as e:
                logging.warning(f"WebSocketManager: Could not connect to PostgreSQL at {self.config.postgres_url}. Message persistence might be limited. Error: {e}")
                error_count.labels(type="postgres_connection").inc()
                self.db_engine = None
        else:
            logging.info("WebSocketManager: PostgreSQL persistence is disabled in settings.")
            self.db_engine = None
        

        # Initialize components, passing self.redis and self.db_engine
        self.auth = JWTAuth(self.app_settings.jwt.secret, self.app_settings.jwt.algorithm)
        self.rate_limiter = RateLimiter(self.redis)
        self.message_queue = MessageQueue(self.redis)
        self.message_persistence = MessagePersistence(self.db_engine)
        self.room_manager = RoomManager(self.redis)
        
        # Background tasks
        self.ping_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        
        # Message filters
        self.message_filters: Dict[str, Callable] = {}
        
        # Shutdown flag
        self.shutdown_flag = False
    
    async def startup(self):
        """Initialize the WebSocket manager"""
        # Start background tasks
        self.ping_task = asyncio.create_task(self._ping_connections())
        self.cleanup_task = asyncio.create_task(self._cleanup_connections())
        
        # Start metrics server (if enabled and not already started by main.py)
        # It's usually better for main.py to handle starting Prometheus if it's a global service.
        # This part should be coordinated with main.py's monitoring setup.
        # For now, leaving as-is, but be aware of potential conflicts if main.py also starts it.
        # if self.app_settings.monitoring_prometheus_enabled:
        #     try:
        #         start_http_server(self.app_settings.monitoring_prometheus_port)
        #         logging.info(f"Prometheus metrics server started on port {self.app_settings.monitoring_prometheus_port}")
        #     except OSError as e:
        #         logging.warning(f"Could not start Prometheus metrics server (port already in use?): {e}")

        logging.info("WebSocket Manager started")
    
    async def shutdown(self):
        """Gracefully shutdown the WebSocket manager"""
        self.shutdown_flag = True
        
        # Cancel background tasks
        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task # Await to ensure it's cancelled
            except asyncio.CancelledError:
                pass
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task # Await to ensure it's cancelled
            except asyncio.CancelledError:
                pass
        
        # Close all connections
        # Create a copy of values to avoid RuntimeError: dictionary changed size during iteration
        for connection in list(self.connections.values()):
            await self.disconnect(connection.id)
        
        # Close Redis connection
        if self.redis:
            await self.redis.close()
            logging.info("WebSocketManager: Redis connection closed.")
        
        # Close DB engine (if using connection pool that needs explicit close)
        if self.db_engine:
            if hasattr(self.db_engine, 'dispose'): # asyncpg engine might have this
                await self.db_engine.dispose()
                logging.info("WebSocketManager: PostgreSQL engine disposed.")
        
        logging.info("WebSocket Manager shut down")
    
    async def connect(self, websocket: WebSocket, user: User, protocol_version: str = "1.0") -> str:
        """Establish a new WebSocket connection"""
        # Check connection limits
        if len(self.connections) >= self.config.max_total_connections:
            raise HTTPException(status_code=503, detail="Server at capacity")
        
        if len(self.user_connections[user.id]) >= self.config.max_connections_per_user:
            raise HTTPException(status_code=429, detail="Too many connections for user")
        
        # Create connection
        connection_id = str(uuid.uuid4())
        connection = Connection(
            id=connection_id,
            websocket=websocket,
            user=user,
            rooms=set(),
            subscriptions=set(),
            last_ping=time.time(),
            last_pong=time.time(),
            message_queue=deque(maxlen=self.config.message_queue_size),
            rate_limit_count=0,
            rate_limit_reset=time.time() + self.config.rate_limit_window,
            created_at=datetime.utcnow(),
            is_authenticated=True,
            protocol_version=protocol_version
        )
        
        # Store connection
        self.connections[connection_id] = connection
        self.user_connections[user.id].add(connection_id)
        
        # Update metrics
        connection_count.set(len(self.connections))
        
        # Send queued messages
        if self.message_queue: # Check if MessageQueue is available
            queued_messages = await self.message_queue.get_queued_messages(user.id)
            for message in queued_messages:
                await self._send_message_to_connection(connection, message)
        
        # Send missed persistent messages
        if self.config.enable_persistence and self.message_persistence: # Check if persistence is enabled and available
            missed_messages = await self.message_persistence.get_missed_messages(
                user.id, datetime.utcnow() - timedelta(hours=24) # TODO: Make this 'since' time configurable/from user last seen
            )
            for message in missed_messages:
                await self._send_message_to_connection(connection, message)
        
        logging.info(f"Connection {connection_id} established for user {user.id}")
        return connection_id
    
    async def disconnect(self, connection_id: str):
        """Disconnect a WebSocket connection"""
        if connection_id not in self.connections:
            return
        
        connection = self.connections[connection_id]
        
        # Leave all rooms
        if self.room_manager: # Check if RoomManager is available
            for room_id in list(connection.rooms):
                await self.room_manager.leave_room(room_id, connection_id)
        
        # Remove from tracking
        self.user_connections[connection.user.id].discard(connection_id)
        del self.connections[connection_id]
        
        # Update metrics
        connection_count.set(len(self.connections))
        connection_duration.observe(
            (datetime.utcnow() - connection.created_at).total_seconds()
        )
        
        # Close WebSocket
        try:
            await connection.websocket.close()
        except Exception as e:
            logging.debug(f"Error closing websocket for {connection_id}: {e}")
        
        logging.info(f"Connection {connection_id} disconnected")
    
    async def handle_message(self, connection_id: str, message: dict):
        """Handle incoming WebSocket message"""
        start_time = time.time()
        
        try:
            connection = self.connections.get(connection_id)
            if not connection:
                logging.warning(f"Received message for non-existent connection: {connection_id}")
                return
            
            # Rate limiting
            if self.rate_limiter and not await self.rate_limiter.is_allowed(
                connection.user.id, self.config.rate_limit_messages, self.config.rate_limit_window
            ):
                await self._send_error(connection, "Rate limit exceeded")
                error_count.labels(type="rate_limit").inc()
                return
            
            # Process message based on type
            msg_type = message.get("type")
            
            if msg_type == "subscribe":
                await self._handle_subscription(connection, message)
            elif msg_type == "join_room":
                await self._handle_join_room(connection, message)
            elif msg_type == "leave_room":
                await self._handle_leave_room(connection, message)
            elif msg_type == "ping":
                await self._handle_ping(connection)
            elif msg_type == "broadcast":
                await self._handle_broadcast(connection, message)
            else:
                await self._send_error(connection, f"Unknown message type: {msg_type}")
        
        except Exception as e:
            error_count.labels(type="message_handling").inc()
            logging.error(f"Error handling message for connection {connection_id}: {e}", exc_info=True)
        
        finally:
            message_latency.observe(time.time() - start_time)
    
    async def broadcast_to_room(self, room_id: str, message: Message):
        """Broadcast message to all connections in a room"""
        # Store persistent message
        if message.persistent and self.message_persistence:
            await self.message_persistence.store_message(message)
        
        if not self.room_manager: # Check if RoomManager is available
            logging.warning("RoomManager not available, cannot broadcast to room.")
            return

        # Get room members
        member_ids = await self.room_manager.get_room_members(room_id)
        
        # Send to connected members
        for connection_id in member_ids:
            if connection_id in self.connections:
                connection = self.connections[connection_id]
                # It's better to convert Message to dict for sending, then JSON.
                # Assuming _should_send_message expects Message object.
                if await self._should_send_message(connection, message):
                    await self._send_message_to_connection(connection, message)
            # else:
                # If you have a distributed system or queues for disconnected users, handle here
                # For now, it means connection is not currently live on this instance.
                # The MessageQueue and MessagePersistence handle messages for offline users.
        
        message_count.labels(type=message.type.value, room=room_id).inc()
    
    async def broadcast_to_user(self, user_id: str, message: Message):
        """Broadcast message to all connections of a user"""
        connection_ids = self.user_connections.get(user_id, set())
        
        if not connection_ids:
            # User not connected, queue message if queue is available
            if self.message_queue:
                await self.message_queue.enqueue(user_id, message)
            else:
                logging.warning(f"User {user_id} not connected and MessageQueue is not available to queue message.")
            return
        
        for connection_id in connection_ids:
            if connection_id in self.connections:
                connection = self.connections[connection_id]
                if await self._should_send_message(connection, message):
                    await self._send_message_to_connection(connection, message)
    
    async def _handle_subscription(self, connection: Connection, message: dict):
        """Handle subscription message"""
        try:
            subscription = Subscription.model_validate(message.get("data", {}))
            
            # Update subscriptions
            connection.subscriptions.clear()
            connection.subscriptions.update(subscription.message_types)
            
            # Join rooms
            if self.room_manager: # Check if RoomManager is available
                for room_id in subscription.rooms:
                    if await self.room_manager.join_room(room_id, connection.id, connection.user):
                        connection.rooms.add(room_id)
                    else:
                        logging.warning(f"Connection {connection.id} failed to join room {room_id} during subscription.")
            
            await self._send_message_to_connection(connection, Message(
                type=MessageType.SYSTEM,
                data={"message": "Subscription updated"}
            ))
        
        except ValidationError as e:
            error_count.labels(type="invalid_subscription").inc()
            await self._send_error(connection, f"Invalid subscription: {e}")
        except Exception as e:
            error_count.labels(type="handle_subscription_error").inc()
            logging.error(f"Error in _handle_subscription for {connection.id}: {e}", exc_info=True)


    async def _handle_join_room(self, connection: Connection, message: dict):
        """Handle join room message"""
        room_id = message.get("room_id")
        if not room_id:
            await self._send_error(connection, "Missing room_id")
            return
        
        if not self.room_manager: # Check if RoomManager is available
            await self._send_error(connection, "Room management service is unavailable.")
            return

        if await self.room_manager.join_room(room_id, connection.id, connection.user):
            connection.rooms.add(room_id)
            await self._send_message_to_connection(connection, Message(
                type=MessageType.SYSTEM,
                data={"message": f"Joined room {room_id}"}
            ))
        else:
            await self._send_error(connection, f"Failed to join room {room_id}")
    
    async def _handle_leave_room(self, connection: Connection, message: dict):
        """Handle leave room message"""
        room_id = message.get("room_id")
        if not room_id:
            await self._send_error(connection, "Missing room_id")
            return
        
        if not self.room_manager: # Check if RoomManager is available
            await self._send_error(connection, "Room management service is unavailable.")
            return

        await self.room_manager.leave_room(room_id, connection.id)
        connection.rooms.discard(room_id)
        
        await self._send_message_to_connection(connection, Message(
            type=MessageType.SYSTEM,
            data={"message": f"Left room {room_id}"}
        ))
    
    async def _handle_ping(self, connection: Connection):
        """Handle ping message"""
        connection.last_pong = time.time() # Update last_pong as a response to ping
        await self._send_message_to_connection(connection, Message(
            type=MessageType.HEARTBEAT,
            data={"type": "pong", "timestamp": time.time()}
        ))
    
    async def _handle_broadcast(self, connection: Connection, message: dict):
        """Handle broadcast message"""
        if not connection.user.has_permission("broadcast"):
            await self._send_error(connection, "Insufficient permissions")
            error_count.labels(type="permission_denied").inc()
            return
        
        try:
            broadcast_msg = Message.model_validate(message.get("data", {}))
            broadcast_msg.user_id = connection.user.id
            
            if broadcast_msg.room:
                await self.broadcast_to_room(broadcast_msg.room, broadcast_msg)
            else:
                await self._send_error(connection, "Room required for broadcast")
                error_count.labels(type="broadcast_no_room").inc()
        
        except ValidationError as e:
            error_count.labels(type="invalid_broadcast").inc()
            await self._send_error(connection, f"Invalid broadcast message: {e}")
        except Exception as e:
            error_count.labels(type="handle_broadcast_error").inc()
            logging.error(f"Error in _handle_broadcast for {connection.id}: {e}", exc_info=True)

    
    async def _send_message_to_connection(self, connection: Connection, message: Message):
        """Send message to a specific connection"""
        try:
            await connection.websocket.send_text(message.model_dump_json())
        except Exception as e:
            error_count.labels(type="send_message").inc()
            logging.error(f"Failed to send message to {connection.id}: {e}")
            await self.disconnect(connection.id) # Disconnect client on send error
    
    async def _send_error(self, connection: Connection, error_message: str):
        """Send error message to connection"""
        error_msg = Message(
            type=MessageType.ERROR,
            data={"error": error_message}
        )
        try:
            await self._send_message_to_connection(connection, error_msg)
        except Exception as e:
            logging.debug(f"Could not send error message to {connection.id}: {e}") # Log as debug to avoid cascading errors
    
    async def _should_send_message(self, connection: Connection, message: Message) -> bool:
        """Check if message should be sent to connection"""
        # Check subscription filters
        if connection.subscriptions and message.type not in connection.subscriptions:
            return False
        
        # Apply custom filters
        for filter_name, filter_func in self.message_filters.items():
            if not filter_func(connection, message):
                return False
        
        return True
    
    async def _check_rate_limit(self, connection: Connection) -> bool:
        """Check if connection is within rate limits (local fallback if RateLimiter is None)"""
        if self.rate_limiter:
            # Use self.config for WebSocket specific rate limit settings
            return await self.rate_limiter.is_allowed(
                connection.user.id, self.config.rate_limit_messages, self.config.rate_limit_window
            )
        else:
            # Fallback to in-memory rate limiting if Redis-based RateLimiter is not available
            current_time = time.time()
            if current_time > connection.rate_limit_reset:
                connection.rate_limit_count = 0
                connection.rate_limit_reset = current_time + self.config.rate_limit_window
            
            if connection.rate_limit_count >= self.config.rate_limit_messages:
                return False
            
            connection.rate_limit_count += 1
            return True
    
    async def _ping_connections(self):
        """Background task to ping connections"""
        while not self.shutdown_flag:
            try:
                current_time = time.time()
                
                # Iterate over a copy of connections to avoid modification during iteration
                for connection in list(self.connections.values()):
                    # Send ping if needed
                    if current_time - connection.last_ping > self.config.ping_interval:
                        ping_msg = Message(
                            type=MessageType.HEARTBEAT,
                            data={"type": "ping", "timestamp": current_time}
                        )
                        await self._send_message_to_connection(connection, ping_msg)
                        connection.last_ping = current_time
                    
                    # Check for stale connections (did not receive a pong back)
                    if current_time - connection.last_pong > self.config.ping_timeout:
                        logging.warning(f"Connection {connection.id} timed out due to no pong response.")
                        await self.disconnect(connection.id)
                
                await asyncio.sleep(self.config.ping_interval)
            
            except asyncio.CancelledError:
                logging.info("Ping task cancelled.")
                break # Exit loop cleanly
            except Exception as e:
                error_count.labels(type="ping_task").inc()
                logging.error(f"Error in ping task: {e}", exc_info=True)
                await asyncio.sleep(5) # Wait before retrying
    
    async def _cleanup_connections(self):
        """Background task to cleanup stale connections and empty room/user sets"""
        while not self.shutdown_flag:
            try:
                # Clean up empty user connection sets
                empty_users = [
                    user_id for user_id, connections in self.user_connections.items()
                    if not connections
                ]
                for user_id in empty_users:
                    del self.user_connections[user_id]
                
                # Clean up empty room connections (in-memory)
                if self.room_manager: # Check if RoomManager is available
                    for room_id in list(self.room_manager.room_connections.keys()):
                        if not self.room_manager.room_connections[room_id]:
                            del self.room_manager.room_connections[room_id]
                            # Optionally, if rooms are ephemeral and created/deleted dynamically,
                            # you might want to delete from Redis as well here:
                            # if self.redis:
                            #     await self.redis.delete(f"room:{room_id}")
                            #     await self.redis.delete(f"room_members:{room_id}")
                
                await asyncio.sleep(self.config.cleanup_interval)
            
            except asyncio.CancelledError:
                logging.info("Cleanup task cancelled.")
                break # Exit loop cleanly
            except Exception as e:
                error_count.labels(type="cleanup_task").inc()
                logging.error(f"Error in cleanup task: {e}", exc_info=True)
                await asyncio.sleep(60) # Wait before retrying
    
    def add_message_filter(self, name: str, filter_func: Callable[[Connection, Message], bool]):
        """Add a custom message filter"""
        self.message_filters[name] = filter_func
    
    def remove_message_filter(self, name: str):
        """Remove a custom message filter"""
        self.message_filters.pop(name, None)
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection statistics"""
        return {
            "total_connections": len(self.connections),
            "connections_by_user": {
                user_id: len(connections)
                for user_id, connections in self.user_connections.items()
            },
            "rooms": len(self.room_manager.rooms) if self.room_manager else 0, # Check if room_manager exists
            "room_connections": {
                room_id: len(connections)
                for room_id, connections in self.room_manager.room_connections.items()
            } if self.room_manager else {} # Check if room_manager exists
        }


# FastAPI Integration
def create_websocket_app(app_settings: AppSettings) -> FastAPI: # Type hint changed to AppSettings
    """Create FastAPI app with WebSocket support"""
    # Use websocket title from settings if available, fallback otherwise
    app = FastAPI(title=app_settings.api.title if hasattr(app_settings.api, 'title') else "WebSocket Analytics API")
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=app_settings.get_allowed_origins(), # Use global settings for CORS
        allow_credentials=True,
        allow_methods=app_settings.get_cors_methods(),    # Use global settings for CORS
        allow_headers=["*"],
    )
    
    # Initialize WebSocket manager with the full application settings
    ws_manager = WebSocketManager(app_settings) 
    
    @app.on_event("startup")
    async def startup_event():
        await ws_manager.startup()
    
    @app.on_event("shutdown")
    async def shutdown_event():
        await ws_manager.shutdown()
    
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket, token: str):
        await websocket.accept()
        
        try:
            # Authenticate user
            credentials = HTTPAuthorizationCredentials(
                scheme="Bearer",
                credentials=token
            )
            # Ensure ws_manager.auth is not None before calling
            if not ws_manager.auth:
                raise HTTPException(status_code=500, detail="Authentication service unavailable")
            user = await ws_manager.auth.authenticate(credentials)
            
            # Establish connection
            connection_id = await ws_manager.connect(websocket, user)
            
            # Handle messages
            while True:
                try:
                    data = await websocket.receive_text()
                    message = json.loads(data)
                    await ws_manager.handle_message(connection_id, message)
                except Exception as e:
                    logging.error(f"Error receiving message for {connection_id}: {e}", exc_info=True)
                    break # Break the loop to close connection
        
        except HTTPException as he:
            logging.warning(f"WebSocket auth/connection failed: {he.detail}")
            # Send specific error message to client before closing
            try:
                await websocket.send_text(json.dumps({"type": "error", "data": {"error": he.detail, "status_code": he.status_code}}))
                await websocket.close(code=he.status_code) # Close with appropriate code
            except Exception as close_e:
                logging.debug(f"Error sending error message or closing websocket on HTTPException: {close_e}")
        except Exception as e:
            logging.error(f"Unhandled WebSocket exception for connection {token}: {e}", exc_info=True)
            # Generic error response
            try:
                await websocket.send_text(json.dumps({"type": "error", "data": {"error": "Internal server error"}}))
                await websocket.close(code=status.WS_1011_INTERNAL_ERROR) # Internal error
            except Exception as close_e:
                logging.debug(f"Error sending error message or closing websocket on unhandled exception: {close_e}")
        finally:
            if 'connection_id' in locals():
                await ws_manager.disconnect(connection_id)
            elif 'websocket' in locals() and not websocket.client_disconnected:
                # If connection_id wasn't created but websocket is still open
                try:
                    await websocket.close()
                except Exception as e:
                    logging.debug(f"Error closing initial websocket on failed connection: {e}")

    @app.get("/stats")
    async def get_stats():
        if ws_manager:
            return ws_manager.get_connection_stats()
        raise HTTPException(status_code=503, detail="WebSocket manager not initialized")
    
    @app.post("/broadcast")
    async def broadcast_message(
        message: Message,
        user: User = Depends(ws_manager.auth.authenticate)
    ):
        if not user.has_permission("broadcast"):
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        if not ws_manager: # Check if ws_manager is available
            raise HTTPException(status_code=503, detail="WebSocket manager not initialized")

        if message.room:
            await ws_manager.broadcast_to_room(message.room, message)
        else:
            raise HTTPException(status_code=400, detail="Room required")
        
        return {"status": "sent", "message_id": message.id}


    # Add an endpoint to create/manage rooms if needed from HTTP API
    @app.post("/rooms")
    async def create_new_room(
        room_data: Room,
        user: User = Depends(ws_manager.auth.authenticate)
    ):
        if not user.has_permission("manage_rooms"): # Example permission
            raise HTTPException(status_code=403, detail="Insufficient permissions to manage rooms")
        
        if not ws_manager.room_manager:
            raise HTTPException(status_code=503, detail="Room manager not available")
        
        # You might want to override permissions/max_size based on user roles
        await ws_manager.room_manager.create_room(room_data)
        return {"status": "room created", "room_id": room_data.id}


    return app


# Usage Example (for direct execution of this file during development/testing)
if __name__ == "__main__":
    import uvicorn
    # Import get_settings from config.settings for standalone run
    from config.settings import get_settings
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get configuration using the global settings loader
    main_settings = get_settings() # This will load settings from .env or defaults
    
    # Create app, passing the global settings
    app = create_websocket_app(main_settings)
    
    # Run server
    uvicorn.run(
        app,
        host=main_settings.websocket.host, # Use settings from main config
        port=main_settings.websocket.port, # Use settings from main config
        log_level=(main_settings.logging.level.value if hasattr(main_settings.logging.level, 'value') else str(main_settings.logging.level)).lower()
    )