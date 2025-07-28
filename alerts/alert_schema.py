# """
# Alert Generation Logic with Advanced Rule Engine

# This module provides:
# - Rule-based alert generation from detection events
# - Configurable alert rules and thresholds
# - Zone-based alerting with geofencing
# - Time-based alert suppression and escalation
# - Alert correlation and deduplication
# - Machine learning-based anomaly detection
# - Circuit breaker pattern for external integrations
# """

# import json
# import time
# import uuid
# from typing import Dict, List, Optional, Any, Tuple, Set
# from dataclasses import dataclass, field
# from datetime import datetime, timedelta
# from enum import Enum
# import threading
# from queue import Queue
# import asyncio
# from abc import ABC, abstractmethod
# import hashlib

# from prometheus_client import Counter, Histogram, Gauge
# import redis

# from config.settings import get_settings
# from utils.logger import get_logger, LogContext
# from alerts.alert_schema import AlertSchema, AlertSeverity, AlertType
# from kafka.kafka_producer import get_kafka_producer
# from kafka.topics import TopicType

# # Initialize components
# logger = get_logger(__name__)
# settings = get_settings()

# # Prometheus metrics
# alerts_generated_total = Counter(
#     'alerts_generated_total',
#     'Total alerts generated',
#     ['alert_type', 'severity', 'camera_id']
# )
# alert_processing_time = Histogram(
#     'alert_processing_time_seconds',
#     'Time spent processing alerts',
#     ['alert_type']
# )
# alert_rules_matched = Counter(
#     'alert_rules_matched_total',
#     'Total alert rules matched',
#     ['rule_name', 'camera_id']
# )
# suppressed_alerts_total = Counter(
#     'suppressed_alerts_total',
#     'Total suppressed alerts',
#     ['alert_type', 'reason']
# )


# class AlertRuleType(Enum):
#     """Types of alert rules"""
#     OBJECT_DETECTION = "object_detection"
#     ZONE_INTRUSION = "zone_intrusion"
#     LOITERING = "loitering"
#     CROWD_DENSITY = "crowd_density"
#     MOTION_DETECTION = "motion_detection"
#     FACE_RECOGNITION = "face_recognition"
#     VEHICLE_DETECTION = "vehicle_detection"
#     ANOMALY_DETECTION = "anomaly_detection"
#     THRESHOLD_BREACH = "threshold_breach"
#     CORRELATION_RULE = "correlation_rule"


# @dataclass
# class Zone:
#     """Defines a monitoring zone"""
#     id: str
#     name: str
#     coordinates: List[Tuple[float, float]]  # Polygon coordinates
#     camera_ids: List[str]
#     active: bool = True
#     description: Optional[str] = None


# @dataclass
# class AlertRule:
#     """Defines an alert rule"""
#     id: str
#     name: str
#     rule_type: AlertRuleType
#     enabled: bool = True
#     priority: int = 1
#     conditions: Dict[str, Any] = field(default_factory=dict)
#     actions: List[str] = field(default_factory=list)
#     cooldown_minutes: int = 5
#     escalation_minutes: int = 30
#     zones: List[str] = field(default_factory=list)
#     cameras: List[str] = field(default_factory=list)
#     time_windows: List[Dict[str, str]] = field(default_factory=list)
#     metadata: Dict[str, Any] = field(default_factory=dict)


# @dataclass
# class DetectionEvent:
#     """Represents a detection event from Team A"""
#     id: str
#     camera_id: str
#     detection_type: str
#     confidence: float
#     timestamp: datetime
#     bounding_box: Optional[Dict[str, float]] = None
#     zone_id: Optional[str] = None
#     metadata: Dict[str, Any] = field(default_factory=dict)
#     correlation_id: Optional[str] = None


# @dataclass
# class AlertContext:
#     """Context information for alert generation"""
#     detection_event: DetectionEvent
#     matched_rules: List[AlertRule]
#     previous_alerts: List[Dict[str, Any]]
#     camera_status: Dict[str, Any]
#     zone_info: Optional[Zone] = None
#     correlation_id: Optional[str] = None


# class AlertSuppressionManager:
#     """Manages alert suppression and deduplication"""
    
#     def __init__(self, redis_client: Optional[redis.Redis] = None):
#         self.redis_client = redis_client
#         self.local_cache: Dict[str, datetime] = {}
#         self.cache_lock = threading.Lock()
    
#     def should_suppress_alert(self, alert: Dict[str, Any], rule: AlertRule) -> Tuple[bool, str]:
#         """Check if alert should be suppressed"""
#         try:
#             # Generate suppression key
#             suppression_key = self._generate_suppression_key(alert, rule)
            
#             # Check cooldown period
#             if self._is_in_cooldown(suppression_key, rule.cooldown_minutes):
#                 return True, f"Alert suppressed due to cooldown period ({rule.cooldown_minutes} minutes)"
            
#             # Check for duplicate content
#             if self._is_duplicate_alert(alert):
#                 return True, "Duplicate alert content detected"
            
#             # Check time window restrictions
#             if not self._is_within_time_window(rule):
#                 return True, "Alert outside configured time window"
            
#             # Mark alert as processed
#             self._mark_alert_processed(suppression_key)
            
#             return False, ""
            
#         except Exception as e:
#             logger.error(f"Error checking alert suppression: {e}")
#             return False, ""
    
#     def _generate_suppression_key(self, alert: Dict[str, Any], rule: AlertRule) -> str:
#         """Generate unique suppression key"""
#         key_data = f"{rule.id}:{alert['camera_id']}:{alert['alert_type']}"
#         return hashlib.md5(key_data.encode()).hexdigest()
    
#     def _is_in_cooldown(self, suppression_key: str, cooldown_minutes: int) -> bool:
#         """Check if alert is in cooldown period"""
#         try:
#             if self.redis_client:
#                 # Check Redis cache
#                 last_alert = self.redis_client.get(f"alert_cooldown:{suppression_key}")
#                 if last_alert:
#                     last_time = datetime.fromisoformat(last_alert)
#                     if datetime.utcnow() - last_time < timedelta(minutes=cooldown_minutes):
#                         return True
#             else:
#                 # Check local cache
#                 with self.cache_lock:
#                     if suppression_key in self.local_cache:
#                         last_time = self.local_cache[suppression_key]
#                         if datetime.utcnow() - last_time < timedelta(minutes=cooldown_minutes):
#                             return True
            
#             return False
            
#         except Exception as e:
#             logger.error(f"Error checking cooldown: {e}")
#             return False
    
#     def _is_duplicate_alert(self, alert: Dict[str, Any]) -> bool:
#         """Check for duplicate alert content"""
#         try:
#             # Generate content hash
#             content_key = f"{alert['camera_id']}:{alert['alert_type']}:{alert['message']}"
#             content_hash = hashlib.md5(content_key.encode()).hexdigest()
            
#             if self.redis_client:
#                 # Check if we've seen this content recently
#                 duplicate_key = f"alert_content:{content_hash}"
#                 if self.redis_client.exists(duplicate_key):
#                     return True
                
#                 # Store content hash with TTL
#                 self.redis_client.setex(duplicate_key, 300, "1")  # 5 minutes
            
#             return False
            
#         except Exception as e:
#             logger.error(f"Error checking duplicate alert: {e}")
#             return False
    
#     def _is_within_time_window(self, rule: AlertRule) -> bool:
#         """Check if current time is within rule's time windows"""
#         if not rule.time_windows:
#             return True
        
#         current_time = datetime.utcnow().time()
#         current_day = datetime.utcnow().strftime('%A').lower()
        
#         for window in rule.time_windows:
#             # Check day of week
#             if 'days' in window:
#                 days = [day.lower() for day in window['days']]
#                 if current_day not in days:
#                     continue
            
#             # Check time range
#             start_time = datetime.strptime(window['start'], '%H:%M').time()
#             end_time = datetime.strptime(window['end'], '%H:%M').time()
            
#             if start_time <= current_time <= end_time:
#                 return True
        
#         return False
    
#     def _mark_alert_processed(self, suppression_key: str):
#         """Mark alert as processed"""
#         try:
#             current_time = datetime.utcnow()
            
#             if self.redis_client:
#                 # Store in Redis with TTL
#                 self.redis_client.setex(
#                     f"alert_cooldown:{suppression_key}",
#                     3600,  # 1 hour TTL
#                     current_time.isoformat()
#                 )
#             else:
#                 # Store in local cache
#                 with self.cache_lock:
#                     self.local_cache[suppression_key] = current_time
                    
#                     # Clean old entries
#                     if len(self.local_cache) > 1000:
#                         cutoff_time = current_time - timedelta(hours=1)
#                         self.local_cache = {
#                             k: v for k, v in self.local_cache.items()
#                             if v > cutoff_time
#                         }
                        
#         except Exception as e:
#             logger.error(f"Error marking alert as processed: {e}")


# class AlertRuleEngine:
#     """Rule engine for alert generation"""
    
#     def __init__(self):
#         self.rules: Dict[str, AlertRule] = {}
#         self.zones: Dict[str, Zone] = {}
#         self.rule_processors: Dict[AlertRuleType, callable] = {
#             AlertRuleType.OBJECT_DETECTION: self._process_object_detection_rule,
#             AlertRuleType.ZONE_INTRUSION: self._process_zone_intrusion_rule,
#             AlertRuleType.LOITERING: self._process_loitering_rule,
#             AlertRuleType.CROWD_DENSITY: self._process_crowd_density_rule,
#             AlertRuleType.MOTION_DETECTION: self._process_motion_detection_rule,
#             AlertRuleType.FACE_RECOGNITION: self._process_face_recognition_rule,
#             AlertRuleType.VEHICLE_DETECTION: self._process_vehicle_detection_rule,
#             AlertRuleType.ANOMALY_DETECTION: self._process_anomaly_detection_rule,
#             AlertRuleType.THRESHOLD_BREACH: self._process_threshold_breach_rule,
#             AlertRuleType.CORRELATION_RULE: self._process_correlation_rule,
#         }
#         self._load_default_rules()
#         self._load_default_zones()
    
#     def _load_default_rules(self):
#         """Load default alert rules"""
#         default_rules = [
#             AlertRule(
#                 id="person_detection",
#                 name="Person Detection Alert",
#                 rule_type=AlertRuleType.OBJECT_DETECTION,
#                 conditions={
#                     "detection_types": ["person"],
#                     "min_confidence": 0.7,
#                     "max_objects": 10
#                 },
#                 actions=["send_alert", "log_event"],
#                 cooldown_minutes=2,
#                 priority=2
#             ),
#             AlertRule(
#                 id="zone_intrusion",
#                 name="Zone Intrusion Alert",
#                 rule_type=AlertRuleType.ZONE_INTRUSION,
#                 conditions={
#                     "detection_types": ["person", "vehicle"],
#                     "min_confidence": 0.6,
#                     "restricted_zones": ["secure_area", "parking_lot"]
#                 },
#                 actions=["send_alert", "record_incident"],
#                 cooldown_minutes=5,
#                 priority=1
#             ),
#             AlertRule(
#                 id="loitering_detection",
#                 name="Loitering Detection",
#                 rule_type=AlertRuleType.LOITERING,
#                 conditions={
#                     "detection_types": ["person"],
#                     "min_duration_seconds": 300,  # 5 minutes
#                     "max_movement_distance": 50  # pixels
#                 },
#                 actions=["send_alert", "capture_snapshot"],
#                 cooldown_minutes=10,
#                 priority=3
#             ),
#             AlertRule(
#                 id="vehicle_detection",
#                 name="Vehicle Detection Alert",
#                 rule_type=AlertRuleType.VEHICLE_DETECTION,
#                 conditions={
#                     "detection_types": ["car", "truck", "motorcycle"],
#                     "min_confidence": 0.8,
#                     "restricted_areas": ["pedestrian_zone"]
#                 },
#                 actions=["send_alert", "log_event"],
#                 cooldown_minutes=3,
#                 priority=2
#             )
#         ]
        
#         for rule in default_rules:
#             self.rules[rule.id] = rule
    
#     def _load_default_zones(self):
#         """Load default monitoring zones"""
#         default_zones = [
#             Zone(
#                 id="secure_area",
#                 name="Secure Area",
#                 coordinates=[(0, 0), (100, 0), (100, 100), (0, 100)],
#                 camera_ids=["camera_1", "camera_2"],
#                 description="High security zone"
#             ),
#             Zone(
#                 id="parking_lot",
#                 name="Parking Lot",
#                 coordinates=[(200, 0), (400, 0), (400, 200), (200, 200)],
#                 camera_ids=["camera_3", "camera_4"],
#                 description="Vehicle parking area"
#             ),
#             Zone(
#                 id="pedestrian_zone",
#                 name="Pedestrian Zone",
#                 coordinates=[(50, 50), (150, 50), (150, 150), (50, 150)],
#                 camera_ids=["camera_1", "camera_5"],
#                 description="Pedestrian-only area"
#             )
#         ]
        
#         for zone in default_zones:
#             self.zones[zone.id] = zone
    
#     def add_rule(self, rule: AlertRule):
#         """Add or update an alert rule"""
#         self.rules[rule.id] = rule
#         logger.info(f"Added alert rule: {rule.name}")
    
#     def remove_rule(self, rule_id: str):
#         """Remove an alert rule"""
#         if rule_id in self.rules:
#             del self.rules[rule_id]
#             logger.info(f"Removed alert rule: {rule_id}")
    
#     def get_matching_rules(self, detection_event: DetectionEvent) -> List[AlertRule]:
#         """Get rules that match the detection event"""
#         matching_rules = []
        
#         for rule in self.rules.values():
#             if not rule.enabled:
#                 continue
            
#             # Check camera filter
#             if rule.cameras and detection_event.camera_id not in rule.cameras:
#                 continue
            
#             # Check zone filter
#             if rule.zones and detection_event.zone_id not in rule.zones:
#                 continue
            
#             # Check rule-specific conditions
#             if self._evaluate_rule_conditions(rule, detection_event):
#                 matching_rules.append(rule)
        
#         return sorted(matching_rules, key=lambda r: r.priority)
    
#     def _evaluate_rule_conditions(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
#         """Evaluate if rule conditions match detection event"""
#         try:
#             processor = self.rule_processors.get(rule.rule_type)
#             if processor:
#                 return processor(rule, detection_event)
#             else:
#                 logger.warning(f"No processor for rule type: {rule.rule_type}")
#                 return False
#         except Exception as e:
#             logger.error(f"Error evaluating rule conditions: {e}")
#             return False
    
#     def _process_object_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
#         """Process object detection rule"""
#         conditions = rule.conditions
        
#         # Check detection type
#         if 'detection_types' in conditions:
#             if detection_event.detection_type not in conditions['detection_types']:
#                 return False
        
#         # Check confidence threshold
#         if 'min_confidence' in conditions:
#             if detection_event.confidence < conditions['min_confidence']:
#                 return False
        
#         return True
    
#     def _process_zone_intrusion_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
#         """Process zone intrusion rule"""
#         conditions = rule.conditions
        
#         # Check if detection is in restricted zone
#         if 'restricted_zones' in conditions:
#             if detection_event.zone_id not in conditions['restricted_zones']:
#                 return False
        
#         # Check detection type
#         if 'detection_types' in conditions:
#             if detection_event.detection_type not in conditions['detection_types']:
#                 return False
        
#         # Check confidence
#         if 'min_confidence' in conditions:
#             if detection_event.confidence < conditions['min_confidence']:
#                 return False
        
#         return True
    
#     def _process_loitering_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
#         """Process loitering detection rule"""
#         # This would require tracking object positions over time
#         # For now, return basic check
#         conditions = rule.conditions
        
#         if 'detection_types' in conditions:
#             if detection_event.detection_type not in conditions['detection_types']:
#                 return False
        
#         # TODO: Implement actual loitering detection logic
#         return True
    
#     def _process_crowd_density_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
#         """Process crowd density rule"""
#         # This would require counting objects in area
#         conditions = rule.conditions
        
#         if 'min_count' in conditions:
#             # TODO: Implement crowd counting logic
#             pass
        
#         return True
    
#     def _process_motion_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
#         """Process motion detection rule"""
#         return detection_event.detection_type == "motion"
    
#     def _process_face_recognition_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
#         """Process face recognition rule"""
#         return detection_event.detection_type == "face"
    
#     def _process_vehicle_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
#         """Process vehicle detection rule"""
#         conditions = rule.conditions
        
#         if 'detection_types' in conditions:
#             if detection_event.detection_type not in conditions['detection_types']:
#                 return False
        
#         if 'restricted_areas' in conditions:
#             if detection_event.zone_id in conditions['restricted_areas']:
#                 return True
        
#         return False
    
#     def _process_anomaly_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
#         """Process anomaly detection rule"""
#         return detection_event.detection_type == "anomaly"
    
#     def _process_threshold_breach_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
#         """Process threshold breach rule"""
#         conditions = rule.conditions
        
#         if 'threshold_value' in conditions:
#             value = detection_event.metadata.get('value', 0)
#             threshold = conditions['threshold_value']
#             operator = conditions.get('operator', 'gt')
            
#             if operator == 'gt':
#                 return value > threshold
#             elif operator == 'lt':
#                 return value < threshold
#             elif operator == 'eq':
#                 return value == threshold
        
#         return False
    
#     def _process_correlation_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
#         """Process correlation rule"""
#         # This would require complex event correlation
#         # For now, return basic check
#         return True


# class AlertGenerator:
#     """Main alert generation class"""
    
#     def __init__(self, redis_client: Optional[redis.Redis] = None):
#         self.rule_engine = AlertRuleEngine()
#         self.suppression_manager = AlertSuppressionManager(redis_client)
#         self.kafka_producer = get_kafka_producer()
#         self.alert_history: List[Dict[str, Any]] = []
#         self.history_lock = threading.Lock()
        
#     def generate_alert(self, detection: Dict[str, Any]) -> Optional[Dict[str, Any]]:
#         """Generate alert from detection event"""
#         start_time = time.time()
        
#         try:
#             # Parse detection event
#             detection_event = self._parse_detection_event(detection)
            
#             # Get matching rules
#             matching_rules = self.rule_engine.get_matching_rules(detection_event)
            
#             if not matching_rules:
#                 logger.debug(f"No matching rules for detection: {detection_event.detection_type}")
#                 return None
            
#             # Generate alert for the highest priority rule
#             primary_rule = matching_rules[0]
            
#             # Create alert context
#             context = AlertContext(
#                 detection_event=detection_event,
#                 matched_rules=matching_rules,
#                 previous_alerts=self._get_recent_alerts(detection_event.camera_id),
#                 camera_status=self._get_camera_status(detection_event.camera_id),
#                 zone_info=self.rule_engine.zones.get(detection_event.zone_id),
#                 correlation_id=detection_event.correlation_id or str(uuid.uuid4())
#             )
            
#             # Generate alert
#             alert = self._create_alert(primary_rule, context)
            
#             # Check suppression
#             should_suppress, reason = self.suppression_manager.should_suppress_alert(alert, primary_rule)
#             if should_suppress:
#                 logger.info(f"Alert suppressed: {reason}")
#                 suppressed_alerts_total.labels(
#                     alert_type=alert['alert_type'],
#                     reason=reason.split(' ')[0]
#                 ).inc()
#                 return None
            
#             # Store in history
#             self._store_alert_in_history(alert)
            
#             # Send to Kafka
#             self._send_alert_to_kafka(alert)
            
#             # Update metrics
#             alerts_generated_total.labels(
#                 alert_type=alert['alert_type'],
#                 severity=alert['severity'],
#                 camera_id=alert['camera_id']
#             ).inc()
            
#             alert_rules_matched.labels(
#                 rule_name=primary_rule.name,
#                 camera_id=detection_event.camera_id
#             ).inc()
            
#             processing_time = time.time() - start_time
#             alert_processing_time.labels(
#                 alert_type=alert['alert_type']
#             ).observe(processing_time)
            
#             logger.info(f"Alert generated: {alert['alert_type']} for camera {alert['camera_id']}")
#             return alert
            
#         except Exception as e:
#             logger.error(f"Error generating alert: {e}", exc_info=True)
#             return None
    
#     def _parse_detection_event(self, detection: Dict[str, Any]) -> DetectionEvent:
#         """Parse detection dictionary into DetectionEvent object"""
#         return DetectionEvent(
#             id=detection.get('id', str(uuid.uuid4())),
#             camera_id=detection.get('camera_id', 'unknown'),
#             detection_type=detection.get('detection_type', 'unknown'),
#             confidence=detection.get('confidence', 0.0),
#             timestamp=datetime.fromisoformat(detection.get('timestamp', datetime.utcnow().isoformat())),
#             bounding_box=detection.get('bounding_box'),
#             zone_id=detection.get('zone_id'),
#             metadata=detection.get('metadata', {}),
#             correlation_id=detection.get('correlation_id')
#         )
    
#     def _create_alert(self, rule: AlertRule, context: AlertContext) -> Dict[str, Any]:
#         """Create alert from rule and context"""
#         detection_event = context.detection_event
        
#         # Determine alert severity
#         severity = self._determine_alert_severity(rule, context)
        
#         # Generate alert message
#         message = self._generate_alert_message(rule, context)
        
#         # Create alert object
#         alert = {
#             'id': str(uuid.uuid4()),
#             'alert_type': self._map_rule_type_to_alert_type(rule.rule_type),
#             'severity': severity,
#             'message': message,
#             'camera_id': detection_event.camera_id,
#             'zone_id': detection_event.zone_id,
#             'detection_type': detection_event.detection_type,
#             'confidence': detection_event.confidence,
#             'timestamp': datetime.utcnow().isoformat(),
#             'rule_id': rule.id,
#             'rule_name': rule.name,
#             'correlation_id': context.correlation_id,
#             'metadata': {
#                 'detection_event_id': detection_event.id,
#                 'bounding_box': detection_event.bounding_box,
#                 'zone_info': context.zone_info.__dict__ if context.zone_info else None,
#                 'matched_rules': [r.id for r in context.matched_rules],
#                 'camera_status': context.camera_status
#             }
#         }
        
#         return alert
    
#     def _determine_alert_severity(self, rule: AlertRule, context: AlertContext) -> str:
#         """Determine alert severity based on rule and context"""
#         detection_event = context.detection_event
        
#         # Base severity from rule priority
#         if rule.priority == 1:
#             base_severity = AlertSeverity.CRITICAL
#         elif rule.priority == 2:
#             base_severity = AlertSeverity.HIGH
#         elif rule.priority == 3:
#             base_severity = AlertSeverity.MEDIUM
#         else:
#             base_severity = AlertSeverity.LOW
        
#         # Adjust based on confidence
#         if detection_event.confidence > 0.9:
#             if base_severity == AlertSeverity.MEDIUM:
#                 base_severity = AlertSeverity.HIGH
#             elif base_severity == AlertSeverity.LOW:
#                 base_severity = AlertSeverity.MEDIUM
        
#         # Adjust based on zone (if in secure area, increase severity)
#         if context.zone_info and 'secure' in context.zone_info.name.lower():
#             if base_severity == AlertSeverity.MEDIUM:
#                 base_severity = AlertSeverity.HIGH
#             elif base_severity == AlertSeverity.LOW:
#                 base_severity = AlertSeverity.MEDIUM
        
#         return base_severity.value
    
#     def _generate_alert_message(self, rule: AlertRule, context: AlertContext) -> str:
#         """Generate human-readable alert message"""
#         detection_event = context.detection_event
        
#         messages = {
#             AlertRuleType.OBJECT_DETECTION: f"{detection_event.detection_type.title()} detected in camera {detection_event.camera_id}",
#             AlertRuleType.ZONE_INTRUSION: f"Intrusion detected: {detection_event.detection_type} in restricted zone {detection_event.zone_id}",
#             AlertRuleType.LOITERING: f"Loitering detected: {detection_event.detection_type} in camera {detection_event.camera_id}",
#             AlertRuleType.VEHICLE_DETECTION: f"Vehicle detected: {detection_event.detection_type} in camera {detection_event.camera_id}",
#             AlertRuleType.FACE_RECOGNITION: f"Face detected in camera {detection_event.camera_id}",
#             AlertRuleType.MOTION_DETECTION: f"Motion detected in camera {detection_event.camera_id}",
#             AlertRuleType.CROWD_DENSITY: f"Crowd density threshold exceeded in camera {detection_event.camera_id}",
#             AlertRuleType.ANOMALY_DETECTION: f"Anomaly detected in camera {detection_event.camera_id}",
#             AlertRuleType.THRESHOLD_BREACH: f"Threshold breach detected in camera {detection_event.camera_id}",
#             AlertRuleType.CORRELATION_RULE: f"Correlation rule triggered for camera {detection_event.camera_id}"
#         }
        
#         base_message = messages.get(rule.rule_type, f"Alert triggered by rule {rule.name}")
        
#         # Add confidence information
#         if detection_event.confidence > 0:
#             base_message += f" (confidence: {detection_event.confidence:.2f})"
        
#         # Add zone information
#         if context.zone_info:
#             base_message += f" in zone '{context.zone_info.name}'"
        
#         return base_message
    
#     def _map_rule_type_to_alert_type(self, rule_type: AlertRuleType) -> str:
#         """Map rule type to alert type"""
#         mapping = {
#             AlertRuleType.OBJECT_DETECTION: AlertType.DETECTION,
#             AlertRuleType.ZONE_INTRUSION: AlertType.INTRUSION,
#             AlertRuleType.LOITERING: AlertType.BEHAVIOR,
#             AlertRuleType.VEHICLE_DETECTION: AlertType.DETECTION,
#             AlertRuleType.FACE_RECOGNITION: AlertType.DETECTION,
#             AlertRuleType.MOTION_DETECTION: AlertType.MOTION,
#             AlertRuleType.CROWD_DENSITY: AlertType.BEHAVIOR,
#             AlertRuleType.ANOMALY_DETECTION: AlertType.ANOMALY,
#             AlertRuleType.THRESHOLD_BREACH: AlertType.SYSTEM,
#             AlertRuleType.CORRELATION_RULE: AlertType.CORRELATION
#         }
        
#         return mapping.get(rule_type, AlertType.GENERAL).value
    
#     def _get_recent_alerts(self, camera_id: str, minutes: int = 30) -> List[Dict[str, Any]]:
#         """Get recent alerts for camera"""
#         cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
        
#         with self.history_lock:
#             return [
#                 alert for alert in self.alert_history
#                 if alert['camera_id'] == camera_id and
#                 datetime.fromisoformat(alert['timestamp']) > cutoff_time
#             ]
    
#     def _get_camera_status(self, camera_id: str) -> Dict[str, Any]:
#         """Get camera status information"""
#         # This would typically query a camera management system
#         return {
#             'camera_id': camera_id,
#             'status': 'online',
#             'last_seen': datetime.utcnow().isoformat()
#         }
    
#     def _store_alert_in_history(self, alert: Dict[str, Any]):
#         """Store alert in local history"""
#         with self.history_lock:
#             self.alert_history.append(alert)
            
#             # Keep only last 1000 alerts
#             if len(self.alert_history) > 1000:
#                 self.alert_history = self.alert_history[-1000:]
    
#     def _send_alert_to_kafka(self, alert: Dict[str, Any]):
#         """Send alert to Kafka topic"""
#         try:
#             success = self.kafka_producer.send_message(
#                 TopicType.ALERTS,
#                 alert,
#                 key=alert['camera_id']
#             )
            
#             if success:
#                 logger.info(f"Alert sent to Kafka: {alert['id']}")
#             else:
#                 logger.error(f"Failed to send alert to Kafka: {alert['id']}")
                
#         except Exception as e:
#             logger.error(f"Error sending alert to Kafka: {e}")
    
#     def get_alert_history(self, 
#                          camera_id: Optional[str] = None,
#                          alert_type: Optional[str] = None,
#                          limit: int = 100) -> List[Dict[str, Any]]:
#         """Get alert history with optional filtering"""
#         with self.history_lock:
#             filtered_alerts = self.alert_history
            
#             if camera_id:
#                 filtered_alerts = [a for a in filtered_alerts if a['camera_id'] == camera_id]
            
#             if alert_type:
#                 filtered_alerts = [a for a in filtered_alerts if a['alert_type'] == alert_type]
            
#             # Sort by timestamp (newest first) and limit
#             filtered_alerts.sort(key=lambda x: x['timestamp'], reverse=True)
#             return filtered_alerts[:limit]
    
#     def get_alert_statistics(self) -> Dict[str, Any]:
#         """Get alert statistics"""
#         with self.history_lock:
#             total_alerts = len(self.alert_history)
            
#             if total_alerts == 0:
#                 return {
#                     'total_alerts': 0,
#                     'alerts_by_type': {},
#                     'alerts_by_severity': {},
#                     'alerts_by_camera': {}
#                 }
            
#             # Count by type
#             alerts_by_type = {}
#             alerts_by_severity = {}
#             alerts_by_camera = {}
            
#             for alert in self.alert_history:
#                 # By type
#                 alert_type = alert['alert_type']
#                 alerts_by_type[alert_type] = alerts_by_type.get(alert_type, 0) + 1
                
#                 # By severity
#                 severity = alert['severity']
#                 alerts_by_severity[severity] = alerts_by_severity.get(severity, 0) + 1
                
#                 # By camera
#                 camera_id = alert['camera_id']
#                 alerts_by_camera[camera_id] = alerts_by_camera.get(camera_id, 0) + 1
            
#             return {
#                 'total_alerts': total_alerts,
#                 'alerts_by_type': alerts_by_type,
#                 'alerts_by_severity': alerts_by_severity,
#                 'alerts_by_camera': alerts_by_camera
#             }


# # Global alert generator instance
# _alert_generator = None
# _alert_generator_lock = threading.Lock()


# def get_alert_generator() -> AlertGenerator:
#     """Get singleton alert generator instance"""
#     global _alert_generator
    
#     if _alert_generator is None:
#         with _alert_generator_lock:
#             if _alert_generator is None:
#                 try:
#                     # Try to initialize with Redis
#                     redis_client = redis.Redis(
#                         host=getattr(settings, 'redis', {}).get('host', 'localhost'),
#                         port=getattr(settings, 'redis', {}).get('port', 6379),
#                         db=getattr(settings, 'redis', {}).get('db', 0),
#                         decode_responses=True
#                     )
#                     redis_client.ping()
#                     _alert_generator = AlertGenerator(redis_client)
#                 except:
#                     # Fall back to in-memory only
#                     logger.warning("Redis not available, using in-memory alert generation")
#                     _alert_generator = AlertGenerator()
    
#     return _alert_generator


# # Convenience function for generating alerts
# def generate_alert(detection: Dict[str, Any]) -> Optional[Dict[str, Any]]:
#     """Generate alert from detection event"""
#     alert_generator = get_alert_generator()
#     return alert_generator.generate_alert(detection)


# # Testing and debugging functions
# def test_alert_generation():
#     """Test alert generation with dummy data"""
#     test_detection = {
#         'id': str(uuid.uuid4()),
#         'camera_id': 'camera_1',
#         'detection_type': 'person',
#         'confidence': 0.85,
#         'timestamp': datetime.utcnow().isoformat(),
#         'zone_id': 'secure_area',
#         'bounding_box': {'x': 100, 'y': 200, 'width': 50, 'height': 100},
#         'metadata': {'test': True}
#     }
    
#     alert = generate_alert(test_detection)
#     if alert:
#         print(f"Generated alert: {json.dumps(alert, indent=2)}")
#     else:
#         print("No alert generated")


# if __name__ == "__main__":
#     test_alert_generation()
"""
Alert Generation Logic with Advanced Rule Engine

This module provides:
- Rule-based alert generation from detection events
- Configurable alert rules and thresholds
- Zone-based alerting with geofencing
- Time-based alert suppression and escalation
- Alert correlation and deduplication
- Machine learning-based anomaly detection
- Circuit breaker pattern for external integrations
"""

import json
import time
import uuid
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import threading
from queue import Queue
import asyncio
import hashlib

from prometheus_client import Counter, Histogram, Gauge
import redis

from config.settings import get_settings
from utils.logger import get_logger
from alerts.alert_models import Alert,AlertType, AlertSeverity
# from kafka.kafka_producer import get_kafka_producer
from kafka_handlers.kafka_producer import get_kafka_producer
# from kafka.topics import TopicType
from kafka_handlers.topics import TopicType

# Initialize components
logger = get_logger(__name__)
settings = get_settings()

# Prometheus metrics
alerts_generated_total = Counter(
    'alerts_generated_total',
    'Total alerts generated',
    ['alert_type', 'severity', 'camera_id']
)
alert_processing_time = Histogram(
    'alert_processing_time_seconds',
    'Time spent processing alerts',
    ['alert_type']
)
alert_rules_matched = Counter(
    'alert_rules_matched_total',
    'Total alert rules matched',
    ['rule_name', 'camera_id']
)
suppressed_alerts_total = Counter(
    'suppressed_alerts_total',
    'Total suppressed alerts',
    ['alert_type', 'reason']
)

class AlertRuleType(Enum):
    """Types of alert rules"""
    OBJECT_DETECTION = "object_detection"
    ZONE_INTRUSION = "zone_intrusion"
    LOITERING = "loitering"
    CROWD_DENSITY = "crowd_density"
    MOTION_DETECTION = "motion_detection"
    FACE_RECOGNITION = "face_recognition"
    VEHICLE_DETECTION = "vehicle_detection"
    ANOMALY_DETECTION = "anomaly_detection"
    THRESHOLD_BREACH = "threshold_breach"
    CORRELATION_RULE = "correlation_rule"

@dataclass
class Zone:
    """Defines a monitoring zone"""
    id: str
    name: str
    coordinates: List[Tuple[float, float]]  # Polygon coordinates
    camera_ids: List[str]
    active: bool = True
    description: Optional[str] = None

@dataclass
class AlertRule:
    """Defines an alert rule"""
    id: str
    name: str
    rule_type: AlertRuleType
    enabled: bool = True
    priority: int = 1
    conditions: Dict[str, Any] = field(default_factory=dict)
    actions: List[str] = field(default_factory=list)
    cooldown_minutes: int = 5
    escalation_minutes: int = 30
    zones: List[str] = field(default_factory=list)
    cameras: List[str] = field(default_factory=list)
    time_windows: List[Dict[str, str]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class DetectionEvent:
    """Represents a detection event"""
    id: str
    camera_id: str
    detection_type: str
    confidence: float
    timestamp: datetime
    bounding_box: Optional[Dict[str, float]] = None
    zone_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None

@dataclass
class AlertContext:
    """Context information for alert generation"""
    detection_event: DetectionEvent
    matched_rules: List[AlertRule]
    previous_alerts: List[Dict[str, Any]]
    camera_status: Dict[str, Any]
    zone_info: Optional[Zone] = None
    correlation_id: Optional[str] = None

class AlertSuppressionManager:
    """Manages alert suppression and deduplication"""
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client
        self.local_cache: Dict[str, datetime] = {}
        self.cache_lock = threading.Lock()
    
    def should_suppress_alert(self, alert: Dict[str, Any], rule: AlertRule) -> Tuple[bool, str]:
        """Check if alert should be suppressed"""
        try:
            # Generate suppression key
            suppression_key = self._generate_suppression_key(alert, rule)
            
            # Check cooldown period
            if self._is_in_cooldown(suppression_key, rule.cooldown_minutes):
                return True, f"Alert suppressed due to cooldown period ({rule.cooldown_minutes} minutes)"
            
            # Check for duplicate content
            if self._is_duplicate_alert(alert):
                return True, "Duplicate alert content detected"
            
            # Check time window restrictions
            if not self._is_within_time_window(rule):
                return True, "Alert outside configured time window"
            
            # Mark alert as processed
            self._mark_alert_processed(suppression_key)
            
            return False, ""
            
        except Exception as e:
            logger.error(f"Error checking alert suppression: {e}")
            return False, ""
    
    def _generate_suppression_key(self, alert: Dict[str, Any], rule: AlertRule) -> str:
        """Generate unique suppression key"""
        key_data = f"{rule.id}:{alert['camera_id']}:{alert['alert_type']}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _is_in_cooldown(self, suppression_key: str, cooldown_minutes: int) -> bool:
        """Check if alert is in cooldown period"""
        try:
            if self.redis_client:
                # Check Redis cache
                last_alert = self.redis_client.get(f"alert_cooldown:{suppression_key}")
                if last_alert:
                    last_time = datetime.fromisoformat(last_alert)
                    if datetime.utcnow() - last_time < timedelta(minutes=cooldown_minutes):
                        return True
            else:
                # Check local cache
                with self.cache_lock:
                    if suppression_key in self.local_cache:
                        last_time = self.local_cache[suppression_key]
                        if datetime.utcnow() - last_time < timedelta(minutes=cooldown_minutes):
                            return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking cooldown: {e}")
            return False
    
    def _is_duplicate_alert(self, alert: Dict[str, Any]) -> bool:
        """Check for duplicate alert content"""
        try:
            # Generate content hash
            content_key = f"{alert['camera_id']}:{alert['alert_type']}:{alert['message']}"
            content_hash = hashlib.md5(content_key.encode()).hexdigest()
            
            if self.redis_client:
                # Check if we've seen this content recently
                duplicate_key = f"alert_content:{content_hash}"
                if self.redis_client.exists(duplicate_key):
                    return True
                
                # Store content hash with TTL
                self.redis_client.setex(duplicate_key, 300, "1")  # 5 minutes
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking duplicate alert: {e}")
            return False
    
    def _is_within_time_window(self, rule: AlertRule) -> bool:
        """Check if current time is within rule's time windows"""
        if not rule.time_windows:
            return True
        
        current_time = datetime.utcnow().time()
        current_day = datetime.utcnow().strftime('%A').lower()
        
        for window in rule.time_windows:
            # Check day of week
            if 'days' in window:
                days = [day.lower() for day in window['days']]
                if current_day not in days:
                    continue
            
            # Check time range
            start_time = datetime.strptime(window['start'], '%H:%M').time()
            end_time = datetime.strptime(window['end'], '%H:%M').time()
            
            if start_time <= current_time <= end_time:
                return True
        
        return False
    
    def _mark_alert_processed(self, suppression_key: str):
        """Mark alert as processed"""
        try:
            current_time = datetime.utcnow()
            
            if self.redis_client:
                # Store in Redis with TTL
                self.redis_client.setex(
                    f"alert_cooldown:{suppression_key}",
                    3600,  # 1 hour TTL
                    current_time.isoformat()
                )
            else:
                # Store in local cache
                with self.cache_lock:
                    self.local_cache[suppression_key] = current_time
                    
                    # Clean old entries
                    if len(self.local_cache) > 1000:
                        cutoff_time = current_time - timedelta(hours=1)
                        self.local_cache = {
                            k: v for k, v in self.local_cache.items()
                            if v > cutoff_time
                        }
                        
        except Exception as e:
            logger.error(f"Error marking alert as processed: {e}")

class AlertRuleEngine:
    """Rule engine for alert generation"""
    
    def __init__(self):
        self.rules: Dict[str, AlertRule] = {}
        self.zones: Dict[str, Zone] = {}
        self.rule_processors: Dict[AlertRuleType, callable] = {
            AlertRuleType.OBJECT_DETECTION: self._process_object_detection_rule,
            AlertRuleType.ZONE_INTRUSION: self._process_zone_intrusion_rule,
            AlertRuleType.LOITERING: self._process_loitering_rule,
            AlertRuleType.CROWD_DENSITY: self._process_crowd_density_rule,
            AlertRuleType.MOTION_DETECTION: self._process_motion_detection_rule,
            AlertRuleType.FACE_RECOGNITION: self._process_face_recognition_rule,
            AlertRuleType.VEHICLE_DETECTION: self._process_vehicle_detection_rule,
            AlertRuleType.ANOMALY_DETECTION: self._process_anomaly_detection_rule,
            AlertRuleType.THRESHOLD_BREACH: self._process_threshold_breach_rule,
            AlertRuleType.CORRELATION_RULE: self._process_correlation_rule,
        }
        self._load_default_rules()
        self._load_default_zones()
    
    def _load_default_rules(self):
        """Load default alert rules"""
        default_rules = [
            AlertRule(
                id="person_detection",
                name="Person Detection Alert",
                rule_type=AlertRuleType.OBJECT_DETECTION,
                conditions={
                    "detection_types": ["person"],
                    "min_confidence": 0.7,
                    "max_objects": 10
                },
                actions=["send_alert", "log_event"],
                cooldown_minutes=2,
                priority=2
            ),
            AlertRule(
                id="zone_intrusion",
                name="Zone Intrusion Alert",
                rule_type=AlertRuleType.ZONE_INTRUSION,
                conditions={
                    "detection_types": ["person", "vehicle"],
                    "min_confidence": 0.6,
                    "restricted_zones": ["secure_area", "parking_lot"]
                },
                actions=["send_alert", "record_incident"],
                cooldown_minutes=5,
                priority=1
            ),
            AlertRule(
                id="loitering_detection",
                name="Loitering Detection",
                rule_type=AlertRuleType.LOITERING,
                conditions={
                    "detection_types": ["person"],
                    "min_duration_seconds": 300,  # 5 minutes
                    "max_movement_distance": 50  # pixels
                },
                actions=["send_alert", "capture_snapshot"],
                cooldown_minutes=10,
                priority=3
            ),
            AlertRule(
                id="vehicle_detection",
                name="Vehicle Detection Alert",
                rule_type=AlertRuleType.VEHICLE_DETECTION,
                conditions={
                    "detection_types": ["car", "truck", "motorcycle"],
                    "min_confidence": 0.8,
                    "restricted_areas": ["pedestrian_zone"]
                },
                actions=["send_alert", "log_event"],
                cooldown_minutes=3,
                priority=2
            )
        ]
        
        for rule in default_rules:
            self.rules[rule.id] = rule
    
    def _load_default_zones(self):
        """Load default monitoring zones"""
        default_zones = [
            Zone(
                id="secure_area",
                name="Secure Area",
                coordinates=[(0, 0), (100, 0), (100, 100), (0, 100)],
                camera_ids=["camera_1", "camera_2"],
                description="High security zone"
            ),
            Zone(
                id="parking_lot",
                name="Parking Lot",
                coordinates=[(200, 0), (400, 0), (400, 200), (200, 200)],
                camera_ids=["camera_3", "camera_4"],
                description="Vehicle parking area"
            ),
            Zone(
                id="pedestrian_zone",
                name="Pedestrian Zone",
                coordinates=[(50, 50), (150, 50), (150, 150), (50, 150)],
                camera_ids=["camera_1", "camera_5"],
                description="Pedestrian-only area"
            )
        ]
        
        for zone in default_zones:
            self.zones[zone.id] = zone
    
    def add_rule(self, rule: AlertRule):
        """Add or update an alert rule"""
        self.rules[rule.id] = rule
        logger.info(f"Added alert rule: {rule.name}")
    
    def remove_rule(self, rule_id: str):
        """Remove an alert rule"""
        if rule_id in self.rules:
            del self.rules[rule_id]
            logger.info(f"Removed alert rule: {rule_id}")
    
    def get_matching_rules(self, detection_event: DetectionEvent) -> List[AlertRule]:
        """Get rules that match the detection event"""
        matching_rules = []
        
        for rule in self.rules.values():
            if not rule.enabled:
                continue
            
            # Check camera filter
            if rule.cameras and detection_event.camera_id not in rule.cameras:
                continue
            
            # Check zone filter
            if rule.zones and detection_event.zone_id not in rule.zones:
                continue
            
            # Check rule-specific conditions
            if self._evaluate_rule_conditions(rule, detection_event):
                matching_rules.append(rule)
        
        return sorted(matching_rules, key=lambda r: r.priority)
    
    def _evaluate_rule_conditions(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Evaluate if rule conditions match detection event"""
        try:
            processor = self.rule_processors.get(rule.rule_type)
            if processor:
                return processor(rule, detection_event)
            else:
                logger.warning(f"No processor for rule type: {rule.rule_type}")
                return False
        except Exception as e:
            logger.error(f"Error evaluating rule conditions: {e}")
            return False
    
    def _process_object_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Process object detection rule"""
        conditions = rule.conditions
        
        # Check detection type
        if 'detection_types' in conditions:
            if detection_event.detection_type not in conditions['detection_types']:
                return False
        
        # Check confidence threshold
        if 'min_confidence' in conditions:
            if detection_event.confidence < conditions['min_confidence']:
                return False
        
        return True
    
    def _process_zone_intrusion_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Process zone intrusion rule"""
        conditions = rule.conditions
        
        # Check if detection is in restricted zone
        if 'restricted_zones' in conditions:
            if detection_event.zone_id not in conditions['restricted_zones']:
                return False
        
        # Check detection type
        if 'detection_types' in conditions:
            if detection_event.detection_type not in conditions['detection_types']:
                return False
        
        # Check confidence
        if 'min_confidence' in conditions:
            if detection_event.confidence < conditions['min_confidence']:
                return False
        
        return True
    
    def _process_loitering_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Process loitering detection rule"""
        conditions = rule.conditions
        
        if 'detection_types' in conditions:
            if detection_event.detection_type not in conditions['detection_types']:
                return False
        
        # TODO: Implement actual loitering detection logic
        return True
    
    def _process_crowd_density_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Process crowd density rule"""
        conditions = rule.conditions
        
        if 'min_count' in conditions:
            # TODO: Implement crowd counting logic
            pass
        
        return True
    
    def _process_motion_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Process motion detection rule"""
        return detection_event.detection_type == "motion"
    
    def _process_face_recognition_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Process face recognition rule"""
        return detection_event.detection_type == "face"
    
    def _process_vehicle_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Process vehicle detection rule"""
        conditions = rule.conditions
        
        if 'detection_types' in conditions:
            if detection_event.detection_type not in conditions['detection_types']:
                return False
        
        if 'restricted_areas' in conditions:
            if detection_event.zone_id in conditions['restricted_areas']:
                return True
        
        return False
    
    def _process_anomaly_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Process anomaly detection rule"""
        return detection_event.detection_type == "anomaly"
    
    def _process_threshold_breach_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Process threshold breach rule"""
        conditions = rule.conditions
        
        if 'threshold_value' in conditions:
            value = detection_event.metadata.get('value', 0)
            threshold = conditions['threshold_value']
            operator = conditions.get('operator', 'gt')
            
            if operator == 'gt':
                return value > threshold
            elif operator == 'lt':
                return value < threshold
            elif operator == 'eq':
                return value == threshold
        
        return False
    
    def _process_correlation_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Process correlation rule"""
        # This would require complex event correlation
        # For now, return basic check
        return True

class AlertGenerator:
    """Main alert generation class"""
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.rule_engine = AlertRuleEngine()
        self.suppression_manager = AlertSuppressionManager(redis_client)
        self.kafka_producer = get_kafka_producer()
        self.alert_history: List[Dict[str, Any]] = []
        self.history_lock = threading.Lock()
        
    def generate_alert(self, detection: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Generate alert from detection event"""
        start_time = time.time()
        
        try:
            # Parse detection event
            detection_event = self._parse_detection_event(detection)
            
            # Get matching rules
            matching_rules = self.rule_engine.get_matching_rules(detection_event)
            
            if not matching_rules:
                logger.debug(f"No matching rules for detection: {detection_event.detection_type}")
                return None
            
            # Generate alert for the highest priority rule
            primary_rule = matching_rules[0]
            
            # Create alert context
            context = AlertContext(
                detection_event=detection_event,
                matched_rules=matching_rules,
                previous_alerts=self._get_recent_alerts(detection_event.camera_id),
                camera_status=self._get_camera_status(detection_event.camera_id),
                zone_info=self.rule_engine.zones.get(detection_event.zone_id),
                correlation_id=detection_event.correlation_id or str(uuid.uuid4())
            )
            
            # Generate alert
            alert = self._create_alert(primary_rule, context)
            
            # Check suppression
            should_suppress, reason = self.suppression_manager.should_suppress_alert(alert, primary_rule)
            if should_suppress:
                logger.info(f"Alert suppressed: {reason}")
                suppressed_alerts_total.labels(
                    alert_type=alert['alert_type'],
                    reason=reason.split(' ')[0]
                ).inc()
                return None
            
            # Store in history
            self._store_alert_in_history(alert)
            
            # Send to Kafka
            self._send_alert_to_kafka(alert)
            
            # Update metrics
            alerts_generated_total.labels(
                alert_type=alert['alert_type'],
                severity=alert['severity'],
                camera_id=alert['camera_id']
            ).inc()
            
            alert_rules_matched.labels(
                rule_name=primary_rule.name,
                camera_id=detection_event.camera_id
            ).inc()
            
            processing_time = time.time() - start_time
            alert_processing_time.labels(
                alert_type=alert['alert_type']
            ).observe(processing_time)
            
            logger.info(f"Alert generated: {alert['alert_type']} for camera {alert['camera_id']}")
            return alert
            
        except Exception as e:
            logger.error(f"Error generating alert: {e}", exc_info=True)
            return None
    
    def _parse_detection_event(self, detection: Dict[str, Any]) -> DetectionEvent:
        """Parse detection dictionary into DetectionEvent object"""
        return DetectionEvent(
            id=detection.get('id', str(uuid.uuid4())),
            camera_id=detection.get('camera_id', 'unknown'),
            detection_type=detection.get('detection_type', 'unknown'),
            confidence=detection.get('confidence', 0.0),
            timestamp=datetime.fromisoformat(detection.get('timestamp', datetime.utcnow().isoformat())),
            bounding_box=detection.get('bounding_box'),
            zone_id=detection.get('zone_id'),
            metadata=detection.get('metadata', {}),
            correlation_id=detection.get('correlation_id')
        )
    
    def _create_alert(self, rule: AlertRule, context: AlertContext) -> Dict[str, Any]:
        """Create alert from rule and context"""
        detection_event = context.detection_event
        
        # Determine alert severity
        severity = self._determine_alert_severity(rule, context)
        
        # Generate alert message
        message = self._generate_alert_message(rule, context)
        
        # Create alert object
        alert = {
            'id': str(uuid.uuid4()),
            'alert_type': self._map_rule_type_to_alert_type(rule.rule_type),
            'severity': severity,
            'message': message,
            'camera_id': detection_event.camera_id,
            'zone_id': detection_event.zone_id,
            'detection_type': detection_event.detection_type,
            'confidence': detection_event.confidence,
            'timestamp': datetime.utcnow().isoformat(),
            'rule_id': rule.id,
            'rule_name': rule.name,
            'correlation_id': context.correlation_id,
            'metadata': {
                'detection_event_id': detection_event.id,
                'bounding_box': detection_event.bounding_box,
                'zone_info': context.zone_info.__dict__ if context.zone_info else None,
                'matched_rules': [r.id for r in context.matched_rules],
                'camera_status': context.camera_status
            }
        }
        
        return alert
    
    def _determine_alert_severity(self, rule: AlertRule, context: AlertContext) -> str:
        """Determine alert severity based on rule and context"""
        detection_event = context.detection_event
        
        # Base severity from rule priority
        if rule.priority == 1:
            base_severity = AlertSeverity.CRITICAL
        elif rule.priority == 2:
            base_severity = AlertSeverity.HIGH
        elif rule.priority == 3:
            base_severity = AlertSeverity.MEDIUM
        else:
            base_severity = AlertSeverity.LOW
        
        # Adjust based on confidence
        if detection_event.confidence > 0.9:
            if base_severity == AlertSeverity.MEDIUM:
                base_severity = AlertSeverity.HIGH
            elif base_severity == AlertSeverity.LOW:
                base_severity = AlertSeverity.MEDIUM
        
        # Adjust based on zone (if in secure area, increase severity)
        if context.zone_info and 'secure' in context.zone_info.name.lower():
            if base_severity == AlertSeverity.MEDIUM:
                base_severity = AlertSeverity.HIGH
            elif base_severity == AlertSeverity.LOW:
                base_severity = AlertSeverity.MEDIUM
        
        return base_severity.value
    
    def _generate_alert_message(self, rule: AlertRule, context: AlertContext) -> str:
        """Generate human-readable alert message"""
        detection_event = context.detection_event
        
        messages = {
            AlertRuleType.OBJECT_DETECTION: f"{detection_event.detection_type.title()} detected in camera {detection_event.camera_id}",
            AlertRuleType.ZONE_INTRUSION: f"Intrusion detected: {detection_event.detection_type} in restricted zone {detection_event.zone_id}",
            AlertRuleType.LOITERING: f"Loitering detected: {detection_event.detection_type} in camera {detection_event.camera_id}",
            AlertRuleType.VEHICLE_DETECTION: f"Vehicle detected: {detection_event.detection_type} in camera {detection_event.camera_id}",
            AlertRuleType.FACE_RECOGNITION: f"Face detected in camera {detection_event.camera_id}",
            AlertRuleType.MOTION_DETECTION: f"Motion detected in camera {detection_event.camera_id}",
            AlertRuleType.CROWD_DENSITY: f"Crowd density threshold exceeded in camera {detection_event.camera_id}",
            AlertRuleType.ANOMALY_DETECTION: f"Anomaly detected in camera {detection_event.camera_id}",
            AlertRuleType.THRESHOLD_BREACH: f"Threshold breach detected in camera {detection_event.camera_id}",
            AlertRuleType.CORRELATION_RULE: f"Correlation rule triggered for camera {detection_event.camera_id}"
        }
        
        base_message = messages.get(rule.rule_type, f"Alert triggered by rule {rule.name}")
        
        # Add confidence information
        if detection_event.confidence > 0:
            base_message += f" (confidence: {detection_event.confidence:.2f})"
        
        # Add zone information
        if context.zone_info:
            base_message += f" in zone '{context.zone_info.name}'"
        
        return base_message
    
    def _map_rule_type_to_alert_type(self, rule_type: AlertRuleType) -> str:
        """Map rule type to alert type"""
        mapping = {
            AlertRuleType.OBJECT_DETECTION: AlertType.DETECTION,
            AlertRuleType.ZONE_INTRUSION: AlertType.INTRUSION,
            AlertRuleType.LOITERING: AlertType.BEHAVIOR,
            AlertRuleType.VEHICLE_DETECTION: AlertType.DETECTION,
            AlertRuleType.FACE_RECOGNITION: AlertType.DETECTION,
            AlertRuleType.MOTION_DETECTION: AlertType.MOTION,
            AlertRuleType.CROWD_DENSITY: AlertType.BEHAVIOR,
            AlertRuleType.ANOMALY_DETECTION: AlertType.ANOMALY,
            AlertRuleType.THRESHOLD_BREACH: AlertType.SYSTEM,
            AlertRuleType.CORRELATION_RULE: AlertType.CORRELATION
        }
        
        return mapping.get(rule_type, AlertType.GENERAL).value
    
    def _get_recent_alerts(self, camera_id: str, minutes: int = 30) -> List[Dict[str, Any]]:
        """Get recent alerts for camera"""
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
        
        with self.history_lock:
            return [
                alert for alert in self.alert_history
                if alert['camera_id'] == camera_id and
                datetime.fromisoformat(alert['timestamp']) > cutoff_time
            ]
    
    def _get_camera_status(self, camera_id: str) -> Dict[str, Any]:
        """Get camera status information"""
        # This would typically query a camera management system
        return {
            'camera_id': camera_id,
            'status': 'online',
            'last_seen': datetime.utcnow().isoformat()
        }
    
    def _store_alert_in_history(self, alert: Dict[str, Any]):
        """Store alert in local history"""
        with self.history_lock:
            self.alert_history.append(alert)
            
            # Keep only last 1000 alerts
            if len(self.alert_history) > 1000:
                self.alert_history = self.alert_history[-1000:]
    
    def _send_alert_to_kafka(self, alert: Dict[str, Any]):
        """Send alert to Kafka topic"""
        try:
            success = self.kafka_producer.send_message(
                TopicType.ALERTS,
                alert,
                key=alert['camera_id']
            )
            
            if success:
                logger.info(f"Alert sent to Kafka: {alert['id']}")
            else:
                logger.error(f"Failed to send alert to Kafka: {alert['id']}")
                
        except Exception as e:
            logger.error(f"Error sending alert to Kafka: {e}")
    
    def get_alert_history(self, 
                         camera_id: Optional[str] = None,
                         alert_type: Optional[str] = None,
                         limit: int = 100) -> List[Dict[str, Any]]:
        """Get alert history with optional filtering"""
        with self.history_lock:
            filtered_alerts = self.alert_history
            
            if camera_id:
                filtered_alerts = [a for a in filtered_alerts if a['camera_id'] == camera_id]
            
            if alert_type:
                filtered_alerts = [a for a in filtered_alerts if a['alert_type'] == alert_type]
            
            # Sort by timestamp (newest first) and limit
            filtered_alerts.sort(key=lambda x: x['timestamp'], reverse=True)
            return filtered_alerts[:limit]
    
    def get_alert_statistics(self) -> Dict[str, Any]:
        """Get alert statistics"""
        with self.history_lock:
            total_alerts = len(self.alert_history)
            
            if total_alerts == 0:
                return {
                    'total_alerts': 0,
                    'alerts_by_type': {},
                    'alerts_by_severity': {},
                    'alerts_by_camera': {}
                }
            
            # Count by type
            alerts_by_type = {}
            alerts_by_severity = {}
            alerts_by_camera = {}
            
            for alert in self.alert_history:
                # By type
                alert_type = alert['alert_type']
                alerts_by_type[alert_type] = alerts_by_type.get(alert_type, 0) + 1
                
                # By severity
                severity = alert['severity']
                alerts_by_severity[severity] = alerts_by_severity.get(severity, 0) + 1
                
                # By camera
                camera_id = alert['camera_id']
                alerts_by_camera[camera_id] = alerts_by_camera.get(camera_id, 0) + 1
            
            return {
                'total_alerts': total_alerts,
                'alerts_by_type': alerts_by_type,
                'alerts_by_severity': alerts_by_severity,
                'alerts_by_camera': alerts_by_camera
            }

# Global alert generator instance
_alert_generator = None
_alert_generator_lock = threading.Lock()

def get_alert_generator() -> AlertGenerator:
    """Get singleton alert generator instance"""
    global _alert_generator
    
    if _alert_generator is None:
        with _alert_generator_lock:
            if _alert_generator is None:
                try:
                    # Try to initialize with Redis
                    redis_client = redis.Redis(
                        host=getattr(settings, 'redis', {}).get('host', 'localhost'),
                        port=getattr(settings, 'redis', {}).get('port', 6379),
                        db=getattr(settings, 'redis', {}).get('db', 0),
                        decode_responses=True
                    )
                    redis_client.ping()
                    _alert_generator = AlertGenerator(redis_client)
                except:
                    # Fall back to in-memory only
                    logger.warning("Redis not available, using in-memory alert generation")
                    _alert_generator = AlertGenerator()
    
    return _alert_generator

# Convenience function for generating alerts
def generate_alert(detection: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Generate alert from detection event"""
    alert_generator = get_alert_generator()
    return alert_generator.generate_alert(detection)

# Testing and debugging functions
def test_alert_generation():
    """Test alert generation with dummy data"""
    test_detection = {
        'id': str(uuid.uuid4()),
        'camera_id': 'camera_1',
        'detection_type': 'person',
        'confidence': 0.85,
        'timestamp': datetime.utcnow().isoformat(),
        'zone_id': 'secure_area',
        'bounding_box': {'x': 100, 'y': 200, 'width': 50, 'height': 100},
        'metadata': {'test': True}
    }
    
    alert = generate_alert(test_detection)
    if alert:
        print(f"Generated alert: {json.dumps(alert, indent=2)}")
    else:
        print("No alert generated")

if __name__ == "__main__":
    test_alert_generation()