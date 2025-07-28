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
# import hashlib

# from prometheus_client import Counter, Histogram, Gauge
# import redis

# from config.settings import get_settings
# from utils.logger import get_logger
# from alerts.alert_schema import AlertType, AlertSeverity
# # from kafka.kafka_producer import get_kafka_producer
# from kafka_handlers.kafka_producer import get_kafka_producer
# # from kafka.topics import TopicType
# from kafka_handlers.topics import TopicType

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
#     """Represents a detection event"""
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
                    
#                     #超.1.0.1
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
# import hashlib

# from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, REGISTRY
# import redis

# from config.settings import get_settings
# from utils.logger import get_logger
# from alerts.alert_schema import Alert,AlertType, AlertSeverity
# # # from kafka.kafka_producer import get_kafka_producer
# from kafka_handlers.kafka_producer import get_kafka_producer
# # # from kafka.topics import TopicType
# from kafka_handlers.topics import TopicType

# # Initialize components
# logger = get_logger(__name__)
# settings = get_settings()

# # Create a custom registry or use the default one safely
# _metrics_registry = None
# _metrics_lock = threading.Lock()

# def get_metrics_registry():
#     """Get or create metrics registry"""
#     global _metrics_registry
#     if _metrics_registry is None:
#         with _metrics_lock:
#             if _metrics_registry is None:
#                 _metrics_registry = CollectorRegistry()
#     return _metrics_registry

# # Initialize Prometheus metrics with error handling
# def create_prometheus_metrics():
#     """Create Prometheus metrics with duplicate handling"""
#     registry = get_metrics_registry()
    
#     try:
#         # Try to create metrics, catch duplicates
#         alerts_generated_total = Counter(
#             'alerts_generated_total',
#             'Total alerts generated',
#             ['alert_type', 'severity', 'camera_id'],
#             registry=registry
#         )
        
#         alert_processing_time = Histogram(
#             'alert_processing_time_seconds',
#             'Time spent processing alerts',
#             ['alert_type'],
#             registry=registry
#         )
        
#         alert_rules_matched = Counter(
#             'alert_rules_matched_total',
#             'Total alert rules matched',
#             ['rule_name', 'camera_id'],
#             registry=registry
#         )
        
#         suppressed_alerts_total = Counter(
#             'suppressed_alerts_total',
#             'Total suppressed alerts',
#             ['alert_type', 'reason'],
#             registry=registry
#         )
        
#         return {
#             'alerts_generated_total': alerts_generated_total,
#             'alert_processing_time': alert_processing_time,
#             'alert_rules_matched': alert_rules_matched,
#             'suppressed_alerts_total': suppressed_alerts_total
#         }
        
#     except ValueError as e:
#         if "Duplicated timeseries" in str(e):
#             logger.warning("Prometheus metrics already registered, reusing existing metrics")
#             # Return existing metrics from the default registry
#             return {
#                 'alerts_generated_total': None,
#                 'alert_processing_time': None,
#                 'alert_rules_matched': None,
#                 'suppressed_alerts_total': None
#             }
#         else:
#             raise e

# # Create metrics with error handling
# _prometheus_metrics = create_prometheus_metrics()

# # Helper function to safely increment metrics
# def safe_metric_inc(metric, **kwargs):
#     """Safely increment metric if it exists"""
#     if metric is not None:
#         try:
#             metric.inc()
#         except Exception as e:
#             logger.debug(f"Error incrementing metric: {e}")

# def safe_metric_observe(metric, value, **kwargs):
#     """Safely observe metric if it exists"""
#     if metric is not None:
#         try:
#             metric.observe(value)
#         except Exception as e:
#             logger.debug(f"Error observing metric: {e}")

# def safe_metric_labels(metric, **kwargs):
#     """Safely get metric labels if metric exists"""
#     if metric is not None:
#         try:
#             return metric.labels(**kwargs)
#         except Exception as e:
#             logger.debug(f"Error getting metric labels: {e}")
#             return None
#     return None


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
#     """Represents a detection event"""
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

# # class AlertEngine:
# #     def __init__(self):
# #         self.generators = {
# #             AlertRuleType.MOTION: MotionBasedAlertGenerator(),
# #             AlertRuleType.OBJECT: ObjectBasedAlertGenerator(),
# #             AlertRuleType.BEHAVIOR: BehaviorBasedAlertGenerator(),
# #             AlertRuleType.CORRELATION: CorrelationAlertGenerator()
# #         }

# class BaseAlertGenerator:
#     """Base class for alert generators"""
    
#     async def initialize(self):
#         """Initialize the generator if needed"""
#         pass
    
#     async def shutdown(self):
#         """Shutdown the generator if needed"""
#         pass
    
#     def generate_alert(self, rule, frame_data):
#         """Generate alert from rule and frame data"""
#         raise NotImplementedError
    

# class BaseAlertGenerator:
#     """Base class for alert generators"""
    
#     async def initialize(self):
#         """Initialize the generator if needed"""
#         pass
    
#     async def shutdown(self):
#         """Shutdown the generator if needed"""
#         pass
    
#     def generate_alert(self, rule: 'AlertRule', detection_event: 'DetectionEvent') -> Optional[Alert]: # Added type hints for clarity
#         """Generate alert from rule and detection event"""
#         raise NotImplementedError

# class MotionBasedAlertGenerator(BaseAlertGenerator):
#     def generate_alert(self, rule: 'AlertRule', detection_event: 'DetectionEvent') -> Optional[Alert]:
#         logger.info(f"Generating motion-based alert for {detection_event.camera_id}")
#         # Implement your motion-specific alert generation logic here
#         # This is a placeholder. You'll need to create a proper Alert object.
#         # For demonstration, creating a generic alert:
#         return Alert(
#             id=str(uuid.uuid4()),
#             alert_type=AlertType.MOTION,
#             severity=AlertSeverity.MEDIUM,
#             message=f"Motion detected in camera {detection_event.camera_id} via rule {rule.name}",
#             camera_id=detection_event.camera_id,
#             timestamp=datetime.utcnow(),
#             detection_type=detection_event.detection_type,
#             confidence=detection_event.confidence,
#             zone_id=detection_event.zone_id,
#             metadata=detection_event.metadata
#         )

# class ObjectBasedAlertGenerator(BaseAlertGenerator):
#     def generate_alert(self, rule: 'AlertRule', detection_event: 'DetectionEvent') -> Optional[Alert]:
#         logger.info(f"Generating object-based alert for {detection_event.detection_type} in {detection_event.camera_id}")
#         # Implement your object-specific alert generation logic here
#         return Alert(
#             id=str(uuid.uuid4()),
#             alert_type=AlertType.DETECTION, # Or other relevant AlertType
#             severity=AlertSeverity.HIGH,
#             message=f"{detection_event.detection_type.title()} detected in camera {detection_event.camera_id} via rule {rule.name}",
#             camera_id=detection_event.camera_id,
#             timestamp=datetime.utcnow(),
#             detection_type=detection_event.detection_type,
#             confidence=detection_event.confidence,
#             zone_id=detection_event.zone_id,
#             metadata=detection_event.metadata
#         )

# class BehaviorBasedAlertGenerator(BaseAlertGenerator):
#     def generate_alert(self, rule: 'AlertRule', detection_event: 'DetectionEvent') -> Optional[Alert]:
#         logger.info(f"Generating behavior-based alert for {detection_event.camera_id}")
#         # Implement your behavior-specific alert generation logic here
#         return Alert(
#             id=str(uuid.uuid4()),
#             alert_type=AlertType.BEHAVIOR,
#             severity=AlertSeverity.CRITICAL,
#             message=f"Unusual behavior detected in camera {detection_event.camera_id} via rule {rule.name}",
#             camera_id=detection_event.camera_id,
#             timestamp=datetime.utcnow(),
#             detection_type=detection_event.detection_type,
#             confidence=detection_event.confidence,
#             zone_id=detection_event.zone_id,
#             metadata=detection_event.metadata
#         )

# class CorrelationAlertGenerator(BaseAlertGenerator):
#     def generate_alert(self, rule: 'AlertRule', detection_event: 'DetectionEvent') -> Optional[Alert]:
#         logger.info(f"Generating correlation-based alert for {detection_event.camera_id}")
#         # Implement your correlation-specific alert generation logic here
#         return Alert(
#             id=str(uuid.uuid4()),
#             alert_type=AlertType.CORRELATION,
#             severity=AlertSeverity.CRITICAL,
#             message=f"Correlated event detected in camera {detection_event.camera_id} via rule {rule.name}",
#             camera_id=detection_event.camera_id,
#             timestamp=datetime.utcnow(),
#             detection_type=detection_event.detection_type,
#             confidence=detection_event.confidence,
#             zone_id=detection_event.zone_id,
#             metadata=detection_event.metadata
#         )


# # class AlertEngine:
# #     def __init__(self, settings, alert_store):
# #         """
# #         Initialize AlertEngine with settings and alert store.
        
# #         Args:
# #             settings: Application settings instance
# #             alert_store: Alert storage backend
# #         """
# #         self.settings = settings
# #         self.alert_store = alert_store
        
# #         # Initialize alert generators, mapping AlertRuleType to specific generators
# #         self.generators = {
# #             AlertRuleType.MOTION_DETECTION: MotionBasedAlertGenerator(),
# #             AlertRuleType.OBJECT_DETECTION: ObjectBasedAlertGenerator(), # Changed from OBJECT
# #             AlertRuleType.LOITERING: BehaviorBasedAlertGenerator(),     # Changed from BEHAVIOR - Loitering is a behavior
# #             AlertRuleType.CROWD_DENSITY: BehaviorBasedAlertGenerator(), # Crowd density is also a behavior
# #             AlertRuleType.ANOMALY_DETECTION: BehaviorBasedAlertGenerator(), # Anomaly detection can also be a behavior
# #             AlertRuleType.CORRELATION_RULE: CorrelationAlertGenerator(), # Changed from CORRELATION
# #             AlertRuleType.ZONE_INTRUSION: ObjectBasedAlertGenerator(), # Intrusion could be object-based
# #             AlertRuleType.FACE_RECOGNITION: ObjectBasedAlertGenerator(), # Face is an object
# #             AlertRuleType.VEHICLE_DETECTION: ObjectBasedAlertGenerator(), # Vehicle is an object
# #             AlertRuleType.THRESHOLD_BREACH: BehaviorBasedAlertGenerator(), # Threshold breach can be behavioral
# #             # Add more mappings as needed for other AlertRuleTypes
# #         }
# #         logger.info("AlertEngine initialized with rule-specific generators.")

# class AlertEngine:
#     def __init__(self, settings, alert_store):
#         """
#         Initialize AlertEngine with settings and alert store.
        
#         Args:
#             settings: Application settings instance
#             alert_store: Alert storage backend
#         """
#         self.settings = settings
#         self.alert_store = alert_store
        
#         # Initialize alert generators
#         # The keys here MUST match the exact members defined in AlertRuleType
#         self.generators = {
#             # Map specific AlertRuleType members to your generator classes
#             AlertRuleType.MOTION_DETECTION: MotionBasedAlertGenerator(),
#             AlertRuleType.OBJECT_DETECTION: ObjectBasedAlertGenerator(),
#             AlertRuleType.ZONE_INTRUSION: ObjectBasedAlertGenerator(), # Zone intrusion often relates to objects entering zones
#             AlertRuleType.LOITERING: BehaviorBasedAlertGenerator(),
#             AlertRuleType.CROWD_DENSITY: BehaviorBasedAlertGenerator(),
#             AlertRuleType.FACE_RECOGNITION: ObjectBasedAlertGenerator(), # Face recognition is a form of object detection
#             AlertRuleType.VEHICLE_DETECTION: ObjectBasedAlertGenerator(),
#             AlertRuleType.ANOMALY_DETECTION: BehaviorBasedAlertGenerator(), # Anomaly detection can be behavioral or systemic
#             AlertRuleType.THRESHOLD_BREACH: BehaviorBasedAlertGenerator(), # Threshold breach can be behavioral or systemic
#             AlertRuleType.CORRELATION_RULE: CorrelationAlertGenerator(),
#             # Make sure all AlertRuleType members that you intend to handle
#             # with a dedicated generator have an entry here.
#             # If a rule type doesn't have a specific generator, you might
#             # want a default one or handle it at a higher level.
#         }
#         logger.info("AlertEngine initialized with rule-specific generators.")

#     async def initialize(self):
#         """Initialize the alert engine and its components."""
#         # Initialize generators if they need async setup
#         for generator in self.generators.values():
#             if hasattr(generator, 'initialize'):
#                 await generator.initialize()
        
#         # Any other initialization logic here
#         pass
    
#     async def shutdown(self):
#         """Shutdown the alert engine gracefully."""
#         # Cleanup generators if needed
#         for generator in self.generators.values():
#             if hasattr(generator, 'shutdown'):
#                 await generator.shutdown()
        
#         # Any other cleanup logic here
#         pass

#     def generate_alert(self, rule: AlertRule, detection_event: DetectionEvent) -> Optional[Alert]: # Changed frame_data to detection_event for clarity
#         """
#         Generates an alert based on a matched rule and detection event.
#         Dispatches to the appropriate generator based on the rule type.
#         """
#         generator = self.generators.get(rule.rule_type)
#         if generator:
#             alert = generator.generate_alert(rule, detection_event) # Pass detection_event
#             if alert:
#                 logger.info(f"Alert generated by {rule.rule_type.value} generator: {alert.id}")
#                 # You would typically store the alert here in alert_store
#                 # await self.alert_store.store_alert(alert) # This would be an async call in a different context
#                 return alert
#         logger.warning(f"No specific generator found or alert not generated for rule type: {rule.rule_type.value}")
#         return None

# # class AlertEngine:
# #     def __init__(self, settings, alert_store):
# #         """
# #         Initialize AlertEngine with settings and alert store.
        
# #         Args:
# #             settings: Application settings instance
# #             alert_store: Alert storage backend
# #         """
# #         self.settings = settings
# #         self.alert_store = alert_store
        
# #         # Initialize alert generators
# #         self.generators = {
# #             AlertRuleType.MOTION: MotionBasedAlertGenerator(),
# #             AlertRuleType.OBJECT: ObjectBasedAlertGenerator(),
# #             AlertRuleType.BEHAVIOR: BehaviorBasedAlertGenerator(),
# #             AlertRuleType.CORRELATION: CorrelationAlertGenerator()
# #         }
        
# #     async def initialize(self):
# #         """Initialize the alert engine and its components."""
# #         # Initialize generators if they need async setup
# #         for generator in self.generators.values():
# #             if hasattr(generator, 'initialize'):
# #                 await generator.initialize()
        
# #         # Any other initialization logic here
# #         pass
    
# #     async def shutdown(self):
# #         """Shutdown the alert engine gracefully."""
# #         # Cleanup generators if needed
# #         for generator in self.generators.values():
# #             if hasattr(generator, 'shutdown'):
# #                 await generator.shutdown()
        
# #         # Any other cleanup logic here
# #         pass

# #     def generate_alert(self, rule: AlertRule, frame_data: Dict[str, Any]) -> Optional[Alert]:
# #         generator = self.generators.get(rule.rule_type)
# #         if generator:
# #             return generator.generate_alert(rule, frame_data)
# #         return None

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
#                     last_time = datetime.fromisoformat(last_alert.decode() if isinstance(last_alert, bytes) else last_alert)
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
#                 # Safe metric increment
#                 suppressed_metric = safe_metric_labels(
#                     _prometheus_metrics['suppressed_alerts_total'],
#                     alert_type=alert['alert_type'],
#                     reason=reason.split(' ')[0]
#                 )
#                 safe_metric_inc(suppressed_metric)
#                 return None
            
#             # Store in history
#             self._store_alert_in_history(alert)
            
#             # Send to Kafka
#             self._send_alert_to_kafka(alert)
            
#             # Update metrics safely
#             alerts_metric = safe_metric_labels(
#                 _prometheus_metrics['alerts_generated_total'],
#                 alert_type=alert['alert_type'],
#                 severity=alert['severity'],
#                 camera_id=alert['camera_id']
#             )
#             safe_metric_inc(alerts_metric)
            
#             rules_metric = safe_metric_labels(
#                 _prometheus_metrics['alert_rules_matched'],
#                 rule_name=primary_rule.name,
#                 camera_id=detection_event.camera_id
#             )
#             safe_metric_inc(rules_metric)
            
#             processing_time = time.time() - start_time
#             timing_metric = safe_metric_labels(
#                 _prometheus_metrics['alert_processing_time'],
#                 alert_type=alert['alert_type']
#             )
#             safe_metric_observe(timing_metric, processing_time)
            
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
# alerts/alert_logic.py
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
from typing import Dict, List, Optional, Any, Tuple, Set, Callable, TYPE_CHECKING
from dataclasses import dataclass, field, asdict # Added asdict for dataclass to dict conversion
from datetime import datetime, timedelta
from enum import Enum
import threading
from queue import Queue, Empty # Added Empty for Queue handling
import asyncio
from abc import ABC, abstractmethod # Added ABC, abstractmethod if you use them
import hashlib
from collections import defaultdict # Added defaultdict

from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, REGISTRY
import redis # Sync Redis client

# Removed module-level get_settings() to prevent early loading issues
# from config.settings import get_settings

# Get logger (it will get a fallback if settings are not fully loaded yet)
from utils.logger import get_logger, LogContext # Ensure LogContext is imported

# Import Alert related models from alert_schema (now that it correctly defines them)
from alerts.alert_schema import Alert, AlertType, AlertSeverity, AlertRuleType, Zone, DetectionEvent, AlertRule, AlertContext # MODIFIED: Import all from alert_schema

# Import Kafka producer and topics manager's getter functions
# These will return the globally managed instances setup in main.py
from kafka_handlers.kafka_producer import get_kafka_producer as _get_global_kafka_producer
from kafka_handlers.topics import TopicType, get_topic_manager as _get_global_topic_manager

# Type hinting for settings and other managers to avoid runtime import issues
if TYPE_CHECKING:
    from config.settings import Settings as AppSettings
    from kafka_handlers.kafka_producer import KafkaProducerClient as AppKafkaProducerClient
    from alerts.alert_store import AlertStore as AppAlertStore # For AlertEngine type hint


# Initialize logger (it will get a basic logger first, then get reconfigured later by main.py)
logger = get_logger(__name__)


# Prometheus metrics (re-defined to be safe from duplicates and configurable)
# Using a custom registry or handling duplicates if default is used
_metrics_registry = None
_metrics_lock = threading.Lock()

def get_metrics_registry():
    """Get or create metrics registry, handling duplicates."""
    global _metrics_registry
    if _metrics_registry is None:
        with _metrics_lock:
            if _metrics_registry is None:
                try:
                    # Check if standard metrics are already registered
                    # This is an internal check, might not be future-proof for all Prometheus versions
                    if 'alerts_generated_total' in REGISTRY._names: # Check if a known metric exists in default
                         _metrics_registry = REGISTRY # Use default if already populated
                         logger.warning("Prometheus default registry already has metrics. Reusing.")
                    else:
                        _metrics_registry = CollectorRegistry()
                        logger.info("Created new Prometheus CollectorRegistry.")
                except Exception: # Catch any error from accessing internal REGISTRY
                    _metrics_registry = CollectorRegistry()
                    logger.info("Created new Prometheus CollectorRegistry after checking default.")
    return _metrics_registry

# Create metrics using the safe registry approach
# Pass registry to each metric constructor
alerts_generated_total = Counter(
    'alerts_generated_total', 'Total alerts generated', ['alert_type', 'severity', 'camera_id'],
    registry=get_metrics_registry()
)
alert_processing_time = Histogram(
    'alert_processing_time_seconds', 'Time spent processing alerts', ['alert_type'],
    registry=get_metrics_registry()
)
alert_rules_matched = Counter(
    'alert_rules_matched_total', 'Total alert rules matched', ['rule_name', 'camera_id'],
    registry=get_metrics_registry()
)
suppressed_alerts_total = Counter(
    'suppressed_alerts_total', 'Total suppressed alerts', ['alert_type', 'reason'],
    registry=get_metrics_registry()
)


class AlertSuppressionManager:
    """Manages alert suppression and deduplication."""
    
    def __init__(self, settings: 'AppSettings', redis_client: Optional[redis.Redis] = None):
        self.settings = settings
        self.redis_client = redis_client
        self.local_cache: Dict[str, datetime] = {}
        self.cache_lock = threading.Lock()
        self.logger = get_logger(f"{__name__}.AlertSuppressionManager")
        if self.redis_client is None:
            self.logger.warning("AlertSuppressionManager initialized without Redis client. Using in-memory cache for suppression.")

    def should_suppress_alert(self, alert: Alert, rule: AlertRule) -> Tuple[bool, str]:
        """Check if alert should be suppressed."""
        try:
            # Generate suppression key
            suppression_key = self._generate_suppression_key(alert, rule)
            
            # Check cooldown period
            if self._is_in_cooldown(suppression_key, rule.cooldown_minutes):
                suppressed_alerts_total.labels(alert_type=alert.alert_type.value, reason="cooldown").inc()
                return True, f"Alert suppressed due to cooldown period ({rule.cooldown_minutes} minutes)"
            
            # Check for duplicate content
            if self._is_duplicate_alert(alert):
                suppressed_alerts_total.labels(alert_type=alert.alert_type.value, reason="duplicate").inc()
                return True, "Duplicate alert content detected"
            
            # Check time window restrictions
            if not self._is_within_time_window(rule):
                suppressed_alerts_total.labels(alert_type=alert.alert_type.value, reason="time_window").inc()
                return True, "Alert outside configured time window"
            
            # Mark alert as processed
            self._mark_alert_processed(suppression_key, rule.cooldown_minutes) # Pass cooldown for TTL
            
            return False, ""
            
        except Exception as e:
            self.logger.error(f"Error checking alert suppression for alert {getattr(alert, 'id', 'N/A')}: {e}", exc_info=True)
            return False, ""
    
    def _generate_suppression_key(self, alert: Alert, rule: AlertRule) -> str:
        """Generate unique suppression key."""
        key_data = f"{rule.id}:{alert.camera_id}:{alert.alert_type.value}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _is_in_cooldown(self, suppression_key: str, cooldown_minutes: int) -> bool:
        """Check if alert is in cooldown period."""
        try:
            if self.redis_client:
                last_alert_iso = self.redis_client.get(f"alert_cooldown:{suppression_key}")
                if last_alert_iso:
                    last_time = datetime.fromisoformat(last_alert_iso)
                    if datetime.utcnow() - last_time < timedelta(minutes=cooldown_minutes):
                        return True
            else:
                with self.cache_lock:
                    if suppression_key in self.local_cache:
                        last_time = self.local_cache[suppression_key]
                        if datetime.utcnow() - last_time < timedelta(minutes=cooldown_minutes):
                            return True
            return False
        except Exception as e:
            self.logger.error(f"Error checking cooldown for {suppression_key}: {e}", exc_info=True)
            return False
    
    def _is_duplicate_alert(self, alert: Alert) -> bool:
        """Check for duplicate alert content."""
        try:
            content_key = f"{alert.camera_id}:{alert.alert_type.value}:{alert.message}"
            content_hash = hashlib.md5(content_key.encode()).hexdigest()
            
            if self.redis_client:
                duplicate_key = f"alert_content:{content_hash}"
                if self.redis_client.setnx(duplicate_key, "1"):
                    self.redis_client.expire(duplicate_key, self.settings.alerts.cooldown_seconds) # Use settings for TTL
                    return False
                else:
                    return True
            return False
        except Exception as e:
            self.logger.error(f"Error checking duplicate alert for {alert.id}: {e}", exc_info=True)
            return False
    
    def _is_within_time_window(self, rule: AlertRule) -> bool:
        """Check if current time is within rule's time windows."""
        if not rule.time_windows:
            return True
        
        current_dt = datetime.utcnow()
        current_time_only = current_dt.time()
        current_day_of_week = current_dt.strftime('%A').lower()
        
        for window in rule.time_windows:
            if 'days' in window:
                days_list = [day.lower() for day in window['days']]
                if current_day_of_week not in days_list:
                    continue
            
            start_time_str = window.get('start')
            end_time_str = window.get('end')

            if not start_time_str or not end_time_str:
                self.logger.warning(f"Invalid time window configuration in rule {rule.id}: Missing start or end time.")
                continue

            try:
                start_time_obj = datetime.strptime(start_time_str, '%H:%M').time()
                end_time_obj = datetime.strptime(end_time_str, '%H:%M').time()
            except ValueError:
                self.logger.error(f"Invalid time format in rule {rule.id}: {start_time_str}-{end_time_str}. Expected HH:MM.")
                continue

            if start_time_obj <= current_time_only <= end_time_obj:
                return True
        return False
    
    def _mark_alert_processed(self, suppression_key: str, cooldown_minutes: int):
        """Mark alert as processed to enforce cooldown."""
        try:
            current_time = datetime.utcnow()
            
            if self.redis_client:
                ttl_seconds = cooldown_minutes * 60
                self.redis_client.setex(
                    f"alert_cooldown:{suppression_key}",
                    ttl_seconds,
                    current_time.isoformat()
                )
            else:
                with self.cache_lock:
                    self.local_cache[suppression_key] = current_time
                    if len(self.local_cache) > self.settings.alerts.max_alerts_in_memory:
                        cutoff_time = current_time - timedelta(minutes=cooldown_minutes * 2)
                        self.local_cache = {
                            k: v for k, v in self.local_cache.items()
                            if v > cutoff_time
                        }
                        self.logger.debug(f"Cleaned local suppression cache. Size: {len(self.local_cache)}")
        except Exception as e:
            self.logger.error(f"Error marking alert as processed for {suppression_key}: {e}", exc_info=True)


class AlertRuleEngine:
    """Rule engine for alert generation."""
    
    def __init__(self, settings: 'AppSettings'): # MODIFIED: Accept settings
        self.settings = settings
        self.rules: Dict[str, AlertRule] = {}
        self.zones: Dict[str, Zone] = {}
        self.rule_processors: Dict[AlertRuleType, Callable] = {
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
        self.logger = get_logger(f"{__name__}.AlertRuleEngine")
        self._load_default_rules()
        self._load_default_zones()
    
    def _load_default_rules(self):
        """Load default alert rules from a configurable source (e.g., settings or DB)."""
        default_rules = [
            AlertRule(
                id="person_detection",
                name="Person Detection Alert",
                rule_type=AlertRuleType.OBJECT_DETECTION,
                conditions={
                    "detection_types": ["person"],
                    "min_confidence": self.settings.alerts.person_detection_threshold, # Use settings
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
                    "min_confidence": self.settings.alerts.threshold_confidence, # Use settings
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
                    "min_confidence": self.settings.alerts.vehicle_detection_threshold, # Use settings
                    "restricted_areas": ["pedestrian_zone"]
                },
                actions=["send_alert", "log_event"],
                cooldown_minutes=3,
                priority=2
            )
        ]
        for rule in default_rules:
            self.rules[rule.id] = rule
        self.logger.info(f"Loaded {len(default_rules)} default alert rules.")
    
    def _load_default_zones(self):
        """Load default monitoring zones."""
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
        self.logger.info(f"Loaded {len(default_zones)} default monitoring zones.")
    
    def add_rule(self, rule: AlertRule):
        """Add or update an alert rule."""
        self.rules[rule.id] = rule
        self.logger.info(f"Added alert rule: {rule.name}")
    
    def remove_rule(self, rule_id: str):
        """Remove an alert rule."""
        if rule_id in self.rules:
            del self.rules[rule_id]
            self.logger.info(f"Removed alert rule: {rule_id}")
    
    def get_matching_rules(self, detection_event: DetectionEvent) -> List[AlertRule]:
        """Get rules that match the detection event."""
        matching_rules = []
        
        for rule in self.rules.values():
            if not rule.enabled:
                continue
            
            if rule.cameras and detection_event.camera_id not in rule.cameras:
                continue
            
            if rule.zones and detection_event.zone_id not in rule.zones:
                continue
            
            if self._evaluate_rule_conditions(rule, detection_event):
                matching_rules.append(rule)
        
        return sorted(matching_rules, key=lambda r: r.priority)
    
    def _evaluate_rule_conditions(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        """Evaluate if rule conditions match detection event."""
        try:
            processor = self.rule_processors.get(rule.rule_type)
            if processor:
                return processor(rule, detection_event)
            else:
                self.logger.warning(f"No processor registered for rule type: {rule.rule_type.value}")
                return False
        except Exception as e:
            self.logger.error(f"Error evaluating rule conditions for rule {rule.id}: {e}", exc_info=True)
            return False
    
    def _process_object_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        conditions = rule.conditions
        if 'detection_types' in conditions and detection_event.detection_type not in conditions['detection_types']:
            return False
        if 'min_confidence' in conditions and detection_event.confidence < conditions['min_confidence']:
            return False
        return True
    
    def _process_zone_intrusion_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        conditions = rule.conditions
        if 'restricted_zones' in conditions and detection_event.zone_id not in conditions['restricted_zones']:
            return False
        if 'detection_types' in conditions and detection_event.detection_type not in conditions['detection_types']:
            return False
        if 'min_confidence' in conditions and detection_event.confidence < conditions['min_confidence']:
            return False
        return True
    
    def _process_loitering_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        conditions = rule.conditions
        if 'detection_types' in conditions and detection_event.detection_type not in conditions['detection_types']:
            return False
        return True # Placeholder for actual loitering logic
    
    def _process_crowd_density_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        conditions = rule.conditions
        if 'min_count' in conditions:
            pass # TODO: Implement crowd counting logic
        return True # Placeholder
    
    def _process_motion_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        return detection_event.detection_type == "motion"
    
    def _process_face_recognition_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        return detection_event.detection_type == "face"
    
    def _process_vehicle_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        conditions = rule.conditions
        if 'detection_types' in conditions and detection_event.detection_type not in conditions['detection_types']:
            return False
        if 'restricted_areas' in conditions and detection_event.zone_id in conditions['restricted_areas']:
            return True
        return False
    
    def _process_anomaly_detection_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        return detection_event.detection_type == "anomaly"
    
    def _process_threshold_breach_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        conditions = rule.conditions
        if 'threshold_value' in conditions:
            value = detection_event.metadata.get('value', 0)
            threshold = conditions['threshold_value']
            operator = conditions.get('operator', 'gt')
            if operator == 'gt': return value > threshold
            elif operator == 'lt': return value < threshold
            elif operator == 'eq': return value == threshold
        return False
    
    def _process_correlation_rule(self, rule: AlertRule, detection_event: DetectionEvent) -> bool:
        return True # Placeholder for complex correlation logic


class AlertEngine:
    """Main alert generation engine, orchestrating rules and suppression."""
    
    def __init__(self, settings_obj: 'AppSettings', alert_store_obj: 'AppAlertStore'):
        self.settings = settings_obj
        self.alert_store = alert_store_obj
        
        # Initialize AlertRuleEngine with settings
        self.rule_engine = AlertRuleEngine(self.settings)
        
        # Initialize AlertSuppressionManager with settings and Redis client
        redis_client_for_suppression = None
        if self.settings.redis.enabled:
            try:
                # Use sync redis.Redis for suppression manager as it's not async methods
                redis_client_for_suppression = redis.Redis(
                    host=self.settings.redis.host,
                    port=self.settings.redis.port,
                    db=self.settings.redis.db,
                    password=self.settings.redis.password,
                    decode_responses=True,
                    socket_timeout=self.settings.redis.socket_timeout,
                    socket_connect_timeout=self.settings.redis.socket_connect_timeout
                )
                redis_client_for_suppression.ping() # Test connection
                logger.info("AlertEngine: Connected to Redis for suppression manager.")
            except Exception as e:
                logger.warning(f"AlertEngine: Could not connect to Redis for suppression manager: {e}")
                redis_client_for_suppression = None

        self.suppression_manager = AlertSuppressionManager(self.settings, redis_client_for_suppression)
        
        # Get Kafka producer instance via global getter, ensuring it's initialized with settings and topic manager
        # These imports are now handled dynamically to prevent circular dependencies at module load.
        # The main.py will have already initialized them globally.
        from kafka_handlers.topics import get_topic_manager as _get_global_topic_manager
        from kafka_handlers.kafka_producer import get_kafka_producer as _get_global_kafka_producer
        
        # Get topic manager (guaranteed to be initialized by main.py)
        # Pass settings to it if it hasn't been initialized yet in this context
        topic_manager_instance = _get_global_topic_manager(settings=self.settings) 

        # Get producer (guaranteed to be initialized by main.py)
        # Pass settings and topic_manager to it if it hasn't been initialized yet in this context
        self.kafka_producer = _get_global_kafka_producer(settings=self.settings, topic_manager=topic_manager_instance)


        self.alert_history: List[Alert] = [] # Changed to list of Alert objects
        self.history_lock = threading.Lock()
        
        # Initialize alert generators, mapping AlertRuleType to specific generators
        self.generators = {
            AlertRuleType.MOTION_DETECTION: MotionBasedAlertGenerator(),
            AlertRuleType.OBJECT_DETECTION: ObjectBasedAlertGenerator(),
            AlertRuleType.ZONE_INTRUSION: ObjectBasedAlertGenerator(),
            AlertRuleType.LOITERING: BehaviorBasedAlertGenerator(),
            AlertRuleType.CROWD_DENSITY: BehaviorBasedAlertGenerator(),
            AlertRuleType.FACE_RECOGNITION: ObjectBasedAlertGenerator(),
            AlertRuleType.VEHICLE_DETECTION: ObjectBasedAlertGenerator(),
            AlertRuleType.ANOMALY_DETECTION: BehaviorBasedAlertGenerator(),
            AlertRuleType.THRESHOLD_BREACH: BehaviorBasedAlertGenerator(),
            AlertRuleType.CORRELATION_RULE: CorrelationAlertGenerator(),
        }
        logger.info("AlertEngine initialized with rule-specific generators.")

    async def initialize(self):
        """Asynchronously initialize the alert engine and its components."""
        # Initialize generators if they need async setup
        for generator in self.generators.values():
            if hasattr(generator, 'initialize'):
                await generator.initialize()
        
        logger.info("AlertEngine: Initialization complete.")
    
    async def shutdown(self):
        """Gracefully shutdown the alert engine and its components."""
        # Cleanup generators if needed
        for generator in self.generators.values():
            if hasattr(generator, 'shutdown'):
                await generator.shutdown()
        
        logger.info("AlertEngine: Shutdown complete.")

    def generate_alert(self, detection_event_data: Dict[str, Any]) -> Optional[Alert]:
        """
        Generates an alert based on a raw detection event dictionary.
        Dispatches to the appropriate generator based on the rule type.
        """
        start_time = time.time()
        generated_alert: Optional[Alert] = None

        try:
            # Parse raw detection event data into a dataclass model
            detection_event = DetectionEvent(**detection_event_data)

            # Get matching rules from the rule engine
            matching_rules = self.rule_engine.get_matching_rules(detection_event)
            
            if not matching_rules:
                logger.debug(f"No matching rules for detection event: {detection_event.id} ({detection_event.detection_type})")
                return None
            
            # Select the highest priority rule
            primary_rule = matching_rules[0]
            
            # Get necessary context for alert generation
            context = AlertContext(
                detection_event=detection_event,
                matched_rules=matching_rules,
                previous_alerts=self._get_recent_alerts(detection_event.camera_id),
                camera_status=self._get_camera_status(detection_event.camera_id),
                zone_info=self.rule_engine.zones.get(detection_event.zone_id),
                correlation_id=detection_event.correlation_id or str(uuid.uuid4())
            )
            
            # Use the specific generator based on rule type
            generator = self.generators.get(primary_rule.rule_type)
            if not generator:
                logger.warning(f"No specific generator found for rule type: {primary_rule.rule_type.value}. Alert not generated.")
                return None
            
            # Generate the alert object (should return an Alert Pydantic model)
            generated_alert = generator.generate_alert(primary_rule, detection_event)

            if not generated_alert:
                logger.debug(f"Generator did not produce an alert for rule {primary_rule.id}.")
                return None

            # Perform suppression check on the generated Alert object
            should_suppress, reason = self.suppression_manager.should_suppress_alert(generated_alert, primary_rule)
            if should_suppress:
                logger.info(f"Alert {generated_alert.id} suppressed: {reason}", extra={'alert_type': generated_alert.alert_type.value, 'reason': reason})
                return None
            
            # Store the generated alert in the AlertStore (async operation)
            # This should be called by the consumer loop (or caller of generate_alert)
            # because generate_alert is synchronous.
            self._store_alert_in_history(generated_alert) # Store in local history
            # The alert_store.store_alert(generated_alert) call should happen in the consumer
            # or in an async task from the consumer, as this method itself is sync.

            # Send to Kafka using the KafkaProducerClient
            self._send_alert_to_kafka(generated_alert)
            
            # Update metrics
            alerts_generated_total.labels(
                alert_type=generated_alert.alert_type.value,
                severity=generated_alert.severity.value,
                camera_id=generated_alert.camera_id
            ).inc()
            
            alert_rules_matched.labels(
                rule_name=primary_rule.name,
                camera_id=generated_alert.camera_id
            ).inc()
            
            processing_time = time.time() - start_time
            alert_processing_time.labels(
                alert_type=generated_alert.alert_type.value
            ).observe(processing_time)
            
            logger.info(f"Alert generated: {generated_alert.alert_type.value} for camera {generated_alert.camera_id} (ID: {generated_alert.id})")
            return generated_alert # Return the Alert object
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Error generating alert from detection event: {e}", exc_info=True, extra={'detection_event': detection_event_data})
            alerts_generated_total.labels(alert_type='error', severity='critical', camera_id='unknown').inc()
            alert_processing_time.labels(alert_type='error').observe(processing_time)
            return None
    
    def _create_alert(self, rule: AlertRule, context: AlertContext) -> Alert:
        detection_event = context.detection_event
        
        severity_enum = self._determine_alert_severity(rule, context)
        message_str = self._generate_alert_message(rule, context)

        alert_id = str(uuid.uuid4())
        correlation_id_val = context.correlation_id or alert_id

        return Alert(
            id=alert_id,
            alert_type=self._map_rule_type_to_alert_type(rule.rule_type),
            severity=severity_enum,
            message=message_str,
            camera_id=detection_event.camera_id,
            timestamp=datetime.utcnow(),
            zone_id=detection_event.zone_id,
            detection_type=detection_event.detection_type,
            confidence=detection_event.confidence,
            metadata={
                'detection_event_id': detection_event.id,
                'bounding_box': detection_event.bounding_box,
                'zone_info': asdict(context.zone_info) if context.zone_info else None,
                'matched_rules': [r.id for r in context.matched_rules],
                'camera_status': context.camera_status,
                'rule_id': rule.id,
                'rule_name': rule.name,
                'correlation_id': correlation_id_val
            }
        )
    
    def _determine_alert_severity(self, rule: AlertRule, context: AlertContext) -> AlertSeverity:
        detection_event = context.detection_event
        
        if rule.priority == 1: base_severity = AlertSeverity.CRITICAL
        elif rule.priority == 2: base_severity = AlertSeverity.HIGH
        elif rule.priority == 3: base_severity = AlertSeverity.MEDIUM
        else: base_severity = AlertSeverity.LOW
        
        if detection_event.confidence is not None and detection_event.confidence > 0.9:
            if base_severity == AlertSeverity.MEDIUM: base_severity = AlertSeverity.HIGH
            elif base_severity == AlertSeverity.LOW: base_severity = AlertSeverity.MEDIUM
        
        if context.zone_info and 'secure' in context.zone_info.name.lower():
            if base_severity == AlertSeverity.MEDIUM: base_severity = AlertSeverity.HIGH
            elif base_severity == AlertSeverity.LOW: base_severity = AlertSeverity.MEDIUM
        
        return base_severity
    
    def _generate_alert_message(self, rule: AlertRule, context: AlertContext) -> str:
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
            AlertRuleType.CORRELATION_RULE: f"Correlated event detected for camera {detection_event.camera_id}" # Fixed typo
        }
        
        base_message = messages.get(rule.rule_type, f"Alert triggered by rule {rule.name}")
        
        if detection_event.confidence is not None and detection_event.confidence > 0:
            base_message += f" (confidence: {detection_event.confidence:.2f})"
        
        if context.zone_info:
            base_message += f" in zone '{context.zone_info.name}'"
        
        return base_message
    
    def _map_rule_type_to_alert_type(self, rule_type: AlertRuleType) -> AlertType:
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
        return mapping.get(rule_type, AlertType.GENERAL)
    
    def _get_recent_alerts(self, camera_id: str, minutes: int = 30) -> List[Dict[str, Any]]:
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
        with self.history_lock:
            return [
                alert.model_dump() for alert in self.alert_history
                if alert.camera_id == camera_id and alert.timestamp > cutoff_time
            ]
    
    def _get_camera_status(self, camera_id: str) -> Dict[str, Any]:
        return {
            'camera_id': camera_id,
            'status': 'online',
            'last_seen': datetime.utcnow().isoformat()
        }
    
    def _store_alert_in_history(self, alert: Alert):
        with self.history_lock:
            self.alert_history.append(alert)
            if len(self.alert_history) > self.settings.alerts.max_alerts_in_memory:
                self.alert_history = self.alert_history[-self.settings.alerts.max_alerts_in_memory:]
    
    def _send_alert_to_kafka(self, alert: Alert):
        try:
            alert_dict = alert.model_dump()
            success = self.kafka_producer.send_message(
                TopicType.ALERTS,
                alert_dict,
                key=alert.camera_id,
                batch_mode=False
            )
            if success: logger.info(f"Alert sent to Kafka: {alert.id}")
            else: logger.error(f"Failed to send alert to Kafka: {alert.id}")
        except Exception as e:
            logger.error(f"Error sending alert {alert.id} to Kafka: {e}", exc_info=True)
    
    def get_alert_history(self, camera_id: Optional[str] = None, alert_type: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        with self.history_lock:
            filtered_alerts = [alert.model_dump() for alert in self.alert_history]
            if camera_id: filtered_alerts = [a for a in filtered_alerts if a['camera_id'] == camera_id]
            if alert_type: filtered_alerts = [a for a in filtered_alerts if a['alert_type'] == alert_type]
            filtered_alerts.sort(key=lambda x: x['timestamp'], reverse=True)
            return filtered_alerts[:limit]
    
    def get_alert_statistics(self) -> Dict[str, Any]:
        with self.history_lock:
            total_alerts = len(self.alert_history)
            if total_alerts == 0: return {'total_alerts': 0, 'alerts_by_type': {}, 'alerts_by_severity': {}, 'alerts_by_camera': {}}
            alerts_by_type = defaultdict(int)
            alerts_by_severity = defaultdict(int)
            alerts_by_camera = defaultdict(int)
            for alert in self.alert_history:
                alerts_by_type[alert.alert_type.value] += 1
                alerts_by_severity[alert.severity.value] += 1
                alerts_by_camera[alert.camera_id] += 1
            return {
                'total_alerts': total_alerts,
                'alerts_by_type': dict(alerts_by_type),
                'alerts_by_severity': dict(alerts_by_severity),
                'alerts_by_camera': dict(alerts_by_camera)
            }


# Placeholder generator classes (defined here, not in alert_schema.py)
# These are the same ones from the previous alert_logic.py
class BaseAlertGenerator:
    """Base class for alert generators."""
    async def initialize(self): pass
    async def shutdown(self): pass
    @abstractmethod
    def generate_alert(self, rule: AlertRule, detection_event: DetectionEvent) -> Optional[Alert]:
        """Abstract method to generate an Alert from a rule and detection event."""
        pass

class MotionBasedAlertGenerator(BaseAlertGenerator):
    def generate_alert(self, rule: AlertRule, detection_event: DetectionEvent) -> Optional[Alert]:
        logger.info(f"Generating motion-based alert for {detection_event.camera_id}")
        return Alert(
            id=str(uuid.uuid4()), alert_type=AlertType.MOTION, severity=AlertSeverity.MEDIUM,
            message=f"Motion detected in camera {detection_event.camera_id} via rule {rule.name}",
            camera_id=detection_event.camera_id, timestamp=datetime.utcnow(),
            detection_type=detection_event.detection_type, confidence=detection_event.confidence,
            zone_id=detection_event.zone_id, metadata=detection_event.metadata or {}
        )

class ObjectBasedAlertGenerator(BaseAlertGenerator):
    def generate_alert(self, rule: AlertRule, detection_event: DetectionEvent) -> Optional[Alert]:
        logger.info(f"Generating object-based alert for {detection_event.detection_type} in {detection_event.camera_id}")
        return Alert(
            id=str(uuid.uuid4()), alert_type=AlertType.DETECTION, severity=AlertSeverity.HIGH,
            message=f"{detection_event.detection_type.title()} detected in camera {detection_event.camera_id} via rule {rule.name}",
            camera_id=detection_event.camera_id, timestamp=datetime.utcnow(),
            detection_type=detection_event.detection_type, confidence=detection_event.confidence,
            zone_id=detection_event.zone_id, metadata=detection_event.metadata or {}
        )

class BehaviorBasedAlertGenerator(BaseAlertGenerator):
    def generate_alert(self, rule: AlertRule, detection_event: DetectionEvent) -> Optional[Alert]:
        logger.info(f"Generating behavior-based alert for {detection_event.camera_id}")
        return Alert(
            id=str(uuid.uuid4()), alert_type=AlertType.BEHAVIOR, severity=AlertSeverity.CRITICAL,
            message=f"Unusual behavior detected in camera {detection_event.camera_id} via rule {rule.name}",
            camera_id=detection_event.camera_id, timestamp=datetime.utcnow(),
            detection_type=detection_event.detection_type, confidence=detection_event.confidence,
            zone_id=detection_event.zone_id, metadata=detection_event.metadata or {}
        )

class CorrelationAlertGenerator(BaseAlertGenerator):
    def generate_alert(self, rule: AlertRule, detection_event: DetectionEvent) -> Optional[Alert]:
        logger.info(f"Generating correlation-based alert for {detection_event.camera_id}")
        return Alert(
            id=str(uuid.uuid4()), alert_type=AlertType.CORRELATION, severity=AlertSeverity.CRITICAL,
            message=f"Correlated event detected in camera {detection_event.camera_id} via rule {rule.name}",
            camera_id=detection_event.camera_id, timestamp=datetime.utcnow(),
            detection_type=detection_event.detection_type, confidence=detection_event.confidence,
            zone_id=detection_event.zone_id, metadata=detection_event.metadata or {}
        )


# Global alert engine instance (will be initialized in main.py's startup sequence)
_alert_engine_instance: Optional[AlertEngine] = None
_alert_engine_lock = threading.Lock()


def get_alert_engine(settings: 'AppSettings', alert_store: 'AppAlertStore') -> AlertEngine:
    """Get singleton alert engine instance."""
    global _alert_engine_instance
    if _alert_engine_instance is None:
        with _alert_engine_lock:
            if _alert_engine_instance is None:
                _alert_engine_instance = AlertEngine(settings, alert_store)
    return _alert_engine_instance


# Convenience function for generating alerts using the global alert engine
def generate_alert_from_data(detection_event_data: Dict[str, Any]) -> Optional[Alert]:
    """Generates an alert from raw detection data using the global alert engine."""
    if _alert_engine_instance:
        return _alert_engine_instance.generate_alert(detection_event_data)
    else:
        logger.error("AlertEngine not initialized globally. Cannot generate alert.")
        return None


if __name__ == "__main__":
    # Test the alert logic module directly
    from dotenv import load_dotenv
    from config.settings import get_settings as _get_app_settings
    from alerts.alert_store import AlertStore as AppAlertStore, get_alert_store as _get_app_alert_store

    print("--- Running alerts/alert_logic.py standalone test ---")
    
    load_dotenv() # Load .env for standalone test
    test_settings = _get_app_settings()

    # Initialize AlertStore and AlertEngine
    async def _init_alert_system_for_test():
        test_alert_store = AppAlertStore(test_settings) # Pass settings to AlertStore init
        await test_alert_store.initialize()
        
        # Get alert engine via getter, passing settings and initialized alert_store
        test_alert_engine = get_alert_engine(test_settings, test_alert_store)
        await test_alert_engine.initialize()
        return test_alert_engine, test_alert_store
    
    test_engine, test_store = asyncio.run(_init_alert_system_for_test())

    try:
        print("\n--- Testing alert generation ---")
        test_detection = {
            'id': str(uuid.uuid4()),
            'camera_id': 'camera_test_001',
            'detection_type': 'person',
            'confidence': 0.85,
            'timestamp': datetime.utcnow().isoformat(),
            'zone_id': 'secure_area',
            'bounding_box': {'x': 100, 'y': 200, 'width': 50, 'height': 100},
            'metadata': {'test_data': True}
        }
        
        # Manually create a rule for testing
        test_engine.rule_engine.add_rule(AlertRule(
            id="test_person_intrusion",
            name="Test Person Intrusion",
            rule_type=AlertRuleType.OBJECT_DETECTION,
            conditions={"detection_types": ["person"], "min_confidence": 0.8, "restricted_zones": ["secure_area"]},
            priority=1,
            cooldown_minutes=1
        ))

        # Test valid detection
        alert_1 = generate_alert_from_data(test_detection)
        if alert_1:
            print(f"Generated Alert 1 (ID: {alert_1.id}, Type: {alert_1.alert_type.value}, Severity: {alert_1.severity.value}): {alert_1.message}")
            # Manually store for test verification if AlertEngine didn't async store it
            asyncio.run(test_store.store_alert(alert_1))
        else:
            print("No alert 1 generated (might be suppressed or no rule match).")

        # Test suppressed detection (due to cooldown)
        print("\n--- Testing suppressed alert (cooldown) ---")
        alert_2 = generate_alert_from_data(test_detection)
        if alert_2:
            print("Generated Alert 2 (should be suppressed, something is wrong).")
        else:
            print("Alert 2 successfully suppressed (expected).")

        # Test detection not matching rule
        test_detection_no_match = {**test_detection, 'detection_type': 'dog'}
        alert_3 = generate_alert_from_data(test_detection_no_match)
        if alert_3:
            print("Generated Alert 3 (should not match, something is wrong).")
        else:
            print("Alert 3 successfully not generated (expected, no rule match).")

        print("\n--- Current Alert History ---")
        history = test_engine.get_alert_history(limit=5)
        for h in history:
            print(f"  - {h.get('alert_type')}: {h.get('message')}")

        print("\n--- Current Alert Statistics ---")
        stats = test_engine.get_alert_statistics()
        print(json.dumps(stats, indent=2))

    except Exception as e:
        print(f"An error occurred during alert logic test: {e}", file=sys.stderr)
        traceback.print_exc()
    finally:
        print("\n--- Shutting down alert system ---")
        asyncio.run(test_engine.shutdown())
        asyncio.run(test_store.close())
        print("Alert system shutdown complete.")