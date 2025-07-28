"""
Basic alert data structures and enums
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Any
# from alerts.alert_models import * AlertRule*
# from dataclasses import dataclass
# from typing import Dict, Any
# from alerts.alert_logic import AlertRuleType  # import your existing enum

# @dataclass
# class AlertRule:
#     id: str
#     rule_type: AlertRuleType
#     config: Dict[str, Any]  # config like thresholds, zones, etc.
#     camera_id: str

class AlertType(Enum):
    """Types of alerts"""
    DETECTION = "detection"
    INTRUSION = "intrusion"
    BEHAVIOR = "behavior"
    MOTION = "motion"
    ANOMALY = "anomaly"
    SYSTEM = "system"
    CORRELATION = "correlation"
    GENERAL = "general"

class AlertSeverity(Enum):
    """Alert severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

@dataclass
class Alert:
    """Basic alert data structure"""
    id: str
    alert_type: AlertType
    severity: AlertSeverity
    message: str
    camera_id: str
    timestamp: datetime
    zone_id: Optional[str] = None
    detection_type: Optional[str] = None
    confidence: Optional[float] = None
    metadata: Dict[str, Any] = None