"""
Flexible filtering system for analytics data retrieval.
Supports multiple filter types, dynamic construction, validation,
complex combinations, presets, caching, and performance optimization.
"""

import json
import re
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from enum import Enum
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import asyncio
from functools import wraps
import logging

from pydantic import BaseModel, Field, validator, root_validator
from sqlalchemy import and_, or_, not_, func, text
from sqlalchemy.orm import Session, Query
from sqlalchemy.sql import Select
import aioredis
from fastapi import Query as FastAPIQuery, HTTPException, status
from fastapi.params import Depends

# Configuration
FILTER_CACHE_TTL = 300  # 5 minutes
MAX_FILTER_COMPLEXITY = 10  # Maximum depth of nested filters
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 1000

logger = logging.getLogger(__name__)

# Enums
class FilterType(str, Enum):
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    GREATER_THAN = "greater_than"
    GREATER_THAN_OR_EQUAL = "greater_than_or_equal"
    LESS_THAN = "less_than"
    LESS_THAN_OR_EQUAL = "less_than_or_equal"
    BETWEEN = "between"
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    REGEX = "regex"
    FUZZY = "fuzzy"
    GEO_WITHIN = "geo_within"
    TIME_RANGE = "time_range"

class LogicalOperator(str, Enum):
    AND = "and"
    OR = "or"
    NOT = "not"

class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"

class AlertSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AlertType(str, Enum):
    MOTION_DETECTION = "motion_detection"
    FACE_RECOGNITION = "face_recognition"
    OBJECT_DETECTION = "object_detection"
    ANOMALY_DETECTION = "anomaly_detection"
    SYSTEM_ERROR = "system_error"
    PERFORMANCE_ISSUE = "performance_issue"

# Base Filter Models
@dataclass
class FilterCondition:
    """Individual filter condition"""
    field: str
    operator: FilterType
    value: Any
    case_sensitive: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class LogicalExpression:
    """Logical expression combining multiple conditions"""
    operator: LogicalOperator
    conditions: List[Union[FilterCondition, 'LogicalExpression']]
    metadata: Dict[str, Any] = field(default_factory=dict)

# Pydantic Models for API
class FilterConditionModel(BaseModel):
    field: str = Field(..., description="Field name to filter on")
    operator: FilterType = Field(..., description="Filter operator")
    value: Any = Field(..., description="Filter value")
    case_sensitive: bool = Field(False, description="Case sensitive matching")
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @validator('field')
    def validate_field(cls, v):
        # Define allowed fields
        allowed_fields = {
            'id', 'camera_id', 'alert_type', 'severity', 'timestamp',
            'confidence', 'location', 'description', 'status', 'created_at',
            'updated_at', 'user_id', 'tenant_id', 'tags', 'metadata'
        }
        if v not in allowed_fields:
            raise ValueError(f"Field '{v}' is not allowed for filtering")
        return v

    @validator('value')
    def validate_value(cls, v, values):
        operator = values.get('operator')
        if operator == FilterType.BETWEEN and not isinstance(v, (list, tuple)):
            raise ValueError("BETWEEN operator requires a list/tuple of two values")
        if operator in [FilterType.IN, FilterType.NOT_IN] and not isinstance(v, (list, tuple)):
            raise ValueError("IN/NOT_IN operators require a list/tuple of values")
        return v

class LogicalExpressionModel(BaseModel):
    operator: LogicalOperator = Field(..., description="Logical operator")
    conditions: List[Union[FilterConditionModel, 'LogicalExpressionModel']] = Field(
        ..., description="List of filter conditions or nested expressions"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @validator('conditions')
    def validate_conditions(cls, v):
        if not v:
            raise ValueError("At least one condition is required")
        return v

    @root_validator
    def validate_complexity(cls, values):
        """Validate filter complexity to prevent performance issues"""
        def count_depth(obj, depth=0):
            if depth > MAX_FILTER_COMPLEXITY:
                raise ValueError(f"Filter complexity exceeds maximum depth of {MAX_FILTER_COMPLEXITY}")
            
            if isinstance(obj, dict) and 'conditions' in obj:
                for condition in obj['conditions']:
                    count_depth(condition, depth + 1)
            elif isinstance(obj, list):
                for item in obj:
                    count_depth(item, depth + 1)
        
        count_depth(values)
        return values

# Update forward reference
LogicalExpressionModel.model_rebuild()

class FilterRequest(BaseModel):
    """Complete filter request with pagination and sorting"""
    filter: Optional[Union[FilterConditionModel, LogicalExpressionModel]] = None
    sort_by: Optional[str] = Field(None, description="Field to sort by")
    sort_order: SortOrder = Field(SortOrder.DESC, description="Sort order")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description="Page size")
    include_count: bool = Field(False, description="Include total count in response")
    preset_name: Optional[str] = Field(None, description="Use saved filter preset")
    
    @validator('sort_by')
    def validate_sort_by(cls, v):
        if v is not None:
            allowed_sort_fields = {
                'id', 'camera_id', 'alert_type', 'severity', 'timestamp',
                'confidence', 'created_at', 'updated_at'
            }
            if v not in allowed_sort_fields:
                raise ValueError(f"Sort field '{v}' is not allowed")
        return v

class FilterPreset(BaseModel):
    """Saved filter preset"""
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    filter_config: Union[FilterConditionModel, LogicalExpressionModel]
    is_public: bool = Field(False, description="Public preset available to all users")
    tags: List[str] = Field(default_factory=list)
    created_by: Optional[str] = None
    tenant_id: Optional[str] = None

class FilterResponse(BaseModel):
    """Filter response with data and metadata"""
    data: List[Dict[str, Any]]
    total_count: Optional[int] = None
    page: int
    page_size: int
    has_next: bool
    has_previous: bool
    filter_metadata: Dict[str, Any] = Field(default_factory=dict)

class FilterStats(BaseModel):
    """Filter statistics and performance metrics"""
    total_filters_applied: int
    execution_time_ms: float
    cache_hit: bool
    result_count: int
    complexity_score: int

# Abstract Filter Plugin
class FilterPlugin(ABC):
    """Abstract base class for custom filter plugins"""
    
    @abstractmethod
    def apply(self, query: Query, condition: FilterCondition) -> Query:
        """Apply the filter to a SQLAlchemy query"""
        pass
    
    @abstractmethod
    def validate(self, condition: FilterCondition) -> bool:
        """Validate the filter condition"""
        pass
    
    @property
    @abstractmethod
    def supported_operators(self) -> List[FilterType]:
        """List of supported filter operators"""
        pass

# Built-in Filter Plugins
class TextFilterPlugin(FilterPlugin):
    """Plugin for text-based filtering"""
    
    def apply(self, query: Query, condition: FilterCondition) -> Query:
        field_attr = getattr(query.column_descriptions[0]['type'], condition.field, None)
        if not field_attr:
            raise ValueError(f"Field '{condition.field}' not found")
        
        value = condition.value
        if not condition.case_sensitive and isinstance(value, str):
            value = value.lower()
            field_attr = func.lower(field_attr)
        
        if condition.operator == FilterType.EQUALS:
            return query.filter(field_attr == value)
        elif condition.operator == FilterType.NOT_EQUALS:
            return query.filter(field_attr != value)
        elif condition.operator == FilterType.CONTAINS:
            return query.filter(field_attr.contains(value))
        elif condition.operator == FilterType.NOT_CONTAINS:
            return query.filter(~field_attr.contains(value))
        elif condition.operator == FilterType.STARTS_WITH:
            return query.filter(field_attr.startswith(value))
        elif condition.operator == FilterType.ENDS_WITH:
            return query.filter(field_attr.endswith(value))
        elif condition.operator == FilterType.REGEX:
            return query.filter(field_attr.op('REGEXP')(value))
        elif condition.operator == FilterType.IS_NULL:
            return query.filter(field_attr.is_(None))
        elif condition.operator == FilterType.IS_NOT_NULL:
            return query.filter(field_attr.isnot(None))
        else:
            raise ValueError(f"Unsupported operator: {condition.operator}")
    
    def validate(self, condition: FilterCondition) -> bool:
        return condition.operator in self.supported_operators
    
    @property
    def supported_operators(self) -> List[FilterType]:
        return [
            FilterType.EQUALS, FilterType.NOT_EQUALS, FilterType.CONTAINS,
            FilterType.NOT_CONTAINS, FilterType.STARTS_WITH, FilterType.ENDS_WITH,
            FilterType.REGEX, FilterType.IS_NULL, FilterType.IS_NOT_NULL
        ]

class NumericFilterPlugin(FilterPlugin):
    """Plugin for numeric filtering"""
    
    def apply(self, query: Query, condition: FilterCondition) -> Query:
        field_attr = getattr(query.column_descriptions[0]['type'], condition.field, None)
        if not field_attr:
            raise ValueError(f"Field '{condition.field}' not found")
        
        value = condition.value
        
        if condition.operator == FilterType.EQUALS:
            return query.filter(field_attr == value)
        elif condition.operator == FilterType.NOT_EQUALS:
            return query.filter(field_attr != value)
        elif condition.operator == FilterType.GREATER_THAN:
            return query.filter(field_attr > value)
        elif condition.operator == FilterType.GREATER_THAN_OR_EQUAL:
            return query.filter(field_attr >= value)
        elif condition.operator == FilterType.LESS_THAN:
            return query.filter(field_attr < value)
        elif condition.operator == FilterType.LESS_THAN_OR_EQUAL:
            return query.filter(field_attr <= value)
        elif condition.operator == FilterType.BETWEEN:
            return query.filter(field_attr.between(value[0], value[1]))
        elif condition.operator == FilterType.IN:
            return query.filter(field_attr.in_(value))
        elif condition.operator == FilterType.NOT_IN:
            return query.filter(~field_attr.in_(value))
        else:
            raise ValueError(f"Unsupported operator: {condition.operator}")
    
    def validate(self, condition: FilterCondition) -> bool:
        return condition.operator in self.supported_operators
    
    @property
    def supported_operators(self) -> List[FilterType]:
        return [
            FilterType.EQUALS, FilterType.NOT_EQUALS, FilterType.GREATER_THAN,
            FilterType.GREATER_THAN_OR_EQUAL, FilterType.LESS_THAN,
            FilterType.LESS_THAN_OR_EQUAL, FilterType.BETWEEN, FilterType.IN,
            FilterType.NOT_IN
        ]

class DateTimeFilterPlugin(FilterPlugin):
    """Plugin for datetime filtering"""
    
    def apply(self, query: Query, condition: FilterCondition) -> Query:
        field_attr = getattr(query.column_descriptions[0]['type'], condition.field, None)
        if not field_attr:
            raise ValueError(f"Field '{condition.field}' not found")
        
        value = condition.value
        
        # Convert string dates to datetime objects
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        elif isinstance(value, (list, tuple)):
            value = [datetime.fromisoformat(v.replace('Z', '+00:00')) if isinstance(v, str) else v for v in value]
        
        if condition.operator == FilterType.EQUALS:
            return query.filter(field_attr == value)
        elif condition.operator == FilterType.GREATER_THAN:
            return query.filter(field_attr > value)
        elif condition.operator == FilterType.GREATER_THAN_OR_EQUAL:
            return query.filter(field_attr >= value)
        elif condition.operator == FilterType.LESS_THAN:
            return query.filter(field_attr < value)
        elif condition.operator == FilterType.LESS_THAN_OR_EQUAL:
            return query.filter(field_attr <= value)
        elif condition.operator == FilterType.BETWEEN:
            return query.filter(field_attr.between(value[0], value[1]))
        elif condition.operator == FilterType.TIME_RANGE:
            # Special time range handling
            if isinstance(value, dict):
                start_time = value.get('start')
                end_time = value.get('end')
                if start_time:
                    query = query.filter(field_attr >= start_time)
                if end_time:
                    query = query.filter(field_attr <= end_time)
            return query
        else:
            raise ValueError(f"Unsupported operator: {condition.operator}")
    
    def validate(self, condition: FilterCondition) -> bool:
        return condition.operator in self.supported_operators
    
    @property
    def supported_operators(self) -> List[FilterType]:
        return [
            FilterType.EQUALS, FilterType.GREATER_THAN, FilterType.GREATER_THAN_OR_EQUAL,
            FilterType.LESS_THAN, FilterType.LESS_THAN_OR_EQUAL, FilterType.BETWEEN,
            FilterType.TIME_RANGE
        ]

# Filter Cache
class FilterCache:
    """Cache for filter results"""
    
    def __init__(self, redis_client):
        self.redis = redis_client
    
    def _generate_cache_key(self, filter_request: FilterRequest, tenant_id: str) -> str:
        """Generate cache key for filter request"""
        filter_dict = filter_request.dict()
        filter_str = json.dumps(filter_dict, sort_keys=True, default=str)
        key_data = f"{tenant_id}:{filter_str}"
        return f"filter_cache:{hashlib.md5(key_data.encode()).hexdigest()}"
    
    async def get(self, filter_request: FilterRequest, tenant_id: str) -> Optional[Dict[str, Any]]:
        """Get cached filter result"""
        cache_key = self._generate_cache_key(filter_request, tenant_id)
        cached_data = await self.redis.get(cache_key)
        
        if cached_data:
            return json.loads(cached_data)
        return None
    
    async def set(self, filter_request: FilterRequest, tenant_id: str, 
                  result: Dict[str, Any], ttl: int = FILTER_CACHE_TTL):
        """Cache filter result"""
        cache_key = self._generate_cache_key(filter_request, tenant_id)
        await self.redis.setex(cache_key, ttl, json.dumps(result, default=str))
    
    async def invalidate_pattern(self, pattern: str):
        """Invalidate cache entries matching pattern"""
        keys = await self.redis.keys(f"filter_cache:{pattern}*")
        if keys:
            await self.redis.delete(*keys)

# Filter Engine
class FilterEngine:
    """Main filter engine for processing filter requests"""
    
    def __init__(self, redis_client=None):
        self.plugins: Dict[str, FilterPlugin] = {}
        self.presets: Dict[str, FilterPreset] = {}
        self.cache = FilterCache(redis_client) if redis_client else None
        
        # Register built-in plugins
        self.register_plugin('text', TextFilterPlugin())
        self.register_plugin('numeric', NumericFilterPlugin())
        self.register_plugin('datetime', DateTimeFilterPlugin())
    
    def register_plugin(self, name: str, plugin: FilterPlugin):
        """Register a filter plugin"""
        self.plugins[name] = plugin
    
    def register_preset(self, name: str, preset: FilterPreset):
        """Register a filter preset"""
        self.presets[name] = preset
    
    def _get_plugin_for_field(self, field: str) -> FilterPlugin:
        """Get appropriate plugin for field type"""
        # Field type mapping
        field_type_mapping = {
            'id': 'text',
            'camera_id': 'text',
            'alert_type': 'text',
            'severity': 'text',
            'timestamp': 'datetime',
            'confidence': 'numeric',
            'created_at': 'datetime',
            'updated_at': 'datetime',
            'user_id': 'text',
            'tenant_id': 'text',
            'status': 'text',
            'description': 'text',
            'location': 'text',
            'tags': 'text'
        }
        
        plugin_type = field_type_mapping.get(field, 'text')
        return self.plugins.get(plugin_type)
    
    def _apply_condition(self, query: Query, condition: FilterCondition) -> Query:
        """Apply a single filter condition"""
        plugin = self._get_plugin_for_field(condition.field)
        if not plugin:
            raise ValueError(f"No plugin found for field: {condition.field}")
        
        if not plugin.validate(condition):
            raise ValueError(f"Invalid condition for field: {condition.field}")
        
        return plugin.apply(query, condition)
    
    def _apply_logical_expression(self, query: Query, expression: LogicalExpression) -> Query:
        """Apply a logical expression"""
        if expression.operator == LogicalOperator.AND:
            for condition in expression.conditions:
                query = self._apply_filter(query, condition)
        elif expression.operator == LogicalOperator.OR:
            or_conditions = []
            for condition in expression.conditions:
                # Create a subquery for each OR condition
                subquery = self._apply_filter(query, condition)
                or_conditions.append(subquery.whereclause)
            query = query.filter(or_(*or_conditions))
        elif expression.operator == LogicalOperator.NOT:
            if len(expression.conditions) != 1:
                raise ValueError("NOT operator requires exactly one condition")
            condition = expression.conditions[0]
            subquery = self._apply_filter(query, condition)
            query = query.filter(not_(subquery.whereclause))
        
        return query
    
    def _apply_filter(self, query: Query, filter_obj: Union[FilterCondition, LogicalExpression]) -> Query:
        """Apply a filter (condition or logical expression)"""
        if isinstance(filter_obj, FilterCondition):
            return self._apply_condition(query, filter_obj)
        elif isinstance(filter_obj, LogicalExpression):
            return self._apply_logical_expression(query, filter_obj)
        else:
            raise ValueError(f"Unknown filter type: {type(filter_obj)}")
    
    def _convert_pydantic_to_dataclass(self, obj: Union[FilterConditionModel, LogicalExpressionModel]) -> Union[FilterCondition, LogicalExpression]:
        """Convert Pydantic models to dataclass objects"""
        if isinstance(obj, FilterConditionModel):
            return FilterCondition(
                field=obj.field,
                operator=obj.operator,
                value=obj.value,
                case_sensitive=obj.case_sensitive,
                metadata=obj.metadata
            )
        elif isinstance(obj, LogicalExpressionModel):
            conditions = [self._convert_pydantic_to_dataclass(c) for c in obj.conditions]
            return LogicalExpression(
                operator=obj.operator,
                conditions=conditions,
                metadata=obj.metadata
            )
        else:
            raise ValueError(f"Unknown model type: {type(obj)}")
    
    def _calculate_complexity_score(self, filter_obj: Union[FilterCondition, LogicalExpression]) -> int:
        """Calculate complexity score for a filter"""
        if isinstance(filter_obj, FilterCondition):
            return 1
        elif isinstance(filter_obj, LogicalExpression):
            score = 1
            for condition in filter_obj.conditions:
                score += self._calculate_complexity_score(condition)
            return score
        return 0
    
    async def apply_filters(self, query: Query, filter_request: FilterRequest, 
                          tenant_id: str) -> Tuple[Query, FilterStats]:
        """Apply filters to a query with caching and statistics"""
        start_time = datetime.now()
        cache_hit = False
        
        # Check cache first
        if self.cache:
            cached_result = await self.cache.get(filter_request, tenant_id)
            if cached_result:
                cache_hit = True
                # Note: For demonstration, we're showing cache structure
                # In practice, you'd need to reconstruct the query from cache
        
        # Apply preset if specified
        if filter_request.preset_name:
            preset = self.presets.get(filter_request.preset_name)
            if not preset:
                raise ValueError(f"Filter preset '{filter_request.preset_name}' not found")
            filter_request.filter = preset.filter_config
        
        # Apply filters
        if filter_request.filter:
            filter_obj = self._convert_pydantic_to_dataclass(filter_request.filter)
            query = self._apply_filter(query, filter_obj)
        
        # Apply sorting
        if filter_request.sort_by:
            sort_field = getattr(query.column_descriptions[0]['type'], filter_request.sort_by, None)
            if sort_field:
                if filter_request.sort_order == SortOrder.DESC:
                    query = query.order_by(sort_field.desc())
                else:
                    query = query.order_by(sort_field.asc())
        
        # Apply pagination
        offset = (filter_request.page - 1) * filter_request.page_size
        query = query.offset(offset).limit(filter_request.page_size)
        
        # Calculate statistics
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        complexity_score = 0
        if filter_request.filter:
            filter_obj = self._convert_pydantic_to_dataclass(filter_request.filter)
            complexity_score = self._calculate_complexity_score(filter_obj)
        
        stats = FilterStats(
            total_filters_applied=1 if filter_request.filter else 0,
            execution_time_ms=execution_time,
            cache_hit=cache_hit,
            result_count=0,  # Will be updated after query execution
            complexity_score=complexity_score
        )
        
        return query, stats

# Query parameter parsing
def parse_query_params(
    camera_id: Optional[str] = FastAPIQuery(None),
    alert_type: Optional[AlertType] = FastAPIQuery(None),
    severity: Optional[AlertSeverity] = FastAPIQuery(None),
    start_time: Optional[datetime] = FastAPIQuery(None),
    end_time: Optional[datetime] = FastAPIQuery(None),
    confidence_min: Optional[float] = FastAPIQuery(None, ge=0.0, le=1.0),
    confidence_max: Optional[float] = FastAPIQuery(None, ge=0.0, le=1.0),
    status: Optional[str] = FastAPIQuery(None),
    tags: Optional[List[str]] = FastAPIQuery(None),
    search: Optional[str] = FastAPIQuery(None),
    sort_by: Optional[str] = FastAPIQuery("timestamp"),
    sort_order: SortOrder = FastAPIQuery(SortOrder.DESC),
    page: int = FastAPIQuery(1, ge=1),
    page_size: int = FastAPIQuery(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)
) -> FilterRequest:
    """Parse query parameters into FilterRequest"""
    conditions = []
    
    # Add individual conditions
    if camera_id:
        conditions.append(FilterConditionModel(
            field="camera_id",
            operator=FilterType.EQUALS,
            value=camera_id
        ))
    
    if alert_type:
        conditions.append(FilterConditionModel(
            field="alert_type",
            operator=FilterType.EQUALS,
            value=alert_type.value
        ))
    
    if severity:
        conditions.append(FilterConditionModel(
            field="severity",
            operator=FilterType.EQUALS,
            value=severity.value
        ))
    
    if start_time and end_time:
        conditions.append(FilterConditionModel(
            field="timestamp",
            operator=FilterType.BETWEEN,
            value=[start_time, end_time]
        ))
    elif start_time:
        conditions.append(FilterConditionModel(
            field="timestamp",
            operator=FilterType.GREATER_THAN_OR_EQUAL,
            value=start_time
        ))
    elif end_time:
        conditions.append(FilterConditionModel(
            field="timestamp",
            operator=FilterType.LESS_THAN_OR_EQUAL,
            value=end_time
        ))
    
    if confidence_min is not None and confidence_max is not None:
        conditions.append(FilterConditionModel(
            field="confidence",
            operator=FilterType.BETWEEN,
            value=[confidence_min, confidence_max]
        ))
    elif confidence_min is not None:
        conditions.append(FilterConditionModel(
            field="confidence",
            operator=FilterType.GREATER_THAN_OR_EQUAL,
            value=confidence_min
        ))
    elif confidence_max is not None:
        conditions.append(FilterConditionModel(
            field="confidence",
            operator=FilterType.LESS_THAN_OR_EQUAL,
            value=confidence_max
        ))
    
    if status:
        conditions.append(FilterConditionModel(
            field="status",
            operator=FilterType.EQUALS,
            value=status
        ))
    
    if tags:
        conditions.append(FilterConditionModel(
            field="tags",
            operator=FilterType.IN,
            value=tags
        ))
    
    if search:
        # Create a search condition that looks in multiple fields
        search_conditions = [
            FilterConditionModel(
                field="description",
                operator=FilterType.CONTAINS,
                value=search,
                case_sensitive=False
            ),
            FilterConditionModel(
                field="camera_id",
                operator=FilterType.CONTAINS,
                value=search,
                case_sensitive=False
            )
        ]
        conditions.append(LogicalExpressionModel(
            operator=LogicalOperator.OR,
            conditions=search_conditions
        ))
    
    # Combine all conditions with AND
    filter_obj = None
    if conditions:
        if len(conditions) == 1:
            filter_obj = conditions[0]
        else:
            filter_obj = LogicalExpressionModel(
                operator=LogicalOperator.AND,
                conditions=conditions
            )
    
    return FilterRequest(
        filter=filter_obj,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        page_size=page_size
    )

# Preset management
class PresetManager:
    """Manager for filter presets"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_preset(self, preset: FilterPreset, user_id: str) -> FilterPreset:
        """Create a new filter preset"""
        preset.created_by = user_id
        # In a real implementation, you'd save to database
        return preset
    
    def get_preset(self, name: str, user_id: str, tenant_id: str) -> Optional[FilterPreset]:
        """Get a filter preset"""
        # In a real implementation, you'd query the database
        return None
    
    def list_presets(self, user_id: str, tenant_id: str) -> List[FilterPreset]:
        """List available filter presets"""
        # In a real implementation, you'd query the database
        return []
    
    def update_preset(self, name: str, preset: FilterPreset, user_id: str) -> FilterPreset:
        """Update a filter preset"""
        # In a real implementation, you'd update the database
        return preset
    
    def delete_preset(self, name: str, user_id: str) -> bool:
        """Delete a filter preset"""
        # In a real implementation, you'd delete from database
        return True

# Filter utilities
def create_quick_filters() -> Dict[str, FilterPreset]:
    """Create common quick filter presets"""
    return {
        "critical_alerts": FilterPreset(
            name="Critical Alerts",
            description="High and critical severity alerts",
            filter_config=FilterConditionModel(
                field="severity",
                operator=FilterType.IN,
                value=["high", "critical"]
            ),
            is_public=True
        ),
        "recent_alerts": FilterPreset(
            name="Recent Alerts",
            description="Alerts from the last 24 hours",
            filter_config=FilterConditionModel(
                field="timestamp",
                operator=FilterType.GREATER_THAN,
                value=datetime.now() - timedelta(days=1)
            ),
            is_public=True
        ),
        "motion_detection": FilterPreset(
            name="Motion Detection",
            description="Motion detection alerts only",
            filter_config=FilterConditionModel(
                field="alert_type",
                operator=FilterType.EQUALS,
                value="motion_detection"
            ),
            is_public=True
        )
    }

# Export main components
__all__ = [
    'FilterEngine', 'FilterRequest', 'FilterResponse', 'FilterStats',
    'FilterConditionModel', 'LogicalExpressionModel', 'FilterPreset',
    'PresetManager', 'FilterPlugin', 'FilterType', 'LogicalOperator',
    'parse_query_params', 'create_quick_filters'
]