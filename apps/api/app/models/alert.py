from sqlalchemy import Column, String, Boolean, DateTime, Text, ForeignKey, Index, Enum, Numeric, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum
from ..core.database import Base


class AlertCategory(enum.Enum):
    PRICE = "price"
    EARNINGS = "earnings"
    MA = "ma"  # M&A activity
    NEWS = "news"
    REGULATORY = "regulatory"
    TECHNICAL = "technical"
    VOLUME = "volume"
    CUSTOM = "custom"


class AlertSeverity(enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertStatus(enum.Enum):
    ACTIVE = "active"
    TRIGGERED = "triggered"
    SNOOZED = "snoozed"
    DISMISSED = "dismissed"
    EXPIRED = "expired"


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    company_id = Column(String, ForeignKey("companies.id"), nullable=True)  # Can be null for general alerts
    deal_id = Column(String, ForeignKey("deals.id"), nullable=True)
    
    # Alert details
    title = Column(String, nullable=False)
    message = Column(Text, nullable=False)
    category = Column(Enum(AlertCategory), nullable=False)
    severity = Column(Enum(AlertSeverity), default=AlertSeverity.MEDIUM)
    status = Column(Enum(AlertStatus), default=AlertStatus.ACTIVE)
    
    # Alert conditions (for custom alerts)
    conditions = Column(JSON, nullable=True)  # Flexible conditions storage
    # Example: {"type": "price", "operator": ">", "value": 100, "ticker": "AAPL"}
    
    # Trigger information
    triggered_at = Column(DateTime(timezone=True), nullable=True)
    trigger_price = Column(Numeric(10, 2), nullable=True)
    trigger_value = Column(String, nullable=True)
    
    # User actions
    is_read = Column(Boolean, default=False)
    read_at = Column(DateTime(timezone=True), nullable=True)
    snoozed_until = Column(DateTime(timezone=True), nullable=True)
    dismissed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Delivery preferences
    email_sent = Column(Boolean, default=False)
    push_sent = Column(Boolean, default=False)
    
    # AI-generated content
    ai_explanation = Column(Text, nullable=True)  # AI-generated explanation of the alert
    ai_confidence = Column(Numeric(3, 2), nullable=True)  # 0-1 confidence score
    
    # Metadata
    data_source = Column(String, nullable=True)
    expires_at = Column(DateTime(timezone=True), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relationships
    user = relationship("User", back_populates="alerts")
    company = relationship("Company", backref="alerts")
    deal = relationship("Deal", backref="alerts")
    
    # Indexes
    __table_args__ = (
        Index('ix_alerts_user_id', 'user_id'),
        Index('ix_alerts_company_id', 'company_id'),
        Index('ix_alerts_deal_id', 'deal_id'),
        Index('ix_alerts_category', 'category'),
        Index('ix_alerts_severity', 'severity'),
        Index('ix_alerts_status', 'status'),
        Index('ix_alerts_is_read', 'is_read'),
        Index('ix_alerts_triggered_at', 'triggered_at'),
        Index('ix_alerts_created_at', 'created_at'),
    )


class AlertHistory(Base):
    __tablename__ = "alert_history"

    id = Column(String, primary_key=True, index=True)
    alert_id = Column(String, ForeignKey("alerts.id"), nullable=False)
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    
    # Trigger details
    triggered_at = Column(DateTime(timezone=True), nullable=False)
    trigger_value = Column(String, nullable=True)  # Value that triggered the alert
    trigger_data = Column(JSON, nullable=True)  # Snapshot of data at trigger time
    
    # Notification status
    notification_sent = Column(Boolean, default=False)
    notification_sent_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    alert = relationship("Alert", backref="history")
    user = relationship("User", backref="alert_history")
    
    # Indexes
    __table_args__ = (
        Index('ix_alert_history_alert_id', 'alert_id'),
        Index('ix_alert_history_user_id', 'user_id'),
        Index('ix_alert_history_triggered_at', 'triggered_at'),
        Index('ix_alert_history_created_at', 'created_at'),
    )
