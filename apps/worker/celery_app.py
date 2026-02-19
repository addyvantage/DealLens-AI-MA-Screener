import os
import logging
from celery import Celery
from celery.schedules import crontab
from celery.signals import setup_logging, worker_ready
import sys

# Ensure API modules are importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../api'))

# Get configuration from environment with validation
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/deallens")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Validate critical environment variables
def validate_config():
    """Validate critical configuration on startup"""
    errors = []
    
    if not REDIS_URL.startswith("redis://"):
        errors.append("REDIS_URL must be a valid Redis connection string")
    
    if not DATABASE_URL.startswith(("postgresql://", "postgres://")):
        errors.append("DATABASE_URL must be a valid PostgreSQL connection string")
    
    if ENVIRONMENT == "production":
        required_keys = {
            "NEWSAPI_KEY": os.getenv("NEWSAPI_KEY"),
            "ALPHAVANTAGE_KEY": os.getenv("ALPHAVANTAGE_KEY"), 
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        }
        
        for key_name, key_value in required_keys.items():
            if not key_value:
                errors.append(f"{key_name} is required in production")
    
    if errors:
        for error in errors:
            logging.error(f"Configuration error: {error}")
        raise SystemExit(1)
    
    logging.info(f"Worker configuration validated for {ENVIRONMENT} environment")

# Validate configuration
validate_config()

# Initialize Celery app
app = Celery(
    "deallens_worker",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=[
        "tasks.sync_market",
        "tasks.sync_news", 
        "tasks.generate_insights",
        "tasks.evaluate_alerts",
        "tasks.send_notifications"
    ]
)

# Celery configuration
app.conf.update(
    # Serialization
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    
    # Timezone
    timezone="UTC",
    enable_utc=True,
    
    # Task routing and execution
    task_routes={
        "tasks.sync_market.*": {"queue": "market_data"},
        "tasks.sync_news.*": {"queue": "news"},
        "tasks.generate_insights.*": {"queue": "ai"},
        "tasks.evaluate_alerts.*": {"queue": "alerts"},
        "tasks.send_notifications.*": {"queue": "notifications"},
    },
    
    # Task result expiration
    result_expires=3600,
    
    # Worker configuration
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=1000,
    
    # Retry and error handling
    task_reject_on_worker_lost=True,
    task_default_retry_delay=60,  # 60 seconds
    task_max_retries=3,
    
    # Rate limiting
    task_annotations={
        'tasks.sync_news.*': {'rate_limit': '10/m'},  # 10 calls per minute
        'tasks.sync_market.*': {'rate_limit': '60/m'},  # 60 calls per minute
        'tasks.generate_insights.*': {'rate_limit': '5/m'},  # 5 AI calls per minute
    },
    
    # Compression for large payloads
    task_compression='gzip',
    result_compression='gzip',
    
    # Beat schedule for periodic tasks
    beat_schedule={
        # Market data sync - every minute during market hours
        "sync-prices-1min": {
            "task": "tasks.sync_market.sync_company_prices",
            "schedule": 60.0,  # Every 60 seconds
            "args": (["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "JPM"],),
        },
        
        # News sync - every 10 minutes
        "sync-news-10min": {
            "task": "tasks.sync_news.sync_company_news",
            "schedule": 600.0,  # Every 10 minutes
            "args": (["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "JPM"],),
        },
        
        # M&A news sync - every 15 minutes
        "sync-ma-news-15min": {
            "task": "tasks.sync_news.sync_ma_news",
            "schedule": 900.0,  # Every 15 minutes
        },
        
        # Alert evaluation - every minute
        "evaluate-alerts-1min": {
            "task": "tasks.evaluate_alerts.evaluate_all_alerts",
            "schedule": 60.0,  # Every 60 seconds
        },
        
        # AI company insights - daily at 3 AM
        "ai-company-insights-daily": {
            "task": "tasks.generate_insights.generate_daily_company_insights",
            "schedule": crontab(hour=3, minute=0),
        },
        
        # AI deal insights - daily at 4 AM  
        "ai-deal-insights-daily": {
            "task": "tasks.generate_insights.generate_daily_deal_insights",
            "schedule": crontab(hour=4, minute=0),
        },
        
        # AI analytics commentary - daily at 5 AM
        "ai-analytics-daily": {
            "task": "tasks.generate_insights.generate_analytics_commentary",
            "schedule": crontab(hour=5, minute=0),
            "args": ("12M",),
        },
        
        # Extended market data sync - every 30 minutes
        "sync-fundamentals-30min": {
            "task": "tasks.sync_market.sync_company_fundamentals",
            "schedule": 1800.0,  # Every 30 minutes
            "args": (["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "JPM"],),
        },
    },
)

# Logging setup
@setup_logging.connect
def config_loggers(*args, **kwargs):
    from logging.config import dictConfig
    import json
    
    # JSON logging configuration
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'json': {
                'format': '%(asctime)s %(name)s %(levelname)s %(message)s',
                'class': 'pythonjsonlogger.jsonlogger.JsonFormatter'
            },
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'json' if ENVIRONMENT == 'production' else 'standard',
                'stream': 'ext://sys.stdout',
            },
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['console'],
                'level': LOG_LEVEL,
                'propagate': False,
            },
            'celery': {
                'handlers': ['console'],
                'level': LOG_LEVEL,
                'propagate': False,
            },
            'tasks': {
                'handlers': ['console'],
                'level': LOG_LEVEL,
                'propagate': False,
            },
        },
    }
    
    try:
        dictConfig(logging_config)
    except Exception:
        # Fallback to basic logging if JSON logger isn't available
        logging.basicConfig(
            level=getattr(logging, LOG_LEVEL),
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        )

@worker_ready.connect
def worker_ready(sender=None, **kwargs):
    """Log when worker is ready"""
    logging.info(f"Worker ready - Environment: {ENVIRONMENT}, Queues: {getattr(sender.app.control, 'active_queues', 'default')}")

# Task autodiscovery
app.autodiscover_tasks()

if __name__ == "__main__":
    app.start()
