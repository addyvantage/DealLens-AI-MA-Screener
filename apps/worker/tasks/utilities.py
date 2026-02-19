import hashlib
import time
import logging
from functools import wraps
from typing import Any, Callable, Optional
from celery import current_task
from celery.exceptions import Retry
import redis
from datetime import datetime, timedelta

# Redis client for caching and idempotency
try:
    import os
    redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
except Exception as e:
    logging.error(f"Failed to connect to Redis: {e}")
    redis_client = None

logger = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiter for external API calls"""
    
    def __init__(self, service_name: str, calls_per_minute: int = 60):
        self.service_name = service_name
        self.calls_per_minute = calls_per_minute
        self.window_size = 60  # 1 minute window
    
    def is_allowed(self) -> bool:
        """Check if call is allowed within rate limit"""
        if not redis_client:
            return True  # Allow if Redis unavailable
        
        key = f"rate_limit:{self.service_name}:{int(time.time() // self.window_size)}"
        
        try:
            current_calls = redis_client.get(key)
            if current_calls is None:
                current_calls = 0
            else:
                current_calls = int(current_calls)
            
            if current_calls >= self.calls_per_minute:
                return False
            
            # Increment counter with expiration
            redis_client.incr(key)
            redis_client.expire(key, self.window_size)
            
            return True
        except Exception as e:
            logger.error(f"Rate limiter error for {self.service_name}: {e}")
            return True  # Allow if error (fail open)
    
    def wait_time(self) -> int:
        """Get wait time in seconds until next call is allowed"""
        current_window = int(time.time() // self.window_size)
        next_window = (current_window + 1) * self.window_size
        return int(next_window - time.time())


def rate_limited(service_name: str, calls_per_minute: int = 60):
    """Decorator to add rate limiting to tasks"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            limiter = RateLimiter(service_name, calls_per_minute)
            
            if not limiter.is_allowed():
                wait_time = limiter.wait_time()
                logger.warning(f"Rate limit hit for {service_name}, retrying in {wait_time}s")
                raise current_task.retry(countdown=wait_time)
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


def idempotent(key_func: Optional[Callable] = None, ttl: int = 3600):
    """Decorator to make tasks idempotent"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not redis_client:
                return func(*args, **kwargs)
            
            # Generate idempotency key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # Default: hash function name and arguments
                task_signature = f"{func.__name__}:{str(args)}:{str(kwargs)}"
                cache_key = f"idempotent:{hashlib.md5(task_signature.encode()).hexdigest()}"
            
            try:
                # Check if task already completed
                cached_result = redis_client.get(cache_key)
                if cached_result is not None:
                    logger.info(f"Task {func.__name__} already completed, returning cached result")
                    return cached_result
                
                # Execute task
                result = func(*args, **kwargs)
                
                # Cache result
                redis_client.setex(cache_key, ttl, str(result))
                
                return result
            except Exception as e:
                logger.error(f"Idempotency check failed for {func.__name__}: {e}")
                return func(*args, **kwargs)
        
        return wrapper
    return decorator


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: int = 60,
    backoff_factor: float = 2.0,
    jitter: bool = True
):
    """Decorator to add exponential backoff retry logic"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if attempt == max_retries:
                        logger.error(f"Task {func.__name__} failed after {max_retries} retries")
                        raise exc
                    
                    # Calculate delay with exponential backoff
                    delay = base_delay * (backoff_factor ** attempt)
                    
                    # Add jitter to prevent thundering herd
                    if jitter:
                        import random
                        delay *= (0.5 + random.random() * 0.5)
                    
                    delay = int(delay)
                    
                    logger.warning(
                        f"Task {func.__name__} failed on attempt {attempt + 1}/{max_retries + 1}, "
                        f"retrying in {delay}s. Error: {exc}"
                    )
                    
                    if hasattr(current_task, 'retry'):
                        raise current_task.retry(countdown=delay, exc=exc)
                    else:
                        time.sleep(delay)
        
        return wrapper
    return decorator


class CostTracker:
    """Track and limit API costs"""
    
    def __init__(self, service_name: str, daily_limit: float = 100.0):
        self.service_name = service_name
        self.daily_limit = daily_limit
    
    def track_cost(self, cost: float) -> bool:
        """Track cost and return True if under limit"""
        if not redis_client:
            return True
        
        today = datetime.utcnow().strftime("%Y-%m-%d")
        key = f"cost_tracker:{self.service_name}:{today}"
        
        try:
            current_cost = redis_client.get(key)
            current_cost = float(current_cost or 0)
            
            new_cost = current_cost + cost
            
            if new_cost > self.daily_limit:
                logger.error(
                    f"Daily cost limit exceeded for {self.service_name}: "
                    f"${new_cost:.2f} > ${self.daily_limit:.2f}"
                )
                return False
            
            # Update cost with 24 hour expiration
            redis_client.setex(key, 86400, new_cost)
            
            logger.info(
                f"Cost tracked for {self.service_name}: ${cost:.2f} "
                f"(Daily total: ${new_cost:.2f}/${self.daily_limit:.2f})"
            )
            
            return True
        except Exception as e:
            logger.error(f"Cost tracking error for {self.service_name}: {e}")
            return True  # Allow if error (fail open)


def cost_limited(service_name: str, cost_per_call: float, daily_limit: float = 100.0):
    """Decorator to add cost limiting to tasks"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            tracker = CostTracker(service_name, daily_limit)
            
            if not tracker.track_cost(cost_per_call):
                raise Exception(f"Daily cost limit exceeded for {service_name}")
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


def log_task_execution(func: Callable) -> Callable:
    """Decorator to log task execution details"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        task_id = getattr(current_task, 'request', {}).get('id', 'unknown')
        
        logger.info(
            f"Task started: {func.__name__}",
            extra={
                "task_name": func.__name__,
                "task_id": task_id,
                "action": "start",
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            
            logger.info(
                f"Task completed: {func.__name__} in {duration:.2f}s",
                extra={
                    "task_name": func.__name__,
                    "task_id": task_id,
                    "action": "complete",
                    "duration": duration,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            return result
        except Exception as exc:
            duration = time.time() - start_time
            
            logger.error(
                f"Task failed: {func.__name__} after {duration:.2f}s - {exc}",
                extra={
                    "task_name": func.__name__,
                    "task_id": task_id,
                    "action": "error",
                    "duration": duration,
                    "error": str(exc),
                    "timestamp": datetime.utcnow().isoformat()
                },
                exc_info=True
            )
            raise
    
    return wrapper

import os
import asyncio
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
import httpx
import redis
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import json
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/deallens")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# API Keys
ALPHAVANTAGE_KEY = os.getenv("ALPHAVANTAGE_KEY")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY") 
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Create database engine
engine = create_engine(
    DATABASE_URL,
    poolclass=StaticPool,
    pool_pre_ping=True,
    pool_recycle=300,
    echo=False
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Redis client
redis_client = redis.from_url(REDIS_URL, decode_responses=True)


@contextmanager
def get_db_session() -> Session:
    """Get database session with automatic cleanup."""
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database error: {e}")
        db.rollback()
        raise
    finally:
        db.close()


async def fetch_alphavantage_data(function: str, symbol: str = None, **params) -> Optional[Dict]:
    """Fetch data from AlphaVantage API with rate limiting and error handling."""
    if not ALPHAVANTAGE_KEY:
        logger.error("AlphaVantage API key not configured")
        return None
    
    base_url = "https://www.alphavantage.co/query"
    request_params = {
        "function": function,
        "apikey": ALPHAVANTAGE_KEY,
        **params
    }
    
    if symbol:
        request_params["symbol"] = symbol
    
    # Rate limiting - AlphaVantage allows 5 calls per minute for free tier
    rate_limit_key = f"alphavantage_rate_limit"
    current_calls = redis_client.get(rate_limit_key)
    
    if current_calls and int(current_calls) >= 5:
        logger.warning("AlphaVantage rate limit reached, waiting...")
        time.sleep(60)  # Wait 1 minute
        redis_client.delete(rate_limit_key)
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(base_url, params=request_params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if "Error Message" in data:
                logger.error(f"AlphaVantage API Error: {data['Error Message']}")
                return None
                
            if "Information" in data:
                logger.warning(f"AlphaVantage API Info: {data['Information']}")
                return None
            
            # Increment rate limit counter
            redis_client.incr(rate_limit_key)
            redis_client.expire(rate_limit_key, 60)  # Expire after 1 minute
            
            return data
            
    except Exception as e:
        logger.error(f"Error fetching AlphaVantage data: {e}")
        return None


async def fetch_newsapi_data(endpoint: str, **params) -> Optional[Dict]:
    """Fetch data from NewsAPI with rate limiting."""
    if not NEWSAPI_KEY:
        logger.error("NewsAPI key not configured")
        return None
    
    base_url = f"https://newsapi.org/v2/{endpoint}"
    headers = {"X-API-Key": NEWSAPI_KEY}
    
    # Rate limiting - NewsAPI allows 1000 requests per day for free tier
    rate_limit_key = f"newsapi_rate_limit"
    current_calls = redis_client.get(rate_limit_key)
    
    if current_calls and int(current_calls) >= 1000:
        logger.warning("NewsAPI daily rate limit reached")
        return None
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("status") != "ok":
                logger.error(f"NewsAPI Error: {data.get('message', 'Unknown error')}")
                return None
            
            # Increment rate limit counter
            redis_client.incr(rate_limit_key)
            redis_client.expire(rate_limit_key, 86400)  # Expire after 24 hours
            
            return data
            
    except Exception as e:
        logger.error(f"Error fetching NewsAPI data: {e}")
        return None


async def fetch_news_data(query: Optional[str] = None, **params) -> Optional[Dict]:
    """Wrapper for fetch_newsapi_data to handle query/endpoint logic."""
    endpoint = "everything" if query else "top-headlines"
    if query:
        params["q"] = query
    # Default country to us for top-headlines if no query
    if endpoint == "top-headlines" and "country" not in params and "category" not in params:
        params["country"] = "us"
    return await fetch_newsapi_data(endpoint, **params)


async def generate_openai_insight(prompt: str, max_tokens: int = 1000) -> Optional[Dict]:
    """Generate AI insight using OpenAI API."""
    if not OPENAI_API_KEY:
        logger.error("OpenAI API key not configured")
        return None
    
    try:
        import openai
        openai.api_key = OPENAI_API_KEY
        
        response = await openai.ChatCompletion.acreate(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {
                    "role": "system", 
                    "content": "You are a professional financial analyst providing concise, actionable insights for M&A and investment decisions."
                },
                {"role": "user", "content": prompt}
            ],
            max_tokens=max_tokens,
            temperature=0.3,
            presence_penalty=0.0,
            frequency_penalty=0.0
        )
        
        return {
            "text": response.choices[0].message.content,
            "model": response.model,
            "tokens": response.usage.total_tokens,
            "cost": estimate_openai_cost(response.usage.total_tokens, response.model)
        }
        
    except Exception as e:
        logger.error(f"Error generating OpenAI insight: {e}")
        return None


def estimate_openai_cost(tokens: int, model: str) -> float:
    """Estimate cost of OpenAI API call."""
    # Approximate costs per 1K tokens (as of 2024)
    pricing = {
        "gpt-4o-mini": 0.00015,  # $0.15 per 1M tokens
        "gpt-4": 0.03,           # $30 per 1M tokens
        "gpt-3.5-turbo": 0.002   # $2 per 1M tokens
    }
    
    cost_per_1k = pricing.get(model, 0.002)
    return (tokens / 1000) * cost_per_1k


def cache_set(key: str, value: Any, ttl: int = 300) -> bool:
    """Set value in Redis cache with TTL."""
    try:
        json_value = json.dumps(value, default=str)
        return redis_client.setex(key, ttl, json_value)
    except Exception as e:
        logger.error(f"Cache set error: {e}")
        return False


def cache_get(key: str) -> Optional[Any]:
    """Get value from Redis cache."""
    try:
        value = redis_client.get(key)
        if value:
            return json.loads(value)
    except Exception as e:
        logger.error(f"Cache get error: {e}")
    return None


def generate_id(prefix: str = "") -> str:
    """Generate unique ID with optional prefix."""
    import uuid
    unique_id = str(uuid.uuid4())
    return f"{prefix}_{unique_id}" if prefix else unique_id


def is_market_hours() -> bool:
    """Check if it's currently market hours (9:30 AM - 4:00 PM ET, Mon-Fri)."""
    from datetime import datetime
    import pytz
    
    try:
        et = pytz.timezone('US/Eastern')
        now = datetime.now(et)
        
        # Check if it's a weekday
        if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        # Check time (9:30 AM - 4:00 PM ET)
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        return market_open <= now <= market_close
        
    except Exception as e:
        logger.error(f"Error checking market hours: {e}")
        return True  # Default to True if we can't determine


def run_async_task(coro):
    """Run async function in sync context."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(coro)


def deduplicate_news(articles: List[Dict], existing_urls: set) -> List[Dict]:
    """Remove duplicate news articles based on URL and title similarity."""
    unique_articles = []
    seen_urls = set()
    seen_titles = set()
    
    for article in articles:
        url = article.get('url', '').strip()
        title = article.get('title', '').strip().lower()
        
        # Skip if URL already exists in database or current batch
        if url in existing_urls or url in seen_urls:
            continue
            
        # Skip if very similar title already seen (simple check)
        title_words = set(title.split())
        is_duplicate = False
        
        for seen_title in seen_titles:
            seen_words = set(seen_title.split())
            # If 80% of words overlap, consider duplicate
            if len(title_words & seen_words) / max(len(title_words), len(seen_words)) > 0.8:
                is_duplicate = True
                break
        
        if not is_duplicate:
            unique_articles.append(article)
            seen_urls.add(url)
            seen_titles.add(title)
    
    return unique_articles


def clean_financial_data(data: Dict) -> Dict:
    """Clean and validate financial data from external APIs."""
    cleaned = {}
    
    for key, value in data.items():
        if value in [None, "None", "-", "", "N/A"]:
            cleaned[key] = None
        elif isinstance(value, str):
            # Try to convert numeric strings
            try:
                if '.' in value or 'e' in value.lower():
                    cleaned[key] = float(value)
                elif value.isdigit():
                    cleaned[key] = int(value)
                else:
                    cleaned[key] = value.strip()
            except (ValueError, AttributeError):
                cleaned[key] = value
        else:
            cleaned[key] = value
    

# Aliases for backward compatibility
deduplicate_news_items = deduplicate_news
openai_chat_completion = generate_openai_insight
