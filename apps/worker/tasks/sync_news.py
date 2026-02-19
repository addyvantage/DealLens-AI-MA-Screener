from celery import shared_task
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import hashlib

from .utilities import (
    get_db_session, 
    fetch_news_data,
    run_async_task,
    cache_set,
    cache_get,
    generate_id,
    deduplicate_news_items,
    openai_chat_completion
)

logger = logging.getLogger(__name__)

# Import models
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../api'))

from app.models.company import Company
from app.models.market_data import NewsItem as NewsArticle


@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def sync_general_market_news(self, categories: List[str] = None) -> Dict[str, Any]:
    """Sync general market news (not company-specific)."""
    if categories is None:
        categories = ["business", "technology"]
    
    logger.info(f"Starting general news sync for categories: {categories}")
    
    results = {
        "fetched": 0,
        "new_articles": 0,
        "duplicates": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            for category in categories:
                try:
                    # Fetch news from API
                    logger.info(f"Fetching news for category: {category}")
                    news_data = run_async_task(
                        fetch_news_data(query=None, category=category, page_size=50)
                    )
                    
                    if not news_data or "articles" not in news_data:
                        logger.error(f"No news data for category {category}")
                        results["failed"] += 1
                        continue
                    
                    articles = news_data["articles"]
                    results["fetched"] += len(articles)
                    
                    # Deduplicate articles
                    deduplicated = deduplicate_news_items(articles)
                    logger.info(f"Deduplicated {len(articles)} to {len(deduplicated)} articles")
                    
                    for article in deduplicated:
                        try:
                            # Create content hash for deduplication
                            content_hash = hashlib.sha256(
                                f"{article.get('title', '')}{article.get('url', '')}".encode()
                            ).hexdigest()
                            
                            # Check if article already exists
                            existing = db.query(NewsArticle).filter(
                                NewsArticle.content_hash == content_hash
                            ).first()
                            
                            if existing:
                                results["duplicates"] += 1
                                continue
                            
                            # Parse publish date
                            published_at = None
                            if article.get("publishedAt"):
                                try:
                                    published_at = datetime.fromisoformat(
                                        article["publishedAt"].replace('Z', '+00:00')
                                    )
                                except ValueError:
                                    logger.warning(f"Invalid date format: {article['publishedAt']}")
                            
                            # Create news article record
                            news_record = NewsArticle(
                                id=generate_id("news"),
                                title=article.get("title"),
                                description=article.get("description"),
                                content=article.get("content"),
                                url=article.get("url"),
                                url_to_image=article.get("urlToImage"),
                                source_name=article.get("source", {}).get("name"),
                                author=article.get("author"),
                                published_at=published_at or datetime.utcnow(),
                                category=category,
                                content_hash=content_hash,
                                created_at=datetime.utcnow()
                            )
                            
                            db.add(news_record)
                            results["new_articles"] += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing article: {e}")
                            results["errors"].append(str(e))
                    
                    db.commit()
                    logger.info(f"Processed {len(deduplicated)} articles for {category}")
                    
                except Exception as e:
                    logger.error(f"Error syncing news for category {category}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"{category}: {str(e)}")
        
        logger.info(f"General news sync completed: {results['new_articles']} new articles")
        return results
        
    except Exception as e:
        logger.error(f"General news sync task failed: {e}")
        raise self.retry(exc=e)


@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def sync_company_news(self, company_tickers: List[str], days_back: int = 7) -> Dict[str, Any]:
    """Sync company-specific news."""
    logger.info(f"Starting company news sync for {len(company_tickers)} companies")
    
    results = {
        "fetched": 0,
        "new_articles": 0,
        "duplicates": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            for ticker in company_tickers:
                try:
                    # Get company record
                    company = db.query(Company).filter(Company.ticker == ticker).first()
                    if not company:
                        logger.warning(f"Company not found for ticker: {ticker}")
                        continue
                    
                    # Check cache first
                    cache_key = f"company_news:{ticker}:{days_back}"
                    cached_articles = cache_get(cache_key)
                    
                    if cached_articles:
                        logger.debug(f"Using cached news for {ticker}")
                        articles = cached_articles
                    else:
                        # Fetch news for company
                        search_terms = [ticker, company.name]
                        if company.name and ticker.upper() not in company.name.upper():
                            search_terms.append(company.name)
                        
                        query = f'"{ticker}" OR "{company.name}"'
                        logger.info(f"Fetching news for {ticker} with query: {query}")
                        
                        from_date = (datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%d')
                        
                        news_data = run_async_task(
                            fetch_news_data(
                                query=query,
                                from_date=from_date,
                                sort_by="publishedAt",
                                page_size=30
                            )
                        )
                        
                        if not news_data or "articles" not in news_data:
                            logger.error(f"No news data for company {ticker}")
                            results["failed"] += 1
                            continue
                        
                        articles = news_data["articles"]
                        
                        # Cache for 5 minutes
                        cache_set(cache_key, articles, ttl=300)
                    
                    results["fetched"] += len(articles)
                    
                    # Deduplicate articles
                    deduplicated = deduplicate_news_items(articles)
                    
                    for article in deduplicated:
                        try:
                            # Create content hash
                            content_hash = hashlib.sha256(
                                f"{article.get('title', '')}{article.get('url', '')}".encode()
                            ).hexdigest()
                            
                            # Check if article already exists
                            existing = db.query(NewsArticle).filter(
                                NewsArticle.content_hash == content_hash
                            ).first()
                            
                            if existing:
                                # Associate with company if not already associated
                                if company not in existing.related_companies:
                                    existing.related_companies.append(company)
                                    db.commit()
                                results["duplicates"] += 1
                                continue
                            
                            # Parse publish date
                            published_at = None
                            if article.get("publishedAt"):
                                try:
                                    published_at = datetime.fromisoformat(
                                        article["publishedAt"].replace('Z', '+00:00')
                                    )
                                except ValueError:
                                    pass
                            
                            # Create news article record
                            news_record = NewsArticle(
                                id=generate_id("news"),
                                title=article.get("title"),
                                description=article.get("description"),
                                content=article.get("content"),
                                url=article.get("url"),
                                url_to_image=article.get("urlToImage"),
                                source_name=article.get("source", {}).get("name"),
                                author=article.get("author"),
                                published_at=published_at or datetime.utcnow(),
                                category="company",
                                content_hash=content_hash,
                                created_at=datetime.utcnow()
                            )
                            
                            # Associate with company
                            news_record.related_companies.append(company)
                            
                            db.add(news_record)
                            results["new_articles"] += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing article for {ticker}: {e}")
                            results["errors"].append(f"{ticker}: {str(e)}")
                    
                    db.commit()
                    logger.info(f"Processed news for {ticker}: {len(deduplicated)} articles")
                    
                except Exception as e:
                    logger.error(f"Error syncing news for company {ticker}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"{ticker}: {str(e)}")
        
        logger.info(f"Company news sync completed: {results['new_articles']} new articles")
        return results
        
    except Exception as e:
        logger.error(f"Company news sync task failed: {e}")
        raise self.retry(exc=e)


@shared_task(bind=True, max_retries=2, default_retry_delay=600)
def analyze_news_sentiment(self, news_ids: List[str] = None, batch_size: int = 10) -> Dict[str, Any]:
    """Analyze sentiment for news articles using OpenAI."""
    logger.info(f"Starting sentiment analysis for {len(news_ids) if news_ids else 'unanalyzed'} articles")
    
    results = {
        "analyzed": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            # Get articles to analyze
            query = db.query(NewsArticle)
            
            if news_ids:
                query = query.filter(NewsArticle.id.in_(news_ids))
            else:
                # Get unanalyzed articles from last 7 days
                cutoff_date = datetime.utcnow() - timedelta(days=7)
                query = query.filter(
                    NewsArticle.sentiment_score.is_(None),
                    NewsArticle.created_at >= cutoff_date
                )
            
            articles = query.limit(batch_size).all()
            
            if not articles:
                logger.info("No articles to analyze")
                return results
            
            # Batch analyze sentiment
            for i in range(0, len(articles), 5):  # Process in smaller batches for API
                batch = articles[i:i+5]
                
                try:
                    # Prepare batch for sentiment analysis
                    article_texts = []
                    for article in batch:
                        text = f"Title: {article.title or ''}\nDescription: {article.description or ''}"
                        article_texts.append(text[:500])  # Limit text length
                    
                    # Call OpenAI for sentiment analysis
                    prompt = f"""
                    Analyze the sentiment of these {len(article_texts)} news articles and return a JSON array with sentiment scores from -1 (very negative) to 1 (very positive):
                    
                    {chr(10).join([f'{idx+1}. {text}' for idx, text in enumerate(article_texts)])}
                    
                    Return format: [{{"score": 0.2, "label": "neutral"}}, {{"score": -0.8, "label": "negative"}}, ...]
                    Labels should be one of: positive, negative, neutral
                    """
                    
                    response = run_async_task(openai_chat_completion(
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=300,
                        temperature=0.1
                    ))
                    
                    if response and "choices" in response:
                        content = response["choices"][0]["message"]["content"]
                        
                        try:
                            import json
                            sentiment_data = json.loads(content)
                            
                            # Update articles with sentiment scores
                            for idx, article in enumerate(batch):
                                if idx < len(sentiment_data):
                                    sentiment = sentiment_data[idx]
                                    article.sentiment_score = sentiment.get("score", 0.0)
                                    article.sentiment_label = sentiment.get("label", "neutral")
                                    results["analyzed"] += 1
                            
                            db.commit()
                            logger.info(f"Analyzed sentiment for batch of {len(batch)} articles")
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse sentiment response: {e}")
                            results["failed"] += len(batch)
                    
                except Exception as e:
                    logger.error(f"Error analyzing sentiment batch: {e}")
                    results["failed"] += len(batch)
                    results["errors"].append(str(e))
        
        logger.info(f"Sentiment analysis completed: {results['analyzed']} analyzed, {results['failed']} failed")
        return results
        
    except Exception as e:
        logger.error(f"Sentiment analysis task failed: {e}")
        raise self.retry(exc=e)


@shared_task
def cleanup_old_news(days_to_keep: int = 30) -> Dict[str, Any]:
    """Clean up old news articles to manage database size."""
    logger.info(f"Starting news cleanup, keeping last {days_to_keep} days")
    
    cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
    
    try:
        with get_db_session() as db:
            deleted_count = db.query(NewsArticle).filter(
                NewsArticle.created_at < cutoff_date
            ).delete()
            
            db.commit()
            
            logger.info(f"Cleaned up {deleted_count} old news articles")
            return {"deleted_articles": deleted_count, "cutoff_date": cutoff_date.isoformat()}
            
    except Exception as e:
        logger.error(f"News cleanup failed: {e}")
        raise


@shared_task(bind=True, max_retries=2, default_retry_delay=1800)
def sync_ma_deal_news(self, keywords: List[str] = None, days_back: int = 14) -> Dict[str, Any]:
    """Sync M&A and deal-specific news."""
    if keywords is None:
        keywords = [
            "merger", "acquisition", "buyout", "takeover", 
            "M&A", "deal", "private equity", "venture capital",
            "IPO", "SPAC", "spinoff"
        ]
    
    logger.info(f"Starting M&A deal news sync with {len(keywords)} keywords")
    
    results = {
        "fetched": 0,
        "new_articles": 0,
        "duplicates": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            for keyword in keywords:
                try:
                    # Build search query
                    query = f'"{keyword}"'
                    from_date = (datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%d')
                    
                    logger.info(f"Fetching M&A news for keyword: {keyword}")
                    
                    news_data = run_async_task(
                        fetch_news_data(
                            query=query,
                            from_date=from_date,
                            sort_by="publishedAt",
                            page_size=20
                        )
                    )
                    
                    if not news_data or "articles" not in news_data:
                        logger.warning(f"No news data for keyword {keyword}")
                        continue
                    
                    articles = news_data["articles"]
                    results["fetched"] += len(articles)
                    
                    # Deduplicate articles
                    deduplicated = deduplicate_news_items(articles)
                    
                    for article in deduplicated:
                        try:
                            # Create content hash
                            content_hash = hashlib.sha256(
                                f"{article.get('title', '')}{article.get('url', '')}".encode()
                            ).hexdigest()
                            
                            # Check if article already exists
                            existing = db.query(NewsArticle).filter(
                                NewsArticle.content_hash == content_hash
                            ).first()
                            
                            if existing:
                                results["duplicates"] += 1
                                continue
                            
                            # Parse publish date
                            published_at = None
                            if article.get("publishedAt"):
                                try:
                                    published_at = datetime.fromisoformat(
                                        article["publishedAt"].replace('Z', '+00:00')
                                    )
                                except ValueError:
                                    pass
                            
                            # Create news article record
                            news_record = NewsArticle(
                                id=generate_id("news"),
                                title=article.get("title"),
                                description=article.get("description"),
                                content=article.get("content"),
                                url=article.get("url"),
                                url_to_image=article.get("urlToImage"),
                                source_name=article.get("source", {}).get("name"),
                                author=article.get("author"),
                                published_at=published_at or datetime.utcnow(),
                                category="deals",
                                tags=[keyword, "M&A"],
                                content_hash=content_hash,
                                created_at=datetime.utcnow()
                            )
                            
                            db.add(news_record)
                            results["new_articles"] += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing M&A article: {e}")
                            results["errors"].append(str(e))
                    
                    db.commit()
                    logger.info(f"Processed M&A news for {keyword}: {len(deduplicated)} articles")
                    
                except Exception as e:
                    logger.error(f"Error syncing M&A news for keyword {keyword}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"{keyword}: {str(e)}")
        
        logger.info(f"M&A news sync completed: {results['new_articles']} new articles")
        return results
        
    except Exception as e:
        logger.error(f"M&A news sync task failed: {e}")
        raise self.retry(exc=e)
