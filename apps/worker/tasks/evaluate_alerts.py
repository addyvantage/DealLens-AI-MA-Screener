from celery import shared_task
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
from decimal import Decimal

from .utilities import (
    get_db_session, 
    cache_set,
    cache_get,
    generate_id,
    run_async_task
)

logger = logging.getLogger(__name__)

# Import models
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../api'))

from app.models.company import Company
from app.models.market_data import MarketData
from app.models.market_data import NewsItem as NewsArticle
from app.models.deal import Deal
from app.models.alert import Alert, AlertHistory

OHLCData = MarketData  # Alias for compatibility


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def evaluate_price_alerts(self, alert_ids: List[str] = None, batch_size: int = 100) -> Dict[str, Any]:
    """Evaluate price-based alerts for companies."""
    logger.info(f"Starting price alerts evaluation for {len(alert_ids) if alert_ids else 'active'} alerts")
    
    results = {
        "evaluated": 0,
        "triggered": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            # Get active price alerts
            query = db.query(Alert).filter(
                Alert.alert_type.in_(["price_above", "price_below", "price_change_percent"]),
                Alert.is_active == True,
                Alert.expires_at > datetime.utcnow()
            )
            
            if alert_ids:
                query = query.filter(Alert.id.in_(alert_ids))
            
            alerts = query.limit(batch_size).all()
            
            if not alerts:
                logger.info("No price alerts to evaluate")
                return results
            
            for alert in alerts:
                try:
                    results["evaluated"] += 1
                    
                    # Get company data
                    company = db.query(Company).filter(Company.id == alert.entity_id).first()
                    if not company:
                        logger.warning(f"Company not found for alert {alert.id}")
                        continue
                    
                    current_price = company.last_price
                    if not current_price:
                        logger.debug(f"No current price for {company.ticker}")
                        continue
                    
                    triggered = False
                    trigger_value = None
                    
                    if alert.alert_type == "price_above":
                        target_price = alert.trigger_conditions.get("price")
                        if target_price and current_price > target_price:
                            triggered = True
                            trigger_value = current_price
                    
                    elif alert.alert_type == "price_below":
                        target_price = alert.trigger_conditions.get("price")
                        if target_price and current_price < target_price:
                            triggered = True
                            trigger_value = current_price
                    
                    elif alert.alert_type == "price_change_percent":
                        # Get price from specified timeframe
                        timeframe = alert.trigger_conditions.get("timeframe", "1d")
                        percent_change = alert.trigger_conditions.get("percent_change")
                        direction = alert.trigger_conditions.get("direction", "any")  # up/down/any
                        
                        if timeframe == "1d":
                            cutoff_date = datetime.utcnow().date() - timedelta(days=1)
                        elif timeframe == "1w":
                            cutoff_date = datetime.utcnow().date() - timedelta(days=7)
                        elif timeframe == "1m":
                            cutoff_date = datetime.utcnow().date() - timedelta(days=30)
                        else:
                            continue
                        
                        # Get historical price
                        historical_ohlc = db.query(OHLCData).filter(
                            OHLCData.company_id == company.id,
                            OHLCData.date >= cutoff_date
                        ).order_by(OHLCData.date.asc()).first()
                        
                        if historical_ohlc and percent_change:
                            historical_price = float(historical_ohlc.close_price)
                            actual_change = ((current_price - historical_price) / historical_price) * 100
                            
                            if direction == "up" and actual_change >= percent_change:
                                triggered = True
                                trigger_value = actual_change
                            elif direction == "down" and actual_change <= -percent_change:
                                triggered = True
                                trigger_value = actual_change
                            elif direction == "any" and abs(actual_change) >= percent_change:
                                triggered = True
                                trigger_value = actual_change
                    
                    if triggered:
                        # Check if alert was already triggered recently (avoid spam)
                        recent_trigger = db.query(AlertHistory).filter(
                            AlertHistory.alert_id == alert.id,
                            AlertHistory.created_at >= datetime.utcnow() - timedelta(hours=1)
                        ).first()
                        
                        if not recent_trigger:
                            # Create alert history entry
                            alert_history = AlertHistory(
                                id=generate_id("alert_history"),
                                alert_id=alert.id,
                                user_id=alert.user_id,
                                triggered_at=datetime.utcnow(),
                                trigger_value=str(trigger_value),
                                trigger_data={
                                    "company_ticker": company.ticker,
                                    "company_name": company.name,
                                    "current_price": current_price,
                                    "alert_type": alert.alert_type,
                                    "trigger_conditions": alert.trigger_conditions
                                },
                                notification_sent=False,
                                created_at=datetime.utcnow()
                            )
                            
                            db.add(alert_history)
                            
                            # Update alert trigger count and last triggered
                            alert.trigger_count = (alert.trigger_count or 0) + 1
                            alert.last_triggered_at = datetime.utcnow()
                            
                            # Deactivate one-time alerts
                            if alert.trigger_conditions.get("frequency") == "once":
                                alert.is_active = False
                            
                            db.commit()
                            
                            results["triggered"] += 1
                            logger.info(f"Alert triggered for {company.ticker}: {alert.alert_type} = {trigger_value}")
                            
                            # Queue notification task
                            from .send_notifications import send_alert_notification
                            send_alert_notification.delay(alert_history.id)
                
                except Exception as e:
                    logger.error(f"Error evaluating alert {alert.id}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"Alert {alert.id}: {str(e)}")
        
        logger.info(f"Price alerts evaluation completed: {results['evaluated']} evaluated, {results['triggered']} triggered")
        return results
        
    except Exception as e:
        logger.error(f"Price alerts evaluation task failed: {e}")
        raise self.retry(exc=e)


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def evaluate_volume_alerts(self, alert_ids: List[str] = None, batch_size: int = 100) -> Dict[str, Any]:
    """Evaluate volume-based alerts for companies."""
    logger.info(f"Starting volume alerts evaluation for {len(alert_ids) if alert_ids else 'active'} alerts")
    
    results = {
        "evaluated": 0,
        "triggered": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            # Get active volume alerts
            query = db.query(Alert).filter(
                Alert.alert_type.in_(["volume_spike", "volume_above"]),
                Alert.is_active == True,
                Alert.expires_at > datetime.utcnow()
            )
            
            if alert_ids:
                query = query.filter(Alert.id.in_(alert_ids))
            
            alerts = query.limit(batch_size).all()
            
            if not alerts:
                logger.info("No volume alerts to evaluate")
                return results
            
            for alert in alerts:
                try:
                    results["evaluated"] += 1
                    
                    # Get company data
                    company = db.query(Company).filter(Company.id == alert.entity_id).first()
                    if not company:
                        logger.warning(f"Company not found for alert {alert.id}")
                        continue
                    
                    # Get today's volume data
                    today = datetime.utcnow().date()
                    today_ohlc = db.query(OHLCData).filter(
                        OHLCData.company_id == company.id,
                        OHLCData.date == today
                    ).first()
                    
                    if not today_ohlc or not today_ohlc.volume:
                        logger.debug(f"No volume data for {company.ticker} today")
                        continue
                    
                    current_volume = int(today_ohlc.volume)
                    triggered = False
                    trigger_value = None
                    
                    if alert.alert_type == "volume_above":
                        target_volume = alert.trigger_conditions.get("volume")
                        if target_volume and current_volume > target_volume:
                            triggered = True
                            trigger_value = current_volume
                    
                    elif alert.alert_type == "volume_spike":
                        # Compare to average volume over specified period
                        period_days = alert.trigger_conditions.get("period_days", 20)
                        spike_multiplier = alert.trigger_conditions.get("spike_multiplier", 2.0)
                        
                        # Get historical volume data
                        cutoff_date = today - timedelta(days=period_days)
                        historical_volumes = db.query(OHLCData.volume).filter(
                            OHLCData.company_id == company.id,
                            OHLCData.date >= cutoff_date,
                            OHLCData.date < today,
                            OHLCData.volume.isnot(None)
                        ).all()
                        
                        if historical_volumes:
                            avg_volume = sum(int(v[0]) for v in historical_volumes) / len(historical_volumes)
                            if current_volume > avg_volume * spike_multiplier:
                                triggered = True
                                trigger_value = current_volume / avg_volume
                    
                    if triggered:
                        # Check if alert was already triggered recently
                        recent_trigger = db.query(AlertHistory).filter(
                            AlertHistory.alert_id == alert.id,
                            AlertHistory.created_at >= datetime.utcnow() - timedelta(hours=4)
                        ).first()
                        
                        if not recent_trigger:
                            # Create alert history entry
                            alert_history = AlertHistory(
                                id=generate_id("alert_history"),
                                alert_id=alert.id,
                                user_id=alert.user_id,
                                triggered_at=datetime.utcnow(),
                                trigger_value=str(trigger_value),
                                trigger_data={
                                    "company_ticker": company.ticker,
                                    "company_name": company.name,
                                    "current_volume": current_volume,
                                    "alert_type": alert.alert_type,
                                    "trigger_conditions": alert.trigger_conditions
                                },
                                notification_sent=False,
                                created_at=datetime.utcnow()
                            )
                            
                            db.add(alert_history)
                            
                            # Update alert
                            alert.trigger_count = (alert.trigger_count or 0) + 1
                            alert.last_triggered_at = datetime.utcnow()
                            
                            if alert.trigger_conditions.get("frequency") == "once":
                                alert.is_active = False
                            
                            db.commit()
                            
                            results["triggered"] += 1
                            logger.info(f"Volume alert triggered for {company.ticker}: {current_volume:,}")
                            
                            # Queue notification
                            from .send_notifications import send_alert_notification
                            send_alert_notification.delay(alert_history.id)
                
                except Exception as e:
                    logger.error(f"Error evaluating volume alert {alert.id}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"Alert {alert.id}: {str(e)}")
        
        logger.info(f"Volume alerts evaluation completed: {results['evaluated']} evaluated, {results['triggered']} triggered")
        return results
        
    except Exception as e:
        logger.error(f"Volume alerts evaluation task failed: {e}")
        raise self.retry(exc=e)


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def evaluate_news_alerts(self, alert_ids: List[str] = None, batch_size: int = 50) -> Dict[str, Any]:
    """Evaluate news-based alerts for companies and deals."""
    logger.info(f"Starting news alerts evaluation for {len(alert_ids) if alert_ids else 'active'} alerts")
    
    results = {
        "evaluated": 0,
        "triggered": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            # Get active news alerts
            query = db.query(Alert).filter(
                Alert.alert_type.in_(["news_mention", "news_sentiment", "ma_news"]),
                Alert.is_active == True,
                Alert.expires_at > datetime.utcnow()
            )
            
            if alert_ids:
                query = query.filter(Alert.id.in_(alert_ids))
            
            alerts = query.limit(batch_size).all()
            
            if not alerts:
                logger.info("No news alerts to evaluate")
                return results
            
            for alert in alerts:
                try:
                    results["evaluated"] += 1
                    
                    # Get recent news based on alert type
                    cutoff_time = datetime.utcnow() - timedelta(hours=24)  # Check last 24 hours
                    
                    if alert.alert_type == "news_mention":
                        # Company-specific news mentions
                        company = db.query(Company).filter(Company.id == alert.entity_id).first()
                        if not company:
                            continue
                        
                        keywords = alert.trigger_conditions.get("keywords", [])
                        if not keywords:
                            keywords = [company.ticker, company.name]
                        
                        # Find recent news mentioning keywords
                        for keyword in keywords:
                            news_articles = db.query(NewsArticle).filter(
                                (NewsArticle.title.contains(keyword) | NewsArticle.description.contains(keyword)),
                                NewsArticle.created_at >= cutoff_time
                            ).all()
                            
                            if news_articles:
                                # Check if we've already alerted on this news
                                latest_article = max(news_articles, key=lambda x: x.created_at)
                                
                                recent_alert = db.query(AlertHistory).filter(
                                    AlertHistory.alert_id == alert.id,
                                    AlertHistory.created_at >= cutoff_time
                                ).first()
                                
                                if not recent_alert:
                                    self._create_news_alert_history(
                                        db, alert, latest_article, company.ticker, 
                                        f"News mention: {keyword}", results
                                    )
                                break
                    
                    elif alert.alert_type == "news_sentiment":
                        # Sentiment-based alerts
                        company = db.query(Company).filter(Company.id == alert.entity_id).first()
                        if not company:
                            continue
                        
                        sentiment_threshold = alert.trigger_conditions.get("sentiment_threshold", 0.5)
                        sentiment_direction = alert.trigger_conditions.get("direction", "positive")  # positive/negative
                        
                        # Get recent news for company
                        recent_news = db.query(NewsArticle).join(
                            NewsArticle.related_companies
                        ).filter(
                            Company.id == company.id,
                            NewsArticle.created_at >= cutoff_time,
                            NewsArticle.sentiment_score.isnot(None)
                        ).all()
                        
                        triggered_articles = []
                        for article in recent_news:
                            if sentiment_direction == "positive" and article.sentiment_score >= sentiment_threshold:
                                triggered_articles.append(article)
                            elif sentiment_direction == "negative" and article.sentiment_score <= -sentiment_threshold:
                                triggered_articles.append(article)
                        
                        if triggered_articles:
                            # Check for recent alerts
                            recent_alert = db.query(AlertHistory).filter(
                                AlertHistory.alert_id == alert.id,
                                AlertHistory.created_at >= cutoff_time
                            ).first()
                            
                            if not recent_alert:
                                latest_article = max(triggered_articles, key=lambda x: x.created_at)
                                self._create_news_alert_history(
                                    db, alert, latest_article, company.ticker,
                                    f"Sentiment {sentiment_direction}: {latest_article.sentiment_score:.2f}", 
                                    results
                                )
                    
                    elif alert.alert_type == "ma_news":
                        # M&A deal news alerts
                        keywords = alert.trigger_conditions.get("keywords", [
                            "merger", "acquisition", "buyout", "takeover", "M&A"
                        ])
                        
                        for keyword in keywords:
                            recent_ma_news = db.query(NewsArticle).filter(
                                (NewsArticle.title.contains(keyword) | NewsArticle.description.contains(keyword)),
                                NewsArticle.created_at >= cutoff_time
                            ).all()
                            
                            if recent_ma_news:
                                recent_alert = db.query(AlertHistory).filter(
                                    AlertHistory.alert_id == alert.id,
                                    AlertHistory.created_at >= cutoff_time
                                ).first()
                                
                                if not recent_alert:
                                    latest_article = max(recent_ma_news, key=lambda x: x.created_at)
                                    self._create_news_alert_history(
                                        db, alert, latest_article, "M&A",
                                        f"M&A news: {keyword}", results
                                    )
                                break
                
                except Exception as e:
                    logger.error(f"Error evaluating news alert {alert.id}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"Alert {alert.id}: {str(e)}")
        
        logger.info(f"News alerts evaluation completed: {results['evaluated']} evaluated, {results['triggered']} triggered")
        return results
        
    except Exception as e:
        logger.error(f"News alerts evaluation task failed: {e}")
        raise self.retry(exc=e)

    def _create_news_alert_history(self, db, alert, article, ticker, trigger_desc, results):
        """Helper method to create news alert history entry."""
        alert_history = AlertHistory(
            id=generate_id("alert_history"),
            alert_id=alert.id,
            user_id=alert.user_id,
            triggered_at=datetime.utcnow(),
            trigger_value=trigger_desc,
            trigger_data={
                "ticker": ticker,
                "article_title": article.title,
                "article_url": article.url,
                "article_published": article.published_at.isoformat(),
                "alert_type": alert.alert_type
            },
            notification_sent=False,
            created_at=datetime.utcnow()
        )
        
        db.add(alert_history)
        
        # Update alert
        alert.trigger_count = (alert.trigger_count or 0) + 1
        alert.last_triggered_at = datetime.utcnow()
        
        if alert.trigger_conditions.get("frequency") == "once":
            alert.is_active = False
        
        db.commit()
        
        results["triggered"] += 1
        logger.info(f"News alert triggered: {trigger_desc}")
        
        # Queue notification
        from .send_notifications import send_alert_notification
        send_alert_notification.delay(alert_history.id)


@shared_task(bind=True, max_retries=2, default_retry_delay=600)
def evaluate_deal_alerts(self, alert_ids: List[str] = None, batch_size: int = 50) -> Dict[str, Any]:
    """Evaluate deal-based alerts for M&A activity."""
    logger.info(f"Starting deal alerts evaluation for {len(alert_ids) if alert_ids else 'active'} alerts")
    
    results = {
        "evaluated": 0,
        "triggered": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            # Get active deal alerts
            query = db.query(Alert).filter(
                Alert.alert_type.in_(["new_deal", "deal_value_above", "deal_status_change"]),
                Alert.is_active == True,
                Alert.expires_at > datetime.utcnow()
            )
            
            if alert_ids:
                query = query.filter(Alert.id.in_(alert_ids))
            
            alerts = query.limit(batch_size).all()
            
            if not alerts:
                logger.info("No deal alerts to evaluate")
                return results
            
            for alert in alerts:
                try:
                    results["evaluated"] += 1
                    cutoff_time = datetime.utcnow() - timedelta(hours=24)
                    
                    if alert.alert_type == "new_deal":
                        # Check for new deals matching criteria
                        criteria = alert.trigger_conditions
                        
                        query_filters = [Deal.created_at >= cutoff_time]
                        
                        if criteria.get("industry"):
                            query_filters.append(Deal.industry == criteria["industry"])
                        
                        if criteria.get("deal_type"):
                            query_filters.append(Deal.deal_type == criteria["deal_type"])
                        
                        if criteria.get("min_value"):
                            query_filters.append(Deal.deal_value >= criteria["min_value"])
                        
                        new_deals = db.query(Deal).filter(*query_filters).all()
                        
                        if new_deals:
                            recent_alert = db.query(AlertHistory).filter(
                                AlertHistory.alert_id == alert.id,
                                AlertHistory.created_at >= cutoff_time
                            ).first()
                            
                            if not recent_alert:
                                latest_deal = max(new_deals, key=lambda x: x.created_at)
                                self._create_deal_alert_history(
                                    db, alert, latest_deal, f"New deal: {latest_deal.target_company}", results
                                )
                    
                    elif alert.alert_type == "deal_value_above":
                        # Check for deals above value threshold
                        min_value = alert.trigger_conditions.get("min_value")
                        if min_value:
                            high_value_deals = db.query(Deal).filter(
                                Deal.created_at >= cutoff_time,
                                Deal.deal_value >= min_value
                            ).all()
                            
                            if high_value_deals:
                                recent_alert = db.query(AlertHistory).filter(
                                    AlertHistory.alert_id == alert.id,
                                    AlertHistory.created_at >= cutoff_time
                                ).first()
                                
                                if not recent_alert:
                                    largest_deal = max(high_value_deals, key=lambda x: x.deal_value)
                                    self._create_deal_alert_history(
                                        db, alert, largest_deal, 
                                        f"High-value deal: ${largest_deal.deal_value:,.0f}M", results
                                    )
                    
                    elif alert.alert_type == "deal_status_change":
                        # Monitor specific deal status changes
                        target_deal_id = alert.entity_id
                        target_statuses = alert.trigger_conditions.get("target_statuses", ["completed", "cancelled"])
                        
                        deal = db.query(Deal).filter(Deal.id == target_deal_id).first()
                        if deal and deal.status in target_statuses:
                            # Check if status changed recently
                            recent_alert = db.query(AlertHistory).filter(
                                AlertHistory.alert_id == alert.id,
                                AlertHistory.trigger_data.contains(f'"status":"{deal.status}"')
                            ).first()
                            
                            if not recent_alert:
                                self._create_deal_alert_history(
                                    db, alert, deal, f"Status change: {deal.status}", results
                                )
                
                except Exception as e:
                    logger.error(f"Error evaluating deal alert {alert.id}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"Alert {alert.id}: {str(e)}")
        
        logger.info(f"Deal alerts evaluation completed: {results['evaluated']} evaluated, {results['triggered']} triggered")
        return results
        
    except Exception as e:
        logger.error(f"Deal alerts evaluation task failed: {e}")
        raise self.retry(exc=e)

    def _create_deal_alert_history(self, db, alert, deal, trigger_desc, results):
        """Helper method to create deal alert history entry."""
        alert_history = AlertHistory(
            id=generate_id("alert_history"),
            alert_id=alert.id,
            user_id=alert.user_id,
            triggered_at=datetime.utcnow(),
            trigger_value=trigger_desc,
            trigger_data={
                "deal_id": deal.id,
                "target_company": deal.target_company,
                "acquirer_company": deal.acquirer_company,
                "deal_value": deal.deal_value,
                "deal_type": deal.deal_type,
                "status": deal.status,
                "alert_type": alert.alert_type
            },
            notification_sent=False,
            created_at=datetime.utcnow()
        )
        
        db.add(alert_history)
        
        # Update alert
        alert.trigger_count = (alert.trigger_count or 0) + 1
        alert.last_triggered_at = datetime.utcnow()
        
        if alert.trigger_conditions.get("frequency") == "once":
            alert.is_active = False
        
        db.commit()
        
        results["triggered"] += 1
        logger.info(f"Deal alert triggered: {trigger_desc}")
        
        # Queue notification
        from .send_notifications import send_alert_notification
        send_alert_notification.delay(alert_history.id)


@shared_task
def cleanup_old_alert_history(days_to_keep: int = 90) -> Dict[str, Any]:
    """Clean up old alert history to manage database size."""
    logger.info(f"Starting alert history cleanup, keeping last {days_to_keep} days")
    
    cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
    
    try:
        with get_db_session() as db:
            deleted_count = db.query(AlertHistory).filter(
                AlertHistory.created_at < cutoff_date
            ).delete()
            
            db.commit()
            
            logger.info(f"Cleaned up {deleted_count} old alert history records")
            return {"deleted_history": deleted_count, "cutoff_date": cutoff_date.isoformat()}
            
    except Exception as e:
        logger.error(f"Alert history cleanup failed: {e}")
        raise


@shared_task
def cleanup_expired_alerts() -> Dict[str, Any]:
    """Deactivate expired alerts."""
    logger.info("Starting expired alerts cleanup")
    
    try:
        with get_db_session() as db:
            updated_count = db.query(Alert).filter(
                Alert.expires_at < datetime.utcnow(),
                Alert.is_active == True
            ).update({"is_active": False})
            
            db.commit()
            
            logger.info(f"Deactivated {updated_count} expired alerts")
            return {"deactivated_alerts": updated_count}
            
    except Exception as e:
        logger.error(f"Expired alerts cleanup failed: {e}")
        raise
