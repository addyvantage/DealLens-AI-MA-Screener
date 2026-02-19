from celery import shared_task
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import json

from .utilities import (
    get_db_session, 
    generate_id,
    cache_set,
    cache_get,
    run_async_task
)

logger = logging.getLogger(__name__)

# Import models
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../api'))

from app.models.alert import Alert, AlertHistory
from app.models.user import User
from app.models.company import Company
from app.models.deal import Deal


# Mock email service - in production, replace with actual email provider
class EmailService:
    @staticmethod
    async def send_email(to_email: str, subject: str, body: str, html_body: str = None) -> bool:
        """Send email notification - mock implementation."""
        logger.info(f"Sending email to {to_email}: {subject}")
        # In production, integrate with SendGrid, AWS SES, etc.
        # For now, just log the email content
        logger.debug(f"Email body: {body}")
        return True


# Mock push notification service
class PushNotificationService:
    @staticmethod
    async def send_push(user_id: str, title: str, body: str, data: dict = None) -> bool:
        """Send push notification - mock implementation."""
        logger.info(f"Sending push notification to user {user_id}: {title}")
        logger.debug(f"Push body: {body}")
        # In production, integrate with Firebase, APNs, etc.
        return True


@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def send_alert_notification(self, alert_history_id: str) -> Dict[str, Any]:
    """Send notification for a triggered alert."""
    logger.info(f"Sending notification for alert history: {alert_history_id}")
    
    results = {
        "email_sent": False,
        "push_sent": False,
        "sms_sent": False,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            # Get alert history record
            alert_history = db.query(AlertHistory).filter(
                AlertHistory.id == alert_history_id
            ).first()
            
            if not alert_history:
                logger.error(f"Alert history not found: {alert_history_id}")
                return {"error": "Alert history not found"}
            
            # Skip if already sent
            if alert_history.notification_sent:
                logger.debug(f"Notification already sent for {alert_history_id}")
                return {"skipped": True, "reason": "already_sent"}
            
            # Get alert and user
            alert = db.query(Alert).filter(Alert.id == alert_history.alert_id).first()
            user = db.query(User).filter(User.id == alert_history.user_id).first()
            
            if not alert or not user:
                logger.error(f"Alert or user not found for {alert_history_id}")
                return {"error": "Alert or user not found"}
            
            # Build notification content based on alert type
            notification_content = self._build_notification_content(
                db, alert, alert_history, user
            )
            
            # Get user's notification preferences
            notification_prefs = getattr(user, 'notification_preferences', {})
            if not notification_prefs:
                notification_prefs = {
                    "email": True,
                    "push": True,
                    "sms": False
                }
            
            # Send email notification
            if notification_prefs.get("email", True) and user.email:
                try:
                    email_sent = run_async_task(EmailService.send_email(
                        to_email=user.email,
                        subject=notification_content["email_subject"],
                        body=notification_content["email_body"],
                        html_body=notification_content.get("email_html")
                    ))
                    results["email_sent"] = email_sent
                    logger.info(f"Email sent to {user.email}")
                except Exception as e:
                    logger.error(f"Failed to send email: {e}")
                    results["errors"].append(f"Email error: {str(e)}")
            
            # Send push notification
            if notification_prefs.get("push", True):
                try:
                    push_sent = run_async_task(PushNotificationService.send_push(
                        user_id=user.id,
                        title=notification_content["push_title"],
                        body=notification_content["push_body"],
                        data=notification_content.get("push_data", {})
                    ))
                    results["push_sent"] = push_sent
                    logger.info(f"Push notification sent to user {user.id}")
                except Exception as e:
                    logger.error(f"Failed to send push notification: {e}")
                    results["errors"].append(f"Push error: {str(e)}")
            
            # Send SMS notification (if enabled and phone number available)
            if notification_prefs.get("sms", False) and getattr(user, 'phone_number', None):
                try:
                    # SMS implementation would go here
                    # sms_sent = await SMSService.send_sms(user.phone_number, notification_content["sms_body"])
                    results["sms_sent"] = False  # Disabled for now
                except Exception as e:
                    logger.error(f"Failed to send SMS: {e}")
                    results["errors"].append(f"SMS error: {str(e)}")
            
            # Mark notification as sent
            alert_history.notification_sent = True
            alert_history.notification_sent_at = datetime.utcnow()
            db.commit()
            
            logger.info(f"Notification processing completed for {alert_history_id}")
            return results
            
    except Exception as e:
        logger.error(f"Failed to send notification for {alert_history_id}: {e}")
        raise self.retry(exc=e)

    def _build_notification_content(self, db, alert: Alert, alert_history: AlertHistory, user: User) -> Dict[str, str]:
        """Build notification content based on alert type."""
        trigger_data = alert_history.trigger_data or {}
        alert_type = alert.alert_type
        
        if alert_type in ["price_above", "price_below", "price_change_percent"]:
            return self._build_price_notification(alert, alert_history, trigger_data, user)
        elif alert_type in ["volume_spike", "volume_above"]:
            return self._build_volume_notification(alert, alert_history, trigger_data, user)
        elif alert_type in ["news_mention", "news_sentiment", "ma_news"]:
            return self._build_news_notification(alert, alert_history, trigger_data, user)
        elif alert_type in ["new_deal", "deal_value_above", "deal_status_change"]:
            return self._build_deal_notification(alert, alert_history, trigger_data, user)
        else:
            return self._build_generic_notification(alert, alert_history, trigger_data, user)
    
    def _build_price_notification(self, alert, alert_history, trigger_data, user):
        """Build price alert notification content."""
        ticker = trigger_data.get("company_ticker", "Unknown")
        company_name = trigger_data.get("company_name", "")
        current_price = trigger_data.get("current_price", 0)
        alert_type = alert.alert_type
        
        if alert_type == "price_above":
            subject = f"🚀 {ticker} Price Alert: Above ${alert.trigger_conditions.get('price', 0):.2f}"
            body = f"Hello {user.username},\n\n{company_name} ({ticker}) has moved above your target price of ${alert.trigger_conditions.get('price', 0):.2f}.\n\nCurrent price: ${current_price:.2f}\n\nThis alert was created on {alert.created_at.strftime('%Y-%m-%d')}."
        elif alert_type == "price_below":
            subject = f"📉 {ticker} Price Alert: Below ${alert.trigger_conditions.get('price', 0):.2f}"
            body = f"Hello {user.username},\n\n{company_name} ({ticker}) has dropped below your target price of ${alert.trigger_conditions.get('price', 0):.2f}.\n\nCurrent price: ${current_price:.2f}\n\nThis alert was created on {alert.created_at.strftime('%Y-%m-%d')}."
        else:  # price_change_percent
            change_pct = float(alert_history.trigger_value)
            direction = "📈" if change_pct > 0 else "📉"
            subject = f"{direction} {ticker} Price Change: {change_pct:+.1f}%"
            body = f"Hello {user.username},\n\n{company_name} ({ticker}) has changed by {change_pct:+.1f}% in the specified timeframe.\n\nCurrent price: ${current_price:.2f}\n\nThis alert was created on {alert.created_at.strftime('%Y-%m-%d')}."
        
        return {
            "email_subject": subject,
            "email_body": body,
            "push_title": subject,
            "push_body": f"{ticker}: ${current_price:.2f}",
            "push_data": {
                "ticker": ticker,
                "price": current_price,
                "alert_type": alert_type
            }
        }
    
    def _build_volume_notification(self, alert, alert_history, trigger_data, user):
        """Build volume alert notification content."""
        ticker = trigger_data.get("company_ticker", "Unknown")
        company_name = trigger_data.get("company_name", "")
        current_volume = trigger_data.get("current_volume", 0)
        alert_type = alert.alert_type
        
        if alert_type == "volume_spike":
            multiplier = float(alert_history.trigger_value)
            subject = f"📊 {ticker} Volume Spike: {multiplier:.1f}x average"
            body = f"Hello {user.username},\n\n{company_name} ({ticker}) is experiencing a volume spike of {multiplier:.1f}x the average.\n\nCurrent volume: {current_volume:,}\n\nThis could indicate increased interest or news."
        else:  # volume_above
            target_volume = alert.trigger_conditions.get("volume", 0)
            subject = f"📊 {ticker} High Volume: {current_volume:,}"
            body = f"Hello {user.username},\n\n{company_name} ({ticker}) volume has exceeded your target of {target_volume:,}.\n\nCurrent volume: {current_volume:,}"
        
        return {
            "email_subject": subject,
            "email_body": body,
            "push_title": subject,
            "push_body": f"{ticker}: {current_volume:,} volume",
            "push_data": {
                "ticker": ticker,
                "volume": current_volume,
                "alert_type": alert_type
            }
        }
    
    def _build_news_notification(self, alert, alert_history, trigger_data, user):
        """Build news alert notification content."""
        ticker = trigger_data.get("ticker", "Unknown")
        article_title = trigger_data.get("article_title", "")
        article_url = trigger_data.get("article_url", "")
        alert_type = alert.alert_type
        
        if alert_type == "news_mention":
            subject = f"📰 {ticker} News Mention"
            body = f"Hello {user.username},\n\nNew news about {ticker}:\n\n\"{article_title}\"\n\nRead more: {article_url}"
        elif alert_type == "news_sentiment":
            sentiment = alert_history.trigger_value.split(":")[1].strip() if ":" in alert_history.trigger_value else "neutral"
            subject = f"📰 {ticker} Sentiment Alert: {sentiment}"
            body = f"Hello {user.username},\n\nNews sentiment for {ticker} has triggered your alert:\n\n\"{article_title}\"\n\nSentiment: {sentiment}\n\nRead more: {article_url}"
        else:  # ma_news
            subject = f"🤝 M&A News Alert"
            body = f"Hello {user.username},\n\nNew M&A related news:\n\n\"{article_title}\"\n\nRead more: {article_url}"
        
        return {
            "email_subject": subject,
            "email_body": body,
            "push_title": subject,
            "push_body": article_title[:100] + "..." if len(article_title) > 100 else article_title,
            "push_data": {
                "ticker": ticker,
                "article_url": article_url,
                "alert_type": alert_type
            }
        }
    
    def _build_deal_notification(self, alert, alert_history, trigger_data, user):
        """Build deal alert notification content."""
        target_company = trigger_data.get("target_company", "Unknown")
        acquirer_company = trigger_data.get("acquirer_company", "Unknown")
        deal_value = trigger_data.get("deal_value", 0)
        deal_type = trigger_data.get("deal_type", "")
        alert_type = alert.alert_type
        
        if alert_type == "new_deal":
            subject = f"🤝 New Deal Alert: {target_company}"
            body = f"Hello {user.username},\n\nA new {deal_type} deal has been announced:\n\n{acquirer_company} acquiring {target_company}\nDeal value: ${deal_value:,.0f}M\n\nThis matches your alert criteria."
        elif alert_type == "deal_value_above":
            subject = f"💰 High-Value Deal Alert: ${deal_value:,.0f}M"
            body = f"Hello {user.username},\n\nA high-value deal above your threshold:\n\n{acquirer_company} acquiring {target_company}\nDeal value: ${deal_value:,.0f}M"
        else:  # deal_status_change
            status = trigger_data.get("status", "")
            subject = f"📋 Deal Status Update: {target_company} - {status}"
            body = f"Hello {user.username},\n\nThe deal you're monitoring has changed status:\n\n{acquirer_company} acquiring {target_company}\nNew status: {status.title()}"
        
        return {
            "email_subject": subject,
            "email_body": body,
            "push_title": subject,
            "push_body": f"{target_company} - ${deal_value:,.0f}M",
            "push_data": {
                "target_company": target_company,
                "deal_value": deal_value,
                "alert_type": alert_type
            }
        }
    
    def _build_generic_notification(self, alert, alert_history, trigger_data, user):
        """Build generic notification content."""
        subject = f"🔔 Alert Triggered: {alert.alert_name or 'Unnamed Alert'}"
        body = f"Hello {user.username},\n\nYour alert has been triggered:\n\nAlert type: {alert.alert_type}\nTrigger value: {alert_history.trigger_value}\n\nCreated: {alert.created_at.strftime('%Y-%m-%d')}"
        
        return {
            "email_subject": subject,
            "email_body": body,
            "push_title": subject,
            "push_body": f"Trigger: {alert_history.trigger_value}",
            "push_data": {
                "alert_type": alert.alert_type,
                "trigger_value": alert_history.trigger_value
            }
        }


@shared_task(bind=True, max_retries=2, default_retry_delay=600)
def send_daily_digest(self, user_ids: List[str] = None) -> Dict[str, Any]:
    """Send daily digest emails to users."""
    logger.info(f"Sending daily digest to {len(user_ids) if user_ids else 'active'} users")
    
    results = {
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            # Get users who want daily digest
            query = db.query(User).filter(User.is_active == True)
            
            if user_ids:
                query = query.filter(User.id.in_(user_ids))
            
            users = query.all()
            
            for user in users:
                try:
                    # Check user preferences
                    prefs = getattr(user, 'notification_preferences', {})
                    if not prefs.get("daily_digest", True):
                        results["skipped"] += 1
                        continue
                    
                    # Check if digest already sent today
                    today = datetime.utcnow().date()
                    cache_key = f"daily_digest_sent:{user.id}:{today}"
                    if cache_get(cache_key):
                        results["skipped"] += 1
                        continue
                    
                    # Build digest content
                    digest_content = self._build_daily_digest(db, user)
                    
                    if digest_content:
                        # Send digest email
                        email_sent = run_async_task(EmailService.send_email(
                            to_email=user.email,
                            subject=digest_content["subject"],
                            body=digest_content["body"],
                            html_body=digest_content.get("html_body")
                        ))
                        
                        if email_sent:
                            # Mark as sent
                            cache_set(cache_key, True, ttl=86400)  # 24 hours
                            results["sent"] += 1
                            logger.info(f"Daily digest sent to {user.email}")
                        else:
                            results["failed"] += 1
                    else:
                        results["skipped"] += 1
                        logger.debug(f"No digest content for user {user.id}")
                
                except Exception as e:
                    logger.error(f"Error sending digest to user {user.id}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"User {user.id}: {str(e)}")
        
        logger.info(f"Daily digest completed: {results['sent']} sent, {results['failed']} failed, {results['skipped']} skipped")
        return results
        
    except Exception as e:
        logger.error(f"Daily digest task failed: {e}")
        raise self.retry(exc=e)

    def _build_daily_digest(self, db, user: User) -> Dict[str, str]:
        """Build daily digest content for user."""
        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)
        
        # Get user's watchlist activity
        from models.watchlist import Watchlist
        watchlist_companies = db.query(Company).join(Watchlist).filter(
            Watchlist.user_id == user.id
        ).all()
        
        if not watchlist_companies:
            return None
        
        digest_sections = []
        
        # Watchlist performance
        watchlist_performance = []
        for company in watchlist_companies[:10]:  # Limit to top 10
            # Get price change
            today_ohlc = db.query(OHLCData).filter(
                OHLCData.company_id == company.id,
                OHLCData.date == today
            ).first()
            
            yesterday_ohlc = db.query(OHLCData).filter(
                OHLCData.company_id == company.id,
                OHLCData.date == yesterday
            ).first()
            
            if today_ohlc and yesterday_ohlc:
                change_pct = ((float(today_ohlc.close_price) - float(yesterday_ohlc.close_price)) / float(yesterday_ohlc.close_price)) * 100
                watchlist_performance.append({
                    "ticker": company.ticker,
                    "name": company.name,
                    "price": float(today_ohlc.close_price),
                    "change_pct": change_pct
                })
        
        if watchlist_performance:
            watchlist_performance.sort(key=lambda x: x["change_pct"], reverse=True)
            
            performance_text = "📊 Your Watchlist Performance:\n\n"
            for item in watchlist_performance:
                direction = "🟢" if item["change_pct"] >= 0 else "🔴"
                performance_text += f"{direction} {item['ticker']}: ${item['price']:.2f} ({item['change_pct']:+.2f}%)\n"
            
            digest_sections.append(performance_text)
        
        # Recent triggered alerts
        recent_alerts = db.query(AlertHistory).filter(
            AlertHistory.user_id == user.id,
            AlertHistory.created_at >= yesterday
        ).order_by(AlertHistory.created_at.desc()).limit(5).all()
        
        if recent_alerts:
            alerts_text = "🔔 Recent Alert Activity:\n\n"
            for alert_history in recent_alerts:
                alerts_text += f"• {alert_history.trigger_value} - {alert_history.triggered_at.strftime('%H:%M')}\n"
            digest_sections.append(alerts_text)
        
        # Recent deals
        recent_deals = db.query(Deal).filter(
            Deal.created_at >= yesterday
        ).order_by(Deal.deal_value.desc()).limit(3).all()
        
        if recent_deals:
            deals_text = "🤝 Recent M&A Activity:\n\n"
            for deal in recent_deals:
                deals_text += f"• {deal.acquirer_company} → {deal.target_company} (${deal.deal_value:,.0f}M)\n"
            digest_sections.append(deals_text)
        
        if not digest_sections:
            return None
        
        # Build email content
        subject = f"DealLens Daily Digest - {today.strftime('%B %d, %Y')}"
        body = f"Good morning, {user.username}!\n\nHere's your daily market update:\n\n"
        body += "\n\n".join(digest_sections)
        body += f"\n\nView your full dashboard: https://deallens.com/dashboard\n\nBest regards,\nThe DealLens Team"
        
        return {
            "subject": subject,
            "body": body
        }


@shared_task(bind=True, max_retries=2, default_retry_delay=600)
def send_weekly_summary(self, user_ids: List[str] = None) -> Dict[str, Any]:
    """Send weekly summary emails to users."""
    logger.info(f"Sending weekly summary to {len(user_ids) if user_ids else 'active'} users")
    
    results = {
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            # Only send on Sundays
            if datetime.utcnow().weekday() != 6:  # 6 = Sunday
                logger.info("Not Sunday, skipping weekly summary")
                return {"skipped": True, "reason": "not_sunday"}
            
            query = db.query(User).filter(User.is_active == True)
            
            if user_ids:
                query = query.filter(User.id.in_(user_ids))
            
            users = query.all()
            
            for user in users:
                try:
                    # Check user preferences
                    prefs = getattr(user, 'notification_preferences', {})
                    if not prefs.get("weekly_summary", True):
                        results["skipped"] += 1
                        continue
                    
                    # Check if summary already sent this week
                    week_start = datetime.utcnow().date() - timedelta(days=7)
                    cache_key = f"weekly_summary_sent:{user.id}:{week_start}"
                    if cache_get(cache_key):
                        results["skipped"] += 1
                        continue
                    
                    # Build summary content
                    summary_content = self._build_weekly_summary(db, user)
                    
                    if summary_content:
                        email_sent = run_async_task(EmailService.send_email(
                            to_email=user.email,
                            subject=summary_content["subject"],
                            body=summary_content["body"],
                            html_body=summary_content.get("html_body")
                        ))
                        
                        if email_sent:
                            cache_set(cache_key, True, ttl=604800)  # 7 days
                            results["sent"] += 1
                            logger.info(f"Weekly summary sent to {user.email}")
                        else:
                            results["failed"] += 1
                    else:
                        results["skipped"] += 1
                
                except Exception as e:
                    logger.error(f"Error sending weekly summary to user {user.id}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"User {user.id}: {str(e)}")
        
        logger.info(f"Weekly summary completed: {results['sent']} sent, {results['failed']} failed")
        return results
        
    except Exception as e:
        logger.error(f"Weekly summary task failed: {e}")
        raise self.retry(exc=e)

    def _build_weekly_summary(self, db, user: User) -> Dict[str, str]:
        """Build weekly summary content for user."""
        week_start = datetime.utcnow() - timedelta(days=7)
        
        # Get week's activity
        weekly_alerts = db.query(AlertHistory).filter(
            AlertHistory.user_id == user.id,
            AlertHistory.created_at >= week_start
        ).count()
        
        weekly_deals = db.query(Deal).filter(
            Deal.created_at >= week_start
        ).count()
        
        if weekly_alerts == 0 and weekly_deals == 0:
            return None
        
        subject = f"DealLens Weekly Summary - Week of {week_start.strftime('%B %d')}"
        body = f"""Hello {user.username},

Here's your weekly activity summary:

📊 This Week's Activity:
• {weekly_alerts} alerts triggered
• {weekly_deals} new deals announced

🔍 Keep monitoring your watchlist and alerts for the latest updates.

View your dashboard: https://deallens.com/dashboard

Best regards,
The DealLens Team
"""
        
        return {
            "subject": subject,
            "body": body
        }


@shared_task
def cleanup_notification_logs(days_to_keep: int = 30) -> Dict[str, Any]:
    """Clean up old notification logs."""
    logger.info(f"Starting notification logs cleanup, keeping last {days_to_keep} days")
    
    # This would clean up notification logs if we stored them
    # For now, just return success
    return {"message": "Notification logs cleanup completed"}
