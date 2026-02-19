from celery import shared_task
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import json

from .utilities import (
    get_db_session, 
    openai_chat_completion,
    run_async_task,
    cache_set,
    cache_get,
    generate_id,
    estimate_openai_cost
)

logger = logging.getLogger(__name__)

# Import models
# sys.path hack is handled in celery_app.py but keeping for standalone run safety
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../api'))

from app.models.company import Company
from app.models.market_data import MarketData
from app.models.market_data import NewsItem as NewsArticle
from app.models.ai_insight import AIInsight
from app.models.deal import Deal

OHLCData = MarketData  # Alias for compatibility


@shared_task(bind=True, max_retries=2, default_retry_delay=1800)
def generate_company_insights(self, company_ids: List[str] = None, batch_size: int = 5) -> Dict[str, Any]:
    """Generate AI-powered insights for companies."""
    logger.info(f"Starting company insights generation for {len(company_ids) if company_ids else 'eligible'} companies")
    
    results = {
        "generated": 0,
        "failed": 0,
        "skipped": 0,
        "cost_estimate": 0.0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            # Get companies to analyze
            query = db.query(Company)
            
            if company_ids:
                query = query.filter(Company.id.in_(company_ids))
            else:
                # Get companies that need fresh insights (older than 24 hours)
                cutoff_time = datetime.utcnow() - timedelta(hours=24)
                subquery = db.query(AIInsight.entity_id).filter(
                    AIInsight.entity_type == "company",
                    AIInsight.created_at >= cutoff_time
                ).subquery()
                
                query = query.filter(~Company.id.in_(subquery))
            
            companies = query.limit(batch_size).all()
            
            if not companies:
                logger.info("No companies need insights")
                return results
            
            for company in companies:
                try:
                    # Check cache first
                    cache_key = f"company_insight:{company.id}"
                    cached_insight = cache_get(cache_key)
                    
                    if cached_insight:
                        logger.debug(f"Using cached insight for {company.ticker}")
                        results["skipped"] += 1
                        continue
                    
                    # Gather data for analysis
                    logger.info(f"Generating insights for {company.ticker}")
                    
                    # Get recent OHLC data
                    recent_ohlc = db.query(OHLCData).filter(
                        OHLCData.company_id == company.id,
                        OHLCData.date >= datetime.utcnow().date() - timedelta(days=30)
                    ).order_by(OHLCData.date.desc()).limit(20).all()
                    
                    # Get recent news
                    recent_news = db.query(NewsArticle).join(
                        NewsArticle.related_companies
                    ).filter(
                        Company.id == company.id,
                        NewsArticle.created_at >= datetime.utcnow() - timedelta(days=14)
                    ).order_by(NewsArticle.published_at.desc()).limit(10).all()
                    
                    # Build analysis prompt
                    price_data = []
                    for ohlc in recent_ohlc:
                        price_data.append({
                            "date": ohlc.date.strftime("%Y-%m-%d"),
                            "close": float(ohlc.close_price),
                            "volume": int(ohlc.volume or 0)
                        })
                    
                    news_summaries = []
                    for news in recent_news:
                        news_summaries.append({
                            "title": news.title,
                            "date": news.published_at.strftime("%Y-%m-%d"),
                            "sentiment": getattr(news, 'sentiment_label', 'neutral')
                        })
                    
                    prompt = f"""
                    Analyze this company and provide investment insights:
                    
                    Company: {company.name} ({company.ticker})
                    Sector: {company.sector}
                    Market Cap: ${company.market_cap:,.0f}
                    Current Price: ${company.last_price:.2f}
                    
                    Financial Ratios: {json.dumps(company.ratios or {}, indent=2)}
                    
                    Recent Price Data (last 30 days): {json.dumps(price_data, indent=2)}
                    
                    Recent News Headlines: {json.dumps(news_summaries, indent=2)}
                    
                    Provide a comprehensive analysis covering:
                    1. Current valuation assessment
                    2. Technical price trends and momentum
                    3. Key strengths and risks
                    4. Recent news impact
                    5. Investment recommendation (Buy/Hold/Sell)
                    6. Price target (if applicable)
                    
                    Format as JSON: {{
                        "summary": "brief overall assessment",
                        "valuation": "overvalued/fairly_valued/undervalued",
                        "technical_outlook": "bullish/neutral/bearish",
                        "strengths": ["strength1", "strength2"],
                        "risks": ["risk1", "risk2"],
                        "recommendation": "buy/hold/sell",
                        "price_target": 123.45,
                        "confidence": 0.85,
                        "key_catalysts": ["catalyst1", "catalyst2"]
                    }}
                    """
                    
                    # Call OpenAI
                    response = run_async_task(openai_chat_completion(
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=800,
                        temperature=0.2
                    ))
                    
                    if response and "choices" in response:
                        content = response["choices"][0]["message"]["content"]
                        cost_estimate = estimate_openai_cost(prompt, content, "gpt-3.5-turbo")
                        results["cost_estimate"] += cost_estimate
                        
                        try:
                            # Parse JSON response
                            insight_data = json.loads(content)
                            
                            # Create AI insight record
                            ai_insight = AIInsight(
                                id=generate_id("insight"),
                                entity_type="company",
                                entity_id=company.id,
                                insight_type="investment_analysis",
                                content=insight_data,
                                summary=insight_data.get("summary", ""),
                                confidence_score=insight_data.get("confidence", 0.5),
                                metadata={
                                    "ticker": company.ticker,
                                    "price_at_analysis": company.last_price,
                                    "data_points": len(price_data) + len(news_summaries),
                                    "cost_estimate": cost_estimate
                                },
                                created_at=datetime.utcnow()
                            )
                            
                            db.add(ai_insight)
                            db.commit()
                            
                            # Cache for 2 hours
                            cache_set(cache_key, insight_data, ttl=7200)
                            
                            results["generated"] += 1
                            logger.info(f"Generated insight for {company.ticker}")
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse AI response for {company.ticker}: {e}")
                            results["failed"] += 1
                    else:
                        logger.error(f"No response from OpenAI for {company.ticker}")
                        results["failed"] += 1
                
                except Exception as e:
                    logger.error(f"Error generating insight for company {company.ticker}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"{company.ticker}: {str(e)}")
        
        logger.info(f"Company insights generation completed: {results['generated']} generated, {results['failed']} failed, cost: ${results['cost_estimate']:.4f}")
        return results
        
    except Exception as e:
        logger.error(f"Company insights generation task failed: {e}")
        raise self.retry(exc=e)


@shared_task(bind=True, max_retries=2, default_retry_delay=1800)
def generate_deal_insights(self, deal_ids: List[str] = None, batch_size: int = 3) -> Dict[str, Any]:
    """Generate AI-powered insights for M&A deals."""
    logger.info(f"Starting deal insights generation for {len(deal_ids) if deal_ids else 'eligible'} deals")
    
    results = {
        "generated": 0,
        "failed": 0,
        "skipped": 0,
        "cost_estimate": 0.0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            # Get deals to analyze
            query = db.query(Deal)
            
            if deal_ids:
                query = query.filter(Deal.id.in_(deal_ids))
            else:
                # Get recent deals that need insights
                cutoff_time = datetime.utcnow() - timedelta(hours=48)
                subquery = db.query(AIInsight.entity_id).filter(
                    AIInsight.entity_type == "deal",
                    AIInsight.created_at >= cutoff_time
                ).subquery()
                
                query = query.filter(
                    ~Deal.id.in_(subquery),
                    Deal.created_at >= datetime.utcnow() - timedelta(days=30)
                )
            
            deals = query.limit(batch_size).all()
            
            if not deals:
                logger.info("No deals need insights")
                return results
            
            for deal in deals:
                try:
                    # Check cache first
                    cache_key = f"deal_insight:{deal.id}"
                    cached_insight = cache_get(cache_key)
                    
                    if cached_insight:
                        logger.debug(f"Using cached insight for deal {deal.id}")
                        results["skipped"] += 1
                        continue
                    
                    logger.info(f"Generating insights for deal: {deal.target_company}")
                    
                    # Get related news articles
                    related_news = []
                    if deal.target_company or deal.acquirer_company:
                        search_terms = [term for term in [deal.target_company, deal.acquirer_company] if term]
                        
                        for term in search_terms:
                            news = db.query(NewsArticle).filter(
                                NewsArticle.title.contains(term) | NewsArticle.description.contains(term),
                                NewsArticle.created_at >= deal.created_at - timedelta(days=30)
                            ).limit(5).all()
                            
                            for article in news:
                                related_news.append({
                                    "title": article.title,
                                    "date": article.published_at.strftime("%Y-%m-%d"),
                                    "sentiment": getattr(article, 'sentiment_label', 'neutral'),
                                    "source": article.source_name
                                })
                    
                    # Build analysis prompt
                    prompt = f"""
                    Analyze this M&A deal and provide strategic insights:
                    
                    Deal Overview:
                    Target: {deal.target_company}
                    Acquirer: {deal.acquirer_company}
                    Deal Value: ${deal.deal_value:,.0f} ({deal.deal_value_currency})
                    Deal Type: {deal.deal_type}
                    Status: {deal.status}
                    Announced: {deal.announced_date.strftime('%Y-%m-%d') if deal.announced_date else 'Unknown'}
                    Industry: {deal.industry}
                    
                    Deal Rationale: {deal.deal_rationale or 'Not specified'}
                    
                    Financial Metrics:
                    Revenue Multiple: {deal.revenue_multiple}x
                    EBITDA Multiple: {deal.ebitda_multiple}x
                    Premium: {deal.premium_percent:.1f}%
                    
                    Related News: {json.dumps(related_news, indent=2)}
                    
                    Provide comprehensive analysis covering:
                    1. Strategic rationale assessment
                    2. Valuation analysis (fair/expensive/cheap)
                    3. Deal completion probability
                    4. Key success factors and risks
                    5. Market impact
                    6. Synergy potential
                    
                    Format as JSON: {{
                        "summary": "brief deal assessment",
                        "strategic_rationale": "strong/moderate/weak",
                        "valuation_assessment": "expensive/fair/cheap",
                        "completion_probability": 0.85,
                        "key_strengths": ["strength1", "strength2"],
                        "key_risks": ["risk1", "risk2"],
                        "synergy_potential": "high/medium/low",
                        "market_impact": "positive/neutral/negative",
                        "confidence": 0.75,
                        "comparable_deals": ["deal1", "deal2"]
                    }}
                    """
                    
                    # Call OpenAI
                    response = run_async_task(openai_chat_completion(
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=1000,
                        temperature=0.2
                    ))
                    
                    if response and "choices" in response:
                        content = response["choices"][0]["message"]["content"]
                        cost_estimate = estimate_openai_cost(prompt, content, "gpt-3.5-turbo")
                        results["cost_estimate"] += cost_estimate
                        
                        try:
                            # Parse JSON response
                            insight_data = json.loads(content)
                            
                            # Create AI insight record
                            ai_insight = AIInsight(
                                id=generate_id("insight"),
                                entity_type="deal",
                                entity_id=deal.id,
                                insight_type="deal_analysis",
                                content=insight_data,
                                summary=insight_data.get("summary", ""),
                                confidence_score=insight_data.get("confidence", 0.5),
                                metadata={
                                    "target_company": deal.target_company,
                                    "deal_value": deal.deal_value,
                                    "deal_type": deal.deal_type,
                                    "related_news_count": len(related_news),
                                    "cost_estimate": cost_estimate
                                },
                                created_at=datetime.utcnow()
                            )
                            
                            db.add(ai_insight)
                            db.commit()
                            
                            # Cache for 4 hours
                            cache_set(cache_key, insight_data, ttl=14400)
                            
                            results["generated"] += 1
                            logger.info(f"Generated insight for deal: {deal.target_company}")
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse AI response for deal {deal.id}: {e}")
                            results["failed"] += 1
                    else:
                        logger.error(f"No response from OpenAI for deal {deal.id}")
                        results["failed"] += 1
                
                except Exception as e:
                    logger.error(f"Error generating insight for deal {deal.id}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"{deal.id}: {str(e)}")
        
        logger.info(f"Deal insights generation completed: {results['generated']} generated, {results['failed']} failed, cost: ${results['cost_estimate']:.4f}")
        return results
        
    except Exception as e:
        logger.error(f"Deal insights generation task failed: {e}")
        raise self.retry(exc=e)


@shared_task(bind=True, max_retries=2, default_retry_delay=3600)
def generate_market_analysis(self, analysis_type: str = "daily_summary") -> Dict[str, Any]:
    """Generate market-wide analysis and insights."""
    logger.info(f"Starting market analysis generation: {analysis_type}")
    
    results = {
        "generated": 0,
        "failed": 0,
        "cost_estimate": 0.0,
        "analysis_type": analysis_type
    }
    
    try:
        with get_db_session() as db:
            # Check cache first
            cache_key = f"market_analysis:{analysis_type}:{datetime.utcnow().strftime('%Y-%m-%d')}"
            cached_analysis = cache_get(cache_key)
            
            if cached_analysis:
                logger.debug(f"Using cached market analysis for {analysis_type}")
                return {"cached": True, "data": cached_analysis}
            
            if analysis_type == "daily_summary":
                # Get today's market data
                today = datetime.utcnow().date()
                yesterday = today - timedelta(days=1)
                
                # Top gainers/losers
                recent_ohlc = db.query(OHLCData).filter(
                    OHLCData.date.in_([today, yesterday])
                ).order_by(OHLCData.date.desc()).all()
                
                # Calculate day changes
                price_changes = {}
                for ohlc in recent_ohlc:
                    company_id = ohlc.company_id
                    if company_id not in price_changes:
                        price_changes[company_id] = {"current": None, "previous": None}
                    
                    if ohlc.date == today:
                        price_changes[company_id]["current"] = float(ohlc.close_price)
                    elif ohlc.date == yesterday:
                        price_changes[company_id]["previous"] = float(ohlc.close_price)
                
                # Get top movers
                movers = []
                for company_id, prices in price_changes.items():
                    if prices["current"] and prices["previous"]:
                        change_pct = ((prices["current"] - prices["previous"]) / prices["previous"]) * 100
                        company = db.query(Company).filter(Company.id == company_id).first()
                        if company:
                            movers.append({
                                "ticker": company.ticker,
                                "name": company.name,
                                "change_percent": round(change_pct, 2),
                                "current_price": prices["current"]
                            })
                
                # Sort and get top/bottom
                movers.sort(key=lambda x: x["change_percent"], reverse=True)
                top_gainers = movers[:5]
                top_losers = movers[-5:]
                
                # Get recent news sentiment
                recent_news = db.query(NewsArticle).filter(
                    NewsArticle.created_at >= datetime.utcnow() - timedelta(days=1),
                    NewsArticle.sentiment_score.isnot(None)
                ).all()
                
                avg_sentiment = sum(n.sentiment_score for n in recent_news) / len(recent_news) if recent_news else 0
                
                # Get recent deals
                recent_deals = db.query(Deal).filter(
                    Deal.created_at >= datetime.utcnow() - timedelta(days=7)
                ).order_by(Deal.deal_value.desc()).limit(5).all()
                
                deal_summary = []
                for deal in recent_deals:
                    deal_summary.append({
                        "target": deal.target_company,
                        "acquirer": deal.acquirer_company,
                        "value": deal.deal_value,
                        "type": deal.deal_type
                    })
                
                # Build analysis prompt
                prompt = f"""
                Generate a daily market summary based on the following data:
                
                Top Gainers Today: {json.dumps(top_gainers, indent=2)}
                
                Top Losers Today: {json.dumps(top_losers, indent=2)}
                
                News Sentiment (avg): {avg_sentiment:.2f} (-1 to 1 scale)
                Recent News Count: {len(recent_news)}
                
                Recent M&A Deals (last 7 days): {json.dumps(deal_summary, indent=2)}
                
                Provide a comprehensive daily market analysis covering:
                1. Overall market sentiment
                2. Key market movers and themes
                3. M&A activity trends
                4. Notable developments
                5. Outlook for tomorrow
                
                Format as JSON: {{
                    "overall_sentiment": "bullish/neutral/bearish",
                    "market_summary": "brief market overview",
                    "key_themes": ["theme1", "theme2"],
                    "notable_movers": ["ticker1 (+X%): reason", "ticker2 (-Y%): reason"],
                    "ma_activity": "high/moderate/low",
                    "outlook": "positive/neutral/negative",
                    "key_events": ["event1", "event2"],
                    "recommendation": "buy/hold/caution"
                }}
                """
                
            else:
                logger.error(f"Unknown analysis type: {analysis_type}")
                results["failed"] = 1
                return results
            
            # Call OpenAI
            response = run_async_task(openai_chat_completion(
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1200,
                temperature=0.3
            ))
            
            if response and "choices" in response:
                content = response["choices"][0]["message"]["content"]
                cost_estimate = estimate_openai_cost(prompt, content, "gpt-3.5-turbo")
                results["cost_estimate"] = cost_estimate
                
                try:
                    # Parse JSON response
                    analysis_data = json.loads(content)
                    
                    # Create AI insight record
                    ai_insight = AIInsight(
                        id=generate_id("insight"),
                        entity_type="market",
                        entity_id="global",
                        insight_type=analysis_type,
                        content=analysis_data,
                        summary=analysis_data.get("market_summary", ""),
                        confidence_score=0.8,
                        metadata={
                            "analysis_date": datetime.utcnow().strftime("%Y-%m-%d"),
                            "data_points": len(movers) + len(recent_news) + len(recent_deals),
                            "cost_estimate": cost_estimate
                        },
                        created_at=datetime.utcnow()
                    )
                    
                    db.add(ai_insight)
                    db.commit()
                    
                    # Cache for 4 hours
                    cache_set(cache_key, analysis_data, ttl=14400)
                    
                    results["generated"] = 1
                    logger.info(f"Generated market analysis: {analysis_type}")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse market analysis response: {e}")
                    results["failed"] = 1
            else:
                logger.error(f"No response from OpenAI for market analysis")
                results["failed"] = 1
        
        logger.info(f"Market analysis generation completed: {results['generated']} generated, cost: ${results['cost_estimate']:.4f}")
        return results
        
    except Exception as e:
        logger.error(f"Market analysis generation task failed: {e}")
        raise self.retry(exc=e)


@shared_task
def cleanup_old_insights(days_to_keep: int = 90) -> Dict[str, Any]:
    """Clean up old AI insights to manage database size."""
    logger.info(f"Starting insights cleanup, keeping last {days_to_keep} days")
    
    cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
    
    try:
        with get_db_session() as db:
            deleted_count = db.query(AIInsight).filter(
                AIInsight.created_at < cutoff_date
            ).delete()
            
            db.commit()
            
            logger.info(f"Cleaned up {deleted_count} old AI insights")
            return {"deleted_insights": deleted_count, "cutoff_date": cutoff_date.isoformat()}
            
    except Exception as e:
        logger.error(f"Insights cleanup failed: {e}")
        raise
