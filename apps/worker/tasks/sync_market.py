from celery import shared_task
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging

from .utilities import (
    get_db_session, 
    fetch_alphavantage_data, 
    run_async_task,
    cache_set,
    cache_get,
    generate_id,
    clean_financial_data,
    is_market_hours
)

logger = logging.getLogger(__name__)

# Import models (need to adjust import path based on your structure)
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../api'))

from app.models.company import Company
from app.models.market_data import MarketData


@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def sync_company_prices(self, tickers: List[str]) -> Dict[str, Any]:
    """Sync current prices for list of company tickers."""
    logger.info(f"Starting price sync for {len(tickers)} tickers")
    
    # Skip during non-market hours to avoid unnecessary API calls
    if not is_market_hours():
        logger.info("Outside market hours, skipping price sync")
        return {"status": "skipped", "reason": "outside_market_hours"}
    
    results = {
        "updated": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            for ticker in tickers:
                try:
                    # Check cache first
                    cache_key = f"price_data:{ticker}"
                    cached_data = cache_get(cache_key)
                    
                    if cached_data:
                        logger.debug(f"Using cached price data for {ticker}")
                        price_data = cached_data
                    else:
                        # Fetch from AlphaVantage
                        logger.info(f"Fetching price data for {ticker}")
                        raw_data = run_async_task(
                            fetch_alphavantage_data("TIME_SERIES_INTRADAY", ticker, interval="5min")
                        )
                        
                        if not raw_data or "Time Series (5min)" not in raw_data:
                            logger.error(f"No intraday data for {ticker}")
                            results["failed"] += 1
                            continue
                        
                        # Extract latest price
                        time_series = raw_data["Time Series (5min)"]
                        latest_time = max(time_series.keys())
                        latest_data = time_series[latest_time]
                        
                        price_data = {
                            "timestamp": latest_time,
                            "open": float(latest_data["1. open"]),
                            "high": float(latest_data["2. high"]),
                            "low": float(latest_data["3. low"]),
                            "close": float(latest_data["4. close"]),
                            "volume": int(latest_data["5. volume"])
                        }
                        
                        # Cache for 1 minute
                        cache_set(cache_key, price_data, ttl=60)
                    
                    # Update company record
                    company = db.query(Company).filter(Company.ticker == ticker).first()
                    if company:
                        company.last_price = price_data["close"]
                        company.updated_at = datetime.utcnow()
                        
                        # Create OHLC record if it's a new trading period
                        existing_ohlc = db.query(MarketData).filter(
                            MarketData.company_id == company.id,
                            MarketData.date == datetime.fromisoformat(price_data["timestamp"].replace('Z', '+00:00')).date()
                        ).first()
                        
                        if not existing_ohlc:
                            ohlc_record = MarketData(
                                id=generate_id("ohlc"),
                                company_id=company.id,
                                date=datetime.fromisoformat(price_data["timestamp"].replace('Z', '+00:00')),
                                open_price=price_data["open"],
                                high_price=price_data["high"],
                                low_price=price_data["low"],
                                close_price=price_data["close"],
                                volume=price_data["volume"]
                            )
                            db.add(ohlc_record)
                        else:
                            # Update existing record with latest data
                            existing_ohlc.high_price = max(existing_ohlc.high_price, price_data["high"])
                            existing_ohlc.low_price = min(existing_ohlc.low_price, price_data["low"])
                            existing_ohlc.close_price = price_data["close"]
                            existing_ohlc.volume = (existing_ohlc.volume or 0) + price_data["volume"]
                        
                        db.commit()
                        results["updated"] += 1
                        logger.info(f"Updated price for {ticker}: ${price_data['close']}")
                    
                except Exception as e:
                    logger.error(f"Error syncing price for {ticker}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"{ticker}: {str(e)}")
        
        logger.info(f"Price sync completed: {results['updated']} updated, {results['failed']} failed")
        return results
        
    except Exception as e:
        logger.error(f"Price sync task failed: {e}")
        raise self.retry(exc=e)


@shared_task(bind=True, max_retries=2, default_retry_delay=600)
def sync_company_fundamentals(self, tickers: List[str]) -> Dict[str, Any]:
    """Sync fundamental data for companies (less frequent)."""
    logger.info(f"Starting fundamentals sync for {len(tickers)} tickers")
    
    results = {
        "updated": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            for ticker in tickers:
                try:
                    # Check cache first (longer TTL for fundamentals)
                    cache_key = f"fundamentals:{ticker}"
                    cached_data = cache_get(cache_key)
                    
                    if cached_data:
                        logger.debug(f"Using cached fundamentals for {ticker}")
                        overview_data = cached_data
                    else:
                        # Fetch company overview from AlphaVantage
                        logger.info(f"Fetching fundamentals for {ticker}")
                        overview_data = run_async_task(
                            fetch_alphavantage_data("OVERVIEW", ticker)
                        )
                        
                        if not overview_data or "Symbol" not in overview_data:
                            logger.error(f"No fundamental data for {ticker}")
                            results["failed"] += 1
                            continue
                        
                        # Cache for 30 minutes
                        cache_set(cache_key, overview_data, ttl=1800)
                    
                    # Clean and update company record
                    company = db.query(Company).filter(Company.ticker == ticker).first()
                    if company:
                        cleaned_data = clean_financial_data(overview_data)
                        
                        # Update company fields
                        company.name = cleaned_data.get("Name", company.name)
                        company.sector = cleaned_data.get("Sector", company.sector)
                        company.industry = cleaned_data.get("Industry", company.industry)
                        company.market_cap = cleaned_data.get("MarketCapitalization", company.market_cap)
                        company.description = cleaned_data.get("Description", company.description)
                        company.employees = cleaned_data.get("FullTimeEmployees", company.employees)
                        
                        # Update financial ratios
                        ratios = {
                            "pe": cleaned_data.get("PERatio"),
                            "pb": cleaned_data.get("PriceToBookRatio"),
                            "ev_ebitda": cleaned_data.get("EVToEBITDA"),
                            "roe": cleaned_data.get("ReturnOnEquityTTM"),
                            "debt_equity": cleaned_data.get("DebtToEquity"),
                            "current_ratio": cleaned_data.get("CurrentRatio"),
                            "gross_margin": cleaned_data.get("GrossProfitTTM"),
                            "operating_margin": cleaned_data.get("OperatingMarginTTM"),
                            "profit_margin": cleaned_data.get("ProfitMargin"),
                            "beta": cleaned_data.get("Beta"),
                            "52_week_high": cleaned_data.get("52WeekHigh"),
                            "52_week_low": cleaned_data.get("52WeekLow"),
                            "dividend_yield": cleaned_data.get("DividendYield"),
                            "forward_pe": cleaned_data.get("ForwardPE")
                        }
                        
                        # Filter out None values
                        company.ratios = {k: v for k, v in ratios.items() if v is not None}
                        company.updated_at = datetime.utcnow()
                        
                        db.commit()
                        results["updated"] += 1
                        logger.info(f"Updated fundamentals for {ticker}")
                    
                except Exception as e:
                    logger.error(f"Error syncing fundamentals for {ticker}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"{ticker}: {str(e)}")
        
        logger.info(f"Fundamentals sync completed: {results['updated']} updated, {results['failed']} failed")
        return results
        
    except Exception as e:
        logger.error(f"Fundamentals sync task failed: {e}")
        raise self.retry(exc=e)


@shared_task(bind=True, max_retries=2, default_retry_delay=300)
def sync_daily_ohlc(self, tickers: List[str], days: int = 5) -> Dict[str, Any]:
    """Sync daily OHLC data for companies."""
    logger.info(f"Starting daily OHLC sync for {len(tickers)} tickers, {days} days")
    
    results = {
        "updated": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with get_db_session() as db:
            for ticker in tickers:
                try:
                    # Fetch daily data
                    logger.info(f"Fetching daily OHLC for {ticker}")
                    daily_data = run_async_task(
                        fetch_alphavantage_data("TIME_SERIES_DAILY_ADJUSTED", ticker, outputsize="compact")
                    )
                    
                    if not daily_data or "Time Series (Daily)" not in daily_data:
                        logger.error(f"No daily OHLC data for {ticker}")
                        results["failed"] += 1
                        continue
                    
                    company = db.query(Company).filter(Company.ticker == ticker).first()
                    if not company:
                        logger.error(f"Company not found for ticker {ticker}")
                        continue
                    
                    time_series = daily_data["Time Series (Daily)"]
                    
                    # Process last N days
                    sorted_dates = sorted(time_series.keys(), reverse=True)
                    for date_str in sorted_dates[:days]:
                        daily_record = time_series[date_str]
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                        
                        # Check if record already exists
                        existing = db.query(MarketData).filter(
                            MarketData.company_id == company.id,
                            MarketData.date == date_obj
                        ).first()
                        
                        if not existing:
                            ohlc_record = MarketData(
                                id=generate_id("ohlc"),
                                company_id=company.id,
                                date=date_obj,
                                open_price=float(daily_record["1. open"]),
                                high_price=float(daily_record["2. high"]),
                                low_price=float(daily_record["3. low"]),
                                close_price=float(daily_record["4. close"]),
                                volume=int(daily_record["6. volume"])
                            )
                            db.add(ohlc_record)
                    
                    db.commit()
                    results["updated"] += 1
                    logger.info(f"Updated daily OHLC for {ticker}")
                    
                except Exception as e:
                    logger.error(f"Error syncing daily OHLC for {ticker}: {e}")
                    results["failed"] += 1
                    results["errors"].append(f"{ticker}: {str(e)}")
        
        logger.info(f"Daily OHLC sync completed: {results['updated']} updated, {results['failed']} failed")
        return results
        
    except Exception as e:
        logger.error(f"Daily OHLC sync task failed: {e}")
        raise self.retry(exc=e)


@shared_task
def cleanup_old_ohlc_data(days_to_keep: int = 365) -> Dict[str, Any]:
    """Clean up old OHLC data to manage database size."""
    logger.info(f"Starting OHLC cleanup, keeping last {days_to_keep} days")
    
    cutoff_date = datetime.utcnow().date() - timedelta(days=days_to_keep)
    
    try:
        with get_db_session() as db:
            deleted_count = db.query(MarketData).filter(
                MarketData.date < cutoff_date
            ).delete()
            
            db.commit()
            
            logger.info(f"Cleaned up {deleted_count} old OHLC records")
            return {"deleted_records": deleted_count, "cutoff_date": cutoff_date.isoformat()}
            
    except Exception as e:
        logger.error(f"OHLC cleanup failed: {e}")
        raise
