"""
Kalshi Sports Liquidity -> Redis Cache

Fetches liquidity data from Kalshi and stores it in Redis for API consumption.

Redis Key Structure:
    kalshi:markets                      -> Set of all market tickers
    kalshi:market:{ticker}              -> HASH with market data
    kalshi:market:{ticker}:orderbook    -> JSON string with full orderbook
    kalshi:series:{series}:markets      -> SET of tickers in this series
    kalshi:liquidity:updated_at         -> Timestamp of last update

Example usage:
    # One-time run
    python kalshi_redis.py

    # Run every 30 seconds (for cron or scheduler)
    python kalshi_redis.py --interval 30

    # Specific series only
    python kalshi_redis.py --series KXNFLGAME KXNBAGAME

Requirements:
    pip install redis requests
"""

import redis
import requests
import json
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from time import sleep
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kalshi API
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Sports series to fetch
DEFAULT_SERIES = [
    "KXNFLGAME",
    "KXNFSPREAD",
    "KXNFLTOTOAL",
    "KXNBAGAME",
    "KXNBASPREAD",
    "KXNBATOTAL",
    "KXMLBGAME",
    "KXNHLGAME",
    "KXNCAAFGAME",
    "KXNCAABGAME",
]

# Redis key prefixes
REDIS_PREFIX = "kalshi"
KEY_MARKETS_SET = f"{REDIS_PREFIX}:markets"
KEY_MARKET = f"{REDIS_PREFIX}:market"        # :{{ticker}}
KEY_ORDERBOOK = f"{REDIS_PREFIX}:orderbook"  # :{ticker}
KEY_SERIES = f"{REDIS_PREFIX}:series"        # :{series}:markets
KEY_UPDATED = f"{REDIS_PREFIX}:updated_at"
KEY_LIQUIDITY_SUMMARY = f"{REDIS_PREFIX}:liquidity:summary"

class KalshiClient:
    """Simple Kalshi API Client"""

    def __init__(self, rate_limit: float = 0.2):
        self.session = requests.Session()
        self.session.headers["Accept"] = "application/json"
        self.rate_limit = rate_limit

    def _get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        url = f"{KALSHI_BASE_URL}{endpoint}"
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        sleep(self.rate_limit)
        return resp.json()
    
    def get_markets(self, series_ticker: str, status: str = "open",
                    limit: int = 200, cursor: Optional[str] = None) -> dict:
        params = {"series_ticker": series_ticker, "status": status, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._get("/markets", params)
    
    def get_orderbook(self, ticker: str) -> dict:
        return self._get(f"/markets/{ticker}/orderbook")
    
class RedisCache:
    """Redis caching layer for Kalshi data"""

    def __init__(self, host: str = "localhost", port: int = 6379,
                 db: int = 0, password: Optional[str] = None, ttl: int = 300):
        """
        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            password: Redis password (if required)
            ttl: Time-to-live for cached data in seconds (default 5 min)
        """
        self.redis = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True  # Return strings instead of bytes
        )
        self.ttl = ttl

    def ping(self) -> bool:
        """Test Redis connection"""
        try:
            self.redis.ping()
            return True
        except redis.ConnectionError:
            return False
        
    def store_market(self, market_data: Dict[str, Any]) -> None:
        """Store market data in Redis hash"""
        ticker = market_data["ticker"]
        key = f"{KEY_MARKET}:{ticker}"

        # Flatten for Redis hash storage
        flat_data = {
            "ticker": ticker,
            "event_ticker": market_data.get("event_ticker", ""),
            "title": market_data.get("title", ""),
            "status": market_data.get("status", ""),
            "close_time": market_data.get("close_time", ""),
            "series_ticker": market_data.get("series_ticker", ""),

            # Team names
            "yes_team": market_data.get("yes_sub_title", "YES"),
            "no_team": market_data.get("no_sub_title", "NO"),

            # Prices (in cents)
            "yes_bid": market_data.get("yes_bid") or 0,
            "yes_ask": market_data.get("yes_ask") or 0,
            "no_bid": market_data.get("no_bid") or 0,
            "no_ask": market_data.get("no_ask") or 0,
            "last_price": market_data.get("last_price") or 0,

            # Volume 
            "volume": market_data.get("volume", 0),
            "volume_24th": market_data.get("volume_24h", 0),
            "open_interest": market_data.get("open_interest", 0),

            # Liquidity (calculated)
            "yes_liquidity_contracts": market_data.get("yes_liquidity_contracts", 0),
            "yes_liquidity_cents": market_data.get("yes_liquidity_cents", 0),
            "no_liquidity_contracts": market_data.get("no_liquidity_contracts", 0),
            "no_liquidity_cents": market_data.get("no_liquidity_cents", 0),

            # Kalshi's reported liquidity
            "kalshi_liquidity": market_data.get("kalshi_liquidity", 0),

            # Metadata
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    
        # Store as hash
        self.redis.hset(key, mapping=flat_data)
        self.redis.expire(key, self.ttl)

        # Add markets set
        self.redis.sadd(KEY_MARKETS_SET, ticker)

        # Add to series set
        series = market_data.get("series_ticker")
        if series:
            self.redis.sadd(f"{KEY_SERIES}:{series}:markets", ticker)

    def store_orderbook(self, ticker: str, orderbook: Dict[str, Any]) -> None:
        """Store full orderbook as JSON string"""
        key = f"{KEY_ORDERBOOK}:{ticker}"

        # Calculate liquidity per level
        processed = {
            "ticker": ticker,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "yes_bids": [],
            "no_bids": []
        }

        for price, qty in orderbook.get("yes", []):
            processed["yes_bids"].append({
                "price_cents": price,
                "quantity": qty,
                "liquidity_cents": price * qty
            })

        for price, qty in orderbook.get("no", []):
            processed["no_bids"].apppend({
                "price_cents": price,
                "quantity": qty,
                "liquidity_cents": price * qty
            })

        self.redis.set(key, json.dumps(processed))
        self.redis.expire(key, self.ttl)

    def store_liquidity_summary(self, summary: Dict[str, Any]) -> None:
        """Store overall liquidity summary"""
        self.redis.set(KEY_LIQUIDITY_SUMMARY, json.dumps(summary))
        self.redis.expire(KEY_LIQUIDITY_SUMMARY, self.ttl)

    def update_timestamp(self) -> None:
        """Update the last refresh timestamp"""
        self.redis.set(KEY_UPDATED, datetime.now(timezone.utc).isoformat())

    # =========== READ METHODS (for our API to use) ===========
    
    def get_market(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get market data by ticker"""
        key = f"{KEY_MARKET:{ticker}}"
        data = self.redis.hgetall(key)  
        if not data:
            return None
        
        # Convert numeric fields back to int
        int_fields = ["yes_bid", "yes_ask", "no_bid", "no_ask", "last_price",
                    "volume", "volume_24h", "open_interest",
                    "yes_liquidity_contracts", "yes_liquidity_cents",
                    "no_liquidity_contracts", "no_liquidity_cents",
                    "kalshi_liquidity"]
            
        for field in int_fields:
            if field in data: # type: ignore
                data[field] = int(data[field]) # type: ignore

        return data # type: ignore

    def get_orderbook(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get full orderbook for a market"""
        key = f"{KEY_ORDERBOOK}:{ticker}"
        data = self.redis.get(key)
        return json.loads(data) if data else None # type: ignore
    
    def get_all_markets(self) -> List[str]:
        """Get list of all cached market tickers"""
        return list(self.redis.smembers(KEY_MARKETS_SET)) # type: ignore
    
    def get_markets_by_series(self, series: str) -> List[str]:
        """Get market tickers for a specific series"""
        return list(self.redis.smembers(f"{KEY_SERIES}:{series}:markets")) # type: ignore
    
    def get_liquidity_summary(self) -> Optional[Dict[str, Any]]:
        """Get overall liquidity summary"""
        data = self.redis.get(KEY_LIQUIDITY_SUMMARY)
        return json.loads(data) if data else None # type: ignore

    def get_last_updated(self) -> Optional[str]:
        """Get timestamp of last data refresh"""
        return self.redis.get(KEY_UPDATED) # type: ignore
    
def calculate_liquidity(orderbook: Dict[str, List]) -> Dict[str, int]:
    """Calculate total liquidity from orderbook"""
    yes_bids = orderbook.get("yes", [])
    no_bids = orderbook.get("no", [])

    yes_contracts = sum(qty for _, qty in yes_bids)
    yes_cents = sum(price * qty for price, qty in yes_bids)

    no_contracts = sum(qty for _, qty in no_bids)
    no_cents = sum(price * qty for price, qty in no_bids)

    return {
        "yes_liquidity_contracts": yes_contracts,
        "yes_liquidity_cents": yes_cents,
        "no_liquidity_contracts": no_contracts,
        "no_liquidity_cents": no_cents
    }

def fetch_and_cache(
        kalshi: KalshiClient,
        cache: RedisCache,
        series_list: List[str]
) -> Dict[str, Any]:
    """Fetch all markets and store in Redis"""

    total_markets = 0
    total_yes_liquidity = 0
    total_no_liquidity = 0

    for series in series_list:
        logger.info(f"Fetching {series}...")
        cursor = None
        series_count = 0

        while True:
            try:
                data = kalshi.get_markets(series, cursor=cursor)
            except requests.HTTPError as e:
                logger.warning(f"Error fetching {series}: {e}")
                break

            markets = data.get("markets", [])
            if not markets:
                break

            for m in markets:
                ticker = m.get("ticker")

                # Fetch orderbook
                try:
                    ob_data = kalshi.get_orderboook(ticker) # type: ignore
                    orderbook = ob_data.get("orderbook", {})
                except Exception as e:
                    logger.warning(f"Could not get orderbook for {ticker}: {e}")
                    orderbook = {"yes": [], "no": []}

                # Calculate liquidity
                liquidity = calculate_liquidity(orderbook)

                # Merge market data with liquidity
                market_data = {
                    **m,
                    "series_ticker": series,
                    **liquidity
                }

                # Store in Redis
                cache.store_market(market_data)
                cache.store_orderbook(ticker, orderbook)

                # Track totals
                total_yes_liquidity += liquidity["yes_liquidity_cents"]
                total_no_liquidity += liquidity["no_liquidity_cents"]
                series_count += 1

                logger.debug(f"  {ticker}: YES=${liquidity['yes_liquidity_cents']/100:.0f} "
                             f"NO=${liquidity['no_liquidity_cents']/100:.0f}")
                
            cursor = data.get("cursor")
            if not cursor:
                break

        logger.info(f"  -> {series_count} markets cached")
        total_markets += series_count

    # Store summary
    summary = {
        "total_markets": total_markets,
        "total_yes_liquidity_cents": total_yes_liquidity,
        "total_no_liquidity_cents": total_no_liquidity,
        "total_yes_liquidity_dollars": round(total_yes_liquidity / 100, 2),
        "total_no_liquidity_dollars": round(total_no_liquidity / 100, 2),
        "series_fetched": series_list,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    cache.store_liquidity_summary(summary)
    cache.update_timestamp()

    return summary

def main():
    parser = argparse.ArgumentParser(description="Kalshi -> Redis Cache")
    parser.add_argument("--redis-host", default="localhost", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis database")
    parser.add_argument("--redis-password", default=None, help="Redis password")
    parser.add_argument("--ttl", type=int, default=300, help="Cache TTL in seconds")
    parser.add_argument("--series", nargs="+", default=None, help="Series to fetch")
    parser.add_argument("--interval", type=int, default=None, 
                        help="Refresh interval in seconds (runs once if not set)")
    args = parser.parse_args()

    # Initialize clients
    kalshi = KalshiClient()
    cache = RedisCache(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        password=args.redis_password,
        ttl=args.ttl
    )

    # Test Redis connection
    if not cache.ping():
        logger.error("Could not connect to Redis!")
        return
    
    logger.info("Connected to Redis")

    series_list = args.series or DEFAULT_SERIES

    # Run once or continuously
    while True:
        logger.info("=" * 50)
        logger.info("Starting Kalshi data fetch...")

        try:
            summary = fetch_and_cache(kalshi, cache, series_list)
            logger.info(f"Done! {summary['total_markets']} markets cached")
            logger.info(f"Total liquidity: ${summary['total_yes_liquidity_dollars'] + summary['total_no_liquidity_dollars']:,.2f}")
        except Exception as e:
            logger.error(f"Error during fetch: {e}")

        if args.interval:
            logger.info(f"Sleeping {args.interval}s until next refresh...")
            sleep(args.interval)
        else:
            break