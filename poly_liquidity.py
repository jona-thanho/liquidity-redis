"""
Polymarket Liquidity -> Redis Cache

Fetches liquidity and orderbook data from Polymarket and stores it in Redis for API consumption.

Redis Key Structure:
    polymarket:markets                      -> Set of all condition_ids
    polymarket:market:{condition_id}        -> HASH with market data
    polymarket:market:{condition_id}:orderbook:yes -> JSON string with YES orderbook
    polymarket:market:{condition_id}:orderbook:no  -> JSON string with NO orderbook
    polymarket:tag:{tag_id}:markets         -> SET of condition_ids in this tag
    polymarket:liquidity:updated_at         -> Timestamp of last update
    polymarket:liquidity:summary            -> JSON with overall stats

Example usage:
    # One-time run (all active markets)
    python polymarket_redis.py

    # Run every 30 seconds
    python polymarket_redis.py --interval 30

    # Sports game lines only (moneyline, spreads, totals)
    python polymarket_redis.py --sports-only

    # Sports game lines, skip orderbooks for faster fetching
    python polymarket_redis.py --sports-only --skip-orderbooks -- limit 100

    # Specific tag only (includes futures, props, non-game markets)
    python polymarket_redis.py --tag-id 450

    Tag IDs:
        450     - NFL (all NFL-related markets)
        745     - NBA
        899     - NHL
        100639  - All Sports

    # Limit number of markets
    python polymarket_redis.py --limit 100

    # Adjust rate limiting
    python polymarket_redis.py --rate-limit 0.2

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

def decimal_to_american(price: float) -> int:
    """
    Convert Polymarket price (0.00-1.00) to American odds.

    price = implied probability

    Examples:
        0.61 (61%) -> -156
        0.39 (39%) -> +156
        0.50 (50%) -> -100/+100
    """
    if price <= 0 or price >= 1:
        return 0
    
    if price >= 0.5:
        # Favorite: negative odds
        return int(-100 * price / (1 - price))
    else:
        # Underdog: positive odds
        return int(100 * (1 - price) / price)
    
# Polymarket API Endpoints
GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"

# Redis key prefixes
REDIS_PREFIX = "polymarket"
KEY_MARKETS_SET = f"{REDIS_PREFIX}:markets"
KEY_MARKET = f"{REDIS_PREFIX}:market"           # :{condition_id}
KEY_ORDERBOOK = f"{REDIS_PREFIX}:orderbook"     # :{condition_id}:{side}
KEY_TAG = f"{REDIS_PREFIX}:tag"                 # :{tag_id}:markets
KEY_UPDATED = f"{REDIS_PREFIX}:updated_at"
KEY_LIQUIDITY_SUMMARY = f"{REDIS_PREFIX}:liquidity:summary"

class PolymarketClient:
    """Polymarket API Client for Gamma (markets) and CLOB (orderbooks)"""

    def __init__(self, rate_limit: float = 0):
        self.session = requests.Session()
        self.session.headers["Accept"] = "application/json"
        self.rate_limit = rate_limit

    def _get(self, url: str, params: Optional[dict] = None) -> Any:
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        sleep(self.rate_limit)
        return resp.json()
    
    def get_markets(
        self,
        limit: int = 100,
        offset: int = 0,
        closed: bool = False,
        tag_id: Optional[int] = None,
        order: str = "liquidityNum",
        ascending: bool = False,
        sports_market_types: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch markets from Gamma API with pagination"""
        params: List[tuple] = [
            ("limit", limit),
            ("offset", offset),
            ("closed", str(closed).lower()),
            ("order", order),
            ("ascending", str(ascending).lower()),
        ]
        if tag_id:
            params.append(("tag_id", tag_id))
        if sports_market_types:
            for smt in sports_market_types:  
                params.append(("sports_market_types", smt))

        url = f"{GAMMA_API_URL}/markets"
        return self._get(url, params) # type: ignore
    
    def get_orderbook(self, token_id: str) -> Dict[str, Any]:
        """Fetch orderbook from CLOB API for a specific token"""
        url = f"{CLOB_API_URL}/book"
        params = {"token_id": token_id}
        return self._get(url, params)
    
    def get_midpoint(self, token_id: str) -> Optional[float]:
        """Get midpoint price for a token"""
        url = f"{CLOB_API_URL}/midpoint"
        params = {"token_id": token_id}
        try:
            data = self._get(url, params)
            return float(data.get("mid", 0))
        except Exception:
            return None
        
    def get_price(self, token_id: str, side: str = "BUY") -> Optional[float]:
        """Get best price for a token"""
        url = f"{CLOB_API_URL}/price"
        params = {"token_id": token_id, "side": side}
        try:
            data = self._get(url, params)
            return float(data.get("price", 0))
        except Exception:
            return None
        
class RedisCache:
    """Redis caching layer for Polymarket data"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        ttl: int = 300
    ):
        self.redis: redis.Redis[str] = redis.Redis(  
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
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
        condition_id = market_data.get("conditionId", "")
        if not condition_id:
            return
        
        key = f"{KEY_MARKET}:{condition_id}"

        # Parse outcomes and prices (stored as JSON strings in API)
        outcomes = []
        outcome_prices = []
        try:
            if market_data.get("outcomes"):
                outcomes = json.loads(market_data["outcomes"]) if isinstance(
                    market_data["outcomes"], str) else market_data["outcomes"]
            if market_data.get("outcomePrices"):
                outcome_prices = json.loads(market_data["outcomePrices"]) if isinstance(
                    market_data["outcomePrices"], str) else market_data["outcomePrices"]
        except (json.JSONDecodeError, TypeError):
            pass
 
        # Parse clob token IDs
        clob_token_ids = []
        try:
            if market_data.get("clobTokenIds"):
                clob_token_ids = json.loads(market_data["clobTokenIds"]) if isinstance(
                    market_data["clobTokenIds"], str) else market_data["clobTokenIds"]
        except (json.JSONDecodeError, TypeError):
            pass

        # Get prices as floats
        yes_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else 0.0
        no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else 0.0

        # Flatten for Redis hash storage
        flat_data: Dict[str, Any] = {
            "condition_id": condition_id,
            "question": market_data.get("question", ""),
            "slug": market_data.get("slug", ""),
            "description": (market_data.get("description", "") or "")[:500],
            "end_date": market_data.get("endDate", ""),
            "active": str(market_data.get("active", False)),
            "closed": str(market_data.get("closed", False)),
            "accepting_orders": str(market_data.get("acceptingOrders", False)),

            # Outcomes
            "outcomes": json.dumps(outcomes),
            "outcome_prices":json.dumps(outcome_prices),

            # Token IDs for orderbook queries
            "clob_token_ids": json.dumps(clob_token_ids),
            "yes_token_id": clob_token_ids[0] if len(clob_token_ids) > 0 else "",
            "no_token_id": clob_token_ids[1] if len(clob_token_ids) > 1 else "",

            # Prices (0.00-1.00 scale)
            "yes_price": yes_price,
            "no_price": no_price,
            "last_trade_price": market_data.get("lastTradePrice") or 0,
            "best_bid": market_data.get("bestBid") or 0,
            "best_ask": market_data.get("bestAsk") or 0,
            "spread": market_data.get("spread") or 0,

            # American odds (converted)
            "yes_american": decimal_to_american(yes_price),
            "no_american": decimal_to_american(no_price),

            # Volume & Liquidity
            "volume": market_data.get("volumeNum") or market_data.get("volume") or 0,
            "volume_24h": market_data.get("volume24hr") or 0,
            "volume_clob": market_data.get("volumeClob") or 0,
            "liquidity": market_data.get("liquidityNum") or market_data.get("liquidity") or 0,
            "liquidity_clob": market_data.get("liquidityClob") or 0,

            # Calculated liquidity from orderbooks (filled later)
            "yes_liquidity_usd": market_data.get("yes_liquidity_usd", 0),
            "no_liquidity_usd": market_data.get("no_liquidity_usd", 0),

            # Market info
            "neg_risk": str(market_data.get("negRiskOther", False)),
            "market_type": market_data.get("sportsMarketType", ""),
            "game_id": market_data.get("gameId", ""),

            # Metadata
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        # Convert all values to strings for Redis (after building flat_data)
        flat_data_clean: Dict[str, str] = {
            k: "" if v is None else str(v)
            for k, v in flat_data.items()
        }

        # Store as hash
        self.redis.hset(key, mapping=flat_data_clean) # type: ignore
        self.redis.expire(key, self.ttl)

        # Add to markets set
        self.redis.sadd(KEY_MARKETS_SET, condition_id)

        # Add to tag sets
        tags = market_data.get("tags", [])
        for tag in tags:
            tag_id = tag.get("id") if isinstance(tag, dict) else tag
            if tag_id:
                self.redis.sadd(f"{KEY_TAG}:{tag_id}:markets", condition_id)

    def store_orderbook(
        self,
        condition_id: str,
        side: str,
        orderbook: Dict[str, Any]
    ) -> Dict[str, float]:
        """Store orderbook as JSON string and return liquidity stats"""
        key = f"{KEY_ORDERBOOK}:{condition_id}:{side}"

        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])

        # Calculate liquidity
        bid_liquidity = sum(
            float(b.get("size", 0)) * float(b.get("price", 0))
            for b in bids
        )
        ask_liquidity = sum(
            float(a.get("size", 0)) * float(a.get("price", 0))
            for a in asks
        )

        processed = {
            "condition_id": condition_id,
            "side": side,
            "token_id": orderbook.get("asset_id", ""),
            "timestamp": orderbook.get("timestamp", ""),
            "neg_risk": orderbook.get("neg_risk", False),
            "tick_size": orderbook.get("tick_size", "0.01"),
            "min_order_size": orderbook.get("min_order_size", "0"),
            "bids": [
                {
                    "price": float(b.get("price", 0)),
                    "price_american": decimal_to_american(float(b.get("price", 0))),
                    "size": float(b.get("size", 0)),
                    "liquidity_usd": float(b.get("size", 0)) * float(b.get("price", 0))
                }
                for b in bids
            ],
            "asks": [
                {
                    "price": float(a.get("price", 0)),
                    "price_american": decimal_to_american(float(a.get("price", 0))),
                    "size": float(a.get("size", 0)),
                    "liquidity_usd": float(a.get("size", 0)) * float(a.get("price", 0))
                }
                for a in asks
            ],
            "total_bid_liquidity_usd": bid_liquidity,
            "total_ask_liquidity_usd": ask_liquidity,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        self.redis.set(key, json.dumps(processed))
        self.redis.expire(key, self.ttl)

        return {
            "bid_liquidity": bid_liquidity,
            "ask_liquidity": ask_liquidity
        }

    def update_market_liquidity(
            self,
            condition_id: str,
            yes_liquidity: float,
            no_liquidity: float
    ) -> None:
        """Update market with calculated orderbook liquidity"""
        key = f"{KEY_MARKET}:{condition_id}"
        self.redis.hset(key, mapping={
            "yes_liquidity_usd": yes_liquidity,
            "no_liquidity_usd": no_liquidity,
        })

    def store_liquidity_summary(self, summary: Dict[str, Any]) -> None:
        """Store overall liquidity summary"""
        self.redis.set(KEY_LIQUIDITY_SUMMARY, json.dumps(summary))
        self.redis.expire(KEY_LIQUIDITY_SUMMARY, self.ttl)

    def update_timestamp(self) -> None:
        """Update the last refresh timestamp"""
        self.redis.set(KEY_UPDATED, datetime.now(timezone.utc).isoformat())

    # =========== READ METHODS (for API consumption) ===========

    def get_market(self, condition_id: str) -> Optional[Dict[str, Any]]:
        """Get market data by condition_id"""
        key = f"{KEY_MARKET}:{condition_id}"
        data = self.redis.hgetall(key)
        if not data:
            return None

        # Convert numeric fields back
        float_fields = [
            "yes_price", "no_price", "last_trade_price", "best_bid",
            "best_ask", "spread", "volume", "volume_24h", "volume_clob",
            "liquidity", "liquidity_clob", "yes_liquidity_usd", "no_liquidity_usd"
        ]
        int_fields = ["yes_american", "no_american"]

        for field in float_fields:
            if field in data:
                try:
                    data[field] = float(data[field]) # type: ignore
                except (ValueError, TypeError):
                    data[field] = 0.0 # type: ignore

        for field in int_fields:
            if field in data:
                try:
                    data[field] = int(float(data[field])) # type: ignore
                except (ValueError, TypeError):
                    data[field] = 0 # type: ignore

        return data

    def get_orderbook(self, condition_id: str, side: str) -> Optional[Dict[str, Any]]:
        """Get orderbook for a market"""
        key = f"{KEY_ORDERBOOK}:{condition_id}:{side}"
        data = self.redis.get(key)
        return json.loads(data) if data else None

    def get_all_markets(self) -> List[str]:
        """Get list of all cached market condition_ids"""
        return list(self.redis.smembers(KEY_MARKETS_SET))

    def get_markets_by_tag(self, tag_id: int) -> List[str]:
        """Get market condition_ids for a specific tag"""
        return list(self.redis.smembers(f"{KEY_TAG}:{tag_id}:markets"))

    def get_liquidity_summary(self) -> Optional[Dict[str, Any]]:
        """Get overall liquidity summary"""
        data = self.redis.get(KEY_LIQUIDITY_SUMMARY)
        return json.loads(data) if data else None

    def get_last_updated(self) -> Optional[str]:
        """Get timestamp of last data refresh"""
        return self.redis.get(KEY_UPDATED)

def fetch_and_cache(
    client: PolymarketClient,
    cache: RedisCache,
    tag_id: Optional[int] = None,
    limit: int = 500,
    fetch_orderbooks: bool = True,
    sports_only: bool = False 
) -> Dict[str, Any]:
    """Fetch all markets and store in Redis"""
    
    total_markets = 0
    total_yes_liquidity = 0.0
    total_no_liquidity = 0.0
    total_volume = 0.0

    offset = 0
    page_size = 100

    logger.info(f"Fetching markets (tag_id={tag_id}, limit={limit})...")

    sports_types = ["moneyline", "spreads", "totals"] if sports_only else None

    while offset < limit:
        try:
            batch_limit = min(page_size, limit - offset)
            markets = client.get_markets(
                limit=batch_limit,
                offset=offset,
                closed=False,
                tag_id=tag_id,
                order="liquidityNum",
                ascending=False,
                sports_market_types=sports_types
            )
        except requests.HTTPError as e:
            logger.warning(f"Error fetching markets at offset {offset}: {e}")
            break

        if not markets:
            logger.info(f"No markets at offset {offset}")
            break
        
        for m in markets:
            condition_id = m.get("conditionId")
            if not condition_id:
                continue

            # Parse token IDs
            clob_token_ids = []
            try:
                if m.get("clobTokenIds"):
                    clob_token_ids = json.loads(m["clobTokenIds"]) if isinstance(
                        m["clobTokenIds"], str) else m["clobTokenIds"]
            except (json.JSONDecodeError, TypeError):
                pass

            yes_token_id = clob_token_ids[0] if len(clob_token_ids) > 0 else None
            no_token_id = clob_token_ids[1] if len(clob_token_ids) > 1 else None

            # Fetch orderbooks if enbaled and token IDs exist
            yes_liquidity = 0.0
            no_liquidity = 0.0

            if fetch_orderbooks:
                if yes_token_id:
                    try:
                        yes_ob = client.get_orderbook(yes_token_id)
                        stats = cache.store_orderbook(condition_id, "yes", yes_ob)
                        yes_liquidity = stats["bid_liquidity"] + stats["ask_liquidity"]
                    except Exception as e:
                        logger.debug(f"Could not get YES orderbook for {condition_id}: {e}")

                if no_token_id:
                    try:
                        no_ob = client.get_orderbook(no_token_id)
                        stats = cache.store_orderbook(condition_id, "no", no_ob)
                        no_liquidity = stats["bid_liquidity"] + stats["ask_liquidity"]
                    except Exception as e:
                        logger.debug(f"Could not get NO orderbook for {condition_id}: {e}")

            # Add liquidity to market data
            m["yes_liquidity_usd"] = yes_liquidity
            m["no_liquidity_usd"] = no_liquidity

            # Store market
            cache.store_market(m)

            # Track totals
            total_yes_liquidity += yes_liquidity
            total_no_liquidity += no_liquidity
            total_volume += float(m.get("volumeNum") or m.get("volume") or 0)
            total_markets += 1

            logger.debug(
                f"  {m.get('slug', condition_id)[:40]} "
                f"YES=${yes_liquidity:.0f} NO=${no_liquidity:.0f}"
            )

        logger.info(f"  -> Processed {len(markets)} markets at offset {offset}")
        offset += len(markets)

        if len(markets) < batch_limit:
            break

    # Store summary
    summary = {
        "total_markets": total_markets,
        "total_yes_liquidity_usd": round(total_yes_liquidity, 2),
        "total_no_liquidity_usd": round(total_no_liquidity, 2),
        "total_liquidity_usd": round(total_yes_liquidity + total_no_liquidity, 2),
        "total_volume_usd": round(total_volume, 2),
        "tag_id": tag_id,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    cache.store_liquidity_summary(summary)
    cache.update_timestamp()

    return summary

def main():
    parser = argparse.ArgumentParser(description="Polymarket -> Redis Cache")
    parser.add_argument("--redis-host", default="localhost", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis database")
    parser.add_argument("--redis-password", default=None, help="Redis password")
    parser.add_argument("--ttl", type=int, default=300, help="Cache TTL in seconds")
    parser.add_argument("--tag-id", type=int, default=None,
                        help="Filter by tag ID")
    parser.add_argument("--sports-only", action="store_true",
                        help="Only fetch sports markets (moneyline, spread, total)")
    parser.add_argument("--limit", type=int, default=500, help="Max markets to fetch")
    parser.add_argument("--skip-orderbooks", action="store_true",
                        help="Skip fetching orderbooks (faster)")
    parser.add_argument("--interval", type=int, default=None,
                        help="Refresh interval in seconds (runs once if not set)")
    parser.add_argument("--rate-limit", type=float, default=0,
                        help="Seconds between API requests")
    args = parser.parse_args()

    # Initialize clients
    client = PolymarketClient(rate_limit=args.rate_limit)
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

    # Run once or continuously
    while True:
        logger.info("=" * 50)
        logger.info("Starting Polymarket data fetch...")

        try:
            summary = fetch_and_cache(
                client,
                cache,
                tag_id=args.tag_id,
                limit=args.limit,
                fetch_orderbooks=not args.skip_orderbooks,
                sports_only=args.sports_only
            )
            logger.info(f"Done! {summary['total_markets']} markets cached")
            logger.info(f"Total liquidity: ${summary['total_liquidity_usd']:,.2f}")
            logger.info(f"Total volume: ${summary['total_volume_usd']:,.2f}")
        except Exception as e:
            logger.error(f"Error during fetch: {e}")
            import traceback
            traceback.print_exc()

        if args.interval:
            logger.info(f"Sleeping {args.interval}s until next refresh...")
            sleep(args.interval)
        else:
            break


if __name__ == "__main__":
    main()
