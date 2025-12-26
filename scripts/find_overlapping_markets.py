#!/usr/bin/env python3
"""
Find overlapping markets on Kalshi and Polymarket based on closing time.

This script:
1. Filters by close time (first pass)
2. Uses fuzzy matching on titles to find semantically similar markets (second pass)

Usage:
    python find_overlapping_markets.py [--days DAYS] [--window HOURS] [--min-score SCORE]
"""

import argparse
import requests
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional
import json
import time
import re
from difflib import SequenceMatcher


# === API Endpoints ===
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
POLY_GAMMA_API = "https://gamma-api.polymarket.com"


@dataclass
class KalshiMarket:
    ticker: str
    title: str
    event_ticker: str
    close_time: datetime
    yes_ask: Optional[int] = None
    no_ask: Optional[int] = None
    series: str = ""


@dataclass  
class PolyMarket:
    slug: str
    question: str
    end_date: Optional[datetime]
    clob_token_ids: tuple = field(default_factory=tuple)
    event_slug: str = ""
    event_title: str = ""


@dataclass
class OverlapMatch:
    kalshi: KalshiMarket
    poly: PolyMarket
    time_diff_hours: float
    fuzzy_score: float  # 0-100 similarity score
    
    def __str__(self) -> str:
        k_close = self.kalshi.close_time.strftime('%Y-%m-%d %H:%M')
        p_end = self.poly.end_date.strftime('%Y-%m-%d %H:%M') if self.poly.end_date else 'N/A'
        
        return (
            f"\n{'─'*80}\n"
            f"✓ MATCH (fuzzy: {self.fuzzy_score:.0f}%, time Δ: {abs(self.time_diff_hours):.1f}h)\n"
            f"{'─'*80}\n"
            f"KALSHI: {self.kalshi.ticker}\n"
            f"  {self.kalshi.title[:75]}\n"
            f"  Close: {k_close} UTC\n"
            f"POLYMARKET: {self.poly.slug}\n"
            f"  {self.poly.question[:75]}\n"
            f"  End: {p_end} UTC\n"
        )


def normalize_text(text: str) -> str:
    """Normalize text for comparison."""
    text = text.lower()
    # Remove punctuation
    text = re.sub(r'[^\w\s]', ' ', text)
    # Remove common words
    stopwords = {'will', 'the', 'a', 'an', 'to', 'be', 'in', 'on', 'at', 'by', 'of', 'for', 'and', 'or', 'is', 'it'}
    words = [w for w in text.split() if w not in stopwords and len(w) > 1]
    return ' '.join(words)


def fuzzy_match_score(text1: str, text2: str) -> float:
    """Calculate fuzzy match score between two texts (0-100)."""
    norm1 = normalize_text(text1)
    norm2 = normalize_text(text2)
    
    # Use SequenceMatcher for fuzzy matching
    ratio = SequenceMatcher(None, norm1, norm2).ratio()
    
    # Also check word overlap (Jaccard similarity)
    words1 = set(norm1.split())
    words2 = set(norm2.split())
    if words1 and words2:
        jaccard = len(words1 & words2) / len(words1 | words2)
        # Combine both scores (weighted average)
        return (ratio * 0.6 + jaccard * 0.4) * 100
    
    return ratio * 100


def fetch_all_kalshi(max_days: int = 60, limit: int = 2000) -> list[KalshiMarket]:
    """Fetch all open Kalshi markets."""
    markets = []
    cursor = None
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=max_days)
    
    while len(markets) < limit:
        params = {"limit": 200, "status": "open"}
        if cursor:
            params["cursor"] = cursor
        
        try:
            resp = requests.get(f"{KALSHI_API}/markets", params=params, timeout=30)
            if resp.status_code == 429:
                print("   Rate limited, waiting...")
                time.sleep(5)
                continue
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"   Error: {e}")
            break
            
        for m in data.get("markets", []):
            try:
                close_time = datetime.fromisoformat(m["close_time"].replace("Z", "+00:00"))
                if close_time > now and close_time <= cutoff:
                    ticker = m.get("ticker", "")
                    markets.append(KalshiMarket(
                        ticker=ticker,
                        title=m.get("title", ""),
                        event_ticker=m.get("event_ticker", ""),
                        close_time=close_time,
                        yes_ask=m.get("yes_ask"),
                        no_ask=m.get("no_ask"),
                        series=ticker.split("-")[0] if "-" in ticker else "",
                    ))
            except:
                continue
        
        cursor = data.get("cursor")
        if not cursor or not data.get("markets"):
            break
        time.sleep(0.3)
            
    return markets


def fetch_all_poly(limit: int = 2000) -> list[PolyMarket]:
    """Fetch all active Polymarket events."""
    markets = []
    offset = 0
    seen_slugs = set()
    
    while len(markets) < limit:
        params = {"limit": 100, "offset": offset, "active": "true", "closed": "false"}
        
        try:
            resp = requests.get(f"{POLY_GAMMA_API}/events", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"   Error: {e}")
            break
            
        if not data:
            break
        
        for event in data:
            event_end = event.get("endDate")
            event_slug = event.get("slug", "")
            event_title = event.get("title", "")
            
            if not event_end:
                continue
                
            try:
                end_date = datetime.fromisoformat(event_end.replace("Z", "+00:00"))
            except:
                continue
            
            for m in event.get("markets", []):
                slug = m.get("slug", "") or event_slug
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                
                try:
                    token_ids = m.get("clobTokenIds")
                    if isinstance(token_ids, str):
                        token_ids = json.loads(token_ids)
                    if not token_ids or len(token_ids) < 2:
                        token_ids = ("", "")
                    else:
                        token_ids = (token_ids[0], token_ids[1])
                    
                    markets.append(PolyMarket(
                        slug=slug,
                        question=m.get("question", "") or event_title,
                        end_date=end_date,
                        clob_token_ids=token_ids,
                        event_slug=event_slug,
                        event_title=event_title,
                    ))
                except:
                    continue
                    
        offset += 100
        if len(data) < 100:
            break
        time.sleep(0.2)
            
    return markets


def find_overlaps(
    kalshi_markets: list[KalshiMarket],
    poly_markets: list[PolyMarket],
    time_window_hours: float = 48,
    min_fuzzy_score: float = 30,
) -> list[OverlapMatch]:
    """Find markets with overlapping close times and similar titles."""
    overlaps = []
    
    # Index poly markets by date for faster lookup
    poly_by_date = {}
    for p in poly_markets:
        if p.end_date:
            date_key = p.end_date.strftime("%Y-%m-%d")
            if date_key not in poly_by_date:
                poly_by_date[date_key] = []
            poly_by_date[date_key].append(p)
    
    total = len(kalshi_markets)
    checked = 0
    
    for kalshi in kalshi_markets:
        checked += 1
        if checked % 200 == 0:
            print(f"   Checking {checked}/{total}...")
        
        # Get poly markets within time window
        kalshi_date = kalshi.close_time.strftime("%Y-%m-%d")
        candidates = []
        
        # Check ±2 days to account for time window
        for delta in range(-2, 3):
            check_date = (kalshi.close_time + timedelta(days=delta)).strftime("%Y-%m-%d")
            if check_date in poly_by_date:
                candidates.extend(poly_by_date[check_date])
        
        for poly in candidates:
            if not poly.end_date:
                continue
            
            # Check time difference
            time_diff = (kalshi.close_time - poly.end_date).total_seconds() / 3600
            if abs(time_diff) > time_window_hours:
                continue
            
            # Calculate fuzzy match score
            # Compare kalshi title with poly question AND event title
            score1 = fuzzy_match_score(kalshi.title, poly.question)
            score2 = fuzzy_match_score(kalshi.title, poly.event_title) if poly.event_title else 0
            fuzzy_score = max(score1, score2)
            
            if fuzzy_score >= min_fuzzy_score:
                overlaps.append(OverlapMatch(
                    kalshi=kalshi,
                    poly=poly,
                    time_diff_hours=time_diff,
                    fuzzy_score=fuzzy_score,
                ))
    
    # Sort by fuzzy score (highest first), then by time difference
    overlaps.sort(key=lambda x: (-x.fuzzy_score, abs(x.time_diff_hours)))
    
    return overlaps


SPORTS_KEYWORDS = {'nfl', 'nba', 'nhl', 'mlb', 'epl', 'ncaa', 'football', 'basketball', 
                   'hockey', 'baseball', 'soccer', 'premier league', 'super bowl', 
                   'stanley cup', 'world series', 'playoff', 'championship game',
                   'all star', 'pro football', 'pro basketball'}

def is_sports_market(text: str) -> bool:
    """Check if market is sports-related."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in SPORTS_KEYWORDS)


def main():
    parser = argparse.ArgumentParser(description="Find overlapping markets with fuzzy matching")
    parser.add_argument("--days", type=int, default=60, help="Days ahead (default: 60)")
    parser.add_argument("--window", type=float, default=48, help="Time window in hours (default: 48)")
    parser.add_argument("--min-score", type=float, default=25, help="Min fuzzy score 0-100 (default: 25)")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    parser.add_argument("--limit", type=int, default=50, help="Max results to show (default: 50)")
    parser.add_argument("--exclude-sports", action="store_true", help="Exclude sports markets")
    
    args = parser.parse_args()
    
    print(f"\n🔍 Finding overlapping markets with fuzzy matching...")
    print(f"   Looking {args.days} days ahead, ±{args.window}h window")
    print(f"   Minimum fuzzy score: {args.min_score}%")
    if args.exclude_sports:
        print(f"   Excluding sports markets")
    print()
    
    print("📡 Fetching Kalshi markets...")
    kalshi_markets = fetch_all_kalshi(max_days=args.days)
    original_kalshi = len(kalshi_markets)
    
    if args.exclude_sports:
        kalshi_markets = [m for m in kalshi_markets if not is_sports_market(m.title)]
    print(f"   → {len(kalshi_markets)} markets" + (f" (filtered from {original_kalshi})" if args.exclude_sports else ""))
    
    print("\n📡 Fetching Polymarket events...")
    poly_markets = fetch_all_poly()
    original_poly = len(poly_markets)
    
    if args.exclude_sports:
        poly_markets = [m for m in poly_markets if not is_sports_market(m.question) and not is_sports_market(m.event_title)]
    print(f"   → {len(poly_markets)} markets" + (f" (filtered from {original_poly})" if args.exclude_sports else ""))
    
    print("\n🔄 Finding overlaps with fuzzy matching...")
    overlaps = find_overlaps(
        kalshi_markets, 
        poly_markets, 
        time_window_hours=args.window,
        min_fuzzy_score=args.min_score,
    )
    
    if args.json:
        output = [
            {
                "kalshi_ticker": o.kalshi.ticker,
                "kalshi_title": o.kalshi.title,
                "kalshi_close": o.kalshi.close_time.isoformat(),
                "poly_slug": o.poly.slug,
                "poly_question": o.poly.question,
                "poly_end": o.poly.end_date.isoformat() if o.poly.end_date else None,
                "fuzzy_score": o.fuzzy_score,
                "time_diff_hours": o.time_diff_hours,
            }
            for o in overlaps[:args.limit]
        ]
        print(json.dumps(output, indent=2))
        return
    
    print(f"\n{'='*80}")
    print(f"🎯 FOUND {len(overlaps)} OVERLAPPING MARKETS (showing top {min(len(overlaps), args.limit)})")
    print(f"{'='*80}")
    
    for i, match in enumerate(overlaps[:args.limit], 1):
        print(f"[{i}] {match}")
    
    if len(overlaps) > args.limit:
        print(f"\n... and {len(overlaps) - args.limit} more matches")
        print(f"Use --limit {len(overlaps)} to see all, or --json for full data")
    
    if not overlaps:
        print("\n   No matches found. Try:")
        print("   --min-score 20  (lower the threshold)")
        print("   --window 72     (wider time window)")


if __name__ == "__main__":
    main()
