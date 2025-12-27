#!/usr/bin/env python3
"""
Match "mention" markets between Kalshi and Polymarket using term extraction.

This script finds exact matches by extracting the key term from each market's
title/question and comparing them directly. Works for markets like:
- "Will Powell say X at his press conference?"
- "Will Powell say 'X' N+ times?"

Usage:
    python match_mention_markets.py
    python match_mention_markets.py --kalshi-series KXFEDMENTION
    python match_mention_markets.py --poly-slug "what-will-powell-say-..."
"""

import argparse
import requests
import re
import json
from dataclasses import dataclass
from typing import Optional

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
POLY_GAMMA_API = "https://gamma-api.polymarket.com"


@dataclass
class MatchedMarket:
    term: str
    kalshi_ticker: str
    kalshi_title: str
    kalshi_close: str
    poly_slug: str
    poly_question: str
    poly_end: str


def extract_kalshi_term(title: str) -> Optional[str]:
    """Extract the key term from Kalshi mention market title."""
    # Pattern: "Will Powell say X at his..."
    match = re.search(r"say\s+(.+?)\s+at\s+his", title, re.I)
    if match:
        return match.group(1).strip().lower()
    return None


def extract_poly_term(question: str) -> Optional[str]:
    """Extract the key term from Polymarket mention market question."""
    # Pattern: Will Powell say "X" N+ times
    match = re.search(r'say\s+"(.+?)"', question, re.I)
    if match:
        return match.group(1).strip().lower()
    return None


def fetch_kalshi_mention_markets(series_ticker: str = "KXFEDMENTION") -> list[dict]:
    """Fetch Kalshi mention markets by series ticker."""
    try:
        resp = requests.get(
            f"{KALSHI_API}/markets",
            params={"series_ticker": series_ticker, "limit": 100, "status": "open"},
            timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("markets", [])
    except Exception as e:
        print(f"   Error fetching Kalshi: {e}")
        return []


def fetch_poly_mention_event(event_slug: str) -> Optional[dict]:
    """Fetch Polymarket event by slug."""
    try:
        resp = requests.get(
            f"{POLY_GAMMA_API}/events",
            params={"slug": event_slug},
            timeout=15
        )
        resp.raise_for_status()
        events = resp.json()
        return events[0] if events else None
    except Exception as e:
        print(f"   Error fetching Polymarket: {e}")
        return None


def find_poly_mention_events() -> list[dict]:
    """Search for Polymarket events that look like mention markets."""
    events = []
    keywords = ["powell", "trump say", "fed say", "will say"]
    
    for kw in keywords:
        try:
            # Note: Gamma API doesn't have good text search, so we fetch all and filter
            resp = requests.get(
                f"{POLY_GAMMA_API}/events",
                params={"active": "true", "closed": "false", "limit": 100},
                timeout=15
            )
            resp.raise_for_status()
            for e in resp.json():
                title = e.get("title", "").lower()
                if "say" in title and e not in events:
                    events.append(e)
        except:
            pass
    
    return events


def match_markets(
    kalshi_markets: list[dict],
    poly_event: dict
) -> tuple[list[MatchedMarket], dict, dict]:
    """Match markets by extracted terms."""
    
    # Extract terms from Kalshi
    k_terms = {}
    for m in kalshi_markets:
        title = m.get("title", "")
        term = extract_kalshi_term(title)
        if term:
            k_terms[term] = m
    
    # Extract terms from Polymarket
    p_terms = {}
    for m in poly_event.get("markets", []):
        question = m.get("question", "")
        term = extract_poly_term(question)
        if term:
            p_terms[term] = m
    
    # Find exact matches
    matches = []
    common_terms = set(k_terms.keys()) & set(p_terms.keys())
    
    for term in common_terms:
        k = k_terms[term]
        p = p_terms[term]
        matches.append(MatchedMarket(
            term=term,
            kalshi_ticker=k.get("ticker", ""),
            kalshi_title=k.get("title", ""),
            kalshi_close=k.get("close_time", ""),
            poly_slug=p.get("slug", ""),
            poly_question=p.get("question", ""),
            poly_end=poly_event.get("endDate", ""),
        ))
    
    return matches, k_terms, p_terms


def main():
    parser = argparse.ArgumentParser(description="Match mention markets by term extraction")
    parser.add_argument("--kalshi-series", default="KXFEDMENTION", help="Kalshi series ticker")
    parser.add_argument("--poly-slug", default="what-will-powell-say-during-january-press-conference-639",
                        help="Polymarket event slug")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--discover", action="store_true", help="Try to discover Poly events automatically")
    
    args = parser.parse_args()
    
    print(f"\n🔍 Matching mention markets...")
    print(f"   Kalshi series: {args.kalshi_series}")
    print(f"   Poly slug: {args.poly_slug[:50]}...")
    
    # Fetch Kalshi markets
    print("\n📡 Fetching Kalshi mention markets...")
    kalshi_markets = fetch_kalshi_mention_markets(args.kalshi_series)
    print(f"   Found {len(kalshi_markets)} markets")
    
    # Fetch Polymarket event
    print("\n📡 Fetching Polymarket event...")
    if args.discover:
        poly_events = find_poly_mention_events()
        print(f"   Discovered {len(poly_events)} potential mention events")
        for e in poly_events[:3]:
            print(f"     - {e.get('slug')}: {e.get('title')[:40]}...")
        poly_event = poly_events[0] if poly_events else None
    else:
        poly_event = fetch_poly_mention_event(args.poly_slug)
    
    if not poly_event:
        print("   ❌ No Polymarket event found")
        return
    
    print(f"   Title: {poly_event.get('title')}")
    print(f"   Markets: {len(poly_event.get('markets', []))}")
    
    # Match markets
    print("\n🔄 Extracting terms and matching...")
    matches, k_terms, p_terms = match_markets(kalshi_markets, poly_event)
    
    if args.json:
        output = [
            {
                "term": m.term,
                "kalshi_ticker": m.kalshi_ticker,
                "kalshi_title": m.kalshi_title,
                "kalshi_close": m.kalshi_close,
                "poly_slug": m.poly_slug,
                "poly_question": m.poly_question,
                "poly_end": m.poly_end,
            }
            for m in matches
        ]
        print(json.dumps(output, indent=2))
        return
    
    # Print results
    print(f"\n{'='*80}")
    print(f"📊 TERM EXTRACTION RESULTS")
    print(f"{'='*80}")
    
    print(f"\nKalshi terms ({len(k_terms)}): {sorted(k_terms.keys())[:10]}")
    if len(k_terms) > 10:
        print(f"   ... and {len(k_terms) - 10} more")
    
    print(f"\nPolymarket terms ({len(p_terms)}): {sorted(p_terms.keys())[:10]}")
    if len(p_terms) > 10:
        print(f"   ... and {len(p_terms) - 10} more")
    
    print(f"\n{'='*80}")
    print(f"✅ EXACT MATCHES ({len(matches)})")
    print(f"{'='*80}")
    
    for m in matches:
        print(f'\n📌 "{m.term}"')
        print(f"   Kalshi: {m.kalshi_ticker}")
        print(f"     {m.kalshi_title[:60]}...")
        print(f"   Poly: {m.poly_slug[:40]}...")
        print(f"     {m.poly_question[:60]}...")
    
    # Show unmatched
    unmatched_k = set(k_terms.keys()) - set(m.term for m in matches)
    unmatched_p = set(p_terms.keys()) - set(m.term for m in matches)
    
    if unmatched_k:
        print(f"\n⚠️  Kalshi-only terms ({len(unmatched_k)}): {sorted(unmatched_k)[:8]}")
    if unmatched_p:
        print(f"⚠️  Poly-only terms ({len(unmatched_p)}): {sorted(unmatched_p)[:8]}")


if __name__ == "__main__":
    main()
