import hashlib
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, urljoin

import requests
import feedparser
from bs4 import BeautifulSoup
from readability import Document

from .config import RATE_LIMIT_SECONDS, REQUEST_TIMEOUT_SECONDS, MAX_FEED_DISCOVERY, MIN_DOC_TEXT_LENGTH


def _text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    return " ".join(soup.get_text(" ").split())


def _extract_readable_text(html: str) -> str:
    try:
        doc = Document(html)
        content_html = doc.summary(html_partial=True)
        return _text_from_html(content_html)
    except Exception:
        return _text_from_html(html)


def _discover_feeds(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links: List[str] = []
    for link in soup.find_all("link"):
        rel = " ".join(link.get("rel", [])).lower()
        ltype = (link.get("type") or "").lower()
        href = link.get("href")
        if not href:
            continue
        if "alternate" in rel and ("rss" in ltype or "atom" in ltype or "xml" in ltype):
            links.append(urljoin(base_url, href))
    seen: set = set()
    out: List[str] = []
    for lnk in links:
        if lnk not in seen:
            seen.add(lnk)
            out.append(lnk)
    return out


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class CollectorResult:
    docs: List[Dict[str, Any]]
    failed: int
    stats: List[Dict[str, Any]]


class Collector:
    def __init__(self, rate_limit_seconds: float = RATE_LIMIT_SECONDS, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS, log_fn=None):
        self.rate_limit_seconds = rate_limit_seconds
        self.timeout_seconds = timeout_seconds
        self._last_request_by_domain: Dict[str, float] = {}
        self._log_fn = log_fn or (lambda msg: print(msg))

    def _rate_limit(self, url: str):
        domain = urlparse(url).netloc
        if not domain:
            return
        now = time.time()
        last = self._last_request_by_domain.get(domain)
        if last is not None:
            elapsed = now - last
            if elapsed < self.rate_limit_seconds:
                time.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_by_domain[domain] = time.time()

    def _fetch_url(self, url: str) -> Optional[str]:
        try:
            self._rate_limit(url)
            resp = requests.get(url, timeout=self.timeout_seconds, headers={"User-Agent": "ORION-External-Agent/1.0"})
            resp.raise_for_status()
            return resp.text
        except Exception:
            return None

    def _fetch_and_parse_feed(self, url: str) -> feedparser.FeedParserDict:
        content = self._fetch_url(url)
        if content:
            return feedparser.parse(content)
        return feedparser.parse("")

    def _doc_from_text(self, source: Dict[str, Any], url: str, text: str, published_at: Optional[str]) -> Dict[str, Any]:
        retrieved_at = datetime.now(timezone.utc).isoformat()
        content_hash = _hash_text(text)
        return {
            "doc_id": str(uuid.uuid4()),
            "source_name": source.get("source_name", ""),
            "source_url": source.get("source_link", ""),
            "canonical_url": url,
            "published_at": published_at,
            "retrieved_at": retrieved_at,
            "clean_text": text[:8000],
            "content_hash": content_hash,
        }

    def fetch_docs(self, sources: List[Dict[str, Any]], max_docs_per_source: int = 1) -> CollectorResult:
        docs: List[Dict[str, Any]] = []
        failed = 0
        stats: List[Dict[str, Any]] = []
        total_sources = len(sources)
        for idx, source in enumerate(sources, 1):
            url = source.get("source_link")
            if not url:
                continue

            source_name = source.get("source_name", url)
            self._log_fn(f"[{idx}/{total_sources}] Processing: {source_name}")

            source_stats: Dict[str, Any] = {
                "source_name": source.get("source_name", ""),
                "source_link": url,
                "feed_url": None,
                "entries_found": 0,
                "docs_created": 0,
                "avg_text_length": 0,
                "errors": [],
            }
            text_length_sum = 0

            feed = self._fetch_and_parse_feed(url)
            feed_url = url if feed.entries else None
            homepage_html = None

            if not feed.entries:
                homepage_html = self._fetch_url(url)
                if homepage_html:
                    feed_urls = _discover_feeds(homepage_html, url)[:MAX_FEED_DISCOVERY]
                    for candidate in feed_urls:
                        test_feed = self._fetch_and_parse_feed(candidate)
                        if test_feed.entries:
                            feed = test_feed
                            feed_url = candidate
                            break

            if feed_url and feed.entries:
                source_stats["feed_url"] = feed_url
                source_stats["entries_found"] = len(feed.entries)
                for entry in feed.entries[:max_docs_per_source]:
                    entry_url = getattr(entry, "link", None) or feed_url
                    published_at = getattr(entry, "published", None) or getattr(entry, "updated", None)
                    text = ""
                    fetched = self._fetch_url(entry_url)
                    if fetched:
                        text = _extract_readable_text(fetched)
                    if not text:
                        summary = getattr(entry, "summary", "") or ""
                        content = ""
                        if getattr(entry, "content", None):
                            content = " ".join(c.get("value", "") for c in entry.content if isinstance(c, dict))
                        text = " ".join([summary, content]).strip()
                    if not text or len(text) < MIN_DOC_TEXT_LENGTH:
                        failed += 1
                        source_stats["errors"].append("empty_or_short_entry")
                        continue
                    text_length_sum += len(text)
                    docs.append(self._doc_from_text(source, entry_url, text, published_at))
                    source_stats["docs_created"] += 1
                if source_stats["docs_created"] > 0:
                    source_stats["avg_text_length"] = int(text_length_sum / source_stats["docs_created"])
                stats.append(source_stats)
                continue

            if not homepage_html:
                homepage_html = self._fetch_url(url)
            if not homepage_html:
                failed += 1
                source_stats["errors"].append("homepage_fetch_failed")
                stats.append(source_stats)
                continue
            text = _extract_readable_text(homepage_html)
            if not text or len(text) < MIN_DOC_TEXT_LENGTH:
                failed += 1
                source_stats["errors"].append("homepage_empty_or_short")
                stats.append(source_stats)
                continue
            text_length_sum += len(text)
            docs.append(self._doc_from_text(source, url, text, None))
            source_stats["docs_created"] += 1
            if source_stats["docs_created"] > 0:
                source_stats["avg_text_length"] = int(text_length_sum / source_stats["docs_created"])
            stats.append(source_stats)

        return CollectorResult(docs=docs, failed=failed, stats=stats)
