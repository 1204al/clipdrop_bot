from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
TWITTER_STATUS_RE = re.compile(r"^/[^/]+/status/\d+", re.IGNORECASE)

TRACKING_QUERY_KEYS = {"si", "feature", "igshid"}


class Platform(StrEnum):
    TIKTOK = "tiktok"
    INSTAGRAM = "instagram"
    X = "x"


@dataclass(frozen=True)
class ExtractedUrl:
    input_url: str
    normalized_url: str
    platform: Platform


def _clean_candidate(url: str) -> str:
    return url.strip().rstrip(").,;!?\"'")


def _normalize_host(netloc: str) -> str:
    host = netloc.strip().lower()
    if "@" in host:
        host = host.split("@", 1)[1]
    if ":" in host:
        host = host.split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def _strip_tracking_query(query: str) -> str:
    kept: list[tuple[str, str]] = []
    for key, value in parse_qsl(query, keep_blank_values=True):
        lower_key = key.lower()
        if lower_key.startswith("utm_"):
            continue
        if lower_key in TRACKING_QUERY_KEYS:
            continue
        kept.append((key, value))
    kept.sort(key=lambda item: (item[0], item[1]))
    return urlencode(kept, doseq=True)


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    host = _normalize_host(parsed.netloc)
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"
    query = _strip_tracking_query(parsed.query)
    return urlunparse(("https", host, path, "", query, ""))


def _is_tiktok(host: str) -> bool:
    return host.endswith("tiktok.com")


def _is_instagram(host: str, path: str) -> bool:
    if not host.endswith("instagram.com"):
        return False
    lowered = path.lower()
    return "/reel/" in lowered or "/p/" in lowered or "/tv/" in lowered


def _is_x_status(host: str, path: str) -> bool:
    if host not in {"x.com", "twitter.com", "mobile.twitter.com"}:
        return False
    return bool(TWITTER_STATUS_RE.match(path))


def classify_url(url: str) -> ExtractedUrl | None:
    cleaned = _clean_candidate(url)
    try:
        parsed = urlparse(cleaned)
    except ValueError:
        return None

    if parsed.scheme.lower() not in {"http", "https"}:
        return None

    host = _normalize_host(parsed.netloc)
    path = parsed.path or "/"

    platform: Platform | None = None
    if _is_tiktok(host):
        platform = Platform.TIKTOK
    elif _is_instagram(host, path):
        platform = Platform.INSTAGRAM
    elif _is_x_status(host, path):
        platform = Platform.X

    if platform is None:
        return None

    return ExtractedUrl(
        input_url=cleaned,
        normalized_url=normalize_url(cleaned),
        platform=platform,
    )


def extract_supported_urls(text: str) -> list[ExtractedUrl]:
    if not text:
        return []

    items: list[ExtractedUrl] = []
    seen: set[str] = set()

    for match in URL_RE.findall(text):
        classified = classify_url(match)
        if classified is None:
            continue
        if classified.normalized_url in seen:
            continue
        seen.add(classified.normalized_url)
        items.append(classified)

    return items
