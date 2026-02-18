from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any

import yt_dlp
from yt_dlp.utils import DownloadError

from url_extractors import Platform

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def _extract_file_path(info: dict[str, Any], ydl: yt_dlp.YoutubeDL) -> Path:
    requested_downloads = info.get("requested_downloads")
    if isinstance(requested_downloads, list) and requested_downloads:
        first = requested_downloads[0]
        if isinstance(first, dict):
            filepath = first.get("filepath")
            if filepath:
                return Path(str(filepath)).resolve()

    filename = info.get("_filename")
    if filename:
        return Path(str(filename)).resolve()

    prepared = ydl.prepare_filename(info)
    if prepared:
        return Path(str(prepared)).resolve()

    raise RuntimeError("Could not determine downloaded file path")


def download_url(
    *,
    input_url: str,
    platform: Platform,
    downloads_dir: Path,
    debug: bool,
) -> dict[str, Any]:
    downloads_dir.mkdir(parents=True, exist_ok=True)

    base_ydl_opts = {
        "outtmpl": str(downloads_dir / "%(extractor)s_%(id)s.%(ext)s"),
        "format": "worst" if debug else "bestvideo*+bestaudio/best",
        "merge_output_format": "mp4",
        "noplaylist": True,
        "quiet": not debug,
        "no_warnings": not debug,
        "ignoreerrors": False,
        "socket_timeout": 30,
    }

    twitter_api_attempts: list[str | None] = [None]
    if platform == Platform.X:
        twitter_api_attempts.extend(["legacy", "syndication"])

    info: dict[str, Any] | None = None
    file_path: Path | None = None
    last_error: Exception | None = None
    should_retry_twitter_api = False

    def _is_twitter_api_dependency_error(exc: DownloadError) -> bool:
        raw = str(exc)
        cleaned = ANSI_ESCAPE_RE.sub("", raw).lower()
        return "while querying api" in cleaned and "dependency: unspecified" in cleaned

    for api_mode in twitter_api_attempts:
        ydl_opts = dict(base_ydl_opts)
        if api_mode:
            ydl_opts["extractor_args"] = {"twitter": {"api": [api_mode]}}

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                extracted = ydl.extract_info(input_url, download=True)
                if not isinstance(extracted, dict):
                    raise RuntimeError("Downloader did not return media metadata")
                info = extracted
                file_path = _extract_file_path(info, ydl)
            break
        except DownloadError as exc:
            last_error = exc

            if platform != Platform.X:
                raise
            if api_mode is None:
                should_retry_twitter_api = _is_twitter_api_dependency_error(exc)
                if not should_retry_twitter_api:
                    raise
                continue
            if should_retry_twitter_api:
                continue
            raise

    if info is None or file_path is None:
        if last_error is not None:
            raise last_error
        raise RuntimeError("Downloader failed without details")

    if not file_path.exists() or not file_path.is_file():
        raise RuntimeError(f"Downloaded file not found: {file_path}")

    return {
        "file_path": str(file_path),
        "file_size_bytes": int(file_path.stat().st_size),
        "duration_sec": float(info.get("duration") or 0.0),
        "platform": platform.value,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
