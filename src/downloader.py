from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yt_dlp

from url_extractors import Platform


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

    ydl_opts = {
        "outtmpl": str(downloads_dir / "%(extractor)s_%(id)s.%(ext)s"),
        "format": "worst" if debug else "bestvideo*+bestaudio/best",
        "merge_output_format": "mp4",
        "noplaylist": True,
        "quiet": not debug,
        "no_warnings": not debug,
        "ignoreerrors": False,
        "socket_timeout": 30,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(input_url, download=True)
        if not isinstance(info, dict):
            raise RuntimeError("Downloader did not return media metadata")
        file_path = _extract_file_path(info, ydl)

    if not file_path.exists() or not file_path.is_file():
        raise RuntimeError(f"Downloaded file not found: {file_path}")

    return {
        "file_path": str(file_path),
        "file_size_bytes": int(file_path.stat().st_size),
        "duration_sec": float(info.get("duration") or 0.0),
        "platform": platform.value,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
