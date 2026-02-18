from __future__ import annotations

from pathlib import Path

import pytest
from yt_dlp.utils import DownloadError

import downloader as downloader_module
from downloader import download_url
from url_extractors import Platform


def _twitter_dependency_error() -> DownloadError:
    return DownloadError(
        "\x1b[0;31mERROR:\x1b[0m [twitter] 2023916042574328069: "
        "Error(s) while querying API: Dependency: Unspecified"
    )


def test_download_url_retries_x_with_legacy_api(monkeypatch, tmp_path: Path) -> None:
    downloaded = tmp_path / "twitter_1.mp4"
    downloaded.write_bytes(b"video")
    captured_opts: list[dict] = []
    call = {"idx": 0}

    class FakeYoutubeDL:
        def __init__(self, opts: dict) -> None:
            captured_opts.append(opts)

        def __enter__(self) -> "FakeYoutubeDL":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def extract_info(self, url: str, download: bool = True) -> dict:
            call["idx"] += 1
            if call["idx"] == 1:
                raise _twitter_dependency_error()
            return {"requested_downloads": [{"filepath": str(downloaded)}], "duration": 1.0}

        def prepare_filename(self, info: dict) -> str:
            return str(downloaded)

    monkeypatch.setattr(downloader_module.yt_dlp, "YoutubeDL", FakeYoutubeDL)

    result = download_url(
        input_url="https://x.com/u/status/1",
        platform=Platform.X,
        downloads_dir=tmp_path,
        debug=False,
    )

    assert result["file_path"] == str(downloaded.resolve())
    assert len(captured_opts) == 2
    assert "extractor_args" not in captured_opts[0]
    assert captured_opts[1]["extractor_args"]["twitter"]["api"] == ["legacy"]


def test_download_url_retries_x_and_raises_after_all_api_modes(monkeypatch, tmp_path: Path) -> None:
    captured_opts: list[dict] = []

    class FakeYoutubeDL:
        def __init__(self, opts: dict) -> None:
            captured_opts.append(opts)

        def __enter__(self) -> "FakeYoutubeDL":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def extract_info(self, url: str, download: bool = True) -> dict:
            raise _twitter_dependency_error()

        def prepare_filename(self, info: dict) -> str:
            return str(tmp_path / "never-used.mp4")

    monkeypatch.setattr(downloader_module.yt_dlp, "YoutubeDL", FakeYoutubeDL)

    with pytest.raises(DownloadError):
        download_url(
            input_url="https://x.com/u/status/2",
            platform=Platform.X,
            downloads_dir=tmp_path,
            debug=False,
        )

    assert len(captured_opts) == 3
    assert captured_opts[1]["extractor_args"]["twitter"]["api"] == ["legacy"]
    assert captured_opts[2]["extractor_args"]["twitter"]["api"] == ["syndication"]


def test_download_url_does_not_retry_non_x_platform(monkeypatch, tmp_path: Path) -> None:
    captured_opts: list[dict] = []

    class FakeYoutubeDL:
        def __init__(self, opts: dict) -> None:
            captured_opts.append(opts)

        def __enter__(self) -> "FakeYoutubeDL":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def extract_info(self, url: str, download: bool = True) -> dict:
            raise _twitter_dependency_error()

        def prepare_filename(self, info: dict) -> str:
            return str(tmp_path / "never-used.mp4")

    monkeypatch.setattr(downloader_module.yt_dlp, "YoutubeDL", FakeYoutubeDL)

    with pytest.raises(DownloadError):
        download_url(
            input_url="https://www.instagram.com/p/abc/",
            platform=Platform.INSTAGRAM,
            downloads_dir=tmp_path,
            debug=False,
        )

    assert len(captured_opts) == 1
