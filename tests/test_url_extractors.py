from __future__ import annotations

from url_extractors import Platform, extract_supported_urls


def test_extract_supported_urls_supported_platforms() -> None:
    text = "\n".join(
        [
            "https://www.tiktok.com/@name/video/12345?utm_source=abc",
            "https://instagram.com/reel/AbcDef/?igshid=zzz",
            "https://x.com/user/status/1234567890?si=abc",
        ]
    )

    rows = extract_supported_urls(text)

    assert [row.platform for row in rows] == [
        Platform.TIKTOK,
        Platform.INSTAGRAM,
        Platform.X,
    ]


def test_extract_supported_urls_ignores_all_youtube_urls() -> None:
    text = (
        "check https://www.youtube.com/watch?v=abc "
        "https://youtu.be/abcd "
        "https://www.youtube.com/shorts/abcdEFG1234?feature=share"
    )
    rows = extract_supported_urls(text)
    assert rows == []


def test_extract_supported_urls_real_world_examples() -> None:
    text = "\n".join(
        [
            "https://vt.tiktok.com/ZSmDoVEBm",
            "https://www.instagram.com/p/DUIvX5LEUZp/",
            "https://x.com/FrontendMasters/status/2023797282978607430",
        ]
    )

    rows = extract_supported_urls(text)

    assert len(rows) == 3
    assert [row.platform for row in rows] == [Platform.TIKTOK, Platform.INSTAGRAM, Platform.X]


def test_extract_supported_urls_deduplicates_by_normalized_url() -> None:
    text = (
        "https://x.com/user/status/123?utm_source=foo "
        "https://twitter.com/user/status/123?si=bar"
    )

    rows = extract_supported_urls(text)

    assert len(rows) == 2
    assert rows[0].normalized_url == "https://x.com/user/status/123"
    assert rows[1].normalized_url == "https://twitter.com/user/status/123"


def test_extract_supported_urls_strips_trailing_punctuation() -> None:
    text = "Try this (https://instagram.com/p/ABC123/?utm_campaign=x)."

    rows = extract_supported_urls(text)

    assert len(rows) == 1
    assert rows[0].normalized_url == "https://instagram.com/p/ABC123"
