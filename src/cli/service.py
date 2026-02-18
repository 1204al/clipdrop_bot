from __future__ import annotations

import argparse

import uvicorn

from config import load_config


def parse_args(default_host: str, default_port: int) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run FastAPI queue service")
    parser.add_argument("--host", default=default_host)
    parser.add_argument("--port", type=int, default=default_port)
    return parser.parse_args()


def main() -> None:
    config = load_config()
    args = parse_args(default_host=config.service_host, default_port=config.service_port)

    uvicorn.run(
        "service_api:app",
        host=args.host,
        port=args.port,
        log_level="debug" if config.debug else "info",
    )


if __name__ == "__main__":
    main()
