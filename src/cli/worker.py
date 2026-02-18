from __future__ import annotations

import argparse

from config import load_config
from worker import run_worker


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start persistent queue worker")
    parser.add_argument("--once", action="store_true", help="Process one job and exit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config()
    run_worker(config, run_once=args.once)


if __name__ == "__main__":
    main()
