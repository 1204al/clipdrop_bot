from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time

from env import load_env_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run service + worker + telegram bot")
    parser.add_argument("--host", default=os.getenv("SERVICE_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("SERVICE_PORT", "8000")))
    parser.add_argument("--callback-host", default=os.getenv("TELEGRAM_CALLBACK_HOST", "127.0.0.1"))
    parser.add_argument("--callback-port", type=int, default=int(os.getenv("TELEGRAM_CALLBACK_PORT", "8090")))
    parser.add_argument(
        "--service-start-delay",
        type=float,
        default=1.2,
        help="Seconds to wait after service start before worker/bot",
    )
    return parser.parse_args()


def _terminate_processes(processes: list[subprocess.Popen[bytes]]) -> None:
    for proc in processes:
        if proc.poll() is None:
            proc.terminate()

    deadline = time.time() + 8.0
    for proc in processes:
        if proc.poll() is not None:
            continue
        timeout = max(0.1, deadline - time.time())
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()


def main() -> None:
    load_env_file(Path(".env"))
    args = parse_args()

    if not os.getenv("TELEGRAM_BOT_TOKEN"):
        raise RuntimeError("Set TELEGRAM_BOT_TOKEN before running stack")

    py = sys.executable
    service_url = f"http://127.0.0.1:{args.port}"
    callback_url = f"http://{args.callback_host}:{args.callback_port}/internal/job-events"

    base_env = os.environ.copy()
    base_env["BOT_SERVICE_URL"] = service_url
    base_env["WORKER_BOT_CALLBACK_URL"] = callback_url
    base_env["TELEGRAM_CALLBACK_HOST"] = args.callback_host
    base_env["TELEGRAM_CALLBACK_PORT"] = str(args.callback_port)

    service_cmd = [py, "-m", "cli.service", "--host", args.host, "--port", str(args.port)]
    bot_cmd = [
        py,
        "-m",
        "cli.telegram_bot",
        "--service-url",
        service_url,
        "--callback-host",
        args.callback_host,
        "--callback-port",
        str(args.callback_port),
    ]
    worker_cmd = [py, "-m", "cli.worker"]

    processes: list[subprocess.Popen[bytes]] = []
    try:
        service_proc = subprocess.Popen(service_cmd, env=base_env)
        processes.append(service_proc)
        time.sleep(max(0.0, args.service_start_delay))

        bot_proc = subprocess.Popen(bot_cmd, env=base_env)
        processes.append(bot_proc)

        worker_proc = subprocess.Popen(worker_cmd, env=base_env)
        processes.append(worker_proc)

        print(
            "Stack started. Press Ctrl+C to stop all. "
            f"service={service_url} callback={callback_url} pids={[p.pid for p in processes]}"
        )

        while True:
            for proc in processes:
                code = proc.poll()
                if code is not None:
                    if code == 0:
                        print(f"Child process exited pid={proc.pid} code=0. Stopping stack...")
                        return
                    raise RuntimeError(f"Child process exited early pid={proc.pid} code={code}")
            time.sleep(0.8)

    except KeyboardInterrupt:
        print("Stopping stack...")
    finally:
        _terminate_processes(processes)


if __name__ == "__main__":
    main()
