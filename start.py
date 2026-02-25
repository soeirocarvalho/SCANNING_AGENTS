import subprocess
import sys
import signal
import os
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("orion_start")

from scheduler import start_scheduler

scheduler = start_scheduler()

MAX_RESTARTS = 10
RESTART_DELAY = 5
restart_count = 0
running = True


def handle_signal(signum, frame):
    global running
    running = False
    scheduler.shutdown(wait=False)
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def launch_streamlit():
    return subprocess.Popen(
        [
            sys.executable, "-m", "streamlit", "run", "dashboard.py",
            "--server.port", "5000",
            "--server.address", "0.0.0.0",
            "--server.headless", "true",
        ],
        env={**os.environ},
    )


while running:
    proc = launch_streamlit()
    exit_code = proc.wait()

    if not running:
        break

    restart_count += 1
    if restart_count > MAX_RESTARTS:
        logger.error(f"Dashboard crashed {MAX_RESTARTS} times, stopping restarts")
        break

    logger.warning(f"Dashboard exited (code {exit_code}), restarting in {RESTART_DELAY}s (attempt {restart_count}/{MAX_RESTARTS})")
    time.sleep(RESTART_DELAY)

scheduler.shutdown(wait=False)
