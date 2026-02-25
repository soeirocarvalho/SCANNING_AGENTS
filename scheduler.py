import os
import logging
import threading
from datetime import date

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from src.pipeline import run_pipeline
from src.sources import load_sources
from src.rotation import get_rotated_sources, get_rotation_info

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("orion_scheduler")

SCHEDULE_HOUR = int(os.environ.get("ORION_SCHEDULE_HOUR", "6"))
SCHEDULE_MINUTE = int(os.environ.get("ORION_SCHEDULE_MINUTE", "0"))

_running_lock = threading.Lock()


def run_daily_rotation():
    if not _running_lock.acquire(blocking=False):
        logger.warning("Pipeline already running, skipping this trigger")
        return

    try:
        date_str = date.today().strftime("%Y-%m-%d")
        logger.info(f"Starting daily rotation for {date_str}")

        all_sources = load_sources()
        sources = get_rotated_sources(all_sources, date_str)
        info = get_rotation_info(all_sources, date_str)
        logger.info(
            f"Rotation Day {info['day_in_cycle']}/{info['total_days_in_cycle']} — "
            f"sources {info['current_offset']+1} to {info['current_offset']+len(sources)}"
        )

        max_runtime = int(os.environ.get("ORION_MAX_RUNTIME_SECONDS", "9000"))
        logger.info(f"Safety timeout set to {max_runtime}s ({max_runtime//3600}h {(max_runtime%3600)//60}m)")

        summary = run_pipeline(
            date_str,
            len(sources),
            max_docs_per_source=2,
            max_docs_total=None,
            max_candidates_per_doc=None,
            max_runtime_seconds=max_runtime,
            sources_override=sources,
            run_synthesis=True,
        )

        logger.info(
            f"Run complete — Docs: {summary.docs_fetched} | "
            f"Candidates: {summary.candidates} | "
            f"Accept: {summary.accept} | Review: {summary.review} | Reject: {summary.reject} | "
            f"Forces: {summary.forces_created}"
        )
    except Exception:
        logger.exception("Pipeline run failed")
    finally:
        _running_lock.release()


def _has_run_today() -> bool:
    from src.config import OUTPUT_ROOT
    date_str = date.today().strftime("%Y-%m-%d")
    day_dir = OUTPUT_ROOT / date_str
    report = day_dir / "collector_report.json"
    return report.exists()


def start_scheduler():
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        run_daily_rotation,
        trigger=CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE),
        id="orion_daily_rotation",
        name="ORION Daily Rotation",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    scheduler.start()
    logger.info(f"Scheduler started — daily rotation at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} UTC")

    if not _has_run_today():
        logger.info("No completed run found for today — triggering immediate pipeline run")
        t = threading.Thread(target=run_daily_rotation, daemon=True)
        t.start()
    else:
        logger.info("Pipeline already ran today — skipping startup run")

    return scheduler
