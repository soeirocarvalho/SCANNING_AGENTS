import os
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

from .config import OUTPUT_ROOT

ROTATION_STATE_FILE = OUTPUT_ROOT / "rotation_state.json"
BATCH_SIZE = int(os.environ.get("ORION_BATCH_SIZE", "50"))


def _load_state() -> Dict[str, Any]:
    if ROTATION_STATE_FILE.exists():
        try:
            return json.loads(ROTATION_STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"last_offset": 0, "last_date": None}


def _save_state(state: Dict[str, Any]):
    ROTATION_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    ROTATION_STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def get_rotated_sources(all_sources: List[Dict[str, Any]], date_str: str, full_sweep: bool = False) -> List[Dict[str, Any]]:
    if full_sweep:
        return all_sources
    
    state = _load_state()
    total = len(all_sources)
    
    if total == 0:
        return []
    
    if state.get("last_date") == date_str:
        offset = state.get("last_offset", 0)
    else:
        last_offset = state.get("last_offset", 0)
        offset = (last_offset + BATCH_SIZE) % total
    
    end = offset + BATCH_SIZE
    if end <= total:
        batch = all_sources[offset:end]
    else:
        batch = all_sources[offset:] + all_sources[:end - total]
    
    _save_state({"last_offset": offset, "last_date": date_str})
    
    return batch


def get_rotation_info(all_sources: List[Dict[str, Any]], date_str: str) -> Dict[str, Any]:
    state = _load_state()
    total = len(all_sources)
    
    if state.get("last_date") == date_str:
        offset = state.get("last_offset", 0)
    else:
        last_offset = state.get("last_offset", 0)
        offset = (last_offset + BATCH_SIZE) % total
    
    day_in_cycle = (offset // BATCH_SIZE) + 1
    total_days = (total + BATCH_SIZE - 1) // BATCH_SIZE
    
    return {
        "current_offset": offset,
        "batch_size": BATCH_SIZE,
        "day_in_cycle": day_in_cycle,
        "total_days_in_cycle": total_days,
        "total_sources": total,
    }
