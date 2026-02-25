import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

from .config import OUTPUT_ROOT, ORION_COLUMNS, OPENAI_MODEL, OPENAI_TIMEOUT_SECONDS
from .export import write_csv, _load_existing_ids

FORCES_MASTER_FILE = OUTPUT_ROOT / "orion_forces_master.csv"

FORCE_TYPE_COLORS = {
    "MT": "#3B82F6",  # Blue
    "T": "#10B981",   # Green
    "WS": "#F59E0B",  # Amber
    "WC": "#EF4444",  # Red
}

FORCE_TYPE_SCOPE = {
    "MT": "megatrends",
    "T": "trends",
    "WS": "weak_signals",
    "WC": "wildcards",
}


def _normalize_tags_to_list(tags_value: Any) -> List[str]:
    if tags_value is None:
        return []
    if isinstance(tags_value, list):
        return [str(t).strip() for t in tags_value if str(t).strip()]
    if isinstance(tags_value, str):
        text = tags_value.strip()
        if not text:
            return []
        try:
            data = json.loads(text)
            if isinstance(data, list):
                return [str(t).strip() for t in data if str(t).strip()]
        except json.JSONDecodeError:
            pass
        return [t.strip() for t in text.split(",") if t.strip()]
    return []


def _build_force_row(force: Dict[str, Any], project_id: str) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    force_type = force.get("type", "WS")
    
    tags_list = _normalize_tags_to_list(force.get("tags", []))
    source_ids = force.get("source_signal_ids", [])
    if source_ids:
        tags_list = list(tags_list) + [f"synthesized_from:{','.join(source_ids)}"]
    tags_str = json.dumps(tags_list) if tags_list else ""
    
    return {
        "id": force.get("force_id", str(uuid.uuid4())),
        "project_id": project_id,
        "title": force.get("title", ""),
        "type": force_type,
        "steep": force.get("steep", ""),
        "dimension": force.get("dimension", ""),
        "scope": FORCE_TYPE_SCOPE.get(force_type, "forces"),
        "impact": 7.0,
        "ttm": "",
        "sentiment": "Neutral",
        "source": ", ".join(force.get("source_signal_ids", [])[:3]),
        "tags": tags_str,
        "text": force.get("text", ""),
        "magnitude": None,
        "distance": 5,
        "color_hex": FORCE_TYPE_COLORS.get(force_type, "#94A3B8"),
        "feasibility": None,
        "urgency": None,
        "created_at": now,
        "updated_at": now,
    }


def _prepare_signals_for_synthesis(accepted_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    signals = []
    for row in accepted_rows:
        sig_id = row.get("id", "")
        title = row.get("title", "")
        text = row.get("text", "")
        steep = row.get("steep", "")
        dimension = row.get("dimension", "")
        
        if not sig_id or not title:
            continue
        if not text:
            text = title
        if not steep:
            steep = "Technological"
        if not dimension:
            dimension = "General"
        
        tags_raw = row.get("tags", "")
        tags_list = _normalize_tags_to_list(tags_raw)
        tags_str = json.dumps(tags_list) if tags_list else ""
        
        signals.append({
            "id": sig_id,
            "title": title,
            "text": text,
            "steep": steep,
            "dimension": dimension,
            "tags": tags_str,
            "source": row.get("source", ""),
            "priority_index": row.get("priority_index", 50),
            "created_at": row.get("created_at", ""),
        })
    return signals


def _load_existing_forces() -> List[Dict[str, Any]]:
    if not FORCES_MASTER_FILE.exists():
        return []
    
    import csv
    forces = []
    with FORCES_MASTER_FILE.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            forces.append({
                "id": row.get("id", ""),
                "title": row.get("title", ""),
                "type": row.get("type", ""),
            })
    return forces


def run_synthesis(
    accepted_rows: List[Dict[str, Any]],
    project_id: str,
    output_dir: Path,
    call_openai_fn,
    log_fn,
    prompts: Dict[str, Any],
) -> Dict[str, Any]:
    if not accepted_rows:
        log_fn({"event": "synthesis_skipped", "reason": "no_accepted_signals"})
        return {"forces_created": 0, "forces": []}
    
    signals = _prepare_signals_for_synthesis(accepted_rows)
    existing_forces = _load_existing_forces()
    
    synthesizer = prompts.get("ORIONSYNTHESIZER")
    if not synthesizer:
        log_fn({"event": "synthesis_error", "error": "ORIONSYNTHESIZER not found in prompts"})
        return {"forces_created": 0, "forces": []}
    
    synth_input = {
        "signals": signals,
        "existing_forces": existing_forces[:50],
        "project_id": project_id,
    }
    
    try:
        synth_output = call_openai_fn(
            synthesizer["system_prompt"],
            synth_input,
            synthesizer["output_schema"],
            "ORIONSYNTHESIZER",
        )
    except Exception as exc:
        log_fn({"event": "synthesis_error", "error": str(exc)})
        return {"forces_created": 0, "forces": [], "error": str(exc)}
    
    forces = synth_output.get("forces", [])
    cluster_summary = synth_output.get("cluster_summary", {})
    
    log_fn({
        "event": "synthesis_complete",
        "signals_input": len(signals),
        "forces_output": len(forces),
        "cluster_summary": cluster_summary,
    })
    
    force_rows = []
    for force in forces:
        force_row = _build_force_row(force, project_id)
        force_rows.append(force_row)
    
    if force_rows:
        write_csv(output_dir / "orion_forces_accepted.csv", force_rows, fieldnames=ORION_COLUMNS)
        
        staging_rows = []
        for force, output in zip(forces, force_rows):
            staging = dict(output)
            staging["source_signal_ids"] = ",".join(force.get("source_signal_ids", []))
            staging["synthesis_rationale"] = force.get("synthesis_rationale", "")
            staging_rows.append(staging)
        
        extra_keys = ["source_signal_ids", "synthesis_rationale"]
        staging_fieldnames = ORION_COLUMNS + extra_keys
        write_csv(output_dir / "orion_forces_all_candidates.csv", staging_rows, fieldnames=staging_fieldnames)
    
    return {
        "forces_created": len(force_rows),
        "forces": force_rows,
        "cluster_summary": cluster_summary,
    }


def append_forces_to_master(force_rows: List[Dict[str, Any]], master_path: Path = FORCES_MASTER_FILE) -> int:
    if not force_rows:
        return 0
    
    master_path.parent.mkdir(parents=True, exist_ok=True)
    existing_ids = _load_existing_ids(master_path)
    
    new_rows = [row for row in force_rows if row.get("id") not in existing_ids]
    if not new_rows:
        return 0
    
    import csv
    file_exists = master_path.exists() and master_path.stat().st_size > 0
    with master_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ORION_COLUMNS)
        if not file_exists:
            writer.writeheader()
        for row in new_rows:
            writer.writerow({k: row.get(k, "") for k in ORION_COLUMNS})
    
    return len(new_rows)
