import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
import time

from jsonschema import validate, ValidationError

from .config import (
    AGENTS_PATHS,
    OUTPUT_ROOT,
    LOG_DIR,
    ORION_COLUMNS,
    OPENAI_MODEL,
    OPENAI_SYNTHESIZER_MODEL,
    OPENAI_TIMEOUT_SECONDS,
    OPENAI_MAX_RETRIES,
    OPENAI_FORCE_RESPONSES,
    MAX_JSON_REPAIR_ATTEMPTS,
    MAX_CANDIDATES_PER_DOC,
    MAX_RUNTIME_SECONDS,
    DUPLICATE_SIMILARITY,
    ACCEPT_PRIORITY,
    REVIEW_MIN_PRIORITY,
    MIN_CREDIBILITY_ACCEPT,
    MIN_CREDIBILITY_REVIEW,
    IMPORTANCE_BINS,
)
from .load_corpus import load_orion_corpus
from .vector_index import VectorIndex
from .sources import load_sources
from .collector import Collector
from .export import write_exports, validate_orion_schema, append_to_master
from .synthesis import run_synthesis as run_synthesis_phase, append_forces_to_master


ORION_DEFAULTS = {
    "type": "S",
    "scope": "signals",
    "impact": 7.0,
    "ttm": "",
    "sentiment": "Neutral",
    "magnitude": None,
    "distance": None,
    "color_hex": "#94A3B8",
    "feasibility": None,
    "urgency": None,
}

ORION_NUMERIC_NULLABLE = {"impact", "magnitude", "distance", "feasibility", "urgency"}


def _normalize_orion_row(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {k: row.get(k, "") for k in ORION_COLUMNS}
    for key in ORION_NUMERIC_NULLABLE:
        val = normalized.get(key)
        if val == "" or (isinstance(val, str) and not val.strip()):
            normalized[key] = None
    for key, val in ORION_DEFAULTS.items():
        if normalized.get(key) in (None, ""):
            normalized[key] = val
    return normalized


def _first_nonempty(*values) -> str:
    for val in values:
        if val is None:
            continue
        if isinstance(val, str) and val.strip():
            return val
        if not isinstance(val, str):
            return str(val)
    return ""


def _normalize_tags(value: Any, fallback: Optional[List[str]] = None) -> List[str]:
    tags: List[str] = []
    if value is None:
        tags = []
    elif isinstance(value, list):
        tags = [str(v).strip() for v in value if str(v).strip()]
    elif isinstance(value, str):
        text = value.strip()
        if text:
            try:
                data = json.loads(text)
                if isinstance(data, list):
                    tags = [str(v).strip() for v in data if str(v).strip()]
                else:
                    tags = [t.strip() for t in text.split(",") if t.strip()]
            except json.JSONDecodeError:
                tags = [t.strip() for t in text.split(",") if t.strip()]
    if not tags and fallback:
        tags = [str(v).strip() for v in fallback if str(v).strip()]
    seen: set = set()
    out: List[str] = []
    for tag in tags:
        if tag in seen:
            continue
        seen.add(tag)
        out.append(tag)
    return out[:8]


def _valid_steep(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    val = value.strip()
    if val in {"Social", "Technological", "Economic", "Environmental"}:
        return val
    return None


def _normalize_candidate(candidate: Dict[str, Any], doc: Dict[str, Any], dimensions: List[str]) -> Dict[str, Any]:
    out = dict(candidate)
    if not out.get("candidate_id"):
        out["candidate_id"] = str(uuid.uuid4())
    out["doc_id"] = doc.get("doc_id", out.get("doc_id", ""))
    out["source_name"] = _first_nonempty(out.get("source_name"), doc.get("source_name"))
    out["canonical_url"] = _first_nonempty(out.get("canonical_url"), doc.get("canonical_url"))
    out["published_at"] = out.get("published_at") or doc.get("published_at")
    out["retrieved_at"] = _first_nonempty(out.get("retrieved_at"), doc.get("retrieved_at"))
    out["content_hash"] = _first_nonempty(out.get("content_hash"), doc.get("content_hash"))

    if not out.get("title"):
        out["title"] = _first_nonempty(doc.get("title"), doc.get("clean_text", "")[:120])
    if not out.get("claim_summary"):
        out["claim_summary"] = _first_nonempty(doc.get("clean_text", "")[:200])
    if not out.get("why_it_matters"):
        out["why_it_matters"] = _first_nonempty(doc.get("clean_text", "")[200:400])

    out["proposed_tags"] = _normalize_tags(out.get("proposed_tags"))
    steep = _valid_steep(out.get("proposed_steep")) or _valid_steep(doc.get("proposed_steep"))
    out["proposed_steep"] = steep or "Technological"
    dim = out.get("proposed_dimension")
    if dim not in dimensions:
        dim = "Other"
    out["proposed_dimension"] = dim
    if out.get("type_suggested") not in {"S", "WS", "T", "WC"}:
        out["type_suggested"] = "S"
    if not out.get("evidence_snippet"):
        snippet = doc.get("clean_text", "")[:240]
        out["evidence_snippet"] = snippet
    return out


def _build_orion_row(
    candidate: Dict[str, Any],
    scores: Dict[str, Any],
    project_id: str,
    curator_row: Optional[Dict[str, Any]],
    dimensions: List[str],
) -> Dict[str, Any]:
    curator_row = curator_row or {}
    now = datetime.now(timezone.utc).isoformat()

    title = _first_nonempty(curator_row.get("title"), candidate.get("title"))
    steep = _valid_steep(curator_row.get("steep")) or _valid_steep(candidate.get("proposed_steep")) or "Technological"
    dimension = curator_row.get("dimension") if curator_row.get("dimension") in dimensions else candidate.get("proposed_dimension")
    if dimension not in dimensions:
        dimension = "Other"
    tags = _normalize_tags(curator_row.get("tags"), candidate.get("proposed_tags"))
    text = _first_nonempty(curator_row.get("text"), f"{candidate.get('claim_summary','')} {candidate.get('why_it_matters','')}".strip())

    row = {
        "id": str(uuid.uuid4()),
        "project_id": project_id,
        "title": title,
        "type": "S",
        "steep": steep,
        "dimension": dimension,
        "scope": "signals",
        "impact": 7.0,
        "ttm": "",
        "sentiment": "Neutral",
        "source": candidate.get("canonical_url"),
        "tags": json.dumps(tags),
        "text": text,
        "magnitude": round(scores.get("priority_index", 0) / 10.0, 2),
        "distance": int(scores.get("importance_distance", 1)),
        "color_hex": "#94A3B8",
        "feasibility": None,
        "urgency": None,
        "created_at": now,
        "updated_at": now,
    }
    return _normalize_orion_row(row)


def _build_staging_row(
    orion_row: Dict[str, Any],
    candidate: Dict[str, Any],
    comparison: Dict[str, Any],
    scores: Dict[str, Any],
    curator_row: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    staging_row = dict(orion_row)
    staging_row.update(
        {
            "candidate_id": candidate.get("candidate_id"),
            "canonical_url": candidate.get("canonical_url"),
            "claim_summary": candidate.get("claim_summary"),
            "content_hash": candidate.get("content_hash"),
            "credibility_score": scores.get("credibility_score"),
            "decision": scores.get("decision"),
            "doc_id": candidate.get("doc_id"),
            "duplicate_flag": comparison.get("duplicate_flag"),
            "evidence_snippet": candidate.get("evidence_snippet"),
            "importance_distance": scores.get("importance_distance"),
            "max_similarity": comparison.get("max_similarity"),
            "nearest_orion_ids": ",".join(comparison.get("nearest_orion_ids", [])),
            "novelty_score": scores.get("novelty_score"),
            "priority_index": scores.get("priority_index"),
            "promotion_suggestion": scores.get("promotion_suggestion"),
            "published_at": candidate.get("published_at"),
            "relevance_score": scores.get("relevance_score"),
            "scoring_rationale": scores.get("scoring_rationale"),
            "why_it_matters": candidate.get("why_it_matters"),
        }
    )
    return staging_row


def _log_event(log_path: Path, payload: Dict[str, Any]):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _parse_json(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start : end + 1])
        raise


try:
    from openai import OpenAI
except Exception:
    OpenAI = None  # type: ignore


@dataclass
class RunSummary:
    docs_fetched: int
    docs_failed: int
    candidates: int
    accept: int
    review: int
    reject: int
    importance_distribution: Dict[int, int]
    forces_created: int = 0


def _load_prompts() -> Dict[str, Dict[str, Any]]:
    for path in AGENTS_PATHS:
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            return {item["agent_name"]: item for item in data}
    raise FileNotFoundError("agents/prompts.json not found")


def _get_openai_client():
    api_key = os.getenv("AI_INTEGRATIONS_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("AI_INTEGRATIONS_OPENAI_BASE_URL")
    if not api_key:
        raise RuntimeError("OpenAI API key not available")
    kwargs = {"api_key": api_key, "timeout": OPENAI_TIMEOUT_SECONDS, "max_retries": OPENAI_MAX_RETRIES}
    if base_url:
        kwargs["base_url"] = base_url
    return OpenAI(**kwargs)


def _repair_json(schema: Dict[str, Any], bad_text: str) -> Dict[str, Any]:
    client = _get_openai_client()
    model = os.getenv("OPENAI_MODEL", OPENAI_MODEL)
    repair_system = (
        "You are a JSON repair assistant. Return ONLY valid JSON that conforms exactly to the provided schema. "
        "No commentary, no markdown."
    )
    payload = {"schema": schema, "invalid_json": bad_text}
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": repair_system},
            {"role": "user", "content": json.dumps(payload)},
        ],
        response_format={"type": "json_object"},
    )
    text = response.choices[0].message.content if response.choices else None
    if not text:
        raise RuntimeError("No response text from JSON repair")
    return _parse_json(text)


def _call_openai(
    system_prompt: str,
    input_obj: Dict[str, Any],
    output_schema: Dict[str, Any],
    agent_name: str,
    log_path: Optional[Path] = None,
    model_override: Optional[str] = None,
) -> Dict[str, Any]:
    if OpenAI is None:
        raise RuntimeError("OpenAI library not available")
    client = _get_openai_client()
    model = model_override or os.getenv("OPENAI_MODEL", OPENAI_MODEL)

    start = time.time()
    if hasattr(client, "responses"):
        response = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(input_obj)},
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "response",
                    "schema": output_schema,
                    "strict": False,
                }
            },
        )
        text = getattr(response, "output_text", None)
        if not text and hasattr(response, "output"):
            text = "".join(part.get("content", "") for part in response.output)
    else:
        if OPENAI_FORCE_RESPONSES or os.getenv("OPENAI_FORCE_RESPONSES") == "1":
            raise RuntimeError("Responses API required but unavailable in this environment")
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(input_obj)},
            ],
            response_format={"type": "json_object"},
        )
        text = response.choices[0].message.content if response.choices else None

    if not text:
        if log_path:
            _log_event(log_path, {"event": "llm_error", "agent": agent_name, "error": "empty_response"})
        raise RuntimeError("No response text from OpenAI")

    data = _parse_json(text)
    try:
        validate(instance=data, schema=output_schema)
    except ValidationError:
        if MAX_JSON_REPAIR_ATTEMPTS > 0:
            data = _repair_json(output_schema, text)
            validate(instance=data, schema=output_schema)
        else:
            raise
    if log_path:
        _log_event(log_path, {"event": "llm_ok", "agent": agent_name, "model": model, "elapsed_s": round(time.time() - start, 2)})
    return data


def _validate_schema(agent_name: str, payload: Dict[str, Any], schema: Dict[str, Any]):
    try:
        validate(instance=payload, schema=schema)
    except ValidationError as exc:
        raise ValueError(f"{agent_name} output failed schema validation: {exc}")


def _priority_to_importance(priority_index: float) -> int:
    for low, high, val in IMPORTANCE_BINS:
        if low <= priority_index <= high:
            return val
    return 1 if priority_index < 0 else 10


def _novelty_from_similarity(max_similarity: float) -> float:
    if max_similarity >= 0.90:
        return max(0.0, 15.0 * (1 - (max_similarity - 0.90) / 0.10))
    if max_similarity <= 0.70:
        return 85.0 + (0.70 - max_similarity) * 150.0
    slope = (85.0 - 15.0) / (0.70 - 0.90)
    return 15.0 + slope * (max_similarity - 0.90)


def _score_stub(candidate: Dict[str, Any], comparison: Dict[str, Any], source_tier: Optional[str]):
    max_sim = comparison["max_similarity"]
    novelty = max(0.0, min(100.0, _novelty_from_similarity(max_sim)))

    tier_base = {"A": 85, "B": 72, "C": 58, "D": 35}.get((source_tier or "C").upper(), 58)
    credibility = float(tier_base)

    relevance = 55.0
    priority_index = 0.45 * relevance + 0.35 * novelty + 0.20 * credibility

    if credibility < 40:
        priority_index = min(priority_index, 50)
    if credibility < 25:
        priority_index = min(priority_index, 35)

    if comparison.get("duplicate_flag") or max_sim >= DUPLICATE_SIMILARITY:
        importance = 1
        decision = "reject"
    else:
        importance = _priority_to_importance(priority_index)
        if priority_index >= ACCEPT_PRIORITY and credibility >= MIN_CREDIBILITY_ACCEPT:
            decision = "accept"
        elif priority_index >= REVIEW_MIN_PRIORITY or credibility >= MIN_CREDIBILITY_REVIEW:
            decision = "review"
        else:
            decision = "reject"

    return {
        "candidate_id": candidate["candidate_id"],
        "novelty_score": round(novelty, 2),
        "credibility_score": round(credibility, 2),
        "relevance_score": round(relevance, 2),
        "priority_index": round(priority_index, 2),
        "importance_distance": int(importance),
        "decision": decision,
        "promotion_suggestion": "none",
        "scoring_rationale": "Stub scoring based on v1 rules.",
    }


def _curator_stub(candidate: Dict[str, Any], comparison: Dict[str, Any], scores: Dict[str, Any], project_id: str):
    now = datetime.now(timezone.utc).isoformat()
    orion_row = {
        "id": str(uuid.uuid4()),
        "project_id": project_id,
        "title": candidate["title"],
        "type": "S" if scores["decision"] == "accept" else "S",
        "steep": candidate["proposed_steep"],
        "dimension": candidate["proposed_dimension"],
        "scope": "signals",
        "impact": 7.0,
        "ttm": "",
        "sentiment": "Neutral",
        "source": candidate.get("canonical_url"),
        "tags": json.dumps(candidate.get("proposed_tags", [])),
        "text": f"{candidate.get('claim_summary','')} {candidate.get('why_it_matters','')}".strip(),
        "magnitude": round(scores["priority_index"] / 10.0, 2),
        "distance": int(scores["importance_distance"]),
        "color_hex": "#94A3B8",
        "feasibility": None,
        "urgency": None,
        "created_at": now,
        "updated_at": now,
    }
    staging_row = dict(orion_row)
    staging_row.update(
        {
            "candidate_id": candidate["candidate_id"],
            "source_name": candidate.get("source_name"),
            "source_url": candidate.get("canonical_url"),
            "published_at": candidate.get("published_at"),
            "retrieved_at": candidate.get("retrieved_at"),
            "content_hash": candidate.get("content_hash"),
            "novelty_score": scores.get("novelty_score"),
            "credibility_score": scores.get("credibility_score"),
            "relevance_score": scores.get("relevance_score"),
            "priority_index": scores.get("priority_index"),
            "max_similarity": comparison.get("max_similarity"),
            "nearest_orion_ids": ",".join(comparison.get("nearest_orion_ids", [])),
            "decision": scores.get("decision"),
            "decision_rationale": scores.get("scoring_rationale"),
            "promotion_suggestion": scores.get("promotion_suggestion"),
        }
    )
    return {
        "candidate_id": candidate["candidate_id"],
        "orion_row": orion_row,
        "staging_row": staging_row,
    }


def _exporter_stub(run_id: str, date: str, rows: List[Dict[str, Any]]):
    accept = [r for r in rows if r["decision"] == "accept"]
    review = [r for r in rows if r["decision"] == "review"]
    reject = [r for r in rows if r["decision"] == "reject"]
    valid, errors = validate_orion_schema([r["orion_row"] for r in rows])
    return {
        "run_id": run_id,
        "outputs": {
            "all_candidates_file": f"out/{date}/orion_daily_all_candidates.csv",
            "accepted_file": f"out/{date}/orion_daily_accepted.csv",
            "pending_review_file": f"out/{date}/orion_daily_pending_review.csv",
            "rejected_file": f"out/{date}/orion_daily_rejected.csv",
        },
        "counts": {"total": len(rows), "accept": len(accept), "review": len(review), "reject": len(reject)},
        "schema_validation": {"valid": valid, "errors": errors},
    }


def _calibrate_importance(scores: List[Dict[str, Any]]):
    eligible = [s for s in scores if s["decision"] != "reject"]
    if len(eligible) < 10:
        return
    ranked = sorted(eligible, key=lambda s: s["priority_index"], reverse=True)
    n = len(ranked)
    top_10 = max(1, int(n * 0.07))
    mid = max(1, int(n * 0.28))
    for i, s in enumerate(ranked):
        if i < top_10:
            s["importance_distance"] = max(8, min(10, s["importance_distance"]))
        elif i < top_10 + mid:
            s["importance_distance"] = max(6, min(7, s["importance_distance"]))
        else:
            s["importance_distance"] = min(5, s["importance_distance"])


def run_pipeline(
    date_str: str,
    max_sources: int,
    max_docs_per_source: int,
    max_docs_total: Optional[int] = None,
    max_candidates_per_doc: Optional[int] = None,
    max_runtime_seconds: Optional[int] = None,
    sources_override: Optional[List[Dict[str, Any]]] = None,
    run_synthesis: bool = False,
) -> RunSummary:
    run_id = str(uuid.uuid4())
    output_dir = OUTPUT_ROOT / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"run_{date_str}_{run_id}.jsonl"
    responses_available = False
    openai_version = None
    if OpenAI is not None:
        try:
            import openai as _openai

            openai_version = getattr(_openai, "__version__", None)
            responses_available = hasattr(OpenAI(), "responses")
        except Exception:
            pass
    start_time = time.time()
    _log_event(
        log_path,
        {
            "event": "run_start",
            "run_id": run_id,
            "date": date_str,
            "openai_version": openai_version,
            "responses_available": responses_available,
        },
    )

    prompts = _load_prompts()
    records, project_id, dimensions, tag_vocab = load_orion_corpus()

    index = VectorIndex()
    index.build(records)

    if sources_override is not None:
        sources = sources_override
    else:
        sources = load_sources()[:max_sources]
    collector = Collector()
    collected = collector.fetch_docs(sources, max_docs_per_source=max_docs_per_source)
    if max_docs_total and max_docs_total > 0:
        collected.docs = collected.docs[:max_docs_total]
    effective_max_candidates = max_candidates_per_doc or MAX_CANDIDATES_PER_DOC
    est_max_calls = len(collected.docs) * (1 + effective_max_candidates * 3) + 1
    _log_event(log_path, {"event": "llm_call_estimate", "docs": len(collected.docs), "estimated_calls": est_max_calls})
    (output_dir / "collector_report.json").write_text(json.dumps(collected.stats, indent=2), encoding="utf-8")
    _log_event(
        log_path,
        {
            "event": "collector_summary",
            "sources": len(sources),
            "docs_fetched": len(collected.docs),
            "failed": collected.failed,
        },
    )

    pathfinder = prompts["ORIONPATHFINDER"]
    comparator = prompts["ORIONCOMPARATOR"]
    scorer = prompts["ORIONSCORER"]
    curator = prompts["ORIONCURATOR"]
    exporter = prompts["ORIONEXPORTER"]

    all_rows: List[Dict[str, Any]] = []
    candidates_total = 0

    for doc in collected.docs:
        pf_input = {"doc": doc, "dimensions": dimensions, "tag_vocab": tag_vocab}
        try:
            pf_output = _call_openai(pathfinder["system_prompt"], pf_input, pathfinder["output_schema"], "ORIONPATHFINDER", log_path)
        except Exception as exc:
            _log_event(log_path, {"event": "pathfinder_error", "doc_id": doc.get("doc_id"), "error": str(exc)})
            pf_output = {"doc_id": doc["doc_id"], "candidates": []}
        _validate_schema("ORIONPATHFINDER", pf_output, pathfinder["output_schema"])
        if max_runtime_seconds or MAX_RUNTIME_SECONDS:
            limit = max_runtime_seconds or MAX_RUNTIME_SECONDS
            if limit and (time.time() - start_time) > limit:
                _log_event(log_path, {"event": "runtime_budget_exceeded"})
                break
        normalized_candidates = []
        for cand in pf_output["candidates"][:effective_max_candidates]:
            normalized_candidates.append(_normalize_candidate(cand, doc, dimensions))
        pf_output["candidates"] = normalized_candidates
        for cand in normalized_candidates:
            candidates_total += 1
            query_text = f"{cand.get('title','')} {cand.get('claim_summary','')} {cand.get('why_it_matters','')}"
            neighbors = index.query(query_text, top_k=5)
            comp_input = {"candidate": cand, "orion_nn": {"neighbors": neighbors}}
            try:
                comp_output = _call_openai(comparator["system_prompt"], comp_input, comparator["output_schema"], "ORIONCOMPARATOR", log_path)
            except Exception as exc:
                _log_event(log_path, {"event": "comparator_error", "candidate_id": cand.get("candidate_id"), "error": str(exc)})
                max_sim = max([n["similarity"] for n in neighbors], default=0.0)
                comp_output = {
                    "candidate_id": cand["candidate_id"],
                    "max_similarity": max_sim,
                    "nearest_orion_ids": [n["id"] for n in neighbors],
                    "duplicate_flag": bool(max_sim >= DUPLICATE_SIMILARITY),
                    "comparison_rationale": "Stub comparator using lexical similarity.",
                }
            _validate_schema("ORIONCOMPARATOR", comp_output, comparator["output_schema"])

            source_tier = None
            for s in sources:
                if s.get("source_name") == cand.get("source_name"):
                    source_tier = s.get("tier")
                    break

            score_input = {"candidate": cand, "comparison": comp_output, "source_tier": source_tier, "corroboration_count": 1}
            try:
                score_output = _call_openai(scorer["system_prompt"], score_input, scorer["output_schema"], "ORIONSCORER", log_path)
            except Exception as exc:
                _log_event(log_path, {"event": "scorer_error", "candidate_id": cand.get("candidate_id"), "error": str(exc)})
                score_output = _score_stub(cand, comp_output, source_tier)
            _validate_schema("ORIONSCORER", score_output, scorer["output_schema"])

            curator_input = {"candidate": cand, "comparison": comp_output, "scores": score_output, "project_id": project_id}
            try:
                curator_output = _call_openai(curator["system_prompt"], curator_input, curator["output_schema"], "ORIONCURATOR", log_path)
            except Exception as exc:
                _log_event(log_path, {"event": "curator_error", "candidate_id": cand.get("candidate_id"), "error": str(exc)})
                curator_output = _curator_stub(cand, comp_output, score_output, project_id)
            _validate_schema("ORIONCURATOR", curator_output, curator["output_schema"])

            orion_row = _normalize_orion_row(curator_output.get("orion_row", {}))
            staging_row = _build_staging_row(orion_row, cand, comp_output, score_output, curator_output.get("orion_row"))

            all_rows.append({
                "orion_row": orion_row,
                "staging_row": staging_row,
                "decision": score_output["decision"],
                "priority_index": score_output["priority_index"],
                "importance_distance": score_output["importance_distance"],
            })

    _calibrate_importance(all_rows)

    staging_rows = [r["staging_row"] for r in all_rows]
    append_rows = [r["orion_row"] for r in all_rows if r["decision"] == "accept"]
    review_rows = [r["staging_row"] for r in all_rows if r["decision"] == "review"]
    reject_rows = [r["staging_row"] for r in all_rows if r["decision"] == "reject"]

    write_exports(output_dir, staging_rows, append_rows, review_rows, reject_rows)

    master_added = append_to_master(append_rows)
    _log_event(log_path, {"event": "master_append", "rows_added": master_added})

    forces_created = 0
    if run_synthesis and append_rows:
        synth_model = os.getenv("OPENAI_SYNTHESIZER_MODEL", OPENAI_SYNTHESIZER_MODEL)
        def synthesis_call_openai(system_prompt, input_data, output_schema, agent_name):
            return _call_openai(system_prompt, input_data, output_schema, agent_name, log_path, model_override=synth_model)
        
        def synthesis_log(event_data):
            _log_event(log_path, event_data)
        
        synthesis_result = run_synthesis_phase(
            accepted_rows=append_rows,
            project_id=project_id,
            output_dir=output_dir,
            call_openai_fn=synthesis_call_openai,
            log_fn=synthesis_log,
            prompts=prompts,
        )
        forces_created = synthesis_result.get("forces_created", 0)
        if synthesis_result.get("forces"):
            forces_added = append_forces_to_master(synthesis_result["forces"])
            _log_event(log_path, {"event": "forces_master_append", "rows_added": forces_added})

    exporter_input = {"run_id": run_id, "date": date_str, "rows": [{"decision": r["decision"], "orion_row": r["orion_row"]} for r in all_rows]}
    try:
        exporter_output = _call_openai(exporter["system_prompt"], exporter_input, exporter["output_schema"], "ORIONEXPORTER", log_path)
    except Exception as exc:
        _log_event(log_path, {"event": "exporter_error", "error": str(exc)})
        exporter_output = _exporter_stub(run_id, date_str, all_rows)
    _log_event(log_path, {"event": "exporter_output", "data": exporter_output})

    accept_count = len(append_rows)
    review_count = len(review_rows)
    reject_count = len(reject_rows)
    importance_dist: Dict[int, int] = {}
    for r in all_rows:
        imp = r.get("importance_distance", 1)
        importance_dist[imp] = importance_dist.get(imp, 0) + 1

    _log_event(
        log_path,
        {
            "event": "run_end",
            "docs_fetched": len(collected.docs),
            "docs_failed": collected.failed,
            "candidates": candidates_total,
            "accept": accept_count,
            "review": review_count,
            "reject": reject_count,
            "importance_distribution": importance_dist,
        },
    )

    return RunSummary(
        docs_fetched=len(collected.docs),
        docs_failed=collected.failed,
        candidates=candidates_total,
        accept=accept_count,
        review=review_count,
        reject=reject_count,
        importance_distribution=importance_dist,
        forces_created=forces_created,
    )
