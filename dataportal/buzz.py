"""
buzz — aggregeringslogikk for BUZZ-1 (epic #177).

Kjører in-process i dataportal-poden (warm imports → ingen OOM-fare).
Triggeres hver time av airflow-DAG-en 03_buzz_metrics.

Skriver til s3://gold/buzz_metrics/latest.json + history/<ts>.json.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone


log = logging.getLogger(__name__)


# Konstanter
HISTORY_LIMIT               = 168   # 7 dager × 24 timer
QUALITY_INCIDENT_THRESHOLD  = 50.0
RECENT_PUBS_WINDOW_DAYS     = 14
POPULAR_QUESTIONS_DAYS      = 7
POPULAR_QUESTIONS_MAX_FILES = 200

# BUZZ-2: snapshot-cache slik at page-load ikke treffer MinIO. 5 min TTL.
_SNAPSHOT_CACHE: dict[str, tuple[dict, float]] = {}
_SNAPSHOT_CACHE_LOCK = threading.Lock()
SNAPSHOT_CACHE_TTL = 300  # 5 min


def get_snapshot_cached(s3_client_factory) -> dict:
    """Returner siste buzz-snapshot fra MinIO med 5-min in-memory cache.

    Returnerer en tom-tilstand-respons hvis filen ikke finnes (graceful):
      {"empty": True, "generated_at": null}
    """
    now = time.time()
    with _SNAPSHOT_CACHE_LOCK:
        cached = _SNAPSHOT_CACHE.get("latest")
        if cached and (now - cached[1]) < SNAPSHOT_CACHE_TTL:
            return cached[0]

    s3 = s3_client_factory()
    try:
        obj  = s3.get_object(Bucket="gold", Key="buzz_metrics/latest.json")
        data = json.loads(obj["Body"].read())
    except Exception as exc:
        log.info("ingen buzz-snapshot funnet i MinIO: %s", exc)
        data = {"empty": True, "generated_at": None}

    with _SNAPSHOT_CACHE_LOCK:
        _SNAPSHOT_CACHE["latest"] = (data, now)
    return data


def invalidate_snapshot_cache() -> None:
    """Kalles av buzz-compute-endepunktet rett etter at ny snapshot er skrevet."""
    with _SNAPSHOT_CACHE_LOCK:
        _SNAPSHOT_CACHE.pop("latest", None)


def _read_latest_safe(s3, bucket: str, key: str) -> dict | None:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def _calc_trending(usage_now: dict, usage_prev: dict, top_n: int = 5) -> list[dict]:
    out = []
    for pid, n in usage_now.items():
        prev = usage_prev.get(pid, 0)
        if prev > 0:
            delta_pct = round((n - prev) / prev * 100, 1)
        elif n > 0:
            delta_pct = 100.0
        else:
            continue
        if delta_pct > 0:
            out.append({"product_id": pid, "usage_window": n, "usage_prev": prev, "delta_pct": delta_pct})
    out.sort(key=lambda x: x["delta_pct"], reverse=True)
    return out[:top_n]


def _calc_recent_publications(list_all_fn, list_versions_fn, days: int = RECENT_PUBS_WINDOW_DAYS) -> list[dict]:
    cutoff_iso = (datetime.now(tz=timezone.utc) - timedelta(days=days)).isoformat()
    events: list[dict] = []
    for product in list_all_fn():
        pid = product["id"]
        try:
            versions = list_versions_fn(pid)
        except Exception as exc:
            log.debug("list_versions feilet for %s: %s", pid, exc)
            continue
        versions_sorted = sorted(versions, key=lambda v: v.get("registered_at", ""))
        for i, v in enumerate(versions_sorted):
            registered_at = v.get("registered_at", "")
            if registered_at < cutoff_iso:
                continue
            events.append({
                "product_id":   pid,
                "version":      v.get("version", ""),
                "event":        "new" if i == 0 else "version_bump",
                "at":           registered_at,
                "by":           (v.get("manifest") or {}).get("owner", ""),
                "domain":       (v.get("manifest") or {}).get("domain", ""),
                "name":         (v.get("manifest") or {}).get("name", pid),
            })
    events.sort(key=lambda e: e["at"], reverse=True)
    return events[:20]


def _calc_health_and_incidents(s3, list_all_fn) -> tuple[dict, list[dict]]:
    sla_compliant = 0
    sla_total     = 0
    quality_scores: list[float] = []
    incidents: list[dict]       = []

    for product in list_all_fn():
        pid = product["id"]
        sla = _read_latest_safe(s3, "gold", f"sla_results/{pid}/latest.json")
        if sla and sla.get("compliant") is not None:
            sla_total += 1
            if sla["compliant"] is True:
                sla_compliant += 1
            else:
                incidents.append({
                    "product_id":  pid,
                    "type":        "sla_breach",
                    "detail":      f"{sla.get('hours_since_update', '?')}t siden oppdatering (SLA: {sla.get('freshness_hours', '?')}t)",
                    "checked_at":  sla.get("checked_at"),
                })
        quality = _read_latest_safe(s3, "gold", f"quality_results/{pid}/latest.json")
        if quality and quality.get("score_pct") is not None:
            score = quality["score_pct"]
            quality_scores.append(score)
            if score < QUALITY_INCIDENT_THRESHOLD:
                incidents.append({
                    "product_id":  pid,
                    "type":        "quality",
                    "detail":      f"kvalitet {score}% ({quality.get('failed', '?')} av {quality.get('total_expectations', '?')} sjekker feilet)",
                    "checked_at":  quality.get("validated_at"),
                })

    sla_pct = round(sla_compliant / sla_total * 100, 1) if sla_total else None
    avg_q   = round(sum(quality_scores) / len(quality_scores), 1) if quality_scores else None
    return (
        {
            "sla_compliance_pct": sla_pct,
            "avg_quality_pct":    avg_q,
            "active_incidents":   len(incidents),
            "sla_products_total": sla_total,
            "quality_products":   len(quality_scores),
        },
        incidents,
    )


def _calc_popular_questions(s3, top_n: int = 5) -> list[dict]:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=POPULAR_QUESTIONS_DAYS)
    cutoff_prefix = cutoff.strftime("%Y%m%dT%H%M%S")
    prefix = "agent_feedback/"
    by_question: dict[str, dict] = {}
    files_seen = 0
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket="gold", Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                fname = obj["Key"][len(prefix):]
                ts_part = fname.split("_", 1)[0]
                if ts_part < cutoff_prefix:
                    continue
                if files_seen >= POPULAR_QUESTIONS_MAX_FILES:
                    break
                files_seen += 1
                try:
                    body  = s3.get_object(Bucket="gold", Key=obj["Key"])["Body"].read()
                    entry = json.loads(body)
                except Exception:
                    continue
                question = (entry.get("question") or "").strip()
                if not question or entry.get("rating") != "thumbs_up":
                    continue
                slot = by_question.setdefault(question, {
                    "question":    question,
                    "product_ids": entry.get("product_ids") or [],
                    "likes":       0,
                })
                slot["likes"] += 1
            if files_seen >= POPULAR_QUESTIONS_MAX_FILES:
                break
    except Exception as exc:
        log.warning("kunne ikke liste agent_feedback: %s", exc)
    out = sorted(by_question.values(), key=lambda q: q["likes"], reverse=True)
    return out[:top_n]


def _write_snapshot(s3, snapshot: dict) -> None:
    body = json.dumps(snapshot, ensure_ascii=False, indent=2).encode()
    s3.put_object(Bucket="gold", Key="buzz_metrics/latest.json",
                  Body=body, ContentType="application/json")
    ts_key = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    s3.put_object(Bucket="gold", Key=f"buzz_metrics/history/{ts_key}.json",
                  Body=body, ContentType="application/json")
    # Trim historikk
    try:
        resp = s3.list_objects_v2(Bucket="gold", Prefix="buzz_metrics/history/")
        items = sorted(resp.get("Contents", []), key=lambda o: o["Key"], reverse=True)
        for old in items[HISTORY_LIMIT:]:
            s3.delete_object(Bucket="gold", Key=old["Key"])
    except Exception as exc:
        log.warning("historikk-trimming feilet (ufarlig): %s", exc)


def compute_and_write(
    s3_client_factory,
    list_all_fn,
    list_versions_fn,
    usage_by_window_fn,   # (start_iso, end_iso) -> dict[product_id, count]
    active_users_fn,      # (start_iso, end_iso) -> int
    window_days: int = 7,
) -> dict:
    """Hovedinngang. Bygger snapshot og skriver til MinIO. Returnerer kort
    sammendrag for logging."""
    t0  = time.time()
    s3  = s3_client_factory()
    now = datetime.now(tz=timezone.utc)

    end    = now.isoformat()
    start  = (now - timedelta(days=window_days)).isoformat()
    prev_s = (now - timedelta(days=2 * window_days)).isoformat()

    usage_now  = usage_by_window_fn(start, end)
    usage_prev = usage_by_window_fn(prev_s, start)
    active_u   = active_users_fn(start, end)

    recent_pubs = _calc_recent_publications(list_all_fn, list_versions_fn)
    health, incidents = _calc_health_and_incidents(s3, list_all_fn)

    # platform-totaler — beregnes inline her i stedet for egen funksjon
    all_products = list(list_all_fn())
    domains      = {p.get("domain") for p in all_products if p.get("domain")}
    new_7d_cut   = (now - timedelta(days=7)).isoformat()
    new_7d       = sum(1 for e in recent_pubs if e["event"] == "new" and e["at"] >= new_7d_cut)

    snapshot = {
        "generated_at":         now.isoformat(),
        "window_days":          window_days,
        "platform_totals": {
            "products":         len(all_products),
            "domains":          len(domains),
            "active_users_7d":  active_u,
            "new_products_7d":  new_7d,
        },
        "health":               health,
        "trending_up":          _calc_trending(usage_now, usage_prev, top_n=5),
        "recent_publications":  recent_pubs,
        "popular_questions":    _calc_popular_questions(s3, top_n=5),
        "incidents":            incidents,
    }

    _write_snapshot(s3, snapshot)
    elapsed_s = round(time.time() - t0, 1)
    return {
        "products":  snapshot["platform_totals"]["products"],
        "trending":  len(snapshot["trending_up"]),
        "incidents": snapshot["health"]["active_incidents"],
        "questions": len(snapshot["popular_questions"]),
        "elapsed_s": elapsed_s,
    }
