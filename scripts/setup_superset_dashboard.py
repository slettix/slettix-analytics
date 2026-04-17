"""
Setter opp Folkeregister Oversikt-dashbordet i Superset via REST API.

Oppretter:
  - 2 virtuelle datasett (population_stats, marital_status_trends)
  - 5 charts (KPI-kort, tidsserie, sivilstand, aldersgruppe)
  - 1 dashboard med alle charts

Kjøring:
  python3 scripts/setup_superset_dashboard.py [--host http://localhost:8088]
"""

import argparse
import json
import sys
import requests

# ── Autentisering ─────────────────────────────────────────────────────────────

def get_token(host: str, username: str, password: str) -> dict:
    resp = requests.post(
        f"{host}/api/v1/security/login",
        json={"username": username, "password": password, "provider": "db", "refresh": True},
    )
    resp.raise_for_status()
    data = resp.json()
    return {
        "Authorization": f"Bearer {data['access_token']}",
        "Content-Type": "application/json",
        "X-CSRFToken": _get_csrf(host, data["access_token"]),
        "Referer": host,
    }


def _get_csrf(host: str, token: str) -> str:
    resp = requests.get(
        f"{host}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    return resp.json()["result"]


# ── Hjelpefunksjoner ──────────────────────────────────────────────────────────

def _post(host, path, headers, payload):
    resp = requests.post(f"{host}{path}", headers=headers, json=payload)
    if not resp.ok:
        print(f"  [FEIL] {path}: {resp.status_code} — {resp.text[:300]}")
        return None
    return resp.json()


def _get_db_id(host: str, headers: dict) -> int:
    resp = requests.get(f"{host}/api/v1/database/", headers=headers)
    resp.raise_for_status()
    for db in resp.json().get("result", []):
        if "Slettix" in db["database_name"] or "DuckDB" in db["database_name"]:
            print(f"  Bruker database '{db['database_name']}' (id={db['id']})")
            return db["id"]
    raise RuntimeError("Fant ikke 'Slettix Delta Lake'-databasen i Superset. Kjør k8s-init først.")


def _find_existing(host, headers, path, name_field, name):
    resp = requests.get(f"{host}{path}?q=(filters:!((col:{name_field},opr:DatasetIsNullOrEmpty,val:false)))", headers=headers)
    if resp.ok:
        for item in resp.json().get("result", []):
            if item.get(name_field) == name:
                return item["id"]
    return None


def create_dataset(host, headers, db_id, table_name, sql):
    existing = _find_existing(host, headers, "/api/v1/dataset/", "table_name", table_name)
    if existing:
        print(f"  Datasett '{table_name}' finnes allerede (id={existing})")
        return existing

    result = _post(host, "/api/v1/dataset/", headers, {
        "database": db_id,
        "schema": None,
        "sql": sql,
        "table_name": table_name,
        "is_managed_externally": False,
    })
    if result:
        ds_id = result["id"]
        print(f"  Opprettet datasett '{table_name}' (id={ds_id})")
        return ds_id
    return None


def create_chart(host, headers, name, ds_id, viz_type, params: dict) -> int | None:
    params_str = json.dumps({**params, "datasource": f"{ds_id}__table", "viz_type": viz_type})
    result = _post(host, "/api/v1/chart/", headers, {
        "slice_name":      name,
        "datasource_id":   ds_id,
        "datasource_type": "table",
        "viz_type":        viz_type,
        "params":          params_str,
    })
    if result:
        chart_id = result["id"]
        print(f"  Opprettet chart '{name}' (id={chart_id})")
        return chart_id
    return None


def create_dashboard(host, headers, title, chart_ids: list[int]) -> int | None:
    result = _post(host, "/api/v1/dashboard/", headers, {
        "dashboard_title": title,
        "published":       True,
        "position_json":   json.dumps(_build_layout(chart_ids)),
    })
    if result:
        dash_id = result["id"]
        print(f"  Opprettet dashboard '{title}' (id={dash_id})")
        return dash_id
    return None


def _build_layout(chart_ids: list[int]) -> dict:
    """Genererer et enkelt grid-layout for dashbordet."""
    ROOT = "ROOT_ID"
    GRID = "GRID_ID"
    ROW1 = "ROW-1"
    ROW2 = "ROW-2"

    layout = {
        ROOT: {"type": "ROOT", "id": ROOT, "children": [GRID]},
        GRID: {"type": "GRID", "id": GRID, "children": [ROW1, ROW2], "parents": [ROOT]},
        ROW1: {"type": "ROW",  "id": ROW1, "children": [], "parents": [GRID],
               "meta": {"background": "BACKGROUND_TRANSPARENT"}},
        ROW2: {"type": "ROW",  "id": ROW2, "children": [], "parents": [GRID],
               "meta": {"background": "BACKGROUND_TRANSPARENT"}},
    }

    # Første rad: KPI-kort + tidsserie
    kpi_ids    = chart_ids[:3]   # 3 KPI-kort
    other_ids  = chart_ids[3:]   # Resten

    for i, cid in enumerate(kpi_ids):
        key = f"CHART-kpi-{i}"
        layout[ROW1]["children"].append(key)
        layout[key] = {
            "type": "CHART", "id": key,
            "children": [],
            "parents": [GRID, ROW1],
            "meta": {"chartId": cid, "width": 4, "height": 50},
        }

    for i, cid in enumerate(other_ids):
        key = f"CHART-{i}"
        layout[ROW2]["children"].append(key)
        layout[key] = {
            "type": "CHART", "id": key,
            "children": [],
            "parents": [GRID, ROW2],
            "meta": {"chartId": cid, "width": 6, "height": 70},
        }

    return layout


# ── Hoved-logikk ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",     default="http://localhost:8088")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="admin")
    args = parser.parse_args()

    host = args.host.rstrip("/")
    print(f"Kobler til Superset på {host} …")

    headers = get_token(host, args.username, args.password)
    db_id   = _get_db_id(host, headers)

    # ── Datasett ───────────────────────────────────────────────────────────
    # Extensions (httpfs, delta) og MinIO-secret settes opp automatisk
    # av superset_config.py via SQLAlchemy Pool.connect-event — ikke nødvendig her.
    print("\n[1/3] Oppretter virtuelle datasett …")

    ds_pop = create_dataset(host, headers, db_id, "fr_population_stats",
        "SELECT * FROM delta_scan('s3://gold/folkeregister/population_stats')")

    ds_mar = create_dataset(host, headers, db_id, "fr_marital_status_trends",
        "SELECT * FROM delta_scan('s3://gold/folkeregister/marital_status_trends')")

    if not ds_pop or not ds_mar:
        print("Kunne ikke opprette datasett — avbryter.")
        sys.exit(1)

    # ── Charts ─────────────────────────────────────────────────────────────
    print("\n[2/3] Oppretter charts …")
    chart_ids = []

    # KPI 1: Total befolkning
    c = create_chart(host, headers, "Total befolkning", ds_pop, "big_number_total", {
        "metric": {
            "expressionType": "SIMPLE",
            "column": {"column_name": "population_total"},
            "aggregate": "SUM",
            "label": "Total befolkning",
        },
        "subheader": "Totalt antall registrerte personer",
        "time_range": "No filter",
    })
    if c: chart_ids.append(c)

    # KPI 2: Fødsler siste år
    c = create_chart(host, headers, "Fødsler (siste år)", ds_pop, "big_number_total", {
        "metric": {
            "expressionType": "SIMPLE",
            "column": {"column_name": "births"},
            "aggregate": "SUM",
            "label": "Fødsler",
        },
        "subheader": "Registrerte fødsler siste år",
        "time_range": "No filter",
    })
    if c: chart_ids.append(c)

    # KPI 3: Dødsfall siste år
    c = create_chart(host, headers, "Dødsfall (siste år)", ds_pop, "big_number_total", {
        "metric": {
            "expressionType": "SIMPLE",
            "column": {"column_name": "deaths"},
            "aggregate": "SUM",
            "label": "Dødsfall",
        },
        "subheader": "Registrerte dødsfall siste år",
        "time_range": "No filter",
    })
    if c: chart_ids.append(c)

    # Tidsserie: befolkningsutvikling
    c = create_chart(host, headers, "Befolkningsutvikling over tid", ds_pop, "echarts_timeseries_line", {
        "x_axis": "reference_year",
        "metrics": [{
            "expressionType": "SIMPLE",
            "column": {"column_name": "population_total"},
            "aggregate": "SUM",
            "label": "Total befolkning",
        }],
        "groupby": [],
        "time_range": "No filter",
        "x_axis_title": "År",
        "y_axis_title": "Befolkning",
        "color_scheme": "supersetColors",
    })
    if c: chart_ids.append(c)

    # Søylediagram: sivilstandsfordeling per år
    c = create_chart(host, headers, "Sivilstandsfordeling per år", ds_mar, "echarts_timeseries_bar", {
        "x_axis": "reference_year",
        "metrics": [{
            "expressionType": "SIMPLE",
            "column": {"column_name": "count"},
            "aggregate": "SUM",
            "label": "Antall",
        }],
        "groupby": ["marital_status"],
        "time_range": "No filter",
        "stack": True,
        "color_scheme": "supersetColors",
    })
    if c: chart_ids.append(c)

    # ── Dashboard ──────────────────────────────────────────────────────────
    print("\n[3/3] Oppretter dashboard …")
    dash_id = create_dashboard(
        host, headers,
        "Folkeregister Oversikt",
        chart_ids,
    )

    if dash_id:
        print(f"\nDashboard tilgjengelig på: {host}/superset/dashboard/{dash_id}/")
    else:
        print("\n[ADVARSEL] Dashboard ble ikke opprettet — opprett manuelt i Superset UI.")
        print(f"Chart-IDer: {chart_ids}")


if __name__ == "__main__":
    main()
