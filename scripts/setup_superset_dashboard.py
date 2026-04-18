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


def _build_query_context(ds_id: int, viz_type: str, params: dict) -> str:
    """
    Bygg query_context-JSON for Superset 4.x.
    Uten dette feltet viser dashbord-charts 'Unexpected error' fordi
    Superset 4.x ikke lenger genererer query_context on-the-fly.
    """
    metrics = params.get("metrics") or []
    if params.get("metric"):
        metrics = [params["metric"]]

    groupby = params.get("groupby") or []

    # x_axis brukes av timeseries-charts (ikke metrics/groupby-mode)
    x_axis = params.get("x_axis")
    columns = [x_axis] if x_axis else []

    qc = {
        "datasource":   {"id": ds_id, "type": "table"},
        "force":        False,
        "queries": [
            {
                "filters":              [],
                "extras":               {"where": "", "having": ""},
                "applied_time_extras":  {},
                "columns":              columns,
                "metrics":              metrics,
                "groupby":              groupby,
                "orderby":              [],
                "annotation_layers":    [],
                "row_limit":            50000,
                "series_columns":       groupby,
                "series_limit":         0,
                "order_desc":           True,
                "url_params":           {},
                "custom_params":        {},
                "custom_form_data":     {},
                "time_range":           params.get("time_range", "No filter"),
            }
        ],
        "result_format": "json",
        "result_type":   "full",
    }
    return json.dumps(qc)


def create_chart(host, headers, name, ds_id, viz_type, params: dict, dashboard_id: int | None = None) -> int | None:
    params_str    = json.dumps({**params, "datasource": f"{ds_id}__table", "viz_type": viz_type})
    query_context = _build_query_context(ds_id, viz_type, params)
    payload = {
        "slice_name":      name,
        "datasource_id":   ds_id,
        "datasource_type": "table",
        "viz_type":        viz_type,
        "params":          params_str,
        "query_context":   query_context,
    }
    if dashboard_id:
        payload["dashboards"] = [dashboard_id]
    result = _post(host, "/api/v1/chart/", headers, payload)
    if result:
        chart_id = result["id"]
        print(f"  Opprettet chart '{name}' (id={chart_id})")
        return chart_id
    return None


def patch_query_context(host: str, headers: dict) -> None:
    """
    Oppdater query_context for alle eksisterende charts som mangler det.
    Kjøres automatisk etter opprettelse av charts for å sikre at
    dashbord-rendering fungerer i Superset 4.x.
    """
    resp = requests.get(f"{host}/api/v1/chart/?q=(page_size:100)", headers=headers)
    if not resp.ok:
        print(f"  [FEIL] Kunne ikke hente charts: {resp.status_code}")
        return

    csrf = _get_csrf(host, headers["Authorization"].split(" ")[1])
    h2   = {**headers, "X-CSRFToken": csrf}

    for chart in resp.json().get("result", []):
        chart_id  = chart["id"]
        chart_name = chart["slice_name"]

        # Hent fullstendig chart for params
        detail = requests.get(f"{host}/api/v1/chart/{chart_id}", headers=headers)
        if not detail.ok:
            continue
        c = detail.json().get("result", {})

        try:
            params = json.loads(c.get("params") or "{}")
        except Exception:
            continue

        viz_type = c.get("viz_type")
        # datasource_id is not exposed in list endpoint; parse from params "N__table"
        datasource_str = params.get("datasource", "")
        try:
            ds_id = int(datasource_str.split("__")[0])
        except (ValueError, IndexError):
            continue
        if not ds_id or not viz_type:
            continue

        qc = _build_query_context(ds_id, viz_type, params)

        put_resp = requests.put(
            f"{host}/api/v1/chart/{chart_id}",
            headers=h2,
            json={"query_context": qc, "query_context_generation": False},
        )
        if put_resp.ok:
            print(f"  Patchet query_context for '{chart_name}' (id={chart_id})")
        else:
            print(f"  [FEIL] Patch feilet for '{chart_name}': {put_resp.status_code} {put_resp.text[:100]}")


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

    # ── Dashboard opprettes først (tomt) ───────────────────────────────────
    print("\n[2/3] Oppretter dashboard …")
    dash_id = create_dashboard(host, headers, "Folkeregister Oversikt", [])
    if not dash_id:
        print("Kunne ikke opprette dashboard — avbryter.")
        sys.exit(1)

    # ── Charts opprettes med dashboard_id slik at slices-relasjonen settes ─
    print("\n[3/3] Oppretter charts og knytter til dashboard …")
    chart_ids = []

    def _chart(name, ds_id, viz_type, params):
        c = create_chart(host, headers, name, ds_id, viz_type, params, dashboard_id=dash_id)
        if c:
            chart_ids.append(c)

    _chart("Total befolkning", ds_pop, "big_number_total", {
        "metric": {"expressionType": "SIMPLE", "column": {"column_name": "population_total"},
                   "aggregate": "SUM", "label": "Total befolkning"},
        "subheader": "Totalt antall registrerte personer",
        "time_range": "No filter",
    })
    _chart("Fødsler (siste år)", ds_pop, "big_number_total", {
        "metric": {"expressionType": "SIMPLE", "column": {"column_name": "births"},
                   "aggregate": "SUM", "label": "Fødsler"},
        "subheader": "Registrerte fødsler siste år",
        "time_range": "No filter",
    })
    _chart("Dødsfall (siste år)", ds_pop, "big_number_total", {
        "metric": {"expressionType": "SIMPLE", "column": {"column_name": "deaths"},
                   "aggregate": "SUM", "label": "Dødsfall"},
        "subheader": "Registrerte dødsfall siste år",
        "time_range": "No filter",
    })
    _chart("Befolkningsutvikling over tid", ds_pop, "echarts_timeseries_line", {
        "x_axis": "reference_year",
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "population_total"},
                     "aggregate": "SUM", "label": "Total befolkning"}],
        "groupby": [], "time_range": "No filter",
        "x_axis_title": "År", "y_axis_title": "Befolkning",
        "color_scheme": "supersetColors",
    })
    _chart("Sivilstandsfordeling per år", ds_mar, "echarts_timeseries_bar", {
        "x_axis": "reference_year",
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "count"},
                     "aggregate": "SUM", "label": "Antall"}],
        "groupby": ["marital_status"], "time_range": "No filter",
        "stack": True, "color_scheme": "supersetColors",
    })

    # Oppdater layout med de faktiske chart-IDene
    if chart_ids:
        csrf = _get_csrf(host, headers["Authorization"].split(" ")[1])
        h2 = {**headers, "X-CSRFToken": csrf}
        requests.put(f"{host}/api/v1/dashboard/{dash_id}", headers=h2,
                     json={"position_json": json.dumps(_build_layout(chart_ids))})
        print(f"  Layout oppdatert med {len(chart_ids)} charts")

    # Patch query_context for alle charts (nødvendig for Superset 4.x dashboard-rendering)
    print("\n[4/4] Patcher query_context for alle charts …")
    patch_query_context(host, headers)

    print(f"\nDashboard tilgjengelig på: {host}/superset/dashboard/{dash_id}/")


if __name__ == "__main__":
    main()
