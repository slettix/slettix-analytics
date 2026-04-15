#!/bin/bash
# Superset-initialisering for Slettix Analytics
# Kjøres én gang av superset-init-tjenesten.
set -e

echo "==> [1/4] Kjører database-migrering..."
superset db upgrade

echo "==> [2/4] Oppretter admin-bruker..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@slettix.local \
    --password admin 2>/dev/null \
    || echo "     (Admin-bruker finnes allerede — hopper over)"

echo "==> [3/4] Initialiserer roller og tillatelser..."
superset init

echo "==> [4/4] Registrerer DuckDB-tilkobling mot Delta Lake..."
python3 /app/pythonpath/setup_duckdb.py

echo ""
echo "✓ Superset-initialisering fullført."
echo "  Logg inn på http://localhost:8088 med admin / admin"
