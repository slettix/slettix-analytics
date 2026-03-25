#!/usr/bin/env bash
# Run unit tests inside the spark-master container.
# Usage: ./scripts/run-tests.sh [pytest args]
#
# Examples:
#   ./scripts/run-tests.sh
#   ./scripts/run-tests.sh -v
#   ./scripts/run-tests.sh tests/test_ingest_to_bronze.py -v

set -e

CONTAINER="spark-master"
PYSPARK_ZIP=$(docker exec "$CONTAINER" find /opt/spark/python/lib -name "py4j-*.zip" | head -1)

echo "Syncing files to container..."
docker exec "$CONTAINER" mkdir -p /tmp/app/tests /tmp/app/jobs /tmp/app/data/sample
docker cp tests/. "$CONTAINER":/tmp/app/tests/
docker cp jobs/.  "$CONTAINER":/tmp/app/jobs/
docker cp data/sample/. "$CONTAINER":/tmp/app/data/sample/

echo "Installing pytest..."
docker exec -e HOME=/tmp "$CONTAINER" pip install pytest pytest-timeout -q

echo "Running tests..."
docker exec \
  -e HOME=/tmp \
  -e PATH="/tmp/.local/bin:/opt/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
  -e PYTHONPATH="/tmp/app/jobs:/opt/spark/python:${PYSPARK_ZIP}" \
  "$CONTAINER" \
  python3 -m pytest /tmp/app/tests "$@"
