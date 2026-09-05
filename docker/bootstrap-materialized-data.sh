#!/bin/sh
set -eu

api_url="${DATAFABRIC_API_URL:-http://dermalert-backend:8000}"

echo "Waiting for DataFabric API at ${api_url}..."
attempt=0
until curl -fsS "${api_url}/" >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 120 ]; then
    echo "DataFabric API did not become ready" >&2
    exit 1
  fi
  sleep 2
done

materialize_bronze() {
  config_id="$1"
  path_indices="$2"
  is_ready=true

  for path_index in $path_indices; do
    data_url="${api_url}/api/bronze/configs/persistent/${config_id}/data?limit=1&offset=0&path_index=${path_index}"
    if ! curl -fsS "$data_url" >/dev/null 2>&1; then
      is_ready=false
    fi
  done

  if [ "$is_ready" = true ]; then
    echo "Bronze ${config_id} already materialized"
    return
  fi

  echo "Materializing Bronze ${config_id}..."
  response_file="/tmp/bronze-${config_id}.json"
  curl -fsS --max-time 1800 -X POST \
    "${api_url}/api/bronze/configs/persistent/${config_id}/execute" \
    >"$response_file"

  if ! grep -q '"status":"success"' "$response_file"; then
    echo "Bronze ${config_id} materialization failed" >&2
    sed -n '1,20p' "$response_file" >&2
    exit 1
  fi

  for path_index in $path_indices; do
    data_url="${api_url}/api/bronze/configs/persistent/${config_id}/data?limit=1&offset=0&path_index=${path_index}"
    curl -fsS "$data_url" >/dev/null
  done
  echo "Bronze ${config_id} ready"
}

materialize_silver() {
  config_id="$1"
  data_url="${api_url}/api/silver/persistent/configs/${config_id}/data?limit=1&offset=0"

  if curl -fsS "$data_url" >/dev/null 2>&1; then
    echo "Silver ${config_id} already materialized"
    return
  fi

  echo "Materializing Silver ${config_id}..."
  response_file="/tmp/silver-${config_id}.json"
  curl -fsS --max-time 1800 -X POST \
    "${api_url}/api/silver/persistent/configs/${config_id}/execute" \
    >"$response_file"

  if ! grep -q '"status":"success"' "$response_file"; then
    echo "Silver ${config_id} materialization failed" >&2
    sed -n '1,20p' "$response_file" >&2
    exit 1
  fi

  curl -fsS "$data_url" >/dev/null
  echo "Silver ${config_id} ready"
}

# Bronze must be ready before the dependent Silver transformations run. The
# second argument lists every output partition expected by the bundled config.
materialize_bronze 1 "0"
materialize_bronze 11 "0 1 2"
materialize_bronze 12 "0"
materialize_bronze 13 "0 1 2"
materialize_bronze 14 "0 1 2"

for config_id in 1 2 3 4 5 6 7; do
  materialize_silver "$config_id"
done

echo "All bundled persistent Bronze and Silver datasets are ready"
