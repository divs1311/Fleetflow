#!/usr/bin/env bash
# Load PorterFlow's exported gold tables into BigQuery (works on the free sandbox).
#   usage: scripts/publish_bigquery.sh <gcp-project> <dataset> [export-dir]
# Requires the Google Cloud CLI (`bq`) and `gcloud auth login`.
set -euo pipefail
PROJECT="${1:?gcp project id}"
DATASET="${2:?dataset name}"
EXPORT_DIR="${3:-data/state/export}"

shopt -s nullglob
files=("$EXPORT_DIR"/*.parquet)
if [ ${#files[@]} -eq 0 ]; then
  echo "no Parquet files in $EXPORT_DIR (run the pipeline with PORTERFLOW_EXPORT_TARGET=s3 first)" >&2
  exit 1
fi

bq --project_id="$PROJECT" show "$DATASET" >/dev/null 2>&1 || bq --project_id="$PROJECT" mk --dataset "$DATASET"
for f in "${files[@]}"; do
  table="$(basename "$f" .parquet)"
  echo "loading $table"
  bq --project_id="$PROJECT" load --replace --source_format=PARQUET "$DATASET.$table" "$f"
done
echo "done: $(ls "$EXPORT_DIR"/*.parquet | wc -l) tables in $PROJECT:$DATASET"
