#!/usr/bin/env bash
# Convert one source vector file into all 6 benchmark formats.
# Usage: convert-one.sh <source> <outdir-root> [name]
# Outputs: <root>/{geojson,shp,fgb,a.fgb,parquet,duckdb}/<name>.<ext>
set -u
SRC="$1"; ROOT="$2"
NAME="${3:-$(basename "$SRC" | sed 's/\.[^.]*$//' | sed 's/ /_/g')}"
FGBA_BIN="${FGBA_BIN:-$HOME/Desktop/work/fgba/target/release/fgba}"

mkdir -p "$ROOT"/{geojson,shp,fgb,a.fgb,parquet,duckdb}
GJ="$ROOT/geojson/$NAME.geojson"; SHP="$ROOT/shp/$NAME.shp"
FGB="$ROOT/fgb/$NAME.fgb"; AFG="$ROOT/a.fgb/$NAME.a.fgb"
PQ="$ROOT/parquet/$NAME.parquet"; DDBF="$ROOT/duckdb/$NAME.duckdb"

step() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# One duckdb invocation per target format; XY axis order pinned.
readsql() { echo "LOAD spatial; SET geometry_always_xy=true; COPY (SELECT * FROM st_read('$SRC')) TO '$1' $2;"; }

if [[ "$SRC" == *.gpkg ]]; then
  step "geojson $NAME";    duckdb -c "$(readsql "$GJ" '(FORMAT GDAL, DRIVER '\''GeoJSON'\'')')" || echo "FAIL geojson $NAME"
  step "shapefile $NAME";  duckdb -c "$(readsql "$SHP" '(FORMAT GDAL, DRIVER '\''ESRI Shapefile'\'')')" || echo "FAIL shp $NAME"
  step "flatgeobuf $NAME"; duckdb -c "$(readsql "$FGB" '(FORMAT GDAL, DRIVER '\''FlatGeobuf'\'')')" || echo "FAIL fgb $NAME"
else
  # GeoJSON source: use the source itself as the geojson-format artifact.
  [[ -e "$GJ" ]] || ln "$SRC" "$GJ"
  step "shapefile $NAME";  duckdb -c "$(readsql "$SHP" '(FORMAT GDAL, DRIVER '\''ESRI Shapefile'\'')')" || echo "FAIL shp $NAME"
  step "flatgeobuf $NAME"; duckdb -c "$(readsql "$FGB" '(FORMAT GDAL, DRIVER '\''FlatGeobuf'\'')')" || echo "FAIL fgb $NAME"
fi
step "geoparquet $NAME"; duckdb -c "LOAD spatial; SET geometry_always_xy=true; COPY (SELECT * FROM st_read('$SRC')) TO '$PQ' (FORMAT parquet, COMPRESSION zstd);" || echo "FAIL parquet $NAME"
step "duckdb $NAME"; rm -f "$DDBF"; duckdb "$DDBF" -c "LOAD spatial; SET geometry_always_xy=true; CREATE OR REPLACE TABLE features AS SELECT * FROM st_read('$SRC');" || echo "FAIL duckdb $NAME"
step "fgba trailer $NAME"; "$FGBA_BIN" build "$FGB" -o "$AFG" --analytics --chunk-rows auto || echo "FAIL a.fgb $NAME"
step "done $NAME"
