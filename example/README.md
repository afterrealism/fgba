# FGBA Range Lab

A local S3-style vector workspace. It lists local buckets and objects, converts uploaded vector files to FGBA through the native CLI, downloads FGBA data with HTTP byte ranges, renders counted A5 aggregate cells and full-resolution geometries in an interactive **deck.gl 9.4.0** map, and runs read-only SQL through `fgba query`.

## Run

Requires Node.js 20 or newer. There are no npm dependencies to install. Build the native CLI once for upload conversion and SQL execution:

```sh
cargo build -p fgba-cli
cd example
npm start
```

Open **http://127.0.0.1:8090**. To use another port:

```sh
PORT=3000 npm start
```

The page, deck.gl/A5 libraries, parser, and generated data are served locally. No AWS account, credentials, internet connection, or frontend build step is needed. WebGL is required for the map. Set `FGBA_CLI=/path/to/fgba` if the CLI is not at `target/debug/fgba`, `target/release/fgba`, or on `PATH`.

## Upload, convert, and query

1. Choose **Upload vector**. Select GeoJSON (`.geojson`/`.json`), CSV/TSV, FlatGeobuf (`.fgb`/`.a.fgb`), or a Shapefile `.shp` with its `.shx` sidecar. Select `.dbf` as well to retain attributes; `.prj` is accepted when present.
2. Set a new `.a.fgb` object name, choose rows per analytical chunk (default **4096**), and keep **Build attribute analytics** enabled for global statistics, zone maps, and Arrow-backed SQL scans. Conversion preserves properties and requires WGS 84 lon/lat coordinates; it does not reproject.
3. Choose **Convert to FGBA**. The server calls the real `fgba convert` command, stores the result in the `uploads` bucket, and opens it on the existing map. Existing objects are never overwritten.
4. Choose **SQL query** to run read-only SQL against the selected object. The workspace includes exact row/count examples, `attribute_stats`, and optional `bbox`/`res` controls for `a5_viewport_approx`. Results are rendered as a table; use `LIMIT` for large results (the server caps query output at 8 MiB).

Uploads accept at most 8 files and 512 MiB per request by default. Conversion and query commands run without a shell and have a 120-second timeout. Set `PORT` as usual; the upload root is `example/data/uploads` and is gitignored.

## Zoom and data flow

- Use mouse wheel, double click, pinch, the `+`/`-` keys, the on-screen zoom buttons, or fit-to-data.
- The map uses deck.gl's `MapView` controller, `SolidPolygonLayer` for aggregate cells, and `GeoJsonLayer` for actual building geometry. Arrow keys pan; the canvas is keyboard-focusable.
- Footer and directory are fetched once per object. Each newly needed A5 band is fetched once and cached. Reusing a cached band costs **0 requests**.
- Viewport queries use overlapping A5 cells and the 48px readability ladder. Feature totals are estimates for the current viewport, not claims about the full object.
- When the estimate is **50,000 features or fewer**, the map loads actual polygons, lines, and points from the FlatGeobuf body with `deserialize(url, { rect })`. The **Buildings** metric is the exact number loaded; exceeding the draw limit falls back to aggregates.
- Body reads are serialized with band reads and recorded as **FGB body** in the range ledger, alongside their actual payload sizes. Camera changes immediately invalidate stale reads, and pending viewport updates coalesce to the newest view.
- The range ledger records every request, response status, byte span, and cumulative payload.

The `india-buildings` bucket opens by default when `example/data/india-buildings.a.fgb` exists. Otherwise, `vector-datasets` opens the small `cities_12.a.fgb` reference vector.

## India buildings dataset

The generated object contains **15,705,867 features** from the user's `hotosm_ind_buildings_osm_gpkg.zip` (5.93 GB GeoPackage inside a 1.66 GB ZIP). The final FGBA object is 3,088,964,297 bytes, with five aggregate bands (resolutions 3, 5, 7, 9, 11) and a 1.2 MiB trailer. Conversion and verification have already been run locally; the intermediate `.fgb` was removed. The existing object has only **`OGC_FID` and `geom`**; other source attributes were not recovered and require the original dataset.

Source: **OpenStreetMap contributors**, snapshot **2026-10-09**, licensed **ODbL 1.0**. See [OpenStreetMap copyright](https://www.openstreetmap.org/copyright) and the [ODbL license](https://opendatacommons.org/licenses/odbl/1-0/).

### Rebuild the object

[DuckDB Spatial's bundled GDAL integration](https://duckdb.org/docs/1.3/core_extensions/spatial/gdal) reads the ZIP directly and writes [FlatGeobuf with a spatial index](https://gdal.org/en/stable/drivers/vector/flatgeobuf.html):

```sh
mkdir -p example/data
duckdb -c "LOAD spatial; SET geometry_always_xy = true; COPY (SELECT * FROM ST_Read('/vsizip//home/sheece/Downloads/hotosm_ind_buildings_osm_gpkg.zip/buildings.gpkg')) TO '/home/sheece/Desktop/work/fgba/example/data/india-buildings.fgb' WITH (FORMAT gdal, DRIVER 'FlatGeobuf', LAYER_CREATION_OPTIONS 'SPATIAL_INDEX=YES', SRS 'EPSG:4326');"
cargo run -p fgba-cli -- build example/data/india-buildings.fgb -o example/data/india-buildings.a.fgb
cargo run -p fgba-cli -- verify example/data/india-buildings.a.fgb
# Optional after verification: rm example/data/india-buildings.fgb
```

`example/data/*.fgb` is gitignored, so rebuilding may be necessary on a fresh checkout. The original ZIP is not modified.

The native reader works against the same local object URL:

```sh
target/debug/fgba info --url http://127.0.0.1:8090/s3/india-buildings/india-buildings.a.fgb
```

## Storage API

- `POST /api/uploads` — multipart vector upload plus `analytics`, `chunkRows`, and optional `outputName`; converts through the native CLI into the `uploads` bucket.
- `POST /api/query` — JSON `{ bucket, key, sql, bbox?, res? }`; runs read-only SQL through the native CLI and returns CSV.
- `GET /api/buckets` — simple JSON bucket listing for the interface.
- `GET /api/buckets/:bucket/objects` — object metadata and download URLs.
- `GET /s3/:bucket/:key` — complete object download, or a single range with `206 Partial Content` and `Content-Range`.
- `HEAD` and unsatisfiable-range `416` responses are supported. Paths are canonicalized and traversal is rejected.

This is intentionally a tiny local S3-style workspace, not a complete S3 API: there is no authentication, XML API, production hardening, or cloud deployment. Uploads and conversion are local-only; SQL remains read-only.

The browser parser is reused from `crates/fgba/examples/wasm/main.js`. It is a pure-JavaScript demo parser, not the standalone `fgba-wasm` artifact; the `/api/query` and other routes above are Range Lab demo contracts, not Tilify's direct-browser API. Pinned local assets are `a5-js@1.0.0`, `deck.gl@9.4.0`, and `flatgeobuf@4.6.1`, with their licenses in `vendor/`. Relevant API references: [deck.gl Controller](https://deck.gl/docs/api-reference/core/controller), [SolidPolygonLayer](https://deck.gl/docs/api-reference/layers/solid-polygon-layer), [GeoJsonLayer](https://deck.gl/docs/api-reference/layers/geojson-layer), [WebMercatorViewport](https://deck.gl/docs/api-reference/core/web-mercator-viewport), and [FlatGeobuf's GeoJSON API](https://unpkg.com/flatgeobuf@4.6.1/lib/mjs/geojson.d.ts).

## Check

```sh
npm test
```

Server tests cover bucket/object discovery, upload conversion, real SQL execution, validation failures, exact range bytes, full and HEAD downloads, error responses, traversal rejection, and static asset serving. Browser checks cover upload → map → SQL, wide-viewport band coverage, query errors, keyboard controls, band switching/caching, full-resolution geometry, and desktop/mobile rendering.
