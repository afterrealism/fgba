# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

For the new `example/` app, the user chose a dependency-free Node.js server with browser JavaScript, reusing the repository's existing FGBA browser parser and test dataset. The interactive map uses deck.gl 9.4.0 with A5 cells rendered through `SolidPolygonLayer` and full-resolution features through `GeoJsonLayer`, read by FlatGeobuf 4.6.1.

## Users

Developers evaluating how FGBA vector data can be listed, fetched, and visualized from S3-like object storage.

## Product Purpose

Provide a small runnable example of an S3-style object browser that pulls vector datasets over HTTP byte ranges and visualizes both FGBA aggregates and full-resolution building geometries.

## Positioning

The example connects a real local object-storage flow to FGBA range-request streaming; it does not replace the vector data with a hard-coded visualization.

## Operating Context

Run locally from the FGBA repository, open the website in a browser, select the sample object, and inspect its visualization and transfer activity. No AWS account or credentials are required.

## Capabilities and Constraints

- Create the example under `example/`.
- Provide simple bucket and object listings plus S3-like object downloads with HTTP Range support.
- Reuse `testdata/cities_12.a.fgb` and the existing FGBA browser parser.
- Accept local vector uploads and convert them through the native CLI with analytics options.
- Run read-only SQL through the same native CLI and present results in the workspace.
- Use deck.gl for wheel, touch, button, and keyboard zoom, fit-to-data, and viewport-driven band loading with caching.
- Switch to actual FlatGeobuf building features when the viewport estimate is 50,000 or fewer, with a hard draw-limit fallback to aggregates.
- Convert the user's India buildings GeoPackage into a verified FGBA object containing 15,705,867 features.
- Keep the server dependency-free and the experience very simple.
- AWS authentication, the complete S3 API, and production deployment are not part of this example. Uploads, conversion, and SQL execution are local-only.

## Evidence on Hand

- `testdata/cities_12.a.fgb`: the repository's normative 12-city vector dataset.
- `crates/fgba/examples/wasm/main.js`: existing footer, directory, aggregate-band, and range-request logic.
- `crates/fgba-cli/src/serve.rs`: existing range-server behavior and security conventions.
- `/home/sheece/Downloads/hotosm_ind_buildings_osm_gpkg.zip`: the user's OpenStreetMap India buildings source archive (ODbL 1.0).
- `example/data/india-buildings.a.fgb`: the converted and verified India FGBA object, with five aggregate bands.

## Product Principles

- Demonstrate the real data flow end to end.
- Reuse the repository's format implementation rather than creating a parallel mock.
- Keep the example easy to run and inspect.
- Treat range-transfer visibility as part of the demonstration.
