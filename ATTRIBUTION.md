# Attribution

FGBA stands on these projects:

- **[FlatGeobuf](https://flatgeobuf.org)** — Björn Harrtell and contributors, BSD-2-Clause.
  The FGBA body is a valid FlatGeobuf file; the `flatgeobuf` crate is a dependency.
- **[A5](https://a5geo.org)** — Felix Palmer and contributors, Apache-2.0.
  Aggregate bands count features into A5 cells; the `a5` crate is a dependency.
  A5 cell IDs are frozen from v1.0.
- **[FGBO — FlatGeobuf Overviews](https://github.com/reearth/flatgeobuf-overviews)** — reearth,
  MIT OR Apache-2.0. The trailer conventions (sentinel, directory, 32-byte footer discovery,
  deterministic builds, the compatibility-testing approach) follow FGBO's design documents.
  Conventions reimplemented here; no FGBO code is vendored.
- **[Apache Arrow](https://arrow.apache.org)** — Apache-2.0. Its format specification and
  project website were the structural models for SPEC.md and this repository's documentation.

Experiments grounding the design were run with a scratch harness against the
[RustGIS](https://github.com/opengeos/rustgis) data crate; the harness is preserved at
`docs/reference/` for provenance and is not part of the build.
