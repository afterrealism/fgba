# Manual pure-JavaScript FGBA browser demo

> **Not the standalone WASM artifact.** This historical example implements a
> small parser in JavaScript. The actual Rust/WASM visualization and SQL package
> is `crates/fgba-wasm`; see `docs/tilify-integration.md`. The checklist below
> is a manual demo, not an automated browser/WASM acceptance suite.

A minimal, dependency-free-**parsing** browser demo of the FGBA two-phase read
pattern (SPEC §9.3), proving that aggregate cells stream off plain static
hosting with zero server-side code:

1. fetch the file's last 32 bytes → parse the footer (this doubles as the
   range-support probe);
2. fetch the directory → verify its CRC-32, read the band table;
3. fetch the deepest band that fits the 8 MiB cap, whole → parse records;
4. compute the world-viewport covering with the official
   [a5-js](https://github.com/felixpalmer/a5) grid library (loaded from CDN via
   the import map in `index.html` — the same library family as the Rust `a5`
   crate the format is built on), binary-search the curve-sorted records, and
   draw the counted pentagons on a 2D canvas.

Very wide viewports are evaluated in longitude panels of at most 90° and
unioned by cell ID, avoiding a broad-polygon covering miss in the pinned A5
implementation. Narrow viewports use the ordinary single-covering path.

**Honest scope:** this is a directory/band parser demo, not a port of the
`fgba` crate. Footer/directory/band parsing and the CRC are pure byte work in
`main.js`; only the A5 grid math is imported. There is no build step and no
map library.

## Manual demo checklist

The page and the data must share an origin (the `Range` header triggers a CORS
preflight otherwise, SPEC §9.2). From the repository root:

```sh
cargo run -p fgba-cli -- serve .          # range-capable static server on :8080
# open http://127.0.0.1:8080/crates/fgba/examples/wasm/index.html
```

The page loads `testdata/cities_12.a.fgb` by default and reports **3 range
requests** (footer, directory, band). Point it at any other `.a.fgb` with
`?url=` or the input box — e.g. serve a single file with
`fgba serve testdata/cities_12.a.fgb` and open the page with
`?url=/anything` (a single-file serve answers every path).

For fully offline use, copy `a5-js`'s `dist/a5.js` next to this page and point
the import map at `./a5.js`. Any static host works for the data as long as it
answers `206 Partial Content` (S3, R2, nginx); the demo says so when a host
does not.
