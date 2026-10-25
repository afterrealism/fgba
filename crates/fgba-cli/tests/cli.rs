//! CLI integration tests: convert → build/verify/info/stats round-trips,
//! determinism, `serve` smoke over loopback HTTP, and bench — spawning the
//! real `fgba` binary via `CARGO_BIN_EXE_fgba`.

use flatgeobuf::geozero::{ColumnValue, PropertyProcessor};
use flatgeobuf::{ColumnType, FallibleStreamingIterator, FeatureProperties};
use std::collections::BTreeMap;
use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// The GeoJSON fixture: the normative vector's 12 points (SPEC Appendix A).
const CITIES_GEOJSON: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/cities.geojson");

/// The normative FGBA vector.
const CITIES_VECTOR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/cities_12.a.fgb");

fn fgba(args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_fgba")).args(args).output()
}

fn assert_ok(out: &Output) {
    assert!(
        out.status.success(),
        "expected success, got {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn assert_fail(out: &Output) {
    assert!(
        !out.status.success(),
        "expected failure, got success\nstdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}

/// A unique temp directory per test, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> std::io::Result<TempDir> {
        let dir = std::env::temp_dir().join(format!("fgba-cli-test-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        Ok(TempDir(dir))
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Kill the spawned server even when a test assertion fails.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// Convert the cities fixture and return the output path.
fn convert_cities(tmp: &TempDir) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let out = tmp.path("cities.a.fgb");
    let o = fgba(&["convert", CITIES_GEOJSON, "-o", &out.to_string_lossy()])?;
    assert_ok(&o);
    assert!(out.exists());
    Ok(out)
}

fn open_fgb(path: &Path) -> TestResult<flatgeobuf::FgbReader<BufReader<std::fs::File>>> {
    let file = std::fs::File::open(path)?;
    Ok(flatgeobuf::FgbReader::open(BufReader::new(file))?)
}

fn fgb_schema(path: &Path) -> TestResult<Vec<(String, ColumnType, bool)>> {
    let reader = open_fgb(path)?;
    let Some(columns) = reader.header().columns() else {
        return Ok(Vec::new());
    };
    Ok((0..columns.len())
        .map(|index| {
            let column = columns.get(index);
            (column.name().to_owned(), column.type_(), column.nullable())
        })
        .collect())
}

fn fgb_names(path: &Path) -> TestResult<Vec<String>> {
    let mut reader = open_fgb(path)?.select_all()?;
    let mut names = Vec::new();
    while let Some(feature) = reader.next()? {
        names.push(feature.property::<String>("name")?);
    }
    Ok(names)
}

#[test]
fn convert_preserves_geojson_names() -> TestResult {
    let tmp = TempDir::new("geojson-properties")?;
    let out = convert_cities(&tmp)?;
    assert_eq!(
        fgb_schema(&out)?,
        vec![("name".to_string(), ColumnType::String, false)]
    );
    let mut actual = fgb_names(&out)?;
    actual.sort();
    let mut expected: Vec<String> = (1..=12).map(|index| format!("city{index}")).collect();
    expected.sort();
    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn convert_preserves_geojson_types_and_nullability() -> TestResult {
    let tmp = TempDir::new("geojson-types")?;
    let input = tmp.path("types.geojson");
    std::fs::write(
        &input,
        r#"{
          "type": "FeatureCollection",
          "features": [
            {
              "type": "Feature",
              "properties": {
                "signed": -1,
                "unsigned": 9223372036854775808,
                "ordinary": 7,
                "double": 1.25,
                "bool": true,
                "text": "alpha",
                "array": [1, "x"],
                "object": {"b": 2, "a": 1},
                "nullable": "yes",
                "explicit": "set",
                "all_null": null
              },
              "geometry": {"type": "Point", "coordinates": [0, 0]}
            },
            {
              "type": "Feature",
              "properties": {
                "signed": 2,
                "ordinary": 8,
                "double": 2.5,
                "bool": false,
                "text": "beta",
                "array": [],
                "object": {},
                "explicit": null,
                "all_null": null
              },
              "geometry": {"type": "Point", "coordinates": [1, 1]}
            }
          ]
        }"#,
    )?;
    let out = tmp.path("types.a.fgb");
    let o = fgba(&[
        "convert",
        &input.to_string_lossy(),
        "-o",
        &out.to_string_lossy(),
    ])?;
    assert_ok(&o);

    let schema = fgb_schema(&out)?;
    let names: Vec<&str> = schema.iter().map(|(name, _, _)| name.as_str()).collect();
    assert!(
        names.windows(2).all(|pair| pair[0] <= pair[1]),
        "columns are not sorted: {names:?}"
    );
    let columns: BTreeMap<String, (ColumnType, bool)> = schema
        .into_iter()
        .map(|(name, kind, nullable)| (name, (kind, nullable)))
        .collect();
    assert_eq!(columns.len(), 11);
    for (name, expected) in [
        ("all_null", (ColumnType::String, true)),
        ("array", (ColumnType::Json, false)),
        ("bool", (ColumnType::Bool, false)),
        ("double", (ColumnType::Double, false)),
        ("explicit", (ColumnType::String, true)),
        ("nullable", (ColumnType::String, true)),
        ("object", (ColumnType::Json, false)),
        ("ordinary", (ColumnType::Long, false)),
        ("signed", (ColumnType::Long, false)),
        ("text", (ColumnType::String, false)),
        ("unsigned", (ColumnType::ULong, true)),
    ] {
        assert_eq!(columns.get(name), Some(&expected), "column {name}");
    }

    let mut reader = open_fgb(&out)?.select_all()?;
    let mut count = 0;
    while let Some(feature) = reader.next()? {
        count += 1;
        assert!(feature.property::<String>("all_null").is_err());
        match feature.property::<String>("text")?.as_str() {
            "alpha" => {
                assert_eq!(feature.property::<i64>("signed")?, -1);
                assert_eq!(feature.property::<i64>("ordinary")?, 7);
                assert_eq!(
                    feature.property::<u64>("unsigned")?,
                    9_223_372_036_854_775_808
                );
                assert_eq!(feature.property::<f64>("double")?, 1.25);
                assert!(feature.property::<bool>("bool")?);
                assert_eq!(feature.property::<String>("array")?, "[1,\"x\"]");
                assert_eq!(feature.property::<String>("object")?, "{\"a\":1,\"b\":2}");
                assert_eq!(feature.property::<String>("nullable")?, "yes");
                assert_eq!(feature.property::<String>("explicit")?, "set");
            }
            "beta" => {
                assert_eq!(feature.property::<i64>("signed")?, 2);
                assert_eq!(feature.property::<i64>("ordinary")?, 8);
                assert_eq!(feature.property::<f64>("double")?, 2.5);
                assert!(!feature.property::<bool>("bool")?);
                assert_eq!(feature.property::<String>("array")?, "[]");
                assert_eq!(feature.property::<String>("object")?, "{}");
                assert!(feature.property::<String>("nullable").is_err());
                assert!(feature.property::<String>("explicit").is_err());
                assert!(feature.property::<u64>("unsigned").is_err());
            }
            other => return Err(format!("unexpected text property {other:?}").into()),
        }
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn convert_verify_info_stats_roundtrip() -> TestResult {
    let tmp = TempDir::new("roundtrip")?;
    let out = convert_cities(&tmp)?;

    // Convert prints the report.
    let o = fgba(&["convert", CITIES_GEOJSON, "-o", &out.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("12 features, 0 skipped"), "{text}");
    assert!(text.contains("bands: 2"), "{text}");

    // Verify exits 0 with a summary.
    let o = fgba(&["verify", &out.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("footer + directory: CRC verified"), "{text}");
    assert!(text.contains("band res 2: 2 cells, 12 features"), "{text}");
    assert!(
        text.contains("OK: valid FGBA, 12 features, 1 band"),
        "{text}"
    );

    // Info reports FGBA, the body header and the band table.
    let o = fgba(&["info", &out.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("— FGBA"), "{text}");
    assert!(text.contains("12 features, Point"), "{text}");
    assert!(text.contains("res  2: 2 cells"), "{text}");

    // Stats over the world viewport at the default (deepest) band.
    let o = fgba(&["stats", &out.to_string_lossy(), "--bbox", "-180,-85,180,85"])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("res 2"), "{text}");
    assert!(text.contains("cells matched: 2"), "{text}");
    assert!(text.contains("estimated features: 12"), "{text}");

    // Stats over a region holding only the 8 European points.
    let o = fgba(&["stats", &out.to_string_lossy(), "--bbox", "-5,45,10,55"])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("estimated features: 8"));

    // Explicit res.
    let o = fgba(&[
        "stats",
        &out.to_string_lossy(),
        "--bbox",
        "-180,-85,180,85",
        "--res",
        "2",
    ])?;
    assert_ok(&o);
    Ok(())
}

#[test]
fn build_and_convert_are_deterministic() -> TestResult {
    let tmp = TempDir::new("determinism")?;
    let a = tmp.path("a.a.fgb");
    let b = tmp.path("b.a.fgb");
    for out in [&a, &b] {
        let o = fgba(&["convert", CITIES_GEOJSON, "-o", &out.to_string_lossy()])?;
        assert_ok(&o);
    }
    assert_eq!(
        std::fs::read(&a)?,
        std::fs::read(&b)?,
        "convert must be byte-identical on identical input"
    );

    // `build` on an FGBA input rebuilds from the body, byte-identically.
    let rebuilt = tmp.path("rebuilt.a.fgb");
    let o = fgba(&[
        "build",
        &a.to_string_lossy(),
        "-o",
        &rebuilt.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert!(stderr(&o).contains("already has an FGBA trailer"));
    assert_eq!(std::fs::read(&a)?, std::fs::read(&rebuilt)?);

    // `build` without -o defaults to <stem>.a.fgb next to the input.
    let plain = tmp.path("plain.fgb");
    let full = std::fs::read(&a)?;
    let reader = fgba::Reader::open_path(&a)?;
    let body_len = reader.body_len() as usize;
    std::fs::write(&plain, &full[..body_len])?;
    let o = fgba(&["build", &plain.to_string_lossy()])?;
    assert_ok(&o);
    assert_eq!(std::fs::read(tmp.path("plain.a.fgb"))?, std::fs::read(&a)?);
    Ok(())
}

#[test]
fn build_supports_same_path_and_hard_linked_output() -> TestResult {
    let tmp = TempDir::new("build-atomic-output")?;
    let converted = convert_cities(&tmp)?;
    let converted_bytes = std::fs::read(&converted)?;
    let body_len = fgba::Reader::open_path(&converted)?.body_len() as usize;
    let body = &converted_bytes[..body_len];

    // Establish the ordinary, separate-input/output build as the reference.
    let normal_input = tmp.path("normal.fgb");
    let normal_output = tmp.path("normal-output.a.fgb");
    std::fs::write(&normal_input, body)?;
    let o = fgba(&[
        "build",
        &normal_input.to_string_lossy(),
        "-o",
        &normal_output.to_string_lossy(),
    ])?;
    assert_ok(&o);
    let expected = std::fs::read(&normal_output)?;

    // Building a plain FlatGeobuf onto itself retains every body byte.
    let in_place = tmp.path("in-place.fgb");
    std::fs::write(&in_place, body)?;
    let in_place_arg = in_place.to_string_lossy();
    let o = fgba(&["build", &in_place_arg, "-o", &in_place_arg])?;
    assert_ok(&o);
    let rebuilt = std::fs::read(&in_place)?;
    assert!(rebuilt.starts_with(body), "in-place build truncated body");
    assert_eq!(rebuilt, expected);
    let o = fgba(&["verify", &in_place_arg])?;
    assert_ok(&o);

    // Rebuilding an existing FGBA in place is equivalent and deterministic.
    let o = fgba(&["build", &in_place_arg, "-o", &in_place_arg])?;
    assert_ok(&o);
    assert!(stderr(&o).contains("already has an FGBA trailer"));
    assert_eq!(std::fs::read(&in_place)?, expected);

    // Replacing one hard link must not truncate the input's original inode.
    let linked_input = tmp.path("linked.fgb");
    let linked_output = tmp.path("linked-output.a.fgb");
    std::fs::write(&linked_input, body)?;
    std::fs::hard_link(&linked_input, &linked_output)?;
    let o = fgba(&[
        "build",
        &linked_input.to_string_lossy(),
        "-o",
        &linked_output.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert_eq!(std::fs::read(&linked_input)?, body);
    assert_eq!(std::fs::read(&linked_output)?, expected);
    Ok(())
}

#[test]
fn analytics_flags_report_and_verify() -> TestResult {
    let tmp = TempDir::new("analytics")?;
    let out = tmp.path("analytics.a.fgb");
    let o = fgba(&[
        "convert",
        CITIES_GEOJSON,
        "--analytics",
        "--chunk-rows",
        "2",
        "-o",
        &out.to_string_lossy(),
    ])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("attribute columns: 1"), "{text}");
    let mut reader = fgba::Reader::open_path(&out)?;
    let analytics = reader
        .read_body_analytics_metadata()?
        .ok_or("missing BODY_ANALYTICS")?;
    assert_eq!(analytics.row_count, 12);
    assert_eq!(analytics.column_count, 1);
    assert_eq!(analytics.chunk_count, 6);
    assert!(!reader.read_arrow_chunk(0, 0)?.bytes.is_empty());
    let metrics = reader
        .read_geometry_metrics_metadata()?
        .ok_or("missing GEOMETRY_METRICS")?;
    assert_eq!(metrics.row_count, 12);
    assert_eq!(metrics.chunk_count, 6);
    assert_eq!(metrics.columns.len(), 7);
    assert!(!reader.read_geometry_arrow_chunk(0, 0)?.bytes.is_empty());

    let o = fgba(&["verify", &out.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(
        text.contains("analytics: 1 columns, 6 chunks — OK"),
        "{text}"
    );
    assert!(
        text.contains("geometry metrics: 7 columns, 6 chunks — OK"),
        "{text}"
    );

    let plain = tmp.path("plain.fgb");
    std::fs::write(&plain, &std::fs::read(&out)?[..reader.body_len() as usize])?;
    let rebuilt = tmp.path("rebuilt.a.fgb");
    let o = fgba(&[
        "build",
        &plain.to_string_lossy(),
        "--analytics",
        "--chunk-rows",
        "2",
        "-o",
        &rebuilt.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("attribute columns: 1"));
    assert_eq!(std::fs::read(&out)?, std::fs::read(&rebuilt)?);

    let rebuilt_again = tmp.path("rebuilt-again.a.fgb");
    let o = fgba(&[
        "build",
        &rebuilt.to_string_lossy(),
        "--analytics",
        "--chunk-rows",
        "2",
        "-o",
        &rebuilt_again.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert!(stderr(&o).contains("already has an FGBA trailer"));
    assert_eq!(std::fs::read(&out)?, std::fs::read(&rebuilt_again)?);
    let o = fgba(&["verify", &rebuilt_again.to_string_lossy()])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("analytics: 1 columns, 6 chunks — OK"));
    let o = fgba(&[
        "query",
        &rebuilt_again.to_string_lossy(),
        "SELECT COUNT(*) FROM features",
    ])?;
    assert_ok(&o);
    assert_eq!(stdout(&o), "count(*)\n12\n");

    let ordinary = tmp.path("ordinary.a.fgb");
    let o = fgba(&["convert", CITIES_GEOJSON, "-o", &ordinary.to_string_lossy()])?;
    assert_ok(&o);
    assert!(!stdout(&o).contains("attribute columns:"));
    let o = fgba(&["verify", &ordinary.to_string_lossy()])?;
    assert_ok(&o);
    assert!(!stdout(&o).contains("analytics:"));

    let property_free_body = tmp.path("property-free.fgb");
    let mut writer =
        flatgeobuf::FgbWriter::create("property-free", flatgeobuf::GeometryType::Point)?;
    writer.add_feature_geom(
        geo_types::Geometry::Point(geo_types::Point::new(0.0, 0.0)),
        |_| {},
    )?;
    let mut body = Vec::new();
    writer.write(&mut body)?;
    std::fs::write(&property_free_body, body)?;
    let property_free = tmp.path("property-free.a.fgb");
    let o = fgba(&[
        "build",
        &property_free_body.to_string_lossy(),
        "--analytics",
        "-o",
        &property_free.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert!(
        stdout(&o).contains("analytics: no property columns"),
        "{}",
        stdout(&o)
    );
    assert!(fgba::Reader::open_path(&property_free)?
        .read_body_analytics_metadata()?
        .is_none());
    assert!(fgba::Reader::open_path(&property_free)?
        .read_geometry_metrics_metadata()?
        .is_none());
    Ok(())
}

#[test]
fn verify_accepts_empty_geometry_and_retains_analytics_rows() -> TestResult {
    let tmp = TempDir::new("verify-empty-geometry")?;
    let mut writer =
        flatgeobuf::FgbWriter::create("empty-geometry", flatgeobuf::GeometryType::LineString)?;
    writer.add_column("name", ColumnType::String, |_, _| {});
    for (name, geometry) in [
        (
            "valid",
            geo_types::Geometry::LineString(geo_types::LineString::from(vec![
                (0.0, 0.0),
                (3.0, 4.0),
            ])),
        ),
        (
            "empty",
            geo_types::Geometry::LineString(geo_types::LineString::new(Vec::new())),
        ),
    ] {
        let mut property_error = None;
        writer.add_feature_geom(geometry, |feature| {
            if let Err(error) = feature.property(0, "name", &ColumnValue::String(name)) {
                property_error = Some(error);
            }
        })?;
        if let Some(error) = property_error {
            return Err(error.into());
        }
    }
    let mut body = Vec::new();
    writer.write(&mut body)?;
    let body_path = tmp.path("empty.fgb");
    std::fs::write(&body_path, body)?;

    let out = tmp.path("empty.a.fgb");
    let o = fgba(&[
        "build",
        &body_path.to_string_lossy(),
        "--analytics",
        "--chunk-rows",
        "1",
        "-o",
        &out.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("features: 1 (1 skipped)"));

    // The empty geometry remains a body/analytics row but is absent from kind 1.
    let mut reader = fgba::Reader::open_path(&out)?;
    assert_eq!(reader.read_band(2)?.total, 1);
    let analytics = reader
        .read_body_analytics_metadata()?
        .ok_or("missing BODY_ANALYTICS")?;
    assert_eq!(analytics.row_count, 2);
    assert_eq!(analytics.chunk_count, 2);
    assert_eq!(analytics.columns[0].global.non_null_count, 2);
    let metrics = reader
        .read_geometry_metrics_metadata()?
        .ok_or("missing GEOMETRY_METRICS")?;
    assert_eq!(metrics.row_count, 2);
    assert_eq!(metrics.chunk_count, 2);
    assert!(metrics
        .columns
        .iter()
        .all(|column| column.global.non_null_count == 1));

    let o = fgba(&["verify", &out.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("body: 2 features"), "{text}");
    assert!(
        text.contains("analytics: 1 columns, 2 chunks — OK"),
        "{text}"
    );
    assert!(
        text.contains("geometry metrics: 7 columns, 2 chunks — OK"),
        "{text}"
    );
    assert!(
        text.contains("OK: valid FGBA, 2 features, 1 band"),
        "{text}"
    );

    let o = fgba(&[
        "query",
        &out.to_string_lossy(),
        "SELECT name, geom_length, geom_area, geom_npoints, bbox_xmin FROM features ORDER BY name",
    ])?;
    assert_ok(&o);
    let text = stdout(&o);
    let rows = text.lines().collect::<Vec<_>>();
    assert_eq!(
        rows.first(),
        Some(&"name,geom_length,geom_area,geom_npoints,bbox_xmin")
    );
    assert_eq!(rows.len(), 3, "{text}");
    assert!(rows.contains(&"empty,,,,"), "{text}");
    let valid = rows
        .iter()
        .find(|row| row.starts_with("valid,"))
        .ok_or("missing valid geometry row")?;
    let values = valid.split(',').collect::<Vec<_>>();
    assert_eq!(values.len(), 5, "{valid}");
    assert_eq!(values[1].parse::<f64>()?, 5.0);
    assert_eq!(values[2].parse::<f64>()?, 0.0);
    assert_eq!(values[3].parse::<u32>()?, 2);
    assert_eq!(values[4].parse::<f64>()?, 0.0);
    Ok(())
}

#[test]
fn geometry_metrics_cli_opt_in_out_and_conflicts() -> TestResult {
    let tmp = TempDir::new("geometry-metrics-flags")?;
    let implied = tmp.path("implied.a.fgb");
    let o = fgba(&[
        "convert",
        CITIES_GEOJSON,
        "--analytics",
        "--chunk-rows",
        "2",
        "-o",
        &implied.to_string_lossy(),
    ])?;
    assert_ok(&o);
    let explicit = tmp.path("explicit.a.fgb");
    let o = fgba(&[
        "convert",
        CITIES_GEOJSON,
        "--geometry-metrics",
        "--chunk-rows",
        "2",
        "-o",
        &explicit.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert_eq!(std::fs::read(&implied)?, std::fs::read(&explicit)?);

    let reader = fgba::Reader::open_path(&implied)?;
    let plain = tmp.path("plain.fgb");
    std::fs::write(
        &plain,
        &std::fs::read(&implied)?[..reader.body_len() as usize],
    )?;
    let built = tmp.path("built.a.fgb");
    let o = fgba(&[
        "build",
        &plain.to_string_lossy(),
        "--geometry-metrics",
        "--chunk-rows",
        "2",
        "-o",
        &built.to_string_lossy(),
    ])?;
    assert_ok(&o);
    let mut reader = fgba::Reader::open_path(&built)?;
    assert!(reader.read_body_analytics_metadata()?.is_some());
    assert!(reader.read_geometry_metrics_metadata()?.is_some());
    assert_eq!(std::fs::read(&implied)?, std::fs::read(&built)?);

    let opted_out = tmp.path("opted-out.a.fgb");
    let o = fgba(&[
        "build",
        &plain.to_string_lossy(),
        "--analytics",
        "--no-geometry-metrics",
        "--chunk-rows",
        "2",
        "-o",
        &opted_out.to_string_lossy(),
    ])?;
    assert_ok(&o);
    let mut reader = fgba::Reader::open_path(&opted_out)?;
    assert!(reader.read_body_analytics_metadata()?.is_some());
    assert!(reader.read_geometry_metrics_metadata()?.is_none());

    let build_conflict = fgba(&[
        "build",
        &plain.to_string_lossy(),
        "--geometry-metrics",
        "--no-geometry-metrics",
        "-o",
        &tmp.path("build-conflict.a.fgb").to_string_lossy(),
    ])?;
    assert_fail(&build_conflict);
    assert!(stderr(&build_conflict).contains("cannot be used with"));
    let convert_conflict = fgba(&[
        "convert",
        CITIES_GEOJSON,
        "--geometry-metrics",
        "--no-geometry-metrics",
        "-o",
        &tmp.path("convert-conflict.a.fgb").to_string_lossy(),
    ])?;
    assert_fail(&convert_conflict);
    assert!(stderr(&convert_conflict).contains("cannot be used with"));
    Ok(())
}

#[test]
fn analytics_chunk_rows_support_default_auto_literal_auto_and_explicit() -> TestResult {
    let tmp = TempDir::new("analytics-chunk-rows")?;
    let automatic = tmp.path("automatic.a.fgb");
    let o = fgba(&[
        "convert",
        CITIES_GEOJSON,
        "--analytics",
        "-o",
        &automatic.to_string_lossy(),
    ])?;
    assert_ok(&o);
    let mut reader = fgba::Reader::open_path(&automatic)?;
    let analytics = reader
        .read_body_analytics_metadata()?
        .ok_or("missing BODY_ANALYTICS")?;
    assert_eq!(analytics.chunk_rows, 4096);
    assert_eq!(analytics.chunk_count, 1);

    let plain = tmp.path("plain.fgb");
    std::fs::write(
        &plain,
        &std::fs::read(&automatic)?[..reader.body_len() as usize],
    )?;
    let literal_auto = tmp.path("literal-auto.a.fgb");
    let o = fgba(&[
        "build",
        &plain.to_string_lossy(),
        "--analytics",
        "--chunk-rows",
        "auto",
        "-o",
        &literal_auto.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert_eq!(std::fs::read(&automatic)?, std::fs::read(&literal_auto)?);

    let explicit = tmp.path("explicit.a.fgb");
    let o = fgba(&[
        "build",
        &plain.to_string_lossy(),
        "--analytics",
        "--chunk-rows",
        "2",
        "-o",
        &explicit.to_string_lossy(),
    ])?;
    assert_ok(&o);
    let analytics = fgba::Reader::open_path(&explicit)?
        .read_body_analytics_metadata()?
        .ok_or("missing BODY_ANALYTICS")?;
    assert_eq!(analytics.chunk_rows, 2);
    assert_eq!(analytics.chunk_count, 6);

    let o = fgba(&[
        "build",
        &plain.to_string_lossy(),
        "--analytics",
        "--chunk-rows",
        "0",
        "-o",
        &tmp.path("invalid.a.fgb").to_string_lossy(),
    ])?;
    assert_fail(&o);
    assert!(stderr(&o).contains("positive"), "{}", stderr(&o));
    Ok(())
}

#[test]
fn verify_rejects_malformed_analytics_without_hiding_body_or_bands() -> TestResult {
    let tmp = TempDir::new("analytics-malformed")?;
    let out = tmp.path("analytics.a.fgb");
    let o = fgba(&[
        "convert",
        CITIES_GEOJSON,
        "--analytics",
        "--chunk-rows",
        "2",
        "-o",
        &out.to_string_lossy(),
    ])?;
    assert_ok(&o);
    let analytics = fgba::Reader::open_path(&out)?
        .read_body_analytics_metadata()?
        .ok_or("missing BODY_ANALYTICS")?;
    let (_, end) = analytics.arrow_range(0, 0)?;
    let mut bytes = std::fs::read(&out)?;
    bytes[end as usize - 6] ^= 0xFF;
    let bad = tmp.path("bad.a.fgb");
    std::fs::write(&bad, bytes)?;
    let o = fgba(&["verify", &bad.to_string_lossy()])?;
    assert_fail(&o);
    let text = stdout(&o);
    assert!(text.contains("body: 12 features"), "{text}");
    assert!(text.contains("band res 2"), "{text}");
    assert!(text.contains("FAIL analytics"), "{text}");

    let metrics = fgba::Reader::open_path(&out)?
        .read_geometry_metrics_metadata()?
        .ok_or("missing GEOMETRY_METRICS")?;
    let (_, end) = metrics.arrow_range(0, 0)?;
    let mut bytes = std::fs::read(&out)?;
    bytes[end as usize - 6] ^= 0xFF;
    let bad_geometry = tmp.path("bad-geometry.a.fgb");
    std::fs::write(&bad_geometry, bytes)?;
    let o = fgba(&["verify", &bad_geometry.to_string_lossy()])?;
    assert_fail(&o);
    let text = stdout(&o);
    assert!(text.contains("body: 12 features"), "{text}");
    assert!(text.contains("band res 2"), "{text}");
    assert!(
        text.contains("analytics: 1 columns, 6 chunks — OK"),
        "{text}"
    );
    assert!(text.contains("FAIL geometry metrics"), "{text}");
    Ok(())
}

#[test]
fn verify_names_failures_and_exits_1() -> TestResult {
    let tmp = TempDir::new("verify-fail")?;
    let good = convert_cities(&tmp)?;

    // A plain FlatGeobuf (no trailer): footer failure named, exit 1.
    let reader = fgba::Reader::open_path(&good)?;
    let body_len = reader.body_len() as usize;
    let plain = tmp.path("plain.fgb");
    std::fs::write(&plain, &std::fs::read(&good)?[..body_len])?;
    let o = fgba(&["verify", &plain.to_string_lossy()])?;
    assert_fail(&o);
    assert!(stdout(&o).contains("FAIL footer"), "{}", stdout(&o));
    assert!(stderr(&o).contains("check(s) failed"));

    // A corrupted trailer (one flipped directory byte): CRC failure named.
    let mut bytes = std::fs::read(&good)?;
    let at = bytes.len() - 33; // last directory byte
    bytes[at] ^= 0xFF;
    let bad = tmp.path("bad.a.fgb");
    std::fs::write(&bad, &bytes)?;
    let o = fgba(&["verify", &bad.to_string_lossy()])?;
    assert_fail(&o);
    assert!(stdout(&o).contains("CRC"), "{}", stdout(&o));
    Ok(())
}

#[test]
fn convert_reads_csv_xy_and_wkt() -> TestResult {
    let tmp = TempDir::new("csv")?;
    let csv_path = tmp.path("cities.csv");
    std::fs::write(
        &csv_path,
        "name,lon,lat\na,2.36,48.84\nb,-0.14,51.49\nc,-73.97,40.73\n",
    )?;
    let out = tmp.path("csv.a.fgb");
    let o = fgba(&[
        "convert",
        &csv_path.to_string_lossy(),
        "-o",
        &out.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("3 features"), "{}", stdout(&o));
    let o = fgba(&["verify", &out.to_string_lossy()])?;
    assert_ok(&o);
    assert_eq!(
        fgb_schema(&out)?,
        vec![
            ("lat".to_string(), ColumnType::Double, false),
            ("lon".to_string(), ColumnType::Double, false),
            ("name".to_string(), ColumnType::String, false),
        ]
    );
    let mut reader = open_fgb(&out)?.select_all()?;
    let mut count = 0;
    while let Some(feature) = reader.next()? {
        count += 1;
        let (lon, lat) = match feature.property::<String>("name")?.as_str() {
            "a" => (2.36, 48.84),
            "b" => (-0.14, 51.49),
            "c" => (-73.97, 40.73),
            other => return Err(format!("unexpected CSV name {other:?}").into()),
        };
        assert_eq!(feature.property::<f64>("lon")?, lon);
        assert_eq!(feature.property::<f64>("lat")?, lat);
    }
    assert_eq!(count, 3);

    let wkt_path = tmp.path("cities.tsv");
    std::fs::write(
        &wkt_path,
        "name\twkt\na\tPOINT (2.36 48.84)\nb\tPOINT (-0.14 51.49)\n",
    )?;
    let out = tmp.path("wkt.a.fgb");
    let o = fgba(&[
        "convert",
        &wkt_path.to_string_lossy(),
        "-o",
        &out.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("2 features"), "{}", stdout(&o));
    assert_eq!(
        fgb_schema(&out)?,
        vec![("name".to_string(), ColumnType::String, false)]
    );
    let mut names = fgb_names(&out)?;
    names.sort();
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);

    // A column-less table fails with a readable error.
    let bad_path = tmp.path("bad.csv");
    std::fs::write(&bad_path, "a,b\n1,2\n")?;
    let o = fgba(&[
        "convert",
        &bad_path.to_string_lossy(),
        "-o",
        &tmp.path("bad.a.fgb").to_string_lossy(),
    ])?;
    assert_fail(&o);
    assert!(stderr(&o).contains("no geometry column"), "{}", stderr(&o));
    Ok(())
}

#[test]
fn convert_reads_shapefile_points() -> TestResult {
    let tmp = TempDir::new("shp")?;
    let shp = tmp.path("cities.shp");
    let mut writer = shapefile::ShapeWriter::from_path(&shp)?;
    for &(x, y) in &[(2.36, 48.84), (-0.14, 51.49), (-73.97, 40.73)] {
        writer.write_shape(&shapefile::Point::new(x, y))?;
    }
    drop(writer);
    assert!(!shp.with_extension("dbf").exists());

    let out = tmp.path("shp.a.fgb");
    let o = fgba(&[
        "convert",
        &shp.to_string_lossy(),
        "-o",
        &out.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("3 features"), "{}", stdout(&o));
    let o = fgba(&["verify", &out.to_string_lossy()])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("3 features"), "{}", stdout(&o));
    assert!(fgb_schema(&out)?.is_empty());
    let o = fgba(&["stats", &out.to_string_lossy(), "--bbox", "-180,-85,180,85"])?;
    assert_ok(&o);
    assert!(
        stdout(&o).contains("estimated features: 3"),
        "{}",
        stdout(&o)
    );
    Ok(())
}

#[test]
fn convert_reads_shapefile_dbf_properties() -> TestResult {
    let tmp = TempDir::new("shp-dbf")?;
    let shp = tmp.path("places.shp");
    let mut writer = shapefile::ShapeWriter::from_path(&shp)?;
    writer.write_shape(&shapefile::Point::new(2.36, 48.84))?;
    drop(writer);

    let mut table = shapefile::dbase::TableWriterBuilder::new()
        .add_character_field(shapefile::dbase::FieldName::try_from("name")?, 32)
        .add_numeric_field(shapefile::dbase::FieldName::try_from("height")?, 10, 2)
        .build_with_file_dest(shp.with_extension("dbf"))?;
    let mut record = shapefile::dbase::Record::default();
    assert!(record
        .insert(
            "name".to_string(),
            shapefile::dbase::FieldValue::Character(Some("alpha".to_string())),
        )
        .is_none());
    assert!(record
        .insert(
            "height".to_string(),
            shapefile::dbase::FieldValue::Numeric(Some(12.5)),
        )
        .is_none());
    table.write_record(&record)?;
    drop(table);

    let out = tmp.path("places.a.fgb");
    let o = fgba(&[
        "convert",
        &shp.to_string_lossy(),
        "-o",
        &out.to_string_lossy(),
    ])?;
    assert_ok(&o);
    assert_eq!(
        fgb_schema(&out)?,
        vec![
            ("height".to_string(), ColumnType::Double, false),
            ("name".to_string(), ColumnType::String, false),
        ]
    );
    let mut reader = open_fgb(&out)?.select_all()?;
    let mut count = 0;
    while let Some(feature) = reader.next()? {
        count += 1;
        assert_eq!(feature.property::<String>("name")?, "alpha");
        assert_eq!(feature.property::<f64>("height")?, 12.5);
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn convert_rejects_property_type_conflicts() -> TestResult {
    let tmp = TempDir::new("type-conflict")?;
    let template = r#"{
      "type": "FeatureCollection",
      "features": [
        {"type": "Feature", "properties": {"value": FIRST}, "geometry": {"type": "Point", "coordinates": [0, 0]}},
        {"type": "Feature", "properties": {"value": SECOND}, "geometry": {"type": "Point", "coordinates": [1, 1]}}
      ]
    }"#;
    for (tag, first, second) in [
        ("string-numeric", "1", "\"one\""),
        ("signed-unsigned", "1", "9223372036854775808"),
        ("integer-double", "1", "1.5"),
    ] {
        let input = tmp.path(&format!("{tag}.geojson"));
        std::fs::write(
            &input,
            template.replace("FIRST", first).replace("SECOND", second),
        )?;
        let o = fgba(&[
            "convert",
            &input.to_string_lossy(),
            "-o",
            &tmp.path(&format!("{tag}.a.fgb")).to_string_lossy(),
        ])?;
        assert_fail(&o);
        let error = stderr(&o);
        assert!(error.contains("type conflict"), "{tag}: {error}");
        assert!(error.contains("\"value\""), "{tag}: {error}");
    }
    Ok(())
}

#[test]
fn convert_rejects_reserved_and_duplicate_property_names() -> TestResult {
    let tmp = TempDir::new("property-names")?;
    let template = r#"{
      "type": "Feature",
      "properties": {"RESERVED": "x"},
      "geometry": {"type": "Point", "coordinates": [0, 0]}
    }"#;
    for (index, name) in [
        "geom_wkb",
        "bbox_xmin",
        "bbox_ymin",
        "bbox_xmax",
        "BBOX_YMAX",
    ]
    .into_iter()
    .enumerate()
    {
        let input = tmp.path(&format!("reserved-{index}.geojson"));
        std::fs::write(&input, template.replace("RESERVED", name))?;
        let o = fgba(&[
            "convert",
            &input.to_string_lossy(),
            "-o",
            &tmp.path(&format!("reserved-{index}.a.fgb"))
                .to_string_lossy(),
        ])?;
        assert_fail(&o);
        let error = stderr(&o);
        assert!(error.contains("reserved"), "{name}: {error}");
        assert!(error.contains(name), "{name}: {error}");
    }

    let case_duplicate = tmp.path("case-duplicate.geojson");
    std::fs::write(
        &case_duplicate,
        r#"{
          "type": "Feature",
          "properties": {"Name": "a", "name": "b"},
          "geometry": {"type": "Point", "coordinates": [0, 0]}
        }"#,
    )?;
    let o = fgba(&[
        "convert",
        &case_duplicate.to_string_lossy(),
        "-o",
        &tmp.path("case-duplicate.a.fgb").to_string_lossy(),
    ])?;
    assert_fail(&o);
    assert!(stderr(&o).contains("ignoring case"), "{}", stderr(&o));

    let exact_duplicate = tmp.path("exact-duplicate.csv");
    std::fs::write(&exact_duplicate, "name,name,lon,lat\na,b,0,0\n")?;
    let o = fgba(&[
        "convert",
        &exact_duplicate.to_string_lossy(),
        "-o",
        &tmp.path("exact-duplicate.a.fgb").to_string_lossy(),
    ])?;
    assert_fail(&o);
    let error = stderr(&o);
    assert!(error.contains("duplicate property names"), "{error}");
    assert!(error.contains("name"), "{error}");
    Ok(())
}

#[test]
fn convert_names_unimplemented_formats() -> TestResult {
    let tmp = TempDir::new("not-yet")?;
    for (name, want) in [
        ("x.parquet", "GeoParquet input is not implemented yet"),
        ("x.gpkg", "GeoPackage input is not implemented yet"),
        ("x.shx", "cannot detect input format"),
    ] {
        let input = tmp.path(name);
        std::fs::write(&input, b"")?;
        let o = fgba(&[
            "convert",
            &input.to_string_lossy(),
            "-o",
            &tmp.path("o.a.fgb").to_string_lossy(),
        ])?;
        assert_fail(&o);
        assert!(stderr(&o).contains(want), "{name}: {}", stderr(&o));
    }

    let o = fgba(&["convert", "--help"])?;
    assert_ok(&o);
    let help = stdout(&o);
    for want in ["properties preserved", "GeoParquet", "GeoPackage"] {
        assert!(help.contains(want), "missing {want:?} in help:\n{help}");
    }
    Ok(())
}

/// A free loopback port (best effort: released before the server binds it).
fn free_port() -> std::io::Result<u16> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

#[test]
fn serve_smoke_ranges_traversal_and_fgba_reads() -> TestResult {
    let tmp = TempDir::new("serve")?;
    convert_cities(&tmp)?;
    let port = free_port()?;

    let child = Command::new(env!("CARGO_BIN_EXE_fgba"))
        .args([
            "serve",
            &tmp.0.to_string_lossy(),
            "--port",
            &port.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let _server = Server(child);

    // The fgba HTTP reader drives the server: retry until it is up.
    let url = format!("http://127.0.0.1:{port}/cities.a.fgb");
    let mut opened = None;
    for _ in 0..50 {
        match fgba::Reader::open_url(&url) {
            Ok(reader) => {
                opened = Some(reader);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    let mut reader = match opened {
        Some(r) => r,
        None => return Err(format!("server never came up on :{port}").into()),
    };
    assert_eq!(reader.bands().len(), 1);
    let stats = reader.stats([-180.0, -85.0, 180.0, 85.0], 2)?;
    assert_eq!(stats.total, 12);
    assert_eq!(stats.cells, 2);

    // Path traversal is refused with 403.
    let status = response_status(port, "GET /../../Cargo.toml HTTP/1.1\r\nHost: x\r\n\r\n")?;
    assert_eq!(status, "HTTP/1.1 403 Forbidden");
    Ok(())
}

/// One raw HTTP request, returning the response status line.
fn response_status(port: u16, request: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.write_all(request.as_bytes())?;
    let mut buf = Vec::new();
    stream.take(4096).read_to_end(&mut buf)?;
    Ok(String::from_utf8_lossy(&buf)
        .lines()
        .next()
        .unwrap_or("")
        .to_string())
}

#[test]
fn serve_status_codes() -> TestResult {
    let tmp = TempDir::new("serve-status")?;
    convert_cities(&tmp)?;
    let port = free_port()?;
    let child = Command::new(env!("CARGO_BIN_EXE_fgba"))
        .args([
            "serve",
            &tmp.0.to_string_lossy(),
            "--port",
            &port.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let _server = Server(child);

    // Wait until the port accepts connections.
    let mut up = false;
    for _ in 0..50 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            up = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(up, "server never came up on :{port}");

    let status = response_status(port, "GET /../../Cargo.toml HTTP/1.1\r\nHost: x\r\n\r\n")?;
    assert_eq!(status, "HTTP/1.1 403 Forbidden");
    let status = response_status(
        port,
        "GET /cities.a.fgb HTTP/1.1\r\nHost: x\r\nRange: bytes=0-0\r\n\r\n",
    )?;
    assert_eq!(status, "HTTP/1.1 206 Partial Content");
    let status = response_status(port, "GET /nope.fgb HTTP/1.1\r\nHost: x\r\n\r\n")?;
    assert_eq!(status, "HTTP/1.1 404 Not Found");
    let status = response_status(
        port,
        "GET /cities.a.fgb HTTP/1.1\r\nHost: x\r\nRange: bytes=99999-100000\r\n\r\n",
    )?;
    assert_eq!(status, "HTTP/1.1 416 Range Not Satisfiable");
    let status = response_status(port, "POST /cities.a.fgb HTTP/1.1\r\nHost: x\r\n\r\n")?;
    assert_eq!(status, "HTTP/1.1 405 Method Not Allowed");
    Ok(())
}

#[test]
fn bench_runs_on_a_small_file() -> TestResult {
    let tmp = TempDir::new("bench")?;
    let out = convert_cities(&tmp)?;
    let o = fgba(&["bench", &out.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("band res"), "{text}");
    assert!(text.contains("         2"), "{text}"); // the res-2 band row
    Ok(())
}

#[test]
fn info_accepts_plain_fgb() -> TestResult {
    let tmp = TempDir::new("plain-info")?;
    let good = convert_cities(&tmp)?;
    let reader = fgba::Reader::open_path(&good)?;
    let body_len = reader.body_len() as usize;
    let plain = tmp.path("plain.fgb");
    std::fs::write(&plain, &std::fs::read(&good)?[..body_len])?;
    let o = fgba(&["info", &plain.to_string_lossy()])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("plain FlatGeobuf"), "{text}");
    assert!(text.contains("12 features, Point"), "{text}");
    Ok(())
}

#[test]
fn info_and_bench_require_input_or_url() -> TestResult {
    let o = fgba(&["info"])?;
    assert_fail(&o);
    assert!(stderr(&o).contains("input file or --url"), "{}", stderr(&o));
    let o = fgba(&["bench"])?;
    assert_fail(&o);
    Ok(())
}

/// The normative vector passes verify and info unchanged.
#[test]
fn normative_vector_roundtrip() -> TestResult {
    assert!(Path::new(CITIES_VECTOR).exists(), "missing {CITIES_VECTOR}");
    let o = fgba(&["verify", CITIES_VECTOR])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("OK: valid FGBA, 12 features, 1 band"));
    let o = fgba(&["info", CITIES_VECTOR])?;
    assert_ok(&o);
    let text = stdout(&o);
    assert!(text.contains("body 1.6 KiB"), "{text}"); // 1672 B body, SPEC Appendix A
    assert!(text.contains("trailer 105 B"), "{text}"); // 105 B trailer, SPEC Appendix A
    assert!(text.contains("res  2: 2 cells, 44 B"), "{text}");
    Ok(())
}

#[test]
fn query_exact_attributes_and_viewport() -> TestResult {
    let o = fgba(&["query", CITIES_VECTOR, "SELECT COUNT(*) FROM features"])?;
    assert_ok(&o);
    assert_eq!(stdout(&o), "count(*)\n12\n");
    let o = fgba(&[
        "query",
        CITIES_VECTOR,
        "--bbox",
        "-10,40,10,60",
        "SELECT COUNT(*) AS cells, SUM(count) AS estimated FROM a5_viewport_approx",
    ])?;
    assert_ok(&o);
    assert_eq!(stdout(&o), "cells,estimated\n1,8\n");
    let o = fgba(&[
        "query",
        CITIES_VECTOR,
        "SELECT SUM(count) AS total FROM a5_band_2",
    ])?;
    assert_ok(&o);
    assert_eq!(stdout(&o), "total\n12\n");

    let tmp = TempDir::new("query-attributes")?;
    let out = tmp.path("cities.a.fgb");
    let o = fgba(&[
        "convert",
        CITIES_GEOJSON,
        "--analytics",
        "--chunk-rows",
        "2",
        "-o",
        &out.to_string_lossy(),
    ])?;
    assert_ok(&o);
    let o = fgba(&[
        "query",
        &out.to_string_lossy(),
        "SELECT name FROM features WHERE name='city1'",
    ])?;
    assert_ok(&o);
    assert_eq!(stdout(&o), "name\ncity1\n");
    Ok(())
}

#[test]
fn query_rejects_invalid_input_and_write_statements() -> TestResult {
    let o = fgba(&["query", CITIES_VECTOR, ""])?;
    assert_fail(&o);
    assert!(
        stderr(&o).contains("SQL must not be empty"),
        "{}",
        stderr(&o)
    );
    let o = fgba(&[
        "query",
        CITIES_VECTOR,
        "--res",
        "2",
        "SELECT * FROM a5_viewport_approx",
    ])?;
    assert_fail(&o);
    assert!(
        stderr(&o).contains("--res requires --bbox"),
        "{}",
        stderr(&o)
    );
    let o = fgba(&["query", CITIES_VECTOR, "CREATE TABLE x (a BIGINT)"])?;
    assert_fail(&o);
    let error = stderr(&o);
    assert!(error.contains("DDL not supported"), "{error}");
    assert_eq!(
        error.lines().count(),
        1,
        "error should be one line: {error}"
    );
    let o = fgba(&["query", CITIES_VECTOR, "EXPLAIN SELECT * FROM features"])?;
    assert_ok(&o);
    assert!(stdout(&o).contains("plan_type,plan"), "{}", stdout(&o));
    Ok(())
}

#[test]
fn query_scan_threads_execute_and_require_positive_values() -> TestResult {
    let tmp = TempDir::new("query-scan-threads")?;
    let out = tmp.path("cities.a.fgb");
    let o = fgba(&[
        "convert",
        CITIES_GEOJSON,
        "--analytics",
        "--chunk-rows",
        "1",
        "-o",
        &out.to_string_lossy(),
    ])?;
    assert_ok(&o);

    let sql = "SELECT name FROM features ORDER BY name";
    let mut names = (1..=12)
        .map(|index| format!("city{index}"))
        .collect::<Vec<_>>();
    names.sort();
    let expected = format!("name\n{}\n", names.join("\n"));
    for threads in ["1", "4"] {
        let o = fgba(&[
            "query",
            &out.to_string_lossy(),
            "--scan-threads",
            threads,
            sql,
        ])?;
        assert_ok(&o);
        assert_eq!(stdout(&o), expected, "scan threads {threads}");
    }

    let o = fgba(&["query", &out.to_string_lossy(), "--scan-threads", "0", sql])?;
    assert_fail(&o);
    assert!(stderr(&o).contains("positive"), "{}", stderr(&o));
    Ok(())
}
