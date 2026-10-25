//! format-bench: analytical + visualization workload benchmarks across vector formats.
//!
//! Usage:
//!   format-bench extent  <fgb-path>                       -> prints "minx miny maxx maxy"
//!   format-bench run-one <root> <dataset> <format> <workload> <minx> <miny> <maxx> <maxy>
//!                                                          -> prints one JSON result line
//!   format-bench orchestrate <root> <results.jsonl> [--formats fgb,afgba-sql]
//!                                                          -> writes JSONL result rows
//!   format-bench report <results.jsonl>                   -> markdown comparison tables
//!
//! Formats: fgb, afgba, afgba-sql, geojson, shp, duckdb, parquet.
//! afgba-sql supports the four analytical workloads only and executes
//! target/release/fgba by default; set FGBA_BIN to override that binary.
//! Workloads:
//!   full_count      analytical: full-scan feature count
//!   geom_agg        analytical: full scan, SUM(Euclidean length + area)
//!   attr_group      analytical: GROUP BY first string column, top-10 group count sum
//!   bbox_count      analytical: count features intersecting a 10%-extent hot bbox
//!   viewport_decode visualization: 1%-extent bbox, decode geometries, count vertices
//!   zoom_agg        visualization (afgba only): whole-extent aggregate count from band

use geo::algorithm::{Area, BoundingRect};
use geo::algorithm::line_measures::Length;
use geo_types::Geometry;
use flatgeobuf::FallibleStreamingIterator;
use geozero::{ColumnValue, FeatureProperties, PropertyProcessor, ToGeo};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::BufReader;
use std::process::Command;
use std::time::Instant;

const FORMATS: [&str; 7] = [
    "fgb",
    "afgba",
    "afgba-sql",
    "geojson",
    "shp",
    "duckdb",
    "parquet",
];
const WORKLOADS: [&str; 6] = [
    "full_count",
    "geom_agg",
    "attr_group",
    "bbox_count",
    "viewport_decode",
    "zoom_agg",
];
const FULL_COUNT_SQL: &str = "SELECT COUNT(*) AS count FROM features";

fn supports_workload(fmt: &str, wl: &str) -> bool {
    match (fmt, wl) {
        ("afgba-sql", "viewport_decode" | "zoom_agg") => false,
        (_, "zoom_agg") => fmt == "afgba",
        _ => FORMATS.contains(&fmt) && WORKLOADS.contains(&wl),
    }
}

fn parse_format_filter(value: &str) -> Result<Vec<&'static str>, String> {
    if value.trim().is_empty() {
        return Err("--formats requires a comma-separated format list".into());
    }
    let mut selected = Vec::new();
    for raw in value.split(',') {
        let name = raw.trim();
        if name.is_empty() {
            return Err("--formats contains an empty format".into());
        }
        let fmt = FORMATS
            .iter()
            .copied()
            .find(|candidate| *candidate == name)
            .ok_or_else(|| {
                format!(
                    "unknown format {name:?}; expected one of {}",
                    FORMATS.join(",")
                )
            })?;
        if !selected.contains(&fmt) {
            selected.push(fmt);
        }
    }
    Ok(selected)
}

fn orchestrate_formats(args: &[String]) -> Result<Vec<&'static str>, String> {
    match args {
        [] => Ok(FORMATS.to_vec()),
        [flag, value] if flag == "--formats" => parse_format_filter(value),
        [flag] if flag == "--formats" => Err("--formats requires a value".into()),
        _ => Err("usage: format-bench orchestrate <root> <results.jsonl> [--formats <list>]".into()),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "extent" => {
            let fgb = open_fgb(&args[2]).expect("open fgb");
            let h = fgb.header();
            let e = h.envelope().expect("no envelope");
            println!("{} {} {} {}", e.get(0), e.get(1), e.get(2), e.get(3));
        }
        "run-one" => {
            let out = run_one(&args[2..]).unwrap_or_else(|e| {
                println!(r#"{{"error":"{e}"}}"#);
                std::process::exit(0);
            });
            println!("{out}");
        }
        "orchestrate" => {
            let formats = match orchestrate_formats(&args[4..]) {
                Ok(formats) => formats,
                Err(error) => {
                    eprintln!("error: {error}");
                    std::process::exit(2);
                }
            };
            orchestrate(&args[2], &args[3], &formats);
        }
        "report" => report(&args[2]),
        _ => eprintln!("unknown command"),
    }
}

// ---------- shared helpers ----------

struct RunCtx {
    root: String,
    dataset: String,
    hot10: [f64; 4],
    hot1: [f64; 4],
}

type Triple = (u64, f64, String); // (count, metric, extra)

fn shrink(b: [f64; 4], f: f64) -> [f64; 4] {
    let cx = (b[0] + b[2]) / 2.0;
    let cy = (b[1] + b[3]) / 2.0;
    let w = (b[2] - b[0]) * f / 2.0;
    let h = (b[3] - b[1]) * f / 2.0;
    [cx - w, cy - h, cx + w, cy + h]
}

fn open_fgb(path: &str) -> flatgeobuf::Result<flatgeobuf::FgbReader<BufReader<std::fs::File>>> {
    flatgeobuf::FgbReader::open(BufReader::new(std::fs::File::open(path)?))
}

fn file_size_mb(p: &str) -> f64 {
    std::fs::metadata(p).map(|m| m.len() as f64 / 1048576.0).unwrap_or(0.0)
}

fn fmt_path(ctx: &RunCtx, fmt: &str) -> String {
    let (dir, ext) = match fmt {
        "fgb" => ("fgb", "fgb"),
        "afgba" | "afgba-sql" => ("a.fgb", "a.fgb"),
        "geojson" => ("geojson", "geojson"),
        "shp" => ("shp", "shp"),
        "duckdb" => ("duckdb", "duckdb"),
        "parquet" => ("parquet", "parquet"),
        _ => unreachable!(),
    };
    format!("{}/{}/{}.{}", ctx.root, dir, ctx.dataset, ext)
}

fn pq_path(ctx: &RunCtx) -> String {
    fmt_path(ctx, "parquet")
}

fn json_out(ms: f64, count: u64, metric: f64, extra: &str) -> String {
    format!(r#"{{"ms":{ms:.1},"count":{count},"metric":{metric:.4},"extra":"{extra}"}}"#)
}

// ---------- workload dispatch ----------

fn run_one(a: &[String]) -> Result<String, String> {
    let bbox = [a[4].parse::<f64>().unwrap(), a[5].parse::<f64>().unwrap(), a[6].parse::<f64>().unwrap(), a[7].parse::<f64>().unwrap()];
    let ctx = RunCtx {
        root: a[0].clone(),
        dataset: a[1].clone(),
        hot10: shrink(bbox, 0.10),
        hot1: shrink(bbox, 0.01),
    };
    let fmt = a[2].as_str();
    let wl = a[3].as_str();
    let t = Instant::now();
    let (count, metric, extra): Triple = if wl == "zoom_agg" {
        if fmt != "afgba" {
            return Err("zoom_agg is afgba-only".into());
        }
        let mut r = fgba::Reader::open_path(std::path::Path::new(&fmt_path(&ctx, "afgba")))
            .map_err(|e| e.to_string())?;
        // pick the coarsest band the trailer actually carries (res 3 if present)
        let mut bands = r.bands();
        bands.sort_by_key(|b| b.res);
        let want: u8 = std::env::var("BENCH_ZOOM_RES").ok().and_then(|v| v.parse().ok()).unwrap_or(4);
        let res = bands
            .iter()
            .find(|b| b.res >= want)
            .map(|b| b.res)
            .or_else(|| bands.last().map(|b| b.res))
            .ok_or("no bands")?;
        // world viewports (±90 / antimeridian) degenerate the A5 covering; clamp per SPEC §9.4
        let mut clamped = [bbox[0], bbox[1].max(-85.0), bbox[2], bbox[3].min(85.0)];
        if let Ok(v) = std::env::var("BENCH_ZOOM_BBOX") {
            let p: Vec<f64> = v.split(',').filter_map(|x| x.parse().ok()).collect();
            if p.len() == 4 { clamped = [p[0], p[1], p[2], p[3]]; }
        }
        let s = r.stats(clamped, res).map_err(|e| e.to_string())?;
        (s.total, s.cells as f64, format!("band_total_res{res}").into())
    } else {
        match (fmt, wl) {
            ("fgb" | "afgba", "full_count") => (fgb_count(&ctx, false)?, 0.0, String::new()),
            ("afgba-sql", "full_count") => (fgba_sql_number::<u64>(&ctx, FULL_COUNT_SQL, "count")?, 0.0, "sql_metadata".into()),
            ("geojson", "full_count") => (geojson_count(&ctx)?, 0.0, String::new()),
            ("shp", "full_count") => (shp_count(&ctx)?, 0.0, String::new()),
            ("duckdb", "full_count") => (sql_scalar(&ctx, "SELECT COUNT(*) FROM features", false)? as u64, 0.0, "sql".into()),
            ("parquet", "full_count") => (sql_scalar(&ctx, &format!("SELECT COUNT(*) FROM read_parquet('{}')", pq_path(&ctx)), false)? as u64, 0.0, "sql".into()),

            ("fgb" | "afgba", "geom_agg") => fgb_geom_agg(&ctx)?,
            ("afgba-sql", "geom_agg") => fgba_sql_geom_agg(&ctx)?,
            ("geojson", "geom_agg") => geojson_geom_agg(&ctx)?,
            ("shp", "geom_agg") => shp_geom_agg(&ctx)?,
            ("duckdb", "geom_agg") => (sql_scalar(&ctx, "SELECT COUNT(*) FROM features", false)? as u64, sql_scalar(&ctx, "SELECT COALESCE(SUM(ST_Length(geom)),0)+COALESCE(SUM(ST_Area(geom)),0) FROM features", true)?, "sql_len_area".into()),
            ("parquet", "geom_agg") => (
                sql_scalar(&ctx, &format!("SELECT COUNT(*) FROM read_parquet('{}')", pq_path(&ctx)), false)? as u64,
                sql_scalar(&ctx, &format!("SELECT COALESCE(SUM(ST_Length(geom)),0)+COALESCE(SUM(ST_Area(geom)),0) FROM read_parquet('{}')", pq_path(&ctx)), true)?,
                "sql_len_area".into(),
            ),

            ("fgb" | "afgba", "attr_group") => fgb_attr_group(&ctx)?,
            ("afgba-sql", "attr_group") => fgba_sql_attr_group(&ctx)?,
            ("geojson", "attr_group") => geojson_attr_group(&ctx)?,
            ("shp", "attr_group") => shp_attr_group(&ctx)?,
            ("duckdb", "attr_group") => sql_group(&ctx, "features", false)?,
            ("parquet", "attr_group") => sql_group(&ctx, &format!("read_parquet('{}')", pq_path(&ctx)), false)?,

            ("fgb" | "afgba", "bbox_count") => (fgb_count(&ctx, true)?, 0.0, "rtree".into()),
            ("afgba-sql", "bbox_count") => (fgba_sql_number::<u64>(&ctx, &bbox_count_sql(ctx.hot10), "count")?, 0.0, "sql_bbox".into()),
            ("geojson", "bbox_count") => (geojson_bbox(&ctx, false)?.0, 0.0, "scan".into()),
            ("shp", "bbox_count") => (shp_bbox(&ctx, false)?.0, 0.0, "scan".into()),
            ("duckdb", "bbox_count") => (sql_scalar(&ctx, &env_count("features", ctx.hot10), false)? as u64, 0.0, "sql".into()),
            ("parquet", "bbox_count") => (sql_scalar(&ctx, &env_count(&format!("read_parquet('{}')", pq_path(&ctx)), ctx.hot10), false)? as u64, 0.0, "sql".into()),

            ("fgb" | "afgba", "viewport_decode") => fgb_viewport(&ctx)?,
            ("geojson", "viewport_decode") => triple_from_pair(geojson_bbox(&ctx, true)?, "scan_decode"),
            ("shp", "viewport_decode") => triple_from_pair(shp_bbox(&ctx, true)?, "scan_decode"),
            ("duckdb", "viewport_decode") => sql_viewport(&ctx, "features")?,
            ("parquet", "viewport_decode") => sql_viewport(&ctx, &format!("read_parquet('{}')", pq_path(&ctx)))?,

            _ => return Err(format!("unknown combo {fmt}/{wl}")),
        }
    };
    Ok(json_out(t.elapsed().as_secs_f64() * 1000.0, count, metric, &extra))
}

fn triple_from_pair(p: (u64, u64), extra: &str) -> Triple {
    (p.0, p.1 as f64, extra.into())
}

fn env_count(from: &str, bb: [f64; 4]) -> String {
    format!("SELECT COUNT(*) FROM {from} WHERE ST_Intersects(ST_MakeEnvelope({}, {}, {}, {}), geom)", bb[0], bb[1], bb[2], bb[3])
}

fn sql_viewport(ctx: &RunCtx, from: &str) -> Result<Triple, String> {
    let sql = format!(
        "SELECT COUNT(*), COALESCE(SUM(ST_NPoints(geom)),0) FROM {from} WHERE ST_Intersects(ST_MakeEnvelope({}, {}, {}, {}), geom)",
        ctx.hot1[0], ctx.hot1[1], ctx.hot1[2], ctx.hot1[3]
    );
    let out = run_duckdb(ctx, &sql)?;
    let mut it = out.lines().next().unwrap_or("").split(',');
    let n: u64 = it.next().and_then(|v| v.trim().parse().ok()).unwrap_or(0);
    let v: f64 = it.next().and_then(|s| s.trim().parse().ok()).unwrap_or(0.0);
    Ok((n, v, "sql_count_npoints".into()))
}

// ---------- FlatGeobuf (fgb and a.fgb body) ----------

fn fgb_count(ctx: &RunCtx, bbox: bool) -> Result<u64, String> {
    let r = open_fgb(&fmt_path(ctx, "fgb")).map_err(|e| e.to_string())?;
    let mut iter = if bbox {
        r.select_bbox(ctx.hot10[0], ctx.hot10[1], ctx.hot10[2], ctx.hot10[3])
    } else {
        r.select_all()
    }
    .map_err(|e| e.to_string())?;
    let mut n = 0u64;
    while let Some(_) = iter.next().map_err(|e: flatgeobuf::Error| e.to_string())? {
        n += 1;
    }
    Ok(n)
}

fn count_verts(g: &Geometry<f64>) -> u64 {
    use geo_types::Geometry::*;
    match g {
        Point(_) => 1,
        LineString(l) => l.0.len() as u64,
        Polygon(p) => p.exterior().0.len() as u64 + p.interiors().iter().map(|i| i.0.len() as u64).sum::<u64>(),
        MultiPoint(m) => m.0.len() as u64,
        MultiLineString(m) => m.0.iter().map(|l| l.0.len() as u64).sum(),
        MultiPolygon(m) => m
            .0
            .iter()
            .map(|p| p.exterior().0.len() as u64 + p.interiors().iter().map(|i| i.0.len() as u64).sum::<u64>())
            .sum(),
        GeometryCollection(c) => c.0.iter().map(count_verts).sum(),
        _ => 0,
    }
}

/// Total line length over any geometry (points/polygons contribute 0).
fn geom_length(g: &Geometry<f64>) -> f64 {
    use geo_types::Geometry::*;
    match g {
        LineString(l) => geo::algorithm::line_measures::Euclidean.length(l),
        MultiLineString(m) => m.iter().map(|l| geo::algorithm::line_measures::Euclidean.length(l)).sum::<f64>(),
        GeometryCollection(c) => c.0.iter().map(geom_length).sum(),
        _ => 0.0,
    }
}

fn metrics(g: &Geometry<f64>) -> (f64, f64, u64) {
    (geom_length(g), g.unsigned_area(), count_verts(g))
}

fn fgb_geom_agg(ctx: &RunCtx) -> Result<Triple, String> {
    let r = open_fgb(&fmt_path(ctx, "fgb")).map_err(|e| e.to_string())?;
    let mut iter = r.select_all().map_err(|e| e.to_string())?;
    let mut n = 0u64;
    let mut sum = 0.0;
    while let Some(f) = iter.next().map_err(|e: flatgeobuf::Error| e.to_string())? {
        let g: Geometry<f64> = f.to_geo().map_err(|e| e.to_string())?;
        let (l, a, _) = metrics(&g);
        sum += l + a;
        n += 1;
    }
    Ok((n, sum, "rust_len_area".into()))
}

struct StrGrab<'a> {
    name: &'a str,
    val: Option<String>,
}
impl PropertyProcessor for StrGrab<'_> {
    fn property(&mut self, _i: usize, col: &str, v: &ColumnValue) -> geozero::error::Result<bool> {
        if col == self.name {
            if let ColumnValue::String(s) = v {
                self.val = Some(s.to_string());
            }
            return Ok(true);
        }
        Ok(false)
    }
}

fn fgb_string_col(path: &str) -> Option<String> {
    let r = open_fgb(path).ok()?;
    let cols = r.header().columns()?;
    for c in cols.iter() {
        if c.type_() == flatgeobuf::ColumnType::String {
            return Some(c.name().to_string());
        }
    }
    None
}

fn fgb_attr_group(ctx: &RunCtx) -> Result<Triple, String> {
    let path = fmt_path(ctx, "fgb");
    let col = fgb_string_col(&path).ok_or("no string column")?;
    let r = open_fgb(&path).map_err(|e| e.to_string())?;
    let mut iter = r.select_all().map_err(|e| e.to_string())?;
    let mut groups: HashMap<String, u64> = HashMap::new();
    let mut n = 0;
    while let Some(f) = iter.next().map_err(|e: flatgeobuf::Error| e.to_string())? {
        let mut grab = StrGrab { name: &col, val: None };
        let _ = f.process_properties(&mut grab);
        let k = grab.val.unwrap_or_else(|| "<null>".into());
        *groups.entry(k).or_default() += 1;
        n += 1;
    }
    let top = top10(&groups);
    Ok((n as u64, top, format!("col={col}")))
}

fn fgb_viewport(ctx: &RunCtx) -> Result<Triple, String> {
    let r = open_fgb(&fmt_path(ctx, "fgb")).map_err(|e| e.to_string())?;
    let mut iter = r
        .select_bbox(ctx.hot1[0], ctx.hot1[1], ctx.hot1[2], ctx.hot1[3])
        .map_err(|e| e.to_string())?;
    let mut n = 0u64;
    let mut verts = 0u64;
    while let Some(f) = iter.next().map_err(|e: flatgeobuf::Error| e.to_string())? {
        let g: Geometry<f64> = f.to_geo().map_err(|e| e.to_string())?;
        verts += count_verts(&g);
        n += 1;
    }
    Ok((n, verts as f64, "index_decode".into()))
}

// ---------- GeoJSON ----------

fn gj_features(
    ctx: &RunCtx,
) -> Result<impl Iterator<Item = geojson::Result<geojson::Feature>>, String> {
    let f = std::fs::File::open(fmt_path(ctx, "geojson")).map_err(|e| e.to_string())?;
    Ok(geojson::FeatureReader::from_reader(BufReader::new(f)).features())
}

fn feat_bbox_intersects(f: &geojson::Feature, bb: [f64; 4]) -> bool {
    use geo::algorithm::CoordsIter;
    if let Some(ref g) = f.geometry {
        if let geojson::Value::Point(ref c) = g.value {
            return c[0] >= bb[0] && c[0] <= bb[2] && c[1] >= bb[1] && c[1] <= bb[3];
        }
        if let Ok(gt) = Geometry::<f64>::try_from(g.clone()) {
            for c in gt.coords_iter() {
                if c.x >= bb[0] && c.x <= bb[2] && c.y >= bb[1] && c.y <= bb[3] {
                    return true;
                }
            }
        }
    }
    false
}

fn geojson_count(ctx: &RunCtx) -> Result<u64, String> {
    let mut n = 0u64;
    for f in gj_features(ctx)? {
        f.map_err(|e| e.to_string())?;
        n += 1;
    }
    Ok(n)
}

fn geojson_geom_agg(ctx: &RunCtx) -> Result<Triple, String> {
    let mut n = 0;
    let mut sum = 0.0;
    for f in gj_features(ctx)? {
        let f = f.map_err(|e| e.to_string())?;
        if let Some(g) = f.geometry {
            if let Ok(gt) = Geometry::<f64>::try_from(g) {
                let (l, a, _) = metrics(&gt);
                sum += l + a;
            }
        }
        n += 1;
    }
    Ok((n, sum, "rust_len_area".into()))
}

fn geojson_attr_group(ctx: &RunCtx) -> Result<Triple, String> {
    let col = gj_features(ctx)
        .ok()
        .and_then(|mut it| it.next())
        .and_then(|f| f.ok())
        .and_then(|f| f.properties)
        .and_then(|p| p.keys().next().cloned())
        .ok_or("no properties")?;
    let mut groups: HashMap<String, u64> = HashMap::new();
    let mut n = 0;
    for f in gj_features(ctx)? {
        let f = f.map_err(|e| e.to_string())?;
        let k = f
            .property(&col)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "<null>".into());
        *groups.entry(k).or_default() += 1;
        n += 1;
    }
    let top = top10(&groups);
    Ok((n as u64, top, format!("col={col}")))
}

fn geojson_bbox(ctx: &RunCtx, decode: bool) -> Result<(u64, u64), String> {
    let bb = if decode { ctx.hot1 } else { ctx.hot10 };
    let mut n = 0u64;
    let mut verts = 0u64;
    for f in gj_features(ctx)? {
        let f = f.map_err(|e| e.to_string())?;
        if feat_bbox_intersects(&f, bb) {
            if decode {
                if let Some(g) = f.geometry {
                    if let Ok(gt) = Geometry::<f64>::try_from(g) {
                        verts += count_verts(&gt);
                    }
                }
            }
            n += 1;
        }
    }
    Ok((n, verts))
}

// ---------- Shapefile ----------

type ShpItem = Result<(shapefile::Shape, dbase::Record), shapefile::Error>;

/// Run `f` over the (shape, record) stream; the iterator borrows its reader,
/// so iteration must happen in the scope where the reader lives.
fn with_shp_records<T>(
    ctx: &RunCtx,
    mut f: impl FnMut(&mut dyn Iterator<Item = ShpItem>) -> Result<T, String>,
) -> Result<T, String> {
    let mut r = shapefile::Reader::from_path(fmt_path(ctx, "shp")).map_err(|e| e.to_string())?;
    let mut it = r.iter_shapes_and_records();
    f(&mut it)
}

fn shp_count(ctx: &RunCtx) -> Result<u64, String> {
    with_shp_records(ctx, |it| Ok(it.count() as u64))
}

fn shape_metrics(s: shapefile::Shape) -> (f64, f64, u64) {
    match Geometry::<f64>::try_from(s) {
        Ok(g) => metrics(&g),
        Err(_) => (0.0, 0.0, 0),
    }
}

fn shp_geom_agg(ctx: &RunCtx) -> Result<Triple, String> {
    with_shp_records(ctx, |it| {
        let mut n = 0;
        let mut sum = 0.0;
        for rec in it {
            let (shp, _) = rec.map_err(|e| e.to_string())?;
            let (l, a, _) = shape_metrics(shp);
            sum += l + a;
            n += 1;
        }
        Ok((n, sum, "rust_len_area".into()))
    })
}

fn shp_first_c_field(ctx: &RunCtx) -> Result<String, String> {
    with_shp_records(ctx, |it| {
    let (_, rec) = match it.next() {
        Some(Ok(v)) => v,
        Some(Err(e)) => return Err(e.to_string()),
        None => return Err("empty shapefile".into()),
    };
        for (k, v) in rec {
            if matches!(v, dbase::FieldValue::Character(_)) {
                return Ok(k.clone());
            }
        }
        Err("no character field".into())
    })
}

fn shp_attr_group(ctx: &RunCtx) -> Result<Triple, String> {
    let col = shp_first_c_field(ctx)?;
    with_shp_records(ctx, |it| {
        let mut groups: HashMap<String, u64> = HashMap::new();
        let mut n = 0;
        for rec in it {
            let (_, dbf) = rec.map_err(|e| e.to_string())?;
            let k = dbf
                .get(&col)
                .and_then(|v| match v {
                    dbase::FieldValue::Character(Some(s)) => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| "<null>".into());
            *groups.entry(k).or_default() += 1;
            n += 1;
        }
        let top = top10(&groups);
        Ok((n as u64, top, format!("col={col}")))
    })
}

fn shp_bbox(ctx: &RunCtx, decode: bool) -> Result<(u64, u64), String> {
    let bb = if decode { ctx.hot1 } else { ctx.hot10 };
    with_shp_records(ctx, |it| {
        let mut n = 0u64;
        let mut verts = 0u64;
        for rec in it {
            let (shp, _) = rec.map_err(|e| e.to_string())?;
            let g = match Geometry::<f64>::try_from(shp) {
                Ok(g) => g,
                Err(_) => continue,
            };
            let hit = g
                .bounding_rect()
                .map(|b| b.min().x <= bb[2] && b.max().x >= bb[0] && b.min().y <= bb[3] && b.max().y >= bb[1])
                .unwrap_or(false);
            if hit {
                if decode {
                    verts += count_verts(&g);
                }
                n += 1;
            }
        }
        Ok((n, verts))
    })
}

// ---------- duckdb / parquet (SQL via CLI) ----------

fn sql_scalar(ctx: &RunCtx, sql: &str, _is_float: bool) -> Result<f64, String> {
    let out = run_duckdb(ctx, sql)?;
    let v: f64 = out.trim().parse().map_err(|e| format!("parse '{out}': {e}"))?;
    Ok(v)
}

fn sql_group(ctx: &RunCtx, from: &str, _t: bool) -> Result<Triple, String> {
    let col = run_duckdb(
        ctx,
        &format!(
            "SELECT column_name FROM (DESCRIBE SELECT * FROM {from}) WHERE column_type = 'VARCHAR' AND column_name NOT IN ('geom','geometry') LIMIT 1"
        ),
    )?
    .trim()
    .trim_matches('"')
    .to_string();
    if col.is_empty() {
        return Err("no varchar column".into());
    }
    let out = run_duckdb(
        ctx,
        &format!("SELECT COALESCE(CAST(\"{col}\" AS VARCHAR),'<null>'), COUNT(*) FROM {from} GROUP BY 1 ORDER BY 2 DESC LIMIT 10"),
    )?;
    let top: f64 = out
        .lines()
        .filter(|l| !l.trim().is_empty())
        .next()
        .and_then(|l| l.rsplit(',').next())
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0.0);
    let total = run_duckdb(ctx, &format!("SELECT COUNT(*) FROM {from}"))?.trim().parse().unwrap_or(0.0);
    Ok((total as u64, top, format!("col={col}")))
}

fn run_duckdb(ctx: &RunCtx, sql: &str) -> Result<String, String> {
    let dbfile = fmt_path(ctx, "duckdb");
    let out = Command::new("duckdb")
        .arg("-noheader")
        .arg("-csv")
        .arg(&dbfile)
        .arg("-c")
        .arg(format!("LOAD spatial; SET geometry_always_xy=true; {sql}"))
        .output()
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).lines().take(2).collect::<Vec<_>>().join(" "));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

// ---------- FGBA SQL (via the real fgba query CLI) ----------

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn attr_group_sql(column: &str) -> String {
    let column = quote_identifier(column);
    format!(
        "SELECT {column} AS group_value, COUNT(*) AS group_count, SUM(COUNT(*)) OVER () AS total_count FROM features GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
    )
}

fn bbox_count_sql(bb: [f64; 4]) -> String {
    format!(
        "SELECT COUNT(*) AS count FROM features WHERE bbox_xmax >= {} AND bbox_xmin <= {} AND bbox_ymax >= {} AND bbox_ymin <= {}",
        bb[0], bb[2], bb[1], bb[3]
    )
}

fn run_fgba(ctx: &RunCtx, sql: &str) -> Result<String, String> {
    let bin = std::env::var_os("FGBA_BIN")
        .unwrap_or_else(|| std::ffi::OsString::from("target/release/fgba"));
    let out = Command::new(bin)
        .arg("query")
        .arg(fmt_path(ctx, "afgba-sql"))
        .arg(sql)
        .output()
        .map_err(|e| format!("spawn fgba query: {e}"))?;
    if !out.status.success() {
        let detail = String::from_utf8_lossy(&out.stderr)
            .lines()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ");
        return Err(if detail.is_empty() {
            format!("fgba query exited with {}", out.status)
        } else {
            detail
        });
    }
    String::from_utf8(out.stdout).map_err(|e| format!("fgba query stdout is not UTF-8: {e}"))
}

fn parse_csv(output: &str, expected_headers: &[&str]) -> Result<Vec<csv::StringRecord>, String> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(output.as_bytes());
    let headers = reader
        .headers()
        .map_err(|e| format!("parse fgba CSV header: {e}"))?;
    if !headers.iter().eq(expected_headers.iter().copied()) {
        return Err(format!(
            "unexpected fgba CSV header {:?}; expected {:?}",
            headers, expected_headers
        ));
    }
    reader
        .records()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parse fgba CSV rows: {e}"))
}

fn parse_csv_scalar<T>(output: &str, header: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let rows = parse_csv(output, &[header])?;
    if rows.len() != 1 {
        return Err(format!(
            "expected one fgba CSV row for {header}, got {}",
            rows.len()
        ));
    }
    let value = rows[0]
        .get(0)
        .ok_or_else(|| format!("missing fgba CSV field {header}"))?
        .trim();
    value
        .parse::<T>()
        .map_err(|e| format!("parse fgba CSV {header} value {value:?}: {e}"))
}

fn fgba_sql_number<T>(ctx: &RunCtx, sql: &str, header: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    parse_csv_scalar(&run_fgba(ctx, sql)?, header)
}

fn fgba_sql_geom_agg(ctx: &RunCtx) -> Result<Triple, String> {
    let count = fgba_sql_number::<u64>(ctx, FULL_COUNT_SQL, "count")?;
    let metric = fgba_sql_number::<f64>(
        ctx,
        "SELECT COALESCE(SUM(geom_length + geom_area), 0) AS metric FROM features",
        "metric",
    )?;
    Ok((count, metric, "sql_kind3_len_area".into()))
}

fn fgba_sql_attr_group(ctx: &RunCtx) -> Result<Triple, String> {
    let col = fgb_string_col(&fmt_path(ctx, "afgba-sql")).ok_or("no string column")?;
    let (total, top) = attr_group_query(&col, |sql| run_fgba(ctx, sql))?;
    Ok((total, top as f64, format!("col={col}")))
}

fn attr_group_query(
    column: &str,
    mut run_query: impl FnMut(&str) -> Result<String, String>,
) -> Result<(u64, u64), String> {
    let rows = parse_csv(
        &run_query(&attr_group_sql(column))?,
        &["group_value", "group_count", "total_count"],
    )?;
    let mut total = None;
    let mut top = 0u64;
    for row in rows {
        let value = row
            .get(1)
            .ok_or_else(|| "missing fgba CSV group_count".to_string())?
            .trim();
        let count = value
            .parse::<u64>()
            .map_err(|e| format!("parse fgba CSV group_count value {value:?}: {e}"))?;
        top = top
            .checked_add(count)
            .ok_or_else(|| "top-10 group count overflows u64".to_string())?;

        let value = row
            .get(2)
            .ok_or_else(|| "missing fgba CSV total_count".to_string())?
            .trim();
        let row_total = value
            .parse::<u64>()
            .map_err(|e| format!("parse fgba CSV total_count value {value:?}: {e}"))?;
        if let Some(expected) = total {
            if expected != row_total {
                return Err(format!(
                    "inconsistent fgba CSV total_count: expected {expected}, got {row_total}"
                ));
            }
        } else {
            total = Some(row_total);
        }
    }
    Ok((total.unwrap_or(0), top))
}


// ---------- orchestrate ----------

/// Spawn one isolated `run-one` process per (dataset, format, workload) combo,
/// wrapped in /usr/bin/time -v for wall time and peak RSS; append JSONL rows.
fn orchestrate(root: &str, out_path: &str, fmts: &[&str]) {
    let wls = WORKLOADS;
    let bin = std::env::current_exe().expect("self path");
    let mut out = String::new();
    let mut datasets: Vec<String> = std::fs::read_dir(format!("{root}/fgb"))
        .expect("fgb dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "fgb").unwrap_or(false))
        .map(|e| e.path().file_stem().unwrap().to_string_lossy().into_owned())
        .collect();
    datasets.sort();
    for ds in &datasets {
        // extent from the fgb header
        let ext = Command::new(&bin)
            .arg("extent")
            .arg(format!("{root}/fgb/{ds}.fgb"))
            .output()
            .expect("spawn extent");
        let ext = String::from_utf8_lossy(&ext.stdout).trim().to_string();
        if ext.is_empty() {
            eprintln!("skip {ds}: no envelope");
            continue;
        }
        eprintln!("dataset {ds} extent {ext}");
        let ev: Vec<&str> = ext.split_whitespace().collect();
        for &f in fmts {
            // size accounting
            let size = size_mb(root, ds, f);
            for wl in wls {
                if !supports_workload(f, wl) {
                    continue;
                }
                eprintln!("  {ds}/{f}/{wl}");
                let t = Command::new("/usr/bin/time")
                    .arg("-f\nTIME %e %M")
                    .arg(&bin)
                    .arg("run-one")
                    .arg(root)
                    .arg(ds)
                    .arg(f)
                    .arg(wl)
                    .args(&ev)
                    .output()
                    .expect("spawn run-one");
                let stdout = String::from_utf8_lossy(&t.stdout).trim().to_string();
                let stderr = String::from_utf8_lossy(&t.stderr).into_owned();
                let (wall, rss) = stderr
                    .lines()
                    .rev()
                    .find(|l| l.starts_with("TIME "))
                    .map(|l| {
                        let p: Vec<&str> = l.split_whitespace().collect();
                        let wall: f64 = p.get(1).and_then(|v| v.parse().ok()).unwrap_or(0.0);
                        let rss: f64 = p.get(2).and_then(|v| v.parse().ok()).unwrap_or(0.0) / 1024.0;
                        (wall * 1000.0, rss)
                    })
                    .unwrap_or((0.0, 0.0));
                let mut row = if stdout.starts_with('{') {
                    stdout
                } else {
                    r#"{"error":"no output"}"#.to_string()
                };
                // merge our fields into the child's JSON
                let mut v: serde_json::Value = serde_json::from_str(&row).unwrap_or(serde_json::json!({"error":"bad json"}));
                let obj = v.as_object_mut().unwrap();
                obj.insert("dataset".into(), serde_json::json!(ds));
                obj.insert("format".into(), serde_json::json!(f));
                obj.insert("workload".into(), serde_json::json!(wl));
                obj.insert("rss_mb".into(), serde_json::json!((rss * 10.0).round() / 10.0));
                obj.insert("size_mb".into(), serde_json::json!((size * 10.0).round() / 10.0));
                let _ = wall;
                row = serde_json::to_string(&v).unwrap();
                out.push_str(&row);
                out.push('\n');
                std::fs::write(out_path, &out).expect("write results");
            }
        }
    }
    eprintln!("wrote {out_path}");
}

fn size_mb(root: &str, ds: &str, f: &str) -> f64 {
    let m = |p: &str| std::fs::metadata(p).map(|m| m.len() as f64 / 1048576.0).unwrap_or(0.0);
    match f {
        "fgb" => m(&format!("{root}/fgb/{ds}.fgb")),
        "afgba" | "afgba-sql" => m(&format!("{root}/a.fgb/{ds}.a.fgb")),
        "geojson" => m(&format!("{root}/geojson/{ds}.geojson")),
        "shp" => ["shp", "shx", "dbf"].iter().map(|e| m(&format!("{root}/shp/{ds}.{e}"))).sum(),
        "duckdb" => m(&format!("{root}/duckdb/{ds}.duckdb")),
        "parquet" => m(&format!("{root}/parquet/{ds}.parquet")),
        _ => 0.0,
    }
}

// ---------- report ----------

#[derive(Serialize, Deserialize, Default)]
struct Row {
    dataset: String,
    format: String,
    workload: String,
    #[serde(default)]
    ms: f64,
    #[serde(default)]
    count: u64,
    #[serde(default)]
    metric: f64,
    #[serde(default)]
    rss_mb: f64,
    #[serde(default)]
    size_mb: f64,
    #[serde(default)]
    extra: String,
    #[serde(default)]
    error: String,
}

fn report(path: &str) {
    let text = std::fs::read_to_string(path).expect("read results");
    let rows: Vec<Row> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()
        .expect("parse results");
    let mut datasets: Vec<&str> = rows.iter().map(|r| r.dataset.as_str()).collect();
    datasets.sort();
    datasets.dedup();
    let fmts = FORMATS;
    for wl in WORKLOADS {
        println!("\n### {wl}\n");
        println!("| dataset | {} |", fmts.join(" | "));
        println!("|---{}|", "---|".repeat(fmts.len()));
        for d in &datasets {
            let mut cells = vec![d.to_string()];
            for f in fmts {
                let cell = rows
                    .iter()
                    .find(|r| r.dataset == *d && r.format == f && r.workload == wl)
                    .map(|r| {
                        if r.error.is_empty() {
                            format!("{:.0} ms", r.ms)
                        } else {
                            r.error.chars().take(18).collect()
                        }
                    })
                    .unwrap_or_else(|| "—".into());
                cells.push(cell);
            }
            println!("| {} |", cells.join(" | "));
        }
    }
    println!("\n### size (MB)\n");
    println!("| dataset | {} |", fmts.join(" | "));
    println!("|---{}|", "---|".repeat(fmts.len()));
    for d in &datasets {
        let mut cells = vec![d.to_string()];
        for f in fmts {
            let cell = rows
                .iter()
                .find(|r| r.dataset == *d && r.format == f && r.size_mb > 0.0)
                .map(|r| format!("{:.0}", r.size_mb))
                .unwrap_or_else(|| "—".into());
            cells.push(cell);
        }
        println!("| {} |", cells.join(" | "));
    }
}

fn top10(groups: &HashMap<String, u64>) -> f64 {
    let mut counts: Vec<u64> = groups.values().copied().collect();
    counts.sort_unstable_by(|a, b| b.cmp(a));
    counts.into_iter().take(10).map(|c| c as f64).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_identifier_quoting_handles_colons_quotes_and_sql_text() {
        assert_eq!(quote_identifier("felt:created_by"), "\"felt:created_by\"");
        assert_eq!(quote_identifier("odd\"name"), "\"odd\"\"name\"");
        let sql = attr_group_sql("x\"; DROP TABLE features; --");
        assert_eq!(
            sql,
            "SELECT \"x\"\"; DROP TABLE features; --\" AS group_value, COUNT(*) AS group_count, SUM(COUNT(*)) OVER () AS total_count FROM features GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
        );
    }

    #[test]
    fn csv_parsing_handles_quoted_fields_and_rejects_bad_scalars() {
        let rows = parse_csv(
            "group_value,group_count\n\"a,b\",2\n\"say \"\"hello\"\"\",3\n\"two\nlines\",4\n",
            &["group_value", "group_count"],
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get(0), Some("a,b"));
        assert_eq!(rows[1].get(0), Some("say \"hello\""));
        assert_eq!(rows[2].get(0), Some("two\nlines"));
        assert_eq!(rows[2].get(1), Some("4"));

        assert_eq!(parse_csv_scalar::<u64>("count\n42\n", "count").unwrap(), 42);
        assert!(parse_csv_scalar::<u64>("count\nnope\n", "count").is_err());
        assert!(parse_csv("count\n1,2\n", &["count"]).is_err());
        assert!(parse_csv("wrong\n42\n", &["count"]).is_err());
    }

    #[test]
    fn attr_group_runs_one_query_and_reads_window_totals() {
        let mut queries = Vec::new();
        let (total, top) = attr_group_query("name", |sql| {
            queries.push(sql.to_string());
            Ok("group_value,group_count,total_count\n\"a,b\",2,12\n\"say \"\"hello\"\"\",3,12\n\"two\nlines\",4,12\n".to_string())
        })
        .unwrap();
        assert_eq!(queries, vec![attr_group_sql("name")]);
        assert_eq!((total, top), (12, 9));

        let empty = attr_group_query("name", |_| {
            Ok("group_value,group_count,total_count\n".to_string())
        })
        .unwrap();
        assert_eq!(empty, (0, 0));
    }

    #[test]
    fn attr_group_rejects_missing_or_inconsistent_totals() {
        for output in [
            "group_value,group_count\na,1\n",
            "group_value,group_count,total_count\na,1\n",
            "group_value,group_count,total_count\na,1,\n",
        ] {
            assert!(
                attr_group_query("name", |_| Ok(output.to_string())).is_err(),
                "accepted missing total_count from {output:?}"
            );
        }

        let error = attr_group_query("name", |_| {
            Ok("group_value,group_count,total_count\na,2,12\nb,3,11\n".to_string())
        })
        .unwrap_err();
        assert_eq!(
            error,
            "inconsistent fgba CSV total_count: expected 12, got 11"
        );
    }

    #[test]
    fn format_filter_validates_and_orchestration_skips_unsupported_workloads() {
        assert_eq!(parse_format_filter("afgba-sql").unwrap(), vec!["afgba-sql"]);
        assert_eq!(
            parse_format_filter("afgba-sql, fgb,afgba-sql").unwrap(),
            vec!["afgba-sql", "fgb"]
        );
        assert_eq!(orchestrate_formats(&[]).unwrap(), FORMATS);
        for invalid in ["", "afgba-sql,", "unknown"] {
            assert!(parse_format_filter(invalid).is_err(), "accepted {invalid:?}");
        }
        assert!(orchestrate_formats(&["--formats".into()]).is_err());

        for workload in ["full_count", "attr_group", "geom_agg", "bbox_count"] {
            assert!(supports_workload("afgba-sql", workload));
        }
        for workload in ["viewport_decode", "zoom_agg"] {
            assert!(!supports_workload("afgba-sql", workload));
        }
        assert!(supports_workload("afgba", "zoom_agg"));
        assert!(supports_workload("fgb", "viewport_decode"));
        assert!(!supports_workload("fgb", "zoom_agg"));
        assert!(!supports_workload("unknown", "full_count"));
    }

    #[test]
    fn afgba_sql_uses_the_afgba_artifact_and_canonical_bbox() {
        let ctx = RunCtx {
            root: "data/formats".into(),
            dataset: "Land".into(),
            hot10: [0.0; 4],
            hot1: [0.0; 4],
        };
        assert_eq!(fmt_path(&ctx, "afgba-sql"), fmt_path(&ctx, "afgba"));
        let sql = bbox_count_sql([1.0, 2.0, 3.0, 4.0]);
        assert!(sql.contains("bbox_xmax >= 1"));
        assert!(sql.contains("bbox_xmin <= 3"));
        assert!(sql.contains("bbox_ymax >= 2"));
        assert!(sql.contains("bbox_ymin <= 4"));
    }
}
