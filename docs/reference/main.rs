//! rgv-lab: grounding experiments for a cloud-optimized aggregate format.
//!
//! Subcommands:
//!   gen        — synthesize 1M clustered points, write FlatGeobuf, report time/size
//!   bench-fgb  — baseline: FGB bbox queries at 5 viewport scales
//!   bench-agg  — build flat (cell,count) arrays (A5 + quadkey bands), query them per viewport:
//!                time, contiguous runs (= HTTP requests), bytes, cells; rollup-from-base timing

use rustgis_data::source::{self, Request};
use rustgis_data::{Coord, Dataset, Extent, Field, FieldType, Geometry, GeometryType, Value};
use std::collections::HashMap;
use std::time::Instant;

mod trailer;

pub(crate) const N: usize = 1_000_000;
pub(crate) const PATH: &str = "/tmp/rgv-lab/data/points.fgb";

// ---------- tiny deterministic RNG (xorshift64*) + Box–Muller ----------
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn f64(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn gauss(&mut self) -> f64 {
        let u1 = self.f64().max(1e-12);
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * self.f64()).cos()
    }
}

fn synth() -> Dataset {
    let mut rng = Rng(42);
    let mut ds = Dataset::new("points", GeometryType::Point, vec![Field::new("value", FieldType::Double)]);
    // 50 cluster centers over the inhabited latitudes.
    let centers: Vec<(f64, f64)> = (0..50).map(|_| (rng.f64() * 300.0 - 150.0, rng.f64() * 130.0 - 55.0)).collect();
    for i in 0..N {
        let (lon, lat) = if i % 10 == 9 {
            (rng.f64() * 340.0 - 170.0, rng.f64() * 140.0 - 60.0)
        } else {
            let &(cx, cy) = &centers[i % 50];
            ((cx + rng.gauss() * 0.4).clamp(-179.9, 179.9), (cy + rng.gauss() * 0.4).clamp(-84.9, 84.9))
        };
        ds.push_next(Some(Geometry::Point(Coord { x: lon, y: lat })), vec![Value::Float(rng.f64() * 1000.0)]);
    }
    ds
}

const VIEW_WIDTHS: [f64; 6] = [120.0, 32.0, 8.0, 1.0, 0.125, 0.0156];
const VIEW_LABELS: [&str; 6] = ["hemisphere~z1", "continent~z3", "country~z5", "metro~z8", "city~z11", "block~z14"];

fn densest_center() -> (f64, f64) {
    let mut rng = Rng(42);
    (rng.f64() * 300.0 - 150.0, rng.f64() * 130.0 - 55.0)
}

pub(crate) fn views() -> Vec<(String, Extent)> {
    let (cx, cy) = densest_center();
    VIEW_WIDTHS
        .iter()
        .zip(VIEW_LABELS)
        .map(|(w, l)| {
            let h = w / 2.0;
            (l.to_string(), Extent::new(cx - w / 2.0, cy - h / 2.0, cx + w / 2.0, cy + h / 2.0))
        })
        .collect()
}

fn gen() {
    let t = Instant::now();
    let ds = synth();
    let t_gen = t.elapsed();
    let t = Instant::now();
    let bytes = source::fgb::write(&ds).expect("fgb write");
    let t_write = t.elapsed();
    std::fs::write(PATH, &bytes).expect("save");
    println!(
        "gen: {N} features | dataset build {:?} | fgb encode {:?} | {:.1} MiB",
        t_gen,
        t_write,
        bytes.len() as f64 / (1 << 20) as f64
    );
}

fn bench_fgb() {
    let src = source::open_path(std::path::Path::new(PATH), None).expect("open fgb");
    println!("fgb info: count {:?}", src.info().count);
    for (label, view) in views() {
        let mut times = Vec::new();
        let (mut matched, mut got) = (0u64, 0usize);
        for _ in 0..3 {
            let t = Instant::now();
            let b = src.read(&Request { extent: Some(view), limit: 2_000_000 }).expect("read");
            times.push(t.elapsed());
            matched = b.matched;
            got = b.features.len();
        }
        times.sort();
        println!("fgb  {label:14} time {:?}  matched {matched}  features {got}", times[1]);
    }
}

// ---------- quadkey helpers ----------
fn lonlat_to_tile(lon: f64, lat: f64, z: u8) -> (u32, u32) {
    let n = (1u64 << z) as f64;
    let x = (((lon + 180.0) / 360.0) * n).floor() as i64;
    let lat = lat.clamp(-85.0511, 85.0511).to_radians();
    let y = (((1.0 - lat.tan().asinh() / std::f64::consts::PI) / 2.0) * n).floor() as i64;
    let m = (1i64 << z) - 1;
    (x.clamp(0, m) as u32, y.clamp(0, m) as u32)
}
fn morton(x: u32, y: u32) -> u64 {
    let mut k = 0u64;
    for i in 0..30 {
        k |= (((x >> i) & 1) as u64) << (2 * i + 1);
        k |= (((y >> i) & 1) as u64) << (2 * i);
    }
    k
}
fn qk(lon: f64, lat: f64, z: u8) -> u64 {
    let (x, y) = lonlat_to_tile(lon, lat, z);
    ((z as u64) << 60) | morton(x, y)
}

/// Sorted occupied-cell array: (cell id, count). Query = binary search per covering cell;
/// matched record indices merge into runs = contiguous byte ranges (= HTTP requests).
pub(crate) struct FlatIndex {
    pub(crate) res_label: String,
    pub(crate) recs: Vec<(u64, u32)>, // sorted by id
}
impl FlatIndex {
    /// returns (runs, records_matched, total_count, bytes_spanned)
    pub(crate) fn query(&self, cells: &[u64]) -> (usize, usize, u64, usize) {
        let mut idxs: Vec<usize> = Vec::new();
        for &c in cells {
            if let Ok(i) = self.recs.binary_search_by_key(&c, |&(id, _)| id) {
                idxs.push(i);
            }
        }
        idxs.sort_unstable();
        idxs.dedup();
        let mut runs = 0usize;
        let mut bytes = 0usize;
        let mut total = 0u64;
        let mut prev: Option<usize> = None;
        let mut run_start = 0usize;
        for &i in &idxs {
            total += self.recs[i].1 as u64;
            match prev {
                Some(p) if i == p + 1 => {}
                _ => {
                    if let Some(p) = prev {
                        bytes += (p - run_start + 1) * 12;
                    }
                    runs += 1;
                    run_start = i;
                }
            }
            prev = Some(i);
        }
        if let Some(p) = prev {
            bytes += (p - run_start + 1) * 12;
        }
        (runs, idxs.len(), total, bytes)
    }
}

pub(crate) fn build_a5(ds: &Dataset, res: i32) -> FlatIndex {
    let mut map: HashMap<u64, u32> = HashMap::new();
    for f in &ds.features {
        let Some(g) = &f.geometry else { continue };
        let e = g.extent();
        let c = a5::LonLat::new((e.min_x + e.max_x) / 2.0, (e.min_y + e.max_y) / 2.0);
        if let Ok(id) = a5::lonlat_to_cell(c, res) {
            *map.entry(id).or_insert(0) += 1;
        }
    }
    let mut recs: Vec<(u64, u32)> = map.into_iter().collect();
    recs.sort_unstable_by_key(|&(id, _)| id);
    FlatIndex { res_label: format!("a5-res{res}"), recs }
}

fn build_qk(ds: &Dataset, z: u8) -> FlatIndex {
    let mut map: HashMap<u64, u32> = HashMap::new();
    for f in &ds.features {
        let Some(g) = &f.geometry else { continue };
        let e = g.extent();
        let id = qk((e.min_x + e.max_x) / 2.0, (e.min_y + e.max_y) / 2.0, z);
        *map.entry(id).or_insert(0) += 1;
    }
    let mut recs: Vec<(u64, u32)> = map.into_iter().collect();
    recs.sort_unstable_by_key(|&(id, _)| id);
    FlatIndex { res_label: format!("qk-z{z}"), recs }
}

/// A5 covering of a viewport at `res` (uncompacted, curve-sorted).
pub(crate) fn a5_cover(view: &Extent, res: i32) -> Vec<u64> {
    let ring = vec![
        a5::LonLat::new(view.min_x, view.min_y),
        a5::LonLat::new(view.max_x, view.min_y),
        a5::LonLat::new(view.max_x, view.max_y),
        a5::LonLat::new(view.min_x, view.max_y),
    ];
    let compacted = a5::polygon_to_cells(
        &[ring],
        res,
        Some(a5::PolygonToCellsOptions { containment: a5::Containment::Overlapping }),
    )
    .expect("covering");
    a5::uncompact(&compacted).expect("uncompact")
}

/// Quadkey covering of a viewport at zoom `z`: all tiles in the bbox, sorted by key.
fn qk_cover(view: &Extent, z: u8) -> Vec<u64> {
    let (x0, y0) = lonlat_to_tile(view.min_x, view.max_y, z); // y0 = north row
    let (x1, y1) = lonlat_to_tile(view.max_x, view.min_y, z); // y1 = south row (y grows southward)
    let mut cells = Vec::new();
    for y in y0..=y1 {
        for x in x0..=x1 {
            cells.push(((z as u64) << 60) | morton(x, y));
        }
    }
    cells.sort_unstable();
    cells
}

fn bench_agg() {
    let t = Instant::now();
    let bytes = std::fs::read(PATH).expect("read fgb");
    let src = source::open_path(std::path::Path::new(PATH), None).expect("open");
    let (ds, _) = source::materialize(src.as_ref()).expect("materialize");
    println!("load+materialize {:?} ({:.1} MiB)", t.elapsed(), bytes.len() as f64 / (1 << 20) as f64);

    // --- build bands ---
    let a5_res = [2, 3, 5, 7, 9];
    let qk_zs = [3u8, 4, 6, 8, 10];
    let mut a5_idxs = Vec::new();
    let mut qk_idxs = Vec::new();
    for &r in &a5_res {
        let t = Instant::now();
        let idx = build_a5(&ds, r);
        println!(
            "build {:8} {:?}  occupied {} cells  {:.2} MiB",
            idx.res_label,
            t.elapsed(),
            idx.recs.len(),
            idx.recs.len() as f64 * 12.0 / (1 << 20) as f64
        );
        a5_idxs.push(idx);
    }
    for &z in &qk_zs {
        let t = Instant::now();
        let idx = build_qk(&ds, z);
        println!(
            "build {:8} {:?}  occupied {} cells  {:.2} MiB",
            idx.res_label,
            t.elapsed(),
            idx.recs.len(),
            idx.recs.len() as f64 * 12.0 / (1 << 20) as f64
        );
        qk_idxs.push(idx);
    }

    // --- rollup from the single base array (res 9) vs direct arrays ---
    let base = a5_idxs.last().unwrap();
    for &r in &[2, 3, 5, 7] {
        let t = Instant::now();
        let mut map: HashMap<u64, u64> = HashMap::new();
        for &(c, n) in &base.recs {
            if let Ok(s) = a5::cell_to_supercell(c, r) {
                *map.entry(s).or_insert(0) += n as u64;
            }
        }
        let total: u64 = map.values().sum();
        println!("rollup res9→res{r}: {:?}  {} cells  total {total}", t.elapsed(), map.len());
    }

    // --- viewport queries ---
    let combos = [(0usize, 2, 3u8), (1, 3, 4), (2, 5, 6), (3, 7, 8), (4, 9, 10), (5, 9, 10)];
    for (vi, res, z) in combos {
        let (label, view) = &views()[vi];
        let a5idx = &a5_idxs[a5_res.iter().position(|&r| r == res).unwrap()];
        let qkidx = &qk_idxs[qk_zs.iter().position(|&zz| zz == z).unwrap()];
        let (mut t_a5, mut t_qk) = (Vec::new(), Vec::new());
        let (mut a5_stats, mut qk_stats) = ((0, 0, 0, 0), (0, 0, 0, 0));
        for _ in 0..3 {
            let t = Instant::now();
            let cover = a5_cover(view, res);
            let s = a5idx.query(&cover);
            t_a5.push(t.elapsed());
            a5_stats = s;
            let t = Instant::now();
            let cover = qk_cover(view, z);
            let s = qkidx.query(&cover);
            t_qk.push(t.elapsed());
            qk_stats = s;
        }
        t_a5.sort();
        t_qk.sort();
        println!(
            "view {label:14} A5(res{res}): {:?} runs={} cells={} sum={} bytes={} | QK(z{z}): {:?} runs={} cells={} sum={} bytes={}",
            t_a5[1], a5_stats.0, a5_stats.1, a5_stats.2, a5_stats.3,
            t_qk[1], qk_stats.0, qk_stats.1, qk_stats.2, qk_stats.3,
        );
    }

    // --- pentagon generation cost at render time (continent view at res 3) ---
    let (_, view) = &views()[1];
    let cover = a5_cover(view, 3);
    let t = Instant::now();
    let mut verts = 0usize;
    for &c in cover.iter().take(500) {
        if let Ok(ring) = a5::cell_to_boundary(c, None) {
            verts += ring.len();
        }
    }
    println!("pentagon boundaries: {} cells → {} verts in {:?}", cover.len().min(500), verts, t.elapsed());
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("gen") => gen(),
        Some("bench-fgb") => bench_fgb(),
        Some("bench-agg") => bench_agg(),
        Some("pack") => trailer::pack(),
        Some("serve") => trailer::serve(),
        Some("read-local") => trailer::read_local(),
        Some("read-http") => trailer::read_http(),
        other => {
            eprintln!("usage: rgv-lab gen|bench-fgb|bench-agg|pack|serve|read-local|read-http (got {other:?})");
            std::process::exit(2);
        }
    }
}
