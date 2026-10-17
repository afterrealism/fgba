//! The aggregate-trailer format experiment: pack band arrays after an FGB body, then read the
//! result back locally and over real HTTP range requests, counting requests and bytes.
//!
//! Layout (little-endian; conventions after FGBO — sentinel + directory + 32B footer):
//! ```text
//! [plain FlatGeobuf body][sentinel FF FF FF FF]
//! [band section]*   u64 num_cells, then records { u64 cell, u32 count }   (curve-sorted)
//! [directory]       u8 version=1, u8 n_bands, then per band { u8 res, u64 offset, u64 size }
//! [footer 32B]      magic "RGVAGG01", u64 dir_offset, u64 dir_size, 8B reserved
//! ```

use crate::{a5_cover, build_a5, views, FlatIndex, PATH};
use rustgis_data::source;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::Instant;

const MAGIC: &[u8; 8] = b"FGBAGG01";
const SENTINEL: [u8; 4] = [0xFF; 4];
const RGV_PATH: &str = "/tmp/rgv-lab/data/points.rgv";
const BANDS: [i32; 5] = [2, 3, 5, 7, 9];

struct BandMeta {
    res: i32,
    offset: u64,
    size: u64,
}

pub fn pack() {
    let body = std::fs::read(PATH).expect("fgb bytes");
    let src = source::open_path(std::path::Path::new(PATH), None).expect("open");
    let (ds, _) = source::materialize(src.as_ref()).expect("materialize");

    let t = Instant::now();
    let bands: Vec<(i32, FlatIndex)> = BANDS.iter().map(|&r| (r, build_a5(&ds, r))).collect();
    let t_build = t.elapsed();

    let total = ds.features.len() as u64;
    let mut out = body.clone();
    out.extend_from_slice(&SENTINEL);
    let mut metas = Vec::new();
    for (res, idx) in &bands {
        let offset = out.len() as u64;
        out.push(1u8); // grid = A5
        out.push(*res as u8);
        out.extend_from_slice(&0u16.to_le_bytes()); // flags
        out.extend_from_slice(&total.to_le_bytes());
        out.extend_from_slice(&(idx.recs.len() as u64).to_le_bytes());
        for &(cell, count) in &idx.recs {
            out.extend_from_slice(&cell.to_le_bytes());
            out.extend_from_slice(&count.to_le_bytes());
        }
        metas.push(BandMeta { res: *res, offset, size: out.len() as u64 - offset });
    }
    let dir_offset = out.len() as u64;
    out.push(1u8); // directory version
    out.push(metas.len() as u8);
    for m in &metas {
        out.extend_from_slice(&1u16.to_le_bytes()); // kind = aggregate band
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&m.offset.to_le_bytes());
        out.extend_from_slice(&m.size.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes());
        out.push(m.res as u8);
    }
    let dir_size = out.len() as u64 - dir_offset;
    let dir_crc = crc32_ieee(&out[dir_offset as usize..]);
    let mut footer = [0u8; 32];
    footer[0..8].copy_from_slice(MAGIC);
    footer[8..16].copy_from_slice(&dir_offset.to_le_bytes());
    footer[16..24].copy_from_slice(&dir_size.to_le_bytes());
    footer[24..28].copy_from_slice(&dir_crc.to_le_bytes());
    out.extend_from_slice(&footer);

    std::fs::write(RGV_PATH, &out).expect("write rgv");
    println!(
        "pack: body {:.1} MiB + trailer {:.2} MiB = {:.1} MiB total | band builds {:?}",
        body.len() as f64 / (1 << 20) as f64,
        (out.len() - body.len()) as f64 / (1 << 20) as f64,
        out.len() as f64 / (1 << 20) as f64,
        t_build
    );
    for (m, (_, idx)) in metas.iter().zip(&bands) {
        println!("  band res{:<2} {:>7} cells  {:>8.1} KiB", m.res, idx.recs.len(), m.size as f64 / 1024.0);
    }
}

// ---------- byte sources with instrumentation ----------

trait Src: Read + Seek {
    fn n_requests(&self) -> usize;
    fn n_bytes(&self) -> u64;
}

struct FileSrc {
    f: std::fs::File,
    reads: usize,
    bytes: u64,
}
impl Read for FileSrc {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.f.read(buf)?;
        self.reads += 1;
        self.bytes += n as u64;
        Ok(n)
    }
}
impl Seek for FileSrc {
    fn seek(&mut self, p: SeekFrom) -> std::io::Result<u64> {
        self.f.seek(p)
    }
}
impl Src for FileSrc {
    fn n_requests(&self) -> usize {
        self.reads
    }
    fn n_bytes(&self) -> u64 {
        self.bytes
    }
}

/// Blocking `Read + Seek` over HTTP range requests (the FGBO `HttpRangeReader` pattern, ureq).
struct HttpSrc {
    url: String,
    agent: ureq::Agent,
    len: u64,
    pos: u64,
    requests: usize,
    bytes: u64,
}
impl HttpSrc {
    fn open(url: &str) -> Self {
        let agent = ureq::Agent::new_with_defaults();
        let res = agent.get(url).header("Range", "bytes=0-0").call().expect("probe");
        assert_eq!(res.status().as_u16(), 206, "server must do ranges");
        let cr = res.headers().get("content-range").and_then(|v| v.to_str().ok()).expect("content-range").to_string();
        let len: u64 = cr.rsplit('/').next().unwrap().trim().parse().unwrap();
        HttpSrc { url: url.into(), agent, len, pos: 0, requests: 1, bytes: 1 }
    }
}
impl Read for HttpSrc {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let want = (buf.len() as u64).min(self.len - self.pos);
        if want == 0 {
            return Ok(0);
        }
        let res = self
            .agent
            .get(&self.url)
            .header("Range", format!("bytes={}-{}", self.pos, self.pos + want - 1))
            .call()
            .map_err(std::io::Error::other)?;
        assert_eq!(res.status().as_u16(), 206);
        let mut body = res.into_body().into_reader();
        let mut filled = 0usize;
        while filled < want as usize {
            let n = body.read(&mut buf[filled..want as usize])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        self.pos += filled as u64;
        self.requests += 1;
        self.bytes += filled as u64;
        Ok(filled)
    }
}
impl Seek for HttpSrc {
    fn seek(&mut self, p: SeekFrom) -> std::io::Result<u64> {
        self.pos = match p {
            SeekFrom::Start(o) => o,
            SeekFrom::Current(d) => (self.pos as i64 + d) as u64,
            SeekFrom::End(d) => (self.len as i64 + d) as u64,
        };
        Ok(self.pos)
    }
}
impl Src for HttpSrc {
    fn n_requests(&self) -> usize {
        self.requests
    }
    fn n_bytes(&self) -> u64 {
        self.bytes
    }
}

// ---------- the read protocol ----------

fn read_exact_at<S: Read + Seek>(s: &mut S, off: u64, len: usize) -> Vec<u8> {
    s.seek(SeekFrom::Start(off)).expect("seek");
    let mut buf = vec![0u8; len];
    s.read_exact(&mut buf).expect("read");
    buf
}

fn open_trailer<S: Read + Seek>(s: &mut S) -> Vec<BandMeta> {
    let end = s.seek(SeekFrom::End(-32)).unwrap();
    let footer = read_exact_at(s, end, 32);
    assert_eq!(&footer[0..8], MAGIC, "not an rgv file");
    let dir_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    let dir_size = u64::from_le_bytes(footer[16..24].try_into().unwrap()) as usize;
    let dir = read_exact_at(s, dir_offset, dir_size);
    assert_eq!(dir[0], 1, "directory version");
    let crc = u32::from_le_bytes(footer[24..28].try_into().unwrap());
    assert_eq!(crc, crc32_ieee(&dir), "directory crc");
    let n = dir[1] as usize;
    let mut bands = Vec::new();
    let mut p = 2;
    for _ in 0..n {
        let kind = u16::from_le_bytes(dir[p..p + 2].try_into().unwrap());
        let offset = u64::from_le_bytes(dir[p + 4..p + 12].try_into().unwrap());
        let size = u64::from_le_bytes(dir[p + 12..p + 20].try_into().unwrap());
        let meta_len = u16::from_le_bytes(dir[p + 20..p + 22].try_into().unwrap()) as usize;
        if kind == 1 && meta_len >= 1 {
            bands.push(BandMeta { res: dir[p + 22] as i32, offset, size });
        }
        p += 22 + meta_len;
    }
    bands
}

fn parse_band(buf: &[u8]) -> FlatIndex {
    assert_eq!(buf[0], 1, "grid A5");
    let n = u64::from_le_bytes(buf[12..20].try_into().unwrap()) as usize;
    let mut recs = Vec::with_capacity(n);
    let mut p = 20;
    for _ in 0..n {
        let cell = u64::from_le_bytes(buf[p..p + 8].try_into().unwrap());
        let count = u32::from_le_bytes(buf[p + 8..p + 12].try_into().unwrap());
        recs.push((cell, count));
        p += 12;
    }
    FlatIndex { res_label: "band".into(), recs }
}

/// View width in degrees → band resolution (the ladder).
fn ladder(width_deg: f64) -> i32 {
    match width_deg {
        w if w > 60.0 => 2,
        w if w > 16.0 => 3,
        w if w > 4.0 => 5,
        w if w > 0.5 => 7,
        _ => 9,
    }
}

fn run_views<S: Src>(s: &mut S, label: &str) {
    let t0 = Instant::now();
    let bands = open_trailer(s);
    let t_dir = t0.elapsed();
    let (req0, bytes0) = (s.n_requests(), s.n_bytes());
    println!("{label}: open (probe+footer+directory) {:?}  requests {}  bytes {}", t_dir, req0, bytes0);

    let mut cache: HashMap<i32, FlatIndex> = HashMap::new();
    for (vlabel, view) in views() {
        let res = ladder(view.width());
        let band = bands.iter().find(|b| b.res == res).expect("band");
        let t = Instant::now();
        if !cache.contains_key(&res) {
            let idx = parse_band(&read_exact_at(s, band.offset, band.size as usize));
            cache.insert(res, idx);
        }
        let idx = cache.get(&res).unwrap();
        let cover = a5_cover(&view, res);
        let (runs, cells, sum, _spanned) = idx.query(&cover);
        println!(
            "{label} view {vlabel:14} res{res}  {:?}  requests {} (+{})  bytes {}  runs {}  cells {}  sum {sum}",
            t.elapsed(),
            s.n_requests(),
            s.n_requests() - req0,
            s.n_bytes(),
            runs,
            cells
        );
    }
    println!("{label}: totals — requests {}  bytes {} ({:.2} MiB)\n", s.n_requests(), s.n_bytes(), s.n_bytes() as f64 / (1 << 20) as f64);
}

pub fn read_local() {
    let mut f = FileSrc { f: std::fs::File::open(RGV_PATH).expect("open rgv"), reads: 0, bytes: 0 };
    run_views(&mut f, "local");
}

pub fn read_http() {
    let mut h = HttpSrc::open("http://127.0.0.1:8750/points.rgv");
    run_views(&mut h, "http ");
}

// ---------- minimal range-capable static server (loopback, sequential) ----------

pub fn serve() {
    let listener = std::net::TcpListener::bind("127.0.0.1:8750").expect("bind");
    eprintln!("serving /tmp/rgv-lab/data on http://127.0.0.1:8750 (ranges)");
    for stream in listener.incoming() {
        if let Ok(s) = stream {
            handle(s);
        }
    }
}

fn handle(mut s: std::net::TcpStream) {
    let mut head = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    while head.len() < 8192 {
        if Read::read(&mut s, &mut byte).unwrap_or(0) == 0 {
            return;
        }
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&head);
    let mut lines = text.lines();
    let path = lines.next().and_then(|l| l.split_whitespace().nth(1)).unwrap_or("/").trim_start_matches('/').to_string();
    if path.contains("..") {
        return;
    }
    let range = lines
        .filter(|l| l.to_ascii_lowercase().starts_with("range:"))
        .find_map(|l| l.split_once("bytes=").map(|(_, v)| v.trim().to_string()));
    let full = match std::fs::read(format!("/tmp/rgv-lab/data/{path}")) {
        Ok(b) => b,
        Err(_) => {
            let _ = s.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
            return;
        }
    };
    if let Some(r) = range.and_then(|r| {
        let (a, b) = r.split_once('-')?;
        let a: u64 = a.parse().ok()?;
        let b: u64 = b.parse().ok()?;
        Some((a, b.min(full.len() as u64 - 1)))
    }) {
        let slice = &full[r.0 as usize..=r.1 as usize];
        let head = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\n\r\n",
            slice.len(), r.0, r.1, full.len()
        );
        let _ = s.write_all(head.as_bytes());
        let _ = s.write_all(slice);
    } else {
        let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n", full.len());
        let _ = s.write_all(head.as_bytes());
        let _ = s.write_all(&full);
    }
}

/// Small deterministic test vector for the spec: 12 known points, one res-2 band.
/// Final v1 format: self-describing band sections, kind-tagged directory, CRC32.
fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 == 1 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
        }
    }
    !crc
}

pub fn pack_small() {
    use rustgis_data::{Coord, Dataset, Field, FieldType, Geometry, GeometryType, Value};
    let pts: [(f64, f64); 12] = [
        (-0.12, 51.50), (-0.09, 51.51), (-0.14, 51.49), (-0.11, 51.52), // London
        (2.33, 48.86), (2.36, 48.84), (2.34, 48.87), (2.37, 48.85),     // Paris
        (-74.00, 40.71), (-73.97, 40.73), (-74.02, 40.70), (-73.98, 40.72), // NYC
    ];
    let mut ds = Dataset::new("cities", GeometryType::Point, vec![Field::new("name", FieldType::Text)]);
    for (i, &(lon, lat)) in pts.iter().enumerate() {
        ds.push_next(Some(Geometry::Point(Coord { x: lon, y: lat })), vec![Value::Text(format!("city{}", i + 1))]);
    }
    let body = source::fgb::write(&ds).expect("fgb");
    let idx = build_a5(&ds, 2);
    let total = ds.features.len() as u64;

    let mut out = body.clone();
    out.extend_from_slice(&SENTINEL);
    // --- band section: self-describing header + records ---
    let band_off = out.len() as u64;
    out.push(1u8); // grid = 1 (A5)
    out.push(2u8); // res = 2
    out.extend_from_slice(&0u16.to_le_bytes()); // band flags = 0 (count only)
    out.extend_from_slice(&total.to_le_bytes());
    out.extend_from_slice(&(idx.recs.len() as u64).to_le_bytes());
    for &(cell, count) in &idx.recs {
        out.extend_from_slice(&cell.to_le_bytes());
        out.extend_from_slice(&count.to_le_bytes());
    }
    let band_size = out.len() as u64 - band_off;
    // --- directory: kind-tagged section table ---
    let dir_offset = out.len() as u64;
    out.push(1u8); // directory version
    out.push(1u8); // n_sections
    out.extend_from_slice(&1u16.to_le_bytes()); // kind = 1 (aggregate band)
    out.extend_from_slice(&0u16.to_le_bytes()); // flags
    out.extend_from_slice(&band_off.to_le_bytes());
    out.extend_from_slice(&band_size.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes()); // meta_len = 1
    out.push(2u8); // meta = [res]
    let dir_size = out.len() as u64 - dir_offset;
    let dir_crc = crc32_ieee(&out[dir_offset as usize..]);
    let mut footer = [0u8; 32];
    footer[0..8].copy_from_slice(MAGIC);
    footer[8..16].copy_from_slice(&dir_offset.to_le_bytes());
    footer[16..24].copy_from_slice(&dir_size.to_le_bytes());
    footer[24..28].copy_from_slice(&dir_crc.to_le_bytes());
    out.extend_from_slice(&footer);

    std::fs::write("/tmp/rgv-lab/data/small.a.fgb", &out).expect("write");
    println!("body {} bytes ({} features), trailer+dir+footer {} bytes, total {}",
        body.len(), total, out.len() - body.len(), out.len());
    println!("band res2: {} occupied cells", idx.recs.len());
    for &(c, n) in &idx.recs {
        let ll = a5::cell_to_lonlat(c).expect("center");
        println!("  cell {:016x} count {} (center lon {:.1} lat {:.1})", c, n, ll.longitude(), ll.latitude());
    }
    println!("dir crc32: {:08x}", dir_crc);
    println!("--- hex: sentinel..EOF (offset {}) ---", body.len());
    for (row, chunk) in out[body.len()..].chunks(16).enumerate() {
        let hex: Vec<String> = chunk.iter().map(|b| format!("{b:02x}")).collect();
        println!("{:08x}  {}", body.len() + row * 16, hex.join(" "));
    }
}
