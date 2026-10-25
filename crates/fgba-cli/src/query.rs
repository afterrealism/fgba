//! `fgba query` command dispatch.

use crate::parse_bbox;
use fgba_sql::QueryOptions;
use std::io::Write;
use std::path::Path;

/// Run one local read-only SQL query and stream CSV to stdout.
pub(crate) fn run(
    input: &Path,
    sql: &str,
    res: Option<u8>,
    bbox: Option<&str>,
    scan_threads: Option<usize>,
) -> Result<(), String> {
    if sql.trim().is_empty() {
        return Err("SQL must not be empty".to_string());
    }
    let bbox = bbox.map(parse_bbox).transpose()?;
    let options = QueryOptions {
        bbox,
        res,
        scan_threads,
        ..QueryOptions::default()
    };
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    fgba_sql::query_csv(input, sql, options, &mut output).map_err(|error| {
        let message = error.to_string().replace(['\r', '\n'], " ");
        format!("{}: {message}", input.display())
    })?;
    output.flush().map_err(|error| error.to_string())
}
