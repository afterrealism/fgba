use fgba_wasm::range::{would_block, SparseRange, WouldBlock};
use flatgeobuf::{FallibleStreamingIterator, FgbReader};
use std::io::{ErrorKind, Read, Seek, SeekFrom};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn exact_reads_and_seeks_across_discontiguous_ranges() -> TestResult {
    let source: Vec<u8> = (0..=255).cycle().take(4096).collect();
    let mut sparse = SparseRange::new(source.len() as u64, 64, 256)?;

    let mut first = [0; 8];
    expect_miss(
        sparse.read_exact(&mut first),
        WouldBlock { offset: 0, len: 8 },
    )?;
    sparse.insert(0, &source[..8])?;
    sparse.read_exact(&mut first)?;
    assert_eq!(first, source[..8]);

    sparse.seek(SeekFrom::Start(1000))?;
    let mut middle = [0; 16];
    expect_miss(
        sparse.read_exact(&mut middle),
        WouldBlock {
            offset: 1000,
            len: 16,
        },
    )?;
    sparse.insert(1000, &source[1000..1016])?;
    sparse.read_exact(&mut middle)?;
    assert_eq!(middle, source[1000..1016]);

    sparse.seek(SeekFrom::End(-8))?;
    let mut last = [0; 8];
    expect_miss(
        sparse.read_exact(&mut last),
        WouldBlock {
            offset: 4088,
            len: 8,
        },
    )?;
    sparse.insert(4088, &source[4088..])?;
    sparse.read_exact(&mut last)?;
    assert_eq!(last, source[4088..]);

    sparse.seek(SeekFrom::Current(-4))?;
    let mut suffix = [0; 4];
    sparse.read_exact(&mut suffix)?;
    assert_eq!(suffix, source[4092..]);
    Ok(())
}

fn expect_miss(result: std::io::Result<()>, expected: WouldBlock) -> TestResult {
    expect_miss_value(result, expected)
}

fn expect_miss_value<T: std::fmt::Debug>(
    result: std::io::Result<T>,
    expected: WouldBlock,
) -> TestResult {
    let error = match result {
        Ok(value) => return Err(format!("uncached read unexpectedly succeeded: {value:?}").into()),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
    assert_eq!(would_block(&error), Some(expected));
    Ok(())
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn miss_preserves_position_and_reports_exact_span() -> TestResult {
    let mut sparse = SparseRange::new(2048, 32, 128)?;
    sparse.seek(SeekFrom::Start(513))?;
    let mut bytes = [0; 17];
    expect_miss_value(
        sparse.read(&mut bytes),
        WouldBlock {
            offset: 513,
            len: 17,
        },
    )?;
    assert_eq!(sparse.position(), 513);
    sparse.insert(513, &[7; 17])?;
    assert_eq!(sparse.read(&mut bytes)?, 17);
    assert_eq!(bytes, [7; 17]);
    assert_eq!(sparse.position(), 530);
    Ok(())
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn miss_does_not_refetch_the_cached_prefix() -> TestResult {
    let mut sparse = SparseRange::new(2048, 32, 128)?;
    sparse.insert(0, &[1])?;
    sparse.seek(SeekFrom::Start(1))?;
    let mut bytes = [0; 8];
    expect_miss_value(sparse.read(&mut bytes), WouldBlock { offset: 1, len: 8 })?;
    sparse.insert(1, &[2; 8])?;
    sparse.read_exact(&mut bytes)?;
    assert_eq!(bytes, [2; 8]);
    Ok(())
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn lru_evicts_bytes_not_only_entries() -> TestResult {
    let mut sparse = SparseRange::new(4096, 16, 32)?;
    sparse.insert(0, &[1; 16])?;
    sparse.insert(32, &[2; 16])?;
    assert_eq!(sparse.cached_bytes(), 32);

    let mut byte = [0];
    sparse.seek(SeekFrom::Start(0))?;
    sparse.read_exact(&mut byte)?;
    sparse.insert(64, &[3; 16])?;
    assert_eq!(sparse.cached_bytes(), 32);

    sparse.seek(SeekFrom::Start(32))?;
    expect_miss_value(sparse.read(&mut byte), WouldBlock { offset: 32, len: 1 })?;
    sparse.seek(SeekFrom::Start(0))?;
    sparse.read_exact(&mut byte)?;
    assert_eq!(byte, [1]);
    Ok(())
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn overlapping_inserts_do_not_double_account_cache_bytes() -> TestResult {
    let mut sparse = SparseRange::new(4096, 16, 32)?;
    sparse.insert(0, &[1; 16])?;
    let replacement: Vec<u8> = (1..=16).collect();
    sparse.insert(8, &replacement)?;
    assert_eq!(sparse.cached_bytes(), 24);
    assert_eq!(sparse.cached_ranges(), 1);

    sparse.seek(SeekFrom::Start(6))?;
    let mut bytes = [0; 12];
    sparse.read_exact(&mut bytes)?;
    assert_eq!(bytes, [1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    Ok(())
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn coalesces_only_small_gaps_within_the_range_cap() -> TestResult {
    let mut sparse = SparseRange::new(16 * 1024, 32, 256)?;
    sparse.insert(100, &[1; 4])?;
    sparse.seek(SeekFrom::Start(108))?;
    let mut bytes = [0; 4];
    expect_miss_value(
        sparse.read(&mut bytes),
        WouldBlock {
            offset: 108,
            len: 4,
        },
    )?;

    sparse.insert(104, &[2; 8])?;
    sparse.seek(SeekFrom::Start(104))?;
    sparse.read_exact(&mut bytes)?;
    assert_eq!(bytes, [2; 4]);
    sparse.seek(SeekFrom::Start(108))?;
    sparse.read_exact(&mut bytes)?;
    assert_eq!(bytes, [2; 4]);

    sparse.insert(1000, &[3; 4])?;
    sparse.seek(SeekFrom::Start(1008))?;
    expect_miss_value(
        sparse.read(&mut bytes),
        WouldBlock {
            offset: 1008,
            len: 4,
        },
    )?;

    sparse.seek(SeekFrom::Start(6000))?;
    expect_miss_value(
        sparse.read(&mut bytes),
        WouldBlock {
            offset: 6000,
            len: 4,
        },
    )?;

    let mut capped = SparseRange::new(4096, 24, 64)?;
    capped.insert(0, &[0; 8])?;
    capped.seek(SeekFrom::Start(16))?;
    let mut sixteen = [0; 16];
    expect_miss_value(
        capped.read(&mut sixteen),
        WouldBlock {
            offset: 16,
            len: 16,
        },
    )?;
    Ok(())
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn coalescing_never_turns_a_small_read_into_a_whole_object_fetch() -> TestResult {
    let mut sparse = SparseRange::new(1777, 2048, 4096)?;
    sparse.insert(1700, &[1; 77])?;
    sparse.seek(SeekFrom::Start(0))?;
    let mut header = [0; 8];
    expect_miss_value(sparse.read(&mut header), WouldBlock { offset: 0, len: 8 })?;
    Ok(())
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn huge_object_is_never_allocated_to_emulate_seek() -> TestResult {
    let one_tebibyte = 1_u64 << 40;
    let mut sparse = SparseRange::new(one_tebibyte, 64, 128)?;
    assert_eq!(sparse.cached_bytes(), 0);
    sparse.seek(SeekFrom::Start(one_tebibyte - 8))?;
    let mut bytes = [0; 8];
    expect_miss_value(
        sparse.read(&mut bytes),
        WouldBlock {
            offset: one_tebibyte - 8,
            len: 8,
        },
    )?;
    sparse.insert(one_tebibyte - 8, &[9; 8])?;
    sparse.read_exact(&mut bytes)?;
    assert_eq!(bytes, [9; 8]);
    assert_eq!(sparse.cached_bytes(), 8);
    Ok(())
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn fully_cached_flatgeobuf_matches_a_cursor() -> TestResult {
    let fixture = include_bytes!("../../../testdata/cities_12.a.fgb");
    for chunk_size in [fixture.len(), 40, 8] {
        let mut sparse = SparseRange::new(fixture.len() as u64, 2048, 4096)?;
        for (index, chunk) in fixture.chunks(chunk_size).enumerate() {
            sparse.insert((index * chunk_size) as u64, chunk)?;
        }
        let fgb = FgbReader::open(sparse)?;
        let mut selected = fgb.select_bbox(-180.0, -85.0511, 180.0, 85.0511)?;
        let mut count = 0;
        while selected.next()?.is_some() {
            count += 1;
        }
        assert_eq!(count, 12, "chunk size {chunk_size}");
    }
    Ok(())
}
