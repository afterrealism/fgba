//! BODY_ANALYTICS layout, semantics, codec, and hostile-input tests.

#[cfg(feature = "analytics")]
mod enabled {
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow_ipc::reader::FileReader;
    use fgba::analytics::{
        arrow_data_type, ANALYTICS_VERSION, CHUNK_STAT_SIZE, COLUMN_DESC_SIZE, GLOBAL_STAT_SIZE,
        HAS_ARROW_IPC, MAX_ARROW_CHUNK_BYTES, SECTION_HEADER_SIZE,
    };
    use fgba::format::{Directory, Footer, SectionEntry, KIND_BODY_ANALYTICS, SENTINEL};
    use fgba::io::SharedReader;
    use fgba::{
        build, build_to_writer, build_trailer_streaming, build_with_options, BodyAnalytics,
        BuildOptions, Reader, ValueClass,
    };
    use flatgeobuf::geozero::{ColumnValue, GeomProcessor, GeozeroGeometry, PropertyProcessor};
    use flatgeobuf::packed_r_tree::NodeItem;
    use flatgeobuf::{
        ColumnType, FallibleStreamingIterator, FeatureProperties, FgbReader, FgbWriter,
        GeometryType,
    };
    use std::io::{Cursor, Read, Seek, SeekFrom};

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    #[derive(Clone, Copy)]
    struct Point(f64, f64);

    impl GeozeroGeometry for Point {
        fn process_geom<P: GeomProcessor>(
            &self,
            processor: &mut P,
        ) -> flatgeobuf::geozero::error::Result<()> {
            processor.point_begin(0)?;
            processor.xy(self.0, self.1, 0)?;
            processor.point_end(0)
        }
    }

    struct Collection(bool);

    impl GeozeroGeometry for Collection {
        fn process_geom<P: GeomProcessor>(
            &self,
            processor: &mut P,
        ) -> flatgeobuf::geozero::error::Result<()> {
            processor.geometrycollection_begin(usize::from(!self.0), 0)?;
            if !self.0 {
                processor.point_begin(0)?;
                processor.xy(0.0, 0.0, 0)?;
                processor.point_end(0)?;
            }
            processor.geometrycollection_end(0)
        }
    }

    fn body<'a>(
        columns: &[(&str, ColumnType, bool)],
        rows: Vec<Vec<Option<ColumnValue<'a>>>>,
    ) -> TestResult<Vec<u8>> {
        let mut writer = FgbWriter::create("analytics", GeometryType::Point)?;
        for (name, kind, nullable) in columns {
            writer.add_column(name, *kind, |_, args| args.nullable = *nullable);
        }
        for properties in rows.into_iter() {
            let mut property_error = None;
            writer.add_feature_geom(Point(0.0, 0.0), |feature| {
                for (column, value) in properties.into_iter().enumerate() {
                    let Some(value) = value else {
                        continue;
                    };
                    if let Err(error) = feature.property(column, columns[column].0, &value) {
                        property_error = Some(error);
                        return;
                    }
                }
            })?;
            if let Some(error) = property_error {
                return Err(error.into());
            }
        }
        let mut bytes = Vec::new();
        writer.write(&mut bytes)?;
        Ok(bytes)
    }

    fn one_string_body(rows: usize) -> TestResult<Vec<u8>> {
        let names: Vec<String> = (0..rows).map(|row| format!("city{row}")).collect();
        body(
            &[("name", ColumnType::String, true)],
            names
                .iter()
                .map(|name| vec![Some(ColumnValue::String(name.as_str()))])
                .collect(),
        )
    }

    fn analytics_options(chunk_rows: u32) -> BuildOptions {
        BuildOptions {
            analytics: true,
            geometry_metrics: false,
            chunk_rows,
        }
    }

    struct ShortReads<R> {
        inner: R,
        max_read: usize,
    }

    impl<R: Read> Read for ShortReads<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let len = buf.len().min(self.max_read);
            self.inner.read(&mut buf[..len])
        }
    }

    impl<R: Seek> Seek for ShortReads<R> {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            self.inner.seek(pos)
        }
    }

    struct OpenMonitor<R> {
        inner: R,
        data_start: u64,
        body_len: u64,
        data_bytes_read: u64,
        seeks: usize,
    }

    impl<R: Read + Seek> Read for OpenMonitor<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let start = self.inner.stream_position()?;
            let count = self.inner.read(buf)?;
            let end = start + count as u64;
            self.data_bytes_read += end
                .min(self.body_len)
                .saturating_sub(start.max(self.data_start));
            Ok(count)
        }
    }

    impl<R: Seek> Seek for OpenMonitor<R> {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            self.seeks += 1;
            self.inner.seek(pos)
        }
    }

    fn assert_streaming_matches_in_memory(body: &[u8], options: &BuildOptions) -> TestResult {
        let (expected, expected_report) = build_with_options(body, options)?;
        let (trailer, report) = build_trailer_streaming(
            ShortReads {
                inner: Cursor::new(body),
                max_read: 31,
            },
            body.len() as u64,
            options,
        )?;
        assert_eq!(report, expected_report);
        assert_eq!(trailer.as_slice(), &expected[body.len()..]);
        let mut complete = body.to_vec();
        complete.extend_from_slice(&trailer);
        assert_eq!(complete, expected);

        let mut file_output = Vec::new();
        let file_report = build_to_writer(
            ShortReads {
                inner: Cursor::new(body),
                max_read: 31,
            },
            body.len() as u64,
            options,
            &mut file_output,
        )?;
        assert_eq!(file_report, expected_report);
        assert_eq!(file_output, expected);
        Ok(())
    }

    fn open_analytics(file: &[u8]) -> TestResult<BodyAnalytics> {
        Ok(Reader::open(Cursor::new(file))?
            .read_body_analytics_metadata()?
            .ok_or("missing BODY_ANALYTICS")?)
    }

    fn analytics_entry(file: &[u8]) -> TestResult<SectionEntry> {
        let reader = Reader::open(Cursor::new(file))?;
        let entries: Vec<_> = reader
            .directory()
            .sections
            .iter()
            .filter(|section| section.kind == KIND_BODY_ANALYTICS)
            .cloned()
            .collect();
        assert_eq!(entries.len(), 1);
        Ok(entries.into_iter().next().ok_or("missing entry")?)
    }

    fn decode_chunk(bytes: Vec<u8>) -> TestResult<arrow_array::RecordBatch> {
        let reader = FileReader::try_new(Cursor::new(bytes), None)?;
        assert_eq!(reader.schema().fields().len(), 1);
        assert_eq!(reader.num_batches(), 1);
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(batches.len(), 1);
        Ok(batches.into_iter().next().ok_or("missing batch")?)
    }

    fn put_u32(bytes: &mut [u8], at: usize, value: u32) {
        bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn put_u64(bytes: &mut [u8], at: usize, value: u64) {
        bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
    }

    #[test]
    fn default_identity_property_free_and_section_layout() -> TestResult {
        let plain_body = one_string_body(5)?;
        let (ordinary, ordinary_report) = build(&plain_body)?;
        let (default_options, default_report) =
            build_with_options(&plain_body, &BuildOptions::default())?;
        assert_eq!(ordinary, default_options);
        assert_eq!(ordinary_report, default_report);
        assert_eq!(ordinary_report.attribute_columns, 0);
        assert!(!ordinary_report.analytics_requested);
        assert!(Reader::open(Cursor::new(&ordinary))?
            .read_body_analytics_metadata()?
            .is_none());

        let (file, report) = build_with_options(&plain_body, &analytics_options(2))?;
        assert_eq!(report.attribute_columns, 1);
        assert!(report.analytics_requested);
        let entry = analytics_entry(&file)?;
        assert_eq!(entry.flags, 0);
        assert!(entry.meta.is_empty());
        let analytics = open_analytics(&file)?;
        assert_eq!(analytics.row_count, 5);
        assert_eq!(analytics.chunk_rows, 2);
        assert_eq!(analytics.column_count, 1);
        assert_eq!(analytics.chunk_count, 3);
        assert_eq!(analytics.chunk_offsets.len(), 4);
        assert_eq!(
            *analytics.chunk_offsets.last().ok_or("no offsets")?,
            plain_body.len() as u64
        );
        assert!(analytics
            .chunk_offsets
            .windows(2)
            .all(|pair| pair[0] < pair[1]));
        assert_eq!(analytics.columns.len(), 1);
        let column = &analytics.columns[0];
        assert_eq!(column.descriptor.flags, HAS_ARROW_IPC);
        assert_eq!(column.descriptor.column_index, 0);
        assert_eq!(column.descriptor.fgb_type, ColumnType::String.0);
        assert_eq!(column.descriptor.value_class, ValueClass::Opaque);
        let c = analytics.chunk_count;
        assert_eq!(
            column.descriptor.arrow_offsets_offset,
            column.descriptor.stream_offset + GLOBAL_STAT_SIZE as u64 + CHUNK_STAT_SIZE as u64 * c
        );
        assert_eq!(
            column.descriptor.stream_size,
            GLOBAL_STAT_SIZE as u64
                + CHUNK_STAT_SIZE as u64 * c
                + 8 * (c + 1)
                + column.descriptor.arrow_bytes
        );
        assert_eq!(
            *column.arrow_offsets.last().ok_or("no Arrow offsets")?,
            analytics.section_start
                + column.descriptor.stream_offset
                + column.descriptor.stream_size
        );
        assert_eq!(
            entry.size,
            SECTION_HEADER_SIZE as u64
                + COLUMN_DESC_SIZE as u64
                + 8 * (c + 1)
                + column.descriptor.stream_size
        );
        assert_eq!(file[entry.offset as usize], ANALYTICS_VERSION);
        assert_eq!(analytics.metadata_bytes_read, 264);

        let property_free = body(&[], vec![vec![], vec![]])?;
        let (no_columns, report) = build_with_options(&property_free, &analytics_options(2))?;
        assert_eq!(report.attribute_columns, 0);
        assert!(report.analytics_requested);
        assert!(Reader::open(Cursor::new(&no_columns))?
            .read_body_analytics_metadata()?
            .is_none());
        assert!(!Reader::open(Cursor::new(&no_columns))?
            .directory()
            .sections
            .iter()
            .any(|section| section.kind == KIND_BODY_ANALYTICS));
        Ok(())
    }

    #[test]
    fn valid_optional_first_open_reads_only_the_last_feature_prefix() -> TestResult {
        let body = one_string_body(12)?;
        let (file, _) = build_with_options(&body, &analytics_options(2))?;
        let data_start = open_analytics(&file)?.chunk_offsets[0];
        let monitored = OpenMonitor {
            inner: Cursor::new(&file),
            data_start,
            body_len: body.len() as u64,
            data_bytes_read: 0,
            seeks: 0,
        };

        let reader = Reader::open(monitored)?;
        assert_eq!(reader.body_len(), body.len() as u64);
        let monitored = reader.into_inner();
        assert_eq!(
            monitored.data_bytes_read, 4,
            "open scanned feature data instead of reading one u32 length"
        );
        assert!(
            monitored.seeks <= 8,
            "open used {} seeks instead of a bounded handful",
            monitored.seeks
        );
        Ok(())
    }

    fn stats_body() -> TestResult<Vec<u8>> {
        body(
            &[
                ("signed", ColumnType::Long, true),
                ("unsigned", ColumnType::ULong, true),
                ("float", ColumnType::Double, true),
                ("finite", ColumnType::Double, true),
                ("single", ColumnType::Float, true),
                ("text", ColumnType::String, true),
            ],
            vec![
                vec![
                    Some(ColumnValue::Long(-5)),
                    Some(ColumnValue::ULong(u64::MAX)),
                    Some(ColumnValue::Double(1.5)),
                    Some(ColumnValue::Double(1.0)),
                    Some(ColumnValue::Float(1.25)),
                    Some(ColumnValue::String("a")),
                ],
                vec![
                    None,
                    None,
                    None,
                    Some(ColumnValue::Double(2.0)),
                    Some(ColumnValue::Float(-0.0)),
                    None,
                ],
                vec![
                    Some(ColumnValue::Long(9_007_199_254_740_993)),
                    Some(ColumnValue::ULong(3)),
                    Some(ColumnValue::Double(f64::NAN)),
                    None,
                    Some(ColumnValue::Float(f32::NAN)),
                    Some(ColumnValue::String("c")),
                ],
                vec![
                    Some(ColumnValue::Long(2)),
                    Some(ColumnValue::ULong(4)),
                    Some(ColumnValue::Double(f64::INFINITY)),
                    Some(ColumnValue::Double(4.0)),
                    None,
                    None,
                ],
                vec![
                    Some(ColumnValue::Long(7)),
                    Some(ColumnValue::ULong(5)),
                    Some(ColumnValue::Double(-0.0)),
                    Some(ColumnValue::Double(-0.0)),
                    Some(ColumnValue::Float(3.5)),
                    Some(ColumnValue::String("e")),
                ],
            ],
        )
    }

    #[test]
    fn streaming_analytics_matches_in_memory_across_chunks_and_edge_cases() -> TestResult {
        for (body, chunk_rows) in [(one_string_body(12)?, 4), (stats_body()?, 2)] {
            assert_streaming_matches_in_memory(&body, &analytics_options(chunk_rows))?;
        }

        let overflow = body(
            &[
                ("overflow", ColumnType::Double, true),
                ("all_null", ColumnType::Long, true),
            ],
            vec![
                vec![Some(ColumnValue::Double(f64::MAX)), None],
                vec![Some(ColumnValue::Double(f64::MAX)), None],
            ],
        )?;
        assert_streaming_matches_in_memory(&overflow, &analytics_options(2))?;

        let property_free = body(&[], vec![vec![], vec![], vec![]])?;
        assert_streaming_matches_in_memory(&property_free, &analytics_options(2))?;
        Ok(())
    }

    #[test]
    fn streaming_analytics_rejects_wrong_body_len_before_output() -> TestResult {
        let body = one_string_body(5)?;
        for wrong_len in [body.len() as u64 - 1, body.len() as u64 + 1] {
            let mut output = Vec::new();
            let error = build_to_writer(
                Cursor::new(&body),
                wrong_len,
                &analytics_options(2),
                &mut output,
            )
            .err()
            .ok_or("wrong body length unexpectedly succeeded")?;
            let expected = format!(
                "FlatGeobuf body terminal {} does not match supplied body length {wrong_len}",
                body.len()
            );
            assert!(
                matches!(error, fgba::Error::Format(message) if message == expected),
                "wrong body length produced an unexpected error"
            );
            assert!(output.is_empty(), "writer received bytes before rejection");
        }
        Ok(())
    }

    #[test]
    fn automatic_analytics_chunks_match_fixed_4096_for_small_bodies() -> TestResult {
        let body = one_string_body(12)?;
        let automatic = BuildOptions {
            analytics: true,
            ..BuildOptions::default()
        };
        assert_streaming_matches_in_memory(&body, &automatic)?;

        let (automatic_file, _) = build_with_options(&body, &automatic)?;
        let (fixed_file, _) = build_with_options(&body, &analytics_options(4096))?;
        assert_eq!(automatic_file, fixed_file);
        let analytics = open_analytics(&automatic_file)?;
        assert_eq!(analytics.chunk_rows, 4096);
        assert_eq!(analytics.chunk_count, 1);
        Ok(())
    }

    #[test]
    fn exact_global_and_chunk_statistics() -> TestResult {
        let (file, _) = build_with_options(&stats_body()?, &analytics_options(2))?;
        let analytics = open_analytics(&file)?;
        assert_eq!(analytics.row_count, 5);
        assert_eq!(analytics.chunk_count, 3);
        let signed = analytics.column(0).ok_or("signed")?;
        assert_eq!(signed.global.non_null_count, 4);
        assert_eq!(signed.global.nan_count, 0);
        assert_eq!(signed.global.min_bits as i64, -5);
        assert_eq!(signed.global.max_bits as i64, 9_007_199_254_740_993);
        assert_eq!(
            i128::from_le_bytes(signed.global.sum),
            -5 + 9_007_199_254_740_993i128 + 2 + 7
        );
        assert!(signed.global.sum_valid());
        assert_eq!(
            signed.chunks.iter().map(|c| c.non_null_count).sum::<u32>(),
            4
        );

        let unsigned = analytics.column(1).ok_or("unsigned")?;
        assert_eq!(unsigned.global.min_bits, 3);
        assert_eq!(unsigned.global.max_bits, u64::MAX);
        assert_eq!(
            u128::from_le_bytes(unsigned.global.sum),
            u128::from(u64::MAX) + 3 + 4 + 5
        );

        let float = analytics.column(2).ok_or("float")?;
        assert_eq!(float.global.non_null_count, 4);
        assert_eq!(float.global.nan_count, 1);
        assert_eq!(f64::from_bits(float.global.min_bits), -0.0);
        assert!(f64::from_bits(float.global.min_bits).is_sign_negative());
        assert_eq!(f64::from_bits(float.global.max_bits), f64::INFINITY);
        assert!(!float.global.sum_valid());
        assert!(float.global.has_infinity());
        assert_eq!(float.global.sum, [0; 16]);
        assert_eq!(float.chunks[0].non_null_count, 1);
        assert_eq!(float.chunks[1].non_null_count, 2);
        assert_eq!(float.chunks[1].nan_count, 1);
        assert_eq!(f64::from_bits(float.chunks[1].min_bits), f64::INFINITY);
        assert!(f64::from_bits(float.chunks[2].min_bits).is_sign_negative());
        assert!(f64::from_bits(float.chunks[2].max_bits).is_sign_positive());

        let finite = analytics.column(3).ok_or("finite")?;
        assert!(finite.global.sum_valid());
        assert_eq!(f64::from_le_bytes(finite.global.sum[..8].try_into()?), 7.0);
        assert_eq!(&finite.global.sum[8..], &[0; 8]);
        assert_eq!(finite.global.non_null_count, 4);
        assert!(f64::from_bits(finite.global.min_bits).is_sign_negative());

        let single = analytics.column(4).ok_or("single")?;
        assert_eq!(single.global.non_null_count, 4);
        assert_eq!(single.global.nan_count, 1);
        assert_eq!(f64::from_bits(single.global.max_bits), 3.5);
        assert!(!single.global.sum_valid());

        let text = analytics.column(5).ok_or("text")?;
        assert_eq!(text.global.non_null_count, 3);
        assert_eq!(text.global.nan_count, 0);
        assert_eq!(text.global.min_bits, 0);
        assert_eq!(text.global.max_bits, 0);
        assert_eq!(text.global.sum, [0; 16]);
        assert_eq!(text.global.flags, 0);
        assert!(text
            .chunks
            .iter()
            .all(|chunk| chunk.min_bits == 0 && chunk.max_bits == 0));
        Ok(())
    }

    #[test]
    fn float_overflow_and_all_null_numeric_storage() -> TestResult {
        let body = body(
            &[
                ("overflow", ColumnType::Double, true),
                ("all_null", ColumnType::Long, true),
            ],
            vec![
                vec![Some(ColumnValue::Double(f64::MAX)), None],
                vec![Some(ColumnValue::Double(f64::MAX)), None],
            ],
        )?;
        let (file, _) = build_with_options(&body, &analytics_options(2))?;
        let analytics = open_analytics(&file)?;
        let overflow = analytics.column(0).ok_or("overflow")?;
        assert_eq!(overflow.global.non_null_count, 2);
        assert!(!overflow.global.sum_valid());
        assert!(!overflow.global.has_infinity());
        assert_eq!(overflow.global.sum, [0; 16]);
        let all_null = analytics.column(1).ok_or("all_null")?;
        assert_eq!(all_null.global.non_null_count, 0);
        assert_eq!(all_null.global.min_bits, 0);
        assert_eq!(all_null.global.max_bits, 0);
        assert_eq!(all_null.global.sum, [0; 16]);
        assert!(all_null.global.sum_valid());
        assert!(all_null
            .chunks
            .iter()
            .all(|chunk| chunk.non_null_count == 0));
        Ok(())
    }

    #[test]
    fn every_arrow_type_round_trips_values_and_validity() -> TestResult {
        let columns = [
            ("byte", ColumnType::Byte),
            ("ubyte", ColumnType::UByte),
            ("bool", ColumnType::Bool),
            ("short", ColumnType::Short),
            ("ushort", ColumnType::UShort),
            ("int", ColumnType::Int),
            ("uint", ColumnType::UInt),
            ("long", ColumnType::Long),
            ("ulong", ColumnType::ULong),
            ("float", ColumnType::Float),
            ("double", ColumnType::Double),
            ("string", ColumnType::String),
            ("json", ColumnType::Json),
            ("datetime", ColumnType::DateTime),
            ("binary", ColumnType::Binary),
        ];
        let declarations: Vec<_> = columns
            .iter()
            .map(|(name, kind)| (*name, *kind, true))
            .collect();
        let first = vec![
            Some(ColumnValue::Byte(-8)),
            Some(ColumnValue::UByte(8)),
            Some(ColumnValue::Bool(true)),
            Some(ColumnValue::Short(-16)),
            Some(ColumnValue::UShort(16)),
            Some(ColumnValue::Int(-32)),
            Some(ColumnValue::UInt(32)),
            Some(ColumnValue::Long(-64)),
            Some(ColumnValue::ULong(64)),
            Some(ColumnValue::Float(1.25)),
            Some(ColumnValue::Double(2.5)),
            Some(ColumnValue::String("text")),
            Some(ColumnValue::Json("{\"a\":1}")),
            Some(ColumnValue::DateTime("2026-10-25T01:02:03")),
            Some(ColumnValue::Binary(&[0, 1, 255])),
        ];
        let body = body(
            &declarations,
            vec![first, (0..columns.len()).map(|_| None).collect()],
        )?;
        let (file, _) = build_with_options(&body, &analytics_options(1))?;
        let mut reader = Reader::open(Cursor::new(&file))?;
        let analytics = reader
            .read_body_analytics_metadata()?
            .ok_or("missing analytics")?;
        assert_eq!(analytics.column_count as usize, columns.len());
        assert_eq!(analytics.chunk_count, 2);
        for column in &analytics.columns {
            assert_eq!(column.global.non_null_count, 1);
            let first = reader.read_arrow_chunk(column.descriptor.column_index, 0)?;
            assert_eq!(first.column_index, column.descriptor.column_index);
            assert_eq!(first.chunk_index, 0);
            let first = decode_chunk(first.bytes)?;
            assert_eq!(first.num_rows(), 1);
            assert_eq!(first.num_columns(), 1);
            assert_eq!(
                first.column(0).data_type(),
                &arrow_data_type(column.descriptor.fgb_type)?
            );
            assert!(!first.column(0).is_null(0));
            let second = decode_chunk(
                reader
                    .read_arrow_chunk(column.descriptor.column_index, 1)?
                    .bytes,
            )?;
            assert_eq!(second.num_rows(), 1);
            assert!(second.column(0).is_null(0));
            if column.descriptor.value_class == ValueClass::Opaque {
                assert_eq!(column.global.flags, 0);
                assert_eq!(column.global.sum, [0; 16]);
            }
        }

        macro_rules! assert_value {
            ($column:expr, $array:ident, $expected:expr) => {{
                let bytes = reader.read_arrow_chunk($column, 0)?.bytes;
                let batch = decode_chunk(bytes)?;
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or("wrong Arrow array type")?;
                assert_eq!(array.value(0), $expected);
            }};
        }
        assert_value!(0, Int8Array, -8);
        assert_value!(1, UInt8Array, 8);
        assert_value!(2, BooleanArray, true);
        assert_value!(3, Int16Array, -16);
        assert_value!(4, UInt16Array, 16);
        assert_value!(5, Int32Array, -32);
        assert_value!(6, UInt32Array, 32);
        assert_value!(7, Int64Array, -64);
        assert_value!(8, UInt64Array, 64);
        assert_value!(9, Float32Array, 1.25);
        assert_value!(10, Float64Array, 2.5);
        assert_value!(11, StringArray, "text");
        assert_value!(12, StringArray, "{\"a\":1}");
        assert_value!(13, StringArray, "2026-10-25T01:02:03");
        assert_value!(14, BinaryArray, &[0, 1, 255][..]);
        assert_streaming_matches_in_memory(&body, &analytics_options(1))?;
        Ok(())
    }

    #[test]
    fn chunk_start_body_offsets_and_leaf_alignment() -> TestResult {
        let body = one_string_body(12)?;
        let (file, _) = build_with_options(&body, &analytics_options(4))?;
        let analytics = open_analytics(&file)?;
        assert_eq!(analytics.chunk_count, 3);
        assert_eq!(
            *analytics.chunk_offsets.last().ok_or("terminal")?,
            body.len() as u64
        );

        let mut all = FgbReader::open(Cursor::new(&body))?.select_all()?;
        let mut names = Vec::new();
        while let Some(feature) = all.next()? {
            names.push(feature.property::<String>("name")?);
        }
        assert_eq!(names.len(), 12);

        let shared = SharedReader::new_at(Cursor::new(&body), analytics.chunk_offsets[0]);
        let mut first = FgbReader::open(shared.handle())?.select_all()?;
        for expected in names.iter().take(4) {
            assert_eq!(
                first
                    .next()?
                    .map(|feature| feature.property::<String>("name"))
                    .transpose()?,
                Some(expected.clone())
            );
        }

        let shared = SharedReader::new_at(Cursor::new(&body), analytics.chunk_offsets[1]);
        let mut middle = FgbReader::open(shared.handle())?.select_all()?;
        assert_eq!(
            middle
                .next()?
                .map(|feature| feature.property::<String>("name"))
                .transpose()?,
            Some(names[4].clone())
        );
        let mut remaining = 1;
        while middle.next()?.is_some() {
            remaining += 1;
        }
        assert_eq!(remaining, 8);
        assert!(shared.position() > analytics.chunk_offsets[1]);
        assert!(shared.bytes_read() > 0);

        let data_start = analytics.chunk_offsets[0];
        let mut leaves = Cursor::new(&body);
        leaves.seek(SeekFrom::Start(data_start - 12 * 40))?;
        let shared = SharedReader::new(Cursor::new(&body));
        let mut features = FgbReader::open(shared.handle())?.select_all()?;
        let mut current = data_start;
        for _ in 0..12 {
            let node = NodeItem::from_reader(&mut leaves)?;
            assert_eq!(data_start + node.offset, current);
            assert!(features.next()?.is_some());
            current = shared.position();
        }
        assert!(features.next()?.is_none());
        assert_eq!(current, body.len() as u64);
        Ok(())
    }

    #[test]
    fn skipped_geometry_still_counts_in_analytics() -> TestResult {
        let mut writer = FgbWriter::create("empty", GeometryType::GeometryCollection)?;
        writer.add_column("value", ColumnType::Long, |_, _| {});
        writer.add_feature_geom(Collection(false), |feature| {
            let _ = feature.property(0, "value", &ColumnValue::Long(10));
        })?;
        writer.add_feature_geom(Collection(true), |feature| {
            let _ = feature.property(0, "value", &ColumnValue::Long(20));
        })?;
        let mut body = Vec::new();
        writer.write(&mut body)?;
        let (file, report) = build_with_options(&body, &analytics_options(2))?;
        assert_eq!(report.skipped, 1);
        assert_eq!(report.features, 1);
        let analytics = open_analytics(&file)?;
        assert_eq!(analytics.row_count, 2);
        assert_eq!(analytics.columns[0].global.non_null_count, 2);
        assert_eq!(i128::from_le_bytes(analytics.columns[0].global.sum), 30);
        assert_eq!(Reader::open(Cursor::new(file))?.read_band(2)?.total, 1);
        assert_streaming_matches_in_memory(&body, &analytics_options(2))?;
        Ok(())
    }

    #[test]
    fn kind_one_and_body_remain_unchanged_with_kind_two() -> TestResult {
        let body = one_string_body(12)?;
        let (ordinary, _) = build(&body)?;
        let (with_analytics, _) = build_with_options(&body, &analytics_options(2))?;
        let mut ordinary_reader = Reader::open(Cursor::new(&ordinary))?;
        let mut analytics_reader = Reader::open(Cursor::new(&with_analytics))?;
        assert_eq!(
            ordinary_reader.bands().len(),
            analytics_reader.bands().len()
        );
        for band in ordinary_reader.bands() {
            assert_eq!(
                ordinary_reader.read_band(band.res)?,
                analytics_reader.read_band(band.res)?
            );
            assert_eq!(
                ordinary_reader.stats([-180.0, -85.0, 180.0, 85.0], band.res)?,
                analytics_reader.stats([-180.0, -85.0, 180.0, 85.0], band.res)?
            );
        }
        let mut features = FgbReader::open(analytics_reader.body()?)?.select_all()?;
        let mut rows = 0;
        while features.next()?.is_some() {
            rows += 1;
        }
        assert_eq!(rows, 12);
        Ok(())
    }

    #[test]
    fn malformed_descriptors_counts_offsets_and_reserved_bytes_are_rejected() -> TestResult {
        let body = one_string_body(5)?;
        let (file, _) = build_with_options(&body, &analytics_options(2))?;
        let entry = analytics_entry(&file)?;
        let start = entry.offset as usize;
        let first_stream = start + SECTION_HEADER_SIZE + COLUMN_DESC_SIZE + 8 * 4;
        let cases: Vec<(usize, u64)> = vec![
            (start + 2, 1),                                       // header reserved
            (start + 12, 1),                                      // header reserved
            (start + 4, 0),                                       // zero chunk_rows
            (start + 24, 4),                                      // wrong chunk_count
            (start + 32, 1),                                      // duplicate/wrong column index
            (start + 32 + 2, 99),                                 // unsupported FGB type
            (start + 32 + 3, 99),                                 // unsupported value class
            (start + 32 + 4, 0),                                  // missing Arrow flag
            (start + 32 + 8, 0),                                  // wrong stream offset
            (start + 32 + 16, 1),                                 // wrong stream size
            (start + 32 + 24, 0),                                 // wrong Arrow offsets location
            (start + 32 + 40, 0),                                 // first body offset outside body
            (start + 32 + 40 + 3 * 8, 0), // terminal body offset outside body
            (first_stream + 52, 1),       // global reserved
            (first_stream, u64::MAX),     // global non-null count mismatch
            (first_stream + 8, u64::MAX), // opaque NaN/count mismatch
            (first_stream + 56 + 3 * CHUNK_STAT_SIZE + 3 * 8, 0), // terminal Arrow offset wrong
        ];
        for (at, value) in cases {
            let mut bad = file.clone();
            if at + 8 <= bad.len() {
                put_u64(&mut bad, at, value);
            } else {
                put_u32(&mut bad, at, value as u32);
            }
            let result = Reader::open(Cursor::new(bad))?.read_body_analytics_metadata();
            assert!(
                result.is_err(),
                "corruption at {at} was accepted (section {start}, first stream {first_stream})"
            );
        }

        let mut bad = file.clone();
        bad[start + 1] = 1;
        assert!(Reader::open(Cursor::new(bad))?
            .read_body_analytics_metadata()
            .is_err());
        let mut bad = file.clone();
        bad[start] = 2;
        assert!(Reader::open(Cursor::new(bad))?
            .read_body_analytics_metadata()
            .is_err());

        let mut reader = Reader::open(Cursor::new(&file))?;
        assert!(reader.read_arrow_chunk(99, 0).is_err());
        assert!(reader.read_arrow_chunk(0, 99).is_err());
        Ok(())
    }

    #[test]
    fn arrow_cap_and_malformed_ipc_fail_cleanly() -> TestResult {
        let (file, _) = build_with_options(&one_string_body(2)?, &analytics_options(2))?;
        let mut analytics = open_analytics(&file)?;
        let column = &mut analytics.columns[0];
        column.arrow_offsets[1] = column.arrow_offsets[0] + MAX_ARROW_CHUNK_BYTES + 1;
        assert!(analytics.arrow_range(0, 0).is_err());

        let mut reader = Reader::open(Cursor::new(&file))?;
        let chunk = reader.read_arrow_chunk(0, 0)?;
        let mut malformed = chunk.bytes;
        let footer_magic = malformed.len() - 6;
        malformed[footer_magic] = b'X';
        assert!(FileReader::try_new(Cursor::new(malformed), None).is_err());
        Ok(())
    }

    struct SectionCut<R> {
        inner: R,
        section_start: u64,
        section_end: u64,
        cut: u64,
    }

    impl<R: Read + Seek> Read for SectionCut<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let position = self.inner.stream_position()?;
            if position >= self.section_start && position < self.section_end && position >= self.cut
            {
                return Ok(0);
            }
            let allowed = if position >= self.section_start && position < self.cut {
                usize::try_from(self.cut - position)
                    .unwrap_or(buf.len())
                    .min(buf.len())
            } else {
                buf.len()
            };
            self.inner.read(&mut buf[..allowed])
        }
    }

    impl<R: Seek> Seek for SectionCut<R> {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            self.inner.seek(pos)
        }
    }

    #[test]
    fn every_metadata_truncation_is_an_error_without_body_or_band_hostage() -> TestResult {
        let (file, _) = build_with_options(&one_string_body(5)?, &analytics_options(2))?;
        let entry = analytics_entry(&file)?;
        let metadata_len = open_analytics(&file)?.metadata_bytes_read;
        for cut in 0..metadata_len {
            let source = SectionCut {
                inner: Cursor::new(&file),
                section_start: entry.offset,
                section_end: entry.offset + entry.size,
                cut: entry.offset + cut,
            };
            let mut reader = Reader::open(source)?;
            assert!(
                reader.read_body_analytics_metadata().is_err(),
                "metadata cut {cut} was accepted"
            );
            assert_eq!(reader.read_band(2)?.total, 5);
            let mut features = FgbReader::open(reader.body()?)?.select_all()?;
            let mut rows = 0;
            while features.next()?.is_some() {
                rows += 1;
            }
            assert_eq!(rows, 5);
        }
        Ok(())
    }

    #[test]
    fn coincidental_sentinel_in_binary_cannot_move_optional_body_boundary() -> TestResult {
        const PAYLOAD: &[u8; 6] = &[1, 255, 255, 255, 255, 2];
        let body = body(
            &[("payload", ColumnType::Binary, true)],
            vec![vec![Some(ColumnValue::Binary(PAYLOAD))]],
        )?;
        let (file, _) = build_with_options(&body, &analytics_options(1))?;
        let expected_band = Reader::open(Cursor::new(&file))?.read_band(2)?;
        let collision = body
            .windows(PAYLOAD.len())
            .position(|window| window == PAYLOAD)
            .ok_or("Binary payload is absent from the valid body")?;
        // Point at the final value byte, immediately after FF FF FF FF. The
        // old check accepted collision + 1 as body_len from the sentinel alone.
        let false_section_start = collision + 5;
        assert_eq!(
            &body[false_section_start - 4..false_section_start],
            &SENTINEL
        );

        let footer = Footer::decode(&file[file.len() - 32..])?;
        let dir_start = footer.dir_offset as usize;
        let mut directory =
            Directory::decode(&file[dir_start..dir_start + footer.dir_size as usize])?;
        let entry = directory
            .sections
            .iter_mut()
            .find(|section| section.kind == KIND_BODY_ANALYTICS)
            .ok_or("missing analytics entry")?;
        entry.offset = false_section_start as u64;
        entry.size = 1;
        let malformed = replace_directory(&file, &directory)?;

        let mut reader = Reader::open(Cursor::new(&malformed))?;
        assert_eq!(reader.body_len(), body.len() as u64);
        assert!(
            reader.read_body_analytics_metadata().is_err(),
            "the in-body kind-2 entry was not disabled"
        );
        assert_eq!(reader.read_band(2)?, expected_band);
        let mut recovered_body = vec![0; body.len()];
        reader.body()?.read_exact(&mut recovered_body)?;
        assert_eq!(recovered_body, body);
        Ok(())
    }

    #[test]
    fn duplicate_kind_two_and_directory_metadata_disable_only_analytics() -> TestResult {
        let (file, _) = build_with_options(&one_string_body(5)?, &analytics_options(2))?;
        let footer = Footer::decode(&file[file.len() - 32..])?;
        let dir_start = footer.dir_offset as usize;
        let directory = Directory::decode(&file[dir_start..dir_start + footer.dir_size as usize])?;

        let mut duplicate = directory.clone();
        let entry = duplicate
            .sections
            .iter()
            .find(|section| section.kind == KIND_BODY_ANALYTICS)
            .cloned()
            .ok_or("missing analytics")?;
        duplicate.sections.push(entry);
        let duplicate_bytes = replace_directory(&file, &duplicate)?;
        let mut reader = Reader::open(Cursor::new(duplicate_bytes))?;
        assert!(reader.read_body_analytics_metadata().is_err());
        assert_eq!(reader.read_band(2)?.total, 5);
        let mut features = FgbReader::open(reader.body()?)?.select_all()?;
        let mut rows = 0;
        while features.next()?.is_some() {
            rows += 1;
        }
        assert_eq!(rows, 5);

        let mut invalid = directory;
        for section in &mut invalid.sections {
            if section.kind == KIND_BODY_ANALYTICS {
                section.flags = 1;
            }
        }
        let invalid_bytes = replace_directory(&file, &invalid)?;
        let mut reader = Reader::open(Cursor::new(invalid_bytes))?;
        assert!(reader.read_body_analytics_metadata().is_err());
        assert_eq!(reader.read_band(2)?.total, 5);
        Ok(())
    }

    fn replace_directory(file: &[u8], directory: &Directory) -> TestResult<Vec<u8>> {
        let old_footer = Footer::decode(&file[file.len() - 32..])?;
        let dir_start = old_footer.dir_offset as usize;
        let encoded = directory.encode()?;
        let footer = Footer {
            dir_offset: old_footer.dir_offset,
            dir_size: encoded.len() as u64,
            dir_crc32: fgba::format::crc32(&encoded),
        };
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&file[..dir_start]);
        bytes.extend_from_slice(&encoded);
        bytes.extend_from_slice(&footer.encode());
        Ok(bytes)
    }
}

#[cfg(not(feature = "analytics"))]
mod disabled {
    use fgba::{build_trailer_streaming, build_with_options, BuildOptions, Error};
    use flatgeobuf::geozero::{GeomProcessor, GeozeroGeometry};
    use flatgeobuf::{FgbWriter, GeometryType};
    use std::io::Cursor;

    struct Point;

    impl GeozeroGeometry for Point {
        fn process_geom<P: GeomProcessor>(
            &self,
            processor: &mut P,
        ) -> flatgeobuf::geozero::error::Result<()> {
            processor.point_begin(0)?;
            processor.xy(0.0, 0.0, 0)?;
            processor.point_end(0)
        }
    }

    #[test]
    fn analytics_request_without_feature_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = FgbWriter::create("point", GeometryType::Point)?;
        writer.add_feature_geom(Point, |_| {})?;
        let mut body = Vec::new();
        writer.write(&mut body)?;
        let error = build_with_options(
            &body,
            &BuildOptions {
                analytics: true,
                geometry_metrics: false,
                chunk_rows: 2,
            },
        )
        .err()
        .ok_or("analytics unexpectedly succeeded")?;
        assert!(
            matches!(&error, Error::Format(message) if message == "analytics support disabled")
        );
        let error = build_trailer_streaming(
            Cursor::new(&body),
            body.len() as u64,
            &BuildOptions {
                analytics: true,
                geometry_metrics: false,
                chunk_rows: 2,
            },
        )
        .err()
        .ok_or("streaming analytics unexpectedly succeeded")?;
        assert!(
            matches!(&error, Error::Format(message) if message == "analytics support disabled")
        );
        Ok(())
    }
}
