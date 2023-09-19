#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Csv(#[from] csv::Error),
    #[error(transparent)]
    Merge(#[from] crate::merge::Error),
    #[error("A {kind} column at index {index} in row at line {line} could not be found")]
    MissingColumn {
        index: usize,
        kind: String,
        line: u64,
    },
    #[error("Date '{date}' contained invalid UTf-8")]
    InvalidDateEncoding { date: String },
    #[error("Failed to parse time '{date_time}'")]
    ParseTime {
        date_time: String,
        source: time::error::Parse,
    },
    #[error("Failed to parse time '{date_time}'")]
    ParseGitTime {
        date_time: String,
        source: gix_date::parse::Error,
    },
}

impl Error {
    pub fn from_position(index: usize, pos: Option<&csv::Position>, kind: &str) -> Self {
        let pos = pos.expect("present");
        Error::MissingColumn {
            line: pos.line(),
            kind: kind.into(),
            index,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Options {
    pub stripe_date_column: String,
    pub stripe_time_column: String,
    pub stripe_delimiter: char,
    pub github_date_column: String,
    pub github_delimiter: char,
    pub max_distance_seconds: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            stripe_date_column: "Date".into(),
            stripe_time_column: "Time".into(),
            stripe_delimiter: ',',
            github_date_column: "Transaction Date".into(),
            github_delimiter: ',',
            max_distance_seconds: 10,
        }
    }
}

pub(crate) mod function {
    use crate::merge;
    use crate::merge_accounts::{Error, Options};

    pub fn merge_accounts(
        github_data: impl IntoIterator<Item = impl std::io::Read>,
        stripe_data: impl IntoIterator<Item = impl std::io::Read>,
        out: impl std::io::Write,
        Options {
            stripe_date_column,
            stripe_time_column,
            stripe_delimiter,
            github_date_column,
            github_delimiter,
            max_distance_seconds,
        }: Options,
    ) -> Result<(), Error> {
        let mut github_csv = Vec::<u8>::new();
        let merge::Outcome {
            sort_column_index: github_date_index,
            delimiter,
            ..
        } = merge(
            github_data,
            &[&github_date_column],
            &mut github_csv,
            merge::Options {
                sort_column: github_date_column.clone(),
                delimiter: github_delimiter,
            },
        )?;

        let mut github_csv = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(true)
            .from_reader(github_csv.as_slice());

        let mut stripe_csv = Vec::<u8>::new();
        let merge::Outcome {
            delimiter,
            key_column_indices,
            ..
        } = merge(
            stripe_data,
            &[&stripe_date_column, &stripe_time_column],
            &mut stripe_csv,
            merge::Options {
                sort_column: stripe_date_column.clone(),
                delimiter: stripe_delimiter,
            },
        )?;
        let mut stripe_csv = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(true)
            .from_reader(stripe_csv.as_slice());
        let stripe_column_count = stripe_csv.headers()?.len();

        let mut out = csv::WriterBuilder::new().delimiter(b',').from_writer(out);
        {
            let mut headers = github_csv.headers()?.clone();
            headers.push_field("Received Date");
            headers.push_field("Distance [s]");
            for field in stripe_csv.headers()? {
                headers.push_field(field);
            }
            out.write_record(&headers)?;
        }
        let mut stripe_lut = into_stripe_lut(
            &mut stripe_csv,
            key_column_indices[0],
            key_column_indices[1],
        )?;

        let mut record = csv::ByteRecord::new();
        while github_csv.read_byte_record(&mut record)? {
            let date_time = record.get(github_date_index).ok_or_else(|| {
                Error::from_position(github_date_index, record.position(), &github_date_column)
            })?;
            let date_time =
                std::str::from_utf8(date_time).map_err(|_| Error::InvalidDateEncoding {
                    date: String::from_utf8_lossy(date_time).into_owned(),
                })?;
            let date_time =
                gix_date::parse(date_time, None).map_err(|err| Error::ParseGitTime {
                    date_time: date_time.to_string(),
                    source: err,
                })?;
            let date_time = time::OffsetDateTime::from_unix_timestamp(date_time.seconds)
                .expect("this should always work for reasonable times")
                .to_offset(
                    time::UtcOffset::from_whole_seconds(date_time.offset)
                        .expect("reasonable offset"),
                );
            let stripe_row = match stripe_lut.binary_search_by(|row| row.date_time.cmp(&date_time))
            {
                Ok(idx) => Some((idx, 0)),
                Err(idx) => [Some(idx), idx.checked_sub(1), Some(idx + 1)]
                    .into_iter()
                    .flatten()
                    .filter_map(|idx| stripe_lut.get(idx).map(|row| (idx, row)))
                    .map(|(idx, row)| (idx, offset_of(row, &date_time)))
                    .filter(|t| t.1 .1.is_positive())
                    // stripe transactions can only happen after the corresponding github transaction
                    .min_by_key(|t| t.1 .1.whole_seconds().abs())
                    .and_then(|t| {
                        let distance = t.1 .1.whole_seconds().unsigned_abs();
                        (distance <= max_distance_seconds).then_some((t.0, distance))
                    }),
            }
            .map(|(idx, distance)| (&stripe_lut[idx], idx, distance));
            match stripe_row {
                Some((row, idx, distance)) => {
                    record.push_field(
                        row.date_time
                            .format(gix_date::time::format::ISO8601)
                            .expect("should always work")
                            .as_bytes(),
                    );
                    record.push_field(distance.to_string().as_bytes());
                    for field in &row.row {
                        record.push_field(field);
                    }
                    stripe_lut.remove(idx);
                }
                None => {
                    record.push_field(&[]); /* combined date-time */
                    record.push_field(&[]); /* distance */
                    for _ in 0..stripe_column_count {
                        record.push_field(&[]);
                    }
                }
            }
            out.write_byte_record(&record)?;
        }

        Ok(())
    }

    fn offset_of<'a>(
        row: &'a StripeRow,
        date_time: &time::OffsetDateTime,
    ) -> (&'a StripeRow, time::Duration) {
        (row, row.date_time - *date_time)
    }

    struct StripeRow {
        /// The date-time generated from the date and the time fields of the row.
        date_time: time::OffsetDateTime,
        /// The unaltered row itself.
        row: csv::ByteRecord,
    }

    /// Returns a Vec sorted by utc_instant for binary searches.
    fn into_stripe_lut(
        csv: &mut csv::Reader<&[u8]>,
        date_index: usize,
        time_index: usize,
    ) -> Result<Vec<StripeRow>, Error> {
        let mut out = Vec::new();
        let mut record = csv::ByteRecord::new();
        static FORMAT: &[time::format_description::FormatItem<'static>] = time::macros::format_description!(
            "[month repr:long] [day padding:none], [year][hour]:[minute]:[second] UTC"
        );
        while csv.read_byte_record(&mut record)? {
            let date = record
                .get(date_index)
                .ok_or_else(|| Error::from_position(date_index, record.position(), "date"))?;
            let time = record
                .get(time_index)
                .ok_or_else(|| Error::from_position(time_index, record.position(), "time"))?;

            let mut date_time = date.to_vec();
            date_time.extend_from_slice(time);
            let date_time =
                std::str::from_utf8(&date_time).map_err(|_| Error::InvalidDateEncoding {
                    date: String::from_utf8_lossy(&date_time).into_owned(),
                })?;
            let date_time = time::PrimitiveDateTime::parse(date_time, FORMAT).map_err(|err| {
                Error::ParseTime {
                    date_time: date_time.into(),
                    source: err,
                }
            })?;
            out.push(StripeRow {
                date_time: date_time.assume_offset(time::UtcOffset::UTC),
                row: record.clone(),
            })
        }
        out.sort_by_key(|row| row.date_time);
        Ok(out)
    }
}
