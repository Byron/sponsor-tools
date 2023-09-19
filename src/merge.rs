#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("No input was provided")]
    NoInput,
    #[error("Cannot use '{0}' as delimiter")]
    InvalidDelimiter(char),
    #[error(transparent)]
    Csv(#[from] csv::Error),
    #[error(
        "A {kind} column of index or name '{name}' could not be found in first line of CSV file"
    )]
    MissingColumn { name: String, kind: &'static str },
    #[error("The schema changed between files as seen in change in the head line: {previous} != {current}")]
    SchemaChange { previous: String, current: String },
    #[error("Row in line {line} did not have a column at index {key_column_index}")]
    ColumnMissingInRow { line: u64, key_column_index: usize },
}

pub struct Outcome {
    /// The column indices of all provided keys.
    pub key_column_indices: Vec<usize>,
    /// The index of the sort column as determined by input index or name.
    pub sort_column_index: usize,
    /// The delimiter that was used to write the output with.
    pub delimiter: u8,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub sort_column: String,
    pub delimiter: char,
}

pub(crate) mod function {
    use crate::merge::{Error, Options, Outcome};
    use std::collections::BTreeMap;

    pub fn merge(
        csv_data: impl IntoIterator<Item = impl std::io::Read>,
        key_columns: &[&str],
        out: impl std::io::Write,
        Options {
            sort_column,
            delimiter,
        }: Options,
    ) -> Result<Outcome, Error> {
        let delimiter = delimiter
            .try_into()
            .map_err(|_| Error::InvalidDelimiter(delimiter))?;
        let mut data = BTreeMap::new();
        let mut previous_headers = None::<csv::StringRecord>;
        let mut sort_index = None;
        let mut key_column_indices = None;

        for csv in csv_data {
            let mut csv = csv::ReaderBuilder::new()
                .delimiter(delimiter)
                .has_headers(true)
                .from_reader(csv);
            let headers = csv.headers()?;
            if let Some(previous_headers) = &previous_headers {
                if previous_headers != headers {
                    return Err(Error::SchemaChange {
                        previous: to_string(previous_headers),
                        current: to_string(headers),
                    });
                }
            } else {
                previous_headers = Some(headers.clone());
            }

            let key_indices = key_columns
                .iter()
                .map(|key| {
                    header_idx(key, headers).ok_or_else(|| Error::MissingColumn {
                        name: key.to_string(),
                        kind: "key",
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            sort_index = header_idx(&sort_column, headers)
                .ok_or_else(|| Error::MissingColumn {
                    name: sort_column.clone(),
                    kind: "sort",
                })?
                .into();

            for record in csv.into_byte_records() {
                let record = record?;
                let mut key = Vec::<u8>::new();
                for key_index in &key_indices {
                    key.extend_from_slice(record.get(*key_index).ok_or_else(|| {
                        Error::ColumnMissingInRow {
                            line: record.position().expect("present").line(),
                            key_column_index: *key_index,
                        }
                    })?);
                }
                data.insert(key, record);
            }

            key_column_indices = Some(key_indices);
        }
        let sort_column_index = sort_index.ok_or(Error::NoInput)?;
        let mut records: Vec<_> = data.values().collect();
        records.sort_by_key(|record| record.get(sort_column_index));

        let headers = previous_headers.ok_or(Error::NoInput)?;
        let delimiter = b',';
        let mut out = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .from_writer(out);
        out.write_record(&headers)?;
        for record in &records {
            out.write_byte_record(record)?;
        }

        Ok(Outcome {
            sort_column_index,
            key_column_indices: key_column_indices.ok_or(Error::NoInput)?,
            delimiter,
        })
    }

    fn to_string(headers: &csv::StringRecord) -> String {
        let mut buf = Vec::<u8>::new();
        {
            let mut out = csv::Writer::from_writer(&mut buf);
            out.write_record(headers).ok();
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// Return the position of `name_or_index` in `headers` or `None` if it wasn't found.
    /// If `name_or_index` is a number, it will be used as number and not as name.
    fn header_idx(name_or_index: &str, headers: &csv::StringRecord) -> Option<usize> {
        if let Ok(index) = name_or_index.parse() {
            headers.get(index).map(|_| index)
        } else {
            headers.iter().position(|name| name == name_or_index)
        }
    }
}
