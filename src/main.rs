use crate::options::Args;
use anyhow::Context;
use clap::Parser;
use std::path::PathBuf;

mod options {
    use std::path::PathBuf;

    #[derive(Debug, clap::Parser)]
    #[structopt(name = "stool", about = "A tool to help dealing with sponsor data")]
    pub enum Args {
        /// Merge github activity and stripe information to learn about the sponsored values in local currency if that's not USD.
        MergeAccounts {
            /// The amount of seconds a stripe account transaction may be away from the best candidate in the sponsor list to be considered.
            #[clap(long, short = 'm', default_value = "5")]
            max_distance_seconds: u64,
            /// The non-overlapping CSV files obtained from a GitHub activity CSV export.
            #[clap(long, short = 'g')]
            github_activity: Vec<PathBuf>,
            /// The non-overlapping CSV files obtained from a stripe activity CSV export.
            #[clap(long, short = 's')]
            stripe_activity: Vec<PathBuf>,
        },
        /// Merge multiple files of the same kind with overlaps together into one stream without overlaps.
        ///
        /// Useful if you download all activity regularly, without fear of loosing older values which might be dropped by stripe
        /// at some point.
        Merge {
            #[clap(long, short = 'd')]
            delimiter: Option<char>,
            /// The index or name of the column to use as key for merging.
            ///
            /// Rows seen later with the key will overwrite those that are seen earlier.
            key_column: String,
            /// The index or name of the column to use for sorting the output.
            sort_column: String,
            /// One or more CSV files to merge - they must have the same shape and a header.
            csv_file: Vec<PathBuf>,
        },
    }
}

fn main() -> anyhow::Result<()> {
    let args = options::Args::parse();
    match args {
        Args::MergeAccounts {
            github_activity,
            stripe_activity,
            max_distance_seconds,
        } => stool::merge_accounts(
            into_read(github_activity)?,
            into_read(stripe_activity)?,
            std::io::BufWriter::new(std::io::stdout()),
            stool::merge_accounts::Options {
                max_distance_seconds,
                ..Default::default()
            },
        )?,
        Args::Merge {
            delimiter,
            key_column,
            sort_column,
            csv_file,
        } => stool::merge(
            into_read(csv_file)?,
            &[&key_column],
            std::io::BufWriter::new(std::io::stdout()),
            stool::merge::Options {
                delimiter: delimiter.unwrap_or(','),
                sort_column,
            },
        )
        .map(|_| ())?,
    };
    Ok(())
}

fn into_read(file_paths: Vec<PathBuf>) -> anyhow::Result<impl Iterator<Item = impl std::io::Read>> {
    Ok(file_paths
        .iter()
        .map(|p| {
            std::fs::read(p)
                .with_context(|| format!("Could not read from CSV file at '{}'", p.display()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(std::io::Cursor::new))
}
