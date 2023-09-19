#![deny(rust_2018_idioms)]

pub mod merge_accounts;
pub use merge_accounts::function::merge_accounts;

pub mod merge;
pub use merge::function::merge;
