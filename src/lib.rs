#![deny(rust_2018_idioms)]

pub mod merge_accounts;

pub use merge_accounts::function::merge_accounts;

pub mod merge;
pub use merge::function::merge;

pub mod sle;

/// Transform typical numbers as encountered in GitHub CSV and stripe CSV and change their thousands and decimal separators.
pub fn normalize_number(
    number: impl Into<Vec<u8>>,
    thousands_separator: char,
    decimal_separator: char,
) -> Vec<u8> {
    fn is_separator(b: &&mut u8) -> bool {
        **b == b'.' || **b == b','
    }
    let mut number = number.into();
    let mut b = number.iter_mut().rev();
    let mut next_thousdands_ofs = 3;
    if let Some(b) = b.nth(2).filter(is_separator) {
        *b = decimal_separator.try_into().expect("sane separators")
    } else {
        next_thousdands_ofs = 0;
    }

    while let Some(b) = b.nth(next_thousdands_ofs) {
        if is_separator(&b) {
            *b = thousands_separator.try_into().expect("sane separators")
        }
        next_thousdands_ofs = 3;
    }
    number
}
