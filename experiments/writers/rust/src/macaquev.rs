use std::fmt;

/// Absolute, relative, or lossless per-value error bound.
#[derive(Debug, Copy, Clone, PartialEq)]
// The enum type used in ModelarDB-RS
pub enum ErrorBound {
    /// An error bound that guarantees each value cannot deviate more than the [`Value`].
    Absolute(f32),
    /// An error bound that guarantees each value cannot deviate more than 0.0% to 100.0%.
    Relative(f32),
    /// An error bound that guarantees each value is stored losslessly.
    Lossless,
}

impl ErrorBound {
    pub fn is_relative(&self) -> usize {
        match self {
            ErrorBound::Relative(_) => 1,
            ErrorBound::Lossless => 1,
            _ => 0,
        }
    }
}

impl fmt::Display for ErrorBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ErrorBound::Absolute(error_bound) => {
                write!(f, "{}", error_bound)
            }
            ErrorBound::Relative(error_bound) => {
                write!(f, "{}%", error_bound)
            }
            ErrorBound::Lossless => {
                write!(f, "0",)
            }
        }
    }
}

// pub fn rewrite_value(value: f32, last_value: f32, error_bound: f32) -> f32 {
//     if error_bound == 0.0 || value == 0.0 || value.is_infinite() || value.is_nan() {
//         value
//     } else if !last_value.is_infinite()
//         && is_value_within_error_bound(value, last_value, error_bound)
//     {
//         value
//     } else {
//         rewrite_value_bits_logarithm_method_with_check(value, error_bound)
//     }
// }

pub fn rewrite_value(value: f32, last_value: f32, error_bound: ErrorBound) -> f32 {
    if error_bound == ErrorBound::Lossless || value.is_infinite() || value.is_nan() {
        value
    } else if !last_value.is_infinite()
        && is_value_within_error_bound(error_bound, value, last_value)
    {
        value
    } else {
        rewrite_value_bits_logarithm_method_with_check(value, error_bound)
    }
}

/// Determine if `approximate_value` is within `error_bound` of `real_value`.
pub fn is_value_within_error_bound(
    error_bound: ErrorBound,
    real_value: f32,
    approximate_value: f32,
) -> bool {
    match error_bound {
        ErrorBound::Absolute(error_bound) => {
            // Needed to allow +INFINITY, -INFINITY, and NAN values to be stored lossless.
            if equal_or_nan(real_value as f64, approximate_value as f64) {
                true
            } else {
                f32::abs(real_value - approximate_value) <= error_bound
            }
        }
        ErrorBound::Relative(error_bound) => {
            // Needed because result becomes NAN and approximate_value is rejected if
            // approximate_value and real_value are zero, and because NAN != NAN.
            if equal_or_nan(real_value as f64, approximate_value as f64) {
                true
            } else {
                let difference = real_value - approximate_value;
                let result = f32::abs(difference / real_value);
                (result * 100.0) <= error_bound
            }
        }
        ErrorBound::Lossless => equal_or_nan(real_value as f64, approximate_value as f64),
    }
}

/// Returns true if `v1` and `v2` are equivalent or both values are NAN.
fn equal_or_nan(v1: f64, v2: f64) -> bool {
    v1 == v2 || (v1.is_nan() && v2.is_nan())
}

pub fn rewrite_value_bits_logarithm_method_with_check(value: f32, error_bound: ErrorBound) -> f32 {
    // TODO: to be deleted if value rewrite is also used
    if error_bound == ErrorBound::Lossless {
        return value;
    }
    let bit_position = compute_rewrite_position_logarithm_with_check(value, error_bound);
    f32::from_bits(rewrite_bits_by_n(value.to_bits(), bit_position))
}

// A log-based method to rewrite bits with check that results in 1 more rewritten bit
//  i.e., better for compression and worse for decompression
fn compute_rewrite_position_logarithm_with_check(value: f32, error_bound: ErrorBound) -> i32 {
    let abs_error_bound = maximum_allowed_deviation(error_bound, value as f64) as f32;
    let exponent = get_exponent(value);
    let factorized_epsilon = abs_error_bound / 2f32.powi(exponent);
    let bit_position = factorized_epsilon.log2().abs().floor();
    let mut bit_to_rewrite = 23 - bit_position as i32;

    // Extra check to ensure if the error is within the error_bound
    let rewritten_bits = rewrite_bits_by_n(value.to_bits(), bit_to_rewrite);
    let rewritten_value = f32::from_bits(rewritten_bits);
    if (value - rewritten_value).abs() > abs_error_bound {
        bit_to_rewrite -= 1;
    }

    bit_to_rewrite
}

fn get_exponent(value: f32) -> i32 {
    let n_bits: u32 = value.to_bits();
    let exponent_ = ((n_bits >> 23) & 0xff) as i32;
    let exponent = exponent_ - 127;
    exponent
}

fn rewrite_bits_by_n(bits_to_rewrite: u32, erase_by_n: i32) -> u32 {
    let mask = u32::MAX << erase_by_n;
    bits_to_rewrite & mask
}

// Used from ModelarDB-RS source code
/// Compute the maximum allowed deviation from `value` within `error bound`.
pub fn maximum_allowed_deviation(error_bound: ErrorBound, value: f64) -> f64 {
    // The allowed deviation is lower than the bound to account for inaccurate floating point math.
    match error_bound {
        ErrorBound::Absolute(error_bound) => error_bound as f64 * 0.99,
        ErrorBound::Relative(error_bound) => f64::abs(value * (error_bound as f64 / 100.1)),
        ErrorBound::Lossless => 0.0,
    }
}
