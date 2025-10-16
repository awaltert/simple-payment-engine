use std::error::Error as StdError;
use std::fmt::Debug;

use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub mod account;
pub mod client;
pub mod transaction;

/// This type represents a non negative decimal for being used at the outer boundaries of the domain, enforcing this constraint.
/// This helps to mitigate a potential attack surface, when used in Deposit or Withrawal transactions in conjunction with negative numbers.
///
/// The [NonNegativeDecimal::into_inner] method is a handy shortcut. It would be much better to implement the necessary traits enabling the required operations, like saturating_add, etc delegating to the underlying type.
/// I didn't implement it due to time constraints.
///
/// Remark: I intentionally didn't implement [std::ops::Deref] as this is considered dangerous. This expose the entire api surface of the underlying type
/// which would contradict the encapsulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct NonNegativeDecimal(Decimal);

impl NonNegativeDecimal {
    pub const ZERO: NonNegativeDecimal = NonNegativeDecimal(Decimal::ZERO);

    pub fn try_from<N>(num: N) -> Result<Self>
    where
        N: TryInto<Decimal> + Debug + Send + Sync + Clone + Copy,
        <N as TryInto<Decimal>>::Error: StdError + Send + Sync + 'static,
    {
        let num = num.try_into().with_context(|| {
            format!(
                "Failed to convert the given number: {:?} to a decimal.",
                num
            )
        })?;

        if num < Decimal::ZERO {
            bail!("Failed to construct NonNegativeDecimal, because is is less than 0");
        }

        Ok(NonNegativeDecimal(num))
    }

    pub fn into_inner(self) -> Decimal {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use claims::{assert_err, assert_ok_eq};
    use rust_decimal::dec;

    use super::*;

    #[test]
    fn can_construct_from_unsigend_integer_value() {
        let given = 42u32;

        let res = NonNegativeDecimal::try_from(given);
        assert_ok_eq!(res, NonNegativeDecimal(dec!(42)));
    }

    #[test]
    fn can_construct_from_f64_value() {
        let given = 123.456_f64;

        let res = NonNegativeDecimal::try_from(given);
        assert_ok_eq!(res, NonNegativeDecimal(dec!(123.456)));
    }

    #[test]
    fn can_construct_from_f32_value() {
        let given = 123.456_f32;

        let res = NonNegativeDecimal::try_from(given);
        assert_ok_eq!(res, NonNegativeDecimal(dec!(123.456)));
    }

    #[test]
    fn can_construct_zero() {
        let given = 0.0;

        let res = NonNegativeDecimal::try_from(given);
        assert_ok_eq!(res, NonNegativeDecimal(Decimal::ZERO));
    }

    #[test]
    fn canr_construct_from_negative() {
        let given = -12;

        let res = NonNegativeDecimal::try_from(given);
        assert_err!(res, "Should fail to construct from a negative value");
    }
}
