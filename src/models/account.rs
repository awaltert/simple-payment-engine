use anyhow::{Result, bail};
use rust_decimal::Decimal;
use serde::Serialize;

use super::client::ClientId;

/// This type represent the client asset account.
///
/// It also hold the critical calculations, thus is intensivele tested with unit test, that can be found in the [tests] submodule
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Account {
    #[serde(rename = "client")]
    pub client_id: ClientId,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,

    #[serde(rename = "locked")]
    pub is_locked: bool,
}

impl Account {
    pub fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            available: Decimal::default(),
            held: Decimal::default(),
            total: Decimal::default(),
            is_locked: false,
        }
    }

    pub fn deposit(&mut self, amount: Decimal) -> Result<()> {
        if amount < Decimal::ZERO {
            bail!("Failed to withdraw a negative amount of {amount:}.")
        }

        self.available = self.available.saturating_add(amount);
        self.total = self.total.saturating_add(amount);

        Ok(())
    }

    pub fn try_withdrawal(&mut self, amount: Decimal) -> Result<()> {
        if amount < Decimal::ZERO {
            bail!("Failed to withdraw a negative amount of {amount:}.")
        }

        if amount > self.available {
            bail!(
                "Failed to withdraw the amount of {amount:}. No sufficient funds available: {:}",
                self.available
            )
        }

        self.available = self.available.saturating_sub(amount);
        self.total = self.total.saturating_sub(amount);

        Ok(())
    }

    pub fn dispute(&mut self, direction: Direction) -> Result<()> {
        match direction {
            // Raised a dispute for a withdrawal
            Direction::Increase(amount) => {
                self.held = self.held.saturating_add(amount);
                self.total = self.available.saturating_add(self.held)
            }
            // Raised a dispute for a deposit
            Direction::Decrease(amount) => {
                if amount < Decimal::ZERO {
                    bail!("Failed to dispute a negative amount of {amount:}.")
                }

                if amount > self.available {
                    bail!(
                        "Failed to dispute an amount:{amount:} that is greater than the available fund."
                    )
                }

                self.available = self.available.saturating_sub(amount);
                self.held = self.held.saturating_add(amount);
            }
        }

        Ok(())
    }

    pub fn resolve(&mut self, direction: Direction) -> Result<()> {
        match direction {
            // Raised a dispute for a withdrawal
            Direction::Increase(amount) => {
                self.available = self.available.saturating_add(amount);
                self.held = self.held.saturating_sub(amount);
            }
            // Raised a dispute for a deposit
            Direction::Decrease(amount) => {
                if amount < Decimal::ZERO {
                    bail!("Failed to dispute a negative amount of {amount:}.")
                }

                self.available = self.available.saturating_add(amount);
                self.held = self.held.saturating_sub(amount);
            }
        }

        Ok(())
    }

    pub fn chargeback(&mut self, direction: Direction) -> Result<()> {
        match direction {
            // Raised a dispute for a withdrawal
            Direction::Increase(_amount) => {
                // Nothing to do here
            }
            // Raised a dispute for a deposit
            Direction::Decrease(amount) => {
                if amount < Decimal::ZERO {
                    bail!("Failed to dispute a negative amount of {amount:}.")
                }

                self.available = self.available.saturating_sub(amount);
                self.total = self.total.saturating_sub(amount);
            }
        }

        self.is_locked = true;

        Ok(())
    }
}

/// I'm not familiar with this domain yet. From the transaction protocol perpective, it is technically possible to raise a dispute and reference a withdrawal transaction.
/// This type is used, to differentiate the cases and adjust the calculation. The type enabled a low invasive change to realize it.
///
/// After the implementation, I have serious concerns, that this was necessary. A remove is simple to do.
pub enum Direction {
    /// Increase happens when there is a dispute on a withdrawal
    Increase(Decimal),
    /// Increase happens when there is a dispute on a deposit
    Decrease(Decimal),
}

#[cfg(test)]
mod tests {
    use claims::{assert_err, assert_ok, assert_ok_eq};
    use rust_decimal::dec;

    use super::*;

    mod deposit {
        use super::*;

        #[test]
        fn can_deposit() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(12);

            let res = acc.deposit(amount);
            assert_ok_eq!(res, ());

            assert_eq!(acc.available, amount);
            assert_eq!(acc.total, amount);
            assert_eq!(acc.held, dec!(0));
            assert!(!acc.is_locked);
        }

        #[test]
        fn can_deposit_multiple_times() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(23.12345);

            (1..=10).for_each(|i| {
                let res = acc.deposit(amount);
                assert_ok_eq!(res, (), "Failed to deposit in iteration: {i}");
            });

            let expected_amount = amount.saturating_mul(dec!(10));

            assert_eq!(acc.available, expected_amount);
            assert_eq!(acc.total, expected_amount);
            assert_eq!(acc.held, dec!(0));
            assert!(!acc.is_locked);
        }

        #[test]
        fn can_deposit_zero_amount() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(3.1415);

            let _res = acc.deposit(amount);
            let res = acc.deposit(Decimal::ZERO);

            assert_ok_eq!(res, (), "Expected zero deposit to succeed");
        }

        #[test]
        fn cant_deposit_negative_amount() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(-23.12345);

            let res = acc.deposit(amount);

            assert_err!(
                res,
                "Expected deposit to fail, if adding a negative account"
            );
        }
    }

    mod withdrawal {
        use super::*;

        #[test]
        fn can_withdrawal() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(12);

            let res = acc.deposit(amount);
            assert_ok_eq!(res, ());

            let res = acc.try_withdrawal(dec!(4));
            assert_ok_eq!(res, ());

            assert_eq!(acc.available, dec!(8));
            assert_eq!(acc.total, dec!(8));
            assert_eq!(acc.held, dec!(0));
            assert!(!acc.is_locked);
        }

        #[test]
        fn can_withdrawal_multiple_times() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(23.12345);

            let res = acc.deposit(amount);
            assert_ok_eq!(res, ());

            (1..=10).for_each(|i| {
                let res = acc.try_withdrawal(dec!(1));
                assert_ok_eq!(res, (), "Failed to withdraw in iteration: {i}");
            });

            let expected_amount = amount.saturating_sub(dec!(10));

            assert_eq!(acc.available, expected_amount);
            assert_eq!(acc.total, expected_amount);
            assert_eq!(acc.held, dec!(0));
            assert!(!acc.is_locked);
        }

        #[test]
        fn cant_deposit_negative_amount() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(-23.12345);

            let res = acc.try_withdrawal(amount);
            assert_err!(
                res,
                "Expected withdrawal to fail, if a negative amount is provided"
            );
        }
    }

    mod dispute {
        use super::*;

        #[test]
        fn can_dispute_the_previous_deposit() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(10);

            let res = acc.deposit(amount);
            assert_ok_eq!(res, ());

            let res = acc.dispute(Direction::Decrease(amount));
            assert_ok_eq!(res, ());

            assert_eq!(acc.available, Decimal::ZERO);
            assert_eq!(acc.total, amount);
            assert_eq!(acc.held, amount);
            assert!(!acc.is_locked);
        }

        #[test]
        fn can_dispute_after_multiple_deposits() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(10);

            let res = acc.deposit(amount);
            assert_ok_eq!(res, ());

            let res = acc.deposit(amount);
            assert_ok_eq!(res, ());

            let res = acc.dispute(Direction::Decrease(dec!(15)));
            assert_ok_eq!(res, ());

            assert_eq!(acc.available, dec!(5));
            assert_eq!(acc.total, dec!(20));
            assert_eq!(acc.held, dec!(15));
            assert!(!acc.is_locked);
        }

        #[test]
        fn can_dispute_zero_amount() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(3.1415);

            let _res = acc.deposit(amount);
            let res = acc.dispute(Direction::Decrease(Decimal::ZERO));

            assert_ok_eq!(res, (), "Expected dispute of zero amount to succeed");
        }

        #[test]
        fn cant_dispute_negative_amount() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(23.12345);

            let _res = acc.deposit(amount);
            let res = acc.dispute(Direction::Decrease(dec!(-30)));

            assert_err!(res, "Expected dispute of negative amount to fail");
        }

        #[test]
        fn cant_dispute_a_greater_amount_than_available() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(23.12345);

            let _res = acc.deposit(amount);
            let res = acc.dispute(Direction::Decrease(dec!(30)));

            assert_err!(res, "Expected dispute of too high amount to fail");
        }
    }

    mod resolve {
        use super::*;

        #[test]
        fn can_resolve_deposited() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(10);

            let _res = acc.deposit(amount);
            let _res = acc.deposit(amount);
            let res = acc.dispute(Direction::Decrease(dec!(10)));

            assert_ok!(res);
            assert_eq!(acc.available, dec!(10));
            assert_eq!(acc.held, dec!(10));
            assert_eq!(acc.total, dec!(20));

            let res = acc.resolve(Direction::Decrease(dec!(10)));

            assert_ok!(res);
            assert_eq!(acc.available, dec!(20));
            assert_eq!(acc.held, dec!(0));
            assert_eq!(acc.total, dec!(20));
        }

        #[test]
        fn can_resolve_withdrawal() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(10);

            let _res = acc.deposit(amount);
            let res = acc.try_withdrawal(dec!(5));
            assert_ok!(res);
            assert_eq!(acc.available, dec!(5));
            assert_eq!(acc.held, dec!(0));
            assert_eq!(acc.total, dec!(5));

            let res = acc.dispute(Direction::Increase(dec!(5)));
            assert_ok!(res);
            assert_eq!(acc.available, dec!(5));
            assert_eq!(acc.held, dec!(5));
            assert_eq!(acc.total, dec!(10));

            let res = acc.resolve(Direction::Increase(dec!(5)));
            assert_ok!(res);
            assert_eq!(acc.available, dec!(10));
            assert_eq!(acc.held, dec!(0));
            assert_eq!(acc.total, dec!(10));
        }
    }

    mod chargeback {
        use super::*;

        #[test]
        fn can_chargeback_deposited() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(10);

            let _res = acc.deposit(amount);
            let _res = acc.deposit(amount);
            let res = acc.dispute(Direction::Decrease(dec!(10)));

            assert_ok!(res);
            assert_eq!(acc.available, dec!(10));
            assert_eq!(acc.held, dec!(10));
            assert_eq!(acc.total, dec!(20));

            let res = acc.resolve(Direction::Decrease(dec!(10)));

            assert_ok!(res);
            assert_eq!(acc.available, dec!(20));
            assert_eq!(acc.held, dec!(0));
            assert_eq!(acc.total, dec!(20));

            let res = acc.chargeback(Direction::Decrease(dec!(10)));

            assert_ok!(res);
            assert_eq!(acc.available, dec!(10));
            assert_eq!(acc.held, dec!(0));
            assert_eq!(acc.total, dec!(10));
        }

        #[test]
        fn can_chargeback_withdrawal() {
            let mut acc = Account::new(ClientId::new(42));
            let amount = dec!(10);

            let _res = acc.deposit(amount);
            let res = acc.try_withdrawal(dec!(5));
            assert_ok!(res);
            assert_eq!(acc.available, dec!(5));
            assert_eq!(acc.held, dec!(0));
            assert_eq!(acc.total, dec!(5));

            let res = acc.dispute(Direction::Increase(dec!(5)));
            assert_ok!(res);
            assert_eq!(acc.available, dec!(5));
            assert_eq!(acc.held, dec!(5));
            assert_eq!(acc.total, dec!(10));

            let res = acc.resolve(Direction::Increase(dec!(5)));
            assert_ok!(res);
            assert_eq!(acc.available, dec!(10));
            assert_eq!(acc.held, dec!(0));
            assert_eq!(acc.total, dec!(10));

            let res = acc.chargeback(Direction::Increase(dec!(5)));
            assert_ok!(res);
            assert_eq!(acc.available, dec!(10));
            assert_eq!(acc.held, dec!(0));
            assert_eq!(acc.total, dec!(10));
        }
    }
}
