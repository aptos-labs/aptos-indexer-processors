use crate::traits::Filterable;
use anyhow::Error;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Allows matching a given value within an array of values, by index
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PositionalFilter<T>
where
    T: PartialEq + Debug,
{
    pub value: T,
    pub index: usize,
}

impl<T> Filterable<Vec<T>> for PositionalFilter<T>
where
    T: PartialEq + Debug,
{
    fn validate_state(&self) -> Result<(), Error> {
        Ok(())
    }

    fn is_allowed(&self, items: &Vec<T>) -> bool {
        items.get(self.index).map_or(false, |v| v == &self.value)
    }
}

impl<T> Filterable<Vec<T>> for Vec<PositionalFilter<T>>
where
    T: PartialEq + Debug,
{
    fn validate_state(&self) -> Result<(), Error> {
        if self.is_empty() {
            return Err(Error::msg(
                "PositionalFilter must have at least one element",
            ));
        }
        Ok(())
    }

    fn is_allowed(&self, items: &Vec<T>) -> bool {
        self.iter()
            .all(|arg| items.get(arg.index).map_or(false, |v| v == &arg.value))
    }
}
