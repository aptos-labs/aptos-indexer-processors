use anyhow::Error;

/// Simple trait to allow for filtering of items of type T
pub trait Filterable<T> {
    /// Whether this filter is correctly configured/initialized
    /// Any call to `is_valid` is responsible for recursively checking the validity of any nested filters
    fn is_valid(&self) -> Result<(), Error>;

    fn is_allowed(&self, item: &T) -> bool;

    fn is_allowed_vec(&self, items: &[T]) -> bool {
        items.iter().all(|item| self.is_allowed(item))
    }

    fn is_allowed_opt(&self, item: &Option<T>) -> bool {
        match item {
            Some(item) => self.is_allowed(item),
            None => false,
        }
    }

    fn is_allowed_opt_vec(&self, items: &Option<&Vec<T>>) -> bool {
        match items {
            Some(items) => self.is_allowed_vec(items),
            None => false,
        }
    }
}

/// This allows for Option<Filterable> to always return true: i.e if the filter is None, then all items are allowed.
impl<T, F> Filterable<T> for Option<F>
where
    F: Filterable<T>,
{
    fn is_valid(&self) -> Result<(), Error> {
        match self {
            Some(filter) => filter.is_valid(),
            None => Ok(()),
        }
    }

    fn is_allowed(&self, item: &T) -> bool {
        match self {
            Some(filter) => filter.is_allowed(item),
            None => true,
        }
    }

    fn is_allowed_opt(&self, item: &Option<T>) -> bool {
        match self {
            Some(filter) => filter.is_allowed_opt(item),
            None => true,
        }
    }
}

impl Filterable<String> for Option<String> {
    fn is_valid(&self) -> Result<(), Error> {
        Ok(())
    }

    fn is_allowed(&self, item: &String) -> bool {
        match self {
            Some(filter) => filter == item,
            None => true,
        }
    }
}

impl Filterable<i32> for Option<i32> {
    fn is_valid(&self) -> Result<(), Error> {
        Ok(())
    }

    fn is_allowed(&self, item: &i32) -> bool {
        match self {
            Some(filter) => filter == item,
            None => true,
        }
    }
}

impl Filterable<bool> for Option<bool> {
    fn is_valid(&self) -> Result<(), Error> {
        Ok(())
    }

    fn is_allowed(&self, item: &bool) -> bool {
        match self {
            Some(filter) => filter == item,
            None => true,
        }
    }
}
