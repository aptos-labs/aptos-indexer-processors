use anyhow::Error;

/// Simple trait to allow for filtering of items of type T
pub trait Filterable<T> {
    /// Whether this filter is correctly configured/initialized
    /// Any call to `validate_state` is responsible for recursively checking the validity of any nested filters *by calling `is_valid`*
    /// The actual public API is via `is_valid` which will call `validate_state` and return an error if it fails, but annotated with the filter type/path
    fn validate_state(&self) -> Result<(), Error>;

    #[inline]
    fn is_valid(&self) -> Result<(), Error> {
        println!("calling: {}", std::any::type_name::<Self>());
        // This is a convenience method to allow for the error to be annotated with the filter type/path
        self.validate_state().map_err(|e| {
            println!("erroring: {}", std::any::type_name::<Self>());
            e.context(std::any::type_name::<T>())
        })
    }

    fn is_allowed(&self, item: &T) -> bool;

    #[inline]
    fn is_allowed_vec(&self, items: &[T]) -> bool {
        items.iter().all(|item| self.is_allowed(item))
    }

    #[inline]
    fn is_allowed_opt(&self, item: &Option<T>) -> bool {
        match item {
            Some(item) => self.is_allowed(item),
            None => false,
        }
    }

    #[inline]
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
    #[inline]
    fn validate_state(&self) -> Result<(), Error> {
        match self {
            Some(filter) => filter.is_valid(),
            None => Ok(()),
        }
    }

    #[inline]
    fn is_allowed(&self, item: &T) -> bool {
        match self {
            Some(filter) => filter.is_allowed(item),
            None => true,
        }
    }

    #[inline]
    fn is_allowed_opt(&self, item: &Option<T>) -> bool {
        match self {
            Some(filter) => filter.is_allowed_opt(item),
            None => true,
        }
    }
}

impl Filterable<String> for Option<String> {
    #[inline]
    fn validate_state(&self) -> Result<(), Error> {
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, item: &String) -> bool {
        match self {
            Some(filter) => filter == item,
            None => true,
        }
    }
}

impl Filterable<i32> for Option<i32> {
    #[inline]
    fn validate_state(&self) -> Result<(), Error> {
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, item: &i32) -> bool {
        match self {
            Some(filter) => filter == item,
            None => true,
        }
    }
}

impl Filterable<bool> for Option<bool> {
    #[inline]
    fn validate_state(&self) -> Result<(), Error> {
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, item: &bool) -> bool {
        match self {
            Some(filter) => filter == item,
            None => true,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::anyhow;

    #[derive(Debug, PartialEq)]
    pub struct InnerStruct {
        pub a: Option<String>,
    }

    impl Filterable<InnerStruct> for InnerStruct {
        fn validate_state(&self) -> Result<(), Error> {
            Err(anyhow!("this is an error"))
        }

        fn is_allowed(&self, _item: &InnerStruct) -> bool {
            true
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct OuterStruct {
        pub inner: Option<InnerStruct>,
    }

    impl Filterable<InnerStruct> for OuterStruct {
        fn validate_state(&self) -> Result<(), Error> {
            self.inner.is_valid()?;
            Ok(())
        }

        fn is_allowed(&self, item: &InnerStruct) -> bool {
            self.inner.is_allowed(item)
        }
    }

    #[test]
    fn test_error_prop() {
        let inner = InnerStruct {
            a: Some("test".to_string()),
        };
        let outer = OuterStruct { inner: Some(inner) };

        let res = outer.is_valid();
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "err");
    }
}
