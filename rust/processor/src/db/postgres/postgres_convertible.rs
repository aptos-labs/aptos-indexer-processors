pub trait PostgresConvertible {
    type PostgresModelType;
    fn to_postgres(&self) -> Self::PostgresModelType;
}

impl<T: PostgresConvertible> PostgresConvertible for Vec<T> {
    type PostgresModelType = Vec<T::PostgresModelType>;

    fn to_postgres(&self) -> Self::PostgresModelType {
        self.iter().map(|item| item.to_postgres()).collect()
    }
}
