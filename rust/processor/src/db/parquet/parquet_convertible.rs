pub trait ParquetConvertible {
    type ParquetModelType;
    fn to_parquet(&self) -> Self::ParquetModelType;
}

impl<T: ParquetConvertible> ParquetConvertible for Vec<T> {
    type ParquetModelType = Vec<T::ParquetModelType>;

    fn to_parquet(&self) -> Self::ParquetModelType {
        self.iter().map(|item| item.to_parquet()).collect()
    }
}
