# Custom Processors: Typescript

> [!WARNING]  
> For production-grade indexers, we recommend the Rust processors.
> The typescript implementation is known to get stuck when there are lots of data to process. The issue is with the GRPC client and we haven't had a chance to optimize. Please proceed with caution.

## Directory Guide

- `examples`: Contains example processors that you can use as a starting point for your own custom processor.
- `sdk`: Contains the custom processor SDK. This package provides a variety of helpful code for writing your own custom processor, such as for connecting to the Transaction Stream Service, creating tables in the database, and keeping track of the last processed transaction.
