# Custom Processors: Typescript

## Directory Guide
- `aptos-indexer-protos`: Contains code generated from the proto files in `protos/` in the root of the repo. Other packages use these to interact with indexer services, such as the Transaction Stream Service.
- `examples`: Contains example processors that you can use as a starting point for your own custom processor.
- `sdk`: Contains the custom processor SDK. This package provides a variety of helpful code for writing your own custom processor, such as for connecting to the Transaction Stream Service, creating tables in the database, and keeping track of the last processed transaction.
