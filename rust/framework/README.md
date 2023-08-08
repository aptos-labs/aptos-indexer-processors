# Aptos Processor Framework

## Crates in this directory
- aptos-processor-framework: The main crate users pull in to create processors. More info in the README there.
- aptos-writeset-parsers: A collection of parsers for txn writesets.
- aptos-indexer-protos: Protobuf definitions for the indexer stack.
- aptos-moving-average: Moving average implementation.

The most notable crate in this directory is aptos-processor-framework. This serves as a top level crate that users import and it gives them everything they need to subscribe the txn stream and push txns into a channel, pull things off that channel and dispatch them to processors, write a processor, deal with storage, etc. It also re-exports other framework crates like the protos and the currently-empty aptos-writeset-parsers crate.
