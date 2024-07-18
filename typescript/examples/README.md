# Custom Processor Templates

> [!WARNING]  
> For production-grade indexers, we recommend the Rust processors.
> The typescript implementation is known to get stuck when there are lots of data to process. The issue is with the GRPC client and we haven't had a chance to optimize. Please proceed with caution.

This directory contains templates you can copy to get started with writing a custom processor in TS.
