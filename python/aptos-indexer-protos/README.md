# Aptos Indexer Protos

This repository contains the protobuf definitions for the Aptos Indexer tech stack.

## Usage
Import generated classes like this:
```
from aptos_indexer_protos.aptos.transaction.v1.transaction_pb2 import Transaction
```

Then use them like this:
```
def parse(transaction: Transaction):
    # Parse the transaction.
```

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md) for more information.
