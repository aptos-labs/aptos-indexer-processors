# Examples: Python

This directory contains examples of processors written in Python. Below you can find a brief description of each processor.

## Example Event Processor

This is a simple example that processes transaction events and inserts the event's data to a database. This example includes

- Processor: Shows an example of processing and parsing events from a transaction
- Database: Writes event's data to a Postgres DB

## Aptos Tontine

This example demonstrates the simplest possible variant of a processor. There are three parts:

- Processor: Processes transactions and writes to the DB based on events.
- Database: It uses sqlite so there are no external dependencies / processes.
- API: Exposes a Flask based API for querying data from the DB.

It also demonstrates a parser function that outputs operations to create, update, and delete rows.

This processor is part of a full Aptos project, so you can see the processor in context alongside the Move module and frontend.

## Aptos Ambassador Token

This example demonstrates how to track changes on the attribute (e.g., ambassador level) of the [Aptos ambassador token](https://github.com/aptos-labs/aptos-core/tree/main/aptos-move/move-examples/token_objects/ambassador/move). This example includes

- Processor: Processes transactions and detects changes on the ambassador level in the events & writesets of the transactions
- Database: No database operation is demonstrated for the simplicity sake. The changes found are simply printed out in the console.

When an existing token's level changes, the smart contract is programmed to emit an event (e.g., LevelUpdateEvent). So, the level change can be observed both in the transaction's event and writeset redundantly.

To run:
```
$ cd aptos-indexer-processors/python
$ poetry run python -m processors.aptos_ambassador_token.processor -c config.yaml
```
