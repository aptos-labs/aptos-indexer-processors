## Python Quickstart

### Prerequisite

- Python 3.7 or higher
- `pip` version 9.0.1 or higher

### Basic Tutorial

In this tutorial, we will be going over how to create and run the Example Event Processor. All source code is in `aptos-indexer-processors/python/example_event_processor`.

1. Install all dependencies

```
poetry install
```

2. Download the example:

```
# Clone the repository to get the example code:
$ git clone https://github.com/aptos-labs/aptos-indexer-processors
# Navigate to the python folder
$ cd aptos-indexer-processors/python
```

In this example, we have created an example event parser.

3. Create a processer.
   - First you need to create an indexer processor that reads the stream of data.
   - We've created an example client in `processor.py`. This client
     - Connects to the gRPC server and reads a stream of transaction data.
     - Calls the function `parse` to parse the transaction
     - Validates the chain ID and transaction version.
4. Create a parser.
   - In `event_parser.py`, we have implemented a `parse` function which accepts a `Transaction` as a parameter.
   - The example code shows how to implement custom filtering and parse a `Transaction` and the associated `Event`'s.
   - The function returns a list of event objects, which we’ll need to add to the database..
5. Insert data rows into database.
   - In the example, we use Postgres and SQLAlchemy to help us interact with the database.
   - If you’re running the Python client locally, you’ll first need to create the table. Run `poetry run python -m processors.example_event_processor.models.create_tables -c config.yaml`.
   - In `processor.py`, after the events are parsed, all the event objects are then added to the database.
6. Update `config.yaml` with your values
   - Set `db_connection_uri`
   - Set `indexer_api_key`
   - (Optional) Set `starting_version_default` and/or `starting_version_override`
7. Run `poetry run python -m processors.example_event_processor.processor -c config.yaml` to start indexing!
8. (Optional) Run locally in Docker
   - The included `Dockerfile` is already set up for you to run the example event processor in Docker.
   - Create `config.yaml` under current folder
   - Run `docker compose up --build --force-recreate`.

## Development

### Install all dependencies

```bash
poetry install
```

### Linting & autoformatting

```bash
poetry run poe pyright # typecheck
poetry run poe format # autoformat via black
```

### Run locally in Docker

```bash
docker compose up --build --force-recreate
```
