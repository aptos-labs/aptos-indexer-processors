## Python Quickstart
### Prerequisite
- Python 3.7 or higher
- `pip` version 9.0.1 or higher
### Tutorial
1. Install the latest version of gRPC and tooling for Python:
  ```
  python -m pip install grpcio
  python -m pip install grpcio-tools
  ```
2. Download the example:
```
# Clone the repository to get the example code:
$ git clone https://github.com/aptos-labs/aptos-indexer-client-examples
# Navigate to the python folder
$ cd aptos-indexer-processors/python
```
In this example, we are creating an event parser.
3. Create a processer.
   - First you need to create an indexer processor that reads the stream of data.
   - We've create an example client in `processor.py`. This client
     - Connects to the gRPC server and reads a stream of transaction data.
     - Calls the function `parse` to parse the transaction
     - Validates the chain ID and transaction version.
4. Create a parser.
   - In `event_parser.py`, we have implemented a `parse` function which accepts a `Transaction` as a parameter.
   - The example code shows how to implement custom filtering and parse a `Transaction` and the associated `Event`'s.
   - The function returns a list of event objects, which we’ll need to add to the database..
5. Insert data rows into database.
   - In the example, we use Postgres and SQLAlchemy to help us interact with the database. To run the example code, install the following:
     ```
     python -m pip install psycopg2
     python -m pip install sqlalchemy
     ```
   - If you’re running the Python client locally, you’ll first need to create the table. Run `python create_table.py`.
   - In `processor.py`, after the events are parsed, all the event objects are then added to the database.
6. Run `python processor.py` to start indexing!

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
