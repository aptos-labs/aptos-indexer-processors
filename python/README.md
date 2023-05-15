## Python Quickstart
### Prerequisite
- Python 3.7 or higher
- `pip` version 9.0.1 or higher
### Tutorial
1. Install all dependencies
  ```
  postry install
  ```
2. Download the example:
```
# Clone the repository to get the example code:
$ git clone https://github.com/aptos-labs/aptos-indexer-client-examples
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
   - If you’re running the Python client locally, you’ll first need to create the table. Run `poetry run python create_table.py`.
   - In `processor.py`, after the events are parsed, all the event objects are then added to the database.
6. Update `config.yaml` with your values
   - Set `db_connection_uri`
   - Set `indexer_api_key`
   - (Optional) Set `starting_version`
7. Run `poetry run python processor.py` to start indexing!

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
