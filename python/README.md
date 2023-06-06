## Python Quickstart

### Prerequisite

- Python 3.7 or higher
- `pip` version 9.0.1 or higher

### Basic Tutorial

In this tutorial, we will be going over how to create and run the Example Event Processor. All source code is in `aptos-indexer-processors/python/processors/example_event_processor`.

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

3. Prepare the `config.yaml` file.
   Make sure to update the `config.yaml` file with the correct indexer settings and database credentials.

   ```
   $ cp config.yaml.example config.yaml
   ```

4. Define the data model and create the table(s).

   - In this tutorial, we want to extract data about transaction events. In `models.py`, you can define an Events data model.
   - The example uses Postgres. For now only Postgres is supported and we use SQLAlchemy ORM to interact with the Postgres database.

5. Define a transaction parsing function and create and run a `TransactionsProcessor`.

   - In `processor.py`, we have implemented a `parse` function which accepts a `transaction_pb2.Transaction` as a parameter. The `parse` function filters out all non-user transactions and reads a transaction and its associated events. It returns a list of `Event` db model objects.
   - Initialize a `TransactionsProcessor` with the parsing function and call `process()`.

6. Run `poetry run python -m processors.example_event_processor.processor -c config.yaml` to start indexing!

7. (Optional) Run locally in Docker

   - The included `Dockerfile` is already set up for you to run the example event processor in Docker.
   - Create `config.yaml` under the `python` folder
   - Run `docker compose up --build --force-recreate`.

8. Query the data from database in your dApp. It's recommended to use SQLAlchemy for this part.

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
