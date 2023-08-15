# Creating a Custom Indexer Processor

In this tutorial, we're going to walk you through all the steps involved with creating a very basic custom indexer processor to track events and data on the Aptos blockchain.

We use a very simple smart contract called **Coin Flip** that emits events for us. The smart contract is already deployed, and you mostly don't need to understand it unless you're curious to mess with it or change things. It's located in the `python/processors/coin_flip/move` folder.

We've provided the example code for the modules involved. This tutorial assumes you've set up your environment and already have the following:

- An API key for the GRPC backend. If you do not have one, you should use your API gateway to obtain one [here](https://github.com/aptos-labs/aptos-indexer-processors)
- The [Aptos CLI](https://aptos.dev/tools/aptos-cli/)
- [Python](https://www.python.org/downloads/) and [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- [Postgresql](https://www.postgresql.org/download/)
    - We will use a database hosted on `localhost` on the port `5432`, which should be the default
    - When you create your username, keep track of it and the password you use for it
    - You can view a tutorial for installing postgresql and psql [here](https://www.digitalocean.com/community/tutorials/how-to-install-postgresql-on-ubuntu-22-04-quickstart) tool to set up your database more quickly
- This cloned repository on your local machine `https://github.com/aptos-labs/aptos-indexer-processors`

Explaining how to create a database is beyond the scope of this tutorial, so if you don't know how to do it, you could check out tutorials on how to create a database with the `psql` tool.

## Setup your database

Make sure to start the `postgresql` service:

The command for Linux/WSL might be something like:

```shell
sudo service postgresql start
```

For mac, if you're using brew, start it up with:

```shell
brew services start postgresql
```

Create your database with the name `coin_flip`, where our username is `user` and our password is `password`.

If your database is set up correctly, and you have the `psql` tool, you should be able to run the command `psql -d coin_flip`.

## Configure your indexer processor

Now let's setup the configuration details for the actual indexer processor we're going to use.

### Setup your config.yaml file

Copy the contents below and save it to a file called `config.yaml`. Save it in the `coin_flip` folder. Your file directory structure should look something like this:

```
- indexer
    - proto
    - python
        - aptos_ambassador_token
        - aptos-tontine
        - coin_flip
            - move
                - sources
                    - coin_flip.move
                    - package_manager.move
                - Move.toml
            - config.yaml     <-------- Edit this config.yaml file
            - models.py
            - processor.py
            - README.md
        - example_event_processor
        - nft_marketplace_v2
        - nft_orderbooks
        __init__.py
        main.py
        README.md
    - rust
    - scripts
    - typescript
```

Once you have your config.yaml file open, you only need to change one field:
```yaml
grpc_data_stream_api_key: "<YOUR_API_KEY_HERE>"
```

If you want to use a different network, change the `grpc_data_stream_endpoint` field to the corresponding desired value:

```yaml
devnet: 35.225.218.95:50051
testnet: 35.223.137.149:50051  # north america
testnet: 34.64.252.224:50051   # asia
mainnet: 34.30.218.153:50051
```

If these values don't work for you, check out the `README.md` at the root folder of the repository. 

If you're using a different database name or processor name, change the `processor_name` field and the `db_connection_uri` to your specific needs. Here's the general structure of the field:

```yaml
db_connection_uri: "postgresql://username:password@database_url:port_number/database_name"
```

### Add your processor name to your processor

First, let's create the name for the database schema we're going to use. We use `coin_flip` in our example, so we need to add it in two places:

1. We need to add it to our `python/utils/processor_name.py` file:
```python
    class ProcessorName(Enum):
        EXAMPLE_EVENT_PROCESSOR = "python_example_event_processor"
        NFT_MARKETPLACE_V1_PROCESSOR = "nft_marketplace_v1_processor"
        NFT_MARKETPLACE_V2_PROCESSOR = "nft_marketplace_v2_processor"
        COIN_FLIP = "coin_flip"
```
2. Add it to the constructor in the `IndexerProcessorServer` match cases in `utils/worker.py`:

```python
match self.config.processor_name:
    case ProcessorName.EXAMPLE_EVENT_PROCESSOR.value:
        self.processor = ExampleEventProcessor()
    case ProcessorName.NFT_MARKETPLACE_V1_PROCESSOR.value:
        self.processor = NFTMarketplaceProcesser()
    case ProcessorName.NFT_MARKETPLACE_V2_PROCESSOR.value:
        self.processor = NFTMarketplaceV2Processor()
    case ProcessorName.COIN_FLIP.value:
        self.processor = CoinFlipProcessor()
```

### Create your database model

In `coin_flip/models.py`, you can see our current model:

class CoinFlipEvent(Base):
    __tablename__ = "coin_flip_events"
    __table_args__ = ({"schema": COIN_FLIP_SCHEMA_NAME},)

    sequence_number: BigIntegerPrimaryKeyType
    creation_number: BigIntegerPrimaryKeyType
    account_address: StringPrimaryKeyType
    prediction: BooleanType
    result: BooleanType
    wins: BigIntegerType
    losses: BigIntegerType
    win_percentage: NumericType
    transaction_version: BigIntegerType
    transaction_timestamp: TimestampType
    inserted_at: InsertedAtType
    event_index: BigIntegerType
