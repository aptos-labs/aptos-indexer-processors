# Event Parser
This is a very simple example that just extracts events from user transactions and logs them.

## Prerequisites
- `pnpm`: The code is tested with pnpm 8.6.2. Later versions should work too.
- `node`: The code is tested with Node 18. Later versions should work too.

## Usage
Install all the dependencies:
```
pnpm install
```

Prepare the `config.yaml` file. Make sure to update the `config.yaml` file with the correct indexer setting and database credentials.
```
$ cp config.yaml.example ~/config.yaml
```

Run the example:
```
pnpm start process --config ~/config.yaml
```

## Explanation
This example provides a basic processor that extracts events from user transactions and logs them.

When creating a custom processor, the two main things you need to define are:
- Parser: How you parse the data from the transactions.
- Models: How you store the data you extract from the transactions.

These are defined in `parser.ts` and `models.ts` respectively.

The SDK handles the rest:
- Connecting to the Transaction Stream Service.
- Creating tables in the database.
- Validating the chain ID.
- Keeping track of the last processed transaction.
- Storing the data from your `parse` function in the database.

In `parser.ts`, we have implemented a `parse` function which accepts a `Transaction` as a parameter. The example code shows how to implement custom filtering and how to extract `Events` from a `Transaction`. The function returns a list of event objects that the SDK will add to the database for us.
