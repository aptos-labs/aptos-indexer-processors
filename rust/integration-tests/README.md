## CLI for Generating Expected Database Output Files

This project includes a CLI tool that generates expected database output files based on different test contexts (e.g., imported transactions from testnet, mainnet, or scripted transactions). The generated JSON files are saved in a predefined folder structure for each context.

### Usage

To run the CLI, use the following command:

```bash
cargo run -- --test_contexts <test_context1> [<test_context2> ...]
```

Where <test_context1>, <test_context2>, etc., can be one or more of the following:

imported_testnet - Generates output for transactions imported from the testnet.
imported_mainnet - Generates output for transactions imported from the mainnet.
scripted - Generates output for scripted transactions.

```
expected_db_output_files/
│
├── imported_testnet_txns/   # Contains output for imported testnet transactions
│   └── <processor_name>_version.json
│
├── imported_mainnet_txns/   # Contains output for imported mainnet transactions
│   └── <processor_name>_version.json
│
└── scripted_txns/           # Contains output for scripted transactions
    └── <processor_name>_version.json
```


The folder for each context will be automatically created if it does not exist, and the files are named using the processor name and transaction version.

## Example
To generate output for the scripted test context, you can run:

```
cargo run -- --test_contexts scripted
```
This will create the output in the expected_db_output_files/scripted_txns/ folder.

## Running Tests
To run the included test:

```
cargo test -- --test_contexts scripted
```

This will verify that the CLI can successfully generate the output and that the folder structure is created as expected.
