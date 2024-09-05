# Hasura Metadata
This directory contains the metadata we use for Hasura. The "metadata" is what Hasura calls the file that configures pretty much everything about a Hasura instance, including table schemas, relations, DB connection configuration, how metrics are exported, etc.

> [!WARNING]
> (09/25/2024) We added a new unified_transition.json to facilitate Fungible Asset Migration. Please use this file in Hasura while the new table is backfilling.

**Note for Labs folks**: Use the file in [internal-ops](https://github.com/aptos-labs/internal-ops/blob/main/infra/apps/aptos-indexer-processors/metadata) instead. See the README there.
