use sea_orm::Statement;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let create_table_statements = vec![
            r#"
        CREATE TABLE "events" (
            "sequence_number" INT8 NOT NULL,
            "creation_number" INT8 NOT NULL,
            "account_address" STRING(66) NOT NULL,
            "transaction_version" STRING(66) NOT NULL,
            "transaction_block_height" INT8 NOT NULL,
            "event_index" INT8 NOT NULL,
            "event_type" STRING(300) NOT NULL,
            "data" JSONB NOT NULL,
            "inserted_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT events_pkey PRIMARY KEY (transaction_version, event_index)
        )"#,
            r#"
        CREATE TABLE transactions (
            transaction_version STRING(66) NOT NULL,
            transaction_block_height INT8 NOT NULL,
            hash STRING(66) NOT NULL,
            transaction_type STRING(50) NOT NULL,
            payload JSONB,
            payload_type STRING(66),
            state_change_hash STRING(66) NOT NULL,
            event_root_hash STRING(66) NOT NULL,
            state_checkpoint_hash STRING(66),
            gas_used DECIMAL NOT NULL,
            success BOOL NOT NULL,
            vm_status STRING NOT NULL,
            accumulator_root_hash STRING(66) NOT NULL,
            num_events INT8 NOT NULL,
            num_write_set_changes INT8 NOT NULL,
            epoch INT8 NOT NULL,
            parent_signature_type STRING,
            sender STRING(66),
            sequence_number INT8,
            max_gas_amount DECIMAL,
            expiration_timestamp_secs TIMESTAMP(6),
            gas_unit_price DECIMAL,
            timestamp TIMESTAMP(6),
            entry_function_id_str STRING,
            signature JSONB,
            id STRING(66),
            round INT8,
            previous_block_votes_bitvec JSONB,
            proposer STRING(66),
            failed_proposer_indices JSONB,
            inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

            CONSTRAINT transactions_pkey PRIMARY KEY (transaction_version)
        )"#,
            r#"
        CREATE TABLE write_set_changes_module (
            transaction_version STRING(66) NOT NULL,
            transaction_block_height INT8 NOT NULL,
            hash STRING(66) NOT NULL,
            write_set_change_type STRING NOT NULL,
            address STRING(66) NOT NULL,
            index INT8 NOT NULL,
            is_deleted BOOL NOT NULL,
            bytecode BYTES,
            friends JSONB,
            exposed_functions JSONB,
            structs JSONB,
            inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

            CONSTRAINT write_set_changes_module_pkey PRIMARY KEY (transaction_version,index)
        )"#,
            r#"
        CREATE TABLE write_set_changes_resource (
            transaction_version STRING(66) NOT NULL,
            transaction_block_height INT8 NOT NULL,
            hash STRING(66) NOT NULL,
            write_set_change_type STRING NOT NULL,
            address STRING(66) NOT NULL,
            index INT8 NOT NULL,
            is_deleted BOOL NOT NULL,
            name STRING NOT NULL,
            module STRING NOT NULL,
            generic_type_params JSONB,
            state_key_hash STRING(66) NOT NULL,
            data JSONB,
            data_type STRING(300),
            inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

            CONSTRAINT write_set_changes_resource_pkey PRIMARY KEY (transaction_version,index)
        )"#,
            r#"
        CREATE TABLE write_set_changes_table (
            transaction_version STRING(66) NOT NULL,
            transaction_block_height INT8 NOT NULL,
            hash STRING(66) NOT NULL,
            write_set_change_type STRING NOT NULL,
            address STRING(66) NOT NULL,
            index INT8 NOT NULL,
            is_deleted BOOL NOT NULL,
            table_handle STRING(66) NOT NULL,
            key STRING NOT NULL,
            data JSONB,
            inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

            CONSTRAINT write_set_changes_table_pkey PRIMARY KEY (transaction_version,index)
        )"#,
            r#"
        CREATE TABLE "write_set_changes" (
            "transaction_version" STRING(66) NOT NULL,
            "transaction_block_height" INT8 NOT NULL,
            "hash" STRING(66) NOT NULL,
            "write_set_change_type" STRING NOT NULL,
            "address" STRING(66) NOT NULL,
            "index" INT8 NOT NULL,
            "inserted_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

            CONSTRAINT "write_set_changes_pkey" PRIMARY KEY ("transaction_version","index")
        )"#,
        ];

        for sql in create_table_statements {
            execute_statement(manager, sql).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let drop_table_names = vec![
            "events",
            "transactions",
            "write_set_changes",
            "write_set_changes_resource",
            "write_set_changes_module",
            "write_set_changes_table",
        ];

        for table_name in drop_table_names {
            let sql = format!(r#"DROP TABLE "{}""#, table_name);
            execute_statement(manager, &sql).await?;
        }

        Ok(())
    }
}

async fn execute_statement(manager: &SchemaManager<'_>, sql: &str) -> Result<(), DbErr> {
    let stmt = Statement::from_string(manager.get_database_backend(), sql.to_owned());
    manager.get_connection().execute(stmt).await.map(|_| ())
}
