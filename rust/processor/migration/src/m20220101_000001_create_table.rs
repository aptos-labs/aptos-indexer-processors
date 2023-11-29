use sea_orm::Statement;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // postgres db
        let events_sql = r#"
        CREATE TABLE events (
            sequence_number int8 NOT NULL,
            creation_number int8 NOT NULL,
            account_address varchar(66) NOT NULL,
            transaction_version int8 NOT NULL,
            transaction_block_height int8 NOT NULL,
            event_type varchar(300) NOT NULL,
            data jsonb NOT NULL,
            inserted_at timestamp NOT NULL DEFAULT now(),
            event_index int8 NOT NULL,
            CONSTRAINT events_pkey PRIMARY KEY (transaction_version, event_index)
        )"#;

        let transactions_sql = r#"
        CREATE TABLE transactions (
            transaction_version varchar(66) NOT NULL,
            transaction_block_height int8 NOT NULL,
            hash varchar(66) NOT NULL,
            transaction_type varchar(50) NOT NULL,
            payload jsonb,
            payload_type varchar(66),
            state_change_hash varchar(66) NOT NULL,
            event_root_hash varchar(66) NOT NULL,
            state_checkpoint_hash varchar(66),
            gas_used DECIMAL NOT NULL,
            success bool NOT NULL,
            vm_status varchar(66) NOT NULL,
            accumulator_root_hash varchar(66) NOT NULL,
            num_events int8 NOT NULL,
            num_write_set_changes int8 NOT NULL,
            epoch int8 NOT NULL,
            parent_signature_type varchar(66),
            sender varchar(66),
            sequence_number int8,
            max_gas_amount DECIMAL,
            expiration_timestamp_secs timestamp,
            gas_unit_price DECIMAL,
            timestamp timestamp,
            entry_function_id_str varchar(66),
            signature jsonb,
            id varchar(66),
            round int8,
            previous_block_votes_bitvec jsonb,
            proposer varchar(66),
            failed_proposer_indices jsonb,
            inserted_at timestamp NOT NULL DEFAULT now(),
            CONSTRAINT transactions_pkey PRIMARY KEY (transaction_version)
        )"#;

        let write_set_changes_module_sql = r#"
        CREATE TABLE write_set_changes_module (
            transaction_version varchar(66) NOT NULL,
            transaction_block_height int8 NOT NULL,
            hash varchar(66) NOT NULL,
            write_set_change_type varchar(66) NOT NULL,
            address varchar(66) NOT NULL,
            index int8 NOT NULL,
            is_deleted bool NOT NULL,
            bytecode bytea,
            friends jsonb,
            exposed_functions jsonb,
            structs jsonb,
            inserted_at timestamp NOT NULL DEFAULT now(),
            CONSTRAINT write_set_changes_module_pkey PRIMARY KEY (transaction_version,index)
        )"#;

        let write_set_changes_resource_sql = r#"
        CREATE TABLE write_set_changes_resource (
            transaction_version varchar(66) NOT NULL,
            transaction_block_height int8 NOT NULL,
            hash varchar(66) NOT NULL,
            write_set_change_type varchar(66) NOT NULL,
            address varchar(66) NOT NULL,
            index int8 NOT NULL,
            is_deleted bool NOT NULL,
            name varchar(66) NOT NULL,
            module varchar(66) NOT NULL,
            generic_type_params jsonb,
            state_key_hash varchar(66) NOT NULL,
            data jsonb,
            data_type varchar(300),
            inserted_at timestamp NOT NULL DEFAULT now(),
            CONSTRAINT write_set_changes_resource_pkey PRIMARY KEY (transaction_version,index)
        )"#;

        let write_set_changes_table_sql = r#"
        CREATE TABLE write_set_changes_table (
            transaction_version varchar(66) NOT NULL,
            transaction_block_height int8 NOT NULL,
            hash varchar(66) NOT NULL,
            write_set_change_type varchar(66) NOT NULL,
            address varchar(66) NOT NULL,
            index int8 NOT NULL,
            is_deleted bool NOT NULL,
            table_handle varchar(66) NOT NULL,
            key varchar(66) NOT NULL,
            data jsonb,
            inserted_at timestamp NOT NULL DEFAULT now(),
            CONSTRAINT write_set_changes_table_pkey PRIMARY KEY (transaction_version,index)
        )"#;

        let write_set_changes_sql = r#"
        CREATE TABLE write_set_changes (
            transaction_version varchar(66) NOT NULL,
            transaction_block_height INT8 NOT NULL,
            hash varchar(66) NOT NULL,
            write_set_change_type varchar(66) NOT NULL,
            address varchar(66) NOT NULL,
            index INT8 NOT NULL,
            inserted_at timestamp NOT NULL DEFAULT now(),
            CONSTRAINT write_set_changes_pkey PRIMARY KEY (transaction_version,index)
        )"#;

        // cockroach db

        // let events_sql = r#"
        // CREATE TABLE "events" (
        //     "sequence_number" INT8 NOT NULL,
        //     "creation_number" INT8 NOT NULL,
        //     "account_address" STRING(66) NOT NULL,
        //     "transaction_version" STRING(66) NOT NULL,
        //     "transaction_block_height" INT8 NOT NULL,
        //     "event_index" INT8 NOT NULL,
        //     "event_type" STRING(300) NOT NULL,
        //     "data" JSONB NOT NULL,
        //     "inserted_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
        //     CONSTRAINT events_pkey PRIMARY KEY (transaction_version, event_index)
        // )"#;

        // let transactions_sql = r#"
        // CREATE TABLE transactions (
        //     transaction_version STRING(66) NOT NULL,
        //     transaction_block_height INT8 NOT NULL,
        //     hash STRING(66) NOT NULL,
        //     transaction_type STRING(50) NOT NULL,
        //     payload JSONB,
        //     payload_type STRING(66),
        //     state_change_hash STRING(66) NOT NULL,
        //     event_root_hash STRING(66) NOT NULL,
        //     state_checkpoint_hash STRING(66),
        //     gas_used DECIMAL NOT NULL,
        //     success BOOL NOT NULL,
        //     vm_status STRING NOT NULL,
        //     accumulator_root_hash STRING(66) NOT NULL,
        //     num_events INT8 NOT NULL,
        //     num_write_set_changes INT8 NOT NULL,
        //     epoch INT8 NOT NULL,
        //     parent_signature_type STRING,
        //     sender STRING(66),
        //     sequence_number INT8,
        //     max_gas_amount DECIMAL,
        //     expiration_timestamp_secs TIMESTAMP(6),
        //     gas_unit_price DECIMAL,
        //     timestamp TIMESTAMP(6),
        //     entry_function_id_str STRING,
        //     signature JSONB,
        //     id STRING(66),
        //     round INT8,
        //     previous_block_votes_bitvec JSONB,
        //     proposer STRING(66),
        //     failed_proposer_indices JSONB,
        //     inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

        //     CONSTRAINT transactions_pkey PRIMARY KEY (transaction_version)
        // )"#;

        // let write_set_changes_module_sql = r#"
        // CREATE TABLE write_set_changes_module (
        //     transaction_version STRING(66) NOT NULL,
        //     transaction_block_height INT8 NOT NULL,
        //     hash STRING(66) NOT NULL,
        //     write_set_change_type STRING NOT NULL,
        //     address STRING(66) NOT NULL,
        //     index INT8 NOT NULL,
        //     is_deleted BOOL NOT NULL,
        //     bytecode BYTES,
        //     friends JSONB,
        //     exposed_functions JSONB,
        //     structs JSONB,
        //     inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

        //     CONSTRAINT write_set_changes_module_pkey PRIMARY KEY (transaction_version,index)
        // )"#;

        // let write_set_changes_resource_sql = r#"
        // CREATE TABLE write_set_changes_resource (
        //     transaction_version STRING(66) NOT NULL,
        //     transaction_block_height INT8 NOT NULL,
        //     hash STRING(66) NOT NULL,
        //     write_set_change_type STRING NOT NULL,
        //     address STRING(66) NOT NULL,
        //     index INT8 NOT NULL,
        //     is_deleted BOOL NOT NULL,
        //     name STRING NOT NULL,
        //     module STRING NOT NULL,
        //     generic_type_params JSONB,
        //     state_key_hash STRING(66) NOT NULL,
        //     data JSONB,
        //     data_type STRING(300),
        //     inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

        //     CONSTRAINT write_set_changes_resource_pkey PRIMARY KEY (transaction_version,index)
        // )"#;

        // let write_set_changes_table_sql = r#"
        // CREATE TABLE write_set_changes_table (
        //     transaction_version STRING(66) NOT NULL,
        //     transaction_block_height INT8 NOT NULL,
        //     hash STRING(66) NOT NULL,
        //     write_set_change_type STRING NOT NULL,
        //     address STRING(66) NOT NULL,
        //     index INT8 NOT NULL,
        //     is_deleted BOOL NOT NULL,
        //     table_handle STRING(66) NOT NULL,
        //     key STRING NOT NULL,
        //     data JSONB,
        //     inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

        //     CONSTRAINT write_set_changes_table_pkey PRIMARY KEY (transaction_version,index)
        // )"#;

        // let write_set_changes_sql = r#"
        // CREATE TABLE "write_set_changes" (
        //     "transaction_version" STRING(66) NOT NULL,
        //     "transaction_block_height" INT8 NOT NULL,
        //     "hash" STRING(66) NOT NULL,
        //     "write_set_change_type" STRING NOT NULL,
        //     "address" STRING(66) NOT NULL,
        //     "index" INT8 NOT NULL,
        //     "inserted_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

        //     CONSTRAINT "write_set_changes_pkey" PRIMARY KEY ("transaction_version","index")
        // )"#;

        let events_stmt =
            Statement::from_string(manager.get_database_backend(), events_sql.to_owned());
        let transactions_stmt =
            Statement::from_string(manager.get_database_backend(), transactions_sql.to_owned());
        let write_set_changes_module_stmt = Statement::from_string(
            manager.get_database_backend(),
            write_set_changes_module_sql.to_owned(),
        );
        let write_set_changes_resource_stmt = Statement::from_string(
            manager.get_database_backend(),
            write_set_changes_resource_sql.to_owned(),
        );
        let write_set_changes_table_stmt = Statement::from_string(
            manager.get_database_backend(),
            write_set_changes_table_sql.to_owned(),
        );
        let write_set_changes_stmt =
            Statement::from_string(
                manager.get_database_backend(),
                write_set_changes_sql.to_owned(),
            );
        let _ =
            manager
                .get_connection()
                .execute(events_stmt)
                .await
                .map(|_| ());
        let _ =
            manager
                .get_connection()
                .execute(transactions_stmt)
                .await
                .map(|_| ());
        let _ =
            manager
                .get_connection()
                .execute(write_set_changes_stmt)
                .await
                .map(|_| ());
        let _ = manager
            .get_connection()
            .execute(write_set_changes_module_stmt)
            .await
            .map(|_| ());
        let _ = manager
            .get_connection()
            .execute(write_set_changes_resource_stmt)
            .await
            .map(|_| ());
        manager
            .get_connection()
            .execute(write_set_changes_table_stmt)
            .await
            .map(|_| ())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let events_sql = r#"DROP TABLE "events""#;
        let transactions_sql = r#"DROP TABLE "transactions""#;
        let write_set_changes_sql = r#"DROP TABLE "write_set_changes""#;
        let write_set_changes_module_sql = r#"DROP TABLE "write_set_changes_module""#;
        let write_set_changes_resource_sql = r#"DROP TABLE "write_set_changes_resource""#;
        let write_set_changes_table_sql = r#"DROP TABLE "write_set_changes_table""#;
        let events_stmt =
            Statement::from_string(manager.get_database_backend(), events_sql.to_owned());
        let transactions_stmt =
            Statement::from_string(manager.get_database_backend(), transactions_sql.to_owned());
        let write_set_changes_stmt =
            Statement::from_string(
                manager.get_database_backend(),
                write_set_changes_sql.to_owned(),
            );
        let write_set_changes_module_stmt = Statement::from_string(
            manager.get_database_backend(),
            write_set_changes_module_sql.to_owned(),
        );
        let write_set_changes_resource_stmt = Statement::from_string(
            manager.get_database_backend(),
            write_set_changes_resource_sql.to_owned(),
        );
        let write_set_changes_table_stmt = Statement::from_string(
            manager.get_database_backend(),
            write_set_changes_table_sql.to_owned(),
        );
        let _ =
            manager
                .get_connection()
                .execute(events_stmt)
                .await
                .map(|_| ());
        let _ =
            manager
                .get_connection()
                .execute(transactions_stmt)
                .await
                .map(|_| ());
        let _ =
            manager
                .get_connection()
                .execute(write_set_changes_stmt)
                .await
                .map(|_| ());
        let _ = manager
            .get_connection()
            .execute(write_set_changes_module_stmt)
            .await
            .map(|_| ());
        let _ = manager
            .get_connection()
            .execute(write_set_changes_resource_stmt)
            .await
            .map(|_| ());
        manager
            .get_connection()
            .execute(write_set_changes_table_stmt)
            .await
            .map(|_| ())
    }
}
