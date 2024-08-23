use anyhow::Context;
use aptos_protos::transaction::v1::Transaction;
use diesel::{pg::PgConnection, sql_query, Connection, RunQueryDsl};
use itertools::Itertools;
use processor::{
    processors::{ProcessorConfig, ProcessorTrait},
    utils::database::{new_db_pool, run_pending_migrations},
    worker::build_processor_for_testing,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

mod event_processor;

/// The test context struct holds the test name and the transaction batches.
pub struct TestContext {
    pub transaction_batches: Vec<Transaction>,
    postgres_container: ContainerAsync<GenericImage>,
}

#[derive(Debug, Clone)]
pub struct TestProcessorConfig {
    config: ProcessorConfig,
}

impl TestContext {
    // TODO: move this to builder pattern to allow chaining.
    pub async fn new(txn_bytes: &[&[u8]]) -> anyhow::Result<Self> {
        let transaction_batches = txn_bytes
            .iter()
            .enumerate()
            .map(|(idx, txn)| {
                let mut txn: Transaction = serde_json::from_slice(txn).unwrap();
                txn.version = idx as u64;
                txn
            })
            .collect::<Vec<Transaction>>();
        let postgres_container = GenericImage::new("postgres", "14")
            .with_exposed_port(5432.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_DB", "postgres")
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .start()
            .await
            .expect("Redis started");
        Ok(TestContext {
            transaction_batches,
            postgres_container,
        })
    }

    async fn create_schema(&self) -> anyhow::Result<()> {
        let db_url = self.get_db_url().await;
        let mut conn = PgConnection::establish(&db_url)
            .with_context(|| format!("Error connecting to {}", db_url))?;
        // Drop the schema and recreate it.
        sql_query("DROP SCHEMA public CASCADE;")
            .execute(&mut conn)
            .unwrap();
        sql_query("CREATE SCHEMA public;")
            .execute(&mut conn)
            .unwrap();
        run_pending_migrations(&mut conn);
        Ok(())
    }

    pub async fn get_db_url(&self) -> String {
        let host = self.postgres_container.get_host().await.unwrap();
        let port = self
            .postgres_container
            .get_host_port_ipv4(5432)
            .await
            .unwrap();
        format!("postgres://postgres:postgres@{host}:{port}/postgres")
    }

    // The `run` function takes a closure that is executed after the test context is created.
    // The closure is executed multiple times with different permutations of the transactions.
    // For example:
    //   test.run(async move | context | {
    //       // Runs after every permutatation
    //       let result = events
    //         .select((transaction_version, event_index, type_))
    //         .filter(transaction_version.eq(0))
    //         .load::<(i64, i64, String)>(conn);
    //       assert_eq!(result.unwrap().len(), 1);
    //   })
    //   .await
    //   .is_ok());
    //
    pub async fn run<F>(
        &self,
        processor_config: TestProcessorConfig,
        verification_f: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(&mut PgConnection) -> anyhow::Result<()> + Send + Sync + 'static,
    {
        let transactions = self.transaction_batches.clone();
        let db_url = self.get_db_url().await;
        let mut conn = PgConnection::establish(&db_url)
            .with_context(|| format!("Error connecting to {}", db_url))?;
        let db_pool = new_db_pool(&db_url, None).await.unwrap();

        // Iterate over all permutations of the transaction batches to ensure that the processor can handle transactions in any order.
        // By testing different permutations, we verify that the order of transactions
        // does not affect the correctness of the processing logic.
        for perm in transactions.iter().permutations(transactions.len()) {
            self.create_schema().await?;
            let processor =
                build_processor_for_testing(processor_config.config.clone(), db_pool.clone());
            for txn in perm {
                let version = txn.version;
                processor
                    .process_transactions(vec![txn.clone()], version, version, None)
                    .await?;
            }
            // Run the verification function.
            verification_f(&mut conn)?;
        }
        Ok(())
    }
}
