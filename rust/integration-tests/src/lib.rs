use anyhow::Context;
use aptos_protos::transaction::v1::Transaction;
use diesel::{pg::PgConnection, sql_query, Connection, RunQueryDsl};
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

mod diff_test_helper;
mod diff_tests;
mod models;
mod scenarios_tests;

/// The test context struct holds the test name and the transaction batches.
pub struct TestContext {
    pub transaction_batches: Vec<Transaction>,
    postgres_container: ContainerAsync<GenericImage>,
}

#[derive(Debug, Clone)]
pub struct TestProcessorConfig {
    pub config: ProcessorConfig,
}

impl TestContext {
    // TODO: move this to builder pattern to allow chaining.
    pub async fn new(txn_bytes: &[&[u8]]) -> anyhow::Result<Self> {
        let transaction_batches = txn_bytes
            .iter()
            .map(|txn| {
                let txn: Transaction = serde_json::from_slice(txn).unwrap();
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
        test_type: TestType,
        verification_f: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(&mut PgConnection, &str) -> anyhow::Result<()> + Send + Sync + 'static,
    {
        let transactions = self.transaction_batches.clone();
        let db_url = self.get_db_url().await;
        let mut conn = PgConnection::establish(&db_url)
            .with_context(|| format!("Error connecting to {}", db_url))?;
        let db_pool = new_db_pool(&db_url, None).await.unwrap();

        self.create_schema().await?;
        let processor =
            build_processor_for_testing(processor_config.config.clone(), db_pool.clone());

        let mut last_version = None;

        for txn in transactions.iter() {
            let version = txn.version;
            processor
                .process_transactions(vec![txn.clone()], version, version, None)
                .await?;
            // }

            last_version = Some(version);

            // For DiffTest, run verification after each transaction
            if matches!(test_type, TestType::Diff(_)) {
                test_type.run_verification(&mut conn, &version.to_string(), &verification_f)?;
            }
        }
        // For ScenarioTest, use the last transaction version if needed
        if matches!(test_type, TestType::Scenario(_)) {
            if let Some(last_version) = last_version {
                test_type.run_verification(
                    &mut conn,
                    &last_version.to_string(),
                    &verification_f,
                )?;
            } else {
                return Err(anyhow::anyhow!(
                    "No transactions found to get the last version"
                ));
            }
        }

        Ok(())
    }
}

trait TestStrategy {
    fn verify(
        &self,
        conn: &mut PgConnection,
        version: &str,
        verification_f: &dyn Fn(&mut PgConnection, &str) -> anyhow::Result<()>,
    ) -> anyhow::Result<()>;
}

pub struct DiffTest;

impl TestStrategy for DiffTest {
    fn verify(
        &self,
        conn: &mut PgConnection,
        version: &str,
        verification_f: &dyn Fn(&mut PgConnection, &str) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        verification_f(conn, version)
    }
}

pub struct ScenarioTest;

impl TestStrategy for ScenarioTest {
    fn verify(
        &self,
        conn: &mut PgConnection,
        version: &str,
        verification_f: &dyn Fn(&mut PgConnection, &str) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        verification_f(conn, version)
    }
}

pub enum TestType {
    Diff(DiffTest),
    Scenario(ScenarioTest),
}

impl TestType {
    fn run_verification(
        &self,
        conn: &mut PgConnection,
        version: &str,
        verification_f: &dyn Fn(&mut PgConnection, &str) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        match self {
            TestType::Diff(strategy) => strategy.verify(conn, version, verification_f),
            TestType::Scenario(strategy) => strategy.verify(conn, version, verification_f),
        }
    }
}
