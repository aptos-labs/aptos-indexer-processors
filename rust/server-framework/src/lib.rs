// Copyright © Aptos Foundation

use anyhow::{bail, Context, Ok, Result};
use backtrace::Backtrace;
use clap::Parser;
use prometheus::{Encoder, TextEncoder};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{fs::File, io::Read, panic::PanicInfo, path::PathBuf, process};
use tokio::runtime::Handle;
use tracing::error;
use tracing_subscriber::EnvFilter;
use warp::{http::Response, Filter};

/// ServerArgs bootstraps a server with all common pieces. And then triggers the run method for
/// the specific service.
#[derive(Parser)]
pub struct ServerArgs {
    #[clap(short, long, value_parser)]
    pub config_path: PathBuf,
    #[clap(short, long)]
    pub verbose: Option<bool>,
}

impl ServerArgs {
    pub async fn run<C>(&self, handle: Handle) -> Result<()>
    where
        C: RunnableConfig,
    {
        // Set up the server.
        setup_logging();
        setup_panic_handler();
        let config = load::<GenericConfig<C>>(&self.config_path)?;
        run_server_with_config(config, handle, self.verbose).await
    }
}

/// Run a server and the necessary probes. For spawning these tasks, the user must
/// provide a handle to a runtime they already have.
pub async fn run_server_with_config<C>(
    config: GenericConfig<C>,
    handle: Handle,
    verbose: Option<bool>,
) -> Result<()>
where
    C: RunnableConfig,
{
    let health_port = config.health_check_port;
    // Start liveness and readiness probes.
    let task_handler = handle.spawn(async move {
        register_probes_and_metrics_handler(health_port).await;
        Ok(())
    });
    let main_task_handler = handle.spawn(async move { config.run(verbose).await });
    tokio::select! {
        _ = task_handler => {
            error!("Probes and metrics handler unexpectedly exited");
            bail!("Probes and metrics handler unexpectedly exited");
        },
        _ = main_task_handler => {
            error!("Main task unexpectedly exited");
            bail!("Main task unexpectedly exited");
        },
    }
}

#[derive(Deserialize, Debug, Serialize)]
pub struct GenericConfig<T> {
    // Shared configuration among all services.
    pub health_check_port: u16,

    // Specific configuration for each service.
    pub server_config: T,
}

#[async_trait::async_trait]
impl<T> RunnableConfig for GenericConfig<T>
where
    T: RunnableConfig,
{
    async fn run(&self, verbose: Option<bool>) -> Result<()> {
        self.server_config.run(verbose).await
    }

    fn get_server_name(&self) -> String {
        self.server_config.get_server_name()
    }
}

/// RunnableConfig is a trait that all services must implement for their configuration.
#[async_trait::async_trait]
pub trait RunnableConfig: DeserializeOwned + Send + Sync + 'static {
    async fn run(&self, verbose: Option<bool>) -> Result<()>;
    fn get_server_name(&self) -> String;
}

/// Parse a yaml file into a struct.
pub fn load<T: for<'de> Deserialize<'de>>(path: &PathBuf) -> Result<T> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open the file at path: {:?}", path))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .with_context(|| format!("failed to read the file at path: {:?}", path))?;
    serde_yaml::from_str::<T>(&contents).context("Unable to parse yaml file")
}

#[derive(Debug, Serialize)]
pub struct CrashInfo {
    details: String,
    backtrace: String,
}

/// Invoke to ensure process exits on a thread panic.
///
/// Tokio's default behavior is to catch panics and ignore them.  Invoking this function will
/// ensure that all subsequent thread panics (even Tokio threads) will report the
/// details/backtrace and then exit.
pub fn setup_panic_handler() {
    std::panic::set_hook(Box::new(move |pi: &PanicInfo<'_>| {
        handle_panic(pi);
    }));
}

// Formats and logs panic information
fn handle_panic(panic_info: &PanicInfo<'_>) {
    // The Display formatter for a PanicInfo contains the message, payload and location.
    let details = format!("{}", panic_info);
    let backtrace = format!("{:#?}", Backtrace::new());
    let info = CrashInfo { details, backtrace };
    let crash_info = toml::to_string_pretty(&info).unwrap();
    error!("{}", crash_info);
    // TODO / HACK ALARM: Write crash info synchronously via eprintln! to ensure it is written before the process exits which error! doesn't guarantee.
    // This is a workaround until https://github.com/aptos-labs/aptos-core/issues/2038 is resolved.
    eprintln!("{}", crash_info);
    // Kill the process
    process::exit(12);
}

/// Set up logging for the server.
pub fn setup_logging() {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();
    tracing_subscriber::fmt()
        .json()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .with_thread_names(true)
        .with_env_filter(env_filter)
        .init();
}

/// Register readiness and liveness probes and set up metrics endpoint.
async fn register_probes_and_metrics_handler(port: u16) {
    let readiness = warp::path("readiness")
        .map(move || warp::reply::with_status("ready", warp::http::StatusCode::OK));
    let metrics_endpoint = warp::path("metrics").map(|| {
        // Metrics encoding.
        let metrics = prometheus::gather();
        let mut encode_buffer = vec![];
        let encoder = TextEncoder::new();
        // If metrics encoding fails, we want to panic and crash the process.
        encoder
            .encode(&metrics, &mut encode_buffer)
            .context("Failed to encode metrics")
            .unwrap();

        Response::builder()
            .header("Content-Type", "text/plain")
            .body(encode_buffer)
    });
    warp::serve(readiness.or(metrics_endpoint))
        .run(([0, 0, 0, 0], port))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[derive(Clone, Debug, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct TestConfig {
        test: u32,
        test_name: String,
    }

    #[async_trait::async_trait]
    impl RunnableConfig for TestConfig {
        async fn run(&self, _verbose: Option<bool>) -> Result<()> {
            assert_eq!(self.test, 123);
            assert_eq!(self.test_name, "test");
            Ok(())
        }

        fn get_server_name(&self) -> String {
            self.test_name.clone()
        }
    }

    #[test]
    fn test_random_config_creation() {
        let dir = tempdir().expect("tempdir failure");

        let file_path = dir.path().join("testing_yaml.yaml");
        let mut file = File::create(&file_path).expect("create failure");
        let raw_yaml_content = r#"
            health_check_port: 12345
            server_config:
                test: 123
                test_name: "test"
        "#;
        writeln!(file, "{}", raw_yaml_content).expect("write_all failure");

        let config = load::<GenericConfig<TestConfig>>(&file_path).unwrap();
        assert_eq!(config.health_check_port, 12345);
        assert_eq!(config.server_config.test, 123);
        assert_eq!(config.server_config.test_name, "test");
    }

    #[test]
    fn verify_tool() {
        use clap::CommandFactory;
        ServerArgs::command().debug_assert()
    }
}
