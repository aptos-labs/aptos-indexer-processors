use anyhow::Context as AnyhowContext;
use aptos_processor_framework::{
    CommonStorageConfig, DispatcherConfig, GrpcStreamSubscriberConfig,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::BufReader, path::PathBuf};

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(long)]
    pub config_path: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub stream_subscriber_config: GrpcStreamSubscriberConfig,
    pub dispatcher_config: DispatcherConfig,
    pub common_storage_config: CommonStorageConfig,
}

impl TryFrom<Args> for Config {
    type Error = anyhow::Error;

    fn try_from(args: Args) -> Result<Self, Self::Error> {
        let file = File::open(&args.config_path).with_context(|| {
            format!(
                "Failed to load config at {}",
                args.config_path.to_string_lossy()
            )
        })?;
        let reader = BufReader::new(file);
        let run_config: Config = serde_yaml::from_reader(reader).with_context(|| {
            format!(
                "Failed to parse config at {}",
                args.config_path.to_string_lossy()
            )
        })?;
        Ok(run_config)
    }
}
