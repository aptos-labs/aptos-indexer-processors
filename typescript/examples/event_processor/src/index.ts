import { program } from "commander";
import { Config, Worker } from "@aptos-labs/aptos-processor-sdk";
import { EventProcessor } from "./processor";
import { Event } from "./models";

type Args = {
  config: string;
  perf: number;
};

program
  .command("process")
  .requiredOption("--config <config>", "Path to a yaml config file")
  .action(async (args: Args) => {
    await main(args);
  });

async function main({ config: configPath }: Args) {
  const config = Config.from_yaml_file(configPath);
  const processor = new EventProcessor();
  const worker = new Worker({
    config,
    processor,
    models: [Event],
  });
  await worker.run();
}

program.parse();
